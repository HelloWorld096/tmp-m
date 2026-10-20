#!/usr/bin/env python3
"""Command agent for the on-demand remote Linux VM.

Runs on the GitHub Actions runner, bound to 127.0.0.1 and exposed through a
Cloudflare quick tunnel. The Worker talks to it with a per-session bearer token.

Design (see README.md and docs/OPERATIONS.md for the surrounding system):
  * Commands run as *jobs*. POST /jobs returns immediately with a job id; the
    caller long-polls GET /jobs/{id}?wait=N. Nothing blocks for minutes, so a
    slow Gradle build can't hit a proxy/Worker timeout and can't be run twice by
    a retry.
  * Every endpoint is a plain `def`, so FastAPI runs it in a worker thread and
    the event loop (and therefore /health) is never blocked by a running job.
  * Output goes to a file, never a pipe. A backgrounded process (`server &`,
    the Gradle daemon, ...) therefore cannot keep a request hanging.
  * Each job gets its own process group (start_new_session) so timeouts and
    cancellation kill the whole tree, not just the top-level bash.
  * The working directory is tracked with an EXIT trap, so it survives
    `exit N`, and stdout/stderr stay interleaved in one stream.
  * Child processes get a reduced environment (no session token, no Actions
    runtime tokens). This only stops *accidental* leakage: commands run as the
    runner user, which can usually sudo and read other processes' memory, so a
    command can still reach the runner's secrets if it tries (docs/SECURITY.md).
  * Kill switch: POST /cancel stops whatever is running without needing its job id
    (the "Ctrl-C" for a command that loops), and with kill_background=true also
    sweeps processes that escaped the job's process group (nohup/setsid daemons).
  * Submission is idempotent: POST /jobs may carry a client-chosen request_id.
    If the reply was lost and the caller retries with the same id, it gets the
    original job back instead of a second execution (or a spurious "busy").
"""
from __future__ import annotations

import hmac
import os
import re
import secrets
import shlex
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Literal, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

MAX_COMMAND_CHARS = 20_000
DEFAULT_JOB_SECONDS = 900
MAX_JOB_SECONDS = 3_600
MAX_WAIT_SECONDS = 55
OUTPUT_TAIL_CHARS_FINISHED = 20_000
OUTPUT_TAIL_CHARS_RUNNING = 4_000
MAX_OUTPUT_BYTES = 100 * 1024 * 1024
KEEP_JOBS = 20
KILL_GRACE_SECONDS = 5

SENSITIVE_ENV_EXACT = {"SESSION_TOKEN", "GITHUB_TOKEN", "GH_TOKEN"}
SENSITIVE_ENV_PREFIXES = ("ACTIONS_RUNTIME", "ACTIONS_ID_TOKEN", "ACTIONS_CACHE")
# Every job's environment carries this variable. Processes inherit it even after they leave the job's
# process group (nohup/setsid), which is how the kill switch finds them again.
JOB_MARK_VAR = "VM_AGENT_JOB"

ACTIVITY_FILE = Path(os.environ.get("VM_ACTIVITY_FILE", "/tmp/vm-activity"))
JOBS_DIR = Path(os.environ.get("VM_JOBS_DIR", "/tmp/vm-jobs"))
# Installed root-owned by the pinned Actions workflow, outside the mutable checkout. Tests may override.
RELAY_SCRIPT = Path(os.environ.get("VM_RELAY_SCRIPT", "/opt/relay/relay.sh"))


def touch_activity() -> None:
    """Record activity. The workflow's idle loop reads this file's mtime."""
    try:
        ACTIVITY_FILE.touch()
        os.utime(ACTIVITY_FILE, None)
    except OSError:
        pass


def scrubbed_env(job_id: Optional[str] = None) -> dict:
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in SENSITIVE_ENV_EXACT and not k.startswith(SENSITIVE_ENV_PREFIXES)
        and k != JOB_MARK_VAR
    }
    if job_id is not None:
        env[JOB_MARK_VAR] = job_id
    env.setdefault("TERM", "dumb")
    env.setdefault("DEBIAN_FRONTEND", "noninteractive")
    return env


def sweep_marked_processes() -> int:
    """SIGKILL every process (of this user) that carries the job marker. Returns how many were signalled.

    Reads /proc/<pid>/environ, which Linux only lets a process read for its own user. The agent, the
    tunnel and the runner never carry the marker, so they are never touched. Best effort: a process that
    cleared its environment (env -i) cannot be found this way.
    """
    me = os.getpid()
    needle = (JOB_MARK_VAR + "=").encode()
    killed = 0
    try:
        pids = [int(n) for n in os.listdir("/proc") if n.isdigit()]
    except OSError:
        return 0
    for pid in pids:
        if pid == me:
            continue
        try:
            with open(f"/proc/{pid}/environ", "rb") as fh:
                env = fh.read()
        except OSError:
            continue
        if any(item.startswith(needle) for item in env.split(b"\0")):
            try:
                os.kill(pid, signal.SIGKILL)
                killed += 1
            except OSError:
                pass
    return killed


class Busy(Exception):
    def __init__(self, job_id: str):
        super().__init__(job_id)
        self.job_id = job_id


class Job:
    def __init__(self, command: str, timeout: int, directory: Path, request_id: Optional[str] = None):
        self.id = uuid.uuid4().hex[:16]
        self.request_id = request_id
        self.command = command
        self.timeout = timeout
        self.dir = directory
        self.output_path = directory / "output"
        self.meta_path = directory / "meta"
        self.started = time.time()
        self.ended: Optional[float] = None
        self.done = threading.Event()
        self.cancel_requested = False
        self.timed_out = False
        self.truncated_by_size = False
        self.exit_code: Optional[int] = None
        self.cwd: Optional[str] = None
        self.proc: Optional[subprocess.Popen] = None
        # Files that must not outlive the job (e.g. a Relay token file), removed by the watcher even when the
        # job's own EXIT trap could not run because the process was SIGKILLed on timeout or cancel.
        self.cleanup_files: list[Path] = []

    @property
    def state(self) -> str:
        if not self.done.is_set():
            return "running"
        if self.timed_out:
            return "timed_out"
        if self.cancel_requested:
            return "cancelled"
        return "finished"

    def read_output(self, limit_chars: int) -> tuple[str, bool]:
        """Return (tail of output, whether anything was cut off)."""
        try:
            size = self.output_path.stat().st_size
        except OSError:
            return "", False
        # UTF-8 is at most 4 bytes/char; read a little extra, then trim by chars.
        want = limit_chars * 4
        with open(self.output_path, "rb") as fh:
            if size > want:
                fh.seek(size - want)
            raw = fh.read()
        text = raw.decode("utf-8", errors="replace")
        cut = size > want
        if len(text) > limit_chars:
            text, cut = text[-limit_chars:], True
        return text, cut

    def describe(self) -> dict:
        finished = self.done.is_set()
        text, cut = self.read_output(
            OUTPUT_TAIL_CHARS_FINISHED if finished else OUTPUT_TAIL_CHARS_RUNNING
        )
        return {
            "job_id": self.id,
            "status": self.state,
            "exit_code": self.exit_code,
            "cwd": self.cwd,
            "elapsed_seconds": round((self.ended or time.time()) - self.started, 1),
            "output": text,
            "output_truncated": cut or self.truncated_by_size,
        }


class JobManager:
    def __init__(self, workspace: str):
        self.workspace = workspace
        self.cwd = workspace
        self.lock = threading.Lock()
        self.current: Optional[Job] = None
        self.jobs: "OrderedDict[str, Job]" = OrderedDict()
        self.by_request: dict[str, Job] = {}
        JOBS_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)

    # -- lifecycle ---------------------------------------------------------
    def submit(self, command: str, timeout: int, request_id: Optional[str] = None, cleanup_files: Optional[list[Path]] = None) -> tuple[Job, bool]:
        """Start a job. Returns (job, duplicate); duplicate=True means request_id was already seen."""
        with self.lock:
            # Replay check comes first: a retry of a submission that already started must get its own
            # job back, not "busy" (the job it started is what is keeping the agent busy).
            if request_id is not None and request_id in self.by_request:
                return self.by_request[request_id], True
            if self.current is not None and not self.current.done.is_set():
                raise Busy(self.current.id)
            job_dir = JOBS_DIR / uuid.uuid4().hex
            job_dir.mkdir(mode=0o700)
            job = Job(command, timeout, job_dir, request_id)
            job.cleanup_files = list(cleanup_files or [])
            job.output_path.touch(mode=0o600)
            script = (
                f"cd -- {shlex.quote(self.cwd)} 2>/dev/null || cd -- {shlex.quote(self.workspace)}\n"
                # EXIT trap: runs after the user's last command *and* after `exit N`.
                f"trap '__vm_rc=$?; printf \"%s\\t%s\" \"$PWD\" \"$__vm_rc\" > {shlex.quote(str(job.meta_path))}' EXIT\n"
                f"{command}\n"
            )
            out = open(job.output_path, "ab", buffering=0)
            try:
                job.proc = subprocess.Popen(
                    ["bash", "-lc", script],
                    stdin=subprocess.DEVNULL,
                    stdout=out,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,  # own process group => killable as a unit
                    env=scrubbed_env(job.id),
                    close_fds=True,
                )
            finally:
                out.close()
            self.current = job
            self.jobs[job.id] = job
            if request_id is not None:
                self.by_request[request_id] = job
            self._prune()
        touch_activity()
        threading.Thread(target=self._watch, args=(job,), daemon=True).start()
        return job, False

    def find_request(self, request_id: str) -> Optional[Job]:
        with self.lock:
            return self.by_request.get(request_id)

    def get(self, job_id: str) -> Optional[Job]:
        with self.lock:
            return self.jobs.get(job_id)

    def cancel(self, job: Job) -> None:
        proc = job.proc
        # A job that already exited (the watcher just hasn't recorded it yet) finished on its own terms;
        # don't relabel it "cancelled".
        if job.done.is_set() or proc is None or proc.poll() is not None:
            return
        job.cancel_requested = True
        self._kill_group(job)

    def running(self) -> Optional[Job]:
        with self.lock:
            cur = self.current
            return cur if cur is not None and not cur.done.is_set() else None

    # -- internals ---------------------------------------------------------
    def _prune(self) -> None:
        while len(self.jobs) > KEEP_JOBS:
            old_id, old = next(iter(self.jobs.items()))
            if not old.done.is_set():
                break
            self.jobs.pop(old_id)
            if old.request_id is not None:
                self.by_request.pop(old.request_id, None)
            for p in (old.output_path, old.meta_path):
                try:
                    p.unlink()
                except OSError:
                    pass
            try:
                old.dir.rmdir()
            except OSError:
                pass

    @staticmethod
    def _kill_group(job: Job) -> None:
        proc = job.proc
        if proc is None or proc.poll() is not None:
            return
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        deadline = time.time() + KILL_GRACE_SECONDS
        while time.time() < deadline and proc.poll() is None:
            time.sleep(0.1)
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def _watch(self, job: Job) -> None:
        proc = job.proc
        assert proc is not None
        deadline = job.started + job.timeout
        last_touch = 0.0
        while proc.poll() is None:
            now = time.time()
            if now - last_touch > 10:  # a running job counts as activity
                touch_activity()
                last_touch = now
            if now > deadline and not job.timed_out:
                job.timed_out = True
                self._kill_group(job)
                break
            try:
                if job.output_path.stat().st_size > MAX_OUTPUT_BYTES:
                    job.truncated_by_size = True
                    job.cancel_requested = True
                    self._kill_group(job)
                    break
            except OSError:
                pass
            time.sleep(0.25)
        try:
            proc.wait(timeout=KILL_GRACE_SECONDS + 5)
        except subprocess.TimeoutExpired:
            pass
        for f in job.cleanup_files:
            try:
                f.unlink(missing_ok=True)
            except OSError:
                pass

        cwd, rc = None, None
        try:
            raw = job.meta_path.read_text()
            cwd_s, rc_s = raw.rsplit("\t", 1)
            cwd, rc = cwd_s, int(rc_s)
        except (OSError, ValueError):
            pass
        if cwd and os.path.isdir(cwd):
            self.cwd = cwd
        job.cwd = self.cwd
        if job.timed_out:
            job.exit_code = 124
        elif job.cancel_requested:
            job.exit_code = 130
        elif rc is not None:
            job.exit_code = rc
        else:
            # No meta file: the shell never ran our EXIT trap (`exec cmd`, or the command replaced the
            # trap). The process's own exit status is still right; only the working directory is lost.
            code = proc.returncode
            if code is None:
                job.exit_code = 1
            else:
                job.exit_code = 128 - code if code < 0 else code
        job.ended = time.time()
        job.done.set()
        touch_activity()


class JobRequest(BaseModel):
    command: str = Field(min_length=1, max_length=MAX_COMMAND_CHARS)
    timeout_seconds: Optional[int] = Field(default=None, ge=1, le=MAX_JOB_SECONDS)
    request_id: Optional[str] = Field(default=None, pattern=r"^[0-9a-f]{32}$")


class RelayJobRequest(BaseModel):
    mode: Literal["recv", "put"]
    repo: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=200)
    path: str = Field(min_length=1, max_length=4096)
    # GitHub App installation tokens are [A-Za-z0-9_]; refuse anything that could break out of a curl config
    # line (quotes, newlines, spaces) rather than trusting the caller.
    token: str = Field(pattern=r"^[A-Za-z0-9_-]{20,512}$")
    request_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    timeout_seconds: int = Field(default=1800, ge=1, le=MAX_JOB_SECONDS)

    @field_validator("repo")
    @classmethod
    def _no_dot_segments(cls, v: str) -> str:
        if any(part in (".", "..") for part in v.split("/")):
            raise ValueError("repo must not contain '.' or '..' segments")
        return v


class CancelRequest(BaseModel):
    kill_background: bool = False


def create_app(token: str, workspace: str) -> FastAPI:
    if not token:
        raise ValueError("session token must not be empty")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    manager = JobManager(workspace)

    def is_authorized(request: Request) -> bool:
        supplied = request.headers.get("Authorization", "")
        return hmac.compare_digest(supplied.encode(), f"Bearer {token}".encode())

    # Authentication is enforced twice on purpose. The middleware runs before any request parsing, so an
    # unauthenticated caller gets 401 for everything (even malformed JSON) and learns nothing about the
    # request schema; the per-route dependency keeps each route safe even if the middleware is ever changed.
    @app.middleware("http")
    async def auth_gate(request: Request, call_next):
        if request.url.path != "/health" and not is_authorized(request):
            return JSONResponse(status_code=401, content={"detail": "unauthorized"})
        return await call_next(request)

    def require_auth(request: Request) -> None:
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="unauthorized")

    @app.get("/health")
    def health():
        return {"status": "alive"}

    @app.get("/status", dependencies=[Depends(require_auth)])
    def agent_status():
        """Is a job running right now? Lets the Worker refuse a transfer BEFORE it creates a repo and a token."""
        cur = manager.current
        busy = cur is not None and not cur.done.is_set()
        return {"busy": busy, "job_id": cur.id if busy else None}

    @app.post("/jobs", dependencies=[Depends(require_auth)])
    def submit(req: JobRequest, request: Request):
        if not req.command.strip():
            raise HTTPException(status_code=400, detail="Missing command")
        try:
            job, duplicate = manager.submit(
                req.command, req.timeout_seconds or DEFAULT_JOB_SECONDS, req.request_id
            )
        except Busy as busy:
            return JSONResponse(
                status_code=409, content={"error": "busy", "job_id": busy.job_id}
            )
        return {"job_id": job.id, "status": job.state, "duplicate": duplicate}

    @app.post("/relay/jobs", dependencies=[Depends(require_auth)])
    def submit_relay(req: RelayJobRequest, request: Request):
        """Start only the Runner-side half of a Relay transfer; credentials never enter shell argv."""
        existing = manager.find_request(req.request_id)
        if existing is not None:
            touch_activity()
            return {"job_id": existing.id, "status": existing.state, "duplicate": True}
        script = RELAY_SCRIPT
        if not script.is_file() or not os.access(script, os.X_OK):
            raise HTTPException(status_code=503, detail="approved relay.sh is not installed on this runner")
        token_file = JOBS_DIR / ("relay-token-" + secrets.token_hex(16))
        try:
            fd = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(req.token)
            qtoken = shlex.quote(str(token_file))
            command = (
                f"( trap 'rm -f -- {qtoken}' EXIT; trap 'exit 143' TERM INT; "
                f"export RELAY_TOKEN_FILE={qtoken}; "
                f"{shlex.quote(str(script))} {req.mode} {shlex.quote(req.repo)} {shlex.quote(req.path)} )"
            )
            job, duplicate = manager.submit(command, req.timeout_seconds, req.request_id, cleanup_files=[token_file])
            if duplicate:
                token_file.unlink(missing_ok=True)
            touch_activity()
            return {"job_id": job.id, "status": job.state, "duplicate": duplicate}
        except Busy as busy:
            token_file.unlink(missing_ok=True)
            return JSONResponse(status_code=409, content={"error": "busy", "job_id": busy.job_id})
        except Exception:
            token_file.unlink(missing_ok=True)
            raise

    @app.get("/jobs/{job_id}", dependencies=[Depends(require_auth)])
    def status(job_id: str, request: Request, wait: float = 0):
        job = manager.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="unknown job")
        touch_activity()
        wait = max(0.0, min(wait, MAX_WAIT_SECONDS))
        if wait and not job.done.is_set():
            job.done.wait(timeout=wait)
        touch_activity()
        return job.describe()

    def do_cancel(job: Job, kill_background: bool) -> dict:
        if not job.done.is_set():
            manager.cancel(job)
            job.done.wait(timeout=KILL_GRACE_SECONDS + 10)
        swept = sweep_marked_processes() if kill_background else 0
        touch_activity()
        return {**job.describe(), "background_killed": swept}

    @app.post("/jobs/{job_id}/cancel", dependencies=[Depends(require_auth)])
    def cancel(job_id: str, request: Request, body: Optional[CancelRequest] = None):
        job = manager.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="unknown job")
        return do_cancel(job, bool(body and body.kill_background))

    @app.post("/cancel", dependencies=[Depends(require_auth)])
    def cancel_current(request: Request, body: Optional[CancelRequest] = None):
        """Kill switch: stop whatever is running, no job id needed. Idle agent => {"status": "idle"}."""
        kill_background = bool(body and body.kill_background)
        job = manager.running()
        if job is None:
            swept = sweep_marked_processes() if kill_background else 0
            touch_activity()
            return {"status": "idle", "job_id": None, "background_killed": swept}
        return do_cancel(job, kill_background)

    return app


def main() -> None:
    # The token arrives on stdin so it never sits in this process's environment
    # or argv (both are visible to other processes of the same user).
    token = sys.stdin.read().strip()
    if not token:
        sys.exit("agent: no session token supplied on stdin")
    workspace = os.environ.get("GITHUB_WORKSPACE") or os.getcwd()
    touch_activity()
    import uvicorn

    uvicorn.run(
        create_app(token, workspace), host="127.0.0.1", port=8000, log_level="warning"
    )


if __name__ == "__main__":
    main()
