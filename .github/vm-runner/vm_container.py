"""A session's container: one long-lived Docker container per session, jobs run in it with `docker exec`.

What is saved is the FILESYSTEM STATE, never the image: `docker diff` lists what changed relative to the image, and only
those entries are copied out of `docker export` into the snapshot (plus a list of deleted paths). On restore the image is
pulled again, a fresh container is created from it, and the saved changes are applied on top. This is independent of the
storage driver and needs nothing inside the image except what the image already has.

Only the Docker CLI is used (the runner user can talk to the daemon on GitHub-hosted runners). The CLI path is
overridable (VM_DOCKER) so tests can run against a fake.
"""
from __future__ import annotations

import io
import json
import os
import re
import shlex
import subprocess
import tarfile
import threading
from pathlib import Path
from typing import Callable, Iterable, Optional

from vm_snapshot import ChunkReader, ChunkWriter, StoreError

DEFAULT_IMAGE = "ubuntu:24.04"
DEFAULT_WORKDIR = "/workspace"
META_MEMBER = ".vm-snapshot/deleted.json"

# docker image reference: [registry[:port]/]path[:tag][@sha256:digest]. Strict on purpose: the value ends up in argv.
IMAGE_RE = re.compile(
    r"^(?:[a-z0-9](?:[a-z0-9._-]{0,253}[a-z0-9])?(?::[0-9]{1,5})?/)?"
    r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?(?:@sha256:[0-9a-f]{64})?$")

# Paths Docker manages itself or that only make sense for one container instance.
# docker-init: the empty mountpoint Docker creates for `--init` (usr/sbin on Debian-likes, sbin on Alpine); it is in the
# `docker diff` of every pristine container (seen on docker 28), so storing it would only add a pointless restore step.
EXCLUDE_EXACT = {"/etc/hosts", "/etc/hostname", "/etc/resolv.conf", "/etc/mtab", "/.dockerenv",
                 "/usr/sbin/docker-init", "/sbin/docker-init"}
EXCLUDE_PREFIXES = ("/proc", "/sys", "/dev", "/tmp/vm-jobs")


def normalize_image(value: Optional[str]) -> str:
    """Empty/None/'ubuntu' mean the default image. Anything else must be a well-formed reference."""
    v = (value or "").strip()
    if not v or v.lower() == "ubuntu":
        return DEFAULT_IMAGE
    if len(v) > 300 or not IMAGE_RE.match(v):
        raise ValueError(f"not a valid Docker image reference: {v[:80]!r}")
    return v


def excluded(path: str) -> bool:
    if path in EXCLUDE_EXACT:
        return True
    return any(path == p or path.startswith(p + "/") for p in EXCLUDE_PREFIXES)


def parse_diff(text: str) -> tuple[set, list]:
    """`docker diff` output -> (paths added or changed, paths deleted). Excluded and malformed lines are dropped."""
    changed: set = set()
    deleted: list = []
    for line in text.splitlines():
        if len(line) < 3 or line[0] not in "ACD" or line[1] != " ":
            continue
        path = line[2:]
        if not path.startswith("/") or excluded(path):
            continue
        if line[0] == "D":
            deleted.append(path)
        else:
            changed.add(path)
    return changed, deleted


# Kills every process of a job inside the container. Docker does not forward signals sent to `docker exec`, so the
# job's processes are found by the marker variable every job carries in its environment (the same trick the agent uses
# on the host).  $1 = marker prefix ("VM_AGENT_JOB=<id>" or "VM_AGENT_JOB="), $2 = signal.
SWEEP_SCRIPT = (
    'me=$$; for p in /proc/[0-9]*; do pid=${p#/proc/}; [ "$pid" = "$me" ] && continue; '
    '[ -r "$p/environ" ] || continue; '
    'if tr "\\0" "\\n" < "$p/environ" 2>/dev/null | grep -q "^$1"; then kill -"$2" "$pid" 2>/dev/null; fi; done; exit 0'
)


class DockerError(StoreError):
    pass


class Docker:
    def __init__(self, binary: Optional[str] = None, runner: Callable = subprocess.run):
        self.bin = binary or os.environ.get("VM_DOCKER", "docker")
        self._run = runner

    def run(self, *args: str, check: bool = True, timeout: Optional[float] = 600, input: Optional[bytes] = None):
        try:
            p = self._run([self.bin, *args], capture_output=True, timeout=timeout, input=input)
        except FileNotFoundError as err:
            raise DockerError("Docker is not available on this runner") from err
        except subprocess.TimeoutExpired as err:
            raise DockerError(f"docker {args[0]} timed out") from err
        if check and p.returncode != 0:
            tail = (p.stderr or b"").decode("utf-8", "replace").strip().splitlines()[-1:] or [""]
            raise DockerError(f"docker {args[0]} failed: {tail[0][:300]}")
        return p

    def popen(self, *args: str, **kw) -> subprocess.Popen:
        return subprocess.Popen([self.bin, *args], **kw)


class SessionContainer:
    def __init__(self, session_id: str, image: str, docker: Docker, *, jobs_dir: Path, workdir: str = DEFAULT_WORKDIR,
                 log: Callable[[str], None] = lambda m: None):
        self.session_id = session_id
        self.image = image
        self.docker = docker
        self.jobs_dir = jobs_dir
        self.workdir = workdir
        self.name = "vm-session-" + re.sub(r"[^a-z0-9]", "", session_id.lower())[:24]
        self.log = log
        self.shell = "bash"
        self.user: Optional[str] = None
        self._lock = threading.Lock()

    # ---- lifecycle --------------------------------------------------------
    def create(self) -> None:
        """Pull the image and start a pristine container (replacing any leftover with the same name)."""
        self.log(f"pulling {self.image}")
        pulled = self.docker.run("pull", self.image, check=False, timeout=1800)
        if pulled.returncode != 0 and self.docker.run("image", "inspect", self.image, check=False).returncode != 0:
            tail = (pulled.stderr or b"").decode("utf-8", "replace").strip().splitlines()[-1:] or [""]
            raise DockerError(f"could not pull image {self.image}: {tail[0][:200]}")
        self.docker.run("rm", "-f", "-v", self.name, check=False)    # -v: also drop its anonymous volumes
        self.docker.run(
            "run", "-d", "--name", self.name, "--label", f"vm.session={self.session_id}", "--init",
            "-v", f"{self.jobs_dir}:{self.jobs_dir}", "-w", self.workdir, "--entrypoint", "sh",
            self.image, "-c", "while :; do sleep 3600; done", timeout=300)
        probe = self.docker.run("exec", self.name, "sh", "-c", "command -v bash >/dev/null 2>&1 && echo bash || echo sh",
                                check=False, timeout=60)
        self.shell = "bash" if b"bash" in (probe.stdout or b"") else "sh"

    def remove(self) -> None:
        self.docker.run("rm", "-f", "-v", self.name, check=False, timeout=120)   # -v: image VOLUMEs would leak

    # ---- running jobs -----------------------------------------------------
    def exec_argv(self, script: str, job_id: str, cwd: Optional[str] = None) -> list:
        argv = [self.docker.bin, "exec", "-w", cwd or self.workdir, "-e", f"VM_AGENT_JOB={job_id}",
                "-e", "TERM=dumb", "-e", "DEBIAN_FRONTEND=noninteractive"]
        if self.user:
            argv += ["-u", self.user]
        argv += [self.name, self.shell, "-lc" if self.shell == "bash" else "-c", script]
        return argv

    def kill_job(self, job_id: str, grace: float = 3.0) -> None:
        self._sweep(f"VM_AGENT_JOB={job_id}", "TERM")
        threading.Event().wait(grace)
        self._sweep(f"VM_AGENT_JOB={job_id}", "KILL")

    def kill_all_jobs(self) -> None:
        self._sweep("VM_AGENT_JOB=", "KILL")

    def _sweep(self, marker: str, sig: str) -> None:
        self.docker.run("exec", self.name, "sh", "-c", SWEEP_SCRIPT, "sweep", marker, sig, check=False, timeout=60)

    # ---- snapshot: capture ------------------------------------------------
    def capture(self, sink: ChunkWriter) -> dict:
        """Write the container's changes into `sink` as tar+gzip. The container is paused for the duration, so the
        copy is crash-consistent; it is always resumed, whatever happens."""
        with self._lock:
            self.docker.run("pause", self.name, check=False)
            try:
                diff = self.docker.run("diff", self.name, timeout=300).stdout.decode("utf-8", "replace")
                changed, deleted = parse_diff(diff)
                exp = self.docker.popen("export", self.name, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                assert exp.stdout is not None
                try:
                    kept = self._filter_export(exp.stdout, sink, changed, deleted)
                except BaseException:
                    sink.abort()
                    exp.kill()
                    raise
                finally:
                    exp.stdout.close()
                    rc = exp.wait()
                if rc != 0:
                    raise DockerError(f"docker export failed (exit {rc})")
                return kept
            finally:
                self.docker.run("unpause", self.name, check=False)

    @staticmethod
    def _filter_export(stream, sink: ChunkWriter, changed: set, deleted: list) -> dict:
        src = tarfile.open(fileobj=stream, mode="r|")
        dst = tarfile.open(fileobj=sink, mode="w|gz", compresslevel=1, format=tarfile.PAX_FORMAT)
        n = 0
        for m in src:
            if "/" + m.name.strip("/") not in changed:
                continue
            dst.addfile(m, src.extractfile(m) if m.isreg() else None)
            n += 1
        meta = json.dumps({"deleted": deleted}, sort_keys=True).encode()
        ti = tarfile.TarInfo(META_MEMBER)
        ti.size = len(meta)
        ti.mtime = 0
        dst.addfile(ti, io.BytesIO(meta))
        dst.close()
        return {"entries": n, "deleted": len(deleted)}

    # ---- snapshot: restore ------------------------------------------------
    def apply(self, chunk_paths: Iterable) -> dict:
        """Apply a verified snapshot onto a freshly created container."""
        reader = ChunkReader(list(chunk_paths))
        src = tarfile.open(fileobj=reader, mode="r|gz")
        cp = self.docker.popen("cp", "-a", "-", f"{self.name}:/", stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        assert cp.stdin is not None
        deleted: list = []
        n = 0
        try:
            dst = tarfile.open(fileobj=cp.stdin, mode="w|", format=tarfile.PAX_FORMAT)
            for m in src:
                if m.name == META_MEMBER:
                    f = src.extractfile(m)
                    data = json.loads(f.read().decode("utf-8")) if f else {}
                    deleted = [d for d in data.get("deleted", []) if isinstance(d, str) and d.startswith("/")]
                    continue
                dst.addfile(m, src.extractfile(m) if m.isreg() else None)
                n += 1
            dst.close()
        except BrokenPipeError:
            pass
        finally:
            try:
                cp.stdin.close()
            except OSError:
                pass
            reader.close()
        err = (cp.stderr.read() if cp.stderr else b"").decode("utf-8", "replace").strip()
        if cp.wait() != 0:
            raise DockerError(f"docker cp failed: {err[-300:]}")
        for i in range(0, len(deleted), 50):
            self.docker.run("exec", self.name, "rm", "-rf", "--", *deleted[i:i + 50], timeout=120)
        return {"entries": n, "deleted": len(deleted)}

    def ensure_workdir(self, path: str) -> str:
        """The directory a restored session should resume in, or the default if it no longer exists."""
        p = self.docker.run("exec", self.name, "test", "-d", path, check=False, timeout=30)
        return path if p.returncode == 0 else self.workdir


def shell_quote(s: str) -> str:
    return shlex.quote(s)
