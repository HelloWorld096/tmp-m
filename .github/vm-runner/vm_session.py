"""Session lifecycle inside the runner: restore on boot, save on demand/idle/shutdown, fencing against the Worker.

Pure stdlib (the agent's dependencies are hash-locked; do not add packages).

State machine (``SessionManager.state``)::

    restoring -> ready -> stopping -> stopped
        |          |
        +--> failed <--+            (failed never saves: an empty container must not replace a good snapshot)

``capturing`` is a flag on top of ``ready``; while it is set no job may start (the container is paused).
Nothing here talks to Docker or GitHub directly: ``container`` is a ``vm_container.SessionContainer`` and ``store`` a
``vm_snapshot.SnapshotStore``; both are injected, so the tests can use fakes.
"""
from __future__ import annotations

import calendar
import http.client
import json
import os
import re
import threading
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import vm_container as vc
import vm_snapshot as vs

SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
TOKEN_REFRESH_MARGIN_S = 300
MAX_REASON = 300

# Flag files shared with keepalive.sh (see the testament, section D).
FLAG_ACTIVE = "vm-session-active"     # written at boot when a session is attached
FLAG_STOPPING = "vm-stopping"          # keepalive asks for a final save
FLAG_HANDOFF = "vm-handoff"            # keepalive asks for a handoff: drain, deep save, the Worker starts a replacement
FLAG_SAVED = "vm-saved"                # the agent finished its final save attempt ("ok" or "failed")
FLAG_STOP_NOW = "vm-stop-now"          # the agent asks the run to end

STATES = ("restoring", "ready", "failed", "stopping", "stopped")


class SessionError(Exception):
    pass


class Unavailable(SessionError):
    """A job cannot start now. ``reason`` is the 503 ``error`` value: restoring|capturing|failed|stopping|stopped."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(detail or reason)
        self.reason, self.detail = reason, detail


class Busy(SessionError):
    """A save/shutdown was refused because a job is running."""


class WorkerError(vs.StoreError):
    """The Worker could not be reached or refused. Subclass of StoreError so the store tolerates it after commit."""


# --------------------------------------------------------------------------- the claim bundle
@dataclass
class Bundle:
    runner_token: str
    session_id: Optional[str] = None
    epoch: int = 0
    repo: str = ""
    image: str = vc.DEFAULT_IMAGE
    repo_token: str = ""
    repo_token_expires_at: float = 0.0       # epoch seconds
    fresh: bool = False
    deep: bool = False                       # apply the snapshot in deep mode (set by the Worker for a handoff)

    @property
    def has_session(self) -> bool:
        return self.session_id is not None

    def secrets(self) -> list:
        return [s for s in (self.runner_token, self.repo_token) if s]


def parse_iso(value) -> float:
    """'2026-10-22T10:00:00Z' -> epoch seconds. Unparseable -> 0 (treated as already expired, so it is refreshed)."""
    try:
        return float(calendar.timegm(time.strptime(str(value)[:19], "%Y-%m-%dT%H:%M:%S")))
    except Exception:
        return 0.0


def parse_bundle(raw: str) -> Bundle:
    """Accept the claim bundle (JSON) or a bare runner token (legacy host mode)."""
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("empty claim bundle")
    if not raw.startswith("{"):
        return Bundle(runner_token=raw)
    try:
        d = json.loads(raw)
    except ValueError as err:
        raise ValueError("claim bundle is not valid JSON") from err
    token = d.get("token")
    if not isinstance(token, str) or not token:
        raise ValueError("claim bundle has no token")
    s = d.get("session")
    if not s:
        return Bundle(runner_token=token)
    sid = str(s.get("id", ""))
    if not SESSION_ID_RE.match(sid):
        raise ValueError("claim bundle has an invalid session id")
    epoch = s.get("epoch")
    if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 1:
        raise ValueError("claim bundle has an invalid epoch")
    repo = str(s.get("repo", ""))
    if not REPO_RE.match(repo):
        raise ValueError("claim bundle has an invalid repository")
    repo_token = s.get("repo_token")
    if not isinstance(repo_token, str) or not repo_token:
        raise ValueError("claim bundle has no repository token")
    return Bundle(runner_token=token, session_id=sid, epoch=epoch, repo=repo,
                  image=vc.normalize_image(s.get("image")), repo_token=repo_token,
                  repo_token_expires_at=parse_iso(s.get("repo_token_expires_at")), fresh=bool(s.get("fresh")),
                  deep=bool(s.get("deep")))


# --------------------------------------------------------------------------- agent -> Worker
class WorkerApi:
    """Calls the Worker's /runner/session/* routes with the runner's bearer. http.client on purpose: no redirects are
    followed, so the bearer can never be forwarded to another host."""

    def __init__(self, base_url: str, runner_token: str, *, timeout: float = 10, retries: int = 3,
                 backoff: float = 1.0, sleep: Callable[[float], None] = time.sleep):
        u = urllib.parse.urlparse(base_url.rstrip("/"))
        if u.scheme != "https" and not (u.scheme == "http" and os.environ.get("VM_SNAPSHOT_TEST") == "1"):
            raise ValueError("VM_BASE_URL must be an https URL")
        if not u.hostname:
            raise ValueError("VM_BASE_URL has no host")
        self._u, self._token = u, runner_token
        self.timeout, self.retries, self.backoff, self._sleep = timeout, retries, backoff, sleep

    def _post(self, path: str, body: dict) -> tuple:
        data = json.dumps(body).encode()
        last = "no attempt"
        for attempt in range(self.retries):
            conn = None
            try:
                cls = http.client.HTTPSConnection if self._u.scheme == "https" else http.client.HTTPConnection
                conn = cls(self._u.hostname, self._u.port, timeout=self.timeout)
                conn.request("POST", (self._u.path or "") + path, body=data, headers={
                    "Authorization": f"Bearer {self._token}", "Content-Type": "application/json",
                    "Content-Length": str(len(data))})
                r = conn.getresponse()
                raw = r.read(1 << 20)
                if r.status in (429, 500, 502, 503, 504):
                    last = f"HTTP {r.status}"
                else:
                    try:
                        parsed = json.loads(raw or b"{}")
                    except ValueError:
                        parsed = {}
                    return r.status, parsed if isinstance(parsed, dict) else {}
            except (OSError, http.client.HTTPException) as err:
                last = type(err).__name__
            finally:
                if conn is not None:
                    conn.close()
            if attempt + 1 < self.retries:
                self._sleep(self.backoff * (2 ** attempt))
        raise WorkerError(f"the Worker did not answer ({last})")

    def repo_token(self) -> tuple:
        status, body = self._post("/runner/session/token", {})
        if status == 409:
            raise vs.Fenced("this runner was superseded by a newer start of the session")
        if status != 200 or not body.get("repo_token"):
            raise WorkerError(f"the Worker refused to issue a repository token (HTTP {status})")
        return str(body["repo_token"]), parse_iso(body.get("expires_at"))

    def event(self, type_: str, **fields) -> None:
        status, _ = self._post("/runner/session/event", {"type": type_, **fields})
        if status == 409:
            raise vs.Fenced("this runner was superseded by a newer start of the session")
        if status != 200:
            raise WorkerError(f"the Worker rejected the '{type_}' event (HTTP {status})")


# --------------------------------------------------------------------------- manager
class SessionManager:
    def __init__(self, bundle: Bundle, *, worker, container, store: Callable[[Callable[[], str]], "vs.SnapshotStore"],
                 flags_dir: Path = Path("/tmp"), kill_jobs: Callable[[], None] = lambda: None,
                 log: Callable[[str], None] = lambda m: None, clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] = time.sleep, autosave_idle_s: float = 60.0,
                 autosave_interval_s: float = 600.0, poll_s: float = 5.0, job_drain_s: float = 30.0,
                 handoff_drain_s: float = 600.0):
        if not bundle.has_session:
            raise ValueError("SessionManager needs a session bundle")
        self.b = bundle
        self.worker = worker
        self.container = container
        self.flags = Path(flags_dir)
        self._kill_jobs = kill_jobs
        self._log = log
        self.clock, self._sleep = clock, sleep
        self.autosave_idle_s, self.autosave_interval_s = autosave_idle_s, autosave_interval_s
        self.poll_s, self.job_drain_s = poll_s, job_drain_s
        self.handoff_drain_s = handoff_drain_s    # a running job may finish for this long before a handoff stops it
        self.handoff = False            # the shutdown under way is a handoff (jobs get 503 "handoff")

        self._cv = threading.Condition()
        self.state = "restoring"
        self.capturing = False          # True only while the container is held still for the capture
        self.uploading = False          # a captured snapshot is being uploaded (jobs may run meanwhile)
        self.captured_seq: Optional[int] = None   # newest snapshot captured locally (maybe not yet durable)
        self.upload_error: Optional[str] = None
        self._upload_thread: Optional[threading.Thread] = None
        self.history = None             # vm_history.HistoryStore, installed by the agent (set_history)
        self.history_note: Optional[str] = None
        self.deep_note: Optional[str] = None        # what the last deep apply found
        self.deep_drift: Optional[dict] = None
        self.last_save_deep = False
        self._gen = 0                   # bumped whenever the filesystem may have changed
        self.error: Optional[str] = None
        self.progress = "starting"
        self.last_save: Optional[dict] = None
        self.save_error: Optional[str] = None
        self.workdir = vc.DEFAULT_WORKDIR
        self._active_jobs = 0
        self._dirty = False
        self._last_activity = clock()
        self._last_save_at = clock()
        self._last_attempt = 0.0
        self._token, self._token_exp = bundle.repo_token, bundle.repo_token_expires_at
        self._known_secrets = set(bundle.secrets())     # every token ever seen, for scrubbing
        self._token_lock = threading.Lock()
        self._save_lock = threading.Lock()
        self._shutdown_done = threading.Event()
        self._stop_threads = threading.Event()
        self._threads: list = []
        self.store = store(self._repo_token)

    # ---- helpers ----------------------------------------------------------
    def log(self, msg: str) -> None:
        self._log(self.scrub(msg))

    def scrub(self, text: str) -> str:
        for s in sorted(self._known_secrets, key=len, reverse=True):
            text = text.replace(s, "***")
        return text

    def _short(self, err: BaseException) -> str:
        return self.scrub(str(err) or type(err).__name__).replace("\n", " ")[:MAX_REASON]

    def _flag(self, name: str, content: str = "") -> None:
        try:
            self.flags.mkdir(parents=True, exist_ok=True)
            (self.flags / name).write_text(content)
        except OSError as err:
            self.log(f"could not write flag {name}: {err}")

    def _set_progress(self, msg: str) -> None:
        self.progress = msg

    def _repo_token(self) -> str:
        """Token provider handed to the store. Refreshes through the Worker when less than 5 minutes remain; a failed
        refresh is tolerated while the current token is still valid."""
        with self._token_lock:
            now = self.clock()
            if self._token and self._token_exp - now > TOKEN_REFRESH_MARGIN_S:
                return self._token
            try:
                tok, exp = self.worker.repo_token()
                self._token, self._token_exp = tok, exp
                self._known_secrets.add(tok)
            except vs.Fenced:
                self._supersede()
                raise
            except Exception as err:
                if self._token and self._token_exp > now:
                    self.log(f"token refresh failed, using the current token: {err}")
                    return self._token
                raise vs.StoreError(f"cannot obtain a repository token: {self._short(err)}") from err
            return self._token

    def _supersede(self) -> None:
        with self._cv:
            if self.state not in ("stopped",):
                self.state = "failed"
                self.error = "this runner was superseded by a newer start of this session; it will not save"
                self._cv.notify_all()
        self._flag(FLAG_STOP_NOW, "superseded")

    # ---- start ------------------------------------------------------------
    def start(self) -> "SessionManager":
        self._flag(FLAG_ACTIVE, self.b.session_id or "")
        t = threading.Thread(target=self.boot, name="session-boot", daemon=True)
        t.start()
        self._threads.append(t)
        a = threading.Thread(target=self._loop, name="session-autosave", daemon=True)
        a.start()
        self._threads.append(a)
        return self

    def boot(self) -> None:
        restored = None
        try:
            self._set_progress("starting the container")
            self.container.create()
            self._set_progress("looking for a snapshot")
            restored = self.store.restore(progress=self._set_progress)
            if restored is None:
                self._set_progress("fresh session")
                self._notify("fresh")
            else:
                cfg = restored.manifest.get("config") or {}
                want = cfg.get("image")
                if want and want != self.b.image:
                    raise SessionError(f"the snapshot was taken on image {want}, this start uses {self.b.image}; "
                                       "a session's image is fixed at creation")
                wd = cfg.get("workdir")
                saved_wd = wd if isinstance(wd, str) and wd.startswith("/") else self.workdir
                self._set_progress("applying the snapshot")
                applied = self.container.apply(restored.chunk_paths)
                extras = (applied or {}).get("extras") or {}
                self._restore_history(extras)
                self._apply_deep(extras, bool(cfg.get("deep")))
                # the saved directory may no longer exist (deleted in the session); fall back to the default
                self.workdir = self.container.workdir = self.container.ensure_workdir(saved_wd)
                skipped = f" ({len(restored.skipped)} newer snapshot(s) were unusable)" if restored.skipped else ""
                self.last_save = {"seq": restored.manifest.get("seq"), "bytes": restored.manifest.get("bytes"),
                                  "at": restored.manifest.get("created_at")}
                self._notify("restored", seq=restored.manifest.get("seq"), bytes=restored.manifest.get("bytes"))
                self.log(f"restored snapshot {restored.manifest.get('seq')}{skipped}")
            with self._cv:
                if self.state == "restoring":
                    self.state = "ready"
                    self.progress = "ready"
                self._last_activity = self._last_save_at = self.clock()
                self._cv.notify_all()
            self._collect_garbage(int((restored.manifest.get("seq") if restored else 0) or 0))
        except BaseException as err:  # noqa: BLE001 - every failure must end in a visible failed state
            self._fail(err)
        finally:
            if restored is not None:
                restored.cleanup()

    def _apply_deep(self, extras: dict, snapshot_is_deep: bool) -> None:
        """Deep apply happens only when this start asked for it (bundle.deep). Never fatal: the filesystem is already back."""
        if not self.b.deep:
            if snapshot_is_deep:
                self.deep_note = "the snapshot is deep; applied as a normal restore (deep apply was not requested)"
            return
        if not snapshot_is_deep or "deep.json" not in extras:
            self.deep_note = "deep apply requested, but this snapshot is a normal one; restored the filesystem only"
            self.log(self.deep_note)
            return
        try:
            self._set_progress("deep apply: restoring volumes and verifying packages")
            res = self.container.apply_deep(extras)
            self.deep_drift = res.get("report")
            self.deep_note = f"deep apply: {res['volumes']} volume(s) restored; {res['summary']}"
        except Exception as err:   # noqa: BLE001
            self.deep_note = f"deep apply failed after the filesystem was restored: {self._short(err)}"
        self.log(self.deep_note)

    def _restore_history(self, extras: dict) -> None:
        raw = extras.get("history.sqlite")
        if self.history is None or raw is None:
            return
        try:
            n = self.history.import_bytes(raw)
            self.history_note = f"restored {n} history entr{'y' if n == 1 else 'ies'}"
        except Exception as err:   # noqa: BLE001 - losing the history must not stop the session from starting
            self.history_note = f"history in the snapshot was not restored: {self._short(err)}"
        self.log(self.history_note)

    def _collect_garbage(self, keep_seq: int) -> None:
        """A run that was killed mid-upload leaves a draft release behind (possibly many GB). This start has the newest
        epoch, so older-epoch drafts can never be committed any more: remove them now instead of waiting for the next
        successful save. Best effort; it never touches a committed snapshot at or above ``keep_seq``."""
        if self.state != "ready":
            return
        try:
            removed = self.store.gc(keep_seq=keep_seq, epoch=self.b.epoch)
            if removed:
                self.log(f"removed {removed} obsolete snapshot asset group(s) left by an earlier run")
        except Exception as err:  # noqa: BLE001
            self.log(f"cleanup of obsolete snapshots failed (will retry after the next save): {self._short(err)}")

    def _fail(self, err: BaseException) -> None:
        reason = self._short(err)
        self.log(f"session start failed: {reason}")
        with self._cv:
            if self.state in ("stopping", "stopped"):
                self.error = self.error or reason      # a shutdown is already under way; do not undo it
                return
            self.state = "failed"
            self.error = reason
            self.progress = "failed"
            self._cv.notify_all()
        self._notify("failed", reason=reason)

    def _notify(self, type_: str, **fields) -> None:
        try:
            self.worker.event(type_, **fields)
        except vs.Fenced:
            self._supersede()
        except Exception as err:  # the Worker learns the state from /status as well
            self.log(f"could not report '{type_}' to the Worker: {self._short(err)}")

    def _reason(self) -> str:
        """The 503 ``error`` value for a job refused now (call with ``_cv`` held)."""
        return "handoff" if self.handoff and self.state == "stopping" else self.state

    # ---- jobs -------------------------------------------------------------
    def acquire_job(self) -> None:
        """Called by the agent before a job starts. Raises Unavailable (-> HTTP 503)."""
        with self._cv:
            if self.state != "ready":
                raise Unavailable(self._reason(), self.error or self.progress)
            if self.capturing:
                raise Unavailable("capturing", "a snapshot is being saved, retry in a moment")
            self._active_jobs += 1
            self._last_activity = self.clock()
            self._dirty = True
            self._gen += 1

    def release_job(self, cwd: Optional[str] = None) -> None:
        with self._cv:
            self._active_jobs = max(0, self._active_jobs - 1)
            self._last_activity = self.clock()
            self._dirty = True
            self._gen += 1
            if cwd and cwd.startswith("/"):
                self.workdir = cwd
            self._cv.notify_all()

    def gate(self) -> Optional[Unavailable]:
        with self._cv:
            if self.state != "ready":
                return Unavailable(self._reason(), self.error or self.progress)
            if self.capturing:
                return Unavailable("capturing", "a snapshot is being saved, retry in a moment")
        return None

    # ---- save -------------------------------------------------------------
    def save(self, reason: str = "manual", background: bool = False, deep: bool = False) -> dict:
        """Capture, upload, verify and commit. The previous snapshot survives any failure. Raises Busy, Unavailable,
        or a StoreError subclass with a message that is safe to show.

        The container is held still only while the snapshot is captured to local scratch space (``capturing``). With
        ``background=True`` the call returns as soon as that is done (``{"status": "uploading", ...}``) and the upload
        continues on its own thread (``uploading``); jobs may start again immediately. Only a snapshot that was uploaded,
        verified and committed counts as saved: ``last_save`` / ``durable_seq`` move then and not before, and a failed
        upload leaves the session dirty (``upload_error`` says why). One save/upload at a time."""
        final = reason == "shutdown"
        with self._cv:
            if self.state == "failed":
                raise Unavailable("failed", self.error or "the session failed to start; saving is disabled")
            ok_states = ("ready", "stopping") if final else ("ready",)
            if self.state not in ok_states:
                raise Unavailable(self.state, self.progress)
            if self._active_jobs:
                raise Busy("a job is running; wait for it or stop it first")
            if not self._save_lock.acquire(blocking=False):
                raise Busy("a snapshot upload is still in progress" if self.uploading else "a save is already in progress")
            self.capturing = True
            gen = self._gen
        pending = None
        try:
            self._last_attempt = self.clock()
            cfg = {"image": self.b.image, "workdir": self.workdir}
            deep_extras: dict = {}
            if deep:
                self._set_progress("collecting deep data (package manifests, volumes)")
                deep_extras = self.container.collect_deep()      # before the pause: docker exec needs a running container
                cfg["deep"] = True
            pending = self.store.capture_local(
                epoch=self.b.epoch,
                capture=lambda w: self.container.capture(w, extras=lambda: {**self._extras(), **deep_extras}),
                config=cfg, progress=self._set_progress)
            deep_extras = {}
        except vs.Fenced:
            self._capture_ended(failed=True)
            self._supersede()
            raise
        except BaseException as err:
            self._capture_ended(failed=True)
            self.save_error = self._short(err)
            self.progress = "save failed"
            self.log(f"save failed ({reason}): {self.save_error}; the previous snapshot is kept")
            raise
        with self._cv:                  # the container is released: jobs may run while the upload goes on
            self.capturing = False
            self.uploading = True
            self.captured_seq = pending.seq
            self.upload_error = None
            self._cv.notify_all()
        if background:
            t = threading.Thread(target=self._upload_quietly, args=(pending, reason, gen), name="session-upload", daemon=True)
            self._upload_thread = t
            self._threads.append(t)
            t.start()
            return {"status": "uploading", "seq": pending.seq, "bytes": pending.total}
        return self._finish_upload(pending, reason, gen)

    def _capture_ended(self, failed: bool) -> None:
        with self._cv:
            self.capturing = False
            self._cv.notify_all()
        if failed:
            self._save_lock.release()

    def _upload_quietly(self, pending, reason: str, gen: int) -> None:
        try:
            self._finish_upload(pending, reason, gen)
        except BaseException:  # noqa: BLE001 - recorded in save_error / upload_error by _finish_upload; nobody to raise to
            pass

    def _finish_upload(self, pending, reason: str, gen: int) -> dict:
        try:
            summary = self.store.upload(pending, fence=self._fence, progress=self._set_progress)
            with self._cv:
                self.last_save = {"seq": summary["seq"], "bytes": summary["bytes"], "at": summary.get("created_at")}
                self.save_error = self.upload_error = None
                self.last_save_deep = bool(pending.config.get("deep"))
                if self._gen == gen:
                    self._dirty = False          # nothing changed since the capture; otherwise it stays dirty
                self._last_save_at = self.clock()
            self.progress = "saved"
            self.log(f"saved snapshot {summary['seq']} ({reason})")
            return summary
        except vs.Fenced:
            self.upload_error = "superseded by a newer start"
            self._supersede()
            raise
        except BaseException as err:
            self.save_error = self.upload_error = self._short(err)
            self.progress = "save failed"
            self.log(f"upload failed ({reason}): {self.save_error}; the previous snapshot is kept, the session stays unsaved")
            raise
        finally:
            pending.discard()
            with self._cv:
                self.uploading = False
                self._cv.notify_all()
            self._save_lock.release()

    def wait_upload(self, timeout: Optional[float] = None) -> bool:
        """Wait for a background upload to end. True when none is running (any more)."""
        t = self._upload_thread
        if t is not None and t.is_alive():
            t.join(timeout)
            return not t.is_alive()
        return True

    def _fence(self, kind: str, info: dict) -> None:
        if kind == "commit":
            try:
                self.worker.event("commit", **info)
            except vs.Fenced:
                raise
            except Exception as err:   # fail closed: without the Worker's permission nothing is committed
                raise vs.StoreError(f"the Worker could not confirm this save: {self._short(err)}") from err
        else:
            self.worker.event(kind, **info)

    # ---- shutdown ---------------------------------------------------------
    def shutdown(self, force: bool = False, handoff: bool = False) -> bool:
        """Start the graceful shutdown (idempotent). Returns True when this call started it. Raises Busy when a job is
        running and force is False.

        ``handoff=True`` is the planned hand-over to a replacement VM (keepalive's vm-handoff flag): new jobs get 503
        ``handoff``; a running job may finish for up to ``handoff_drain_s`` before it is stopped (``force``); the final
        save is a *deep* one; the Worker is told with a ``handoff`` event first and ``stopped(handoff=true, saved=...)``
        last, and the usual vm-saved / vm-stop-now flags end the run."""
        with self._cv:
            if self.state in ("stopping", "stopped"):
                return False
            if self._active_jobs and not force and not handoff:
                raise Busy("a job is running; pass force to stop it")
            previous = self.state
            self.state = "stopping"
            self.handoff = handoff
            self.progress = "handing over to a new VM" if handoff else "shutting down"
        if handoff:
            self._notify("handoff", phase="draining" if self._active_jobs else "saving")
        t = threading.Thread(target=self._shutdown, args=(previous, force, handoff), name="session-shutdown", daemon=True)
        t.start()
        self._threads.append(t)
        return True

    def _shutdown(self, previous: str, force: bool, handoff: bool = False) -> None:
        saved, note = False, None
        try:
            if handoff and self._active_jobs:
                self.log(f"handoff: waiting up to {self.handoff_drain_s:.0f}s for the running job to finish")
                end = self.clock() + self.handoff_drain_s
                with self._cv:
                    while self._active_jobs and self.clock() < end:
                        self._cv.wait(0.2)
            if self._active_jobs and (force or handoff):
                self.log("stopping running jobs before the final save")
                self._kill_jobs()
                end = self.clock() + self.job_drain_s
                with self._cv:
                    while self._active_jobs and self.clock() < end:
                        self._cv.wait(0.2)
            if previous == "ready":
                self.wait_upload()          # a background upload in flight finishes (or fails) before we judge what is saved
                if not self._dirty and self.last_save and (not handoff or self.last_save_deep):
                    saved, note = True, "nothing changed since the last snapshot"
                else:
                    if handoff:
                        self._notify("handoff", phase="saving")
                    for attempt in (1, 2):
                        try:
                            self.save("shutdown", deep=handoff)
                            saved = True
                            break
                        except vs.Fenced:
                            note = "superseded by a newer start; not saved"
                            break
                        except Exception as err:
                            note = self._short(err)
                            if attempt == 1:
                                self._sleep(2)
            elif previous == "failed":
                note = "the session never started correctly; nothing was saved and the last snapshot is untouched"
            else:
                note = f"was {previous}; nothing was saved"
        except BaseException as err:  # noqa: BLE001
            note = self._short(err)
        finally:
            try:
                self.container.remove()
            except Exception as err:
                self.log(f"could not remove the container: {self._short(err)}")
            with self._cv:
                self.state = "stopped"
                self.progress = "stopped"
                if note and not saved:
                    self.error = note
                self._cv.notify_all()
            self._notify("stopped", saved=saved, **({"handoff": True} if handoff else {}),
                         **({"reason": note} if note else {}))
            self._flag(FLAG_SAVED, "ok" if saved else "failed")
            self._flag(FLAG_STOP_NOW, "stopped")
            self._shutdown_done.set()

    def wait_stopped(self, timeout: Optional[float] = None) -> bool:
        return self._shutdown_done.wait(timeout)

    # ---- background loop --------------------------------------------------
    def _loop(self) -> None:
        while not self._stop_threads.wait(self.poll_s):
            try:
                self.tick()
            except Exception as err:  # never let the loop die
                self.log(f"session loop error: {self._short(err)}")

    def tick(self) -> None:
        """One iteration of the background loop (public so tests can drive it without threads)."""
        if (self.flags / FLAG_HANDOFF).exists() and self.state in ("ready", "failed"):
            try:      # a failed session has nothing to hand over: it just stops (and saves nothing)
                self.shutdown(force=True, handoff=self.state == "ready")
            except Busy:
                pass
            return
        if (self.flags / FLAG_STOPPING).exists() and self.state in ("ready", "failed"):
            try:
                self.shutdown(force=True)
            except Busy:
                pass
            return
        now = self.clock()
        # Deliberately disable periodic idle autosaves. Snapshots are made
        # only on explicit save requests or during graceful session shutdown.
        due = False
        if due:
            try:
                self.save("autosave")
            except (Busy, Unavailable):
                pass
            except Exception:
                pass          # already logged and recorded in save_error; retried after the interval

    def set_history(self, history) -> None:
        """The agent installs the host-side command history; it travels inside every snapshot."""
        self.history = history

    def _extras(self) -> dict:
        """Host-side side data written into a snapshot (called while the container is paused)."""
        out: dict = {}
        if self.history is not None:
            out["history.sqlite"] = self.history.export_bytes()
        return out

    def set_kill_jobs(self, fn: Callable[[], None]) -> None:
        """The agent installs the callback that stops running jobs (it owns the JobManager)."""
        self._kill_jobs = fn

    def close(self) -> None:
        self._stop_threads.set()

    # ---- status -----------------------------------------------------------
    def status(self) -> dict:
        with self._cv:
            return {"state": self.state, "handoff": self.handoff, "capturing": self.capturing, "uploading": self.uploading,
                    "captured_seq": self.captured_seq, "durable_seq": (self.last_save or {}).get("seq"),
                    "upload_error": self.upload_error, "history": self.history_note,
                    "deep": {"last_save_deep": self.last_save_deep, "note": self.deep_note,
                             "drift": self.deep_drift and {"clean": self.deep_drift.get("clean"),
                                                           "sections": self.deep_drift.get("sections"),
                                                           "tool_missing": self.deep_drift.get("tool_missing")}},
                    "id": self.b.session_id, "epoch": self.b.epoch,
                    "image": self.b.image, "workdir": self.workdir, "last_save": self.last_save,
                    "progress": self.progress, "error": self.error, "save_error": self.save_error,
                    "dirty": self._dirty, "active_jobs": self._active_jobs}


# --------------------------------------------------------------------------- relay transfers with a container
def relay_script(container, mode: str, relay: str, repo: str, path: str, token_file: str, jobs_dir: str,
                 cwd: Optional[str] = None) -> str:
    """Shell command (run on the HOST, not in the container) for a Relay transfer that ends/starts inside the session
    container. relay.sh itself is hash-pinned and unchanged; it only ever sees a host staging directory.

    recv: relay.sh recv -> stage dir -> `docker cp` into the container.   put: `docker cp` out -> relay.sh put.
    The staging directory is removed on every exit path.
    """
    import shlex
    q = shlex.quote
    if mode not in ("recv", "put"):
        raise ValueError("mode must be recv or put")
    if not path.startswith("/"):
        path = (cwd or container.workdir).rstrip("/") + "/" + path
    d, name = q(container.docker.bin), q(container.name)
    lines = [
        f"( export RELAY_TOKEN_FILE={q(token_file)}",
        f"stage=$(mktemp -d -- {q(jobs_dir.rstrip('/') + '/relay-stage-')}XXXXXX) || exit 1",
        f"trap 'rm -rf -- \"$stage\"; rm -f -- '{q(token_file)} EXIT",
        "trap 'exit 143' TERM INT",
    ]
    if mode == "recv":
        lines += [
            f"{q(relay)} recv {q(repo)} \"$stage/\" || exit $?",
            "set -- \"$stage\"/*",
            "if [ \"$#\" -ne 1 ] || [ ! -f \"$1\" ]; then echo 'relay: expected exactly one received file' >&2; exit 1; fi",
            "src=$1",
            f"dest={q(path)}",
            "case \"$dest\" in",
            "  */) dir=${dest%/}; [ -n \"$dir\" ] || dir=/; target=\"$dir/\"; check=\"$dir/$(basename -- \"$src\")\" ;;",
            "  *) dir=$(dirname -- \"$dest\"); target=$dest; check=$dest ;;",
            "esac",
            f"{d} exec {name} mkdir -p \"$dir\" || exit 1",
            f"{d} cp \"$src\" {name}:\"$target\" || exit 1",
            f"{d} exec {name} test -e \"$check\" || {{ echo 'relay: the file did not arrive in the container' >&2; exit 1; }}",
            "echo \"RESULT: OK (copied into the session container: $check)\"",
        ]
    else:
        lines += [
            f"src={q(path)}",
            f"{d} exec {name} test -f \"$src\" || {{ echo \"relay: $src is not a regular file in the session\" >&2; exit 1; }}",
            f"{d} cp {name}:\"$src\" \"$stage/\" || exit 1",
            "f=\"$stage/$(basename -- \"$src\")\"",
            "[ -f \"$f\" ] || { echo 'relay: the copy out of the container failed' >&2; exit 1; }",
            f"{q(relay)} put {q(repo)} \"$f\"",
        ]
    lines.append(")")
    return "\n".join(lines)
