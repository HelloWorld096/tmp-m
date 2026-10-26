"""Live view: the agent pushes what it is doing to the Worker, about once a second, while someone is watching.

Why a socket and not polling: a poll is one Worker request per update. Here the agent opens ONE outbound WebSocket to the
Worker's LiveHub (a Durable Object) when the Worker asks it to (POST /live/start, sent when a viewer connects), streams
frames over it, and closes it when the hub says nobody is watching any more. Nothing is sent without a viewer.

The WebSocket client is written with the standard library (RFC 6455, client side only) so the agent needs no new
dependency: the agent's requirements are hash-pinned and installed on every VM start.

What a frame carries (JSON text, at most one per second, only when something changed, else a heartbeat every 5 s):
  vm    the VM's own state: session state, paused, uploading, structured activity, running counters
  rows  history rows that are new or changed since the last frame (the same shape as GET /history)
  out   new output of running jobs since the last frame: {job_id: {text, offset}}
The hub answers with {"type":"subs","n":<viewers>}; with n == 0 for IDLE_GRACE_S the agent closes the stream.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import select
import socket
import ssl
import struct
import threading
import time
import urllib.parse
from typing import Callable, Optional

import vm_history

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_MESSAGE = 1 << 20
INTERVAL_S = 1.0
HEARTBEAT_S = 5.0
IDLE_GRACE_S = 10.0
MAX_DELTA = 8_192          # output bytes per running job per frame
FIRST_BACKFILL = 4_096     # a job already running when the stream starts: show its last 4 KB, the rest is one click away
MAX_FRAME_ROWS = 40
LIVE_PATH = "/runner/live"


class WsError(Exception):
    pass


def encode_frame(opcode: int, payload: bytes = b"", mask: bool = True) -> bytes:
    head = bytearray([0x80 | opcode])
    n = len(payload)
    flag = 0x80 if mask else 0
    if n < 126:
        head.append(flag | n)
    elif n < 1 << 16:
        head.append(flag | 126); head += struct.pack("!H", n)
    else:
        head.append(flag | 127); head += struct.pack("!Q", n)
    if not mask:
        return bytes(head) + payload
    key = secrets.token_bytes(4)
    masked = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
    return bytes(head) + key + masked


class FrameReader:
    """Incremental decoder: feed bytes, get whole messages as (opcode, payload). Handles fragmentation and masks."""

    def __init__(self, max_message: int = MAX_MESSAGE):
        self.buf = bytearray()
        self.max = max_message
        self._frag_op: Optional[int] = None
        self._frag = bytearray()

    def feed(self, data: bytes) -> list:
        self.buf += data
        out = []
        while True:
            b = self.buf
            if len(b) < 2:
                break
            fin, op = bool(b[0] & 0x80), b[0] & 0x0F
            masked, n = bool(b[1] & 0x80), b[1] & 0x7F
            pos = 2
            if n == 126:
                if len(b) < 4: break
                n, pos = struct.unpack("!H", bytes(b[2:4]))[0], 4
            elif n == 127:
                if len(b) < 10: break
                n, pos = struct.unpack("!Q", bytes(b[2:10]))[0], 10
            if n > self.max:
                raise WsError("message too large")
            key = b""
            if masked:
                if len(b) < pos + 4: break
                key, pos = bytes(b[pos:pos + 4]), pos + 4
            if len(b) < pos + n:
                break
            payload = bytes(b[pos:pos + n])
            del self.buf[:pos + n]
            if masked:
                payload = bytes(c ^ key[i % 4] for i, c in enumerate(payload))
            if op >= 8:                                   # control frames are never fragmented
                out.append((op, payload))
            elif op == 0:                                 # continuation
                if self._frag_op is None:
                    raise WsError("unexpected continuation")
                self._frag += payload
                if len(self._frag) > self.max:
                    raise WsError("message too large")
                if fin:
                    out.append((self._frag_op, bytes(self._frag))); self._frag_op, self._frag = None, bytearray()
            else:
                if fin:
                    out.append((op, payload))
                else:
                    self._frag_op, self._frag = op, bytearray(payload)
        return out


def _accept(key: str) -> str:
    return base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()


class LiveFeed:
    """Builds the frames. Remembers what it has already sent so each frame is only what changed."""

    def __init__(self, manager, history, session=None, clock: Callable[[], float] = time.time):
        self.manager, self.history, self.session, self.clock = manager, history, session, clock
        self.seq = 0
        self._rows: dict = {}          # job_id -> (status, exit_code, ended_at) as last sent
        self._offsets: dict = {}       # job_id -> output byte offset already sent

    def _vm(self) -> dict:
        vm: dict = {"busy": self.manager.running() is not None, "dash_running": len(self.manager.running_dash())}
        if self.session is not None:
            st = self.session.status()
            keep = ("state", "handoff", "paused", "capturing", "uploading", "activity", "progress", "durable_seq", "captured_seq",
                    "dirty", "active_jobs", "error", "save_error", "upload_error", "last_save", "id", "workdir")
            vm["session"] = {k: st.get(k) for k in keep}
        return vm

    def _rows_changed(self) -> list:
        rows, _ = self.history.page(limit=MAX_FRAME_ROWS)
        changed = []
        for r in rows:
            sig = (r["status"], r["exit_code"], r["ended_at"])
            if self._rows.get(r["job_id"]) != sig:
                self._rows[r["job_id"]] = sig
                changed.append(vm_history.list_view(r))
        if len(self._rows) > 400:                                   # forget the oldest signatures
            for k in list(self._rows)[:200]:
                self._rows.pop(k, None)
        return list(reversed(changed))                              # oldest first, so a viewer appends in order

    def _output(self) -> dict:
        out = {}
        running = {j.id: j for j in self.manager.all_running()}
        # jobs that were running at the last frame and ended since: one last read, then forget them
        for jid in [j for j in self._offsets if j not in running]:
            job = self.manager.get(jid)
            if job is not None:
                self._read(job, out)
            self._offsets.pop(jid, None)
        for job in running.values():
            self._read(job, out)
        return out

    def _read(self, job, out: dict) -> None:
        try:
            size = job.output_path.stat().st_size
        except OSError:
            return
        known = job.id in self._offsets
        off = self._offsets.get(job.id, max(0, size - FIRST_BACKFILL))
        if size <= off and known:
            return
        with open(job.output_path, "rb") as fh:
            fh.seek(off)
            raw = fh.read(MAX_DELTA)
        end = off + len(raw)
        if end < size or not job.done.is_set():                     # keep a half-written UTF-8 character for the next frame
            for back in range(min(3, len(raw))):
                try:
                    raw[:len(raw) - back].decode("utf-8")
                    raw, end = raw[:len(raw) - back], end - back
                    break
                except UnicodeDecodeError:
                    continue
        self._offsets[job.id] = end
        if raw:
            out[job.id] = {"text": raw.decode("utf-8", errors="replace"), "offset": end}

    def frame(self) -> dict:
        self.seq += 1
        return {"type": "frame", "seq": self.seq, "t": self.clock(), "vm": self._vm(),
                "rows": self._rows_changed(), "out": self._output()}


class LiveStreamer:
    def __init__(self, base_url: str, token: str, feed: LiveFeed, *, interval: float = INTERVAL_S, heartbeat: float = HEARTBEAT_S,
                 idle_grace: float = IDLE_GRACE_S, log: Callable[[str], None] = lambda m: None, allow_http: bool = False):
        u = urllib.parse.urlparse(base_url.rstrip("/"))
        if u.scheme != "https" and not (u.scheme == "http" and allow_http):
            raise ValueError("the live stream needs an https Worker address")
        if not u.hostname:
            raise ValueError("the Worker address has no host")
        self._u, self._token, self.feed = u, token, feed
        self.interval, self.heartbeat, self.idle_grace, self.log = interval, heartbeat, idle_grace, log
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.viewers: Optional[int] = None
        self.frames = 0
        self.started_at: Optional[float] = None
        self.last_error: Optional[str] = None

    # ---- control
    def start(self) -> bool:
        """Begin streaming. False if a stream is already running (the call is then a no-op)."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop.clear()
            self.viewers, self.frames, self.last_error, self.started_at = None, 0, None, time.time()
            self._thread = threading.Thread(target=self._run, name="live-stream", daemon=True)
            self._thread.start()
            return True

    def stop(self) -> None:
        self._stop.set()

    def status(self) -> dict:
        alive = self._thread is not None and self._thread.is_alive()
        return {"running": alive, "viewers": self.viewers, "frames": self.frames, "since": self.started_at if alive else None,
                "error": self.last_error}

    # ---- the stream
    def _connect(self):
        host, port = self._u.hostname, self._u.port or (443 if self._u.scheme == "https" else 80)
        sock = socket.create_connection((host, port), timeout=10)
        if self._u.scheme == "https":
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
        key = base64.b64encode(secrets.token_bytes(16)).decode()
        path = (self._u.path or "") + LIVE_PATH
        head = (f"GET {path} HTTP/1.1\r\nHost: {self._u.netloc}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nAuthorization: Bearer {self._token}\r\n\r\n")
        sock.sendall(head.encode())
        buf = b""
        deadline = time.time() + 10
        while b"\r\n\r\n" not in buf:
            if time.time() > deadline or len(buf) > 16_384:
                raise WsError("no handshake answer")
            chunk = sock.recv(4096)
            if not chunk:
                raise WsError("the Worker closed the connection during the handshake")
            buf += chunk
        header, _, rest = buf.partition(b"\r\n\r\n")
        lines = header.decode("latin-1").split("\r\n")
        if " 101 " not in lines[0] + " ":
            raise WsError(f"the Worker refused the stream ({lines[0][:60]})")
        got = {k.strip().lower(): v.strip() for k, v in (ln.split(":", 1) for ln in lines[1:] if ":" in ln)}
        if got.get("sec-websocket-accept") != _accept(key):
            raise WsError("the Worker's handshake answer was wrong")
        sock.setblocking(False)
        return sock, rest

    def _run(self) -> None:
        attempts = 0
        while not self._stop.is_set():
            try:
                reason = self._stream()
                if reason in ("idle", "stopped", "closed"):
                    break
                attempts = 0
            except Exception as err:  # noqa: BLE001
                self.last_error = f"{type(err).__name__}: {str(err)[:120]}"
                self.log(f"live stream: {self.last_error}")
                attempts += 1
                if attempts >= 3:
                    break
                self._stop.wait(min(8.0, 2.0 ** attempts))
        self.log("live stream ended")

    def _send(self, sock, data: bytes) -> None:
        view = memoryview(data)
        deadline = time.time() + 10
        while view:
            try:
                n = sock.send(view)
                view = view[n:]
            except (BlockingIOError, ssl.SSLWantWriteError):
                if time.time() > deadline:
                    raise WsError("send timed out")
                select.select([], [sock], [], 0.5)

    def _stream(self) -> str:
        sock, rest = self._connect()
        reader = FrameReader()
        try:
            idle_since: Optional[float] = None
            last_sent, last_sig = 0.0, None
            pending = reader.feed(rest) if rest else []
            while not self._stop.is_set():
                for op, payload in pending:
                    if op == 8:
                        return "closed"
                    if op == 9:
                        self._send(sock, encode_frame(10, payload))
                    elif op == 1:
                        try:
                            msg = json.loads(payload.decode("utf-8"))
                        except ValueError:
                            continue
                        if isinstance(msg, dict) and msg.get("type") == "subs" and isinstance(msg.get("n"), int):
                            self.viewers = msg["n"]
                pending = []
                now = time.time()
                if self.viewers == 0:
                    idle_since = idle_since or now
                    if now - idle_since >= self.idle_grace:
                        self._send(sock, encode_frame(8, struct.pack("!H", 1000) + b"idle"))
                        return "idle"
                else:
                    idle_since = None
                if self.viewers != 0:
                    frame = self.feed.frame()
                    sig = json.dumps({"vm": frame["vm"], "rows": frame["rows"], "out": frame["out"]}, sort_keys=True, default=str)
                    if sig != last_sig or now - last_sent >= self.heartbeat:
                        self._send(sock, encode_frame(1, json.dumps(frame, default=str).encode()))
                        last_sent, last_sig = now, sig
                        self.frames += 1
                # wait out the rest of the interval, but wake at once for a message from the hub
                ready = select.select([sock], [], [], self.interval)[0]
                if ready or (hasattr(sock, "pending") and sock.pending()):
                    try:
                        data = sock.recv(65536)
                    except (BlockingIOError, ssl.SSLWantReadError):
                        data = None
                    if data == b"":
                        return "lost"
                    if data:
                        pending = reader.feed(data)
            try:
                self._send(sock, encode_frame(8, struct.pack("!H", 1000) + b"stop"))
            except Exception:  # noqa: BLE001
                pass
            return "stopped"
        finally:
            try:
                sock.close()
            except OSError:
                pass
