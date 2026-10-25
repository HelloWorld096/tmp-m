"""Chunked, SHA-256-verified session snapshots stored as GitHub Release assets in ONE private repo per session.

Transport and verification reuse the Relay protocol's rules (relay.sh `put`/`get`), but not its script: relay.sh is a
one-shot transfer that burns the repository, while a session repository must outlive the VM.

  * Assets are uploaded to uploads.github.com as a stream with an exact Content-Length; the reply's `size` and
    `digest` are compared with what was hashed locally (GitHub computes the digest server-side, so nothing has to be
    downloaded again). A reply without a digest falls back to download-and-hash.
  * The token never goes on a command line, and it is never sent to a CDN host: redirects are followed by hand and the
    Authorization header is dropped as soon as the host changes (the same rule as curl in relay.sh).
  * A snapshot is a draft release `snap-uploading-<epoch>-<seq>` holding chunk-NNNNN.bin assets plus manifest.json.
    It becomes visible to restore by ONE rename (PATCH name -> `snap-<seq>`), which is the commit. Nothing older is
    deleted before that rename succeeded, so a failed save leaves the previous valid snapshot untouched.
  * A newer epoch always wins over an older one, and the Worker is asked for permission right before the commit
    ("fence"), so a runner that was replaced cannot overwrite its successor's snapshot.
  * Stdlib only: the agent's dependency set is hash-locked.
"""
from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import secrets
import shutil
import socket
import ssl
import tempfile
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

MANIFEST_VERSION = 1
MANIFEST_NAME = "manifest.json"
CHUNK_RE = re.compile(r"^chunk-(\d{5})\.bin$")
COMMITTED_RE = re.compile(r"^snap-(\d{1,9})$")
DRAFT_RE = re.compile(r"^snap-uploading-(\d{1,9})-(\d{1,9})$")
TAG_RE = re.compile(r"^vmsnap-(\d{1,9})-(\d{1,9})-[0-9a-f]{8}$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")

GIB = 1 << 30
DEFAULT_CAP_BYTES = 40_000_000_000                 # 40 GB of snapshot storage per session repository
DEFAULT_CHUNK_BYTES = 1 * GIB                      # a release asset must stay under 2 GiB; 40 GB is ~40 chunks
MAX_CHUNK_BYTES = 2_000_000_000
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MIN_FREE_BYTES = 512 * 1024 * 1024                 # never fill the runner's disk completely
IO_BLOCK = 1 << 20


class StoreError(Exception):
    """Anything that makes a save or restore fail. The message is safe to show to the user."""


class CapExceeded(StoreError):
    pass


class DiskFull(StoreError):
    pass


class IntegrityError(StoreError):
    pass


class Fenced(StoreError):
    """The Worker says another runner now owns this session: stop writing."""


class RestoreFailed(StoreError):
    pass


def _endpoints() -> tuple[str, str]:
    api, uploads = "https://api.github.com", "https://uploads.github.com"
    # Same rule as relay.sh: the places a bearer token is sent can only change under the explicit test flag.
    if os.environ.get("VM_SNAPSHOT_TEST") == "1":
        api = os.environ.get("VM_SNAPSHOT_API", api)
        uploads = os.environ.get("VM_SNAPSHOT_UPLOADS", uploads)
    return api, uploads


# --------------------------------------------------------------------------- HTTP
@dataclass
class Resp:
    status: int
    headers: dict
    body: bytes = b""

    def json(self):
        try:
            return json.loads(self.body.decode("utf-8") or "null")
        except (ValueError, UnicodeDecodeError):
            return None


class Http:
    """Minimal HTTP client: retries, manual redirects (token never leaves the API host), streamed bodies."""

    def __init__(self, token_provider: Callable[[], str], *, retries: int = 4, backoff: float = 2.0,
                 sleep: Callable[[float], None] = time.sleep, log: Callable[[str], None] = lambda m: None):
        self._token = token_provider
        self.retries = max(1, retries)
        self.backoff = backoff
        self._sleep = sleep
        self._log = log

    @staticmethod
    def _conn(url: urllib.parse.ParseResult, timeout: float):
        if url.scheme == "https":
            return http.client.HTTPSConnection(url.hostname, url.port or 443, timeout=timeout,
                                               context=ssl.create_default_context(), blocksize=IO_BLOCK)
        if url.scheme == "http" and os.environ.get("VM_SNAPSHOT_TEST") == "1":
            return http.client.HTTPConnection(url.hostname, url.port or 80, timeout=timeout, blocksize=IO_BLOCK)
        raise StoreError(f"refusing to use a non-https endpoint: {url.scheme}://{url.hostname}")

    def _once(self, method: str, url: str, *, headers: dict, body, timeout: float, auth_host: str,
              sink: Optional[Callable[[bytes], None]]) -> Resp:
        for _hop in range(6):
            u = urllib.parse.urlparse(url)
            h = dict(headers)
            if u.hostname == auth_host:
                h["Authorization"] = f"Bearer {self._token()}"
            h["User-Agent"] = "remote-linux-vm-session"
            conn = self._conn(u, timeout)
            try:
                target = u.path + (f"?{u.query}" if u.query else "")
                b = body() if callable(body) else body
                try:
                    conn.request(method, target, body=b, headers=h)
                finally:
                    if callable(body) and hasattr(b, "close"):
                        b.close()
                r = conn.getresponse()
                rh = {k.lower(): v for k, v in r.getheaders()}
                if r.status in (301, 302, 303, 307, 308) and "location" in rh:
                    r.read()
                    url = urllib.parse.urljoin(url, rh["location"])
                    method, body = ("GET", None) if r.status == 303 else (method, body)
                    continue
                if sink is not None and 200 <= r.status < 300:
                    while True:
                        block = r.read(IO_BLOCK)
                        if not block:
                            break
                        sink(block)
                    return Resp(r.status, rh)
                return Resp(r.status, rh, r.read())
            finally:
                conn.close()
        raise StoreError("too many redirects")

    def call(self, method: str, url: str, *, body=None, headers: Optional[dict] = None, timeout: float = 60,
             sink_factory: Optional[Callable[[], Callable[[bytes], None]]] = None) -> Resp:
        """One logical request with retries. `body` may be a callable returning a fresh body for each attempt."""
        auth_host = urllib.parse.urlparse(url).hostname or ""
        delay = self.backoff
        last = "no attempt"
        for attempt in range(1, self.retries + 1):
            try:
                sink = sink_factory() if sink_factory else None
                resp = self._once(method, url, headers=headers or {}, body=body, timeout=timeout,
                                  auth_host=auth_host, sink=sink)
                limited = resp.status == 429 or (resp.status == 403 and (
                    resp.headers.get("x-ratelimit-remaining") == "0" or "retry-after" in resp.headers))
                if resp.status < 500 and not limited:
                    return resp
                last = f"HTTP {resp.status}"
                if "retry-after" in resp.headers and resp.headers["retry-after"].isdigit():
                    delay = max(delay, min(float(resp.headers["retry-after"]), 120))
            except (OSError, http.client.HTTPException, socket.timeout) as err:
                last = f"{type(err).__name__}: {err}"
            if attempt < self.retries:
                self._log(f"{method} {urllib.parse.urlparse(url).path} failed ({last}); retry {attempt}/{self.retries - 1} in {delay:.0f}s")
                self._sleep(delay)
                delay = min(delay * 2, 30)
        raise StoreError(f"{method} {urllib.parse.urlparse(url).path} failed after {self.retries} attempts ({last})")


# --------------------------------------------------------------------------- chunking
@dataclass
class Chunk:
    index: int
    name: str
    path: Path
    bytes: int = 0
    sha256: str = ""


class ChunkWriter:
    """Write-only file object that splits a byte stream into chunk files, hashing every chunk and the whole stream."""

    def __init__(self, directory: Path, chunk_bytes: int, *, max_bytes: int, min_free: int = MIN_FREE_BYTES,
                 disk_usage: Callable = shutil.disk_usage):
        if not 1 <= chunk_bytes <= MAX_CHUNK_BYTES:
            raise StoreError(f"chunk size must be 1..{MAX_CHUNK_BYTES} bytes")
        self.dir = directory
        self.chunk_bytes = chunk_bytes
        self.max_bytes = max_bytes
        self.min_free = min_free
        self._disk_usage = disk_usage
        self.chunks: list[Chunk] = []
        self.total = 0
        self.whole = hashlib.sha256()
        self._fh = None
        self._h = None
        self._cur: Optional[Chunk] = None
        self._aborted = False

    def _open(self) -> None:
        free = self._disk_usage(self.dir).free
        if free < self.min_free:
            raise DiskFull(f"only {free // (1 << 20)} MiB free on the runner; refusing to fill the disk")
        i = len(self.chunks)
        self._cur = Chunk(i, f"chunk-{i:05d}.bin", self.dir / f"chunk-{i:05d}.bin")
        self._fh = open(self._cur.path, "wb")
        self._h = hashlib.sha256()

    def _close_chunk(self) -> None:
        if self._fh is None:
            return
        self._fh.flush()
        self._fh.close()
        assert self._cur is not None and self._h is not None
        self._cur.sha256 = self._h.hexdigest()
        self.chunks.append(self._cur)
        self._fh = self._h = self._cur = None

    def write(self, data) -> int:
        n = len(data)
        if self._aborted:            # a half-closed tar stream may still flush from its finalizer: discard, don't raise
            return n
        view = memoryview(data)
        while len(view):
            if self._fh is None:
                self._open()
            assert self._cur is not None and self._h is not None and self._fh is not None
            room = self.chunk_bytes - self._cur.bytes
            piece = view[:room]
            try:
                self._fh.write(piece)
            except OSError as err:
                raise DiskFull(f"writing the snapshot failed: {err}") from err
            self._h.update(piece)
            self.whole.update(piece)
            self._cur.bytes += len(piece)
            self.total += len(piece)
            view = view[len(piece):]
            if self.total > self.max_bytes:
                raise CapExceeded(
                    f"the snapshot grew past the {self.max_bytes / 1e9:.1f} GB still available for this session")
            if self._cur.bytes >= self.chunk_bytes:
                self._close_chunk()
        return n

    def flush(self) -> None:
        pass

    def finish(self) -> None:
        self._close_chunk()

    def abort(self) -> None:
        self._aborted = True
        if self._fh is not None:
            try:
                self._fh.close()
            except OSError:
                pass
            self._fh = None


# --------------------------------------------------------------------------- manifest
def build_manifest(*, session: str, epoch: int, seq: int, previous_seq: int, chunk_bytes: int,
                   writer: ChunkWriter, config: dict) -> dict:
    return {
        "version": MANIFEST_VERSION,
        "kind": "vm-session-snapshot",
        "session": session,
        "epoch": epoch,
        "seq": seq,
        "previous_seq": previous_seq,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "format": "tar+gzip",
        "chunk_bytes": chunk_bytes,
        "bytes": writer.total,
        "sha256": writer.whole.hexdigest(),
        "chunks": [{"index": c.index, "name": c.name, "bytes": c.bytes, "sha256": c.sha256} for c in writer.chunks],
        "config": config,
    }


def parse_manifest(raw: bytes, *, expect_seq: Optional[int] = None, session: Optional[str] = None) -> dict:
    """Strictly validate a manifest. Anything unexpected is an IntegrityError, never a guess."""
    try:
        m = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as err:
        raise IntegrityError(f"manifest is not valid JSON ({err})") from err
    if not isinstance(m, dict) or m.get("version") != MANIFEST_VERSION or m.get("kind") != "vm-session-snapshot":
        raise IntegrityError("manifest has an unknown version or kind")
    for key, typ in (("seq", int), ("epoch", int), ("bytes", int), ("chunks", list), ("config", dict)):
        if not isinstance(m.get(key), typ) or isinstance(m.get(key), bool):
            raise IntegrityError(f"manifest field {key!r} is missing or malformed")
    if not isinstance(m.get("sha256"), str) or not SHA_RE.match(m["sha256"]):
        raise IntegrityError("manifest sha256 is malformed")
    if expect_seq is not None and m["seq"] != expect_seq:
        raise IntegrityError(f"manifest seq {m['seq']} does not match its release (snap-{expect_seq})")
    if session is not None and m.get("session") != session:
        raise IntegrityError("manifest belongs to a different session")
    total = 0
    for i, c in enumerate(m["chunks"]):
        if (not isinstance(c, dict) or c.get("index") != i or c.get("name") != f"chunk-{i:05d}.bin"
                or not isinstance(c.get("bytes"), int) or c["bytes"] < 0
                or not isinstance(c.get("sha256"), str) or not SHA_RE.match(c["sha256"])):
            raise IntegrityError(f"manifest chunk entry {i} is malformed")
        total += c["bytes"]
    if total != m["bytes"]:
        raise IntegrityError(f"manifest says {m['bytes']} bytes but its chunks add up to {total}")
    return m


# --------------------------------------------------------------------------- store
@dataclass
class Release:
    id: int
    name: str
    tag: str
    draft: bool
    body: str
    assets: list = field(default_factory=list)

    @property
    def seq(self) -> Optional[int]:
        m = COMMITTED_RE.match(self.name) or DRAFT_RE.match(self.name)
        if m:
            return int(m.group(m.lastindex))
        return None

    @property
    def epoch(self) -> int:
        t = TAG_RE.match(self.tag or "")
        d = DRAFT_RE.match(self.name)
        if t:
            return int(t.group(1))
        if d:
            return int(d.group(1))
        try:                      # GitHub may rewrite the tag of a draft; the commit body carries the epoch too
            e = json.loads(self.body or "{}").get("epoch")
            return e if isinstance(e, int) and not isinstance(e, bool) and e >= 0 else 0
        except (ValueError, AttributeError):
            return 0

    @property
    def committed(self) -> bool:
        return bool(COMMITTED_RE.match(self.name))

    @property
    def stored_bytes(self) -> int:
        return sum(int(a.get("size") or 0) for a in self.assets)


@dataclass
class Restored:
    manifest: dict
    chunk_paths: list
    directory: Path
    skipped: list = field(default_factory=list)

    def cleanup(self) -> None:
        shutil.rmtree(self.directory, ignore_errors=True)


Fence = Callable[[str, dict], None]


@dataclass
class Pending:
    """A snapshot that exists only in local scratch space (captured, not yet uploaded)."""
    work: Path
    writer: "ChunkWriter"
    seq: int
    epoch: int
    previous_seq: int
    retained: int
    config: dict

    @property
    def total(self) -> int:
        return self.writer.total

    def discard(self) -> None:
        shutil.rmtree(self.work, ignore_errors=True)


class SnapshotStore:
    def __init__(self, repo: str, token_provider: Callable[[], str], *, session: str,
                 cap_bytes: int = DEFAULT_CAP_BYTES, cap_includes_previous: bool = True,
                 chunk_bytes: int = DEFAULT_CHUNK_BYTES, scratch: Optional[Path] = None,
                 retries: int = 4, backoff: float = 2.0, sleep: Callable[[float], None] = time.sleep,
                 log: Callable[[str], None] = lambda m: None, min_free: int = MIN_FREE_BYTES):
        if not re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", repo):
            raise StoreError("repository must look like OWNER/REPO")
        self.repo = repo
        self.session = session
        self.cap = cap_bytes
        self.cap_includes_previous = cap_includes_previous
        self.chunk_bytes = chunk_bytes
        self.scratch = Path(scratch or tempfile.gettempdir())
        self.min_free = min_free
        self.log = log
        self.http = Http(token_provider, retries=retries, backoff=backoff, sleep=sleep, log=log)
        self.api, self.uploads = _endpoints()

    # ---- GitHub primitives ------------------------------------------------
    def _api(self, method: str, path: str, payload=None, *, ok=(200, 201, 204), timeout: float = 60) -> Resp:
        body = None if payload is None else json.dumps(payload).encode()
        hdrs = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        if body is not None:
            hdrs["Content-Type"] = "application/json"
        r = self.http.call(method, f"{self.api}{path}", body=body, headers=hdrs, timeout=timeout)
        if r.status not in ok:
            msg = ""
            data = r.json()
            if isinstance(data, dict):
                msg = str(data.get("message", ""))[:160]
            raise StoreError(f"{method} {path.split('?')[0]} -> HTTP {r.status} {msg}".strip())
        return r

    def list_releases(self) -> list[Release]:
        out: list[Release] = []
        for page in range(1, 51):
            data = self._api("GET", f"/repos/{self.repo}/releases?per_page=100&page={page}").json()
            if not isinstance(data, list):
                raise StoreError("GitHub returned an unexpected release listing")
            for r in data:
                out.append(Release(int(r["id"]), str(r.get("name") or ""), str(r.get("tag_name") or ""),
                                   bool(r.get("draft")), str(r.get("body") or ""), []))
            if len(data) < 100:
                break
        for rel in out:
            if COMMITTED_RE.match(rel.name) or DRAFT_RE.match(rel.name):
                rel.assets = self.list_assets(rel.id)
        return out

    def list_assets(self, release_id: int) -> list[dict]:
        out: list[dict] = []
        for page in range(1, 51):
            data = self._api("GET", f"/repos/{self.repo}/releases/{release_id}/assets?per_page=100&page={page}").json()
            if not isinstance(data, list):
                raise StoreError("GitHub returned an unexpected asset listing")
            out.extend(data)
            if len(data) < 100:
                break
        return out

    def _upload(self, release_id: int, name: str, size: int, body_factory) -> dict:
        url = f"{self.uploads}/repos/{self.repo}/releases/{release_id}/assets?name={urllib.parse.quote(name)}"
        for attempt in range(3):
            # A previous half-finished attempt (or a reply that got lost after GitHub stored the asset) may have left
            # an asset of the same name; GitHub refuses duplicates with 422, so remove it first.
            for a in self.list_assets(release_id):
                if a.get("name") == name:
                    self._api("DELETE", f"/repos/{self.repo}/releases/assets/{a['id']}", ok=(204, 404))
            r = self.http.call("POST", url, body=body_factory, timeout=1800, headers={
                "Accept": "application/vnd.github+json", "Content-Type": "application/octet-stream",
                "Content-Length": str(size)})
            if r.status == 422 and attempt < 2:
                continue
            if r.status != 201:
                raise StoreError(f"upload of {name} failed: HTTP {r.status}")
            data = r.json()
            if not isinstance(data, dict):
                raise StoreError(f"upload of {name} returned an unreadable reply")
            return data
        raise StoreError(f"upload of {name} failed")

    def _download(self, asset_id: int, dest: Optional[Path], *, expect_bytes: int, expect_sha: Optional[str],
                  limit: Optional[int] = None) -> tuple[str, bytes]:
        """Stream an asset, verifying size and SHA-256 on the way. Returns (sha256, bytes when dest is None).

        Every retry starts from scratch with a fresh hash, so a half-read attempt can never leak into the result.
        """
        state: dict = {}

        def fresh_sink() -> Callable[[bytes], None]:
            if state.get("fh"):
                state["fh"].close()
            h, buf, n = hashlib.sha256(), bytearray(), [0]
            fh = open(dest, "wb") if dest else None
            state.update(h=h, buf=buf, n=n, fh=fh)

            def sink(block: bytes) -> None:
                h.update(block)
                n[0] += len(block)
                if limit is not None and n[0] > limit:
                    raise IntegrityError("asset is larger than allowed")
                if fh:
                    fh.write(block)
                else:
                    buf.extend(block)
            return sink

        try:
            r = self.http.call("GET", f"{self.api}/repos/{self.repo}/releases/assets/{asset_id}",
                               headers={"Accept": "application/octet-stream"}, timeout=1800,
                               sink_factory=fresh_sink)
        finally:
            if state.get("fh"):
                state["fh"].close()
        if r.status != 200:
            raise StoreError(f"download of asset {asset_id} failed: HTTP {r.status}")
        got_n, got = state["n"][0], state["h"].hexdigest()
        if got_n != expect_bytes:
            raise IntegrityError(f"asset {asset_id}: got {got_n} bytes, expected {expect_bytes}")
        if expect_sha is not None and got != expect_sha:
            raise IntegrityError(f"asset {asset_id}: SHA-256 mismatch (got {got[:12]}..., expected {expect_sha[:12]}...)")
        return got, bytes(state["buf"])

    def _verified_upload(self, release_id: int, name: str, size: int, sha: str, body_factory) -> dict:
        """Upload and PROVE the stored copy equals what was hashed: size, then GitHub's digest, else a re-download."""
        a = self._upload(release_id, name, size, body_factory)
        if a.get("state") != "uploaded":
            raise IntegrityError(f"{name}: GitHub reports state {a.get('state')!r}, not 'uploaded'")
        if int(a.get("size", -1)) != size:
            raise IntegrityError(f"{name}: GitHub stored {a.get('size')} bytes, expected {size}")
        digest = a.get("digest")
        if digest:
            if digest != f"sha256:{sha}":
                raise IntegrityError(f"{name}: GitHub digest {digest} differs from local sha256:{sha}")
        else:
            self.log(f"{name}: GitHub returned no digest; verifying by downloading it again")
            if size <= MAX_MANIFEST_BYTES:
                self._download(int(a["id"]), None, expect_bytes=size, expect_sha=sha)
            else:
                self._download_to_null(int(a["id"]), size, sha)
        return a

    def _download_to_null(self, asset_id: int, size: int, sha: str) -> None:
        tmp = self.scratch / f".verify-{secrets.token_hex(6)}"
        try:
            self._download(asset_id, tmp, expect_bytes=size, expect_sha=sha)
        finally:
            tmp.unlink(missing_ok=True)

    # ---- queries ----------------------------------------------------------
    @staticmethod
    def _order(rel: Release) -> tuple:
        return (rel.epoch, rel.seq or 0)

    def committed(self, releases: Optional[list[Release]] = None) -> list[Release]:
        rels = releases if releases is not None else self.list_releases()
        return sorted((r for r in rels if r.committed), key=self._order, reverse=True)

    def usage(self) -> dict:
        rels = self.list_releases()
        c = self.committed(rels)
        return {"stored_bytes": sum(r.stored_bytes for r in rels), "committed": [r.seq for r in c],
                "latest": c[0].seq if c else None}

    # ---- save -------------------------------------------------------------
    def save(self, *, epoch: int, capture: Callable[[ChunkWriter], None], config: dict, fence: Fence,
             progress: Callable[[str], None] = lambda m: None) -> dict:
        """Capture -> upload -> verify -> commit -> collect garbage. Any failure leaves the previous snapshot intact.

        Synchronous convenience: the session manager uses capture_local() and upload() separately so the container is
        only held still for the capture, while the (slow) upload runs in the background."""
        pending = self.capture_local(epoch=epoch, capture=capture, config=config, progress=progress)
        return self.upload(pending, fence=fence, progress=progress)

    def capture_local(self, *, epoch: int, capture: Callable[[ChunkWriter], None], config: dict,
                      progress: Callable[[str], None] = lambda m: None) -> "Pending":
        """Phase 1: write the snapshot to local scratch space. Nothing is uploaded and nothing is visible remotely, so a
        failure here changes nothing. The caller owns the returned Pending and must upload() or discard() it."""
        progress("inspecting the session repository")
        rels = self.list_releases()
        seq = max([r.seq or 0 for r in rels] + [0]) + 1
        previous = [r for r in self.committed(rels)]
        retained = sum(r.stored_bytes for r in previous)
        budget = self.cap - (retained if self.cap_includes_previous else 0)
        if budget <= 0:
            raise CapExceeded(
                f"the session already stores {retained / 1e9:.1f} GB of its {self.cap / 1e9:.0f} GB limit; "
                "the previous snapshot is kept, nothing was changed")
        work = Path(tempfile.mkdtemp(prefix="snap-", dir=self.scratch))
        writer = ChunkWriter(work, self.chunk_bytes, max_bytes=budget, min_free=self.min_free)
        try:
            progress("capturing the filesystem state")
            try:
                capture(writer)
                writer.finish()
            except CapExceeded as err:
                raise CapExceeded(f"{err} (previous snapshot: {retained / 1e9:.1f} GB kept). "
                                  "Delete data in the session or start a fresh session.") from err
            finally:
                writer.abort()
        except BaseException:
            shutil.rmtree(work, ignore_errors=True)
            raise
        return Pending(work=work, writer=writer, seq=seq, epoch=epoch, previous_seq=previous[0].seq if previous else 0,
                       retained=retained, config=config)

    def upload(self, pending: "Pending", *, fence: Fence, progress: Callable[[str], None] = lambda m: None) -> dict:
        """Phase 2: upload, verify, commit, collect garbage. Always removes the scratch files, success or not.
        Only a normal return means the snapshot is durable."""
        writer, seq, epoch = pending.writer, pending.seq, pending.epoch
        release_id: Optional[int] = None
        try:
            progress(f"uploading {len(writer.chunks)} chunk(s), {writer.total} bytes")
            tag = f"vmsnap-{epoch}-{seq}-{secrets.token_hex(4)}"
            created = self._api("POST", f"/repos/{self.repo}/releases", {
                "tag_name": tag, "name": f"snap-uploading-{epoch}-{seq}", "draft": True,
                "body": "snapshot upload in progress"}, ok=(201,)).json()
            release_id = int(created["id"])
            for ch in writer.chunks:
                progress(f"uploading {ch.name} ({ch.index + 1}/{len(writer.chunks)})")
                self._verified_upload(release_id, ch.name, ch.bytes, ch.sha256,
                                      lambda p=ch.path: open(p, "rb"))
                ch.path.unlink(missing_ok=True)        # verified remotely; free the scratch space
            manifest = build_manifest(session=self.session, epoch=epoch, seq=seq,
                                      previous_seq=pending.previous_seq,
                                      chunk_bytes=self.chunk_bytes, writer=writer, config=pending.config)
            mbytes = json.dumps(manifest, sort_keys=True, indent=1).encode()
            msha = hashlib.sha256(mbytes).hexdigest()
            self._verified_upload(release_id, MANIFEST_NAME, len(mbytes), msha, lambda: mbytes)
            self._check_assets(release_id, manifest, with_manifest=len(mbytes))

            progress("committing")
            fence("commit", {"seq": seq, "bytes": writer.total, "sha256": manifest["sha256"]})
            body = json.dumps({"seq": seq, "epoch": epoch, "manifest_sha256": msha, "bytes": writer.total,
                               "sha256": manifest["sha256"], "chunks": len(writer.chunks)}, sort_keys=True)
            self._api("PATCH", f"/repos/{self.repo}/releases/{release_id}", {"name": f"snap-{seq}", "body": body})
            final = self._api("GET", f"/repos/{self.repo}/releases/{release_id}").json()
            if final.get("name") != f"snap-{seq}":
                raise StoreError("the commit rename did not take effect")
            self._check_assets(release_id, manifest, with_manifest=len(mbytes))
            committed_id, release_id = release_id, None      # from here on a failure must NOT delete it
        except BaseException:
            if release_id is not None:
                try:
                    self._api("DELETE", f"/repos/{self.repo}/releases/{release_id}", ok=(204, 404))
                except StoreError as err:
                    self.log(f"could not remove the unfinished upload: {err}")
            raise
        finally:
            pending.discard()

        summary = {"seq": seq, "epoch": epoch, "bytes": writer.total, "sha256": manifest["sha256"],
                   "chunks": len(writer.chunks), "created_at": manifest["created_at"], "release_id": committed_id}
        try:
            fence("committed", summary)
        except StoreError as err:
            self.log(f"snapshot {seq} is committed but the Worker could not be told: {err}")
        removed = self.gc(keep_seq=seq, epoch=epoch)
        summary["removed_obsolete"] = removed
        return summary

    def _check_assets(self, release_id: int, manifest: dict, *, with_manifest: int) -> None:
        have = {a["name"]: int(a.get("size", -1)) for a in self.list_assets(release_id) if a.get("state") == "uploaded"}
        want = {c["name"]: c["bytes"] for c in manifest["chunks"]}
        want[MANIFEST_NAME] = with_manifest
        if have != want:
            raise IntegrityError(f"release assets differ from the manifest (have {sorted(have)}, want {sorted(want)})")

    # ---- garbage collection ----------------------------------------------
    def gc(self, *, keep_seq: int, epoch: int) -> int:
        """Delete obsolete snapshots and abandoned uploads. Only call after `keep_seq` is committed and verified.

        Never touches anything newer than keep_seq (another owner got ahead), nor releases it does not recognise.
        """
        removed = 0
        for rel in self.list_releases():
            seq = rel.seq
            if seq is None or rel.committed and seq == keep_seq:
                continue
            older = seq < keep_seq or (not rel.committed and rel.epoch < epoch)
            if not older:
                continue
            try:
                self._api("DELETE", f"/repos/{self.repo}/releases/{rel.id}", ok=(204, 404))
                removed += 1
            except StoreError as err:
                self.log(f"could not delete obsolete release {rel.name}: {err}")
        return removed

    # ---- restore ----------------------------------------------------------
    def restore(self, directory: Optional[Path] = None, *,
                progress: Callable[[str], None] = lambda m: None) -> Optional[Restored]:
        """Download and verify the newest valid snapshot. None means the repository holds no snapshot (a fresh session).

        Raises RestoreFailed when snapshots exist but none verifies: the caller must NOT start empty and overwrite them.
        """
        rels = self.list_releases()
        candidates = self.committed(rels)
        if not candidates:
            return None
        skipped: list[str] = []
        for rel in candidates:
            work = Path(directory or tempfile.mkdtemp(prefix="restore-", dir=self.scratch))
            work.mkdir(parents=True, exist_ok=True)
            try:
                res = self._restore_one(rel, work, progress)
                res.skipped = skipped
                return res
            except StoreError as err:
                skipped.append(f"snap-{rel.seq}: {err}")
                self.log(f"snapshot snap-{rel.seq} is unusable ({err}); trying the previous one")
                shutil.rmtree(work, ignore_errors=True)
        raise RestoreFailed("no snapshot passed verification: " + "; ".join(skipped))

    def _restore_one(self, rel: Release, work: Path, progress: Callable[[str], None]) -> Restored:
        try:
            body = json.loads(rel.body or "{}")
        except ValueError:
            body = {}
        want_msha = body.get("manifest_sha256")
        if not isinstance(want_msha, str) or not SHA_RE.match(want_msha):
            raise IntegrityError("release carries no manifest checksum")
        by_name = {a["name"]: a for a in rel.assets if a.get("state") == "uploaded"}
        ma = by_name.get(MANIFEST_NAME)
        if not ma or int(ma.get("size", 0)) > MAX_MANIFEST_BYTES:
            raise IntegrityError("manifest.json is missing or too large")
        progress("verifying the manifest")
        _, raw = self._download(int(ma["id"]), None, expect_bytes=int(ma["size"]), expect_sha=want_msha,
                                limit=MAX_MANIFEST_BYTES)
        m = parse_manifest(raw, expect_seq=rel.seq, session=self.session)
        for c in m["chunks"]:
            a = by_name.get(c["name"])
            if not a or int(a.get("size", -1)) != c["bytes"]:
                raise IntegrityError(f"{c['name']} is missing or has the wrong size on GitHub")
        free = shutil.disk_usage(work).free
        if free < m["bytes"] + self.min_free:
            raise DiskFull(f"restore needs {m['bytes'] / 1e9:.1f} GB of scratch space but only {free / 1e9:.1f} GB is free")
        whole = hashlib.sha256()
        paths: list[Path] = []
        for c in m["chunks"]:
            progress(f"downloading {c['name']} ({c['index'] + 1}/{len(m['chunks'])})")
            dest = work / c["name"]
            self._download(int(by_name[c["name"]]["id"]), dest, expect_bytes=c["bytes"], expect_sha=c["sha256"])
            with open(dest, "rb") as fh:
                for block in iter(lambda: fh.read(IO_BLOCK), b""):
                    whole.update(block)
            paths.append(dest)
        if whole.hexdigest() != m["sha256"]:
            raise IntegrityError("the joined snapshot does not match the manifest's SHA-256")
        return Restored(m, paths, work)


def read_chunks(paths: list) -> "ChunkReader":
    return ChunkReader(paths)


class ChunkReader:
    """Read-only file object over the concatenation of chunk files (what tarfile's streaming mode needs)."""

    def __init__(self, paths: list):
        self._paths = list(paths)
        self._i = 0
        self._fh = None

    def read(self, n: int = -1) -> bytes:
        out = bytearray()
        while n < 0 or len(out) < n:
            if self._fh is None:
                if self._i >= len(self._paths):
                    break
                self._fh = open(self._paths[self._i], "rb")
                self._i += 1
            block = self._fh.read(IO_BLOCK if n < 0 else n - len(out))
            if not block:
                self._fh.close()
                self._fh = None
                continue
            out.extend(block)
        return bytes(out)

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None
