"""Tests for vm_snapshot against an in-process mock of the GitHub Releases API.

The mock serves the API on 127.0.0.1 and the "CDN" (asset downloads after a redirect) on `localhost`, a different
hostname, so a leaked Authorization header is visible in `seen_auth`.
"""
import hashlib
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

os.environ["VM_SNAPSHOT_TEST"] = "1"
import vm_snapshot as vs  # noqa: E402

REPO = "owner/vmsession-abc"
TOKEN = "ghs_testtoken"


class MockGitHub:
    def __init__(self):
        self.releases: dict[int, dict] = {}
        self.assets: dict[int, dict] = {}           # id -> {release, name, data, shown_size, digest, state}
        self.next_id = 100
        self.lock = threading.Lock()
        self.seen_auth: list[tuple[str, str]] = []  # (host name, Authorization header)
        self.fail = {}                              # key -> remaining count of injected HTTP 500s
        self.no_digest = False
        self.liar_digest = False
        self.truncate_upload = False
        self.corrupt_stored = False                 # store flipped bytes but claim the right size and digest
        self.lose_reply_once = False                # store the upload but answer 500 (retry must cope)
        self.digest_header_sha = None
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.port = self.srv.server_address[1]
        self.thread = threading.Thread(target=self.srv.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()

    def nid(self):
        self.next_id += 1
        return self.next_id

    def release_json(self, r):
        return {k: r[k] for k in ("id", "name", "tag_name", "draft", "body")}

    def asset_json(self, a):
        out = {"id": a["id"], "name": a["name"], "size": a["shown_size"], "state": a["state"]}
        if a.get("digest"):
            out["digest"] = a["digest"]
        return out

    def _handler(self):
        gh = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _send(self, status, obj=None, raw=None, headers=None):
                body = raw if raw is not None else (b"" if obj is None else json.dumps(obj).encode())
                self.send_response(status)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _maybe_fail(self, key):
                with gh.lock:
                    if gh.fail.get(key, 0) > 0:
                        gh.fail[key] -= 1
                        self._send(500, {"message": "injected"})
                        return True
                return False

            def _route(self, method):
                host = (self.headers.get("Host") or "").split(":")[0]
                gh.seen_auth.append((host, self.headers.get("Authorization", "")))
                path, _, query = self.path.partition("?")
                length = int(self.headers.get("Content-Length") or 0)
                if host == "localhost":
                    m = re.match(r"^/cdn/(\d+)$", path)
                    if not m:
                        return self._send(404, {})
                    a = gh.assets.get(int(m.group(1)))
                    return self._send(200, raw=a["data"]) if a else self._send(404, {})
                body = self.rfile.read(length) if length else b""
                is_upload = method == "POST" and re.match(r"^/repos/[^/]+/[^/]+/releases/\d+/assets$", path)
                base = f"/repos/{REPO}"
                if not path.startswith(base):
                    return self._send(404, {})
                rest = path[len(base):]
                with gh.lock:
                    if method == "GET" and rest == "/releases":
                        qs = dict(p.split("=") for p in query.split("&") if p)
                        per, page = int(qs.get("per_page", 30)), int(qs.get("page", 1))
                        items = [gh.release_json(r) for r in gh.releases.values()]
                        return self._send(200, items[(page - 1) * per: page * per])
                    if method == "POST" and rest == "/releases":
                        d = json.loads(body)
                        r = {"id": gh.nid(), "name": d["name"], "tag_name": d["tag_name"],
                             "draft": d.get("draft", False), "body": d.get("body", "")}
                        gh.releases[r["id"]] = r
                        return self._send(201, gh.release_json(r))
                    m = re.match(r"^/releases/(\d+)$", rest)
                    if m:
                        r = gh.releases.get(int(m.group(1)))
                        if not r:
                            return self._send(404, {"message": "Not Found"})
                        if method == "GET":
                            return self._send(200, gh.release_json(r))
                        if method == "PATCH":
                            r.update(json.loads(body))
                            return self._send(200, gh.release_json(r))
                        if method == "DELETE":
                            gh.releases.pop(r["id"])
                            for aid in [k for k, a in gh.assets.items() if a["release"] == r["id"]]:
                                gh.assets.pop(aid)
                            return self._send(204)
                    m = re.match(r"^/releases/(\d+)/assets$", rest)
                    if m and method == "GET":
                        rid = int(m.group(1))
                        qs = dict(p.split("=") for p in query.split("&") if p)
                        per, page = int(qs.get("per_page", 30)), int(qs.get("page", 1))
                        items = [gh.asset_json(a) for a in gh.assets.values() if a["release"] == rid]
                        return self._send(200, items[(page - 1) * per: page * per])
                    m = re.match(r"^/releases/assets/(\d+)$", rest)
                    if m:
                        a = gh.assets.get(int(m.group(1)))
                        if method == "DELETE":
                            if a:
                                gh.assets.pop(a["id"])
                                return self._send(204)
                            return self._send(404, {})
                        if method == "GET" and a:
                            return self._send(302, headers={"Location": f"http://localhost:{gh.port}/cdn/{a['id']}"})
                    if is_upload:
                        rid = int(re.match(r"^/repos/[^/]+/[^/]+/releases/(\d+)/assets$", path).group(1))
                        name = dict(p.split("=") for p in query.split("&"))["name"]
                        if gh.fail.get("upload", 0) > 0:
                            gh.fail["upload"] -= 1
                            return self._send(500, {"message": "injected"})
                        if any(a["release"] == rid and a["name"] == name for a in gh.assets.values()):
                            return self._send(422, {"message": "already_exists"})
                        stored = body
                        if gh.truncate_upload:
                            stored = body[:-1]
                        if gh.corrupt_stored and stored:
                            stored = bytes([stored[0] ^ 0xFF]) + stored[1:]
                        digest = None if gh.no_digest else "sha256:" + hashlib.sha256(body).hexdigest()
                        if gh.liar_digest:
                            digest = "sha256:" + "0" * 64
                        a = {"id": gh.nid(), "release": rid, "name": name, "data": stored,
                             "shown_size": len(stored), "digest": digest, "state": "uploaded"}
                        gh.assets[a["id"]] = a
                        if gh.lose_reply_once:
                            gh.lose_reply_once = False
                            return self._send(500, {"message": "reply lost"})
                        return self._send(201, gh.asset_json(a))
                return self._send(404, {"message": "no route"})

            def do_GET(self): self._route("GET")
            def do_POST(self): self._route("POST")
            def do_PATCH(self): self._route("PATCH")
            def do_DELETE(self): self._route("DELETE")

        return H

    # helpers for tests
    def by_name(self):
        return {r["name"]: r for r in self.releases.values()}

    def api_tokens_leaked(self):
        return [h for h, a in self.seen_auth if h == "localhost" and a]


@pytest.fixture
def gh(monkeypatch):
    g = MockGitHub()
    monkeypatch.setenv("VM_SNAPSHOT_API", f"http://127.0.0.1:{g.port}")
    monkeypatch.setenv("VM_SNAPSHOT_UPLOADS", f"http://127.0.0.1:{g.port}")
    yield g
    g.close()


def store(tmp_path, **kw):
    kw.setdefault("chunk_bytes", 1000)
    kw.setdefault("min_free", 0)
    kw.setdefault("backoff", 0)
    kw.setdefault("sleep", lambda s: None)
    kw.setdefault("scratch", tmp_path)
    return vs.SnapshotStore(REPO, lambda: TOKEN, session="abc", **kw)


def payload(n, seed=1):
    out, x = bytearray(), seed
    while len(out) < n:
        x = hashlib.sha256(str(x).encode()).digest()
        out += x
    return bytes(out[:n])


def saver(data):
    return lambda w: w.write(data)


def ok_fence(calls=None):
    def f(kind, info):
        if calls is not None:
            calls.append((kind, info))
    return f


def read_back(res):
    r = vs.read_chunks(res.chunk_paths)
    try:
        return r.read()
    finally:
        r.close()


CFG = {"image": "ubuntu:24.04", "workdir": "/workspace"}


# --------------------------------------------------------------------------- unit pieces
def test_chunkwriter_splits_and_hashes(tmp_path):
    w = vs.ChunkWriter(tmp_path, 1000, max_bytes=10**9, min_free=0)
    data = payload(2500)
    w.write(data[:700]); w.write(data[700:]); w.finish()
    assert [c.bytes for c in w.chunks] == [1000, 1000, 500]
    assert w.total == 2500 and w.whole.hexdigest() == hashlib.sha256(data).hexdigest()
    for c in w.chunks:
        assert hashlib.sha256(c.path.read_bytes()).hexdigest() == c.sha256


def test_chunkwriter_exact_multiple_has_no_empty_chunk(tmp_path):
    w = vs.ChunkWriter(tmp_path, 1000, max_bytes=10**9, min_free=0)
    w.write(payload(2000)); w.finish()
    assert [c.bytes for c in w.chunks] == [1000, 1000]


def test_chunkwriter_cap_and_disk(tmp_path):
    w = vs.ChunkWriter(tmp_path, 1000, max_bytes=1500, min_free=0)
    with pytest.raises(vs.CapExceeded):
        w.write(payload(2000))
    w.abort()

    class Free:
        free = 10
    w2 = vs.ChunkWriter(tmp_path / "x", 1000, max_bytes=10**9, min_free=100, disk_usage=lambda d: Free)
    (tmp_path / "x").mkdir()
    with pytest.raises(vs.DiskFull):
        w2.write(b"a")


def test_manifest_roundtrip_and_strict_parse(tmp_path):
    w = vs.ChunkWriter(tmp_path, 1000, max_bytes=10**9, min_free=0)
    w.write(payload(1500)); w.finish()
    m = vs.build_manifest(session="abc", epoch=2, seq=3, previous_seq=2, chunk_bytes=1000, writer=w, config=CFG)
    raw = json.dumps(m).encode()
    assert vs.parse_manifest(raw, expect_seq=3, session="abc")["bytes"] == 1500
    for bad in (b"not json", json.dumps({**m, "version": 9}).encode(), json.dumps({**m, "bytes": 1}).encode(),
                json.dumps({**m, "sha256": "zz"}).encode()):
        with pytest.raises(vs.IntegrityError):
            vs.parse_manifest(bad)
    with pytest.raises(vs.IntegrityError):
        vs.parse_manifest(raw, expect_seq=4)
    with pytest.raises(vs.IntegrityError):
        vs.parse_manifest(raw, session="other")


def test_chunkreader_reads_across_files(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_bytes(b"hello "); b.write_bytes(b"world")
    r = vs.read_chunks([a, b])
    assert r.read(3) == b"hel" and r.read(5) == b"lo wo" and r.read() == b"rld" and r.read() == b""


# --------------------------------------------------------------------------- store: happy paths
def test_round_trip_multi_chunk(gh, tmp_path):
    s = store(tmp_path)
    data = payload(3456)
    calls = []
    res = s.save(epoch=1, capture=saver(data), config=CFG, fence=ok_fence(calls))
    assert res["seq"] == 1 and res["chunks"] == 4 and res["bytes"] == 3456
    assert [c[0] for c in calls] == ["commit", "committed"]
    assert "snap-1" in gh.by_name()
    restored = s.restore()
    try:
        assert read_back(restored) == data
        assert restored.manifest["config"] == CFG and restored.manifest["epoch"] == 1
    finally:
        restored.cleanup()
    assert gh.api_tokens_leaked() == []                    # the token never went to the CDN host
    assert any(a for h, a in gh.seen_auth if h == "127.0.0.1")


def test_fresh_repo_restore_is_none(gh, tmp_path):
    assert store(tmp_path).restore() is None


def test_second_save_replaces_first_and_gc_removes_it(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(2000, 1)), config=CFG, fence=ok_fence())
    res = s.save(epoch=1, capture=saver(payload(1500, 2)), config=CFG, fence=ok_fence())
    assert res["seq"] == 2 and res["removed_obsolete"] == 1
    assert sorted(gh.by_name()) == ["snap-2"]
    assert not any(a["release"] not in gh.releases for a in gh.assets.values())
    r = s.restore()
    assert read_back(r) == payload(1500, 2)
    r.cleanup()


def test_gc_removes_only_obsolete(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(500)), config=CFG, fence=ok_fence())
    # an abandoned upload from an older epoch, an unrelated release, and something NEWER than what we keep
    gh.releases[900] = {"id": 900, "name": "snap-uploading-0-1", "tag_name": "vmsnap-0-1-aaaaaaaa", "draft": True, "body": ""}
    gh.releases[901] = {"id": 901, "name": "my notes", "tag_name": "v1", "draft": False, "body": ""}
    gh.releases[902] = {"id": 902, "name": "snap-uploading-5-9", "tag_name": "vmsnap-5-9-aaaaaaaa", "draft": True, "body": ""}
    s.gc(keep_seq=2, epoch=2)
    names = {r["name"] for r in gh.releases.values()}
    assert "snap-uploading-0-1" not in names and "snap-1" not in names
    assert "my notes" in names and "snap-uploading-5-9" in names


# --------------------------------------------------------------------------- store: cap
def test_cap_exceeded_keeps_previous(gh, tmp_path):
    s = store(tmp_path, cap_bytes=3000)
    s.save(epoch=1, capture=saver(payload(1500)), config=CFG, fence=ok_fence())
    with pytest.raises(vs.CapExceeded) as e:
        s.save(epoch=1, capture=saver(payload(2000, 2)), config=CFG, fence=ok_fence())
    assert "previous snapshot" in str(e.value)
    assert list(gh.by_name()) == ["snap-1"]                # nothing was uploaded, previous intact
    r = s.restore()
    assert read_back(r) == payload(1500)
    r.cleanup()


def test_cap_can_exclude_previous(gh, tmp_path):
    s = store(tmp_path, cap_bytes=3000, cap_includes_previous=False)
    s.save(epoch=1, capture=saver(payload(1500)), config=CFG, fence=ok_fence())
    s.save(epoch=1, capture=saver(payload(2800, 2)), config=CFG, fence=ok_fence())
    assert list(gh.by_name()) == ["snap-2"]


def test_cap_already_full_refuses_before_capture(gh, tmp_path):
    s = store(tmp_path, cap_bytes=1000)
    s.save(epoch=1, capture=saver(payload(1000)), config=CFG, fence=ok_fence())
    called = []
    with pytest.raises(vs.CapExceeded):
        s.save(epoch=1, capture=lambda w: called.append(1), config=CFG, fence=ok_fence())
    assert called == []


# --------------------------------------------------------------------------- store: failures keep the previous snapshot
def test_failed_capture_keeps_previous(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(1200)), config=CFG, fence=ok_fence())

    def boom(w):
        w.write(payload(500))
        raise RuntimeError("docker export died")
    with pytest.raises(RuntimeError):
        s.save(epoch=1, capture=boom, config=CFG, fence=ok_fence())
    assert list(gh.by_name()) == ["snap-1"]
    assert list(tmp_path.iterdir()) == []                  # scratch space was cleaned up


def test_upload_failure_deletes_draft_and_keeps_previous(gh, tmp_path):
    s = store(tmp_path, retries=2)
    s.save(epoch=1, capture=saver(payload(1200)), config=CFG, fence=ok_fence())
    gh.fail["upload"] = 99
    with pytest.raises(vs.StoreError):
        s.save(epoch=1, capture=saver(payload(1200, 2)), config=CFG, fence=ok_fence())
    gh.fail["upload"] = 0
    assert list(gh.by_name()) == ["snap-1"]
    r = s.restore()
    assert read_back(r) == payload(1200)
    r.cleanup()


def test_transient_500s_are_retried(gh, tmp_path):
    s = store(tmp_path)
    gh.fail["upload"] = 2
    s.save(epoch=1, capture=saver(payload(2500)), config=CFG, fence=ok_fence())
    r = s.restore()
    assert read_back(r) == payload(2500)
    r.cleanup()


def test_lost_upload_reply_is_recovered(gh, tmp_path):
    """GitHub stored the asset but the reply was lost; the retry hits 'already exists' and must clean up and re-upload."""
    s = store(tmp_path)
    gh.lose_reply_once = True
    s.save(epoch=1, capture=saver(payload(900)), config=CFG, fence=ok_fence())
    r = s.restore()
    assert read_back(r) == payload(900)
    r.cleanup()


def test_truncated_upload_is_detected(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(1200)), config=CFG, fence=ok_fence())
    gh.truncate_upload = True
    with pytest.raises(vs.IntegrityError):
        s.save(epoch=1, capture=saver(payload(1200, 2)), config=CFG, fence=ok_fence())
    assert list(gh.by_name()) == ["snap-1"]


def test_liar_digest_is_detected(gh, tmp_path):
    s = store(tmp_path)
    gh.liar_digest = True
    with pytest.raises(vs.IntegrityError):
        s.save(epoch=1, capture=saver(payload(1200)), config=CFG, fence=ok_fence())
    assert gh.by_name() == {}


def test_missing_digest_falls_back_to_download_and_catches_corruption(gh, tmp_path):
    s = store(tmp_path)
    gh.no_digest = True
    s.save(epoch=1, capture=saver(payload(2100)), config=CFG, fence=ok_fence())      # clean data passes
    assert "snap-1" in gh.by_name()
    gh.corrupt_stored = True                                                         # same size, wrong bytes
    with pytest.raises(vs.IntegrityError):
        s.save(epoch=1, capture=saver(payload(2100, 2)), config=CFG, fence=ok_fence())
    assert list(gh.by_name()) == ["snap-1"]


def test_fenced_commit_aborts_and_deletes_draft(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(1200)), config=CFG, fence=ok_fence())

    def deny(kind, info):
        if kind == "commit":
            raise vs.Fenced("another runner owns this session")
    with pytest.raises(vs.Fenced):
        s.save(epoch=1, capture=saver(payload(1200, 2)), config=CFG, fence=deny)
    assert list(gh.by_name()) == ["snap-1"]


def test_worker_unreachable_after_commit_is_not_fatal(gh, tmp_path):
    s = store(tmp_path)

    def flaky(kind, info):
        if kind == "committed":
            raise vs.StoreError("worker down")
    res = s.save(epoch=1, capture=saver(payload(800)), config=CFG, fence=flaky)
    assert res["seq"] == 1 and list(gh.by_name()) == ["snap-1"]


# --------------------------------------------------------------------------- restore
def _corrupt_chunk(gh, release_name, chunk="chunk-00000.bin"):
    rid = gh.by_name()[release_name]["id"]
    for a in gh.assets.values():
        if a["release"] == rid and a["name"] == chunk:
            a["data"] = bytes([a["data"][0] ^ 1]) + a["data"][1:]


def test_restore_falls_back_to_older_snapshot(gh, tmp_path):
    s = store(tmp_path, cap_bytes=10**9)
    s.save(epoch=1, capture=saver(payload(1500, 1)), config=CFG, fence=ok_fence())
    # keep snap-1 around (simulate GC not having run) by committing snap-2 by hand through a second store call
    # that skips GC: copy the release structure instead.
    first = dict(gh.by_name()["snap-1"])
    s.save(epoch=1, capture=saver(payload(1500, 2)), config=CFG, fence=ok_fence())
    # restore the older release into the mock as if GC had not removed it
    old_id = 777
    gh.releases[old_id] = {**first, "id": old_id, "name": "snap-1", "tag_name": "vmsnap-1-1-00000000"}
    good = payload(1500, 1)
    w = vs.ChunkWriter(tmp_path / "w", 1000, max_bytes=10**9, min_free=0)
    (tmp_path / "w").mkdir()
    w.write(good); w.finish()
    for c in w.chunks:
        aid = gh.nid()
        gh.assets[aid] = {"id": aid, "release": old_id, "name": c.name, "data": c.path.read_bytes(),
                          "shown_size": c.bytes, "digest": None, "state": "uploaded"}
    m = vs.build_manifest(session="abc", epoch=1, seq=1, previous_seq=0, chunk_bytes=1000, writer=w, config=CFG)
    mb = json.dumps(m, sort_keys=True, indent=1).encode()
    aid = gh.nid()
    gh.assets[aid] = {"id": aid, "release": old_id, "name": "manifest.json", "data": mb, "shown_size": len(mb),
                      "digest": None, "state": "uploaded"}
    gh.releases[old_id]["body"] = json.dumps({"manifest_sha256": hashlib.sha256(mb).hexdigest()})
    _corrupt_chunk(gh, "snap-2")
    r = s.restore()
    try:
        assert r.manifest["seq"] == 1 and read_back(r) == good
        assert len(r.skipped) == 1 and r.skipped[0].startswith("snap-2")
    finally:
        r.cleanup()


def test_restore_all_corrupt_raises(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(1500)), config=CFG, fence=ok_fence())
    _corrupt_chunk(gh, "snap-1")
    with pytest.raises(vs.RestoreFailed):
        s.restore()
    assert "snap-1" in gh.by_name()                        # never deleted because it failed to verify


def test_restore_rejects_tampered_manifest_checksum(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(1500)), config=CFG, fence=ok_fence())
    gh.by_name()["snap-1"]["body"] = json.dumps({"manifest_sha256": "0" * 64})
    with pytest.raises(vs.RestoreFailed):
        s.restore()


def test_restore_disk_full_is_a_clean_failure(gh, tmp_path):
    s = store(tmp_path, min_free=10**15)
    s2 = store(tmp_path)
    s2.save(epoch=1, capture=saver(payload(1500)), config=CFG, fence=ok_fence())
    with pytest.raises(vs.RestoreFailed) as e:
        s.restore()
    assert "scratch space" in str(e.value)


def test_epoch_ordering_beats_sequence(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(600, 1)), config=CFG, fence=ok_fence())
    # a newer epoch's snapshot exists; an older-epoch release with a higher seq must lose
    r1 = gh.by_name()["snap-1"]
    gh.releases[801] = {**r1, "id": 801, "name": "snap-2", "tag_name": "vmsnap-0-2-aaaaaaaa",
                        "body": json.dumps({"epoch": 0, "manifest_sha256": "0" * 64})}
    order = [r.name for r in s.committed()]
    assert order[0] == "snap-1" and order[1] == "snap-2"


def test_epoch_falls_back_to_body_when_tag_is_rewritten():
    r = vs.Release(1, "snap-4", "untagged-abc", False, json.dumps({"epoch": 7}), [])
    assert r.epoch == 7 and r.committed and r.seq == 4


def test_usage_reports_stored_bytes(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(2500)), config=CFG, fence=ok_fence())
    u = s.usage()
    assert u["latest"] == 1 and u["committed"] == [1] and u["stored_bytes"] > 2500


def test_non_https_refused_without_test_flag(monkeypatch):
    monkeypatch.delenv("VM_SNAPSHOT_TEST")
    h = vs.Http(lambda: "t", retries=1)
    with pytest.raises(vs.StoreError):
        h.call("GET", "http://127.0.0.1:1/x")


def test_token_not_sent_to_other_host(gh, tmp_path):
    s = store(tmp_path)
    s.save(epoch=1, capture=saver(payload(1500)), config=CFG, fence=ok_fence())
    gh.seen_auth.clear()
    r = s.restore()
    r.cleanup()
    cdn = [(h, a) for h, a in gh.seen_auth if h == "localhost"]
    assert cdn, "the download should have been redirected to the CDN host"
    assert all(a == "" for _, a in cdn)
