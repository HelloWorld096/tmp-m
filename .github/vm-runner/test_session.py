"""Tests for vm_session: boot/restore, saves, fencing, token refresh, autosave, graceful shutdown.

Real GitHub, Docker and Cloudflare are NOT used: MockGitHub (test_snapshot), fake_docker (directory-backed containers)
and MockWorker (below) stand in. See the testament for what remains unverified on real systems.
"""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

os.environ["VM_SNAPSHOT_TEST"] = "1"
import vm_container as vc  # noqa: E402
import vm_session as vsn  # noqa: E402
import vm_snapshot as vs  # noqa: E402
from test_container import env, make, rootfs, IMG  # noqa: E402,F401  (env is a fixture)
from test_snapshot import gh, REPO  # noqa: E402,F401  (gh is a fixture)

RUNNER_TOKEN = "runner-secret-token-0123456789"
REPO_TOKEN = "ghs_session_repo_token"


class MockWorker:
    """Records /runner/session/* calls. Knobs: commit_status, event_status, token_status, token_ttl, tokens_issued."""

    def __init__(self):
        self.events, self.token_calls, self.bad_auth = [], 0, 0
        self.commit_status, self.event_status, self.token_status = 200, 200, 200
        self.token_ttl = 3600
        self.lock = threading.Lock()
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a): pass

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"{}")
                if self.headers.get("Authorization") != f"Bearer {RUNNER_TOKEN}":
                    outer.bad_auth += 1
                    return self._send(401, {"error": "unauthorized"})
                with outer.lock:
                    if self.path == "/runner/session/token":
                        outer.token_calls += 1
                        if outer.token_status != 200:
                            return self._send(outer.token_status, {"error": "x"})
                        exp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + outer.token_ttl))
                        return self._send(200, {"repo_token": REPO_TOKEN, "expires_at": exp})
                    if self.path == "/runner/session/event":
                        outer.events.append(body)
                        status = outer.commit_status if body.get("type") == "commit" else outer.event_status
                        return self._send(status, {"ok": status == 200})
                self._send(404, {})

            def _send(self, code, obj):
                data = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def url(self): return f"http://127.0.0.1:{self.port}"
    def close(self): self.srv.shutdown(); self.srv.server_close()
    def types(self): return [e["type"] for e in self.events]


@pytest.fixture
def mw():
    w = MockWorker()
    yield w
    w.close()


class Clock:
    def __init__(self): self.t = 1_000_000.0
    def __call__(self): return self.t


def bundle(**kw):
    d = dict(runner_token=RUNNER_TOKEN, session_id="abc", epoch=1, repo=REPO, image=IMG, repo_token=REPO_TOKEN,
             repo_token_expires_at=time.time() + 3600)
    d.update(kw)
    return vsn.Bundle(**d)


def manager(env, mw, tmp, *, clock=None, b=None, chunk=2000, **kw):
    scratch, flags = tmp / "scratch", tmp / "flags"
    scratch.mkdir(parents=True, exist_ok=True)
    c = make(env)
    worker = vsn.WorkerApi(mw.url(), RUNNER_TOKEN, retries=2, backoff=0, sleep=lambda s: None)
    b = b or bundle()

    def mk(provider):
        return vs.SnapshotStore(b.repo, provider, session=b.session_id, chunk_bytes=chunk, min_free=0, backoff=0,
                                sleep=lambda s: None, scratch=scratch)

    kw.setdefault("poll_s", 0.05)
    m = vsn.SessionManager(b, worker=worker, container=c, store=mk, flags_dir=flags,
                           clock=clock or time.time, sleep=lambda s: None, **kw)
    return m, c, flags


def wait_state(m, states, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        if m.state in states:
            return m.state
        time.sleep(0.02)
    raise AssertionError(f"state stayed {m.state} ({m.error})")


def boot_sync(m):
    m.boot()
    return m.state


def write_file(env, c, name="a.txt", text="hello\n"):
    (rootfs(env, c) / "workspace").mkdir(parents=True, exist_ok=True)
    (rootfs(env, c) / "workspace" / name).write_text(text)


# --------------------------------------------------------------------------- bundle parsing
def test_parse_bare_token_is_legacy():
    b = vsn.parse_bundle("  abc123  \n")
    assert b.runner_token == "abc123" and not b.has_session


def test_parse_bundle_with_session():
    raw = json.dumps({"token": "t", "session": {"id": "s_ab12cd34", "epoch": 3, "repo": "o/vmsession-s_ab12cd34",
                                                "image": "ubuntu", "repo_token": "ghs_x",
                                                "repo_token_expires_at": "2026-10-22T10:00:00Z", "fresh": True}})
    b = vsn.parse_bundle(raw)
    assert b.session_id == "s_ab12cd34" and b.epoch == 3 and b.image == vc.DEFAULT_IMAGE and b.fresh
    assert b.repo_token_expires_at > 1_700_000_000


def test_parse_bundle_without_session_is_legacy():
    assert not vsn.parse_bundle('{"token": "t"}').has_session


@pytest.mark.parametrize("raw", ["", "{bad", '{"session": {}}', '{"token": ""}',
                                 json.dumps({"token": "t", "session": {"id": "../x", "epoch": 1, "repo": "o/r", "repo_token": "x"}}),
                                 json.dumps({"token": "t", "session": {"id": "a", "epoch": 0, "repo": "o/r", "repo_token": "x"}}),
                                 json.dumps({"token": "t", "session": {"id": "a", "epoch": True, "repo": "o/r", "repo_token": "x"}}),
                                 json.dumps({"token": "t", "session": {"id": "a", "epoch": 1, "repo": "no-slash", "repo_token": "x"}}),
                                 json.dumps({"token": "t", "session": {"id": "a", "epoch": 1, "repo": "o/r", "repo_token": ""}}),
                                 json.dumps({"token": "t", "session": {"id": "a", "epoch": 1, "repo": "o/r", "repo_token": "x", "image": "--privileged"}})])
def test_parse_bundle_rejects(raw):
    with pytest.raises(ValueError):
        vsn.parse_bundle(raw)


def test_worker_api_requires_https_outside_tests(monkeypatch):
    monkeypatch.delenv("VM_SNAPSHOT_TEST")
    with pytest.raises(ValueError):
        vsn.WorkerApi("http://example.com", "t")
    vsn.WorkerApi("https://example.com", "t")


# --------------------------------------------------------------------------- boot
def test_fresh_session_boots_ready_and_reports(gh, mw, env, tmp_path):
    m, c, flags = manager(env, mw, tmp_path)
    assert boot_sync(m) == "ready"
    assert mw.types() == ["fresh"]
    assert (rootfs(env, c) / "workspace").is_dir()
    assert m.status()["last_save"] is None


def test_start_writes_active_flag(gh, mw, env, tmp_path):
    m, c, flags = manager(env, mw, tmp_path)
    m.start()
    try:
        wait_state(m, ("ready",))
        assert (flags / vsn.FLAG_ACTIVE).read_text() == "abc"
    finally:
        m.close()


def test_save_then_restore_into_new_container(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c, "a.txt", "persist me\n")
    (rootfs(env, c) / "workspace" / "sub").mkdir()
    m.workdir = "/workspace/sub"
    summary = m.save("manual")
    assert summary["seq"] == 1 and "commit" in mw.types() and "committed" in mw.types()
    # a brand new runner for the same session (epoch 2)
    mw.events.clear()
    m2, c2, _ = manager(env, mw, tmp_path / "second", b=bundle(epoch=2))
    assert boot_sync(m2) == "ready"
    assert mw.types() == ["restored"]
    assert (rootfs(env, c2) / "workspace" / "a.txt").read_text() == "persist me\n"
    assert m2.workdir == "/workspace/sub" and (rootfs(env, c2) / "workspace" / "sub").is_dir()
    assert m2.status()["last_save"]["seq"] == 1


def test_boot_removes_drafts_left_by_a_killed_run_but_keeps_the_snapshot(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    m.save("manual")                                    # snap-1 committed (epoch 1)
    # a run of epoch 1 was killed halfway through uploading snap-2: its draft is still on GitHub
    gh.releases[950] = {"id": 950, "name": "snap-uploading-1-2", "tag_name": "vmsnap-1-2-aaaaaaaa", "draft": True,
                        "body": "", "assets": []}
    m2, c2, _ = manager(env, mw, tmp_path / "second", b=bundle(epoch=2))
    assert boot_sync(m2) == "ready"
    names = sorted(r["name"] for r in gh.releases.values())
    assert names == ["snap-1"]                          # the orphaned draft is gone, the valid snapshot is untouched
    assert (rootfs(env, c2) / "workspace" / "a.txt").exists()


def test_boot_cleanup_never_touches_a_newer_epoch_draft(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    m.save("manual")
    gh.releases[951] = {"id": 951, "name": "snap-uploading-5-2", "tag_name": "vmsnap-5-2-aaaaaaaa", "draft": True,
                        "body": "", "assets": []}      # belongs to a NEWER owner than the epoch-2 runner below
    m2, c2, _ = manager(env, mw, tmp_path / "second", b=bundle(epoch=2))
    assert boot_sync(m2) == "ready"
    assert "snap-uploading-5-2" in [r["name"] for r in gh.releases.values()]


def test_restore_falls_back_when_the_saved_workdir_is_gone(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    m.workdir = "/workspace/deleted"
    m.save()
    m2, c2, _ = manager(env, mw, tmp_path / "s2", b=bundle(epoch=2))
    assert boot_sync(m2) == "ready" and m2.workdir == "/workspace"


def test_multi_chunk_snapshot_restores(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path, chunk=700)
    boot_sync(m)
    (rootfs(env, c) / "workspace").mkdir(exist_ok=True)
    (rootfs(env, c) / "workspace" / "big.bin").write_bytes(os.urandom(6000))
    assert m.save()["chunks"] >= 2
    m2, c2, _ = manager(env, mw, tmp_path / "s2", b=bundle(epoch=2), chunk=700)
    assert boot_sync(m2) == "ready"
    assert (rootfs(env, c2) / "workspace" / "big.bin").stat().st_size == 6000


def test_restore_failure_goes_failed_and_never_saves(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    m.save()
    gh.corrupt_stored = True            # every snapshot now unusable? corrupt what is stored from here
    for a in gh.assets.values():
        a["data"] = bytes([a["data"][0] ^ 0xFF]) + a["data"][1:]
    gh.corrupt_stored = False
    before = {r["name"] for r in gh.releases.values()}
    mw.events.clear()
    m2, c2, _ = manager(env, mw, tmp_path / "s2", b=bundle(epoch=2))
    assert boot_sync(m2) == "failed"
    assert mw.types() == ["failed"] and "verif" in m2.status()["error"].lower()
    with pytest.raises(vsn.Unavailable) as e:
        m2.save()
    assert e.value.reason == "failed"
    with pytest.raises(vsn.Unavailable):
        m2.acquire_job()
    assert {r["name"] for r in gh.releases.values()} == before, "a failed restore must not touch the snapshots"


def test_image_mismatch_refuses_to_apply(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    m.save()
    (env["images"] / "python_3.12-slim").mkdir()
    m2, c2, _ = manager(env, mw, tmp_path / "s2", b=bundle(epoch=2, image="python:3.12-slim"))
    assert boot_sync(m2) == "failed"
    assert "fixed at creation" in m2.status()["error"]


def test_boot_failure_reports_without_secrets(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    m.container.create = lambda: (_ for _ in ()).throw(vc.DockerError(f"boom {REPO_TOKEN} {RUNNER_TOKEN}"))
    assert boot_sync(m) == "failed"
    ev = [e for e in mw.events if e["type"] == "failed"][0]
    assert REPO_TOKEN not in ev["reason"] and RUNNER_TOKEN not in ev["reason"] and len(ev["reason"]) <= 300


# --------------------------------------------------------------------------- saves and fencing
def test_stale_epoch_on_commit_aborts_and_keeps_previous(gh, mw, env, tmp_path):
    m, c, flags = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c, "a.txt", "v1\n")
    m.save()
    names = {r["name"] for r in gh.releases.values()}
    write_file(env, c, "a.txt", "v2\n")
    mw.commit_status = 409
    with pytest.raises(vs.Fenced):
        m.save()
    assert {r["name"] for r in gh.releases.values()} == names, "the unfinished upload must be deleted"
    assert m.state == "failed" and "superseded" in m.error
    assert (flags / vsn.FLAG_STOP_NOW).exists()
    with pytest.raises(vsn.Unavailable):
        m.save()


def test_worker_down_at_commit_fails_closed(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    mw.commit_status = 500
    with pytest.raises(vs.StoreError):
        m.save()
    assert not [r for r in gh.releases.values() if r["name"].startswith("snap-") and "uploading" not in r["name"]]
    assert m.state == "ready", "a failed save must not take the session down"
    assert m.status()["save_error"]


def test_committed_event_failure_is_tolerated(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    mw.event_status = 500
    assert m.save()["seq"] == 1


def test_save_refused_while_a_job_runs(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    m.acquire_job()
    with pytest.raises(vsn.Busy):
        m.save()
    m.release_job("/workspace/x")
    assert m.workdir == "/workspace/x"
    m.save()


def test_jobs_are_refused_while_capturing(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    seen = {}
    real = c.capture

    def slow(w):
        seen["gate"] = m.gate()
        try:
            m.acquire_job()
        except vsn.Unavailable as e:
            seen["reason"] = e.reason
        return real(w)

    c.capture = slow
    m.save()
    assert seen["reason"] == "capturing" and seen["gate"].reason == "capturing"
    m.acquire_job()      # fine again afterwards


def test_two_saves_do_not_overlap(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    errors, gate = [], threading.Event()
    real = c.capture

    def slow(w):
        gate.wait(5)
        return real(w)

    c.capture = slow
    t = threading.Thread(target=lambda: m.save())
    t.start()
    time.sleep(0.3)
    try:
        m.save()
    except vsn.Busy as e:
        errors.append(e)
    gate.set()
    t.join(20)
    assert len(errors) == 1


# --------------------------------------------------------------------------- tokens
def test_token_is_refreshed_when_close_to_expiry(gh, mw, env, tmp_path):
    clock = Clock()
    b = bundle(repo_token="ghs_old", repo_token_expires_at=clock() + 60)
    m, c, _ = manager(env, mw, tmp_path, clock=clock, b=b)
    boot_sync(m)
    assert mw.token_calls >= 1 and m._repo_token() == REPO_TOKEN
    calls = mw.token_calls
    m._repo_token()
    assert mw.token_calls == calls, "a fresh token is reused"


def test_refresh_failure_keeps_a_still_valid_token(gh, mw, env, tmp_path):
    clock = Clock()
    b = bundle(repo_token="ghs_old", repo_token_expires_at=clock() + 200)   # inside the margin, but valid
    m, c, _ = manager(env, mw, tmp_path, clock=clock, b=b)
    mw.token_status = 500
    assert m._repo_token() == "ghs_old"


def test_refresh_failure_with_expired_token_is_an_error(gh, mw, env, tmp_path):
    clock = Clock()
    b = bundle(repo_token="ghs_old", repo_token_expires_at=clock() - 5)
    m, c, _ = manager(env, mw, tmp_path, clock=clock, b=b)
    mw.token_status = 500
    with pytest.raises(vs.StoreError):
        m._repo_token()


def test_refresh_409_marks_the_runner_superseded(gh, mw, env, tmp_path):
    clock = Clock()
    b = bundle(repo_token_expires_at=clock() - 5)
    m, c, flags = manager(env, mw, tmp_path, clock=clock, b=b)
    mw.token_status = 409
    with pytest.raises(vs.Fenced):
        m._repo_token()
    assert m.state == "failed" and (flags / vsn.FLAG_STOP_NOW).exists()


def test_runner_token_is_sent_to_the_worker_only(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    m.save()
    assert mw.bad_auth == 0
    assert not any(h for h, a in gh.seen_auth if RUNNER_TOKEN in (a or "")), "runner token must never reach GitHub"


# --------------------------------------------------------------------------- autosave
def test_tick_does_not_periodically_autosave_dirty_idle_session(gh, mw, env, tmp_path):
    clock = Clock()
    m, c, _ = manager(env, mw, tmp_path, clock=clock,
                      autosave_idle_s=60, autosave_interval_s=600)
    boot_sync(m)
    write_file(env, c)
    m.acquire_job()
    m.release_job()

    # Advance beyond both former autosave thresholds.
    clock.t += 3000
    m.tick()

    assert m.last_save is None
    assert mw.types().count("commit") == 0
    assert m.state == "ready"


def test_tick_does_not_autosave_while_a_job_runs(gh, mw, env, tmp_path):
    clock = Clock()
    m, c, _ = manager(env, mw, tmp_path, clock=clock)
    boot_sync(m)
    write_file(env, c)
    m.acquire_job()
    clock.t += 5000
    m.tick()

    assert m.last_save is None
    assert mw.types().count("commit") == 0


def test_tick_does_not_attempt_periodic_autosave(gh, mw, env, tmp_path):
    clock = Clock()
    m, c, _ = manager(env, mw, tmp_path, clock=clock)
    boot_sync(m)
    write_file(env, c)
    m.acquire_job()
    m.release_job()
    mw.commit_status = 500

    clock.t += 5000
    m.tick()
    clock.t += 700
    m.tick()

    assert mw.types().count("commit") == 0
    assert m.last_save is None
    assert m.state == "ready"


def test_shutdown_saves_then_stops(gh, mw, env, tmp_path):
    m, c, flags = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c, "final.txt", "bye\n")
    m.acquire_job(); m.release_job()
    assert m.shutdown() is True
    assert m.shutdown() is False, "idempotent"
    assert m.wait_stopped(30)
    assert m.state == "stopped" and m.last_save["seq"] == 1
    assert (flags / vsn.FLAG_SAVED).read_text() == "ok" and (flags / vsn.FLAG_STOP_NOW).exists()
    stopped = [e for e in mw.events if e["type"] == "stopped"][0]
    assert stopped["saved"] is True
    assert not (env["state"] / "containers" / c.name).exists(), "container removed"
    m2, c2, _ = manager(env, mw, tmp_path / "s2", b=bundle(epoch=2))
    boot_sync(m2)
    assert (rootfs(env, c2) / "workspace" / "final.txt").read_text() == "bye\n"


def test_shutdown_of_a_clean_session_skips_the_upload(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    m.save()
    commits = mw.types().count("commit")
    m.shutdown()
    assert m.wait_stopped(30)
    assert mw.types().count("commit") == commits
    assert [e for e in mw.events if e["type"] == "stopped"][0]["saved"] is True


def test_shutdown_with_a_running_job_needs_force(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    m.acquire_job()
    with pytest.raises(vsn.Busy):
        m.shutdown()
    assert m.state == "ready"
    killed = []
    m._kill_jobs = lambda: (killed.append(1), m.release_job())
    assert m.shutdown(force=True)
    assert m.wait_stopped(30) and killed == [1] and m.state == "stopped"


def test_failed_final_save_reports_not_saved_and_keeps_previous(gh, mw, env, tmp_path):
    m, c, flags = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c, "a.txt", "v1\n")
    m.save()
    write_file(env, c, "a.txt", "v2\n")
    m.acquire_job(); m.release_job()
    mw.commit_status = 500
    m.shutdown()
    assert m.wait_stopped(30)
    stopped = [e for e in mw.events if e["type"] == "stopped"][-1]
    assert stopped["saved"] is False and stopped["reason"]
    assert (flags / vsn.FLAG_SAVED).read_text() == "failed"
    assert any(r["name"] == "snap-1" for r in gh.releases.values())


def test_shutdown_of_a_failed_session_saves_nothing(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    m.container.create = lambda: (_ for _ in ()).throw(RuntimeError("no docker"))
    boot_sync(m)
    m.shutdown()
    assert m.wait_stopped(30)
    assert "commit" not in mw.types()
    assert [e for e in mw.events if e["type"] == "stopped"][0]["saved"] is False


def test_keepalive_stopping_flag_triggers_final_save(gh, mw, env, tmp_path):
    m, c, flags = manager(env, mw, tmp_path)
    boot_sync(m)
    write_file(env, c)
    m.acquire_job(); m.release_job()
    flags.mkdir(exist_ok=True)
    (flags / vsn.FLAG_STOPPING).write_text("")
    m.tick()
    assert m.wait_stopped(30) and (flags / vsn.FLAG_SAVED).read_text() == "ok"


def test_jobs_refused_after_shutdown_starts(gh, mw, env, tmp_path):
    m, c, _ = manager(env, mw, tmp_path)
    boot_sync(m)
    m.shutdown()
    m.wait_stopped(30)
    with pytest.raises(vsn.Unavailable) as e:
        m.acquire_job()
    assert e.value.reason == "stopped"


# --------------------------------------------------------------------------- relay with a container
STUB_RELAY = """#!/bin/bash
echo "$@" >> "$STUB_LOG"
mode=$1; repo=$2; arg=$3
case $mode in
  recv) if [ -n "$STUB_MULTI" ]; then echo a > "${arg%/}/one"; echo b > "${arg%/}/two"; else echo "payload-from-relay" > "${arg%/}/uploaded.txt"; fi;;
  put) cp "$arg" "$STUB_OUT/" && echo "RESULT: OK";;
esac
"""


def relay_env(env, monkeypatch):
    t = env["tmp"]
    relay = t / "relay.sh"
    relay.write_text(STUB_RELAY)
    relay.chmod(0o755)
    (t / "out").mkdir()
    monkeypatch.setenv("STUB_LOG", str(t / "relay.log"))
    monkeypatch.setenv("STUB_OUT", str(t / "out"))
    c = make(env)
    c.create()
    jobs = t / "jobs"
    tok = t / "tokfile"
    tok.write_text("secret")
    return c, relay, jobs, tok


def run_relay(c, relay, jobs, tok, mode, path, repo="o/r"):
    import subprocess
    script = vsn.relay_script(c, mode, str(relay), repo, path, str(tok), str(jobs), cwd="/workspace")
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=60)


def test_relay_recv_into_container_file(env, monkeypatch):
    c, relay, jobs, tok = relay_env(env, monkeypatch)
    r = run_relay(c, relay, jobs, tok, "recv", "/workspace/in/new.txt")
    assert r.returncode == 0, r.stderr
    assert (rootfs(env, c) / "workspace" / "in" / "new.txt").read_text() == "payload-from-relay\n"
    assert "RESULT: OK" in r.stdout
    assert not tok.exists(), "token file removed"
    assert not list(jobs.glob("relay-stage-*")), "staging removed"


def test_relay_recv_into_directory_and_relative_path(env, monkeypatch):
    c, relay, jobs, tok = relay_env(env, monkeypatch)
    assert run_relay(c, relay, jobs, tok, "recv", "/workspace/dl/").returncode == 0
    assert (rootfs(env, c) / "workspace" / "dl" / "uploaded.txt").exists()
    tok.write_text("secret")
    assert run_relay(c, relay, jobs, tok, "recv", "rel/x.txt").returncode == 0
    assert (rootfs(env, c) / "workspace" / "rel" / "x.txt").exists(), "relative paths resolve against the job cwd"


def test_relay_recv_refuses_more_than_one_file(env, monkeypatch):
    c, relay, jobs, tok = relay_env(env, monkeypatch)
    monkeypatch.setenv("STUB_MULTI", "1")
    r = run_relay(c, relay, jobs, tok, "recv", "/workspace/x/")
    assert r.returncode == 1 and "exactly one" in r.stderr
    assert not list(jobs.glob("relay-stage-*"))


def test_relay_put_from_container(env, monkeypatch):
    c, relay, jobs, tok = relay_env(env, monkeypatch)
    (rootfs(env, c) / "workspace" / "report.txt").write_text("results\n")
    r = run_relay(c, relay, jobs, tok, "put", "/workspace/report.txt")
    assert r.returncode == 0, r.stderr
    assert (env["tmp"] / "out" / "report.txt").read_text() == "results\n"
    assert not list(jobs.glob("relay-stage-*")) and not tok.exists()


def test_relay_put_of_a_missing_file_never_calls_relay(env, monkeypatch):
    c, relay, jobs, tok = relay_env(env, monkeypatch)
    r = run_relay(c, relay, jobs, tok, "put", "/workspace/nope.txt")
    assert r.returncode == 1 and "not a regular file" in r.stderr
    assert not (env["tmp"] / "relay.log").exists()


def test_relay_script_does_not_execute_hostile_paths(env, monkeypatch):
    c, relay, jobs, tok = relay_env(env, monkeypatch)
    marker = env["tmp"] / "pwned"
    evil = f"/workspace/a b/$(touch {marker})`touch {marker}`;touch {marker}.txt"
    r = run_relay(c, relay, jobs, tok, "recv", evil)
    assert r.returncode == 0, r.stderr
    assert not marker.exists() and not Path(str(marker) + ".txt").exists()
    assert (rootfs(env, c) / evil.lstrip("/")).exists()


def test_relay_script_rejects_bad_mode(env, monkeypatch):
    c, relay, jobs, tok = relay_env(env, monkeypatch)
    with pytest.raises(ValueError):
        vsn.relay_script(c, "rm", str(relay), "o/r", "/x", str(tok), str(jobs))
