"""Agent tests for persistent-session mode: 503 while restoring, jobs through the container, /session/save,
/session/shutdown, relay transfers into the container, and that legacy mode exposes none of it.

Real Docker/GitHub/Cloudflare are NOT used (fake_docker, MockGitHub, MockWorker). A real uvicorn server is started, as in
test_agent.py, so thread behaviour is exercised for real.
"""
import os
import socket
import stat
import threading
import time

import httpx
import pytest
import uvicorn

os.environ["VM_SNAPSHOT_TEST"] = "1"
import agent  # noqa: E402
import vm_session as vsn  # noqa: E402
from test_container import env, rootfs  # noqa: E402,F401
from test_snapshot import gh  # noqa: E402,F401
from test_session import manager, mw, wait_state, RUNNER_TOKEN, REPO_TOKEN  # noqa: E402,F401

TOKEN = "agent-token-0123456789"


def H():
    return {"Authorization": f"Bearer {TOKEN}"}


class Served:
    def __init__(self, app):
        s = socket.socket(); s.bind(("127.0.0.1", 0)); self.port = s.getsockname()[1]; s.close()
        self.srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="error"))
        threading.Thread(target=self.srv.run, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.port}"
        for _ in range(100):
            try:
                httpx.get(self.url + "/health", timeout=1); break
            except Exception:
                time.sleep(0.05)

    def get(self, path, **kw): return httpx.get(self.url + path, headers=H(), timeout=30, **kw)
    def post(self, path, json=None): return httpx.post(self.url + path, headers=H(), json=json or {}, timeout=60)
    def stop(self): self.srv.should_exit = True


@pytest.fixture
def stack(env, gh, mw, tmp_path, monkeypatch):
    monkeypatch.setattr(agent, "JOBS_DIR", env["tmp"] / "jobs")
    monkeypatch.setattr(agent, "ACTIVITY_FILE", tmp_path / "activity")
    (env["tmp"] / "jobs").mkdir(exist_ok=True)
    made = []

    def build(boot=True):
        m, c, flags = manager(env, mw, tmp_path)
        m._kill_jobs = lambda: None
        ws = tmp_path / "ws"; ws.mkdir(exist_ok=True)
        app = agent.create_app(TOKEN, str(ws), m)
        srv = Served(app)
        made.append((srv, m))
        if boot:
            m.boot()
            assert m.state == "ready", m.error
        return srv, m, c, flags

    yield build
    for srv, m in made:
        m.close(); srv.stop()


def run(srv, command, wait=20, **extra):
    r = srv.post("/jobs", {"command": command, **extra})
    assert r.status_code == 200, r.text
    jid = r.json()["job_id"]
    return srv.get(f"/jobs/{jid}?wait={wait}").json()


# --------------------------------------------------------------------------- availability
def test_jobs_are_refused_with_503_while_restoring(stack):
    srv, m, c, _ = stack(boot=False)
    r = srv.post("/jobs", {"command": "echo hi"})
    assert r.status_code == 503
    assert r.json()["error"] == "restoring"
    st = srv.get("/status").json()
    assert st["busy"] is False and st["session"]["state"] == "restoring"


def test_job_runs_through_the_container_and_tracks_workdir(stack):
    srv, m, c, _ = stack()
    out = run(srv, "cd /tmp && echo marker-$VM_AGENT_JOB")
    assert out["status"] == "finished" and out["exit_code"] == 0
    assert "marker-" + out["job_id"] in out["output"]
    assert m.workdir == "/tmp"                       # the session remembers where the last command ended
    assert srv.get("/status").json()["session"]["dirty"] is True


def test_second_job_is_busy_but_failed_start_does_not_leak_a_slot(stack):
    srv, m, c, _ = stack()
    first = srv.post("/jobs", {"command": "sleep 5"}).json()["job_id"]
    r = srv.post("/jobs", {"command": "echo no"})
    assert r.status_code == 409 and r.json()["job_id"] == first
    assert srv.post("/cancel").status_code == 200
    # the slot count must be back to zero so a save is possible again
    assert m.status()["active_jobs"] == 0


def test_failed_restore_means_503_failed_and_no_save(stack, mw):
    srv, m, c, _ = stack(boot=False)
    m.state, m.error = "failed", "snapshot was taken on another image"
    r = srv.post("/jobs", {"command": "echo hi"})
    assert r.status_code == 503 and r.json()["error"] == "failed"
    s = srv.post("/session/save")
    assert s.status_code == 503 and s.json()["error"] == "failed"
    assert "commit" not in mw.types()


# --------------------------------------------------------------------------- save
def test_save_route_commits_and_a_new_manager_restores_it(stack, env, mw, tmp_path):
    srv, m, c, _ = stack()
    (rootfs(env, c) / "workspace").mkdir(parents=True, exist_ok=True)
    (rootfs(env, c) / "workspace" / "note.txt").write_text("persist me\n")
    m._dirty = True
    r = srv.post("/session/save")
    assert r.status_code == 200, r.text
    snap = r.json()["snapshot"]
    assert snap["seq"] == 1 and "commit" in mw.types()
    assert r.json()["session"]["last_save"]["seq"] == 1
    # the repository now holds the snapshot: a second manager (same session id) restores it
    m2, c2, _ = manager(env, mw, tmp_path / "second")
    m2.boot()
    assert m2.state == "ready", m2.error
    assert (rootfs(env, c2) / "workspace" / "note.txt").read_text() == "persist me\n"


def test_save_is_refused_while_a_job_runs(stack):
    srv, m, c, _ = stack()
    srv.post("/jobs", {"command": "sleep 5"})
    r = srv.post("/session/save")
    assert r.status_code == 409 and r.json()["error"] == "busy"
    srv.post("/cancel")


def test_save_failure_reports_500_and_keeps_the_previous_snapshot(stack, mw, gh):
    srv, m, c, _ = stack()
    m._dirty = True
    assert srv.post("/session/save").status_code == 200
    mw.commit_status = 500                      # the Worker cannot confirm: fail closed
    m._dirty = True
    r = srv.post("/session/save")
    assert r.status_code == 500 and r.json()["error"] == "save_failed"
    assert "previous snapshot" in r.json()["note"]
    names = sorted(r["name"] for r in gh.releases.values())
    assert names == ["snap-1"]                  # the committed snapshot is still there; the failed upload was removed
    assert REPO_TOKEN not in r.text and RUNNER_TOKEN not in r.text


def test_session_routes_require_auth(stack):
    srv, m, c, _ = stack()
    for path in ("/session/save", "/session/shutdown"):
        r = httpx.post(srv.url + path, json={}, timeout=10)
        assert r.status_code == 401
        r = httpx.post(srv.url + path, json={}, headers={"Authorization": "Bearer wrong"}, timeout=10)
        assert r.status_code == 401


# --------------------------------------------------------------------------- shutdown
def test_shutdown_refuses_while_a_job_runs_unless_forced(stack, mw):
    srv, m, c, flags = stack()
    jid = srv.post("/jobs", {"command": "sleep 30"}).json()["job_id"]
    r = srv.post("/session/shutdown", {})
    assert r.status_code == 409 and r.json()["job_id"] == jid
    r = srv.post("/session/shutdown", {"force": True})
    assert r.status_code == 200 and r.json()["started"] is True
    assert m.wait_stopped(60)
    assert srv.get("/status").json()["session"]["state"] == "stopped"
    assert "stopped" in mw.types()
    assert (flags / "vm-saved").read_text() == "ok"
    again = srv.post("/session/shutdown", {})
    assert again.status_code == 200 and again.json()["started"] is False       # idempotent


def test_shutdown_saves_dirty_state_then_new_jobs_are_503(stack, mw):
    srv, m, c, flags = stack()
    run(srv, "echo changed")
    assert srv.post("/session/shutdown", {}).json()["started"] is True
    assert m.wait_stopped(60)
    assert "commit" in mw.types()
    r = srv.post("/jobs", {"command": "echo late"})
    assert r.status_code == 503 and r.json()["error"] == "stopped"


# --------------------------------------------------------------------------- relay into the container
@pytest.fixture
def fake_relay(tmp_path, monkeypatch):
    """A stand-in for the pinned relay.sh: `recv REPO DIR/` writes a file; `put REPO FILE` records what it got."""
    script = tmp_path / "relay.sh"
    script.write_text(
        "#!/bin/sh\n"
        'mode=$1; repo=$2; target=$3\n'
        'if [ "$mode" = recv ]; then printf "from-relay" > "${target}payload.txt"; '
        'else cp "$target" "' + str(tmp_path) + '/put-got.txt"; fi\n'
        'echo "RESULT: OK"\n'
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setattr(agent, "RELAY_SCRIPT", script)
    return tmp_path


def relay(srv, mode, path, rid):
    r = srv.post("/relay/jobs", {"mode": mode, "repo": "owner/relay-x", "path": path, "token": "t" * 30,
                                 "request_id": rid, "timeout_seconds": 60})
    assert r.status_code == 200, r.text
    return srv.get(f"/jobs/{r.json()['job_id']}?wait=30").json()


def test_relay_recv_lands_inside_the_container(stack, env, fake_relay):
    srv, m, c, _ = stack()
    out = relay(srv, "recv", "/workspace/in/payload.txt", "a" * 32)
    assert out["exit_code"] == 0, out["output"]
    assert (rootfs(env, c) / "workspace" / "in" / "payload.txt").read_text() == "from-relay"
    # no token file or staging directory is left behind
    assert not list((env["tmp"] / "jobs").glob("relay-token-*"))
    assert not list((env["tmp"] / "jobs").glob("relay-stage-*"))


def test_relay_put_reads_from_the_container(stack, env, fake_relay):
    srv, m, c, _ = stack()
    (rootfs(env, c) / "workspace").mkdir(parents=True, exist_ok=True)
    (rootfs(env, c) / "workspace" / "out.bin").write_text("inside")
    out = relay(srv, "put", "/workspace/out.bin", "b" * 32)
    assert out["exit_code"] == 0, out["output"]
    assert (fake_relay / "put-got.txt").read_text() == "inside"


def test_relay_is_503_while_restoring_and_cleans_its_token_file(stack, env, fake_relay):
    srv, m, c, _ = stack(boot=False)
    r = srv.post("/relay/jobs", {"mode": "recv", "repo": "owner/relay-x", "path": "/workspace/f", "token": "t" * 30,
                                 "request_id": "c" * 32})
    assert r.status_code == 503 and r.json()["error"] == "restoring"
    assert not list((env["tmp"] / "jobs").glob("relay-token-*"))


# --------------------------------------------------------------------------- legacy mode is unchanged
def test_legacy_app_has_no_session_routes(tmp_path, monkeypatch):
    monkeypatch.setattr(agent, "JOBS_DIR", tmp_path / "jobs")
    monkeypatch.setattr(agent, "ACTIVITY_FILE", tmp_path / "activity")
    srv = Served(agent.create_app(TOKEN, str(tmp_path)))
    try:
        assert srv.post("/session/save").status_code == 404
        assert "session" not in srv.get("/status").json()
        out = srv.post("/jobs", {"command": "echo ok"})
        assert out.status_code == 200
    finally:
        srv.stop()
