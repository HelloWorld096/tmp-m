"""Tests for agent.py. Starts a real uvicorn server so event-loop behaviour is
exercised for real (a TestClient would hide the blocking-handler bug).

Run:  pip install -r requirements-dev.txt && pytest -q
"""
import os
import socket
import threading
import time

import httpx
import pytest
import uvicorn

TOKEN = "test-token-0123456789"


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    base = tmp_path_factory.mktemp("vm")
    os.environ["VM_ACTIVITY_FILE"] = str(base / "activity")
    os.environ["VM_JOBS_DIR"] = str(base / "jobs")
    os.environ["SESSION_TOKEN"] = "must-not-leak"          # scrubbing test
    os.environ["ACTIONS_RUNTIME_TOKEN"] = "must-not-leak"  # scrubbing test
    import importlib
    import agent
    importlib.reload(agent)

    workspace = base / "ws"
    workspace.mkdir()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    cfg = uvicorn.Config(agent.create_app(TOKEN, str(workspace)), host="127.0.0.1",
                         port=port, log_level="error")
    srv = uvicorn.Server(cfg)
    t = threading.Thread(target=srv.run, daemon=True)
    t.start()
    for _ in range(100):
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=1)
            break
        except Exception:
            time.sleep(0.1)
    yield {"url": f"http://127.0.0.1:{port}", "workspace": str(workspace), "agent": agent,
           "activity": base / "activity"}
    srv.should_exit = True


def H(extra=None):
    return {"Authorization": f"Bearer {TOKEN}", **(extra or {})}


def run(server, command, timeout_seconds=None, wait=30):
    body = {"command": command}
    if timeout_seconds:
        body["timeout_seconds"] = timeout_seconds
    r = httpx.post(f"{server['url']}/jobs", json=body, headers=H(), timeout=10)
    assert r.status_code == 200, r.text
    jid = r.json()["job_id"]
    s = httpx.get(f"{server['url']}/jobs/{jid}", params={"wait": wait}, headers=H(),
                  timeout=wait + 10)
    assert s.status_code == 200, s.text
    return s.json()


def wait_idle(server):
    """Make sure no job is left running from a previous test."""
    for _ in range(100):
        r = httpx.post(f"{server['url']}/jobs", json={"command": "true"}, headers=H())
        if r.status_code == 200:
            httpx.get(f"{server['url']}/jobs/{r.json()['job_id']}", params={"wait": 5}, headers=H())
            return
        time.sleep(0.2)


def test_health_is_open_and_jobs_require_auth(server):
    assert httpx.get(f"{server['url']}/health").status_code == 200
    assert httpx.post(f"{server['url']}/jobs", json={"command": "true"}).status_code == 401
    bad = httpx.post(f"{server['url']}/jobs", json={"command": "true"},
                     headers={"Authorization": "Bearer nope"})
    assert bad.status_code == 401
    assert httpx.get(f"{server['url']}/jobs/abc").status_code == 401


def test_basic_output_and_exit_code(server):
    r = run(server, "echo hello; echo oops >&2; exit 7")
    assert r["status"] == "finished"
    assert r["exit_code"] == 7              # exit N is honoured (EXIT trap)
    assert "hello" in r["output"] and "oops" in r["output"]


def test_cwd_persists_between_jobs(server):
    run(server, "mkdir -p sub && cd sub")
    r = run(server, "pwd")
    assert r["output"].strip().endswith("/sub")
    assert r["cwd"].endswith("/sub")


def test_environment_is_scrubbed(server):
    r = run(server, "env")
    assert "must-not-leak" not in r["output"]
    assert "SESSION_TOKEN" not in r["output"]


def test_stdin_is_closed(server):
    t0 = time.time()
    r = run(server, "cat; echo done")
    assert "done" in r["output"] and time.time() - t0 < 5


def test_background_process_does_not_block_response(server):
    t0 = time.time()
    r = run(server, "sleep 60 & echo started")
    assert r["status"] == "finished" and "started" in r["output"]
    assert time.time() - t0 < 5          # old agent would hang until timeout
    os.system("pkill -f '[s]leep 60'")


def test_health_responds_while_job_runs_and_second_job_is_rejected(server):
    wait_idle(server)
    jid = httpx.post(f"{server['url']}/jobs", json={"command": "sleep 20"}, headers=H()).json()["job_id"]
    # long-poll in a thread (this would starve the event loop in a blocking agent)
    poller = threading.Thread(
        target=lambda: httpx.get(f"{server['url']}/jobs/{jid}", params={"wait": 6}, headers=H(), timeout=15))
    poller.start()
    time.sleep(0.5)
    t0 = time.time()
    assert httpx.get(f"{server['url']}/health", timeout=3).status_code == 200
    assert time.time() - t0 < 1.0
    busy = httpx.post(f"{server['url']}/jobs", json={"command": "true"}, headers=H())
    assert busy.status_code == 409 and busy.json()["job_id"] == jid
    c = httpx.post(f"{server['url']}/jobs/{jid}/cancel", headers=H(), timeout=20).json()
    assert c["status"] == "cancelled" and c["exit_code"] == 130
    poller.join()


def test_timeout_kills_whole_process_group(server):
    wait_idle(server)
    r = run(server, "sleep 300 & sleep 300; echo never", timeout_seconds=1)
    assert r["status"] == "timed_out" and r["exit_code"] == 124
    time.sleep(0.5)
    assert os.system("pgrep -f '[s]leep 300' >/dev/null") != 0   # children are gone too


def test_running_job_reports_partial_output_then_finishes(server):
    wait_idle(server)
    jid = httpx.post(f"{server['url']}/jobs", json={"command": "echo first; sleep 2; echo second"},
                     headers=H()).json()["job_id"]
    early = httpx.get(f"{server['url']}/jobs/{jid}", params={"wait": 1}, headers=H()).json()
    assert early["status"] == "running" and "first" in early["output"]
    final = httpx.get(f"{server['url']}/jobs/{jid}", params={"wait": 10}, headers=H()).json()
    assert final["status"] == "finished" and "second" in final["output"]


def test_output_is_tail_truncated(server):
    r = run(server, "python3 - <<'EOF'\nprint('x' * 50000)\nprint('END')\nEOF")
    assert r["output_truncated"] is True
    assert len(r["output"]) <= 20_000 and "END" in r["output"]


def test_validation(server):
    assert httpx.post(f"{server['url']}/jobs", json={"command": "   "}, headers=H()).status_code == 400
    assert httpx.post(f"{server['url']}/jobs", json={"command": "x" * 20_001}, headers=H()).status_code == 422
    assert httpx.post(f"{server['url']}/jobs", json={"command": "true", "timeout_seconds": 99999},
                      headers=H()).status_code == 422
    assert httpx.get(f"{server['url']}/jobs/unknown", headers=H()).status_code == 404


def test_activity_file_is_touched(server):
    before = server["activity"].stat().st_mtime
    time.sleep(1.1)
    run(server, "true")
    assert server["activity"].stat().st_mtime > before


def test_submit_is_idempotent_for_a_repeated_request_id(server):
    """A retry after a lost reply must return the SAME job, not run the command twice or say 'busy'."""
    wait_idle(server)
    marker = os.path.join(server["workspace"], "ran-count")
    rid = "a" * 32
    body = {"command": f"echo x >> {marker}; sleep 2", "request_id": rid}
    first = httpx.post(f"{server['url']}/jobs", json=body, headers=H(), timeout=10).json()
    assert first["duplicate"] is False
    # while the first is STILL running a retry with the same id gets that job, not a 409
    again = httpx.post(f"{server['url']}/jobs", json=body, headers=H(), timeout=10)
    assert again.status_code == 200
    assert again.json()["job_id"] == first["job_id"] and again.json()["duplicate"] is True
    done = httpx.get(f"{server['url']}/jobs/{first['job_id']}", params={"wait": 20}, headers=H(), timeout=30).json()
    assert done["status"] == "finished"
    # ... and after it finished too
    third = httpx.post(f"{server['url']}/jobs", json=body, headers=H(), timeout=10).json()
    assert third["job_id"] == first["job_id"]
    assert open(marker).read().count("x") == 1  # executed exactly once


def test_different_request_ids_are_different_jobs_and_busy_still_applies(server):
    wait_idle(server)
    a = httpx.post(f"{server['url']}/jobs", json={"command": "sleep 2", "request_id": "b" * 32}, headers=H()).json()
    busy = httpx.post(f"{server['url']}/jobs", json={"command": "true", "request_id": "c" * 32}, headers=H())
    assert busy.status_code == 409 and busy.json()["job_id"] == a["job_id"]
    httpx.post(f"{server['url']}/jobs/{a['job_id']}/cancel", headers=H(), timeout=20)


def test_request_id_must_be_32_hex_chars(server):
    for bad in ("short", "z" * 32, "A" * 32, "a" * 33):
        r = httpx.post(f"{server['url']}/jobs", json={"command": "true", "request_id": bad}, headers=H())
        assert r.status_code == 422, bad


def test_pruning_forgets_request_ids(server):
    wait_idle(server)
    mgr_ids = []
    for i in range(agent_keep(server) + 2):
        rid = f"{i:032x}"
        mgr_ids.append(rid)
        r = httpx.post(f"{server['url']}/jobs", json={"command": "true", "request_id": rid}, headers=H()).json()
        httpx.get(f"{server['url']}/jobs/{r['job_id']}", params={"wait": 10}, headers=H(), timeout=20)
    # the oldest job has been pruned, so its request_id is forgotten and submits fresh
    r = httpx.post(f"{server['url']}/jobs", json={"command": "true", "request_id": mgr_ids[0]}, headers=H()).json()
    assert r["duplicate"] is False
    httpx.get(f"{server['url']}/jobs/{r['job_id']}", params={"wait": 10}, headers=H(), timeout=20)


def agent_keep(server):
    return server["agent"].KEEP_JOBS


# ---- kill switch ----------------------------------------------------------------------------------

def _alive(pattern: str, wait_gone: float = 0.0) -> bool:
    """True if a live (non-zombie) process matches `ps` args pattern; optionally wait for it to vanish."""
    deadline = time.time() + wait_gone
    while True:
        out = subprocess.run(["ps", "-eo", "stat,args"], capture_output=True, text=True).stdout
        alive = any(pattern in line and not line.startswith("Z") and "ps -eo" not in line
                    for line in out.splitlines())
        if not alive or time.time() >= deadline:
            return alive
        time.sleep(0.1)


def _post_cancel(server, body=None, auth=True):
    return httpx.post(f"{server['url']}/cancel", json=body, headers=H() if auth else {}, timeout=30)


def test_cancel_without_job_id_stops_a_looping_command_and_frees_the_agent(server):
    wait_idle(server)
    jid = httpx.post(f"{server['url']}/jobs", json={"command": "while true; do :; done"},
                     headers=H()).json()["job_id"]
    time.sleep(0.5)
    assert httpx.post(f"{server['url']}/jobs", json={"command": "true"}, headers=H()).status_code == 409
    c = _post_cancel(server).json()
    assert c["job_id"] == jid and c["status"] == "cancelled" and c["exit_code"] == 130
    nxt = run(server, "echo next")                      # the agent is free again straight away
    assert nxt["status"] == "finished" and "next" in nxt["output"]


def test_cancel_without_job_id_when_nothing_runs_is_a_harmless_idle_reply(server):
    wait_idle(server)
    r = _post_cancel(server)
    assert r.status_code == 200 and r.json()["status"] == "idle" and r.json()["job_id"] is None


def test_cancel_current_requires_auth(server):
    assert _post_cancel(server, auth=False).status_code == 401


def test_cancel_by_id_still_works_and_accepts_no_body(server):
    wait_idle(server)
    jid = httpx.post(f"{server['url']}/jobs", json={"command": "sleep 30"}, headers=H()).json()["job_id"]
    c = httpx.post(f"{server['url']}/jobs/{jid}/cancel", headers=H(), timeout=30).json()
    assert c["status"] == "cancelled" and c["background_killed"] == 0


def test_escaped_background_process_survives_cancel_but_not_kill_background(server):
    """A setsid daemon leaves the job's process group, so plain cancel must NOT touch it (nohup servers
    are meant to survive), while kill_background finds it through its inherited job marker."""
    wait_idle(server)
    tag = "sleep 4242"
    httpx.post(f"{server['url']}/jobs", json={"command": f"setsid {tag} >/dev/null 2>&1 & sleep 30"}, headers=H())
    time.sleep(1)
    assert _post_cancel(server).json()["status"] == "cancelled"
    time.sleep(0.5)
    assert _alive("sleep 4242"), "plain cancel must leave escaped daemons alone"
    r = _post_cancel(server, {"kill_background": True}).json()
    assert r["status"] == "idle" and r["background_killed"] >= 1
    assert not _alive("sleep 4242", wait_gone=5)


def test_job_marker_is_in_job_environment_but_not_inherited_from_the_agent(server):
    wait_idle(server)
    os.environ["VM_AGENT_JOB"] = "stale"
    try:
        r = run(server, 'echo "mark=$VM_AGENT_JOB"')
    finally:
        del os.environ["VM_AGENT_JOB"]
    assert f"mark={r['job_id']}" in r["output"]


# ---- keepalive.sh -----------------------------------------------------------------------------
import subprocess
from pathlib import Path

KEEPALIVE = Path(__file__).with_name("keepalive.sh")


def _keepalive(tmp_path, *, activity_age, health_url, pid, extra=None):
    act = tmp_path / "activity"
    act.write_text("")
    old = time.time() - activity_age
    os.utime(act, (old, old))
    pidfile = tmp_path / "tunnel.pid"
    if pid is not None:
        pidfile.write_text(str(pid))
    env = {
        **os.environ,
        "VM_ACTIVITY_FILE": str(act), "VM_TUNNEL_PID_FILE": str(pidfile),
        "VM_AGENT_HEALTH_URL": health_url, "VM_POLL_SECONDS": "0.2",
        "VM_IDLE_SECONDS": "100000", "VM_DEAD_CHECKS": "3", "VM_DEAD_STALE_SECONDS": "120",
        "VM_START_FILE": str(tmp_path / "no-such-start-file"),
        **(extra or {}),
    }
    return subprocess.Popen(["bash", str(KEEPALIVE)], env=env, stdout=subprocess.PIPE, text=True)


def _finishes(proc, seconds):
    try:
        proc.wait(timeout=seconds)
        return True
    except subprocess.TimeoutExpired:
        proc.kill()
        return False


def test_keepalive_exits_when_idle(tmp_path):
    p = _keepalive(tmp_path, activity_age=500, health_url="http://127.0.0.1:1/",
                   pid=None, extra={"VM_IDLE_SECONDS": "300"})
    assert _finishes(p, 10)
    assert "Idle timeout" in p.stdout.read()


def test_keepalive_exits_when_agent_and_tunnel_are_dead_and_idle(tmp_path):
    p = _keepalive(tmp_path, activity_age=200, health_url="http://127.0.0.1:1/", pid=2**22 + 12345)
    assert _finishes(p, 15), "a dead VM must release the concurrency group"
    assert "replacement" in p.stdout.read()


def test_keepalive_never_kills_a_busy_vm_even_if_health_is_failing(tmp_path):
    """A CPU-starved build can make /health slow. Fresh activity (a job is running) must keep the VM up."""
    sleeper = subprocess.Popen(["sleep", "60"])
    try:
        p = _keepalive(tmp_path, activity_age=1, health_url="http://127.0.0.1:1/", pid=sleeper.pid)
        assert not _finishes(p, 4)
    finally:
        sleeper.kill()


def test_keepalive_stays_up_while_everything_is_healthy(tmp_path, server):
    sleeper = subprocess.Popen(["sleep", "60"])
    try:
        p = _keepalive(tmp_path, activity_age=500, health_url=f"{server['url']}/health", pid=sleeper.pid)
        assert not _finishes(p, 4)  # stale activity alone is not "dead" while agent and tunnel are up
    finally:
        sleeper.kill()


def test_keepalive_ends_at_the_maximum_lifetime_even_while_a_job_keeps_the_vm_busy(tmp_path):
    start = tmp_path / "started"
    start.write_text(str(int(time.time()) - 200))
    sleeper = subprocess.Popen(["sleep", "60"])
    try:
        # activity is fresh (a job is running) and everything is healthy, yet the lifetime is over
        p = _keepalive(tmp_path, activity_age=1, health_url="http://127.0.0.1:1/", pid=sleeper.pid,
                       extra={"VM_MAX_SECONDS": "100", "VM_START_FILE": str(start)})
        assert _finishes(p, 10)
        assert "Maximum VM lifetime" in p.stdout.read()
    finally:
        sleeper.kill()


def test_keepalive_stays_up_before_the_maximum_lifetime(tmp_path):
    start = tmp_path / "started"
    start.write_text(str(int(time.time())))
    sleeper = subprocess.Popen(["sleep", "60"])
    try:
        p = _keepalive(tmp_path, activity_age=1, health_url="http://127.0.0.1:1/", pid=sleeper.pid,
                       extra={"VM_MAX_SECONDS": "100", "VM_START_FILE": str(start)})
        assert not _finishes(p, 3)
    finally:
        sleeper.kill()


def test_keepalive_defaults_are_60_minute_idle_and_6_hour_maximum():
    src = KEEPALIVE.read_text()
    assert 'VM_IDLE_SECONDS:-3600' in src and 'VM_MAX_SECONDS:-21600' in src


def test_unauthenticated_requests_get_401_before_the_body_is_validated(server):
    """Auth runs as a dependency, so a bad body without credentials must not leak schema details (422)."""
    r = httpx.post(f"{server['url']}/jobs", json={"nonsense": True})
    assert r.status_code == 401 and "command" not in r.text
    r = httpx.post(f"{server['url']}/jobs", content=b"{not json", headers={"Content-Type": "application/json"})
    assert r.status_code == 401
    # With credentials the same bad body is a normal validation error.
    assert httpx.post(f"{server['url']}/jobs", json={"nonsense": True}, headers=H()).status_code == 422


def test_exec_reports_the_real_exit_code(server):
    """`exec` replaces the shell, so the EXIT trap never runs. The exit status must still be right."""
    wait_idle(server)
    ok = run(server, "exec true")
    assert ok["status"] == "finished" and ok["exit_code"] == 0
    bad = run(server, "exec sh -c 'exit 3'")
    assert bad["exit_code"] == 3


def test_overriding_the_exit_trap_keeps_the_exit_code(server):
    wait_idle(server)
    r = run(server, "trap 'echo bye' EXIT; exit 4")
    assert "bye" in r["output"] and r["exit_code"] == 4


def test_cancelling_an_already_finished_job_does_not_relabel_it(server):
    wait_idle(server)
    r = run(server, "echo done")
    jid = r["job_id"]
    c = httpx.post(f"{server['url']}/jobs/{jid}/cancel", headers=H(), timeout=30).json()
    assert c["status"] == "finished" and c["exit_code"] == 0
