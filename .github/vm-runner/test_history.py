"""History database + replay: every /jobs attempt is recorded (refusals too), outside the container, and a replay is an
ordinary submission with its own job id."""
import os
import socket
import threading
import time

import httpx
import pytest
import uvicorn

import agent
import vm_history

TOKEN = "history-token-0123456789"


def H():
    return {"Authorization": f"Bearer {TOKEN}"}


@pytest.fixture
def srv(tmp_path, monkeypatch):
    monkeypatch.setattr(agent, "JOBS_DIR", tmp_path / "jobs")
    monkeypatch.setattr(agent, "ACTIVITY_FILE", tmp_path / "activity")
    (tmp_path / "ws").mkdir()
    hist = vm_history.HistoryStore(tmp_path / "hostonly" / "history.db")
    app = agent.create_app(TOKEN, str(tmp_path / "ws"), None, hist)
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    threading.Thread(target=server.run, daemon=True).start()
    url = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            httpx.get(url + "/health", timeout=1); break
        except Exception:
            time.sleep(0.05)
    yield url, hist, tmp_path
    server.should_exit = True


def post(url, body):
    return httpx.post(url + "/jobs", json=body, headers=H(), timeout=10)


def finish(url, jid, wait=20):
    return httpx.get(f"{url}/jobs/{jid}", params={"wait": wait}, headers=H(), timeout=wait + 10).json()


def test_attempt_and_outcome_are_recorded(srv):
    url, hist, _ = srv
    jid = post(url, {"command": "echo recorded; exit 3"}).json()["job_id"]
    finish(url, jid)
    time.sleep(0.3)
    row = hist.get(jid)
    assert row["command"] == "echo recorded; exit 3" and row["kind"] == "run" and row["outcome"] == "accepted"
    assert row["status"] == "finished" and row["exit_code"] == 3 and "recorded" in row["output_tail"]


def test_rejected_attempt_gets_an_id_and_can_be_replayed(srv):
    url, hist, _ = srv
    first = post(url, {"command": "sleep 3"}).json()["job_id"]
    busy = post(url, {"command": "echo later"})
    assert busy.status_code == 409
    rejected = [r for r in hist.recent(10) if r["outcome"] == "rejected"]
    assert len(rejected) == 1 and rejected[0]["reject_reason"] == "busy" and rejected[0]["command"] == "echo later"
    httpx.post(url + "/cancel", json={}, headers=H(), timeout=20)
    finish(url, first)
    rep = post(url, {"replay_of": rejected[0]["job_id"]})
    assert rep.status_code == 200 and rep.json()["replay_of"] == rejected[0]["job_id"]
    out = finish(url, rep.json()["job_id"])
    assert out["status"] == "finished" and "later" in out["output"]
    assert rep.json()["job_id"] != rejected[0]["job_id"]
    time.sleep(0.3)
    row = hist.get(rep.json()["job_id"])
    assert row["kind"] == "replay" and row["replay_of"] == rejected[0]["job_id"] and row["exit_code"] == 0


def test_replay_of_unknown_id_is_404_and_nothing_runs(srv):
    url, hist, _ = srv
    r = post(url, {"replay_of": "0123456789abcdef"})
    assert r.status_code == 404
    assert hist.count() == 0


def test_replay_with_a_supplied_command_records_the_origin(srv):
    """The Worker queues a command while the VM is moving and replays it later under its own q- id."""
    url, hist, _ = srv
    r = post(url, {"command": "echo queued-cmd", "replay_of": "q-0123456789abcdef"})
    assert r.status_code == 200
    assert "queued-cmd" in finish(url, r.json()["job_id"])["output"]
    time.sleep(0.3)
    assert hist.get(r.json()["job_id"])["replay_of"] == "q-0123456789abcdef"


def test_bad_replay_id_shape_is_rejected(srv):
    url, _, _ = srv
    assert post(url, {"replay_of": "../../etc/passwd"}).status_code == 422
    assert post(url, {}).status_code == 422


def test_history_endpoints_list_newest_first_and_shorten_commands(srv):
    url, hist, _ = srv
    long_cmd = "echo " + "a" * 500
    a = post(url, {"command": long_cmd}).json()["job_id"]; finish(url, a)
    b = post(url, {"command": "echo b"}).json()["job_id"]; finish(url, b)
    time.sleep(0.3)
    lst = httpx.get(url + "/history?limit=10", headers=H()).json()
    assert [j["job_id"] for j in lst["jobs"]] == [b, a] and lst["total"] == 2
    assert lst["jobs"][1]["command"].endswith("...") and lst["jobs"][1]["command_chars"] == len(long_cmd)
    full = httpx.get(f"{url}/history/{a}", headers=H()).json()
    assert full["command"] == long_cmd
    assert httpx.get(url + "/history").status_code == 401
    assert httpx.get(url + "/history/zzz", headers=H()).status_code == 404


def test_history_database_is_not_inside_the_jobs_directory(srv):
    """The container sees only JOBS_DIR; the database must be outside it and private."""
    _, hist, tmp = srv
    assert agent.JOBS_DIR not in hist.path.parents
    assert oct(hist.path.stat().st_mode & 0o777) == "0o600"
    assert oct(hist.path.parent.stat().st_mode & 0o777) == "0o700"


def test_long_commands_up_to_60000_chars_are_accepted(srv):
    url, _, _ = srv
    cmd = "true # " + "x" * 59_000
    r = post(url, {"command": cmd})
    assert r.status_code == 200
    finish(url, r.json()["job_id"])
    assert post(url, {"command": "x" * 60_001}).status_code == 422


def test_running_rows_become_interrupted_when_the_agent_restarts(tmp_path):
    h = vm_history.HistoryStore(tmp_path / "h.db")
    h.record_attempt("aaaaaaaaaaaaaaaa", "sleep 99", 60)
    assert h.get("aaaaaaaaaaaaaaaa")["status"] == "running"
    agent.JobManager(str(tmp_path), None, h)
    assert h.get("aaaaaaaaaaaaaaaa")["status"] == "interrupted"


def test_export_import_round_trip_keeps_rejections_and_marks_running(tmp_path):
    a = vm_history.HistoryStore(tmp_path / "a.db")
    a.record_attempt("1111111111111111", "echo one", 10)
    a.finish("1111111111111111", "finished", 0, "/w", "one\n")
    a.record_attempt("2222222222222222", "echo two", 10, rejected="capturing")
    a.record_attempt("3333333333333333", "sleep 9", 10)
    b = vm_history.HistoryStore(tmp_path / "b.db")
    assert b.import_bytes(a.export_bytes()) == 3
    assert b.get("2222222222222222")["reject_reason"] == "capturing"
    assert b.get("3333333333333333")["status"] == "interrupted"
    with pytest.raises(vm_history.HistoryError):
        b.import_bytes(b"not a database")
