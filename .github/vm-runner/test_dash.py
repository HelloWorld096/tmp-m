"""The dashboard's own terminal (a lane beside the MCP lane), the paged history and the paged output."""
import socket
import sqlite3
import threading
import time

import httpx
import pytest
import uvicorn

import agent
import vm_history

TOKEN = "dash-token-0123456789"


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


def mcp(url, command, **kw):
    return httpx.post(url + "/jobs", json={"command": command, **kw}, headers=H(), timeout=10)


def dash(url, command, **kw):
    return httpx.post(url + "/dash/jobs", json={"command": command, **kw}, headers=H(), timeout=10)


def wait(url, jid, secs=20):
    return httpx.get(f"{url}/jobs/{jid}", params={"wait": secs}, headers=H(), timeout=secs + 10).json()


def test_dash_runs_beside_a_running_mcp_command(srv):
    url, _, _ = srv
    slow = mcp(url, "sleep 3; echo mcp-done").json()["job_id"]
    t0 = time.time()
    d = dash(url, "echo dash-done", description="  parallel\n check ")
    assert d.status_code == 200 and d.json()["lane"] == "dash"
    out = wait(url, d.json()["job_id"])
    assert out["status"] == "finished" and "dash-done" in out["output"]
    assert time.time() - t0 < 2.5, "the dashboard command waited for the MCP command"
    assert mcp(url, "echo second").status_code == 409                    # the MCP lane is still one at a time
    assert "mcp-done" in wait(url, slow)["output"]


def test_mcp_is_not_blocked_by_dash_commands(srv):
    url, _, _ = srv
    dj = dash(url, "sleep 3").json()["job_id"]
    t0 = time.time()
    r = mcp(url, "echo mcp-free")
    assert r.status_code == 200
    assert "mcp-free" in wait(url, r.json()["job_id"])["output"] and time.time() - t0 < 2.5
    httpx.post(f"{url}/jobs/{dj}/cancel", headers=H(), timeout=15)


def test_dash_lane_cap_and_refusal_is_recorded(srv):
    url, hist, _ = srv
    ids = [dash(url, "sleep 5").json()["job_id"] for _ in range(agent.DASH_MAX_PARALLEL)]
    r = dash(url, "echo no-room", description="over the cap")
    assert r.status_code == 429 and r.json() == {"error": "too_many", "limit": agent.DASH_MAX_PARALLEL}
    rows = httpx.get(url + "/history", params={"source": "dash", "limit": 10}, headers=H()).json()["jobs"]
    refused = [x for x in rows if x["reject_reason"] == "too_many"]
    assert len(refused) == 1 and refused[0]["description"] == "over the cap" and refused[0]["source"] == "dash"
    for i in ids:
        httpx.post(f"{url}/jobs/{i}/cancel", headers=H(), timeout=15)


def test_a_dash_cd_does_not_move_the_mcp_directory(srv):
    url, _, tmp = srv
    (tmp / "ws" / "sub").mkdir()
    assert wait(url, dash(url, "cd sub && pwd").json()["job_id"])["status"] == "finished"
    out = wait(url, mcp(url, "pwd").json()["job_id"])
    assert out["output"].strip() == str(tmp / "ws")


def test_dash_jobs_do_not_evict_mcp_jobs(srv):
    url, _, _ = srv
    keep = mcp(url, "echo keep-me").json()["job_id"]
    wait(url, keep)
    for _ in range(agent.KEEP_DASH_JOBS + 5):
        wait(url, dash(url, "true").json()["job_id"])
    assert "keep-me" in wait(url, keep)["output"]


def test_description_is_cleaned_and_listed(srv):
    url, hist, _ = srv
    jid = mcp(url, "echo hi", description="line one\nline two\x00\t" + "x" * 300).json()["job_id"]
    wait(url, jid); time.sleep(0.3)
    row = hist.get(jid)
    assert "\n" not in row["description"] and "\x00" not in row["description"] and len(row["description"]) <= 200
    assert row["source"] == "mcp"


def test_history_pages_by_cursor_and_filters(srv):
    url, _, _ = srv
    for i in range(7):
        wait(url, mcp(url, f"exit {i % 2}", description=f"cmd {i}").json()["job_id"])
    time.sleep(0.3)
    p1 = httpx.get(url + "/history", params={"limit": 3}, headers=H()).json()
    assert [j["description"] for j in p1["jobs"]] == ["cmd 6", "cmd 5", "cmd 4"] and p1["next_before"] is not None
    p2 = httpx.get(url + "/history", params={"limit": 3, "before": p1["next_before"]}, headers=H()).json()
    assert [j["description"] for j in p2["jobs"]] == ["cmd 3", "cmd 2", "cmd 1"]
    p3 = httpx.get(url + "/history", params={"limit": 3, "before": p2["next_before"]}, headers=H()).json()
    assert [j["description"] for j in p3["jobs"]] == ["cmd 0"] and p3["next_before"] is None
    failed = httpx.get(url + "/history", params={"status": "failed", "limit": 20}, headers=H()).json()["jobs"]
    assert {j["exit_code"] for j in failed} == {1} and len(failed) == 3
    ok = httpx.get(url + "/history", params={"status": "ok", "limit": 20}, headers=H()).json()["jobs"]
    assert len(ok) == 4
    found = httpx.get(url + "/history", params={"q": "cmd 4"}, headers=H()).json()["jobs"]
    assert [j["description"] for j in found] == ["cmd 4"]
    assert httpx.get(url + "/history", params={"q": "%"}, headers=H()).json()["jobs"] == []     # LIKE wildcards are literal


def test_output_pages_and_never_splits_a_character(srv):
    url, _, _ = srv
    jid = mcp(url, "python3 -c \"print('é' * 5000)\"").json()["job_id"]
    wait(url, jid); time.sleep(0.3)
    got, off, pages = "", 0, 0
    while True:
        r = httpx.get(f"{url}/history/{jid}/output", params={"offset": off, "limit": 1001}, headers=H()).json()
        got += r["text"]; off = r["next_offset"]; pages += 1
        assert "\ufffd" not in r["text"]
        if r["complete"]:
            break
        assert pages < 50
    assert got.strip() == "é" * 5000 and pages > 5 and r["retained"] is True


def test_following_a_running_job(srv):
    url, _, _ = srv
    jid = dash(url, "echo one; sleep 1; echo two; sleep 1; echo three").json()["job_id"]
    seen, off, done = "", 0, False
    for _ in range(40):
        r = httpx.get(f"{url}/history/{jid}/output", params={"offset": off}, headers=H()).json()
        seen += r["text"]; off = r["next_offset"]
        if r["complete"]:
            done = True; break
        time.sleep(0.25)
    assert done and seen.split() == ["one", "two", "three"]


def test_history_after_retention_falls_back_to_the_saved_tail(srv):
    url, hist, _ = srv
    jid = mcp(url, "echo old-output").json()["job_id"]
    wait(url, jid); time.sleep(0.3)
    for _ in range(agent.KEEP_JOBS + 3):
        wait(url, mcp(url, "true").json()["job_id"])
    r = httpx.get(f"{url}/history/{jid}/output", headers=H()).json()
    assert r["retained"] is False and "old-output" in r["text"]


def test_a_version_1_database_is_upgraded_and_a_v1_snapshot_imports(tmp_path):
    old = tmp_path / "old.db"
    db = sqlite3.connect(old)
    db.executescript("""CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE jobs (seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL UNIQUE, request_id TEXT, kind TEXT NOT NULL,
      replay_of TEXT, command TEXT NOT NULL, timeout_seconds INTEGER, outcome TEXT NOT NULL, reject_reason TEXT, status TEXT NOT NULL,
      exit_code INTEGER, cwd TEXT, submitted_at REAL NOT NULL, ended_at REAL, output_tail TEXT);
    INSERT INTO meta VALUES ('schema_version','1');
    INSERT INTO jobs(job_id,kind,command,outcome,status,exit_code,submitted_at,output_tail) VALUES ('aaaa','run','echo old','accepted','finished',0,1.0,'old');
    INSERT INTO jobs(job_id,kind,command,outcome,status,submitted_at) VALUES ('bbbb','run','sleep 9','accepted','running',2.0);""")
    db.commit(); db.close()
    # 1) opening an old database upgrades it in place
    up = vm_history.HistoryStore(old)
    assert up.get("aaaa")["source"] == "mcp" and up.get("aaaa")["description"] is None
    # 2) a version-1 snapshot is imported into a current store; the running row becomes 'interrupted'
    fresh = vm_history.HistoryStore(tmp_path / "new" / "history.db")
    fresh.record_attempt("cccc", "echo local", 5, description="kept", source="dash")
    raw = (tmp_path / "v1src.db")
    src = sqlite3.connect(raw)
    src.executescript("""CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE jobs (seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL UNIQUE, request_id TEXT, kind TEXT NOT NULL,
      replay_of TEXT, command TEXT NOT NULL, timeout_seconds INTEGER, outcome TEXT NOT NULL, reject_reason TEXT, status TEXT NOT NULL,
      exit_code INTEGER, cwd TEXT, submitted_at REAL NOT NULL, ended_at REAL, output_tail TEXT);
    INSERT INTO meta VALUES ('schema_version','1');
    INSERT INTO jobs(job_id,kind,command,outcome,status,submitted_at) VALUES ('dddd','run','sleep 9','accepted','running',2.0);""")
    src.commit(); src.close()
    n = fresh.import_bytes(raw.read_bytes())
    assert n == 1 and fresh.get("dddd")["status"] == "interrupted" and fresh.get("dddd")["source"] == "mcp"
    assert fresh.get("cccc")["description"] == "kept" and fresh.get("cccc")["source"] == "dash"
    assert [r["job_id"] for r in fresh.page()[0]] == ["cccc", "dddd"]
