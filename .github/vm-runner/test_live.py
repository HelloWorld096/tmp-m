"""The live stream: a hand-written RFC 6455 client checked against the RFC's own examples and a real WebSocket server."""
import asyncio
import json
import socket
import threading
import time

import httpx
import pytest
import uvicorn
import websockets

import agent
import vm_activity
import vm_history
import vm_live

TOKEN = "live-token-0123456789"


def H():
    return {"Authorization": f"Bearer {TOKEN}"}


# ---- codec -------------------------------------------------------------------------------------------------------
def test_codec_matches_the_rfc_examples():
    # RFC 6455 section 5.7: a single-frame masked text message "Hello"
    r = vm_live.FrameReader()
    assert r.feed(bytes.fromhex("818537fa213d7f9f4d5158")) == [(1, b"Hello")]
    # and an unmasked one (what a server sends), arriving one byte at a time
    r = vm_live.FrameReader()
    out = []
    for b in bytes.fromhex("810548656c6c6f"):
        out += r.feed(bytes([b]))
    assert out == [(1, b"Hello")]
    # fragmented: "Hel" + "lo"
    r = vm_live.FrameReader()
    assert r.feed(bytes.fromhex("010348656c") + bytes.fromhex("80026c6f")) == [(1, b"Hello")]
    # a ping may arrive between the fragments
    r = vm_live.FrameReader()
    assert r.feed(bytes.fromhex("010348656c") + bytes.fromhex("890470696e67") + bytes.fromhex("80026c6f")) == [(9, b"ping"), (1, b"Hello")]


@pytest.mark.parametrize("n", [0, 5, 125, 126, 65535, 65536, 200_000])
def test_codec_roundtrip_all_length_forms(n):
    payload = bytes(i % 251 for i in range(n))
    for mask in (True, False):
        assert vm_live.FrameReader().feed(vm_live.encode_frame(2, payload, mask=mask)) == [(2, payload)]


def test_oversized_message_is_refused():
    with pytest.raises(vm_live.WsError):
        vm_live.FrameReader(max_message=100).feed(vm_live.encode_frame(1, b"x" * 101, mask=False))


# ---- activity phases ---------------------------------------------------------------------------------------------
def test_activity_names_the_phase_and_counts_chunks():
    a = vm_activity.Activity()
    a.set("capturing the filesystem state")
    assert a.snapshot()["phase"] == "paused"
    a.set("uploading 9 chunk(s), 1288490188 bytes")
    s = a.snapshot()
    assert (s["phase"], s["done"], s["total"], s["bytes_total"]) == ("uploading", 0, 9, 1288490188)
    a.set("uploading snapshot-3.part003 (3/9)")
    s = a.snapshot()
    assert (s["phase"], s["done"], s["total"], s["bytes_total"]) == ("uploading", 2, 9, 1288490188)   # total bytes carried forward
    a.set("committing"); a.set("saved")
    assert a.snapshot()["phase"] == "saved" and [t["phase"] for t in a.snapshot()["timeline"]][-3:] == ["uploading", "committing", "saved"]
    a.set("downloading chunk-1 (1/4)")
    assert a.snapshot()["phase"] == "downloading" and a.snapshot()["total"] == 4
    a.set("something new")
    assert a.snapshot()["phase"] == "other"


# ---- a real stream -----------------------------------------------------------------------------------------------
class Hub:
    """A WebSocket server that plays the Worker's LiveHub: records what the agent sends, tells it how many watch."""

    def __init__(self, refuse=False):
        self.messages, self.headers, self.path, self.refuse = [], None, None, refuse
        self.sockets, self.loop = [], None
        s = socket.socket(); s.bind(("127.0.0.1", 0)); self.port = s.getsockname()[1]; s.close()
        self.ready = threading.Event()
        threading.Thread(target=self._thread, daemon=True).start()
        assert self.ready.wait(5)

    def _thread(self):
        async def handler(ws):
            self.path, self.headers = ws.request.path, {k.lower(): v for k, v in ws.request.headers.items()}
            self.sockets.append(ws)
            await ws.send(json.dumps({"type": "subs", "n": 1}))
            async for m in ws:
                self.messages.append(json.loads(m))

        async def process(conn, request):
            if self.refuse:
                return conn.respond(401, "no\n")

        async def main():
            self.loop = asyncio.get_running_loop()
            async with websockets.serve(handler, "127.0.0.1", self.port, process_request=process):
                self.ready.set()
                await asyncio.Future()
        try:
            asyncio.run(main())
        except Exception:
            pass

    def tell(self, obj):
        async def go():
            for ws in self.sockets:
                try:
                    await ws.send(json.dumps(obj))
                except Exception:
                    pass
        asyncio.run_coroutine_threadsafe(go(), self.loop).result(5)

    def wait_for(self, pred, secs=8):
        end = time.time() + secs
        while time.time() < end:
            for m in list(self.messages):
                if pred(m):
                    return m
            time.sleep(0.05)
        raise AssertionError(f"no matching message in {secs}s; got {self.messages[-3:]}")


@pytest.fixture
def agent_url(tmp_path, monkeypatch):
    monkeypatch.setattr(agent, "JOBS_DIR", tmp_path / "jobs")
    monkeypatch.setattr(agent, "ACTIVITY_FILE", tmp_path / "activity")
    monkeypatch.setenv("VM_SNAPSHOT_TEST", "1")
    monkeypatch.setenv("VM_LIVE_INTERVAL_S", "0.2")
    monkeypatch.setenv("VM_LIVE_IDLE_GRACE_S", "0.6")
    (tmp_path / "ws").mkdir()
    hist = vm_history.HistoryStore(tmp_path / "hostonly" / "history.db")
    servers = []

    def make(hub_port):
        if hub_port:
            monkeypatch.setenv("VM_BASE_URL", f"http://127.0.0.1:{hub_port}")
        else:
            monkeypatch.delenv("VM_BASE_URL", raising=False)
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
        servers.append(server)
        return url
    yield make
    for s in servers:
        s.should_exit = True


def test_stream_carries_rows_and_output_then_stops_when_nobody_watches(agent_url):
    hub = Hub()
    url = agent_url(hub.port)
    r = httpx.post(url + "/live/start", headers=H(), timeout=10)
    assert r.status_code == 200 and r.json()["started"] is True
    assert httpx.post(url + "/live/start", headers=H(), timeout=10).json()["started"] is False      # already streaming: no second socket
    first = hub.wait_for(lambda m: m.get("type") == "frame")
    assert hub.path == "/runner/live" and hub.headers["authorization"] == f"Bearer {TOKEN}"
    assert first["vm"]["busy"] is False and "seq" in first
    # a command from the dashboard lane while an MCP command runs: both rows and the output reach the viewer, in order
    jid = httpx.post(url + "/dash/jobs", json={"command": "echo hello-live; sleep 1; echo bye-live", "description": "live demo"}, headers=H(), timeout=10).json()["job_id"]
    row = hub.wait_for(lambda m: any(x["job_id"] == jid for x in m.get("rows", [])))
    mine = [x for x in row["rows"] if x["job_id"] == jid][0]
    assert mine["source"] == "dash" and mine["description"] == "live demo"
    text = ""
    end = time.time() + 8
    while time.time() < end and "bye-live" not in text:
        text = "".join(m["out"][jid]["text"] for m in hub.messages if jid in m.get("out", {}))
        time.sleep(0.1)
    assert text.split() == ["hello-live", "bye-live"]
    done = hub.wait_for(lambda m: any(x["job_id"] == jid and x["status"] == "finished" for x in m.get("rows", [])))
    assert done
    # frames are paced: at most one per interval, and an unchanged VM is not re-sent more often than the heartbeat
    n0 = len(hub.messages); time.sleep(1.0)
    assert len(hub.messages) - n0 <= 2
    # nobody watches any more: the agent closes its socket on its own
    hub.tell({"type": "subs", "n": 0})
    end = time.time() + 6
    while time.time() < end and httpx.get(url + "/live/status", headers=H(), timeout=5).json()["running"]:
        time.sleep(0.1)
    assert httpx.get(url + "/live/status", headers=H(), timeout=5).json()["running"] is False
    # and it can be started again
    assert httpx.post(url + "/live/start", headers=H(), timeout=10).json()["started"] is True
    httpx.post(url + "/live/stop", headers=H(), timeout=5)


def test_a_refused_handshake_is_reported_not_retried_forever(agent_url):
    hub = Hub(refuse=True)
    url = agent_url(hub.port)
    httpx.post(url + "/live/start", headers=H(), timeout=10)
    end = time.time() + 15
    st = {}
    while time.time() < end:
        st = httpx.get(url + "/live/status", headers=H(), timeout=5).json()
        if not st["running"]:
            break
        time.sleep(0.2)
    assert st["running"] is False and "refused" in (st["error"] or "")


def test_without_a_worker_address_the_stream_is_not_available(agent_url):
    url = agent_url(None)
    r = httpx.post(url + "/live/start", headers=H(), timeout=10)
    assert r.status_code == 501 and "Worker address" in r.json()["detail"]


def test_live_routes_need_the_token(agent_url):
    url = agent_url(None)
    assert httpx.post(url + "/live/start", timeout=5).status_code in (401, 403)
