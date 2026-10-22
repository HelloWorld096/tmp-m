"""keepalive.sh with a persistent session attached: stop request, wait for the final save, early lifetime limit."""
import os
import subprocess
import threading
import time
from pathlib import Path

KEEPALIVE = Path(__file__).with_name("keepalive.sh")
DOWN = "http://127.0.0.1:1/"          # nothing listens there


def _run(tmp_path, *, session=True, activity_age=1, pid=None, health=DOWN, extra=None):
    flags = tmp_path / "flags"
    flags.mkdir(exist_ok=True)
    if session:
        (flags / "vm-session-active").write_text("s-1")
    act = tmp_path / "activity"
    act.write_text("")
    old = time.time() - activity_age
    os.utime(act, (old, old))
    pidfile = tmp_path / "tunnel.pid"
    if pid is not None:
        pidfile.write_text(str(pid))
    env = {**os.environ, "VM_ACTIVITY_FILE": str(act), "VM_TUNNEL_PID_FILE": str(pidfile), "VM_FLAGS_DIR": str(flags),
           "VM_AGENT_HEALTH_URL": health, "VM_POLL_SECONDS": "0.2", "VM_IDLE_SECONDS": "100000", "VM_DEAD_CHECKS": "3",
           "VM_DEAD_STALE_SECONDS": "100000", "VM_START_FILE": str(tmp_path / "none"), **(extra or {})}
    p = subprocess.Popen(["bash", str(KEEPALIVE)], env=env, stdout=subprocess.PIPE, text=True)
    return p, flags


def _done(p, seconds):
    try:
        p.wait(timeout=seconds)
        return True
    except subprocess.TimeoutExpired:
        p.kill()
        return False


def _wait_for(path: Path, seconds=8.0):
    end = time.time() + seconds
    while time.time() < end:
        if path.exists():
            return True
        time.sleep(0.05)
    return False


def _alive_pid(tmp_path):
    s = subprocess.Popen(["sleep", "60"])
    return s


def test_idle_session_asks_for_a_save_and_waits_for_it(tmp_path):
    s = _alive_pid(tmp_path)
    try:
        p, flags = _run(tmp_path, pid=s.pid, activity_age=500, extra={"VM_IDLE_SECONDS": "300"})
        assert _wait_for(flags / "vm-stopping"), "idle must request a final save, not end the run"
        assert "Idle timeout" in (flags / "vm-stopping").read_text()
        time.sleep(1.0)
        assert p.poll() is None, "the run must stay open until the agent reports it saved"
        (flags / "vm-saved").write_text("ok")
        assert _done(p, 5)
        assert p.returncode == 0
        assert "Final session save finished (ok)" in p.stdout.read()
    finally:
        s.kill()


def test_a_plain_vm_still_ends_immediately_when_idle(tmp_path):
    p, flags = _run(tmp_path, session=False, activity_age=500, extra={"VM_IDLE_SECONDS": "300"})
    assert _done(p, 10)
    assert not (flags / "vm-stopping").exists()
    assert "Idle timeout" in p.stdout.read()


def test_stop_now_flag_ends_the_run_at_any_time(tmp_path):
    s = _alive_pid(tmp_path)
    try:
        p, flags = _run(tmp_path, pid=s.pid)
        time.sleep(0.8)
        assert p.poll() is None
        (flags / "vm-stop-now").write_text("stopped")
        assert _done(p, 5)
        assert "asked the run to end (stopped)" in p.stdout.read()
    finally:
        s.kill()


def test_a_save_that_never_finishes_cannot_hold_the_run_forever(tmp_path):
    s = _alive_pid(tmp_path)
    try:
        p, flags = _run(tmp_path, pid=s.pid, activity_age=500,
                        extra={"VM_IDLE_SECONDS": "300", "VM_SAVE_WAIT_SECONDS": "2"})
        assert _done(p, 12)
        assert "did not finish" in p.stdout.read()
        assert (flags / "vm-stopping").exists()
    finally:
        s.kill()


def test_session_vm_starts_saving_before_the_hard_lifetime_limit(tmp_path):
    start = tmp_path / "started"
    start.write_text(str(int(time.time()) - 60))
    s = _alive_pid(tmp_path)
    try:
        # hard limit 100 s, 40 s reserve: a session must begin its save after 60 s, not after 100 s
        p, flags = _run(tmp_path, pid=s.pid, extra={"VM_MAX_SECONDS": "100", "VM_SESSION_RESERVE_SECONDS": "40",
                                                    "VM_START_FILE": str(start)})
        assert _wait_for(flags / "vm-stopping")
        assert "Maximum VM lifetime" in (flags / "vm-stopping").read_text()
        assert p.poll() is None
        (flags / "vm-saved").write_text("ok")
        assert _done(p, 5)
    finally:
        s.kill()


def test_the_hard_limit_still_wins_over_a_running_save(tmp_path):
    start = tmp_path / "started"
    start.write_text(str(int(time.time()) - 98))
    s = _alive_pid(tmp_path)
    try:
        p, flags = _run(tmp_path, pid=s.pid, extra={"VM_MAX_SECONDS": "100", "VM_SESSION_RESERVE_SECONDS": "40",
                                                    "VM_START_FILE": str(start), "VM_SAVE_WAIT_SECONDS": "1000"})
        assert _done(p, 10), "GitHub cuts the job at its limit; keepalive must not wait past it"
        out = p.stdout.read()
        assert "Maximum VM lifetime reached." in out
    finally:
        s.kill()


def test_a_dead_agent_ends_the_run_even_while_a_save_is_requested(tmp_path):
    # idle -> stopping requested, but agent and tunnel are gone: nothing can save, so do not wait 25 minutes
    p, flags = _run(tmp_path, pid=2**22 + 999, activity_age=500,
                    extra={"VM_IDLE_SECONDS": "300", "VM_DEAD_STALE_SECONDS": "120", "VM_SAVE_WAIT_SECONDS": "1000"})
    assert _done(p, 15)
    assert "replacement" in p.stdout.read()


def test_without_a_session_the_stop_flags_are_never_written(tmp_path):
    s = _alive_pid(tmp_path)
    try:
        p, flags = _run(tmp_path, session=False, pid=s.pid, activity_age=1)
        time.sleep(1.0)
        assert not (flags / "vm-stopping").exists()
        assert p.poll() is None
        p.kill()
    finally:
        s.kill()
