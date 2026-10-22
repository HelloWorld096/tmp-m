"""Tests for vm_container: image names, `docker diff` parsing, capture/apply round trips and the job sweep.

A real Docker daemon is NOT used. fake_docker.py backs containers with plain directories; see its docstring for what
it does and does not emulate. A smoke test on a real runner is still required before trusting this.
"""
import os
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

os.environ["VM_SNAPSHOT_TEST"] = "1"
import vm_container as vc  # noqa: E402
import vm_snapshot as vs  # noqa: E402
from test_snapshot import MockGitHub, store, ok_fence, REPO  # noqa: E402,F401
from test_snapshot import gh  # noqa: E402,F401  (fixture)

FAKE = str(Path(__file__).with_name("fake_docker.py"))
IMG = "ubuntu:24.04"


@pytest.fixture
def env(tmp_path, monkeypatch):
    images, state = tmp_path / "images", tmp_path / "state"
    base = images / "ubuntu_24.04"
    (base / "etc").mkdir(parents=True)
    (base / "bin").mkdir()
    (base / "etc" / "os-release").write_text("NAME=Ubuntu\n")
    (base / "bin" / "tool").write_text("#!/bin/sh\necho tool\n")
    (base / "bin" / "keep").write_text("keep\n")
    # A wrapper we own, so this works wherever the repo is checked out (no reliance on the exec bit of fake_docker.py).
    wrapper = tmp_path / "docker"
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{FAKE}" "$@"\n')
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("VM_DOCKER", str(wrapper))
    monkeypatch.setenv("FAKE_DOCKER_IMAGES", str(images))
    monkeypatch.setenv("FAKE_DOCKER_STATE", str(state))
    # fake_docker reads these at import time in ITS process, which is spawned fresh each call
    return {"images": images, "state": state, "tmp": tmp_path}


def make(env, sid="abc123", image=IMG):
    jobs = env["tmp"] / "jobs"
    jobs.mkdir(exist_ok=True)
    c = vc.SessionContainer(sid, image, vc.Docker(), jobs_dir=jobs)
    return c


def rootfs(env, c):
    return env["state"] / "containers" / c.name / "rootfs"


# --------------------------------------------------------------------------- pure functions
@pytest.mark.parametrize("value,want", [
    (None, vc.DEFAULT_IMAGE), ("", vc.DEFAULT_IMAGE), ("  Ubuntu ", vc.DEFAULT_IMAGE),
    ("python:3.12-slim", "python:3.12-slim"), ("ghcr.io/owner/img:v1", "ghcr.io/owner/img:v1"),
    ("localhost:5000/team/app", "localhost:5000/team/app"),
    ("alpine@sha256:" + "a" * 64, "alpine@sha256:" + "a" * 64),
])
def test_normalize_image_accepts(value, want):
    assert vc.normalize_image(value) == want


@pytest.mark.parametrize("value", ["--privileged", "ubuntu; rm -rf /", "UPPER/case", "a b", "-v /:/host", "x" * 400,
                                   "img:", "img@sha256:short"])
def test_normalize_image_rejects(value):
    with pytest.raises(ValueError):
        vc.normalize_image(value)


def test_parse_diff_filters():
    text = "\n".join(["A /workspace/a.txt", "C /etc", "C /etc/hosts", "D /bin/tool", "A /proc/1", "A /tmp/vm-jobs/j1",
                      "A /tmp/vm-jobsX/ok", "garbage", "A relative", "X /nope", "", "C /.dockerenv"])
    changed, deleted = vc.parse_diff(text)
    assert changed == {"/workspace/a.txt", "/etc", "/tmp/vm-jobsX/ok"}
    assert deleted == ["/bin/tool"]


# --------------------------------------------------------------------------- lifecycle
def test_create_starts_container_and_detects_shell(env):
    c = make(env)
    c.create()
    assert (rootfs(env, c) / "workspace").is_dir()
    assert c.shell in ("bash", "sh")
    assert c.name == "vm-session-abc123"


def test_create_survives_pull_failure_when_image_is_cached(env, monkeypatch):
    monkeypatch.setenv("FAKE_DOCKER_PULL_FAIL", "1")
    make(env).create()


def test_create_fails_cleanly_when_image_is_missing(env):
    with pytest.raises(vc.DockerError) as e:
        make(env, image="nosuch/image:1").create()
    assert "could not pull image" in str(e.value)


def test_missing_docker_binary_is_a_clean_error(monkeypatch):
    monkeypatch.setenv("VM_DOCKER", "/nonexistent/docker")
    with pytest.raises(vc.DockerError) as e:
        vc.Docker().run("ps")
    assert "not available" in str(e.value)


def test_create_replaces_leftover_container(env):
    c = make(env)
    c.create()
    (rootfs(env, c) / "workspace" / "old").write_text("x")
    c.create()
    assert not (rootfs(env, c) / "workspace" / "old").exists()


def test_exec_argv_shape(env):
    c = make(env)
    c.shell = "bash"
    a = c.exec_argv("echo hi", "job9", cwd="/srv")
    assert a[:2] == [c.docker.bin, "exec"] and "-w" in a and a[a.index("-w") + 1] == "/srv"
    assert "VM_AGENT_JOB=job9" in a and a[-3:] == ["bash", "-lc", "echo hi"]
    c.user = "1001:1001"
    assert "-u" in c.exec_argv("x", "j")


# --------------------------------------------------------------------------- capture / apply
def mutate(env, c):
    r = rootfs(env, c)
    (r / "workspace" / "a.txt").write_text("hello world\n")
    (r / "workspace" / "sub").mkdir()
    (r / "workspace" / "sub" / "b.bin").write_bytes(os.urandom(5000))
    (r / "etc" / "os-release").write_text("NAME=Changed\n")
    (r / "bin" / "tool").unlink()
    (r / "tmp" / "vm-jobs").mkdir(parents=True)
    (r / "tmp" / "vm-jobs" / "j1.log").write_text("noise")
    (r / "workspace" / "link").symlink_to("a.txt")


def snapshot_of(env, c, tmp, chunk=700):
    w = vs.ChunkWriter(tmp, chunk, max_bytes=10**9, min_free=0)
    tmp.mkdir(exist_ok=True)
    info = c.capture(w)
    w.finish()
    return w, info


def test_capture_apply_round_trip(env):
    c = make(env)
    c.create()
    mutate(env, c)
    expected_b = (rootfs(env, c) / "workspace" / "sub" / "b.bin").read_bytes()
    w, info = snapshot_of(env, c, env["tmp"] / "chunks")
    assert len(w.chunks) >= 2, "tiny chunk size should force several chunks"
    assert info["deleted"] >= 1
    assert not (env["state"] / "containers" / c.name / "paused").exists()      # always resumed

    c2 = make(env)
    c2.create()                                                                  # pristine replacement
    out = c2.apply([ch.path for ch in w.chunks])
    r = rootfs(env, c2)
    assert (r / "workspace" / "a.txt").read_text() == "hello world\n"
    assert (r / "workspace" / "sub" / "b.bin").read_bytes() == expected_b
    assert (r / "etc" / "os-release").read_text() == "NAME=Changed\n"
    assert not (r / "bin" / "tool").exists() and (r / "bin" / "keep").exists()
    assert os.readlink(r / "workspace" / "link") == "a.txt"
    assert not (r / "tmp" / "vm-jobs").exists()                                  # excluded paths are not saved
    assert (r / "etc" / "hosts").read_text() == "docker-managed"                 # nor Docker-managed files
    assert out["entries"] > 0


def test_snapshot_omits_docker_init_mountpoint(env):
    """Real docker (--init) reports /usr/sbin/docker-init in `docker diff` of a pristine container (seen on docker 28).
    It is Docker's own mountpoint, not user state, so it must not be stored or replayed."""
    import tarfile
    c = make(env)
    c.create()
    mutate(env, c)
    assert "docker-init" in c.docker.run("diff", c.name).stdout.decode()          # precondition: the diff does contain it
    w, _ = snapshot_of(env, c, env["tmp"] / "chunks-init")
    names = tarfile.open(fileobj=vs.ChunkReader([ch.path for ch in w.chunks]), mode="r|gz").getnames()
    assert "workspace/a.txt" in names
    assert not [n for n in names if n.endswith("docker-init")], names


def test_capture_unpauses_even_when_export_fails(env, monkeypatch):
    c = make(env)
    c.create()
    mutate(env, c)
    monkeypatch.setenv("FAKE_DOCKER_EXPORT_FAIL", "1")
    w = vs.ChunkWriter(env["tmp"] / "c2", 700, max_bytes=10**9, min_free=0)
    (env["tmp"] / "c2").mkdir()
    with pytest.raises(Exception):
        c.capture(w)
    assert not (env["state"] / "containers" / c.name / "paused").exists()


def test_capture_respects_cap(env):
    c = make(env)
    c.create()
    (rootfs(env, c) / "workspace" / "big").write_bytes(os.urandom(200_000))
    w = vs.ChunkWriter(env["tmp"] / "c3", 1000, max_bytes=5000, min_free=0)
    (env["tmp"] / "c3").mkdir()
    with pytest.raises(vs.CapExceeded):
        c.capture(w)
    w.abort()
    assert not (env["state"] / "containers" / c.name / "paused").exists()


def test_empty_container_still_produces_valid_snapshot(env):
    c = make(env)
    c.create()
    w, info = snapshot_of(env, c, env["tmp"] / "c4")
    assert w.total > 0
    c2 = make(env)
    c2.create()
    c2.apply([ch.path for ch in w.chunks])


def test_ensure_workdir(env):
    c = make(env)
    c.create()
    (rootfs(env, c) / "srv" / "proj").mkdir(parents=True)
    assert c.ensure_workdir("/srv/proj") == "/srv/proj"
    assert c.ensure_workdir("/gone") == vc.DEFAULT_WORKDIR


def test_apply_removes_only_listed_paths_and_ignores_non_absolute(env):
    c = make(env)
    c.create()
    mutate(env, c)
    w, _ = snapshot_of(env, c, env["tmp"] / "c5")
    c2 = make(env)
    c2.create()
    c2.apply([ch.path for ch in w.chunks])
    assert (rootfs(env, c2) / "bin" / "keep").exists()


# --------------------------------------------------------------------------- the sweep script
def test_kill_job_kills_only_the_marked_job(env):
    c = make(env)
    c.create()
    c.shell = "sh"
    mine = subprocess.Popen(c.exec_argv("sleep 60", "jobA"))
    other = subprocess.Popen(c.exec_argv("sleep 60", "jobB"))
    time.sleep(0.8)
    try:
        assert mine.poll() is None and other.poll() is None
        c.kill_job("jobA", grace=0.3)
        mine.wait(timeout=10)
        assert mine.returncode != 0
        assert other.poll() is None, "another job must survive"
        c.kill_all_jobs()
        other.wait(timeout=10)
    finally:
        for p in (mine, other):
            if p.poll() is None:
                p.kill()


def test_sweep_never_touches_processes_outside_the_container(env):
    """fake_docker runs the sweep on the HOST. On a real agent VM every job shell carries VM_AGENT_JOB, so an unscoped
    sweep killed the job running the tests (exit 137). The fake must only reach processes it started itself."""
    c = make(env)
    c.create()
    c.shell = "sh"
    decoy = subprocess.Popen(["sleep", "60"], env={**os.environ, "VM_AGENT_JOB": "decoy"})   # like the agent's own job shell
    inside = subprocess.Popen(c.exec_argv("sleep 60", "jobA"))
    time.sleep(0.8)
    try:
        assert decoy.poll() is None and inside.poll() is None
        c.kill_all_jobs()
        inside.wait(timeout=10)                                  # the sweep still works inside the "container"
        assert decoy.poll() is None, "a process outside the container was killed by the sweep"
    finally:
        for p in (decoy, inside):
            if p.poll() is None:
                p.kill()


# --------------------------------------------------------------------------- end to end with the store
def test_save_and_restore_through_store(gh, env, tmp_path):
    c = make(env)
    c.create()
    mutate(env, c)
    s = store(tmp_path / "scratch", chunk_bytes=900)
    (tmp_path / "scratch").mkdir(exist_ok=True)
    res = s.save(epoch=1, capture=lambda w: c.capture(w), config={"image": IMG, "workdir": "/workspace"},
                 fence=ok_fence())
    assert res["chunks"] >= 2
    restored = s.restore()
    c2 = make(env)
    c2.create()
    try:
        c2.apply(restored.chunk_paths)
    finally:
        restored.cleanup()
    assert (rootfs(env, c2) / "workspace" / "a.txt").read_text() == "hello world\n"
    assert not (rootfs(env, c2) / "bin" / "tool").exists()
