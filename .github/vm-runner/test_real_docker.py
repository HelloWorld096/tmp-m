"""Opt-in checks of SessionContainer against a REAL docker daemon (the rest of the suite uses fake_docker.py).

Run:  VM_REAL_DOCKER=1 python -m pytest -q test_real_docker.py
Needs a working `docker` and network access to pull the image (default ubuntu:24.04, override VM_REAL_DOCKER_IMAGE).
"""
import os
import subprocess
from pathlib import Path

import pytest

from vm_container import Docker, SessionContainer
from vm_snapshot import ChunkWriter

pytestmark = pytest.mark.skipif(os.environ.get("VM_REAL_DOCKER") != "1",
                                reason="set VM_REAL_DOCKER=1 to run against a real docker daemon")
IMAGE = os.environ.get("VM_REAL_DOCKER_IMAGE", "ubuntu:24.04")


def sh(c: SessionContainer, script: str, check: bool = True) -> str:
    p = subprocess.run(["docker", "exec", c.name, "bash", "-c", script], capture_output=True, text=True)
    if check and p.returncode != 0:
        raise AssertionError(f"exec failed rc={p.returncode}: {script!r}\n{p.stdout}\n{p.stderr}")
    return p.stdout


def paused(c: SessionContainer) -> str:
    return subprocess.run(["docker", "inspect", "-f", "{{.State.Paused}}", c.name], capture_output=True,
                          text=True).stdout.strip()


@pytest.fixture
def box(tmp_path):
    jobs = tmp_path / "vm-jobs"
    jobs.mkdir()
    c = SessionContainer("s-realdockertest", IMAGE, Docker(), jobs_dir=jobs)
    c.create()
    try:
        yield c, tmp_path
    finally:
        c.remove()


def capture(c, tmp_path, chunk_bytes):
    d = tmp_path / "chunks"
    d.mkdir(exist_ok=True)
    w = ChunkWriter(d, chunk_bytes, max_bytes=10 ** 10)
    meta = c.capture(w)
    w.finish()
    return meta, w


FINGERPRINT = (
    "find /workspace /tmp/rd.txt /etc/issue -printf '%p|%y|%m|%U:%G|%s|%l\\n' | sort; "
    "find /workspace /tmp/rd.txt /etc/issue -type f -exec sha256sum {} + | sort -k2"
)


def test_roundtrip_through_a_real_container(box):
    c, tmp = box
    sh(c, r"""set -e
mkdir -p /workspace/proj/sub "/workspace/dir with space" /workspace/emptydir
echo hello > /workspace/proj/a.txt
printf 'u\n' > "/workspace/dir with space/ünï code file.txt"
ln -s a.txt /workspace/proj/link
ln /workspace/proj/a.txt /workspace/proj/hard
echo x > /workspace/proj/exec.sh; chmod 755 /workspace/proj/exec.sh
echo secret > /workspace/proj/priv; chmod 600 /workspace/proj/priv; chown 1234:5678 /workspace/proj/priv
head -c 12000000 /dev/urandom > /workspace/big.bin
echo changed >> /etc/issue
echo tmp > /tmp/rd.txt
test -e /etc/bash.bashrc && rm -f /etc/bash.bashrc
test -d /etc/skel && rm -rf /etc/skel
""")
    before = sh(c, FINGERPRINT)
    assert "big.bin" in before

    meta, w = capture(c, tmp, 4 << 20)
    assert paused(c) == "false", "container must be resumed after capture"
    assert len(w.chunks) >= 3, f"12 MB of random data at 4 MiB chunks should span 3+ chunks, got {len(w.chunks)}"
    assert meta["deleted"] >= 2

    c.remove()
    c.create()                                   # pristine container again
    assert sh(c, "test -e /workspace/proj/a.txt && echo present || echo absent").strip() == "absent"
    applied = c.apply([ch.path for ch in w.chunks])
    assert applied["entries"] > 0

    assert sh(c, FINGERPRINT) == before          # content, type, mode, owner, size, symlink target all identical
    sh(c, "[ /workspace/proj/a.txt -ef /workspace/proj/hard ]")       # hardlink still one inode
    sh(c, "[ ! -e /etc/bash.bashrc ] && [ ! -e /etc/skel ]")         # deletions replayed
    sh(c, "[ -d /workspace/emptydir ]")
    assert sh(c, "stat -c %u:%g /workspace/proj/priv").strip() == "1234:5678"


def test_hardlink_to_an_unchanged_base_image_file(box):
    """A new path hard-linked to a file that exists in the image: the tar entry is a link whose target may be filtered out."""
    c, tmp = box
    sh(c, "ln /usr/bin/true /workspace/true-link && echo ok > /workspace/marker")
    before = sh(c, "sha256sum /workspace/true-link /workspace/marker")
    meta, w = capture(c, tmp, 8 << 20)
    c.remove()
    c.create()
    c.apply([ch.path for ch in w.chunks])
    assert sh(c, "sha256sum /workspace/true-link /workspace/marker") == before
    sh(c, "/workspace/true-link")                # still an executable


def test_capture_while_a_process_is_writing(box):
    """A process appending to a file while capture() runs: the snapshot must hold lines 1..N (none torn, skipped or
    repeated), and the writer must keep running afterwards (container resumed). NOTE: this does not prove the pause
    works (it also passes with pause disabled, because appends keep any prefix valid); see the lockstep test below."""
    c, tmp = box
    subprocess.run(["docker", "exec", "-d", c.name, "bash", "-c",
                    "i=0; while :; do i=$((i+1)); echo $i >> /workspace/seq.txt; done"], check=True)
    deadline = __import__("time").time() + 20
    while int(sh(c, "wc -l < /workspace/seq.txt 2>/dev/null || echo 0").strip() or 0) < 2000:
        assert __import__("time").time() < deadline, "writer did not produce output"
        __import__("time").sleep(0.2)

    meta, w = capture(c, tmp, 8 << 20)
    assert paused(c) == "false"
    n1 = int(sh(c, "wc -l < /workspace/seq.txt").strip())
    __import__("time").sleep(1.0)
    n2 = int(sh(c, "wc -l < /workspace/seq.txt").strip())
    assert n2 > n1, "the writer must keep running after capture (container resumed)"

    c.remove()
    c.create()
    c.apply([ch.path for ch in w.chunks])
    lines = sh(c, "cat /workspace/seq.txt").split("\n")
    assert lines[-1] == "", "snapshot must not end in a torn line"
    nums = [int(x) for x in lines[:-1]]            # every line must parse as an integer
    assert len(nums) >= 2000
    assert nums == list(range(1, len(nums) + 1)), "snapshot must be a consistent prefix: 1..N, no gaps or repeats"


@pytest.mark.skipif(os.environ.get("VM_REAL_DOCKER_LARGE") != "1",
                    reason="set VM_REAL_DOCKER_LARGE=1 (moves ~1.5 GiB through docker export/cp; takes a minute or two)")
def test_snapshot_larger_than_one_chunk_at_the_default_chunk_size(box):
    import resource
    from vm_snapshot import DEFAULT_CHUNK_BYTES
    c, tmp = box
    size = 3 * (1 << 29)                                   # 1.5 GiB of incompressible data: needs 2 chunks at 1 GiB
    sh(c, f"head -c {size} /dev/urandom > /workspace/huge.bin && echo small > /workspace/small.txt")
    before = sh(c, "sha256sum /workspace/huge.bin /workspace/small.txt")

    d = tmp / "chunks"
    d.mkdir()
    w = ChunkWriter(d, DEFAULT_CHUNK_BYTES, max_bytes=40 * 10 ** 9)
    c.capture(w)
    w.finish()
    assert len(w.chunks) >= 2, f"expected 2+ chunks, got {len(w.chunks)}"
    assert all(ch.bytes <= DEFAULT_CHUNK_BYTES for ch in w.chunks)
    assert w.total > size, "snapshot must contain the 1.5 GiB payload"
    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    assert rss_mb < 700, f"capture should stream; peak RSS of the test process was {rss_mb:.0f} MB"

    c.remove()
    c.create()
    c.apply([ch.path for ch in w.chunks])
    assert sh(c, "sha256sum /workspace/huge.bin /workspace/small.txt") == before


def test_capture_is_atomic_across_files(box):
    """Two files kept in lockstep by a writer: /workspace/a is replaced first, /zzz/b second. docker export walks the tree
    in order, so WITHOUT a pause b is read long after a and ends up far ahead. With the pause, a is b or b+1.
    The writer replaces each file by rename (write temp, mv) so that no single file can ever be observed half-written:
    a plain `echo $i > f` truncates before it writes, and a process frozen between the two would leave an empty file
    (true of any pause-based snapshot, and not what this test is about)."""
    c, tmp = box
    sh(c, "mkdir -p /zzz /workspace && echo 0 > /workspace/a && echo 0 > /zzz/b")
    # Filler sorted between a and b: incompressible, so gzip needs seconds for it and export reads b long after a.
    sh(c, "head -c 150000000 /dev/urandom > /workspace/m.bin")
    subprocess.run(["docker", "exec", "-d", c.name, "bash", "-c",
                    "i=0; while :; do i=$((i+1)); "
                    "echo $i > /workspace/a.t && mv /workspace/a.t /workspace/a; "
                    "echo $i > /zzz/b.t && mv /zzz/b.t /zzz/b; done"], check=True)
    deadline = __import__("time").time() + 30
    while int(sh(c, "cat /workspace/a").strip() or 0) < 300:      # let the writer make real progress first
        assert __import__("time").time() < deadline, "writer did not make progress"
        __import__("time").sleep(0.2)
    meta, w = capture(c, tmp, 8 << 20)
    assert paused(c) == "false"
    c.remove()
    c.create()
    c.apply([ch.path for ch in w.chunks])
    a, b = (int(x) for x in sh(c, "cat /workspace/a /zzz/b").split())
    assert a >= 300, "the writer should have made real progress"
    assert a - b in (0, 1), f"snapshot is not atomic: a={a} b={b}"


# ---- images that declare VOLUME (session 8) ---------------------------------------------------------------------
def _volume_names() -> set:
    out = subprocess.run(["docker", "volume", "ls", "-q"], capture_output=True, text=True, check=True).stdout
    return set(out.split())


@pytest.fixture
def volume_image():
    """A throwaway image = IMAGE + `VOLUME /data` (docker makes an ANONYMOUS volume for it on every `docker run`)."""
    import uuid
    tag = f"vm-test-volume-{uuid.uuid4().hex[:8]}"
    dockerfile = f"FROM {IMAGE}\nVOLUME /data\n"
    subprocess.run(["docker", "build", "-q", "-t", tag, "-"], input=dockerfile.encode(), check=True,
                   capture_output=True, timeout=600)
    try:
        yield tag
    finally:
        subprocess.run(["docker", "rmi", "-f", tag], capture_output=True)


def test_remove_leaves_no_anonymous_volume(tmp_path, volume_image):
    """remove() and the replace-on-create path must not leak the anonymous volume of an image with VOLUME."""
    jobs = tmp_path / "vm-jobs"
    jobs.mkdir()
    before = _volume_names()
    c = SessionContainer("s-realvolumetest", volume_image, Docker(), jobs_dir=jobs)
    leaked: set = set()
    try:
        c.create()
        assert len(_volume_names() - before) == 1, "expected the image's anonymous volume to exist while running"
        c.create()                      # replaces the container with the same name; the first one's volume must go too
    finally:
        c.remove()
        leaked = _volume_names() - before
        for v in leaked:                # never leave scratch behind, even when the assertion below fails
            subprocess.run(["docker", "volume", "rm", "-f", v], capture_output=True)
    assert not leaked, f"anonymous volume(s) leaked after remove(): {sorted(leaked)}"
    assert (jobs).is_dir(), "the bind-mounted jobs dir must survive remove()"
