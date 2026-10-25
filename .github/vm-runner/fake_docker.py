#!/usr/bin/env python3
"""A directory-backed stand-in for the docker CLI, used only by tests (VM_DOCKER=/path/to/fake_docker.py).

Images are directories under $FAKE_DOCKER_IMAGES/<ref with / : @ replaced by _>. A container is a copy of its image
under $FAKE_DOCKER_STATE/containers/<name>/rootfs plus a pristine copy (base) used to compute `docker diff`.
`exec` runs the command ON THE HOST with the -e variables in its environment (no chroot), except for `test` and `rm`,
whose paths are mapped into the rootfs. That is enough to exercise vm_container's argv, its sweep script and its
snapshot capture/apply logic. Not a Docker emulator beyond that.
"""
import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

STATE = Path(os.environ.get("FAKE_DOCKER_STATE", "/tmp/fake-docker-state"))
IMAGES = Path(os.environ.get("FAKE_DOCKER_IMAGES", "/tmp/fake-docker-images"))
LOG = STATE / "calls.log"


def img_dir(ref):
    return IMAGES / ref.replace("/", "_").replace(":", "_").replace("@", "_")


def cdir(name):
    return STATE / "containers" / name


def die(msg, code=1):
    sys.stderr.write(msg + "\n")
    sys.exit(code)


def walk(root: Path):
    out = {}
    for dp, dns, fns in os.walk(root):
        for n in dns + fns:
            p = Path(dp) / n
            out["/" + str(p.relative_to(root))] = p
    return out


def same(a: Path, b: Path):
    if a.is_symlink() or b.is_symlink():
        return a.is_symlink() and b.is_symlink() and os.readlink(a) == os.readlink(b)
    if a.is_dir() or b.is_dir():
        return a.is_dir() and b.is_dir()
    return a.read_bytes() == b.read_bytes() and (a.stat().st_mode == b.stat().st_mode)


def cmd_diff(name):
    c = cdir(name)
    if not c.exists():
        die(f"Error response from daemon: No such container: {name}")
    now, base = walk(c / "rootfs"), walk(c / "base")
    lines, parents = [], set()
    for p in sorted(now):
        if p not in base:
            lines.append(f"A {p}")
        elif not same(now[p], base[p]):
            lines.append(f"C {p}")
        else:
            continue
        parent = os.path.dirname(p)
        while parent and parent != "/":
            parents.add(parent)
            parent = os.path.dirname(parent)
    for p in sorted(base):
        if p not in now:
            lines.append(f"D {p}")
            parent = os.path.dirname(p)
            while parent and parent != "/":
                parents.add(parent)
                parent = os.path.dirname(parent)
    have = {line[2:] for line in lines}
    for p in sorted(parents - have):
        lines.append(f"C {p}")
    print("\n".join(lines))


def cmd_export(name):
    if os.environ.get("FAKE_DOCKER_EXPORT_FAIL") == "1":
        sys.stdout.buffer.write(b"partial")
        sys.stdout.flush()
        die("export exploded")
    root = cdir(name) / "rootfs"
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|", format=tarfile.PAX_FORMAT) as tf:
        for p, path in sorted(walk(root).items()):
            ti = tf.gettarinfo(str(path), arcname=p.lstrip("/"))
            if ti.isreg():
                with open(path, "rb") as fh:
                    tf.addfile(ti, fh)
            else:
                tf.addfile(ti)


def cmd_inspect(name):
    c = cdir(name)
    if not c.exists():
        die(f"Error: No such object: {name}")
    custom = c / "inspect.json"
    info = json.loads(custom.read_text()) if custom.exists() else {}
    info.setdefault("Image", "sha256:fakeimage")
    info.setdefault("Config", {"Image": "fake", "Env": ["PATH=/usr/bin"], "User": ""})
    info.setdefault("Mounts", [])
    print(json.dumps([info]))


def cmd_cp(args):
    # tar stream out of a directory:  docker cp NAME:/dir/. -
    if len(args) == 2 and args[1] == "-" and ":" in args[0]:
        n, _, p = args[0].partition(":")
        src = cdir(n) / "rootfs" / p.removesuffix("/.").lstrip("/")
        if not src.is_dir():
            die(f"Error response from daemon: Could not find the file {p} in container {n}")
        with tarfile.open(fileobj=sys.stdout.buffer, mode="w|", format=tarfile.PAX_FORMAT) as tf:
            for q, path in sorted(walk(src).items()):
                ti = tf.gettarinfo(str(path), arcname=q.lstrip("/"))
                if ti.isreg():
                    with open(path, "rb") as fh:
                        tf.addfile(ti, fh)
                else:
                    tf.addfile(ti)
        return
    # tar stream into a directory:  docker cp - NAME:/dir/
    if args[0] == "-" and len(args) == 2:
        n, _, p = args[1].partition(":")
        root = cdir(n) / "rootfs"
        if not root.exists():
            die(f"Error response from daemon: No such container: {n}")
        dest = root / p.lstrip("/")
        dest.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=sys.stdin.buffer, mode="r|") as tf:
            tf.extractall(dest, filter="fully_trusted")
        return
    # docker cp -a - NAME:/   (tar stream)   |   docker cp SRC NAME:PATH   |   docker cp NAME:PATH DEST
    if args[0] != "-a" and ":" in (args[0] + args[1]):
        src, dest = args[0], args[1]
        def resolve(spec):
            if ":" in spec and not spec.startswith("/"):
                n, _, p = spec.partition(":")
                if not (cdir(n) / "rootfs").exists():
                    die(f"Error response from daemon: No such container: {n}")
                return cdir(n) / "rootfs" / p.lstrip("/"), spec.endswith("/")
            return Path(spec), spec.endswith("/")
        sp, _ = resolve(src)
        dp, slash = resolve(dest)
        if not sp.exists():
            die(f"Error response from daemon: Could not find the file {src}")
        if dp.is_dir() or slash:
            dp.mkdir(parents=True, exist_ok=True)
            dp = dp / sp.name
        elif not dp.parent.exists():
            die(f"Error response from daemon: Could not find the destination {dest}")
        shutil.copy2(sp, dp)
        return
    name = args[-1].split(":")[0]
    root = cdir(name) / "rootfs"
    if not root.exists():
        die(f"Error response from daemon: No such container: {name}")
    with tarfile.open(fileobj=sys.stdin.buffer, mode="r|") as tf:
        tf.extractall(root, filter="fully_trusted")


def cmd_exec(args):
    env, cwd, i = dict(os.environ), None, 0
    while args[i].startswith("-"):
        if args[i] == "-w":
            cwd = args[i + 1]; i += 2
        elif args[i] == "-e":
            k, _, v = args[i + 1].partition("="); env[k] = v; i += 2
        elif args[i] == "-u":
            i += 2
        else:
            i += 1
    name, cmd = args[i], args[i + 1:]
    c = cdir(name)
    if not c.exists():
        die(f"Error response from daemon: No such container: {name}")
    if (c / "paused").exists():
        die("Error response from daemon: Container is paused")
    root = c / "rootfs"
    if cmd[0] == "test":
        t = root / cmd[2].lstrip("/")
        ok = {"-d": t.is_dir(), "-f": t.is_file(), "-e": t.exists() or t.is_symlink()}.get(cmd[1], False)
        sys.exit(0 if ok else 1)
    if cmd[0] == "mkdir" and cmd[1] == "-p":
        for p in cmd[2:]:
            (root / p.lstrip("/")).mkdir(parents=True, exist_ok=True)
        sys.exit(0)
    if cmd[0] == "rm":
        for p in cmd[3:]:
            t = root / p.lstrip("/")
            if t.is_dir() and not t.is_symlink():
                shutil.rmtree(t, ignore_errors=True)
            elif t.exists() or t.is_symlink():
                t.unlink()
        sys.exit(0)
    # The fake runs everything on the HOST, so tag what it starts and confine vm_container's process sweep to those
    # processes. Unscoped, the sweep killed every process of the machine carrying VM_AGENT_JOB (e.g. the agent's own job).
    scope = f"FAKE_DOCKER_SCOPE={STATE}"
    env["FAKE_DOCKER_SCOPE"] = str(STATE)
    if len(cmd) >= 3 and cmd[:2] == ["sh", "-c"] and "/proc/[0-9]*" in cmd[2] and "environ" in cmd[2]:
        marker = "if tr "
        if cmd[2].count(marker) != 1:
            die("fake_docker: sweep script changed shape; update the scoping in cmd_exec")
        cmd = cmd[:2] + [cmd[2].replace(marker, f'if grep -qxz "{scope}" "$p/environ" 2>/dev/null && tr ')] + cmd[3:]
    sys.exit(subprocess.call(cmd, env=env, cwd=None))


def main(argv):
    STATE.mkdir(parents=True, exist_ok=True)
    with open(LOG, "a") as fh:
        fh.write(" ".join(argv) + "\n")
    cmd, args = argv[0], argv[1:]
    if cmd == "pull":
        if os.environ.get("FAKE_DOCKER_PULL_FAIL") == "1" or not img_dir(args[0]).exists():
            die(f"Error response from daemon: pull access denied for {args[0]}")
    elif cmd == "image" and args[0] == "inspect":
        sys.exit(0 if img_dir(args[1]).exists() else 1)
    elif cmd == "rm":
        shutil.rmtree(cdir(args[-1]), ignore_errors=True)
    elif cmd == "run":
        name = args[args.index("--name") + 1]
        workdir = args[args.index("-w") + 1]
        image = args[args.index("sh") + 1] if "sh" in args else args[-3]
        src = img_dir(image)
        if not src.exists():
            die(f"Unable to find image {image}")
        c = cdir(name)
        if c.exists():
            die(f'Conflict. The container name "{name}" is already in use')
        shutil.copytree(src, c / "base", symlinks=True)
        shutil.copytree(src, c / "rootfs", symlinks=True)
        (c / "rootfs" / workdir.lstrip("/")).mkdir(parents=True, exist_ok=True)
        (c / "base" / workdir.lstrip("/")).mkdir(parents=True, exist_ok=True)
        for f in (".dockerenv", "etc/hosts", "etc/hostname", "etc/resolv.conf"):
            (c / "rootfs" / f).parent.mkdir(parents=True, exist_ok=True)
            (c / "rootfs" / f).write_text("docker-managed")
        if "--init" in args:        # real docker leaves an empty mountpoint for its tini binary; it shows up in `docker diff`
            init = c / "rootfs" / "usr" / "sbin" / "docker-init"
            init.parent.mkdir(parents=True, exist_ok=True)
            init.write_bytes(b"")
        print("c0ffee" + name)
    elif cmd == "exec":
        cmd_exec(args)
    elif cmd == "diff":
        cmd_diff(args[0])
    elif cmd == "export":
        cmd_export(args[0])
    elif cmd == "cp":
        cmd_cp(args)
    elif cmd == "inspect":
        cmd_inspect(args[-1])
    elif cmd == "pause":
        (cdir(args[0]) / "paused").touch()
    elif cmd == "unpause":
        (cdir(args[0]) / "paused").unlink(missing_ok=True)
    else:
        die(f"fake docker: unsupported command {cmd}")


if __name__ == "__main__":
    main(sys.argv[1:])
