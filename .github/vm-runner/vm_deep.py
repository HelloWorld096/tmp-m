"""Deep snapshots: what a normal snapshot does not carry, and how a restored session is checked against it.

A NORMAL snapshot already keeps everything that changed on the container's filesystem: installed packages (their files
and dpkg/pip/npm databases), caches (~/.cache, pip, npm, cargo registries), dotfiles and configuration under /etc, ~/.config
and the like. What it cannot keep is whatever lives outside `docker diff`: data in the image's VOLUME paths, and facts
about how the container was built. DEEP adds, as host-written side data (vm_container.EXTRA_PREFIX):

  deep.json        image reference and digest, the container's runtime configuration (user, working directory,
                   environment with secret-looking names dropped), and package manifests taken inside the container
                   (dpkg, apt "manual", apk, pip, npm -g, cargo, gem);
  volume-<n>.tar   the contents of each image VOLUME / anonymous-volume directory, up to MAX_VOLUME_BYTES each.

Applying a deep snapshot (only when asked to) restores the volume directories and then compares the manifests with what
the restored container really contains. Drift is REPORTED, never "fixed": nothing is reinstalled from the network.

Not captured by any mode: running processes, shell variables or `export`s from earlier commands (every command is a new
shell; only the working directory carries over), open sockets, kernel state.
"""
from __future__ import annotations

import json
import re
import time
from typing import Optional

SCHEMA_VERSION = 1
MAX_VOLUME_BYTES = 96 * 1024 * 1024
MAX_VOLUMES = 8
MANIFEST_TIMEOUT_S = 180
SECTION_RE = re.compile(r"^### ([a-z0-9-]+)$")
SECRET_NAME_RE = re.compile(r"(TOKEN|SECRET|PASS(WORD|WD)?|KEY|CRED|AUTH|COOKIE|SESSION|PRIVATE|CERT)", re.I)
SAFE_DEST_RE = re.compile(r"^/[A-Za-z0-9_./@+-]{1,300}$")

# One script, one `docker exec`. Each section is skipped silently when its tool is absent. POSIX sh only (alpine, busybox).
MANIFEST_SCRIPT = r"""
sec() { printf '### %s\n' "$1"; }
sec dpkg
command -v dpkg-query >/dev/null 2>&1 && dpkg-query -W -f='${Package}\t${Version}\t${Architecture}\n' 2>/dev/null | sort
sec apt-manual
command -v apt-mark >/dev/null 2>&1 && apt-mark showmanual 2>/dev/null | sort
sec apk
command -v apk >/dev/null 2>&1 && apk info -v 2>/dev/null | sort
sec pip
for py in python3 python; do
  if command -v "$py" >/dev/null 2>&1; then "$py" -m pip list --format=freeze 2>/dev/null | sort; break; fi
done
sec npm-global
command -v npm >/dev/null 2>&1 && npm ls -g --depth=0 2>/dev/null | sed -n 's/^.*[+`]-- //p; s/^.*[├└]── //p' | sort
sec cargo
command -v cargo >/dev/null 2>&1 && cargo install --list 2>/dev/null | grep -v '^ ' | sort
sec gem
command -v gem >/dev/null 2>&1 && gem list --local 2>/dev/null | sort
exit 0
"""


def redact_env(items) -> dict:
    """["A=1", "TOKEN=x"] -> {"A": "1"} (names that look like credentials are dropped, and listed by name only)."""
    kept, dropped = {}, []
    for item in items or []:
        if not isinstance(item, str) or "=" not in item:
            continue
        k, _, v = item.partition("=")
        if SECRET_NAME_RE.search(k):
            dropped.append(k)
        else:
            kept[k] = v[:2000]
    return {"env": kept, "env_dropped": sorted(dropped)}


def parse_manifests(text: str) -> dict:
    """The MANIFEST_SCRIPT output -> {section: [lines]} (empty sections are kept: "tool present, nothing installed")."""
    out: dict = {}
    cur: Optional[list] = None
    for line in (text or "").splitlines():
        m = SECTION_RE.match(line)
        if m:
            cur = out.setdefault(m.group(1), [])
        elif cur is not None and line.strip():
            cur.append(line.rstrip())
    return out


def volume_dests(inspect: dict) -> list:
    """Directories of the image's VOLUMEs / the container's anonymous volumes, as safe absolute paths."""
    dests = []
    for mnt in inspect.get("Mounts") or []:
        if isinstance(mnt, dict) and mnt.get("Type") == "volume":
            dests.append(mnt.get("Destination"))
    dests.extend(((inspect.get("Config") or {}).get("Volumes") or {}).keys())
    out, seen = [], set()
    for d in dests:
        if isinstance(d, str) and SAFE_DEST_RE.match(d) and ".." not in d.split("/") and d not in seen:
            seen.add(d)
            out.append(d)
    return out[:MAX_VOLUMES]


def build(inspect: dict, manifest_text: str, *, workdir: str, volumes: list, skipped: list) -> dict:
    cfg = inspect.get("Config") or {}
    return {
        "schema": SCHEMA_VERSION,
        "taken_at": time.time(),
        "image": cfg.get("Image"),
        "image_id": inspect.get("Image"),
        "user": cfg.get("User") or "",
        "workdir": workdir,
        **redact_env(cfg.get("Env")),
        "manifests": parse_manifests(manifest_text),
        "volumes": volumes,                   # [{"dest", "extra", "bytes"}]
        "volumes_skipped": skipped,           # [{"dest", "reason"}]
    }


def parse(raw: bytes) -> dict:
    try:
        d = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as err:
        raise ValueError(f"deep.json is not valid JSON: {err}") from err
    if not isinstance(d, dict) or int(d.get("schema", 0)) != SCHEMA_VERSION or not isinstance(d.get("manifests"), dict):
        raise ValueError("deep.json has an unsupported shape")
    vols = []
    for v in d.get("volumes") or []:
        if isinstance(v, dict) and isinstance(v.get("dest"), str) and SAFE_DEST_RE.match(v["dest"]) \
                and ".." not in v["dest"].split("/") and isinstance(v.get("extra"), str):
            vols.append({"dest": v["dest"], "extra": v["extra"], "bytes": int(v.get("bytes", 0))})
    d["volumes"] = vols
    return d


def compare(saved: dict, current: dict, limit: int = 15) -> dict:
    """Per section: lines that were saved but are missing now, and lines present now that were not saved. Sections whose
    tool is not present in the restored container at all are reported as ``tool_missing``."""
    report = {"sections": {}, "tool_missing": [], "clean": True}
    for name, lines in sorted(saved.items()):
        if name not in current:
            if lines:
                report["tool_missing"].append(name)
                report["clean"] = False
            continue
        a, b = set(lines), set(current[name])
        missing, extra = sorted(a - b), sorted(b - a)
        if missing or extra:
            report["clean"] = False
            report["sections"][name] = {"missing": len(missing), "unexpected": len(extra),
                                        "missing_examples": missing[:limit], "unexpected_examples": extra[:limit]}
    return report


def summarize(report: dict) -> str:
    if report["clean"]:
        return "installed packages match the snapshot"
    parts = []
    for name, r in report["sections"].items():
        parts.append(f"{name}: {r['missing']} missing, {r['unexpected']} unexpected")
    if report["tool_missing"]:
        parts.append("tool missing: " + ", ".join(report["tool_missing"]))
    return "drift from the snapshot (" + "; ".join(parts) + ")"
