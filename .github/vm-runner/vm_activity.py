"""What the VM is doing right now, as data the dashboard can draw (not a sentence to parse).

The snapshot code reports free-text progress ("uploading name (3/9)"). Every such message passes through ``Activity.set``,
which names the phase, pulls out the counters, and keeps a short timeline, so the dashboard can show "paused, capturing
locally", "9 chunks, 1.2 GB", "uploading chunk 3 of 9" without guessing.

Phases (the ``phase`` field):
  starting, looking, verifying, downloading, applying, collecting, preparing
  paused      the container is paused while its filesystem is captured to local disk
  uploading   the captured snapshot goes up in chunks (done/total chunks, bytes_total once known)
  committing, saved, failed, ready, stopping, stopped, handoff, other
"""
from __future__ import annotations

import re
import threading
import time
from collections import deque
from typing import Optional

TIMELINE = 40

_PATTERNS = (
    (re.compile(r"^starting the container"), "starting"),
    (re.compile(r"^looking for a snapshot"), "looking"),
    (re.compile(r"^fresh session"), "ready"),
    (re.compile(r"^verifying the manifest"), "verifying"),
    (re.compile(r"^applying the snapshot"), "applying"),
    (re.compile(r"^deep apply"), "applying"),
    (re.compile(r"^collecting deep data"), "collecting"),
    (re.compile(r"^inspecting the session repository"), "preparing"),
    (re.compile(r"^committing"), "committing"),
    (re.compile(r"^saved$"), "saved"),
    (re.compile(r"^save failed"), "failed"),
    (re.compile(r"^failed$"), "failed"),
    (re.compile(r"^ready$"), "ready"),
    (re.compile(r"^handing over"), "handoff"),
    (re.compile(r"^shutting down"), "stopping"),
    (re.compile(r"^stopped$"), "stopped"),
)
_CAPTURE = re.compile(r"^capturing the filesystem state")
_CHUNKED = re.compile(r"^uploading (\d+) chunk\(s\), (\d+) bytes")
_UPLOAD = re.compile(r"^uploading \S+ \((\d+)/(\d+)\)")
_DOWNLOAD = re.compile(r"^downloading \S+ \((\d+)/(\d+)\)")


def classify(msg: str) -> dict:
    """{phase, done, total, bytes_total} for one progress message. Unknown messages are phase 'other'."""
    m = _CAPTURE.match(msg)
    if m:
        return {"phase": "paused", "done": None, "total": None, "bytes_total": None}
    m = _CHUNKED.match(msg)
    if m:
        return {"phase": "uploading", "done": 0, "total": int(m.group(1)), "bytes_total": int(m.group(2))}
    m = _UPLOAD.match(msg)
    if m:
        return {"phase": "uploading", "done": int(m.group(1)) - 1, "total": int(m.group(2)), "bytes_total": None}
    m = _DOWNLOAD.match(msg)
    if m:
        return {"phase": "downloading", "done": int(m.group(1)) - 1, "total": int(m.group(2)), "bytes_total": None}
    for pattern, phase in _PATTERNS:
        if pattern.match(msg):
            return {"phase": phase, "done": None, "total": None, "bytes_total": None}
    return {"phase": "other", "done": None, "total": None, "bytes_total": None}


class Activity:
    """Thread-safe current phase plus a bounded timeline."""

    def __init__(self, clock=time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self._timeline: deque = deque(maxlen=TIMELINE)
        self._cur = {"phase": "starting", "label": "starting", "done": None, "total": None, "bytes_total": None, "since": clock()}

    def set(self, msg: str) -> None:
        info = classify(msg)
        now = self._clock()
        with self._lock:
            prev = self._cur
            # the chunk count and byte total belong to the whole upload: keep them while its chunks tick by
            if info["phase"] == "uploading" and info["bytes_total"] is None and prev["phase"] == "uploading":
                info["bytes_total"] = prev["bytes_total"]
            changed_phase = info["phase"] != prev["phase"]
            self._cur = {**info, "label": msg[:200], "since": now if changed_phase else prev["since"]}
            self._timeline.append({"t": now, "phase": info["phase"], "label": msg[:200]})

    def snapshot(self, timeline: int = 12) -> dict:
        with self._lock:
            return {**self._cur, "timeline": list(self._timeline)[-timeline:]}
