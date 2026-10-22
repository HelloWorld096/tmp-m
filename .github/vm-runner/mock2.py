#!/usr/bin/env python3
"""mock2.py - mock GitHub Releases API for relay_test.sh section 12 (relay.sh `put` / `get`).

RECONSTRUCTED IN SESSION 7. The original mock2.py was missing from the upload, so this is NOT the original file. It
was written from what relay_test.sh section 12 asserts and what relay.sh sends. Passing proves relay.sh is consistent
with THIS model of the GitHub Releases API, not that it matches real GitHub.

Usage: mock2.py DIR      writes DIR/port; logs DIR/req.log (API+uploads host) and DIR/blob.log (separate CDN host)

Hosts: API and uploads share one port on 127.0.0.1. Downloads are redirected (302) to a SECOND server on 127.0.0.2,
so curl -L really crosses hosts and must drop the Authorization header by itself (blob.log then shows auth=None).

Control: GET /_ctl?reset=1[&preload=FILE][&mode=KIND=VALUE]
  reset=1    forget releases, truncate both logs, clear modes
  preload=F  release 7 holding an uploaded asset 99 named payload.bin with the bytes and sha256 of file F
  mode=up=500|liedigest|nodigest   upload answers 500 / a wrong digest / no digest at all
  mode=api=404|401                 every /repos and /installation request answers that status
  mode=dl=corrupt|truncate|json    download serves a flipped byte / half the bytes / JSON metadata instead of the file
Fixed ids: release 7, asset 99.
"""
import hashlib
import http.server
import json
import os
import sys
import threading
import urllib.parse

D = sys.argv[1] if len(sys.argv) > 1 else "."
REL_ID, ASSET_ID = 7, 99
LOCK = threading.Lock()
STATE = {"release": None, "asset": None, "modes": {}}   # asset: {"name", "size", "digest"|None, "path"}
CDN_PORT = 0


def reset():
    STATE.update(release=None, asset=None, modes={})
    for n in ("req.log", "blob.log"):
        open(os.path.join(D, n), "w").close()


def log(name, line):
    with LOCK, open(os.path.join(D, name), "a") as f:
        f.write(line + "\n")


def sha_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def asset_json(a):
    d = {"id": ASSET_ID, "name": a["name"], "size": a["size"], "state": "uploaded"}
    if a["digest"]:
        d["digest"] = a["digest"]
    return d


class Base(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"            # lets Expect: 100-continue work for large uploads

    def log_message(self, *a):
        pass

    def send(self, code, body=b"", ctype="application/json", extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)


class Api(Base):
    def note(self):
        log("req.log", "%s %s auth=%s accept=%s" % (self.command, self.path, self.headers.get("Authorization"),
                                                    self.headers.get("Accept")))

    def api_mode(self):
        m = STATE["modes"].get("api")
        if m in ("404", "401"):
            self.send(int(m), b'{"message":"mock"}')
            return True
        return False

    def drain(self):
        n = int(self.headers.get("Content-Length") or 0)
        h, size = hashlib.sha256(), 0
        path = os.path.join(D, "asset.bin")
        with open(path, "wb") as f:
            while size < n:
                chunk = self.rfile.read(min(1 << 20, n - size))
                if not chunk:
                    break
                f.write(chunk)
                h.update(chunk)
                size += len(chunk)
        return path, size, h.hexdigest()

    def do_GET(self):
        u = urllib.parse.urlsplit(self.path)
        if u.path == "/_ctl":                           # control channel: not part of the API log
            q = urllib.parse.parse_qs(u.query)
            if q.get("reset"):
                reset()
            if q.get("preload"):
                p = q["preload"][0]
                STATE["release"] = REL_ID
                STATE["asset"] = {"name": "payload.bin", "size": os.path.getsize(p), "digest": "sha256:" + sha_of(p),
                                  "path": p}
            for m in q.get("mode", []):
                k, _, v = m.partition("=")
                STATE["modes"][k] = v
            return self.send(200, b"ok", "text/plain")
        self.note()
        if self.api_mode():
            return
        if u.path == "/repos/o/rev/releases":
            rels = []
            if STATE["release"]:
                a = STATE["asset"]
                rels.append({"id": REL_ID, "draft": True, "assets": [asset_json(a)] if a else []})
            return self.send(200, json.dumps(rels).encode())
        if u.path == "/repos/o/rev/releases/assets/%d" % ASSET_ID and STATE["asset"]:
            a = STATE["asset"]
            if "application/octet-stream" not in (self.headers.get("Accept") or "") or STATE["modes"].get("dl") == "json":
                # real GitHub answers JSON metadata when the Accept header is missing
                return self.send(200, json.dumps(asset_json(a)).encode())
            return self.send(302, b"", extra={"Location": "http://127.0.0.2:%d/blob/%d" % (CDN_PORT, ASSET_ID)})
        self.send(404, b'{"message":"Not Found"}')

    def do_POST(self):
        self.note()
        u = urllib.parse.urlsplit(self.path)
        if u.path == "/repos/o/rev/releases/%d/assets" % REL_ID:      # uploads host (same port here)
            path, size, sha = self.drain()
            mode = STATE["modes"].get("up")
            if self.api_mode():
                return
            if mode == "500":
                return self.send(500, b'{"message":"boom"}')
            name = urllib.parse.parse_qs(u.query).get("name", ["payload.bin"])[0]
            digest = {"liedigest": "sha256:" + "0" * 64, "nodigest": None}.get(mode, "sha256:" + sha)
            STATE["asset"] = {"name": name, "size": size, "digest": digest, "path": path}
            return self.send(201, json.dumps(asset_json(STATE["asset"])).encode())
        self.drain()
        if self.api_mode():
            return
        if u.path == "/repos/o/rev/releases":
            STATE["release"] = REL_ID
            return self.send(201, json.dumps({"id": REL_ID, "draft": True, "assets": []}).encode())
        self.send(404, b'{"message":"Not Found"}')

    def do_DELETE(self):
        self.note()
        u = urllib.parse.urlsplit(self.path)
        if u.path == "/repos/o/rev/releases/%d" % REL_ID:
            STATE["release"], STATE["asset"] = None, None
        self.send(204)                                   # repo delete and token revoke: success


class Cdn(Base):
    def do_GET(self):
        log("blob.log", "GET %s auth=%s" % (self.path, self.headers.get("Authorization")))
        a = STATE["asset"]
        if not a:
            return self.send(404, b"")
        data = open(a["path"], "rb").read()
        mode = STATE["modes"].get("dl")
        if mode == "corrupt":
            data = bytes([data[0] ^ 0xFF]) + data[1:]
        elif mode == "truncate":
            data = data[: len(data) // 2]
        self.send(200, data, "application/octet-stream")


if __name__ == "__main__":
    reset()
    cdn = http.server.ThreadingHTTPServer(("127.0.0.2", 0), Cdn)
    CDN_PORT = cdn.server_address[1]
    threading.Thread(target=cdn.serve_forever, daemon=True).start()
    api = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Api)
    with open(os.path.join(D, "port"), "w") as f:
        f.write(str(api.server_address[1]))
    api.serve_forever()
