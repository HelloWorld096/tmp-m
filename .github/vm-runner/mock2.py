# Mock GitHub for the reverse road: API + uploads host (127.0.0.1) and a CDN host (localhost).
import http.server, json, sys, os, hashlib, urllib.parse, threading
D = sys.argv[1]; TOKEN = "Bearer SECRET-TOKEN-0123456789"
S = {"releases": [], "blob": b"", "mode": set(), "next": 7}
def log(f, line): open(os.path.join(D, f), "a").write(line + "\n")
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def _send(self, code, body=b"", ctype="application/json", extra=None):
        self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items(): self.send_header(k, v)
        self.end_headers(); self.wfile.write(body)
    def _auth_ok(self):
        if "api=401" in S["mode"]: self._send(401, b"{}"); return False
        if "api=404" in S["mode"]: self._send(404, b"{}"); return False
        if self.headers.get("Authorization") != TOKEN: self._send(401, b"{}"); return False
        return True
    def do_GET(self):
        u = urllib.parse.urlparse(self.path); q = urllib.parse.parse_qs(u.query)
        if u.path == "/_ctl":
            if q.get("reset"): S.update(releases=[], blob=b"", mode=set(), next=7); open(os.path.join(D,"req.log"),"w").close(); open(os.path.join(D,"blob.log"),"w").close()
            if q.get("mode"): S["mode"] = set(q["mode"][0].split(","))
            if q.get("preload"):
                data = open(q["preload"][0], "rb").read(); S["blob"] = data
                S["releases"] = [{"id": 7, "assets": [{"id": 99, "name": "payload.bin", "size": len(data), "state": "uploaded",
                                  "digest": "sha256:" + hashlib.sha256(data).hexdigest()}]}]
            return self._send(200, b"ok")
        if self.headers.get("Host", "").startswith("localhost"):          # the CDN host
            log("blob.log", "auth=%s" % self.headers.get("Authorization"))
            data = S["blob"]
            if "dl=truncate" in S["mode"]: data = data[: len(data) // 2]
            if "dl=corrupt" in S["mode"] and data: data = bytes([data[0] ^ 0xFF]) + data[1:]
            return self._send(200, data, "application/octet-stream")
        log("req.log", "GET %s auth=%s accept=%s" % (u.path, self.headers.get("Authorization"), self.headers.get("Accept")))
        if not self._auth_ok(): return
        if u.path.endswith("/releases"):   # mimic real GitHub: an empty list is pretty-printed as "[\n\n]"
            return self._send(200, json.dumps(S["releases"]).encode() if S["releases"] else b"[\n\n]")
        if "/releases/assets/" in u.path:
            if "octet-stream" in (self.headers.get("Accept") or ""):
                if "dl=json" in S["mode"]: return self._send(200, b'{"oops":"metadata not file"}')
                return self._send(302, b"", extra={"Location": "http://localhost:%d/blob/99" % self.server.server_address[1]})
            return self._send(200, b'{"metadata":true}')
        self._send(404, b"{}")
    def do_POST(self):
        u = urllib.parse.urlparse(self.path); n = int(self.headers.get("Content-Length") or 0)
        body = b""
        while len(body) < n: body += self.rfile.read(min(1 << 20, n - len(body)))
        log("req.log", "POST %s auth=%s" % (u.path, self.headers.get("Authorization")))
        if not self._auth_ok(): return
        if u.path.endswith("/releases"):
            rid = S["next"]; S["releases"].append({"id": rid, "assets": []}); return self._send(201, json.dumps({"id": rid}).encode())
        if "/assets" in u.path:
            if "up=500" in S["mode"]: return self._send(500, b'{"message":"boom"}')
            name = urllib.parse.parse_qs(u.query)["name"][0]; S["blob"] = body
            dig = "sha256:" + hashlib.sha256(body).hexdigest()
            if "up=liedigest" in S["mode"]: dig = "sha256:" + "0" * 64
            a = {"id": 99, "name": name, "size": len(body), "state": "uploaded", "digest": dig}
            if "up=nodigest" in S["mode"]: del a["digest"]
            S["releases"][-1]["assets"].append(a); return self._send(201, json.dumps(a).encode())
        self._send(404, b"{}")
    def do_DELETE(self):
        log("req.log", "DELETE %s auth=%s" % (self.path, self.headers.get("Authorization"))); self._send(204)
    def log_message(self, *a): pass
s = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
open(os.path.join(D, "port"), "w").write(str(s.server_address[1])); s.serve_forever()
