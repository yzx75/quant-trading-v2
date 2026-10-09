# -*- coding: utf-8 -*-
"""main.py — v2 看板云端镜像（只读）
本地引擎每 5 秒 POST /push 推送看板数据（token 鉴权）；
访客 GET / 看板，/api/state 与本地同构，手机/外网直接看。
零第三方依赖（纯标准库）。"""
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8080"))
HERE = os.path.dirname(os.path.abspath(__file__))
STORE_PATH = os.path.join(HERE, "mirror.json")


def _load_token():
    if os.environ.get("MIRROR_TOKEN"):
        return os.environ["MIRROR_TOKEN"]
    try:
        for ln in open(os.path.join(HERE, ".env"), encoding="utf-8"):
            if ln.startswith("MIRROR_TOKEN="):
                return ln.split("=", 1)[1].strip()
    except Exception:
        pass
    return ""


TOKEN = _load_token()
STORE = {"payload": {}, "t": 0.0}


def _load_store():
    try:
        d = json.load(open(STORE_PATH, encoding="utf-8"))
        STORE["payload"] = d.get("payload") or {}
        STORE["t"] = float(d.get("t") or 0)
    except Exception:
        pass


def _save_store():
    try:
        json.dump(STORE, open(STORE_PATH, "w", encoding="utf-8"), ensure_ascii=False)
    except Exception:
        pass


class H(BaseHTTPRequestHandler):
    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/push":
            return self._send(404, "text/plain", b"not found")
        if not TOKEN or self.headers.get("X-Token") != TOKEN:
            return self._send(403, "text/plain", b"forbidden")
        n = int(self.headers.get("Content-Length") or 0)
        try:
            payload = json.loads(self.rfile.read(n).decode("utf-8"))
        except Exception:
            return self._send(400, "text/plain", b"bad json")
        STORE["payload"] = payload
        STORE["t"] = time.time()
        _save_store()
        self._send(200, "application/json", b'{"ok":true}')

    def do_GET(self):
        p = self.path.split("?")[0]
        if p in ("/", "/index.html"):
            try:
                html = open(os.path.join(HERE, "index.html"), "rb").read()
            except Exception:
                html = "<meta charset='utf-8'><h3>看板文件缺失</h3>".encode()
            self._send(200, "text/html; charset=utf-8", html)
        elif p == "/api/state":
            body = json.dumps(STORE["payload"], ensure_ascii=False).encode()
            self._send(200, "application/json; charset=utf-8", body)
        elif p == "/api/log":
            lines = (STORE["payload"] or {}).get("log_lines") or []
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(lines, ensure_ascii=False).encode())
        elif p == "/health":
            age = round(time.time() - STORE["t"]) if STORE["t"] else None
            self._send(200, "application/json",
                       json.dumps({"ok": True, "last_push_ago_s": age}).encode())
        else:
            self._send(404, "text/plain; charset=utf-8", "not found".encode())

    def log_message(self, *a):
        pass


_load_store()
ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
