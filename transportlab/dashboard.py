"""HTTP dashboard: static files + a Server-Sent-Events telemetry stream + a
small JSON control API.  Pure ``http.server`` -- no third-party packages.

Endpoints
---------
GET  /                 the dashboard page (web/index.html)
GET  /<asset>          web/app.js, web/style.css, ...
GET  /events           text/event-stream of every EventBus event
GET  /state            JSON snapshot (link config, run params, counters)
POST /link             {loss: 0.1, latency_ms: 40, ...}  live link impairments
POST /preset           {name: "satellite"}
POST /params           {arq: "reno"...} applied on the next run
POST /run              {action: "start" | "stop"}
GET  /export.csv       every event so far, flattened -- paste into a report
GET  /export.jsonl     same, as newline-delimited JSON
"""

from __future__ import annotations

import json
import os
import queue
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .events import EventBus
from .scenarios import DESCRIPTIONS, PRESETS
from .session import Session

_CTYPE = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
}


def make_server(session: Session, bus: EventBus, web_dir: str,
                host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):  # keep the console clean
            pass

        # -- helpers ------------------------------------------------
        def _send(self, code: int, body: bytes, ctype: str,
                  extra: dict | None = None) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _json(self, obj, code: int = 200) -> None:
            self._send(code, json.dumps(obj).encode(), "application/json")

        def _body(self) -> dict:
            n = int(self.headers.get("Content-Length", 0) or 0)
            if not n:
                return {}
            try:
                return json.loads(self.rfile.read(n) or b"{}")
            except json.JSONDecodeError:
                return {}

        # -- GET --------------------------------------------------
        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path == "/":
                return self._asset("index.html")
            if path == "/events":
                return self._events()
            if path == "/state":
                return self._json(self._full_state())
            if path == "/export.csv":
                return self._export_csv()
            if path == "/export.jsonl":
                return self._export_jsonl()
            if path == "/export.pcap":
                data = session.pcap_bytes()
                return self._send(200, data, "application/vnd.tcpdump.pcap",
                                  {"Content-Disposition":
                                   "attachment; filename=transportlab.pcap"})
            if path.startswith("/") and ".." not in path:
                return self._asset(path.lstrip("/"))
            self._send(404, b"not found", "text/plain")

        def _asset(self, rel: str) -> None:
            fp = os.path.join(web_dir, rel)
            if not os.path.isfile(fp):
                return self._send(404, b"not found", "text/plain")
            with open(fp, "rb") as f:
                data = f.read()
            ext = os.path.splitext(fp)[1]
            self._send(200, data, _CTYPE.get(ext, "application/octet-stream"))

        def _full_state(self) -> dict:
            s = session.state()
            s["presets"] = {k: DESCRIPTIONS.get(k, "") for k in PRESETS}
            s["events_logged"] = len(bus.log)
            return s

        def _events(self) -> None:
            sid, q = bus.subscribe()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            try:
                # replay recent history so a fresh page has context
                for ev in bus.snapshot()[-3000:]:
                    self.wfile.write(f"data: {json.dumps(ev)}\n\n".encode())
                self.wfile.write(b": synced\n\n")
                self.wfile.flush()
                last_ping = time.monotonic()
                while True:
                    try:
                        ev = q.get(timeout=1.0)
                        self.wfile.write(f"data: {json.dumps(ev)}\n\n".encode())
                        self.wfile.flush()
                    except queue.Empty:
                        if time.monotonic() - last_ping > 10:
                            self.wfile.write(b": ping\n\n")
                            self.wfile.flush()
                            last_ping = time.monotonic()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                bus.unsubscribe(sid)

        def _export_csv(self) -> None:
            rows = bus.snapshot()
            cols: list[str] = ["t", "kind"]
            for r in rows:
                for k in r:
                    if k not in cols:
                        cols.append(k)
            out = [",".join(cols)]
            for r in rows:
                out.append(",".join(_csv_cell(r.get(c, "")) for c in cols))
            self._send(200, ("\n".join(out)).encode(), "text/csv",
                       {"Content-Disposition": "attachment; filename=transportlab.csv"})

        def _export_jsonl(self) -> None:
            body = "\n".join(json.dumps(r) for r in bus.snapshot())
            self._send(200, body.encode(), "application/x-ndjson",
                       {"Content-Disposition": "attachment; filename=transportlab.jsonl"})

        # -- POST -------------------------------------------------
        def do_POST(self) -> None:
            path = self.path.split("?", 1)[0]
            body = self._body()
            try:
                if path == "/link":
                    return self._json(session.set_link(body))
                if path == "/preset":
                    return self._json(session.apply_preset(body.get("name", "")))
                if path == "/params":
                    return self._json(session.configure(**body))
                if path == "/run":
                    if body.get("action") == "stop":
                        session.stop()
                    else:
                        session.start()
                    return self._json({"running": session.running})
                if path == "/sweep":
                    cc = body.get("cc")
                    threading.Thread(target=session.run_sweep,
                                     kwargs={"cc": cc}, daemon=True).start()
                    return self._json({"started": True})
            except (KeyError, ValueError, TypeError) as e:
                return self._json({"error": str(e)}, code=400)
            self._send(404, b"not found", "text/plain")

    class Server(ThreadingHTTPServer):
        daemon_threads = True

        def handle_error(self, request, client_address):
            # SSE clients drop their connection all the time; that is not news.
            exc = sys.exc_info()[1]
            if isinstance(exc, (ConnectionError, BrokenPipeError)):
                return
            super().handle_error(request, client_address)

    return Server((host, port), Handler)


def _csv_cell(v) -> str:
    s = str(v)
    if any(c in s for c in ',"\n'):
        return '"' + s.replace('"', '""') + '"'
    return s
