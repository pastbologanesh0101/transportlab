#!/usr/bin/env python3
"""TransportLab launcher.

    python3 run.py                     # dashboard at http://127.0.0.1:8080
    python3 run.py --headless          # same, but do not open a browser
    python3 run.py --auto --preset wifi_cafe --arq go_back_n --cc reno
                                       # run one transfer, print stats, exit

Everything is standard-library Python 3.9+; nothing to pip install.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import webbrowser

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from transportlab.dashboard import make_server          # noqa: E402
from transportlab.events import EventBus                 # noqa: E402
from transportlab.scenarios import PRESETS              # noqa: E402
from transportlab.session import Session                # noqa: E402

ARQ = ("stop_and_wait", "go_back_n", "selective_repeat")
CC = ("none", "tahoe", "reno", "cubic")


def _common(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--arq", choices=ARQ, default="selective_repeat")
    ap.add_argument("--cc", choices=CC, default="reno")
    ap.add_argument("--preset", choices=sorted(PRESETS), default="pristine")
    ap.add_argument("--size", type=float, default=2.0, help="transfer size in MB")
    ap.add_argument("--mss", type=int, default=1024)
    ap.add_argument("--rwnd", type=int, default=64, help="advertised window (segments)")
    ap.add_argument("--loss", type=float, help="override link loss probability")
    ap.add_argument("--latency", type=float, help="override one-way latency (ms)")
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--port-base", type=int, default=9800)


def _configure(session: Session, a: argparse.Namespace) -> None:
    session.apply_preset(a.preset)
    over = {}
    if a.loss is not None:
        over["loss"] = a.loss
    if a.latency is not None:
        over["latency_ms"] = a.latency
    if over:
        session.set_link(over)
    session.configure(arq=a.arq, cc=a.cc, rwnd=a.rwnd, mss=a.mss,
                      size_bytes=int(a.size * 1_000_000))


def cmd_auto(a: argparse.Namespace) -> int:
    bus = EventBus()
    collected: dict[str, dict] = {}
    done = threading.Event()

    sid, q = bus.subscribe()

    def pump() -> None:
        while not done.is_set():
            try:
                ev = q.get(timeout=0.5)
            except Exception:
                continue
            if ev["kind"] in ("verify", "stats", "run_end"):
                collected[ev["kind"]] = ev
            if ev["kind"] == "run_end":
                done.set()

    threading.Thread(target=pump, daemon=True).start()

    session = Session(bus, port_base=a.port_base, seed=a.seed)
    _configure(session, a)
    print(f"[auto] arq={a.arq} cc={a.cc} preset={a.preset} "
          f"size={a.size}MB link={session.emulator.get_config()}")
    session.start(wait=True)
    done.wait(timeout=10)
    bus.unsubscribe(sid)
    session.shutdown()

    verify = collected.get("verify", {})
    stats = collected.get("stats", {})
    summary = {
        "verified": verify.get("ok"),
        "bytes": verify.get("bytes"),
        "segments": verify.get("segments"),
        "goodput_kbps": stats.get("goodput_kbps"),
        "seconds": stats.get("seconds"),
        "retransmits": stats.get("retransmits"),
        "fast_retx": stats.get("fast_retx"),
        "timeouts": stats.get("timeouts"),
        "dupacks": stats.get("dupacks"),
        "max_cwnd": stats.get("max_cwnd"),
        "srtt_ms": stats.get("srtt_ms"),
        "retransmit_overhead_pct": stats.get("overhead_pct"),
        "link_forwarded": session.emulator.counts["fwd"],
        "link_dropped_loss": session.emulator.counts["drop_loss"],
        "link_dropped_buffer": session.emulator.counts["drop_buffer"],
        "link_corrupted": session.emulator.counts["corrupted"],
        "link_reordered": session.emulator.counts["reordered"],
    }
    print(json.dumps(summary, indent=2))
    return 0 if verify.get("ok") else 1


def cmd_dashboard(a: argparse.Namespace) -> int:
    bus = EventBus()
    session = Session(bus, port_base=a.port_base, seed=a.seed)
    _configure(session, a)
    httpd = make_server(session, bus, os.path.join(HERE, "web"),
                        host="127.0.0.1", port=a.port)
    url = f"http://127.0.0.1:{a.port}/"
    print(f"TransportLab dashboard  ->  {url}")
    print("  arq =", a.arq, " cc =", a.cc, " preset =", a.preset,
          " size =", a.size, "MB")
    print("  Ctrl-C to quit.")

    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    if not a.headless:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    if a.autostart:
        threading.Timer(1.4, session.start).start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        httpd.shutdown()
        session.shutdown()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--auto", action="store_true",
                    help="run one transfer head-less and print a JSON summary")
    ap.add_argument("--headless", action="store_true",
                    help="serve the dashboard but do not open a browser")
    ap.add_argument("--no-autostart", dest="autostart", action="store_false",
                    help="do not kick off a transfer automatically")
    ap.add_argument("--port", type=int, default=8080)
    _common(ap)
    a = ap.parse_args(argv)
    try:
        return cmd_auto(a) if a.auto else cmd_dashboard(a)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
