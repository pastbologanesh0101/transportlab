#!/usr/bin/env python3
"""TransportLab launcher.

    python3 run.py                        # dashboard at http://127.0.0.1:8080
    python3 run.py --headless             # same, but don't open a browser
    python3 run.py --auto --preset wifi_cafe --arq go_back_n --cc reno
                                          # one transfer, print JSON, exit
    python3 run.py --auto --flows 3 --flow-cc reno,cubic,bbr --preset bufferbloat
                                          # Arena: 3 flows share one bottleneck
    python3 run.py --sweep --cc cubic     # loss sweep vs the Mathis model

Standard-library Python 3.9+; nothing to pip install.
"""

from __future__ import annotations

import argparse
import json
import math
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
CC = ("none", "tahoe", "reno", "cubic", "bbr")


def _common(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--arq", choices=ARQ, default="selective_repeat")
    ap.add_argument("--cc", choices=CC, default="reno")
    ap.add_argument("--preset", choices=sorted(PRESETS), default="pristine")
    ap.add_argument("--size", type=float, default=2.0, help="transfer size in MB")
    ap.add_argument("--mss", type=int, default=1024)
    ap.add_argument("--rwnd", type=int, default=64, help="advertised window (segments)")
    ap.add_argument("--flows", type=int, default=1, help="number of competing flows (1-4)")
    ap.add_argument("--flow-cc", default="", help="comma list of per-flow CC, e.g. reno,cubic,bbr")
    ap.add_argument("--stagger", type=float, default=2.0, help="seconds between flow starts")
    ap.add_argument("--mux", type=int, default=1, help="QUIC-style logical streams over one connection")
    ap.add_argument("--hol", action="store_true", help="one byte stream (TCP-style head-of-line blocking)")
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
    flow_cc = [s.strip() for s in a.flow_cc.split(",") if s.strip()] or None
    session.configure(arq=a.arq, cc=a.cc, rwnd=a.rwnd, mss=a.mss,
                      size_bytes=int(a.size * 1_000_000),
                      flows=a.flows, stagger_s=a.stagger, flow_cc=flow_cc,
                      mux=a.mux, hol=1 if a.hol else 0)


def _run_and_collect(session: Session, bus: EventBus, timeout: float = 900):
    """Run one transfer to completion; return {flow: {stats, verify}}."""
    per: dict[int, dict] = {}
    done = threading.Event()
    sid, q = bus.subscribe()

    def pump() -> None:
        while not done.is_set():
            try:
                ev = q.get(timeout=0.4)
            except Exception:
                continue
            if ev["kind"] in ("stats", "verify"):
                per.setdefault(ev.get("flow", 0), {})[ev["kind"]] = ev
            elif ev["kind"] == "run_end":
                per["_end"] = ev
                done.set()

    threading.Thread(target=pump, daemon=True).start()
    session.start(wait=True)
    done.wait(timeout=10)
    bus.unsubscribe(sid)
    return per


def cmd_auto(a: argparse.Namespace) -> int:
    bus = EventBus()
    session = Session(bus, port_base=a.port_base, seed=a.seed)
    _configure(session, a)
    n = session.params["flows"]
    ccs = session.flow_cc[:n] if n > 1 else [a.cc]
    print(f"[auto] preset={a.preset} arq={a.arq} flows={n} cc={ccs} "
          f"size={a.size}MB link={session.emulator.get_config()}")

    per = _run_and_collect(session, bus)
    session.shutdown()

    flows_out = []
    for i in range(n):
        s = per.get(i, {}).get("stats", {})
        v = per.get(i, {}).get("verify", {})
        flows_out.append({
            "flow": i, "cc": ccs[i] if i < len(ccs) else a.cc,
            "verified": v.get("ok"),
            "goodput_kbps": s.get("goodput_kbps"),
            "seconds": s.get("seconds"),
            "retransmits": s.get("retransmits"),
            "fast_retx": s.get("fast_retx"),
            "timeouts": s.get("timeouts"),
            "max_cwnd": s.get("max_cwnd"),
            "srtt_ms": s.get("srtt_ms"),
        })
    summary = {
        "flows": flows_out,
        "all_verified": all(f["verified"] for f in flows_out),
        "link": {k: session.emulator.counts[k] for k in
                 ("fwd", "drop_loss", "drop_buffer", "corrupted", "reordered")},
    }
    if n > 1 and all(f["goodput_kbps"] for f in flows_out):
        tot = sum(f["goodput_kbps"] for f in flows_out)
        sq = sum(f["goodput_kbps"] ** 2 for f in flows_out)
        summary["jain_fairness"] = round(tot * tot / (n * sq), 4)   # 1.0 == perfectly fair
    print(json.dumps(summary, indent=2))
    return 0 if summary["all_verified"] else 1


def cmd_sweep(a: argparse.Namespace) -> int:
    """Sweep loss, compare measured goodput to the Mathis sqrt(p) model."""
    losses = [0.0, 0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.12]
    mss_bits = a.mss * 8
    print(f"[sweep] cc={a.cc} arq={a.arq} size={a.size}MB  (loss -> goodput)")
    print(f"{'loss%':>6} {'measured kbps':>14} {'Mathis kbps':>13} {'rtt ms':>8}")
    rows = []
    for p in losses:
        bus = EventBus()
        session = Session(bus, port_base=a.port_base, seed=a.seed)
        session.apply_preset(a.preset)
        session.set_link({"loss": p} | ({"latency_ms": a.latency} if a.latency else {}))
        session.configure(arq=a.arq, cc=a.cc, rwnd=a.rwnd, mss=a.mss,
                          size_bytes=int(a.size * 1_000_000), flows=1)
        per = _run_and_collect(session, bus)
        session.shutdown()
        s = per.get(0, {}).get("stats", {})
        g = s.get("goodput_kbps") or 0.0
        rtt = (s.get("srtt_ms") or 0.0) / 1000.0
        mathis = (mss_bits / (rtt * math.sqrt(2 * p / 3)) / 1000.0) if (p > 0 and rtt) else None
        rows.append({"loss": p, "measured_kbps": g, "mathis_kbps": mathis,
                     "srtt_ms": s.get("srtt_ms")})
        print(f"{p*100:>6.1f} {g:>14.1f} "
              f"{('%.1f' % mathis) if mathis else '   n/a':>13} {s.get('srtt_ms', 0):>8.1f}")
    print(json.dumps({"cc": a.cc, "rows": rows}, indent=2))
    return 0


def cmd_dashboard(a: argparse.Namespace) -> int:
    bus = EventBus()
    session = Session(bus, port_base=a.port_base, seed=a.seed)
    _configure(session, a)
    httpd = make_server(session, bus, os.path.join(HERE, "web"),
                        host="127.0.0.1", port=a.port)
    url = f"http://127.0.0.1:{a.port}/"
    print(f"TransportLab dashboard  ->  {url}")
    print(f"  preset={a.preset} arq={a.arq} flows={session.params['flows']} "
          f"size={a.size}MB")
    print("  Ctrl-C to quit.")

    threading.Thread(target=httpd.serve_forever, daemon=True).start()
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
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--auto", action="store_true",
                    help="run one transfer head-less and print a JSON summary")
    ap.add_argument("--sweep", action="store_true",
                    help="sweep loss and compare goodput to the Mathis model")
    ap.add_argument("--headless", action="store_true",
                    help="serve the dashboard but do not open a browser")
    ap.add_argument("--no-autostart", dest="autostart", action="store_false",
                    help="do not kick off a transfer automatically")
    ap.add_argument("--port", type=int, default=8080)
    _common(ap)
    a = ap.parse_args(argv)
    try:
        if a.sweep:
            return cmd_sweep(a)
        return cmd_auto(a) if a.auto else cmd_dashboard(a)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
