"""Multi-flow arena, BBR, the loss sweep and pcap export."""

import itertools
import os
import struct
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from transportlab.events import EventBus          # noqa: E402
from transportlab.pcap import write_pcap          # noqa: E402
from transportlab.session import Session          # noqa: E402

PORT = itertools.count(10100, 12)


def collect(session, bus, kinds=("stats", "verify", "sweep_point", "run_end")):
    got = {"stats": {}, "verify": {}, "sweep_point": [], "run_end": None}
    stop = threading.Event()
    sid, q = bus.subscribe()

    def pump():
        while not stop.is_set():
            try:
                ev = q.get(timeout=0.3)
            except Exception:
                continue
            if ev["kind"] in ("stats", "verify"):
                got[ev["kind"]][ev.get("flow", 0)] = ev
            elif ev["kind"] == "sweep_point":
                got["sweep_point"].append(ev)
            elif ev["kind"] == "run_end":
                got["run_end"] = ev

    t = threading.Thread(target=pump, daemon=True)
    t.start()
    return got, stop, t, sid


class ArenaTests(unittest.TestCase):
    def test_three_flows_share_bottleneck_and_verify(self):
        bus = EventBus()
        session = Session(bus, port_base=next(PORT), out_dir=tempfile.mkdtemp(prefix='tl_'), seed=3)
        session.apply_preset("wifi_cafe")
        session.configure(size_bytes=150_000, flows=3, stagger_s=0.5,
                          flow_cc=["reno", "cubic", "bbr"])
        got, stop, t, sid = collect(session, bus)
        session.start(wait=True)
        stop.set(); t.join(timeout=1); bus.unsubscribe(sid)
        session.shutdown()

        self.assertEqual(got["run_end"]["flows"], 3)
        self.assertTrue(got["run_end"]["all_verified"], got["run_end"])
        for i in range(3):
            with open(os.path.join(session.out_dir, "source.bin"), "rb") as fh:
                src = fh.read()
            with open(os.path.join(session.out_dir, f"received_{i}.bin"), "rb") as fh:
                dst = fh.read()
            self.assertEqual(src, dst, f"flow {i} corrupted")

    def test_bbr_ignores_loss(self):
        """Given room to reach steady state on a lossy link, BBR out-goodputs
        Reno -- it does not treat loss as congestion."""
        def goodput(cc):
            bus = EventBus()
            s = Session(bus, port_base=next(PORT), out_dir=tempfile.mkdtemp(prefix='tl_'), seed=5)
            s.set_link({"loss": 0.08, "latency_ms": 30, "jitter_ms": 5,
                        "rate_kbps": 20000})
            s.configure(size_bytes=700_000, flows=1, cc=cc)
            got, stop, t, sid = collect(s, bus)
            s.start(wait=True)
            stop.set(); t.join(timeout=1); bus.unsubscribe(sid)
            g = got["stats"].get(0, {}).get("goodput_kbps", 0)
            s.shutdown()
            return g

        self.assertGreater(goodput("bbr"), goodput("reno") * 1.2)

    def test_sweep_emits_points_with_mathis(self):
        bus = EventBus()
        session = Session(bus, port_base=next(PORT), out_dir=tempfile.mkdtemp(prefix='tl_'), seed=1)
        session.apply_preset("wifi_cafe")
        session.configure(size_bytes=80_000)
        got, stop, t, sid = collect(session, bus)
        session.run_sweep(cc="reno", losses=[0.0, 0.02, 0.08])
        stop.set(); t.join(timeout=1); bus.unsubscribe(sid)
        session.shutdown()

        pts = got["sweep_point"]
        self.assertEqual(len(pts), 3)
        self.assertIsNone(pts[0]["mathis_kbps"])          # loss 0 -> no prediction
        self.assertIsNotNone(pts[2]["mathis_kbps"])       # loss 8% -> a number

    def test_pcap_is_well_formed(self):
        bus = EventBus()
        session = Session(bus, port_base=next(PORT), out_dir=tempfile.mkdtemp(prefix='tl_'), seed=2)
        session.configure(size_bytes=40_000)
        session.start(wait=True)
        blob = session.pcap_bytes()
        session.shutdown()

        magic, vmaj, vmin = struct.unpack("!IHH", blob[:8])
        self.assertEqual(magic, 0xA1B2C3D4)
        self.assertEqual((vmaj, vmin), (2, 4))
        self.assertGreater(len(blob), 24 + 100)          # header + some frames


if __name__ == "__main__":
    unittest.main(verbosity=2)
