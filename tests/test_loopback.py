"""End-to-end proof: a file survives a lossy, reordering, corrupting link
intact, for every ARQ strategy and a couple of congestion controllers."""

import itertools
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from transportlab.events import EventBus          # noqa: E402
from transportlab.session import Session          # noqa: E402

PORT = itertools.count(9900, 6)


def run_once(arq, cc, link, size=120_000, seed=7):
    bus = EventBus()
    got = {}
    sid, q = bus.subscribe()
    stop = threading.Event()

    def pump():
        while not stop.is_set():
            try:
                ev = q.get(timeout=0.3)
            except Exception:
                continue
            if ev["kind"] in ("verify", "stats"):
                got[ev["kind"]] = ev

    th = threading.Thread(target=pump, daemon=True)
    th.start()

    session = Session(bus, port_base=next(PORT), out_dir=tempfile.mkdtemp(prefix='tl_'), seed=seed)
    session.set_link(link)
    session.configure(arq=arq, cc=cc, rwnd=48, mss=1024, size_bytes=size)
    session.start(wait=True)
    stop.set()
    th.join(timeout=1)
    bus.unsubscribe(sid)

    with open(os.path.join(session.out_dir, "source.bin"), "rb") as f:
        src = f.read()
    with open(os.path.join(session.out_dir, "received_0.bin"), "rb") as f:
        dst = f.read()
    session.shutdown()
    return got, src, dst


class LoopbackTests(unittest.TestCase):
    LINK = dict(loss=0.08, corrupt=0.01, dup=0.02, reorder=0.05,
                latency_ms=8, jitter_ms=6)

    def test_all_arq_strategies_deliver_intact(self):
        for arq in ("stop_and_wait", "go_back_n", "selective_repeat"):
            with self.subTest(arq=arq):
                got, src, dst = run_once(arq, "reno", self.LINK)
                self.assertIn("verify", got, "no verify event")
                self.assertTrue(got["verify"]["ok"], got["verify"])
                self.assertEqual(src, dst)

    def test_congestion_controls_run(self):
        for cc in ("none", "tahoe", "reno", "cubic"):
            with self.subTest(cc=cc):
                got, src, dst = run_once("selective_repeat", cc,
                                         dict(loss=0.05, latency_ms=10, jitter_ms=4),
                                         size=200_000)
                self.assertTrue(got["verify"]["ok"], got["verify"])
                self.assertEqual(src, dst)

    def test_pristine_is_fast_and_clean(self):
        got, src, dst = run_once("selective_repeat", "reno",
                                 dict(loss=0.0, latency_ms=2, jitter_ms=0),
                                 size=400_000)
        self.assertEqual(src, dst)
        self.assertEqual(got["stats"]["timeouts"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
