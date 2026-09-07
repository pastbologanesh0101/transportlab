"""A tiny thread-safe telemetry bus.

The protocol engine, the emulator and the endpoints all call
:meth:`EventBus.publish`.  The dashboard opens a Server-Sent-Events stream and
drains a per-subscriber queue; the report exporter walks :attr:`EventBus.log`.

Every event is a plain dict with at least ``t`` (milliseconds since the bus was
created) and ``kind``.  Event kinds used in this project:

    hello                 one-shot, carries the run configuration
    tx / rx               a segment left / arrived at an endpoint
    link_drop             the emulator discarded a segment (reason=...)
    ack                   cumulative-ACK advanced at the sender
    dupack                a duplicate ACK was seen
    retransmit            a segment was resent (reason=timeout|fast)
    rto                   the retransmission timeout fired / backed off
    rtt                   a fresh RTT sample + smoothed SRTT/RTO
    cwnd                  congestion window / ssthresh / state changed
    rwnd                  advertised receive window changed
    progress              bytes delivered so far (drives the throughput chart)
    handshake / close     connection lifecycle
    verify                final SHA-256 comparison
    stats                 end-of-run summary
"""

from __future__ import annotations

import itertools
import queue
import threading
import time
from typing import Dict, Tuple

_T0 = time.monotonic()


def now_ms() -> float:
    return (time.monotonic() - _T0) * 1000.0


class EventBus:
    def __init__(self, log_cap: int = 250_000) -> None:
        self._subs: Dict[int, "queue.Queue[dict]"] = {}
        self._lock = threading.Lock()
        self._ids = itertools.count(1)
        self._log_cap = log_cap
        self.log: list[dict] = []

    def publish(self, kind: str, **data) -> dict:
        ev = {"t": round(now_ms(), 2), "kind": kind}
        ev.update(data)
        with self._lock:
            self.log.append(ev)
            if len(self.log) > self._log_cap:
                del self.log[: len(self.log) - self._log_cap]
            subs = list(self._subs.values())
        for q in subs:
            try:
                q.put_nowait(ev)
            except queue.Full:
                pass
        return ev

    def subscribe(self) -> Tuple[int, "queue.Queue[dict]"]:
        q: "queue.Queue[dict]" = queue.Queue(maxsize=20_000)
        sid = next(self._ids)
        with self._lock:
            self._subs[sid] = q
        return sid, q

    def unsubscribe(self, sid: int) -> None:
        with self._lock:
            self._subs.pop(sid, None)

    def snapshot(self) -> list[dict]:
        with self._lock:
            return list(self.log)
