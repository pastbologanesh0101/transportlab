"""A software "bad link".

The emulator owns one UDP socket sitting between the two endpoints.  The client
sends every segment to the emulator; the emulator forwards it to the server and
vice-versa, applying impairments on the way:

    loss          independent Bernoulli drop
    burst_loss    Gilbert-style: occasionally flip into a high-loss state
    corrupt       flip one random bit (the receiver's checksum then fails)
    dup           deliver the segment twice
    reorder       give this one segment a large extra delay so a later one
                  overtakes it
    latency/jitter base one-way delay + uniform jitter
    rate_kbps     token-bucket shaper; segments wait for capacity
    buffer_bytes  shaper queue limit -- excess segments are tail-dropped
                  (turn this up with a slow rate to demonstrate bufferbloat)

A single delivery thread drains a time-ordered heap, so ordering is exact:
segments stay FIFO per direction unless one is explicitly picked for
reordering.  Jitter therefore changes *spacing*, not order -- the ``reorder``
knob is the one control that produces out-of-order arrivals (and duplicate
ACKs).
"""

from __future__ import annotations

import heapq
import itertools
import random
import socket
import threading
import time
from typing import Optional, Tuple

from .events import EventBus
from .packet import ChecksumError, Packet

Addr = Tuple[str, int]

DEFAULTS = dict(
    loss=0.0,
    corrupt=0.0,
    dup=0.0,
    reorder=0.0,
    latency_ms=5.0,
    jitter_ms=0.0,
    reorder_ms=120.0,
    rate_kbps=0.0,      # 0 == unlimited
    buffer_bytes=0,     # 0 == unlimited
    burst_loss=0.0,     # loss probability while in the "bad" state
    burst_ms=250.0,     # how long a bad state lasts
    burst_enter=0.0,    # per-segment probability of entering the bad state
)


class LinkEmulator:
    """One shared bottleneck between one or more (client, server) flow pairs.

    Loss / corruption / delay / reordering are applied per segment.  The rate
    shaper and its finite buffer act on the *aggregate* of every flow in a
    direction -- that shared queue is what makes multiple flows compete, and is
    what the Arena view visualises.
    """

    def __init__(
        self,
        link_addr: Addr,
        flows,                       # list[(client_addr, server_addr)]
        bus: EventBus,
        config: Optional[dict] = None,
        seed: Optional[int] = None,
        capture: bool = False,
    ) -> None:
        self.link_addr = link_addr
        self.flows = list(flows)
        # routing table: any endpoint addr -> (peer addr, direction, flow id)
        self._route: dict = {}
        self._route_by_port: dict = {}
        for fid, (c, s) in enumerate(self.flows):
            self._route[c] = (s, "up", fid)
            self._route[s] = (c, "down", fid)
            self._route_by_port[c[1]] = (s, "up", fid)
            self._route_by_port[s[1]] = (c, "down", fid)
        self.bus = bus
        self.cfg = dict(DEFAULTS)
        if config:
            self.cfg.update(config)
        self._rnd = random.Random(seed)
        self._lock = threading.Lock()

        # optional packet capture for pcap export: (t, src_ip_port, dst_ip_port, raw)
        self.capture = capture
        self._cap: list = []
        self._cap_cap = 120_000

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(link_addr)
        self._sock.settimeout(0.3)

        self._stop = threading.Event()
        self._rx_thread = threading.Thread(target=self._rx_loop, name="link-rx", daemon=True)
        self._tx_thread = threading.Thread(target=self._tx_loop, name="link-tx", daemon=True)

        # time-ordered delivery queue
        self._cv = threading.Condition()
        self._heap: list = []
        self._seq = itertools.count()

        # fluid-queue shaper: _queue_bytes is the backlog waiting behind the
        # bottleneck; it drains at rate_bps and tail-drops past buffer_bytes.
        self._queue_bytes = 0.0
        self._last_drain = time.monotonic()
        self._last_deliver = {"up": 0.0, "down": 0.0}

        # burst-loss state
        self._burst_until = 0.0

        self.counts = dict(fwd=0, drop_loss=0, drop_buffer=0, corrupted=0,
                           duplicated=0, reordered=0)

    # -- lifecycle --------------------------------------------------------
    def start(self) -> None:
        self._rx_thread.start()
        self._tx_thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._cv:
            self._cv.notify_all()
        try:
            self._sock.close()
        except OSError:
            pass

    def update(self, changes: dict) -> dict:
        with self._lock:
            for k, v in changes.items():
                if k in self.cfg:
                    self.cfg[k] = type(DEFAULTS[k])(v)
            return dict(self.cfg)

    def get_config(self) -> dict:
        with self._lock:
            return dict(self.cfg)

    # -- receive side --------------------------------------------------
    def _rx_loop(self) -> None:
        while not self._stop.is_set():
            try:
                raw, src = self._sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            route = self._route.get(src) or self._route_by_port.get(src[1])
            if route is None:
                continue                     # unknown endpoint
            dst, direction, fid = route
            self._handle(raw, direction, dst, fid, src)

    def _record(self, t: float, src: Addr, dst: Addr, raw: bytes) -> None:
        if not self.capture:
            return
        self._cap.append((t, src, dst, raw))
        if len(self._cap) > self._cap_cap:
            del self._cap[: len(self._cap) - self._cap_cap]

    def capture_packets(self) -> list:
        return list(self._cap)

    def _peek(self, raw: bytes):
        try:
            return Packet.decode(raw)
        except (ChecksumError, ValueError):
            return None

    def _handle(self, raw: bytes, direction: str, dst: Addr, fid: int, src: Addr) -> None:
        cfg = self.get_config()
        pkt = self._peek(raw)
        seq = pkt.seq if pkt else -1
        kind = pkt.kind if pkt else "?"
        # Handshake / teardown segments (SYN, FIN) pass untouched so a demo with
        # heavy loss does not spend its first seconds just opening the link.
        control = pkt is not None and bool(pkt.flags & 0x05)  # SYN | FIN
        now = time.monotonic()

        # --- burst-loss (Gilbert) state machine ---
        in_burst = now < self._burst_until
        if (not in_burst and cfg["burst_enter"] > 0
                and self._rnd.random() < cfg["burst_enter"]):
            self._burst_until = now + cfg["burst_ms"] / 1000.0
            in_burst = True
        loss_p = cfg["burst_loss"] if in_burst else cfg["loss"]

        if not control and loss_p > 0 and self._rnd.random() < loss_p:
            self.counts["drop_loss"] += 1
            self.bus.publish("link_drop", flow=fid, dir=direction, seq=seq,
                             ptype=kind, reason="burst" if in_burst else "loss")
            return

        # --- fluid-queue shaper: adds queueing delay, tail-drops when full ---
        # NOTE: the backlog and buffer are shared across every flow -- that is
        # the shared bottleneck the Arena view is all about.
        queue_delay = 0.0
        size = len(raw)
        if cfg["rate_kbps"] > 0:
            rate_bps = cfg["rate_kbps"] * 1000.0 / 8.0
            with self._lock:
                self._queue_bytes = max(
                    0.0, self._queue_bytes - (now - self._last_drain) * rate_bps)
                self._last_drain = now
                if (cfg["buffer_bytes"]
                        and self._queue_bytes + size > cfg["buffer_bytes"]):
                    self.counts["drop_buffer"] += 1
                    self.bus.publish("link_drop", flow=fid, dir=direction, seq=seq,
                                     ptype=kind, reason="buffer_overflow")
                    return
                queue_delay = self._queue_bytes / rate_bps   # wait behind backlog
                self._queue_bytes += size

        # --- corruption: flip one bit; the receiver's checksum will fail ---
        out = raw
        if (not control and cfg["corrupt"] > 0
                and self._rnd.random() < cfg["corrupt"]):
            b = bytearray(raw)
            b[self._rnd.randrange(len(b))] ^= 1 << self._rnd.randrange(8)
            out = bytes(b)
            self.counts["corrupted"] += 1
            self.bus.publish("link_corrupt", flow=fid, dir=direction, seq=seq,
                             ptype=kind)

        # --- delay: base + jitter (+ a big extra shove if reordered) ---
        delay = cfg["latency_ms"] / 1000.0
        if cfg["jitter_ms"] > 0:
            delay += self._rnd.uniform(0.0, cfg["jitter_ms"] / 1000.0)
        delay += queue_delay
        reordered = (not control and cfg["reorder"] > 0
                     and self._rnd.random() < cfg["reorder"])
        if reordered:
            delay += cfg["reorder_ms"] / 1000.0
            self.counts["reordered"] += 1

        deliver_at = now + delay
        if not reordered:
            deliver_at = max(deliver_at, self._last_deliver[direction] + 1e-4)
            self._last_deliver[direction] = deliver_at

        dup = (not control and cfg["dup"] > 0 and self._rnd.random() < cfg["dup"])
        self.bus.publish("link_fwd", flow=fid, dir=direction, seq=seq, ptype=kind,
                         delay_ms=round((deliver_at - now) * 1000, 1), dup=dup)
        self._enqueue(deliver_at, out, dst, size, src)
        if dup:
            self.counts["duplicated"] += 1
            self._enqueue(deliver_at + 5e-4, out, dst, size, src)

    # -- transmit side -------------------------------------------------
    def _enqueue(self, deliver_at: float, data: bytes, dst: Addr, size: int,
                 src: Addr) -> None:
        with self._cv:
            heapq.heappush(self._heap,
                           (deliver_at, next(self._seq), data, dst, size, src))
            self._cv.notify()

    def _tx_loop(self) -> None:
        while not self._stop.is_set():
            with self._cv:
                while not self._heap and not self._stop.is_set():
                    self._cv.wait(timeout=0.25)
                if self._stop.is_set():
                    return
                deliver_at = self._heap[0][0]
                wait = deliver_at - time.monotonic()
                if wait > 0:
                    self._cv.wait(timeout=min(wait, 0.25))
                    continue
                _, _, data, dst, size, src = heapq.heappop(self._heap)
            try:
                self._sock.sendto(data, dst)
                self.counts["fwd"] += 1
                self._record(time.time(), src, dst, data)
            except OSError:
                pass
