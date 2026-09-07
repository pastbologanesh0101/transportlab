"""Congestion-control algorithms.

All of them work in units of **segments** (one MSS).  ``cwnd`` is kept as a
float so congestion avoidance can grow it by ``1/cwnd`` per ACK (i.e. ~1 MSS
per RTT).  The sender uses ``int(min(cwnd, rwnd))`` as the number of segments it
is allowed to have in flight.

    NoCC     window is limited only by the receiver (flow control only)
    Tahoe    slow start + AIMD; every loss -> ssthresh=cwnd/2, cwnd=1
    Reno     Tahoe + fast retransmit / fast recovery on 3 duplicate ACKs
    Cubic    RFC-8312-flavoured cubic growth (simplified, no HyStart)
"""

from __future__ import annotations


class CongestionControl:
    name = "none"

    def __init__(self, iw: float = 1.0, ssthresh: float = 64.0) -> None:
        self.cwnd = float(iw)
        self.ssthresh = float(ssthresh)
        self.state = "slow_start"

    # --- events from the sender ------------------------------------------
    def on_ack(self, newly_acked: int) -> None:
        """``newly_acked`` = number of segments the cumulative ACK just cleared."""

    def on_dupack(self) -> None:
        """A single duplicate ACK (n < 3)."""

    def on_fast_retransmit(self) -> None:
        """3 duplicate ACKs -> fast retransmit."""

    def on_timeout(self) -> None:
        """The retransmission timer expired."""

    # --- introspection -------------------------------------------------
    def as_dict(self) -> dict:
        return {
            "cc": self.name,
            "cwnd": round(self.cwnd, 3),
            "ssthresh": round(self.ssthresh, 3),
            "state": self.state,
        }


class NoCC(CongestionControl):
    name = "none"

    def __init__(self) -> None:
        super().__init__(iw=1_000_000.0, ssthresh=1_000_000.0)
        self.state = "disabled"


class Tahoe(CongestionControl):
    name = "tahoe"

    def __init__(self) -> None:
        super().__init__(iw=1.0, ssthresh=32.0)

    def on_ack(self, newly_acked: int) -> None:
        for _ in range(max(1, newly_acked)):
            if self.cwnd < self.ssthresh:
                self.cwnd += 1.0  # slow start: exponential
                self.state = "slow_start"
            else:
                self.cwnd += 1.0 / self.cwnd  # congestion avoidance: linear
                self.state = "congestion_avoidance"

    def _collapse(self) -> None:
        self.ssthresh = max(2.0, self.cwnd / 2.0)
        self.cwnd = 1.0
        self.state = "slow_start"

    on_fast_retransmit = _collapse
    on_timeout = _collapse


class Reno(CongestionControl):
    name = "reno"

    def __init__(self) -> None:
        super().__init__(iw=1.0, ssthresh=32.0)

    def on_ack(self, newly_acked: int) -> None:
        if self.state == "fast_recovery":
            # A fresh ACK ends fast recovery: deflate the window.
            self.cwnd = self.ssthresh
            self.state = "congestion_avoidance"
            return
        for _ in range(max(1, newly_acked)):
            if self.cwnd < self.ssthresh:
                self.cwnd += 1.0
                self.state = "slow_start"
            else:
                self.cwnd += 1.0 / self.cwnd
                self.state = "congestion_avoidance"

    def on_dupack(self) -> None:
        if self.state == "fast_recovery":
            self.cwnd += 1.0  # window inflation while recovering

    def on_fast_retransmit(self) -> None:
        self.ssthresh = max(2.0, self.cwnd / 2.0)
        self.cwnd = self.ssthresh + 3.0
        self.state = "fast_recovery"

    def on_timeout(self) -> None:
        self.ssthresh = max(2.0, self.cwnd / 2.0)
        self.cwnd = 1.0
        self.state = "slow_start"


class Cubic(CongestionControl):
    name = "cubic"
    C = 0.4
    BETA = 0.7

    def __init__(self) -> None:
        super().__init__(iw=1.0, ssthresh=32.0)
        self._w_max = 0.0
        self._k = 0.0
        self._t = 0.0  # RTTs since the last reduction

    def _reduce(self) -> None:
        self._w_max = self.cwnd
        self.ssthresh = max(2.0, self.cwnd * self.BETA)
        self.cwnd = self.ssthresh
        self._k = (self._w_max * (1.0 - self.BETA) / self.C) ** (1.0 / 3.0)
        self._t = 0.0
        self.state = "cubic_recovery"

    on_fast_retransmit = _reduce

    def on_timeout(self) -> None:
        self._reduce()
        self.cwnd = 1.0
        self.state = "slow_start"

    def on_ack(self, newly_acked: int) -> None:
        n = max(1, newly_acked)
        if self.cwnd < self.ssthresh:
            self.cwnd += float(n)
            self.state = "slow_start"
            return
        # Advance the cubic clock ~1 RTT per cwnd segments acked.
        self._t += n / max(self.cwnd, 1.0)
        target = self.C * (self._t - self._k) ** 3 + self._w_max
        if target > self.cwnd:
            self.cwnd += (target - self.cwnd) / max(self.cwnd, 1.0) * n
        else:
            self.cwnd += 0.01 * n  # TCP-friendly floor growth
        self.state = "cubic_avoidance"


_REGISTRY = {c.name: c for c in (NoCC, Tahoe, Reno, Cubic)}


def make_cc(name: str) -> CongestionControl:
    try:
        return _REGISTRY[name]()
    except KeyError:
        raise ValueError(f"unknown congestion control {name!r}; "
                         f"choose from {sorted(_REGISTRY)}")
