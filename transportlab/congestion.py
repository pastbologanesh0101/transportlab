"""Congestion-control algorithms.

All of them work in units of **segments** (one MSS).  ``cwnd`` is kept as a
float so congestion avoidance can grow it by ``1/cwnd`` per ACK (i.e. ~1 MSS
per RTT).  The sender uses ``int(min(cwnd, rwnd))`` as the number of segments it
is allowed to have in flight.

    NoCC     window is limited only by the receiver (flow control only)
    Tahoe    slow start + AIMD; every loss -> ssthresh=cwnd/2, cwnd=1
    Reno     Tahoe + fast retransmit / fast recovery on 3 duplicate ACKs
    Cubic    RFC-8312-flavoured cubic growth (simplified, no HyStart)
    Bbr      model-based: cwnd = 2 x (BtlBw x RTprop), loss-agnostic
             (simplified: STARTUP / DRAIN / PROBE_BW gain cycle / PROBE_RTT)
"""

from __future__ import annotations

import math


class CongestionControl:
    name = "none"

    def __init__(self, iw: float = 1.0, ssthresh: float = 64.0) -> None:
        self.cwnd = float(iw)
        self.ssthresh = float(ssthresh)
        self.state = "slow_start"

    # --- events from the sender ------------------------------------------
    def on_ack(self, newly_acked: int, **kw) -> None:
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

    def on_ack(self, newly_acked: int, **kw) -> None:
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

    def on_ack(self, newly_acked: int, **kw) -> None:
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

    def on_ack(self, newly_acked: int, **kw) -> None:
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


class Bbr(CongestionControl):
    """A teaching-scale BBR.

    BBR ignores loss.  It continuously estimates the two things that actually
    define a path -- the bottleneck bandwidth ``BtlBw`` (a windowed max of the
    delivery rate) and the round-trip propagation delay ``RTprop`` (a windowed
    min of the RTT) -- and paces so that exactly one bandwidth-delay product of
    data is in flight:

        BDP  = BtlBw * RTprop
        cwnd = 2 * BDP            (headroom for delayed/aggregated ACKs)

    States: STARTUP doubles every RTT until bandwidth stops growing, DRAIN
    empties the queue STARTUP built, PROBE_BW cycles the pacing gain through
    [1.25, 0.75, 1, 1, 1, 1, 1, 1] to keep re-checking BtlBw, and PROBE_RTT
    briefly shrinks cwnd every ~10 s to re-measure RTprop.
    """

    name = "bbr"
    GAIN_CYCLE = (1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    STARTUP_GAIN = 2.885  # 2 / ln(2)
    CWND_GAIN = 2.0

    def __init__(self) -> None:
        super().__init__(iw=4.0, ssthresh=1_000_000.0)
        self.state = "startup"
        self.btlbw = 0.0          # bytes/s  (windowed max of delivery rate)
        self.rtprop = 1.0         # s        (windowed min of RTT)
        self.pacing_gain = self.STARTUP_GAIN
        self._mss = 1024.0
        self._acks = 0
        self._round = 0
        self._round_acks = 0
        self._bw_hist: list[tuple] = []       # (round, rate)
        self._full_bw = 0.0
        self._plateau = 0
        self._rtprop_round = 0
        self._cycle = 0
        self._cycle_round = -1
        self._probe_until = 0

    def _bdp(self) -> float:
        return max(4.0, self.btlbw * self.rtprop / self._mss)

    def on_ack(self, newly_acked: int, sample_rtt=None, delivery_rate=None,
               mss: float = 1024.0, **kw) -> None:
        self._mss = mss or self._mss
        self._acks += newly_acked

        # RTprop -- windowed min of the RTT, refreshed at least every ~10 rounds
        if sample_rtt and sample_rtt > 0:
            if sample_rtt < self.rtprop or self._round - self._rtprop_round > 10:
                self.rtprop = sample_rtt
                self._rtprop_round = self._round

        # BtlBw -- windowed max of the delivery rate over the last ~6 rounds
        if delivery_rate and delivery_rate > 0:
            self._bw_hist.append((self._round, delivery_rate))
            self._bw_hist = [(r, b) for r, b in self._bw_hist
                             if self._round - r <= 6]
            self.btlbw = max(b for _, b in self._bw_hist)

        # advance one "round" per cwnd worth of ACKs
        self._round_acks += newly_acked
        if self._round_acks >= max(self.cwnd, 1.0):
            self._round_acks = 0.0
            self._round += 1
            if self.state == "startup":
                if self.btlbw > self._full_bw * 1.25:
                    self._full_bw = self.btlbw
                    self._plateau = 0
                else:
                    self._plateau += 1
                if self._plateau >= 3:
                    self.state = "drain"

        bdp = self._bdp()

        if self.state == "startup":
            self.pacing_gain = self.STARTUP_GAIN
            if self.btlbw > 0:
                self.cwnd = self.STARTUP_GAIN * bdp
            else:                              # bootstrap: exponential for ~1 RTT
                self.cwnd += newly_acked

        elif self.state == "drain":
            self.pacing_gain = 1.0 / self.STARTUP_GAIN
            self.cwnd = bdp
            self.state = "probe_bw"            # one-shot drain

        else:                                  # probe_bw / probe_rtt
            if (self._round - self._rtprop_round > 12
                    and self.state != "probe_rtt"):
                self.state = "probe_rtt"
                self._probe_until = self._acks + max(bdp, 8)
            if self.state == "probe_rtt":
                self.cwnd = 4.0
                if self._acks >= self._probe_until:
                    self.state = "probe_bw"
                    self.rtprop = sample_rtt or self.rtprop
                    self._rtprop_round = self._round
            else:
                if self._round != self._cycle_round:
                    self._cycle_round = self._round
                    self._cycle = (self._cycle + 1) % len(self.GAIN_CYCLE)
                self.pacing_gain = self.GAIN_CYCLE[self._cycle]
                self.cwnd = self.CWND_GAIN * bdp

        # never let the estimate run away
        self.cwnd = min(self.cwnd, 4.0 * bdp, 4096.0)

    # BBR ignores loss entirely -- that is the whole point.
    def on_dupack(self) -> None: ...
    def on_fast_retransmit(self) -> None: ...

    def on_timeout(self) -> None:
        # a real stall (no ACKs at all) is the one thing it must respect
        self.cwnd = max(4.0, self._bdp())
        self.state = "startup"
        self._full_bw = 0.0
        self._plateau = 0

    def as_dict(self) -> dict:
        d = super().as_dict()
        d["btlbw_mbps"] = round(self.btlbw * 8 / 1e6, 3)
        d["rtprop_ms"] = round(self.rtprop * 1000, 2)
        d["pacing_gain"] = self.pacing_gain
        return d


_REGISTRY = {c.name: c for c in (NoCC, Tahoe, Reno, Cubic, Bbr)}


def make_cc(name: str) -> CongestionControl:
    try:
        return _REGISTRY[name]()
    except KeyError:
        raise ValueError(f"unknown congestion control {name!r}; "
                         f"choose from {sorted(_REGISTRY)}")
