"""The reliable data-transfer engine.

One :class:`Connection` object per endpoint.  It runs a single thread with a
``select`` loop, so all protocol state is touched from exactly one place and
there are no locks to reason about.

What it implements, and where it lives in a Computer Networks course:

* 3-way handshake (SYN / SYN-ACK / ACK) and FIN / FIN-ACK teardown
* packet-indexed sequence numbers + cumulative ACKs
* three ARQ strategies -- ``stop_and_wait``, ``go_back_n``, ``selective_repeat``
  (the last one also emits SACK blocks)
* sliding-window flow control: the sender never has more than
  ``min(cwnd, advertised_rwnd)`` segments in flight
* adaptive retransmission timeout via the Jacobson/Karels estimator, with
  Karn's algorithm (ignore RTT samples from retransmitted segments) and
  exponential backoff
* fast retransmit on 3 duplicate ACKs
* pluggable congestion control (see :mod:`transportlab.congestion`)
"""

from __future__ import annotations

import hashlib
import select
import socket
import time
from typing import Callable, Dict, List, Optional, Set

from .congestion import make_cc
from .events import EventBus
from .packet import (
    FLAG_ACK,
    FLAG_DATA,
    FLAG_FIN,
    FLAG_SACK,
    FLAG_SYN,
    MAX_SACK,
    ChecksumError,
    Packet,
)

MONO = time.monotonic

MIN_RTO = 0.15
MAX_RTO = 4.0            # lab-scale cap (real stacks use ~120 s)
CLOCK_G = 0.010          # assumed clock granularity for the RTO floor
FIN_TRIES = 6
SYN_TRIES = 8


class Connection:
    def __init__(
        self,
        role: str,                    # "client" | "server"
        sock: socket.socket,
        link_addr,                    # where to send segments (the emulator)
        bus: EventBus,
        *,
        arq: str = "selective_repeat",
        cc: str = "reno",
        rwnd: int = 64,
        mss: int = 1024,
        stop_flag=None,
        flow_id: int = 0,
        label: str = "",
        mux: int = 1,
        hol: bool = False,
    ) -> None:
        self.role = role
        self.sock = sock
        self.link_addr = link_addr
        self.bus = bus
        self.arq = arq
        self.mss = mss
        self.mux = max(1, int(mux))        # number of logical streams (QUIC-style)
        self.hol = bool(hol)              # True == one byte stream (TCP-style HoL)
        self.rwnd_self = rwnd          # what we advertise to the peer
        self.cc = make_cc(cc)
        self.cc_name = cc
        self.stop_flag = stop_flag
        self.flow_id = flow_id
        self.label = label or f"flow {flow_id}"
        self.sock.setblocking(False)

        # delivery-rate estimate (bytes/s) averaged over ~1 RTT, for
        # model-based congestion control such as BBR
        self.delivery_rate = 0.0
        self._total_acked = 0
        self._deliv: List[tuple] = []   # (t, cumulative acked bytes)

        # RTT / RTO estimator
        self.srtt: Optional[float] = None
        self.rttvar: Optional[float] = None
        self.rto = 1.0

        # ---- sender state (client) ----
        self.chunks: List[bytes] = []
        self.N = 0
        self.file_digest = b""
        self.snd_base = 0
        self.snd_next = 0
        self.rwnd_peer = rwnd
        self.acked: Set[int] = set()          # SACKed seqs above snd_base
        self.send_ts: Dict[int, float] = {}   # seq -> send time (Karn-valid only)
        self.retx: Set[int] = set()
        self.retx_at: Dict[int, float] = {}   # seq -> last (re)transmit time
        self.sr_timers: Dict[int, float] = {}
        self.gbn_deadline: Optional[float] = None
        self.dupacks = 0
        self._last_cc_loss_t = 0.0
        self.fin_sent = False
        self.fin_acked = False
        self.fin_deadline: Optional[float] = None
        self.fin_tries = 0
        self._first_data_t: Optional[float] = None
        self._last_ack_t: Optional[float] = None

        # ---- receiver state (server) ----
        self.rcv_next = 0
        self.rcv_buf: Dict[int, bytes] = {}
        self.on_deliver: Optional[Callable[[bytes], None]] = None
        self.bytes_delivered = 0
        self.peer_digest: Optional[bytes] = None
        self.finished = False
        self._recv_hash = hashlib.sha256()
        self._last_progress = 0.0
        self._arrived: Set[int] = set()        # every DATA seq seen (for mux viz)
        self._stream_seg = [0] * self.mux      # per-stream contiguous prefix
        self._stream_emitted = [0] * self.mux

        # counters (both roles)
        self.stat = dict(retransmits=0, timeouts=0, fast_retx=0, dupacks=0,
                         max_cwnd=1.0, corrupt_rx=0)

    # ================================================================
    #  helpers
    # ================================================================
    def _ev(self, kind: str, **data) -> dict:
        """Publish a telemetry event, tagged with this flow's id."""
        return self.bus.publish(kind, flow=self.flow_id, **data)

    def _send(self, pkt: Packet, who: str) -> None:
        try:
            self.sock.sendto(pkt.encode(), self.link_addr)
        except OSError:
            return
        self._ev("tx", who=who, seq=pkt.seq, ack=pkt.ack,
                 ptype=pkt.kind, retransmit=False)

    def _rtt_sample(self, r: float) -> None:
        if self.srtt is None:
            self.srtt, self.rttvar = r, r / 2.0
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - r)
            self.srtt = 0.875 * self.srtt + 0.125 * r
        # RFC 6298: RTO = SRTT + max(G, 4*RTTVAR), then clamp.
        self.rto = min(max(self.srtt + max(CLOCK_G, 4.0 * self.rttvar),
                           MIN_RTO), MAX_RTO)
        self._ev("rtt", sample_ms=round(r * 1000, 2),
                         srtt_ms=round(self.srtt * 1000, 2),
                         rttvar_ms=round(self.rttvar * 1000, 2),
                         rto_ms=round(self.rto * 1000, 2))

    def _emit_cwnd(self) -> None:
        d = self.cc.as_dict()
        self.stat["max_cwnd"] = max(self.stat["max_cwnd"], d["cwnd"])
        self._ev("cwnd", **d, rwnd=self.rwnd_peer,
                         inflight=self._inflight())

    def _inflight(self) -> int:
        if self.arq == "selective_repeat":
            return sum(1 for s in range(self.snd_base, self.snd_next)
                       if s not in self.acked)
        return self.snd_next - self.snd_base

    def _eff_window(self) -> int:
        if self.arq == "stop_and_wait":
            return 1
        return max(1, int(min(self.cc.cwnd, self.rwnd_peer)))

    def _stopping(self) -> bool:
        return self.stop_flag is not None and self.stop_flag.is_set()

    # ================================================================
    #  CLIENT  (file sender)
    # ================================================================
    def send_file(self, data: bytes) -> None:
        self.chunks = [data[i : i + self.mss] for i in range(0, len(data), self.mss)] or [b""]
        self.N = len(self.chunks)
        self.file_digest = hashlib.sha256(data).digest()
        self._ev("hello", role="client", arq=self.arq, cc=self.cc.name,
                         segments=self.N, bytes=len(data), mss=self.mss,
                         rwnd=self.rwnd_self)

        if not self._handshake_client():
            self._ev("close", role="client", reason="handshake_failed")
            return

        self._emit_cwnd()
        while not self._stopping():
            self._fill_window()
            timeout = self._next_wakeup()
            try:
                ready, _, _ = select.select([self.sock], [], [], timeout)
            except (OSError, ValueError):
                break
            if ready:
                self._drain_socket_client()
            self._check_timers_client()

            if self.snd_base >= self.N and self._inflight() == 0:
                if not self.fin_sent:
                    self._send_fin()
                elif self.fin_acked:
                    break
                elif self.fin_deadline and MONO() >= self.fin_deadline:
                    self._send_fin(resend=True)
                    if self.fin_tries >= FIN_TRIES:
                        break

        self._emit_client_stats()
        self._ev("close", role="client", reason="done")

    def _handshake_client(self) -> bool:
        rto = 0.5
        for _ in range(SYN_TRIES):
            if self._stopping():
                return False
            self._send(Packet(FLAG_SYN, seq=0, window=self.rwnd_self), "client")
            ready, _, _ = select.select([self.sock], [], [], rto)
            if not ready:
                rto = min(rto * 2, 4.0)
                continue
            try:
                raw, _ = self.sock.recvfrom(65535)
                pkt = Packet.decode(raw)
            except (ChecksumError, ValueError, OSError):
                continue
            if pkt.has(FLAG_SYN) and pkt.has(FLAG_ACK):
                self.rwnd_peer = pkt.window or self.rwnd_peer
                self._send(Packet(FLAG_ACK, seq=1, ack=pkt.seq + 1,
                                  window=self.rwnd_self), "client")
                self._ev("handshake", role="client", state="established",
                                 rwnd_peer=self.rwnd_peer)
                return True
        return False

    def _fill_window(self) -> None:
        eff = self._eff_window()
        while self._inflight() < eff and self.snd_next < self.N:
            self._transmit(self.snd_next, retx=False)
            self.snd_next += 1

    def _transmit(self, seq: int, *, retx: bool, reason: str = "") -> None:
        pkt = Packet(FLAG_DATA, seq=seq, ack=0, window=self.rwnd_self,
                     payload=self.chunks[seq], stream=seq % self.mux)
        try:
            self.sock.sendto(pkt.encode(), self.link_addr)
        except OSError:
            return
        now = MONO()
        if self._first_data_t is None:
            self._first_data_t = now
        self.retx_at[seq] = now
        if retx:
            self.retx.add(seq)
            self.send_ts.pop(seq, None)
            self.stat["retransmits"] += 1
            if reason == "fast":
                self.stat["fast_retx"] += 1
            self._ev("retransmit", seq=seq, reason=reason)
        else:
            self.send_ts[seq] = now
        self._ev("tx", who="client", seq=seq, ptype="DATA", retransmit=retx)
        self._arm_timer(seq)

    def _arm_timer(self, seq: int) -> None:
        deadline = MONO() + self.rto
        if self.arq == "selective_repeat":
            self.sr_timers[seq] = deadline
        elif self.gbn_deadline is None:
            self.gbn_deadline = deadline

    def _next_wakeup(self) -> float:
        now = MONO()
        deadlines: List[float] = []
        if self.arq == "selective_repeat":
            deadlines.extend(self.sr_timers.values())
        elif self.gbn_deadline is not None:
            deadlines.append(self.gbn_deadline)
        if self.fin_sent and not self.fin_acked and self.fin_deadline:
            deadlines.append(self.fin_deadline)
        if not deadlines:
            return 0.05
        return min(max(min(deadlines) - now, 0.0), 0.1)

    def _drain_socket_client(self) -> None:
        while True:
            try:
                raw, _ = self.sock.recvfrom(65535)
            except BlockingIOError:
                return
            except OSError:
                return
            try:
                pkt = Packet.decode(raw)
            except ChecksumError:
                self.stat["corrupt_rx"] += 1
                continue
            except ValueError:
                continue
            self._on_ack(pkt)

    def _on_ack(self, pkt: Packet) -> None:
        if pkt.has(FLAG_SYN):
            return
        if pkt.has(FLAG_FIN):          # FIN-ACK for our teardown
            self.fin_acked = True
            return
        self._ev("rx", who="client", seq=pkt.seq, ack=pkt.ack, ptype="ACK")
        self.rwnd_peer = pkt.window or self.rwnd_peer

        for s in pkt.sacks:
            if s >= self.snd_base:
                self.acked.add(s)
                self.sr_timers.pop(s, None)   # SACKed: stop timing it

        cum = pkt.ack
        now = MONO()
        if cum > self.snd_base:
            newly = cum - self.snd_base
            sample = None
            if (cum - 1) in self.send_ts and (cum - 1) not in self.retx:
                sample = now - self.send_ts[cum - 1]
                self._rtt_sample(sample)
            # delivery rate (bytes/s), measured over a ~1 RTT window with a
            # 30 ms floor so a burst of closely-spaced ACKs can't spike it
            self._total_acked += newly * self.mss
            if not self._deliv or now - self._deliv[-1][0] >= 0.005:
                self._deliv.append((now, self._total_acked))
            horizon = now - max(2.0 * (self.srtt or 0.1), 0.1)
            while len(self._deliv) > 2 and self._deliv[0][0] < horizon:
                self._deliv.pop(0)
            t_old, b_old = self._deliv[0]
            span = now - t_old
            if span >= max(0.8 * (self.srtt or 0.1), 0.03):
                self.delivery_rate = (self._total_acked - b_old) / span
            for s in range(self.snd_base, cum):
                self.send_ts.pop(s, None)
                self.sr_timers.pop(s, None)
                self.retx_at.pop(s, None)
                self.acked.discard(s)
                self.retx.discard(s)
            self.snd_base = cum
            while self.snd_base in self.acked and self.snd_base < self.snd_next:
                self.acked.discard(self.snd_base)
                self.sr_timers.pop(self.snd_base, None)
                self.send_ts.pop(self.snd_base, None)
                self.snd_base += 1
            self.dupacks = 0
            self._last_ack_t = now
            # Forward progress means the path is delivering again: undo any
            # exponential backoff so the next loss is not punished by a stale,
            # inflated RTO.
            if self.srtt is not None:
                self.rto = min(max(self.srtt + max(CLOCK_G, 4 * self.rttvar),
                                   MIN_RTO), 2.0)
            self.cc.on_ack(newly, sample_rtt=sample or self.srtt,
                           delivery_rate=self.delivery_rate, mss=self.mss)
            self._emit_cwnd()
            self._ev("ack", cum=cum, base=self.snd_base, next=self.snd_next)
            if self.arq != "selective_repeat":
                self.gbn_deadline = now + self.rto if self.snd_base < self.snd_next else None
        elif cum == self.snd_base and self.snd_base < self.snd_next:
            self.dupacks += 1
            self.stat["dupacks"] += 1
            self._ev("dupack", ack=cum, n=self.dupacks)
            if self.dupacks == 3:
                self._cc_loss("fast")
                self._fast_retransmit()
            elif self.dupacks > 3:
                self.cc.on_dupack()
                self._emit_cwnd()
                if self.arq == "selective_repeat":
                    self._sack_recovery()

    def _cc_loss(self, kind: str) -> None:
        """Apply one congestion reaction per loss *episode*.

        A burst that trips several timers (or many duplicate ACKs) should halve
        the window once, not drive it to the floor -- mirroring TCP's "one
        reduction per RTT" rule.
        """
        now = MONO()
        if now - self._last_cc_loss_t < max(self.srtt or 0.1, 0.05):
            return
        self._last_cc_loss_t = now
        if kind == "timeout":
            self.cc.on_timeout()
        else:
            self.cc.on_fast_retransmit()
        self._emit_cwnd()

    def _fast_retransmit(self) -> None:
        if self.arq == "selective_repeat":
            self._sack_recovery()
        else:
            self._transmit(self.snd_base, retx=True, reason="fast")
            self.gbn_deadline = MONO() + self.rto

    def _sack_recovery(self) -> None:
        """Resend every gap the SACK blocks reveal, at most once per RTT each.

        This is the idea behind RFC 6675: with selective ACKs the sender knows
        which segments are missing, so it need not wait for a timeout on the
        second, third, ... loss in the same window.
        """
        now = MONO()
        guard = max(self.srtt or 0.05, 0.05)
        hi = max(self.acked) if self.acked else self.snd_next
        sent = False
        for s in range(self.snd_base, hi):
            if s in self.acked:
                continue
            if now - self.retx_at.get(s, -1e9) >= guard:
                self._transmit(s, retx=True, reason="fast")
                sent = True
        if (not sent and self.snd_base < self.snd_next
                and self.snd_base not in self.acked
                and now - self.retx_at.get(self.snd_base, -1e9) >= guard):
            self._transmit(self.snd_base, retx=True, reason="fast")

    def _check_timers_client(self) -> None:
        now = MONO()
        if self.arq == "selective_repeat":
            due = [s for s, d in list(self.sr_timers.items()) if d <= now]
            # A timer is only "real" if that segment is still an outstanding
            # hole; stale timers (SACKed / already ACKed) are just dropped so
            # they cannot trigger a retransmission storm.
            real = [s for s in due
                    if s not in self.acked and self.snd_base <= s < self.snd_next]
            for s in due:
                if s not in real:
                    self.sr_timers.pop(s, None)
            if real:
                self.stat["timeouts"] += 1
                self.rto = min(self.rto * 2.0, MAX_RTO)
                self._ev("rto", scope="sr", count=len(real),
                                 rto_ms=round(self.rto * 1000, 2))
                self._cc_loss("timeout")
                for s in sorted(real):
                    self._transmit(s, retx=True, reason="timeout")
        else:
            if self.gbn_deadline is not None and now >= self.gbn_deadline:
                self.stat["timeouts"] += 1
                self.rto = min(self.rto * 2.0, MAX_RTO)
                self._ev("rto", scope="gbn",
                                 rto_ms=round(self.rto * 1000, 2))
                self._cc_loss("timeout")
                for s in range(self.snd_base, self.snd_next):
                    self._transmit(s, retx=True, reason="timeout")
                self.gbn_deadline = now + self.rto if self.snd_base < self.snd_next else None

    def _send_fin(self, resend: bool = False) -> None:
        self.fin_sent = True
        self.fin_tries += 1
        self.fin_deadline = MONO() + max(self.rto, 0.4)
        self._send(Packet(FLAG_FIN, seq=self.N, window=self.rwnd_self,
                          payload=self.file_digest), "client")
        if resend:
            self._ev("retransmit", seq=self.N, reason="fin")

    def _emit_client_stats(self) -> None:
        start = self._first_data_t or MONO()
        end = self._last_ack_t or MONO()
        secs = max(end - start, 1e-6)
        total = sum(len(c) for c in self.chunks)
        self._ev(
            "stats", role="client",
            bytes=total, seconds=round(secs, 3),
            goodput_kbps=round(total * 8 / secs / 1000, 1),
            retransmits=self.stat["retransmits"],
            fast_retx=self.stat["fast_retx"],
            timeouts=self.stat["timeouts"],
            dupacks=self.stat["dupacks"],
            max_cwnd=round(self.stat["max_cwnd"], 2),
            srtt_ms=round((self.srtt or 0) * 1000, 2),
            overhead_pct=round(self.stat["retransmits"] / max(self.N, 1) * 100, 1),
            fin_acked=self.fin_acked,
        )

    # ================================================================
    #  SERVER  (file receiver)
    # ================================================================
    def recv_file(self, on_deliver: Callable[[bytes], None]) -> None:
        self.on_deliver = on_deliver
        self._ev("hello", role="server", arq=self.arq, rwnd=self.rwnd_self)
        while not self._stopping():
            try:
                ready, _, _ = select.select([self.sock], [], [], 0.3)
            except (OSError, ValueError):
                break
            if not ready:
                continue
            self._drain_socket_server()
            if self.finished:
                # Linger briefly so a retransmitted FIN still gets a FIN-ACK.
                end = MONO() + 1.0
                while MONO() < end and not self._stopping():
                    r, _, _ = select.select([self.sock], [], [], 0.2)
                    if r:
                        self._drain_socket_server()
                break
        self._ev("close", role="server", reason="done")

    def _drain_socket_server(self) -> None:
        while True:
            try:
                raw, src = self.sock.recvfrom(65535)
            except BlockingIOError:
                return
            except OSError:
                return
            try:
                pkt = Packet.decode(raw)
            except ChecksumError:
                self.stat["corrupt_rx"] += 1
                self._ev("rx_drop", who="server", reason="checksum")
                continue
            except ValueError:
                continue
            self._on_segment_server(pkt, src)

    def _adv_window(self) -> int:
        # Advertised window shrinks as the reassembly buffer fills (flow control).
        return max(1, self.rwnd_self - len(self.rcv_buf))

    def _sacks(self) -> List[int]:
        return sorted(self.rcv_buf)[:MAX_SACK]

    def _ack_packet(self) -> Packet:
        flags = FLAG_ACK | (FLAG_SACK if self.rcv_buf else 0)
        return Packet(flags, seq=0, ack=self.rcv_next,
                      window=self._adv_window(), sacks=self._sacks())

    def _on_segment_server(self, pkt: Packet, src) -> None:
        if pkt.has(FLAG_SYN):
            self.link_addr = src
            self._send(Packet(FLAG_SYN | FLAG_ACK, seq=0, ack=pkt.seq + 1,
                              window=self.rwnd_self), "server")
            self._ev("handshake", role="server", state="syn_received")
            return

        if pkt.has(FLAG_FIN):
            self.peer_digest = pkt.payload
            self._send(Packet(FLAG_FIN | FLAG_ACK, seq=0, ack=self.rcv_next,
                              window=self.rwnd_self), "server")
            if not self.finished:
                self._finalise()
            return

        if pkt.has(FLAG_DATA):
            self._ev("rx", who="server", seq=pkt.seq, ptype="DATA")
            if self.mux > 1:
                self._arrived.add(pkt.seq)
            self._accept(pkt.seq, pkt.payload)
            self._send(self._ack_packet(), "server")
            self._maybe_progress()
            self._stream_progress()

    def _accept(self, seq: int, payload: bytes) -> None:
        if self.arq == "selective_repeat":
            if seq < self.rcv_next:
                return                       # duplicate; ACK already covers it
            if seq == self.rcv_next:
                self._deliver(payload)
                while self.rcv_next in self.rcv_buf:
                    self._deliver(self.rcv_buf.pop(self.rcv_next))
            elif seq < self.rcv_next + self.rwnd_self:
                self.rcv_buf.setdefault(seq, payload)
            # else: outside window -> drop
        else:  # stop_and_wait / go_back_n: strictly in-order
            if seq == self.rcv_next:
                self._deliver(payload)
            else:
                self._ev("rx_drop", who="server", seq=seq,
                                 reason="out_of_order")

    def _deliver(self, payload: bytes) -> None:
        if self.on_deliver:
            self.on_deliver(payload)
        self._recv_hash.update(payload)
        self.bytes_delivered += len(payload)
        self.rcv_next += 1

    def _maybe_progress(self) -> None:
        now = MONO()
        if now - self._last_progress >= 0.05:
            self._last_progress = now
            self._ev("progress", side="server", bytes=self.bytes_delivered,
                             segments=self.rcv_next, buffered=len(self.rcv_buf))

    def _stream_progress(self) -> None:
        """Per-stream deliverable prefix, for the QUIC vs head-of-line demo.

        QUIC (hol=False): a stream advances as soon as *its* next segment has
        arrived, in any order -- a loss on one stream never stalls another.
        TCP  (hol=True) : the whole connection is one byte stream, so a
        stream's data is only released once global in-order delivery reaches
        it -- one lost segment freezes every stream.
        """
        if self.mux <= 1:
            return
        for s in range(self.mux):
            nxt = s + self._stream_seg[s] * self.mux
            if self.hol:
                while nxt < self.rcv_next:
                    self._stream_seg[s] += 1
                    nxt += self.mux
            else:
                while nxt in self._arrived:
                    self._stream_seg[s] += 1
                    nxt += self.mux
            # emit only when this stream actually moved, so the chart shows
            # QUIC streams advancing independently vs HoL streams in lockstep
            if self._stream_seg[s] != self._stream_emitted[s]:
                self._stream_emitted[s] = self._stream_seg[s]
                self._ev("stream_progress", stream=s,
                         segments=self._stream_seg[s],
                         bytes=self._stream_seg[s] * self.mss)

    def _finalise(self) -> None:
        self.finished = True
        recv_digest = self._recv_hash.digest()
        ok = self.peer_digest == recv_digest
        self._ev("progress", side="server", bytes=self.bytes_delivered,
                         segments=self.rcv_next, buffered=len(self.rcv_buf))
        self._ev(
            "verify", ok=bool(ok), bytes=self.bytes_delivered,
            segments=self.rcv_next,
            sent_sha256=(self.peer_digest or b"").hex(),
            recv_sha256=recv_digest.hex(),
        )
