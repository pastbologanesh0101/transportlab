# TransportLab

**A reliable transport protocol *and* a congestion-control arena, built from
scratch on top of UDP, made visible.**

TransportLab reimplements the interesting half of TCP — handshake, sliding
window, retransmission timers, the three classic ARQ strategies — then goes
further: pluggable congestion control (Tahoe / Reno / CUBIC / **BBR**), a
**multi-flow arena** where those algorithms fight over one bottleneck, a
**QUIC-style multiplexing** mode that kills head-of-line blocking, and a
**loss-sweep** that checks your measurements against the Mathis √p model. A live
browser dashboard shows every segment, window, and drop as a real file transfers
across a simulated bad link.

Everything runs on one machine. No hardware, no root, no external network, no
`pip install` — Python 3.9+ standard library only.

---

## 1. What it demonstrates (transport layer)

| Concept | Where | What you see |
|---|---|---|
| Framing, seq/ACK numbers, Internet checksum | `packet.py` | segments on the flow ladder; corrupted ones rejected |
| 3-way handshake / FIN teardown | `protocol.py` | violet SYN / FIN lines |
| Stop-and-Wait / Go-Back-N / Selective Repeat (+SACK) | `protocol.py` | goodput and retransmit counts side by side |
| Sliding-window flow control | `protocol.py` (`rwnd`) | sender clamped to `min(cwnd, rwnd)` |
| Adaptive RTO — Jacobson/Karels + Karn + backoff | `protocol.py` | RTT chart: sample vs SRTT vs RTO |
| Fast retransmit (3 dup ACKs), SACK recovery | `protocol.py` | amber dashed lines; dup-ACK counter |
| Slow start / AIMD / fast recovery | `congestion.py` | the cwnd sawtooth |
| Tahoe vs Reno vs CUBIC vs **BBR** | `congestion.py` | CUBIC beats Reno on long fat pipes; BBR ignores loss |
| **Fairness & the arena** | `session.py`, dashboard | AIMD convergence, RTT-unfairness, BBR vs loss-based, Jain index |
| **Bufferbloat** | `emulator.py` shaper | RTT balloons under the `bufferbloat` preset, zero loss |
| **Head-of-line blocking / why QUIC exists** | `protocol.py` (`mux`, `hol`) | independent streams keep flowing when one stalls |
| **Throughput ∝ 1/√p** | Lab → loss sweep | measured goodput tracks the Mathis curve |

---

## 2. Architecture

```
                 ┌───────────────────────────────────────────────┐
   browser  <──► │ dashboard.py  ── EventBus (events.py) ◄──────┐ │
  (SSE +         │ http.server        ▲                         │ │
   controls)     │      │ start/stop/link/params/sweep          │ │
                 │      ▼                                        │ │
                 │  session.py  — spawns N flows + the emulator  │ │
                 │      │                                        │ │
   flow 0  ──────┼─► sender ─┐                    ┌─► receiver ──┼─► received_0.bin
   flow 1  ──────┼─► sender ─┤   UDP  ┌────────┐  ├─► receiver ──┼─► received_1.bin
     ...         │           ├──────► │emulator│  ┤              │      (SHA-256 verified)
                 │  protocol.py│      │ (link) │  │ protocol.py  │
                 │             └───── └────────┘ ─┘              │ │
                 │              one shared rate + buffer         │ │
                 └───────────────────────────────────────────────┘
```

The endpoints only ever talk over real UDP sockets on `127.0.0.1`; running them
as threads in one process just makes start / stop / restart and telemetry
trivial. In the arena, every flow shares **one** emulator instance — one
bottleneck rate, one router buffer — which is what makes them compete.

---

## 3. Running it

### Dashboard

```bash
python3 run.py
```

Opens `http://127.0.0.1:8080/`. Pick an ARQ strategy, congestion control and
link preset, hit **Start**, and watch. Drag the impairment sliders *while it
runs*.

```bash
python3 run.py --preset satellite --cc cubic --size 3
python3 run.py --flows 3 --flow-cc reno,cubic,bbr --preset transoceanic   # arena
python3 run.py --mux 4 --preset mobile_handoff                            # QUIC streams
python3 run.py --headless        # don't open a browser
```

### Head-less (reproducible numbers)

```bash
python3 run.py --auto --preset wifi_cafe --arq go_back_n --cc reno --size 1
python3 run.py --auto --flows 3 --flow-cc reno,cubic,bbr --preset bufferbloat
python3 run.py --sweep --cc cubic --preset wifi_cafe        # loss sweep vs Mathis
```

`--auto` prints a JSON summary (per-flow goodput, retransmits, timeouts, max
cwnd, SRTT, Jain fairness index) and exits non-zero if any SHA-256 mismatched.

### Tests

```bash
python3 -m unittest discover -s tests -v
```

`test_packet` — wire format + checksum. `test_loopback` — a file survives an
8 % loss / reordering / corrupting link intact for every ARQ strategy and every
congestion controller. `test_arena` — 3 competing flows verify, BBR out-goodputs
Reno on a lossy link, the sweep emits Mathis predictions, the pcap is well
formed.

---

## 4. The dashboard

* **Segment flow** — live ladder. Sender left, receiver right, time scrolling
  down. DATA is tinted per flow; green = ACK, amber dashed = retransmit,
  red ✕ = dropped, violet = SYN/FIN.
* **Congestion window** — one cwnd line per flow. The sawtooth is
  slow-start → AIMD → loss → repeat; BBR's curve is visibly different.
* **Bandwidth share** *(arena)* — per-flow goodput, stacked. Watch a new flow
  push in and the shares re-divide.
* **Fairness** *(arena)* — for 2 flows, a phase plot of *cwnd₀ vs cwnd₁* with
  the *y = x* fair-share line (AIMD converges toward it); for 3–4 flows, the
  current goodput split + Jain's fairness index.
* **Stream delivery** *(mux)* — segments delivered per logical stream. In QUIC
  mode the lines advance independently; tick the **head-of-line blocking** box
  and they stall in lockstep — one lost segment freezes every stream.
* **Goodput / Round-trip time** — Mbps delivered; SRTT with RTO spikes on
  timeout.
* **Lab** — *Run loss sweep vs Mathis model*: fires ~8 short transfers at
  rising loss and overlays measured goodput on the `MSS·8 / (RTT·√(2p/3))`
  prediction.
* **Export** — `events.csv` / `events.jsonl` (every number came from here),
  `capture.pcap` (open in Wireshark), `charts.png`.

### Link presets

| Preset | Character |
|---|---|
| `pristine` | near-perfect loopback; the baseline |
| `wifi_cafe` | ~6 % loss, jitter, a shallow queue |
| `satellite` | 300 ms one-way, mild loss, long fat pipe |
| `mobile_handoff` | periodic 350 ms loss bursts + reordering |
| `transoceanic` | 4 % loss, corruption, reordering, 140 ms |
| `bufferbloat` | 5 Mbps behind a 384 KB buffer — watch RTT balloon |

---

## 5. Demo script (~6 min)

1. **`pristine`, Selective Repeat, Reno.** Clean cwnd sawtooth toward `rwnd`,
   ~20 Mb/s, zero retransmits.
2. **Drag loss to 8 % mid-transfer.** cwnd collapses, RTO spikes, red ✕s,
   goodput drops an order of magnitude — "throughput ∝ 1/√p" live.
3. **Restart as Go-Back-N**, then **Stop-and-Wait.** Retransmits ~4–5×; then
   every loss is a full timeout.
4. **`satellite`, Reno vs CUBIC** (single flow, switch CC). CUBIC's cubic growth
   fills the long fat pipe; Reno crawls.
5. **Arena: 3 flows, `reno,cubic,bbr`, `transoceanic`.** Bandwidth-share chart —
   BBR takes the biggest slice because it ignores the 4 % loss; Jain index drops.
   The classic BBR-unfairness result, on screen.
6. **`bufferbloat`.** Zero loss, RTT climbs to hundreds of ms.
7. **Lab → loss sweep.** Measured goodput lands on the Mathis curve.
8. **`mux 4`, `mobile_handoff`.** Toggle head-of-line blocking and re-run:
   independent stream lines vs lockstep stall.
9. **Export** the CSV / PNG / pcap.

---

## 6. What is real, what is simplified

**Real:** UDP sockets between endpoints; the Internet checksum; the 3-way
handshake; packet-indexed sequence numbers + cumulative ACKs; SACK; the
Jacobson/Karels RTO with Karn's algorithm and exponential backoff; fast
retransmit + SACK-based loss recovery; Tahoe/Reno slow-start + AIMD + fast
recovery; a fluid-queue shaper with tail drop; a shared bottleneck across flows;
a real `.pcap` (Ethernet/IPv4/UDP frames).

**Simplified (all noted in the source):**

* Sequence numbers count **segments**, not bytes — same concept, readable ladder.
* One congestion reaction per loss *episode* (~1 SRTT), mirroring TCP's "one
  reduction per RTT".
* **BBR** is the STARTUP/DRAIN/PROBE_BW/PROBE_RTT skeleton with a windowed
  BtlBw/RTprop estimate — no pacing engine, no HyStart; enough to show
  loss-agnostic, BDP-seeking behaviour.
* **CUBIC** is the RFC-8312 growth function without the TCP-friendly region.
* **QUIC mux** shares one reliability/congestion context (as QUIC does) and
  reassembles the file in order for the SHA-256 check; the per-stream *delivery*
  view is what would be handed to the app. The head-of-line contrast is
  strongest under burst loss (`mobile_handoff`).
* RTO capped at 4 s (real stacks: ~120 s) so a stalled lab demo recovers.
* SYN/FIN bypass the impairment engine so a 30 %-loss demo still connects.
* Endpoints run as threads in one process (still only talking over UDP).

---

## 7. File layout

```
run.py                     launcher / CLI  (--auto, --sweep, --flows, --mux, --hol)
transportlab/
  packet.py                wire format, flags, stream id, Internet checksum
  congestion.py            NoCC / Tahoe / Reno / Cubic / Bbr
  protocol.py              RDT engine: handshake, windows, timers, ARQ, mux
  emulator.py              shared bottleneck: loss/burst/delay/jitter/reorder/
                           dup/corrupt/rate/buffer, time-ordered delivery, capture
  events.py                telemetry bus (SSE + CSV feed off this)
  scenarios.py             link presets
  session.py               builds emulator + N flows; loss sweep
  dashboard.py             http.server: static files, SSE, control API, exports
  pcap.py                  writes a classic .pcap for Wireshark
web/  index.html  style.css  app.js        canvas dashboard, no dependencies
tests/  test_packet.py  test_loopback.py  test_arena.py
```
