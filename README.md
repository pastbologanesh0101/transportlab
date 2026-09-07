# TransportLab

**A reliable transport protocol, built from scratch on top of UDP, made visible.**

TransportLab reimplements the interesting half of TCP — the handshake, the
sliding window, retransmission timers, the three classic ARQ strategies and
pluggable congestion control — as a small pure‑Python program, then puts a live
browser dashboard on top so you can *watch* every segment, every drop, every
window adjustment as a real file transfers across a simulated bad link.

Everything runs on one machine. No hardware, no root, no external network, no
`pip install` — Python 3.9+ standard library only.

---

## 1. What it demonstrates (Computer Networks — transport layer)

| Concept | Where it lives | What you can see |
|---|---|---|
| Framing, sequence / ACK numbers | `transportlab/packet.py` | every segment on the flow ladder |
| Internet checksum, error detection | `packet.py` + emulator `corrupt` | corrupted segments rejected at the receiver |
| 3‑way handshake / FIN teardown | `protocol.py` | violet SYN / FIN lines |
| Stop‑and‑Wait / Go‑Back‑N / Selective Repeat | `protocol.py` (`arq=`) | throughput and retransmit counts side by side |
| Selective ACK (SACK) | `protocol.py` | Selective Repeat resends only the holes |
| Sliding‑window flow control | `protocol.py` (`rwnd`) | sender clamped to `min(cwnd, rwnd)` |
| Adaptive RTO (Jacobson/Karels + Karn) | `protocol.py` `_rtt_sample` | RTT chart: sample vs smoothed SRTT vs RTO |
| Fast retransmit (3 duplicate ACKs) | `protocol.py` | amber dashed lines, dup‑ACK counter |
| Slow start / AIMD / fast recovery | `congestion.py` | the cwnd sawtooth chart |
| Tahoe vs Reno vs CUBIC | `congestion.py` (`cc=`) | CUBIC beats Reno on the satellite link |
| Congestion collapse, bufferbloat | `emulator.py` shaper | RTT balloons under the `bufferbloat` preset |
| Throughput ∝ 1/√p | any lossy preset | goodput craters as you raise the loss slider |

---

## 2. Architecture

```
                 ┌─────────────────────────────────────────┐
                 │            run.py  (one process)         │
                 │                                          │
  browser  <───► │  dashboard.py   ──── EventBus ◄────────┐ │
  (SSE +         │  http.server         (events.py)       │ │
   controls)     │      │                                 │ │
                 │      │ start/stop/link/params          │ │
                 │      ▼                                  │ │
                 │  session.py                             │ │
                 │      │ spawns 3 threads                 │ │
                 │      ▼                                  │ │
                 │  ┌────────┐   UDP    ┌──────────┐  UDP  ┌────────┐
                 │  │ sender │ ───────► │ emulator │ ────► │receiver│
                 │  │(client)│ ◄─────── │  (link)  │ ◄──── │(server)│
                 │  └────────┘          └──────────┘       └────────┘
                 │   protocol.py         emulator.py        protocol.py
                 └─────────────────────────────────────────┘
        source.bin  ──────────────────────────────────►  received.bin
                        (SHA-256 verified end to end)
```

The three endpoints only ever talk through real UDP sockets on `127.0.0.1`, so
it is a genuine networked protocol; running them as threads in one process just
makes start/stop/restart and telemetry trivial.

---

## 3. Running it

### Dashboard (the main way)

```bash
python3 run.py
```

Opens `http://127.0.0.1:8080/`. Pick an ARQ strategy, a congestion control and a
link preset, hit **Start transfer**, and watch. Drag the impairment sliders
*while it runs* — they take effect live.

Useful flags:

```bash
python3 run.py --preset satellite --arq selective_repeat --cc cubic --size 3
python3 run.py --headless          # don't auto-open a browser
python3 run.py --no-autostart      # wait for you to press Start
python3 run.py --port 9000
```

### Headless (for the report — reproducible numbers)

```bash
python3 run.py --auto --preset wifi_cafe --arq go_back_n --cc reno --size 1
```

Runs one transfer and prints a JSON summary (goodput, retransmits, timeouts,
max cwnd, SRTT, link drop counts). Exit code 0 iff the SHA‑256 matched.

### Tests

```bash
python3 -m unittest discover -s tests -v
```

`test_packet.py` checks the wire format and that the checksum catches
corruption. `test_loopback.py` pushes a file through an 8 %‑loss, reordering,
corrupting link for **every** ARQ strategy and every congestion controller and
asserts the received bytes are identical.

---

## 4. Dashboard tour

* **Segment flow** — a live ladder diagram. Sender on the left rail, receiver on
  the right, time scrolling downward. Cyan = DATA, green = ACK, amber dashed =
  retransmit, red ✕ = dropped, violet = SYN/FIN. This is the "what is actually
  on the wire right now" view.
* **Congestion window** — `cwnd` (solid), `ssthresh` (dashed), advertised
  `rwnd` (grey). The sawtooth is slow start → AIMD → loss → repeat.
* **Goodput** — application bytes delivered per second.
* **Round‑trip time** — raw RTT sample, smoothed SRTT, and the current RTO. RTO
  spikes mark timeouts.
* **Stat tiles** — goodput, progress, cwnd, in‑flight, SRTT, CC state,
  retransmits, timeouts, dup ACKs.
* **Export** — `events.csv` / `events.jsonl` (the full event log — every number
  in this project came from there) and `charts.png` (stitches the three scope
  charts for pasting into a report).

### Link presets

| Preset | Character |
|---|---|
| `pristine` | near‑perfect loopback; the baseline |
| `wifi_cafe` | ~6 % loss, jitter, a shallow queue |
| `satellite` | 300 ms one‑way, mild loss, long fat pipe |
| `mobile_handoff` | periodic 350 ms loss bursts + reordering |
| `transoceanic` | 4 % loss, corruption, reordering, 140 ms |
| `bufferbloat` | 5 Mbps behind a 384 KB buffer — watch RTT balloon |

---

## 5. A 4‑minute demo script

1. **`pristine` + Selective Repeat + Reno.** Start. Point out the clean cwnd
   sawtooth ramping toward `rwnd`, ~20 Mb/s goodput, zero retransmits.
2. **Drag the loss slider to 8 % mid‑transfer.** cwnd collapses, RTO spikes,
   red ✕s appear, goodput drops an order of magnitude. This is
   "throughput ∝ 1/√p" happening in front of you.
3. **Restart as Go‑Back‑N, same link.** Retransmit count roughly 4–5×; whole
   windows resent on every loss. Restart as Stop‑and‑Wait — now it is painfully
   slow and every loss is a full timeout.
4. **`satellite` preset, Reno vs CUBIC.** Reno crawls (cwnd ~20); CUBIC's cubic
   growth curve fills the long fat pipe far better (cwnd ~50+, ~2× goodput).
5. **`bufferbloat` preset.** Zero loss, but the RTT chart climbs to hundreds of
   ms as the queue fills — loss is not the only way a network hurts you.
6. Hit **events.csv** / **charts.png** and show the figures go straight into the
   report.

---

## 6. What is real and what is simplified

**Real:** UDP sockets between endpoints; the Internet checksum; the 3‑way
handshake; packet‑indexed sequence numbers and cumulative ACKs; SACK; the
Jacobson/Karels RTO estimator with Karn's algorithm and exponential backoff;
fast retransmit; Tahoe/Reno slow‑start + AIMD + fast recovery; a token‑free
fluid‑queue traffic shaper with tail drop.

**Simplified deliberately (all documented in the source):**

* Sequence numbers count **segments**, not bytes — keeps the visualisation
  legible; the sequence‑space concept is identical.
* One reduction per loss *episode* (~1 SRTT) so a burst halves cwnd once instead
  of flooring it — mirrors TCP's "one reduction per RTT".
* CUBIC is the RFC‑8312 growth function without HyStart or the TCP‑friendly
  region maths — enough to show cubic vs linear.
* RTO is capped at 4 s (real stacks: ~120 s) so a stalled lab demo recovers.
* SYN/FIN segments bypass the impairment engine so a 30 %‑loss demo doesn't
  spend its first ten seconds just opening the connection.
* Endpoints run as threads in one process (they still only communicate over
  UDP).

---

## 7. File layout

```
run.py                     launcher / CLI
transportlab/
  packet.py                wire format, flags, Internet checksum
  congestion.py            NoCC / Tahoe / Reno / Cubic
  protocol.py              the RDT engine (handshake, windows, timers, ARQ)
  emulator.py              the "bad link": loss/delay/jitter/reorder/dup/
                           corrupt/rate/buffer, time-ordered delivery
  endpoints — see recv_file / send_file in protocol.py
  events.py                the telemetry bus (SSE + CSV export feed it)
  scenarios.py             the link presets
  session.py               builds emulator + sender + receiver, runs a transfer
  dashboard.py             http.server: static files, SSE, control API
web/
  index.html  style.css  app.js     the dashboard (vanilla JS + canvas)
tests/
  test_packet.py  test_loopback.py
sample/
  source.bin  received.bin          generated at run time
```

---

## 8. Ideas for going further

* **QUIC‑style multiplexing:** carry several independent streams over one
  connection and show head‑of‑line blocking disappear when one stream loses a
  segment.
* **Race mode:** two senders sharing one bottleneck; plot fairness between Reno
  and CUBIC, or RTT‑unfairness between a short‑RTT and a long‑RTT flow.
* **ECN:** mark instead of drop at the queue threshold and react to the mark.
* **BBR‑lite:** model‑based congestion control (estimate bottleneck bandwidth ×
  min‑RTT) instead of loss‑based.
