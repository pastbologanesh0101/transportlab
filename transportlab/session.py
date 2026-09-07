"""Wire the emulator + one or more sender/receiver pairs together for a run.

Everything runs as threads in one process and talks only over UDP sockets on
loopback, so it stays a genuine networked protocol -- there is no in-memory
shortcut between endpoints -- while staying trivial to start, stop and restart
from the dashboard.

With ``flows > 1`` every flow shares the single emulator (one bottleneck rate,
one buffer): that is the Arena, where you watch congestion controllers compete.
"""

from __future__ import annotations

import math
import os
import random
import socket
import threading
import time
from typing import List, Optional

from .emulator import LinkEmulator
from .events import EventBus
from .pcap import write_pcap
from .protocol import Connection
from .scenarios import PRESETS

MAX_FLOWS = 4
DEFAULT_FLOW_CC = ["reno", "cubic", "bbr", "tahoe"]


def _udp(addr, tries: int = 40) -> socket.socket:
    last = None
    for _ in range(tries):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(addr)
            return s
        except OSError as e:  # still in a TIME_WAIT-ish state after a restart
            last = e
            s.close()
            time.sleep(0.05)
    raise last  # type: ignore[misc]


class Session:
    def __init__(self, bus: EventBus, host: str = "127.0.0.1",
                 port_base: int = 9800, seed: Optional[int] = 1234,
                 out_dir: str = "sample") -> None:
        self.bus = bus
        self.host = host
        self.seed = seed
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)

        self.link_addr = (host, port_base)
        self.srv_addr = [(host, port_base + 1 + 2 * i) for i in range(MAX_FLOWS)]
        self.cli_addr = [(host, port_base + 2 + 2 * i) for i in range(MAX_FLOWS)]

        self.params = dict(arq="selective_repeat", cc="reno", rwnd=64,
                           mss=1024, size_bytes=2_000_000,
                           flows=1, stagger_s=2.0,
                           mux=1, hol=0)   # mux>1: QUIC-style streams; hol: TCP head-of-line
        self.flow_cc: List[str] = list(DEFAULT_FLOW_CC)

        self.emulator = LinkEmulator(
            self.link_addr, list(zip(self.cli_addr, self.srv_addr)), bus,
            config=dict(PRESETS["pristine"]), seed=seed, capture=True)
        self.emulator.start()

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._runner: Optional[threading.Thread] = None
        self._src_cache: tuple[int, bytes] | None = None
        self.running = False
        self.last_run: dict = {}

    # -- configuration ------------------------------------------------
    def configure(self, flow_cc: Optional[list] = None, **kw) -> dict:
        with self._lock:
            for k, v in kw.items():
                if k in self.params and v is not None:
                    self.params[k] = type(self.params[k])(v)
            self.params["flows"] = max(1, min(MAX_FLOWS, int(self.params["flows"])))
            if flow_cc:
                for i, name in enumerate(flow_cc[:MAX_FLOWS]):
                    self.flow_cc[i] = name
        return self.state()

    def set_link(self, changes: dict) -> dict:
        return self.emulator.update(changes)

    def apply_preset(self, name: str) -> dict:
        if name not in PRESETS:
            raise KeyError(name)
        cfg = self.emulator.update(dict(PRESETS[name]))
        self.bus.publish("preset", name=name, **cfg)
        return cfg

    def state(self) -> dict:
        return {
            "running": self.running,
            "params": dict(self.params),
            "flow_cc": list(self.flow_cc),
            "max_flows": MAX_FLOWS,
            "link": self.emulator.get_config(),
            "counts": dict(self.emulator.counts),
            "last_run": dict(self.last_run),
        }

    # -- source data ------------------------------------------------
    def _source(self, n: int) -> bytes:
        if self._src_cache and self._src_cache[0] == n:
            return self._src_cache[1]
        data = random.Random(self.seed).randbytes(n)
        with open(os.path.join(self.out_dir, "source.bin"), "wb") as f:
            f.write(data)
        self._src_cache = (n, data)
        return data

    def pcap_bytes(self) -> bytes:
        return write_pcap(self.emulator.capture_packets())

    # -- loss sweep vs the Mathis model -----------------------------
    SWEEP_LOSSES = (0.0, 0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.12)

    def run_sweep(self, cc: Optional[str] = None,
                  losses: Optional[list] = None) -> None:
        """One single-flow transfer per loss value; publish sweep_point events
        carrying measured goodput next to the Mathis sqrt(p) prediction."""
        losses = list(losses or self.SWEEP_LOSSES)
        self.stop()
        base_params = dict(self.params)
        base_link = self.emulator.get_config()
        self.params["flows"] = 1
        if cc:
            self.params["cc"] = cc
        mss_bits = self.params["mss"] * 8
        self.bus.publish("sweep_begin", cc=self.params["cc"],
                         arq=self.params["arq"], points=len(losses))
        try:
            for p in losses:
                self.emulator.update({"loss": p})
                grab: dict = {}
                sid, q = self.bus.subscribe()
                halt = threading.Event()

                def _pump() -> None:                       # drain concurrently
                    while not halt.is_set():
                        try:
                            ev = q.get(timeout=0.2)
                        except Exception:
                            continue
                        if ev["kind"] == "stats" and ev.get("flow", 0) == 0:
                            grab["g"] = ev.get("goodput_kbps")
                            grab["rtt"] = ev.get("srtt_ms")

                th = threading.Thread(target=_pump, daemon=True)
                th.start()
                self.start(wait=True)
                time.sleep(0.3)                            # catch the final stats
                halt.set()
                th.join(timeout=1.0)
                self.bus.unsubscribe(sid)

                g, rtt = grab.get("g"), grab.get("rtt")
                mathis = None
                if p > 0 and rtt:
                    mathis = mss_bits / ((rtt / 1000.0) * math.sqrt(2 * p / 3)) / 1000.0
                self.bus.publish("sweep_point", loss=p, goodput_kbps=g,
                                 mathis_kbps=round(mathis, 1) if mathis else None,
                                 srtt_ms=rtt)
        finally:
            self.emulator.update(base_link)
            self.params.update(base_params)
            self.bus.publish("sweep_done")

    # -- run lifecycle --------------------------------------------
    def start(self, wait: bool = False) -> None:
        self.stop()
        self._stop.clear()
        p = dict(self.params)
        n = p["flows"]
        data = self._source(p["size_bytes"])

        def _run() -> None:
            self.running = True
            mux = max(1, int(p["mux"])) if n == 1 else 1
            hol = bool(p["hol"])
            self.bus.publish("run_begin", flows=n, arq=p["arq"],
                             flow_cc=self.flow_cc[:n], mss=p["mss"],
                             rwnd=p["rwnd"], size_bytes=p["size_bytes"],
                             stagger_s=p["stagger_s"], mux=mux, hol=hol,
                             link=self.emulator.get_config())
            socks: list = []
            servers: list = []
            threads: list = []
            t0 = time.monotonic()

            for i in range(n):
                ssock = _udp(self.srv_addr[i])
                csock = _udp(self.cli_addr[i])
                socks += [ssock, csock]
                cc = self.flow_cc[i] if n > 1 else p["cc"]
                fh = open(os.path.join(self.out_dir, f"received_{i}.bin"), "wb")
                srv = Connection("server", ssock, self.link_addr, self.bus,
                                 arq=p["arq"], cc=cc, rwnd=p["rwnd"],
                                 mss=p["mss"], stop_flag=self._stop, flow_id=i,
                                 label=f"{cc}", mux=mux, hol=hol)
                cli = Connection("client", csock, self.link_addr, self.bus,
                                 arq=p["arq"], cc=cc, rwnd=p["rwnd"],
                                 mss=p["mss"], stop_flag=self._stop, flow_id=i,
                                 label=f"{cc}", mux=mux, hol=hol)
                srv._fh = fh                       # noqa: SLF001  (closed below)
                servers.append(srv)

                st = threading.Thread(target=srv.recv_file, args=(fh.write,),
                                      name=f"srv{i}", daemon=True)
                st.start()
                threads.append(st)

                delay = 0.15 + (i * p["stagger_s"] if n > 1 else 0.0)

                def _client(c=cli, d=delay, idx=i):
                    if self._stop.wait(d):
                        return
                    self.bus.publish("flow_begin", flow=idx, cc=c.cc_name)
                    c.send_file(data)

                ct = threading.Thread(target=_client, name=f"cli{i}", daemon=True)
                ct.start()
                threads.append(ct)

            # wait for the client threads (even index+1) to finish
            for i in range(n):
                threads[2 * i + 1].join(
                    timeout=600 if not self._stop.is_set() else 2.0)

            self._stop.set()
            for st in threads:
                st.join(timeout=3.0)
            for s in socks:
                try:
                    s.close()
                except OSError:
                    pass
            for srv in servers:
                try:
                    srv._fh.close()
                except Exception:
                    pass

            verified = [
                bool(srv.finished and srv.peer_digest == srv._recv_hash.digest())
                for srv in servers
            ]
            self.running = False
            self.last_run = {
                "flows": n, "flow_cc": self.flow_cc[:n],
                "seconds": round(time.monotonic() - t0, 3),
                "verified": verified, "all_verified": all(verified),
            }
            self.bus.publish("run_end", **self.last_run)

        self._runner = threading.Thread(target=_run, name="session-run", daemon=True)
        self._runner.start()
        if wait:
            self._runner.join()

    def stop(self) -> None:
        self._stop.set()
        r = self._runner
        if r and r.is_alive():
            r.join(timeout=8.0)
        self._runner = None
        self.running = False

    def shutdown(self) -> None:
        self.stop()
        self.emulator.stop()
