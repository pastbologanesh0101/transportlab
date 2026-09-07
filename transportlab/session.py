"""Wire an emulator + a sender + a receiver together for one transfer.

All three run as threads inside one process and talk only over UDP sockets on
loopback, so it is still a genuine networked protocol -- there is no in-memory
shortcut between the endpoints -- but everything is easy to start, stop and
restart from the dashboard.
"""

from __future__ import annotations

import os
import random
import socket
import threading
import time
from typing import Optional

from .emulator import LinkEmulator
from .events import EventBus
from .protocol import Connection
from .scenarios import PRESETS


def _udp(addr, tries: int = 40) -> socket.socket:
    last = None
    for _ in range(tries):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(addr)
            return s
        except OSError as e:  # address still in TIME_WAIT-ish state after a restart
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
        self.link_addr = (host, port_base)
        self.server_addr = (host, port_base + 1)
        self.client_addr = (host, port_base + 2)
        self.seed = seed
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)

        self.params = dict(arq="selective_repeat", cc="reno", rwnd=64,
                           mss=1024, size_bytes=2_000_000)

        self.emulator = LinkEmulator(self.link_addr, self.client_addr,
                                     self.server_addr, bus,
                                     config=dict(PRESETS["pristine"]), seed=seed)
        self.emulator.start()

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._runner: Optional[threading.Thread] = None
        self._src_cache: tuple[int, bytes] | None = None
        self.running = False

    # -- configuration ------------------------------------------------
    def configure(self, **kw) -> dict:
        with self._lock:
            for k, v in kw.items():
                if k in self.params and v is not None:
                    self.params[k] = type(self.params[k])(v)
        return dict(self.params)

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
            "link": self.emulator.get_config(),
            "counts": dict(self.emulator.counts),
            "addrs": {
                "link": list(self.link_addr),
                "server": list(self.server_addr),
                "client": list(self.client_addr),
            },
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

    # -- run lifecycle --------------------------------------------
    def start(self, wait: bool = False) -> None:
        self.stop()
        self._stop.clear()
        p = dict(self.params)
        data = self._source(p["size_bytes"])
        recv_path = os.path.join(self.out_dir, "received.bin")

        def _run() -> None:
            self.running = True
            self.bus.publish("run_begin", **p,
                             link=self.emulator.get_config())
            ssock = _udp(self.server_addr)
            csock = _udp(self.client_addr)
            fh = open(recv_path, "wb")
            server = Connection("server", ssock, self.link_addr, self.bus,
                                arq=p["arq"], cc=p["cc"], rwnd=p["rwnd"],
                                mss=p["mss"], stop_flag=self._stop)
            client = Connection("client", csock, self.link_addr, self.bus,
                                arq=p["arq"], cc=p["cc"], rwnd=p["rwnd"],
                                mss=p["mss"], stop_flag=self._stop)
            st = threading.Thread(target=server.recv_file, args=(fh.write,),
                                  name="server", daemon=True)
            st.start()
            time.sleep(0.15)
            t0 = time.monotonic()
            try:
                client.send_file(data)
            finally:
                self._stop.set()          # tell the server loop to wind down
                st.join(timeout=3.0)
                fh.close()
                for s in (ssock, csock):
                    try:
                        s.close()
                    except OSError:
                        pass
                self.running = False
                self.bus.publish("run_end", seconds=round(time.monotonic() - t0, 3),
                                 verified=bool(server.finished and
                                               server.peer_digest == server._recv_hash.digest()),
                                 received_path=recv_path)

        self._runner = threading.Thread(target=_run, name="session-run", daemon=True)
        self._runner.start()
        if wait:
            self._runner.join()

    def stop(self) -> None:
        self._stop.set()
        r = self._runner
        if r and r.is_alive():
            r.join(timeout=5.0)
        self._runner = None
        self.running = False

    def shutdown(self) -> None:
        self.stop()
        self.emulator.stop()
