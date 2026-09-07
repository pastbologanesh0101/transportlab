"""Write captured segments as a classic ``.pcap`` file.

Each TransportLab segment is wrapped in a synthetic Ethernet / IPv4 / UDP frame
so Wireshark dissects the addressing and timing.  The UDP payload is the raw
TransportLab segment -- Wireshark shows it as data; drop in the bundled Lua
dissector (``tools/transportlab.lua``, optional) to decode the header fields.

Only the standard library is used.
"""

from __future__ import annotations

import struct
from typing import Iterable, Tuple

Addr = Tuple[str, int]

_PCAP_MAGIC = 0xA1B2C3D4
_LINKTYPE_ETHERNET = 1


def _ip_to_bytes(ip: str) -> bytes:
    return bytes(int(o) for o in ip.split("."))


def _checksum(data: bytes) -> int:
    if len(data) % 2:
        data += b"\x00"
    s = 0
    for i in range(0, len(data), 2):
        s += (data[i] << 8) | data[i + 1]
        s = (s & 0xFFFF) + (s >> 16)
    return (~s) & 0xFFFF


def _frame(src: Addr, dst: Addr, payload: bytes) -> bytes:
    # Ethernet II: locally-administered MACs derived from the port number.
    smac = b"\x02\x00\x00\x00" + struct.pack("!H", src[1] & 0xFFFF)
    dmac = b"\x02\x00\x00\x00" + struct.pack("!H", dst[1] & 0xFFFF)
    eth = dmac + smac + b"\x08\x00"

    udp_len = 8 + len(payload)
    udp = struct.pack("!HHHH", src[1] & 0xFFFF, dst[1] & 0xFFFF, udp_len, 0) + payload

    total = 20 + udp_len
    ip = struct.pack(
        "!BBHHHBBH4s4s",
        0x45, 0x00, total, 0x0000, 0x0000, 64, 17, 0,
        _ip_to_bytes(src[0]), _ip_to_bytes(dst[0]),
    )
    ip = ip[:10] + struct.pack("!H", _checksum(ip)) + ip[12:]
    return eth + ip + udp


def write_pcap(packets: Iterable[Tuple[float, Addr, Addr, bytes]]) -> bytes:
    """``packets`` is an iterable of ``(unix_ts, src_addr, dst_addr, raw)``."""
    out = bytearray()
    out += struct.pack("!IHHiIII", _PCAP_MAGIC, 2, 4, 0, 0, 65535,
                       _LINKTYPE_ETHERNET)
    for ts, src, dst, raw in packets:
        frame = _frame(src, dst, raw)
        sec = int(ts)
        usec = int((ts - sec) * 1_000_000)
        out += struct.pack("!IIII", sec, usec, len(frame), len(frame))
        out += frame
    return bytes(out)
