"""Wire format for TransportLab segments.

A segment is a fixed 22-byte header followed by an optional list of SACK
sequence numbers and then the payload:

    0        2   3   4       8      12      16   17  18      20      22
    +--------+---+---+-------+-------+-------+---+---+-------+-------+
    | magic  |ver|flg|  seq  |  ack  | window|s_n|rsv|cksum  | p_len |
    +--------+---+---+-------+-------+-------+---+---+-------+-------+
    | sack[0] .. sack[s_n-1]  (4 bytes each) | payload (p_len bytes) |
    +-----------------------------------------+-----------------------+

* ``seq``    -- packet-indexed sequence number (segment 0, 1, 2, ...).  Using
               packet indices instead of byte offsets keeps the visualisation
               readable; the concept (a monotonic sequence space) is identical.
* ``ack``    -- next in-order sequence number the receiver still wants
               (cumulative ACK, exactly like TCP).
* ``window`` -- receiver's advertised window, in segments (flow control).
* ``sack``   -- up to 4 sequence numbers the receiver has buffered out of
               order (used by Selective Repeat).
* ``cksum``  -- 16-bit one's-complement Internet checksum over the whole
               segment with the checksum field zeroed.
"""

from __future__ import annotations

import struct
from typing import List

MAGIC = b"TL"
VERSION = 1

FLAG_SYN = 0x01
FLAG_ACK = 0x02
FLAG_FIN = 0x04
FLAG_DATA = 0x08
FLAG_SACK = 0x10
FLAG_RST = 0x20

MAX_SACK = 4

_HDR = struct.Struct("!2sBB III BBHH")
HDR_LEN = _HDR.size  # 22 bytes


def flag_names(flags: int) -> str:
    names = [
        (FLAG_SYN, "SYN"),
        (FLAG_ACK, "ACK"),
        (FLAG_FIN, "FIN"),
        (FLAG_DATA, "DATA"),
        (FLAG_SACK, "SACK"),
        (FLAG_RST, "RST"),
    ]
    return "|".join(n for bit, n in names if flags & bit) or "-"


def internet_checksum(data: bytes) -> int:
    """16-bit one's-complement sum, as used by IP/TCP/UDP."""
    if len(data) % 2:
        data += b"\x00"
    total = 0
    for i in range(0, len(data), 2):
        total += (data[i] << 8) | data[i + 1]
        total = (total & 0xFFFF) + (total >> 16)
    return (~total) & 0xFFFF


class ChecksumError(ValueError):
    """Raised by :meth:`Packet.decode` when the checksum does not verify."""


class Packet:
    __slots__ = ("flags", "seq", "ack", "window", "sacks", "payload")

    def __init__(
        self,
        flags: int = 0,
        seq: int = 0,
        ack: int = 0,
        window: int = 0,
        sacks: List[int] | None = None,
        payload: bytes = b"",
    ) -> None:
        self.flags = flags
        self.seq = seq
        self.ack = ack
        self.window = window
        self.sacks = list(sacks or [])[:MAX_SACK]
        self.payload = payload

    # -- helpers -------------------------------------------------------------
    def has(self, bit: int) -> bool:
        return bool(self.flags & bit)

    @property
    def kind(self) -> str:
        if self.has(FLAG_SYN):
            return "SYN-ACK" if self.has(FLAG_ACK) else "SYN"
        if self.has(FLAG_FIN):
            return "FIN-ACK" if self.has(FLAG_ACK) else "FIN"
        if self.has(FLAG_DATA):
            return "DATA"
        return "ACK"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"<Packet {flag_names(self.flags)} seq={self.seq} ack={self.ack} "
            f"win={self.window} sack={self.sacks} len={len(self.payload)}>"
        )

    # -- serialisation -----------------------------------------------------
    def encode(self) -> bytes:
        sack_bytes = b"".join(struct.pack("!I", s & 0xFFFFFFFF) for s in self.sacks)
        body = sack_bytes + self.payload
        blank = _HDR.pack(
            MAGIC, VERSION, self.flags, self.seq, self.ack, self.window,
            len(self.sacks), 0, 0, len(self.payload),
        )
        cksum = internet_checksum(blank + body)
        header = _HDR.pack(
            MAGIC, VERSION, self.flags, self.seq, self.ack, self.window,
            len(self.sacks), 0, cksum, len(self.payload),
        )
        return header + body

    @classmethod
    def decode(cls, raw: bytes) -> "Packet":
        if len(raw) < HDR_LEN:
            raise ValueError("segment shorter than header")
        magic, ver, flags, seq, ack, window, sack_n, _res, cksum, plen = _HDR.unpack(
            raw[:HDR_LEN]
        )
        if magic != MAGIC:
            raise ValueError("bad magic")
        if ver != VERSION:
            raise ValueError(f"unsupported version {ver}")
        body = raw[HDR_LEN:]
        blank = _HDR.pack(magic, ver, flags, seq, ack, window, sack_n, 0, 0, plen)
        if internet_checksum(blank + body) != cksum:
            raise ChecksumError("checksum mismatch")
        sacks = [
            struct.unpack("!I", body[i * 4 : i * 4 + 4])[0] for i in range(sack_n)
        ]
        payload = body[sack_n * 4 : sack_n * 4 + plen]
        if len(payload) != plen:
            raise ValueError("truncated payload")
        return cls(flags, seq, ack, window, sacks, payload)
