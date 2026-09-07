import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from transportlab.packet import (  # noqa: E402
    FLAG_ACK,
    FLAG_DATA,
    FLAG_SACK,
    ChecksumError,
    Packet,
    internet_checksum,
)


class PacketTests(unittest.TestCase):
    def test_round_trip_data(self):
        p = Packet(FLAG_DATA, seq=42, ack=0, window=64, payload=b"hello world")
        q = Packet.decode(p.encode())
        self.assertEqual(q.seq, 42)
        self.assertEqual(q.window, 64)
        self.assertEqual(q.payload, b"hello world")
        self.assertEqual(q.kind, "DATA")

    def test_round_trip_sack(self):
        p = Packet(FLAG_ACK | FLAG_SACK, ack=10, window=32, sacks=[12, 13, 15])
        q = Packet.decode(p.encode())
        self.assertEqual(q.ack, 10)
        self.assertEqual(q.sacks, [12, 13, 15])

    def test_checksum_catches_corruption(self):
        raw = bytearray(Packet(FLAG_DATA, seq=1, payload=b"A" * 100).encode())
        raw[40] ^= 0x20
        with self.assertRaises(ChecksumError):
            Packet.decode(bytes(raw))

    def test_checksum_zero_of_valid_segment(self):
        raw = Packet(FLAG_DATA, seq=7, payload=b"payload payload").encode()
        # A correct Internet checksum makes the total sum fold to zero.
        self.assertEqual(internet_checksum(raw), 0)

    def test_empty_payload(self):
        q = Packet.decode(Packet(FLAG_ACK, ack=5, window=8).encode())
        self.assertEqual(q.payload, b"")
        self.assertEqual(q.kind, "ACK")


if __name__ == "__main__":
    unittest.main()
