"""TransportLab -- a from-scratch reliable transport protocol over UDP.

The package is split so each file maps to one part of the transport-layer
syllabus:

    packet.py      framing, sequence/ack numbers, the Internet checksum
    congestion.py  slow start, AIMD, fast recovery (Tahoe / Reno / CUBIC)
    protocol.py    the reliable data-transfer engine: handshake, sliding
                   window, RTT/RTO estimation, Stop-and-Wait / Go-Back-N /
                   Selective Repeat
    emulator.py    a software "bad link" that drops / delays / reorders /
                   corrupts / rate-limits packets on loopback
    endpoints.py   file sender + file receiver built on the engine
    events.py      a telemetry bus the dashboard subscribes to
    dashboard.py   an http.server dashboard (SSE stream + control API)
    session.py     wires emulator + sender + receiver together for one run
"""

__version__ = "0.1.0"
