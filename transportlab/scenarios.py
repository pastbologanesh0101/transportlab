"""Named bundles of link-emulator settings, for one-click demos."""

from __future__ import annotations

PRESETS: dict[str, dict] = {
    "pristine": dict(
        loss=0.0, corrupt=0.0, dup=0.0, reorder=0.0,
        latency_ms=4, jitter_ms=1, rate_kbps=0, buffer_bytes=0,
        burst_loss=0.0, burst_enter=0.0,
    ),
    "wifi_cafe": dict(
        loss=0.06, corrupt=0.003, dup=0.01, reorder=0.03,
        latency_ms=22, jitter_ms=18, rate_kbps=16000, buffer_bytes=131072,
        burst_loss=0.0, burst_enter=0.0,
    ),
    "satellite": dict(
        loss=0.015, corrupt=0.0, dup=0.0, reorder=0.0,
        latency_ms=300, jitter_ms=15, rate_kbps=6000, buffer_bytes=262144,
        burst_loss=0.0, burst_enter=0.0,
    ),
    "mobile_handoff": dict(
        loss=0.005, corrupt=0.0, dup=0.02, reorder=0.05,
        latency_ms=55, jitter_ms=30, rate_kbps=4000, buffer_bytes=65536,
        burst_loss=0.45, burst_ms=350, burst_enter=0.02,
    ),
    "transoceanic": dict(
        loss=0.04, corrupt=0.006, dup=0.0, reorder=0.04,
        latency_ms=140, jitter_ms=25, rate_kbps=20000, buffer_bytes=262144,
        burst_loss=0.0, burst_enter=0.0,
    ),
    "bufferbloat": dict(
        loss=0.0, corrupt=0.0, dup=0.0, reorder=0.0,
        latency_ms=15, jitter_ms=2, rate_kbps=5000, buffer_bytes=393216,
        burst_loss=0.0, burst_enter=0.0,
    ),
}

DESCRIPTIONS = {
    "pristine": "Near-perfect loopback. Baseline for every comparison.",
    "wifi_cafe": "Crowded 2.4 GHz Wi-Fi: ~6% loss, jitter, a shallow queue.",
    "satellite": "Geostationary hop: 300 ms one-way, mild loss, long fat pipe.",
    "mobile_handoff": "LTE cell handover: periodic 350 ms loss bursts + reordering.",
    "transoceanic": "Congested undersea route: 4% loss, corruption, reordering.",
    "bufferbloat": "5 Mbps link behind a 384 KB buffer. Watch RTT balloon.",
}
