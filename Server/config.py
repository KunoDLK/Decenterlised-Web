"""
config.py — Unified Configuration

All tunable parameters for the peer engine in one place.
Modify these to balance network behaviour without touching logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# ===================================================================
# PeerConfig — single source of truth for all tunables
# ===================================================================


@dataclass
class PeerConfig:
    """All configurable parameters for the decentralised peer engine.

    Instantiate with defaults, override fields as needed.
    """

    # ---- Network ----
    udp_port: int = 32128
    web_port: int = 0  # 0 = disabled
    web_host: str = "127.0.0.1"
    no_tui: bool = False
    no_lan: bool = False

    # ---- Storage ----
    storage_limit_mb: int = 500
    data_dir: str = "data"

    # ---- Hole punching ----
    hole_punch_packets: int = 3
    hole_punch_interval: float = 0.1  # seconds between punch packets
    hole_punch_timeout: float = 5.0   # seconds to wait for hello response
    max_direct_attempts: int = 5      # before marking direct_blocked

    # ---- Keepalive / Liveness ----
    keepalive_interval: float = 30.0      # seconds between PINGs per peer
    ping_response_timeout: float = 2.0   # seconds to wait for PING response
    peer_timeout: float = 90.0            # 3 missed pings = dead
    max_missed_pings: int = 3

    # ---- Reliability / Retransmit ----
    ack_timeout_base: float = 0.5    # initial retransmit timeout (seconds)
    ack_timeout_max: float = 4.0     # max retransmit timeout after backoff
    ack_timeout_multiplier: float = 2.0  # exponential backoff multiplier
    max_retries: int = 5             # max retransmit attempts
    sliding_window_size: int = 256

    # ---- File Transfer ----
    max_chunk_size: int = 8192       # bytes per UDP datagram
    chunk_ack_timeout: float = 0.5   # seconds to wait for chunk ACK
    download_timeout: float = 60.0   # max seconds for a full download

    # ---- Replication / Rebalance ----
    rebalance_interval: float = 60.0          # seconds between rebalance cycles
    network_target_min: int = 3               # minimum replica count
    network_target_max: int = 10              # maximum replica count
    temporary_replica_ttl: float = 3600.0     # 1 hour before temp → permanent
    min_publish_bytes: int = 1_048_576        # 1 MB min for publishing

    # ---- Periodic Maintenance ----
    cleanup_temp_interval: float = 300.0      # 5 minutes
    gc_old_versions_interval: float = 1800.0  # 30 minutes
    peer_cleanup_interval: float = 3600.0     # 1 hour
    peer_cleanup_max_age_days: int = 30

    # ---- LAN Broadcast ----
    lan_broadcast_interval: float = 30.0
    lan_broadcast_min_peers: int = 2          # broadcast if connected < this

    # ---- Peer Book Tiering ----
    tier_2_recent_days: int = 7
    consecutive_fails_demote: int = 5

    # ---- Retransmit polling (only used if not scheduler-driven) ----
    retransmit_poll_interval: float = 0.1

    # ---- Bootstrap ----
    bootstrap_peers: list[tuple[str, str, str, int]] = field(default_factory=list)
    # Format: [(node_id, public_key_base64, ip, port), ...]

    # ---- Logging ----
    log_file: Optional[str] = None    # e.g. "app.log"
    udp_trace_file: Optional[str] = None  # e.g. "packets.hex"

    # ---- Multi-instance testing ----
    tui_port_offset: int = 0


# ===================================================================
# Default instance
# ===================================================================

DEFAULT_CONFIG = PeerConfig()
