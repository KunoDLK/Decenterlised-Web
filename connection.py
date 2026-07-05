"""
connection.py — Per-Peer Connection State

Thin module: ConnectionState dataclass + lifecycle helpers.
Most logic lives in udp_engine.py.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Literal

_log = logging.getLogger("conn")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PEER_TIMEOUT: float = 90.0  # 3 missed pings at 30s intervals
MAX_HOLE_PUNCH_ATTEMPTS: int = 5

ConnectionStateLiteral = Literal[
    "PUNCHING", "CONNECTED", "ASSISTED", "DISCONNECTED", "UNREACHABLE"
]

# ---------------------------------------------------------------------------
# ConnectionState
# ---------------------------------------------------------------------------


@dataclass
class ConnectionState:
    """Per-peer connection state tracked by UDPEngine."""

    node_id: str
    public_key: bytes
    address: tuple[str, int]
    state: ConnectionStateLiteral = "PUNCHING"
    hello_received: threading.Event = field(default_factory=threading.Event)
    last_seen: float = field(default_factory=time.time)
    uptime_since: float = 0.0
    hole_punch_attempts: int = 0
    direct_blocked: bool = False

    @property
    def is_connected(self) -> bool:
        return self.state == "CONNECTED"

    @property
    def is_punching(self) -> bool:
        return self.state == "PUNCHING"

    @property
    def is_assisted(self) -> bool:
        return self.state == "ASSISTED"

    @property
    def is_dead(self) -> bool:
        return self.state in ("DISCONNECTED", "UNREACHABLE")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def new_connection(
    node_id: str, pubkey: bytes, addr: tuple[str, int]
) -> ConnectionState:
    """Create ConnectionState in PUNCHING state."""
    return ConnectionState(
        node_id=node_id,
        public_key=pubkey,
        address=addr,
        state="PUNCHING",
        hello_received=threading.Event(),
        last_seen=time.time(),
    )


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------


def mark_connected(conn: ConnectionState) -> None:
    """Transition to CONNECTED, fire hello_received Event, update last_seen."""
    prev = conn.state
    conn.state = "CONNECTED"
    conn.hello_received.set()
    conn.last_seen = time.time()
    _log.debug("Peer %s: %s → CONNECTED", conn.node_id[:12], prev)


def mark_assisted(conn: ConnectionState) -> None:
    """Transition to ASSISTED."""
    prev = conn.state
    conn.state = "ASSISTED"
    conn.last_seen = time.time()
    _log.debug("Peer %s: %s → ASSISTED", conn.node_id[:12], prev)


def mark_disconnected(conn: ConnectionState) -> None:
    """Transition to DISCONNECTED."""
    prev = conn.state
    conn.state = "DISCONNECTED"
    _log.debug("Peer %s: %s → DISCONNECTED", conn.node_id[:12], prev)
    conn.last_seen = time.time()


def mark_unreachable(conn: ConnectionState) -> None:
    """Transition to UNREACHABLE."""
    conn.state = "UNREACHABLE"
    conn.last_seen = time.time()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


def is_alive(conn: ConnectionState, timeout: float = PEER_TIMEOUT) -> bool:
    """True if last_seen is within timeout seconds."""
    return (time.time() - conn.last_seen) <= timeout


def increment_attempts(conn: ConnectionState) -> None:
    """Increment hole_punch_attempts. If >= 5, set direct_blocked=True."""
    conn.hole_punch_attempts += 1
    if conn.hole_punch_attempts >= MAX_HOLE_PUNCH_ATTEMPTS:
        conn.direct_blocked = True
