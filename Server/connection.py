"""
connection.py — Compatibility shim

All connection state has moved to peer_book.py (peers table columns:
state, address_ip, address_port, last_ping_sent, hole_punch_attempts,
direct_blocked).  This module is kept for backward compatibility only.

New code should use peer_book.set_connection_state() / get_connection_state().
"""

from __future__ import annotations

import logging
from typing import Literal

_log = logging.getLogger("conn")

# Re-export ConnectionStateLiteral for modules that still import it
ConnectionStateLiteral = Literal[
    "PUNCHING", "CONNECTED", "ASSISTED", "DISCONNECTED", "UNREACHABLE"
]

# Compatibility constants
PEER_TIMEOUT: float = 90.0
MAX_HOLE_PUNCH_ATTEMPTS: int = 5


def is_alive(peer_book: object, node_id: str, timeout: float = PEER_TIMEOUT) -> bool:
    """Delegate to peer_book.is_alive()."""
    if hasattr(peer_book, "is_alive"):
        return peer_book.is_alive(node_id, timeout)
    return False


def mark_connected(peer_book: object, node_id: str, addr: tuple[str, int]) -> None:
    """Delegate to peer_book.set_connection_state()."""
    if hasattr(peer_book, "set_connection_state"):
        peer_book.set_connection_state(node_id, "CONNECTED", addr[0], addr[1])


def mark_disconnected(peer_book: object, node_id: str) -> None:
    """Delegate to peer_book.set_connection_state()."""
    if hasattr(peer_book, "set_connection_state"):
        peer_book.set_connection_state(node_id, "DISCONNECTED")


def increment_attempts(peer_book: object, node_id: str) -> bool:
    """Delegate to peer_book.increment_hole_punch_attempts()."""
    if hasattr(peer_book, "increment_hole_punch_attempts"):
        return peer_book.increment_hole_punch_attempts(node_id)
    return False

