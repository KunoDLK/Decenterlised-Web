"""
reliable.py — Reliability Layer

Sequence number tracking, ACK generation, retransmit timers.
Sits between protocol.py and udp_engine.py.
"""

from __future__ import annotations

import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ACK_TIMEOUT: float = 0.5
MAX_RETRIES: int = 5
SLIDING_WINDOW_SIZE: int = 256

# Message types requiring ACK (reliable delivery)
RELIABLE_MSG_TYPES: frozenset[int] = frozenset(
    {
        0x31,  # FILE_CHUNK
        0x20,  # FILE_REGISTRY_QUERY
        0x21,  # FILE_REGISTRY_RESPONSE
        0x22,  # FILE_REGISTRY_PUSH
        0x50,  # FILE_PUBLISH
        0x51,  # FILE_UPDATE
        0x52,  # FILE_DELETE
        0x70,  # SHARE_FILE_QUERY
        0x71,  # SHARE_FILE_RESPONSE
    }
)

ACK_MSG_TYPE: int = 0x03


# ---------------------------------------------------------------------------
# PendingMessage
# ---------------------------------------------------------------------------


@dataclass
class PendingMessage:
    """Message awaiting ACK from peer."""

    peer_id: str
    seq_num: int
    payload: bytes
    msg_type: int
    retry_count: int = 0
    expiry: float = 0.0
    created_at: float = field(default_factory=time.monotonic)


# ---------------------------------------------------------------------------
# ReliabilityManager
# ---------------------------------------------------------------------------


class ReliabilityManager:
    """Per-udp_engine singleton managing sequence numbers & pending ACKs."""

    def __init__(
        self,
        on_retry_failed: Optional[Callable[[PendingMessage], None]] = None,
    ) -> None:
        self._lock = threading.Lock()

        # (peer_id, seq_num) -> PendingMessage
        self.pending_acks: dict[tuple[str, int], PendingMessage] = {}

        # Next outgoing seq_num per peer
        self.peer_seq_out: dict[str, int] = {}

        # Last received seq_num per peer
        self.peer_seq_in: dict[str, int] = {}

        # Sliding window of received seq_nums per peer
        self.received_seqs: dict[str, set[int]] = {}

        self.on_retry_failed: Callable[[PendingMessage], None] = (
            on_retry_failed if on_retry_failed is not None else lambda _msg: None
        )

    # ------------------------------------------------------------------
    # Sequence numbers
    # ------------------------------------------------------------------

    def next_seq(self, peer_id: str) -> int:
        """Return and increment next outgoing seq_num for peer_id."""
        current = self.peer_seq_out.get(peer_id, 0)
        self.peer_seq_out[peer_id] = (current + 1) & 0xFFFF_FFFF
        return current

    def is_duplicate(self, peer_id: str, seq_num: int) -> bool:
        """Check if seq_num from peer_id is a duplicate.

        Uses a sliding window; handles uint32 wraparound.
        """
        received = self.received_seqs.get(peer_id)
        last = self.peer_seq_in.get(peer_id, -1)

        if received is None:
            received = set()
            self.received_seqs[peer_id] = received
        elif seq_num in received:
            return True

        # Wraparound-aware behind check
        if last != -1:
            diff_behind = (last - seq_num) & 0xFFFF_FFFF
            if 0 < diff_behind <= SLIDING_WINDOW_SIZE:
                return True

        # Record and prune
        received.add(seq_num)
        if len(received) > SLIDING_WINDOW_SIZE:
            sorted_seqs = sorted(received, key=lambda s: (s - seq_num) & 0xFFFF_FFFF)
            received.clear()
            received.update(sorted_seqs[-SLIDING_WINDOW_SIZE:])

        # Advance last-seen pointer
        if last == -1 or ((seq_num - last) & 0xFFFF_FFFF) < 0x8000_0000:
            self.peer_seq_in[peer_id] = seq_num

        return False

    # ------------------------------------------------------------------
    # ACK tracking
    # ------------------------------------------------------------------

    def track_pending(
        self, peer_id: str, seq_num: int, payload: bytes, msg_type: int, critical: bool
    ) -> None:
        """Store message in pending_acks if critical (requires ACK)."""
        if not critical:
            return
        with self._lock:
            self.pending_acks[(peer_id, seq_num)] = PendingMessage(
                peer_id=peer_id,
                seq_num=seq_num,
                payload=payload,
                msg_type=msg_type,
                retry_count=0,
                expiry=time.monotonic() + ACK_TIMEOUT,
            )

    def ack_received(self, peer_id: str, seq_num: int) -> None:
        """Remove from pending_acks when ACK arrives."""
        with self._lock:
            self.pending_acks.pop((peer_id, seq_num), None)

    def get_expired(self) -> list[PendingMessage]:
        """Return messages whose ACK timer expired, increment retry count.

        If retries >= MAX_RETRIES, calls on_retry_failed callback.
        """
        now = time.monotonic()
        expired: list[PendingMessage] = []
        to_remove: list[tuple[str, int]] = []

        with self._lock:
            for key, msg in self.pending_acks.items():
                if msg.expiry <= now:
                    msg.retry_count += 1
                    if msg.retry_count >= MAX_RETRIES:
                        to_remove.append(key)
                        self.on_retry_failed(msg)
                    else:
                        msg.expiry = now + ACK_TIMEOUT
                        expired.append(msg)

            for key in to_remove:
                self.pending_acks.pop(key, None)

        return expired

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def needs_ack(msg_type: int) -> bool:
        """Return True if msg_type requires an ACK."""
        return msg_type in RELIABLE_MSG_TYPES

    @staticmethod
    def build_ack(peer_id: str, acked_msg_type: int, ack_seq: int) -> bytes:
        """Build ACK payload: [2B acked_msg_type][4B ack_seq_num]."""
        return struct.pack(">H I", acked_msg_type, ack_seq)
