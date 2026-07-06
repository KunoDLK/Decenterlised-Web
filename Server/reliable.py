"""
reliable.py — Reliability Layer (Scheduler-Integrated)

Sequence number tracking, ACK tracking, duplicate detection.
Retransmit is event-driven: track_sent() returns an expiry time so the
caller can queue a CHECK_RETRANSMIT action.  No polling needed.
"""

from __future__ import annotations

import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Message types requiring ACK (reliable delivery)
# ---------------------------------------------------------------------------

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

SLIDING_WINDOW_SIZE: int = 256

# ===================================================================
# PendingMessage
# ===================================================================


@dataclass
class PendingMessage:
    """Message awaiting ACK from a peer."""

    peer_id: str
    seq_num: int
    payload: bytes
    msg_type: int
    retry_count: int = 0
    expiry: float = 0.0
    created_at: float = field(default_factory=time.monotonic)


# ===================================================================
# ReliabilityManager
# ===================================================================


class ReliabilityManager:
    """Manages sequence numbers, duplicate detection, and pending ACKs.

    Does NOT poll or spawn threads.  The caller queues scheduler actions
    based on the expiry times returned by track_sent().
    """

    def __init__(
        self,
        ack_timeout: float = 0.5,
        max_retries: int = 5,
        ack_timeout_max: float = 4.0,
        ack_timeout_multiplier: float = 2.0,
    ) -> None:
        self._ack_timeout = ack_timeout
        self._max_retries = max_retries
        self._ack_timeout_max = ack_timeout_max
        self._ack_timeout_multiplier = ack_timeout_multiplier
        self._lock = threading.Lock()

        # (peer_id, seq_num) → PendingMessage
        self.pending_acks: dict[tuple[str, int], PendingMessage] = {}

        # Next outgoing seq_num per peer
        self.peer_seq_out: dict[str, int] = {}

        # Last received seq_num per peer (for duplicate detection)
        self.peer_seq_in: dict[str, int] = {}

        # Sliding window of received seq_nums per peer
        self.received_seqs: dict[str, set[int]] = {}

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
    # ACK tracking — event-driven
    # ------------------------------------------------------------------

    def track_sent(
        self, peer_id: str, seq_num: int, payload: bytes, msg_type: int
    ) -> Optional[float]:
        """Record a sent message that needs an ACK.

        Returns the expiry time (monotonic) when a retransmit check should
        fire, or None if the message type doesn't need ACKs.
        """
        if not self.needs_ack(msg_type):
            return None

        expiry = time.monotonic() + self._ack_timeout
        with self._lock:
            self.pending_acks[(peer_id, seq_num)] = PendingMessage(
                peer_id=peer_id,
                seq_num=seq_num,
                payload=payload,
                msg_type=msg_type,
                retry_count=0,
                expiry=expiry,
            )
        return expiry

    def ack_received(self, peer_id: str, seq_num: int) -> bool:
        """Record an ACK. Returns True if this cancelled a pending retransmit."""
        with self._lock:
            removed = self.pending_acks.pop((peer_id, seq_num), None)
        return removed is not None

    def get_pending(
        self, peer_id: str, seq_num: int
    ) -> Optional[PendingMessage]:
        """Get a pending message by (peer_id, seq_num)."""
        with self._lock:
            return self.pending_acks.get((peer_id, seq_num))

    def mark_retry(
        self, peer_id: str, seq_num: int
    ) -> tuple[Optional[PendingMessage], Optional[float]]:
        """Increment retry count for a pending message.

        Returns (msg, new_expiry) where:
        - msg is None if already removed or max retries exceeded
        - new_expiry is the time for the next retransmit check, or None if
          max retries exceeded (caller should give up)
        """
        with self._lock:
            msg = self.pending_acks.get((peer_id, seq_num))
            if msg is None:
                return (None, None)

            msg.retry_count += 1
            if msg.retry_count >= self._max_retries:
                self.pending_acks.pop((peer_id, seq_num), None)
                return (None, None)  # give up

            # Exponential backoff: ack_timeout * multiplier^retry_count, capped
            backoff = self._ack_timeout * (
                self._ack_timeout_multiplier ** msg.retry_count
            )
            msg.expiry = time.monotonic() + min(backoff, self._ack_timeout_max)
            return (msg, msg.expiry)

    def discard(self, peer_id: str, seq_num: int) -> None:
        """Explicitly discard a pending message."""
        with self._lock:
            self.pending_acks.pop((peer_id, seq_num), None)

    def discard_all_for_peer(self, peer_id: str) -> int:
        """Remove all pending messages for a peer. Returns count removed."""
        with self._lock:
            to_remove = [
                k for k in self.pending_acks if k[0] == peer_id
            ]
            for k in to_remove:
                self.pending_acks.pop(k, None)
        return len(to_remove)

    def pending_count(self) -> int:
        """Number of messages awaiting ACK."""
        with self._lock:
            return len(self.pending_acks)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def needs_ack(msg_type: int) -> bool:
        """Return True if msg_type requires an ACK."""
        return msg_type in RELIABLE_MSG_TYPES

    @staticmethod
    def build_ack(acked_msg_type: int, ack_seq: int) -> bytes:
        """Build ACK payload: [2B acked_msg_type][4B ack_seq_num]."""
        return struct.pack(">H I", acked_msg_type, ack_seq)

