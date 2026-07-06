"""
scheduler.py — Unified Priority Action Scheduler

Single-threaded priority heap that replaces all scattered timer threads.
Actions are queued, popped in (scheduled_time, priority) order, and executed
by registered handler functions. The recv thread signals wake() when a packet
arrives that may trigger new actions.

Architecture:
  packet arrives → recv thread updates DB → signals scheduler.wake()
  → scheduler pops next ready action → handler reads DB, does work,
    optionally queues follow-up actions → loop
"""

from __future__ import annotations

import enum
import heapq
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

_log = logging.getLogger("scheduler")

# ===================================================================
# Priority
# ===================================================================


class Priority(enum.IntEnum):
    CRITICAL = 0   # responses to incoming packets (HELLO reply, send chunk)
    HIGH = 1       # retransmits, chunk follow-ups, ping response checks
    NORMAL = 2     # rebalance, registry exchange, hole punch
    LOW = 3        # keepalive pings, cleanup, GC


# ===================================================================
# Action
# ===================================================================


@dataclass(order=True)
class Action:
    """A scheduled action in the priority queue.

    Ordering: (scheduled_at, priority, seq).  seq is a tie-breaker for
    stable ordering when two actions have identical time + priority.
    """

    scheduled_at: float
    priority: int
    seq: int
    action_type: str = field(compare=False)
    params: dict[str, Any] = field(default_factory=dict, compare=False)
    action_id: str = field(default_factory=lambda: uuid.uuid4().hex, compare=False)

    @classmethod
    def critical(cls, action_type: str, **params: Any) -> "Action":
        return cls(
            scheduled_at=0,
            priority=Priority.CRITICAL,
            seq=0,
            action_type=action_type,
            params=params,
        )

    @classmethod
    def high(cls, action_type: str, delay: float = 0, **params: Any) -> "Action":
        return cls(
            scheduled_at=time.monotonic() + delay,
            priority=Priority.HIGH,
            seq=0,
            action_type=action_type,
            params=params,
        )

    @classmethod
    def normal(cls, action_type: str, delay: float = 0, **params: Any) -> "Action":
        return cls(
            scheduled_at=time.monotonic() + delay,
            priority=Priority.NORMAL,
            seq=0,
            action_type=action_type,
            params=params,
        )

    @classmethod
    def low(cls, action_type: str, delay: float = 0, **params: Any) -> "Action":
        return cls(
            scheduled_at=time.monotonic() + delay,
            priority=Priority.LOW,
            seq=0,
            action_type=action_type,
            params=params,
        )


# ===================================================================
# Action type constants
# ===================================================================


class ActionType:
    """String constants for all action types."""

    # ---- Critical (responses to incoming packets) ----
    SEND_HELLO_REPLY = "send_hello_reply"
    SEND_CHUNK = "send_chunk"
    SEND_FILE_CHUNK_ACK = "send_file_chunk_ack"
    SEND_CONNECT_INTRODUCE = "send_connect_introduce"
    SEND_CONNECT_ACK = "send_connect_ack"

    # ---- High (retransmits, follow-ups) ----
    CHECK_RETRANSMIT = "check_retransmit"
    CHECK_CHUNK_ACK = "check_chunk_ack"
    CHECK_PING_RESPONSE = "check_ping_response"
    CHECK_HOLE_PUNCH = "check_hole_punch"
    CHECK_DOWNLOAD_COMPLETE = "check_download_complete"
    RESEND_CHUNK = "resend_chunk"
    SOLICIT_REPLICATION = "solicit_replication"

    # ---- Normal (periodic maintenance) ----
    REBALANCE = "rebalance"
    EXCHANGE_REGISTRY = "exchange_registry"
    REQUEST_PEER_LIST = "request_peer_list"
    HOLE_PUNCH_PEER = "hole_punch_peer"
    PEER_ASSISTED_CONNECT = "peer_assisted_connect"
    LAN_BROADCAST = "lan_broadcast"
    START_RECONNECT = "start_reconnect"
    RECONNECT_PHASE = "reconnect_phase"

    # ---- Low (background) ----
    PING_PEER = "ping_peer"
    CLEANUP_TEMP = "cleanup_temp"
    GC_OLD_VERSIONS = "gc_old_versions"
    PEER_CLEANUP = "peer_cleanup"
    LIVENESS_CHECK = "liveness_check"


# ===================================================================
# ActionHandler protocol
# ===================================================================

ActionHandler = Callable[["Action"], None]

# ===================================================================
# Scheduler
# ===================================================================


class Scheduler:
    """Unified priority action scheduler.

    Replaces all scattered timer threads with a single event loop.
    Thread-safe: enqueue/cancel can be called from any thread.
    """

    def __init__(self) -> None:
        self._heap: list[Action] = []
        self._pending: dict[str, Action] = {}   # action_id → Action (for cancellation)
        self._cancelled: set[str] = set()        # lazy-delete set
        self._handlers: dict[str, ActionHandler] = {}
        self._wake_event = threading.Event()
        self._lock = threading.Lock()
        self._seq_counter: int = 0
        self.running: bool = False
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, action_type: str, handler: ActionHandler) -> None:
        """Register a handler for an action type."""
        self._handlers[action_type] = handler

    def register_all(self, handlers: dict[str, ActionHandler]) -> None:
        """Register multiple handlers at once."""
        self._handlers.update(handlers)

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq_counter += 1
        return self._seq_counter

    def enqueue(self, action: Action) -> str:
        """Enqueue an action. Returns action_id for cancellation.

        Thread-safe: may be called from recv thread.
        """
        action.seq = self._next_seq()
        with self._lock:
            heapq.heappush(self._heap, action)
            self._pending[action.action_id] = action
        self._wake_event.set()
        return action.action_id

    def enqueue_at_front(self, action: Action) -> str:
        """Enqueue at highest priority, scheduled now.

        For responses to incoming packets that must be handled immediately.
        """
        action.scheduled_at = 0
        action.priority = Priority.CRITICAL
        return self.enqueue(action)

    def cancel(self, action_id: str) -> bool:
        """Cancel a pending action by ID. Returns True if found.

        Lazy deletion: the action is marked cancelled and skipped on pop.
        Thread-safe.
        """
        with self._lock:
            if action_id in self._pending:
                self._cancelled.add(action_id)
                self._pending.pop(action_id, None)
                return True
        return False

    def cancel_by_type(self, action_type: str, match_params: Optional[dict] = None) -> int:
        """Cancel all pending actions of a given type. Returns count cancelled.

        If match_params is provided, only cancels actions whose params
        contain all key-value pairs in match_params.
        """
        count = 0
        with self._lock:
            to_cancel: list[str] = []
            for aid, action in list(self._pending.items()):
                if action.action_type == action_type:
                    if match_params:
                        if all(
                            action.params.get(k) == v
                            for k, v in match_params.items()
                        ):
                            to_cancel.append(aid)
                    else:
                        to_cancel.append(aid)
            for aid in to_cancel:
                self._cancelled.add(aid)
                self._pending.pop(aid, None)
                count += 1
        return count

    # ------------------------------------------------------------------
    # Wake / signal
    # ------------------------------------------------------------------

    def wake(self) -> None:
        """Signal the scheduler to check for ready actions.

        Called by the recv thread after processing an incoming packet.
        """
        self._wake_event.set()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Run the scheduler loop. Blocks until stop() is called.

        This replaces the main thread's idle loop.
        """
        self.running = True
        _log.info("Scheduler started")

        while self.running:
            self._tick()

        _log.info("Scheduler stopped")

    def _tick(self) -> None:
        """One iteration: pop and execute all ready actions, then wait."""
        # Drain all ready actions
        while self.running:
            action = self._pop_ready()
            if action is None:
                break
            self._execute(action)

        # Wait for next action or wake signal
        if not self.running:
            return

        timeout = self._time_until_next()
        if timeout is None:
            # No pending actions — wait indefinitely for wake
            self._wake_event.wait(timeout=1.0)
        elif timeout > 0:
            self._wake_event.wait(timeout=min(timeout, 1.0))
        self._wake_event.clear()

    def _pop_ready(self) -> Optional[Action]:
        """Pop the next ready action from the heap, skipping cancelled ones."""
        with self._lock:
            while self._heap:
                if self._heap[0].scheduled_at > time.monotonic():
                    return None
                action = heapq.heappop(self._heap)
                if action.action_id in self._cancelled:
                    self._cancelled.discard(action.action_id)
                    continue
                self._pending.pop(action.action_id, None)
                return action
        return None

    def _time_until_next(self) -> Optional[float]:
        """Seconds until the next action is due, or None if empty."""
        with self._lock:
            if not self._heap:
                return None
            return max(0, self._heap[0].scheduled_at - time.monotonic())

    def _execute(self, action: Action) -> None:
        """Dispatch an action to its registered handler."""
        handler = self._handlers.get(action.action_type)
        if handler is None:
            _log.warning("No handler for action type: %s", action.action_type)
            return
        try:
            handler(action)
        except Exception:
            _log.exception("Handler for %s failed", action.action_type)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Signal the scheduler to stop."""
        self.running = False
        self._wake_event.set()

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def pending_count(self) -> int:
        """Number of non-cancelled pending actions."""
        with self._lock:
            return len(self._pending)

    def pending_by_type(self) -> dict[str, int]:
        """Count of pending actions by type."""
        counts: dict[str, int] = {}
        with self._lock:
            for action in self._pending.values():
                counts[action.action_type] = counts.get(action.action_type, 0) + 1
        return counts
