"""
peer_book.py — SQLite Peer Directory

Persistent directory of all known peers with tiering, last-seen tracking,
connection state management, and cleanup.  All per-peer connection state
that was formerly held in-memory (ConnectionState dataclass) now lives here.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from file_registry import FileRegistry

_log = logging.getLogger("peers")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TIER_2_RECENT_DAYS: int = 7
CONSECUTIVE_FAILS_DEMOTE: int = 5
PEER_BOOK_CLEANUP_DAYS: int = 30
MAX_HOLE_PUNCH_ATTEMPTS: int = 5

ConnectionStateLiteral = Literal[
    "PUNCHING", "CONNECTED", "ASSISTED", "DISCONNECTED", "UNREACHABLE"
]


# ---------------------------------------------------------------------------
# PeerBook
# ---------------------------------------------------------------------------


class PeerBook:
    """SQLite-backed peer directory with tier-based prioritisation.

    Also manages per-peer connection state (formerly ConnectionState dataclass).
    """

    def __init__(self, data_dir: str) -> None:
        self.db_path = str(Path(data_dir) / "peers.db")
        self._lock = threading.Lock()

        Path(data_dir).mkdir(parents=True, exist_ok=True)

        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS peers (
                    node_id         TEXT PRIMARY KEY,
                    public_key      BLOB NOT NULL,
                    public_ip       TEXT NOT NULL,
                    public_port     INTEGER NOT NULL,
                    uptime_since    REAL,
                    last_seen       REAL,
                    tier            INTEGER NOT NULL DEFAULT 3,
                    consecutive_fails INTEGER DEFAULT 0,
                    first_seen      REAL NOT NULL,
                    is_bootstrap    INTEGER DEFAULT 0,
                    state           TEXT NOT NULL DEFAULT 'DISCONNECTED',
                    address_ip      TEXT NOT NULL DEFAULT '',
                    address_port    INTEGER NOT NULL DEFAULT 0,
                    last_ping_sent  REAL NOT NULL DEFAULT 0,
                    hole_punch_attempts INTEGER NOT NULL DEFAULT 0,
                    direct_blocked  INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_peers_tier ON peers(tier, last_seen DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_peers_last_seen ON peers(last_seen)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_peers_state ON peers(state)"
            )
            conn.commit()

        # Migrate existing databases that lack the new columns
        self._migrate_schema()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _migrate_schema(self) -> None:
        """Add new columns if they don't exist (idempotent)."""
        new_columns = {
            "state": "TEXT NOT NULL DEFAULT 'DISCONNECTED'",
            "address_ip": "TEXT NOT NULL DEFAULT ''",
            "address_port": "INTEGER NOT NULL DEFAULT 0",
            "last_ping_sent": "REAL NOT NULL DEFAULT 0",
            "hole_punch_attempts": "INTEGER NOT NULL DEFAULT 0",
            "direct_blocked": "INTEGER NOT NULL DEFAULT 0",
        }
        with self._lock:
            with self._get_conn() as conn:
                existing = {
                    row["name"]
                    for row in conn.execute("PRAGMA table_info(peers)").fetchall()
                }
                for col_name, col_def in new_columns.items():
                    if col_name not in existing:
                        try:
                            conn.execute(
                                f"ALTER TABLE peers ADD COLUMN {col_name} {col_def}"
                            )
                            _log.info("Migrated peers table: added column %s", col_name)
                        except sqlite3.OperationalError:
                            pass  # already exists
                conn.commit()

    # ------------------------------------------------------------------
    # Connection state management (replaces connection.py)
    # ------------------------------------------------------------------

    def set_connection_state(
        self,
        node_id: str,
        state: ConnectionStateLiteral,
        address_ip: str = "",
        address_port: int = 0,
    ) -> None:
        """Set the connection state and optional address for a peer."""
        now = time.time()
        with self._lock:
            with self._get_conn() as conn:
                if address_ip:
                    conn.execute(
                        """UPDATE peers SET state=?, last_seen=?, address_ip=?, address_port=?
                           WHERE node_id=?""",
                        (state, now, address_ip, address_port, node_id),
                    )
                else:
                    conn.execute(
                        "UPDATE peers SET state=?, last_seen=? WHERE node_id=?",
                        (state, now, node_id),
                    )
                conn.commit()
        _log.debug("Peer %s state → %s", node_id[:12], state)

    def get_connection_state(self, node_id: str) -> Optional[dict[str, Any]]:
        """Get connection-related fields for a peer."""
        row = self.get(node_id)
        if row is None:
            return None
        return {
            "node_id": row["node_id"],
            "state": row["state"],
            "address_ip": row["address_ip"],
            "address_port": row["address_port"],
            "last_seen": row["last_seen"],
            "last_ping_sent": row["last_ping_sent"],
            "hole_punch_attempts": row["hole_punch_attempts"],
            "direct_blocked": bool(row["direct_blocked"]),
            "public_key": bytes(row["public_key"]),
        }

    def get_connected_peers(self) -> list[str]:
        """Return node_ids of all peers with state='CONNECTED'."""
        with self._lock:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT node_id FROM peers WHERE state='CONNECTED'"
                ).fetchall()
        return [r["node_id"] for r in rows]

    def is_connected(self, node_id: str) -> bool:
        """Check if a peer is in CONNECTED state."""
        row = self.get_connection_state(node_id)
        return row is not None and row["state"] == "CONNECTED"

    def is_alive(self, node_id: str, timeout: float = 90.0) -> bool:
        """True if last_seen is within timeout seconds."""
        row = self.get(node_id)
        if row is None:
            return False
        return (time.time() - row["last_seen"]) <= timeout

    def record_ping_sent(self, node_id: str) -> None:
        """Record that a PING was sent to this peer."""
        now = time.time()
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE peers SET last_ping_sent=? WHERE node_id=?",
                    (now, node_id),
                )
                conn.commit()

    def record_ping_received(self, node_id: str) -> None:
        """Record that a response was received (updates last_seen)."""
        self.mark_seen(node_id)

    def set_connection_address(
        self, node_id: str, ip: str, port: int
    ) -> None:
        """Update the last-known address for a peer."""
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE peers SET address_ip=?, address_port=? WHERE node_id=?",
                    (ip, port, node_id),
                )
                conn.commit()

    def increment_hole_punch_attempts(self, node_id: str) -> bool:
        """Increment hole punch attempts. Returns True if now direct_blocked.

        Only demotes state if currently PUNCHING — never overrides CONNECTED.
        """
        with self._lock:
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT hole_punch_attempts, state FROM peers WHERE node_id=?",
                    (node_id,),
                ).fetchone()
                if row is None:
                    return False
                attempts = row["hole_punch_attempts"] + 1
                blocked = 1 if attempts >= MAX_HOLE_PUNCH_ATTEMPTS else 0
                # Only demote state if still PUNCHING (preserve CONNECTED from HELLO reply)
                if row["state"] == "PUNCHING":
                    new_state = "UNREACHABLE" if blocked else "DISCONNECTED"
                    conn.execute(
                        """UPDATE peers SET hole_punch_attempts=?, direct_blocked=?,
                           state=? WHERE node_id=?""",
                        (attempts, blocked, new_state, node_id),
                    )
                else:
                    conn.execute(
                        """UPDATE peers SET hole_punch_attempts=?, direct_blocked=?
                           WHERE node_id=?""",
                        (attempts, blocked, node_id),
                    )
                conn.commit()
                return blocked == 1

    def resolve_node_id(self, addr: tuple[str, int]) -> Optional[str]:
        """Resolve (ip, port) to node_id via last-known address."""
        with self._lock:
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT node_id FROM peers WHERE address_ip=? AND address_port=?",
                    (addr[0], addr[1]),
                ).fetchone()
        return row["node_id"] if row else None

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def add_or_update(
        self,
        node_id: str,
        public_key: bytes,
        ip: str,
        port: int,
        uptime_since: float,
        is_bootstrap: bool = False,
    ) -> None:
        """INSERT OR REPLACE a peer. Resets consecutive_fails to 0."""
        now = time.time()
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO peers (node_id, public_key, public_ip, public_port,
                                       uptime_since, last_seen, tier, consecutive_fails,
                                       first_seen, is_bootstrap)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                    ON CONFLICT(node_id) DO UPDATE SET
                        public_key = excluded.public_key,
                        public_ip = excluded.public_ip,
                        public_port = excluded.public_port,
                        uptime_since = excluded.uptime_since,
                        last_seen = excluded.last_seen,
                        consecutive_fails = 0,
                        is_bootstrap = excluded.is_bootstrap
                    """,
                    (
                        node_id,
                        public_key,
                        ip,
                        port,
                        uptime_since,
                        now,
                        3,
                        now,
                        1 if is_bootstrap else 0,
                    ),
                )
                conn.commit()
        _log.debug("Peer %s added/updated (ip=%s:%d tier=3)", node_id[:12], ip, port)

    def get(self, node_id: str) -> Optional[dict[str, Any]]:
        """Return peer row as dict or None."""
        with self._lock:
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT * FROM peers WHERE node_id = ?", (node_id,)
                ).fetchone()
        if row is None:
            return None
        return dict(row)

    def get_by_tier(self, tier: int, limit: int = 100) -> list[dict[str, Any]]:
        """Return peers in a tier, ordered by last_seen DESC."""
        with self._lock:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM peers WHERE tier = ? ORDER BY last_seen DESC LIMIT ?",
                    (tier, limit),
                ).fetchall()
        return [dict(r) for r in rows]

    def get_all_ordered(self) -> list[dict[str, Any]]:
        """Return all peers sorted by tier ASC, last_seen DESC."""
        with self._lock:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM peers ORDER BY tier ASC, last_seen DESC"
                ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Status updates
    # ------------------------------------------------------------------

    def mark_seen(self, node_id: str) -> None:
        """Update last_seen, reset consecutive_fails."""
        now = time.time()
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE peers SET last_seen = ?, consecutive_fails = 0 WHERE node_id = ?",
                    (now, node_id),
                )
                conn.commit()

    def cleanup_placeholder(
        self, ip: str, port: int, real_node_id: str
    ) -> None:
        """Remove placeholder peers at (ip, port) that aren't the real peer.

        Placeholders have empty public_key (b''), created during loopback discovery.
        """
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    """DELETE FROM peers
                       WHERE public_ip = ? AND public_port = ?
                         AND public_key = X'' AND node_id != ?""",
                    (ip, port, real_node_id),
                )
                conn.commit()

    def mark_offline(self, node_id: str) -> None:
        """Update last_seen only (does not remove)."""
        now = time.time()
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE peers SET last_seen = ? WHERE node_id = ?",
                    (now, node_id),
                )
                conn.commit()
        _log.info("Peer %s marked offline", node_id[:12])

    def record_failure(self, node_id: str) -> None:
        """Increment consecutive_fails. If >= 5, demote tier by 1 (min 3)."""
        with self._lock:
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT consecutive_fails, tier FROM peers WHERE node_id = ?",
                    (node_id,),
                ).fetchone()
                if row is None:
                    return
                fails = row["consecutive_fails"] + 1
                tier = row["tier"]
                if fails >= CONSECUTIVE_FAILS_DEMOTE:
                    tier = min(tier + 1, 3)
                conn.execute(
                    "UPDATE peers SET consecutive_fails = ?, tier = ? WHERE node_id = ?",
                    (fails, tier, node_id),
                )
                conn.commit()
        if fails >= CONSECUTIVE_FAILS_DEMOTE:
            _log.warning("Peer %s demoted to tier %d after %d consecutive failures",
                          node_id[:12], tier, fails)

    def recalculate_tiers(self, file_registry: "FileRegistry") -> None:
        """Recalculate tiers for all peers.

        Tier assignment logic:
          Tier 1 — "trusted circle":
            - Peer authored at least one file this node currently stores locally.
            - (Future / full plan): also peers that host replicas of files *we*
              authored.  That requires an author_id on this node and a replicas
              table cross-reference.  Currently that second check is skipped
              because the simplified single-node deployment does not populate
              its own author_id in a way that makes replica-hosting queries
              meaningful.  When the full replication layer is active, add a
              query against the replicas table filtered by our author_id.

          Tier 2 — "recently seen":
            - last_seen within TIER_2_RECENT_DAYS (default 7) AND not Tier 1.

          Tier 3 — "stale / unknown":
            - Everyone else.

        Uses a single batch UPDATE with executemany instead of per-row commits.
        """
        now = time.time()
        cutoff = now - TIER_2_RECENT_DAYS * 86400

        # ---- Tier 1 candidates ----
        tier1_ids: set[str] = set()

        # Peers that authored files this node stores locally
        for entry in file_registry.get_all():
            if file_registry.storage and file_registry.storage.has_file(
                entry.file_id
            ):
                tier1_ids.add(entry.author_id)

        # (Future) Peers hosting replicas of files we authored.
        # Omitted in simplified mode — see docstring.

        # ---- Calculate tier for every peer ----
        tier_updates: list[tuple[int, str]] = []

        with self._lock:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT node_id, last_seen FROM peers"
                ).fetchall()

            for row in rows:
                nid = row["node_id"]
                if nid in tier1_ids:
                    tier = 1
                elif row["last_seen"] >= cutoff:
                    tier = 2
                else:
                    tier = 3
                tier_updates.append((tier, nid))

            # Single batch UPDATE
            with self._get_conn() as conn:
                conn.executemany(
                    "UPDATE peers SET tier = ? WHERE node_id = ?",
                    tier_updates,
                )
                conn.commit()

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def cleanup(self, max_age_days: float = PEER_BOOK_CLEANUP_DAYS) -> int:
        """Delete old peers: last_seen < now - max_age_days AND tier >= 3
        AND consecutive_fails >= 10 AND is_bootstrap = 0."""
        cutoff = time.time() - max_age_days * 86400
        with self._lock:
            with self._get_conn() as conn:
                cursor = conn.execute(
                    """
                    DELETE FROM peers WHERE
                        last_seen < ? AND
                        tier >= 3 AND
                        consecutive_fails >= 10 AND
                        is_bootstrap = 0
                    """,
                    (cutoff,),
                )
                conn.commit()
                return cursor.rowcount

    def count(self) -> int:
        """Total peer count."""
        with self._lock:
            with self._get_conn() as conn:
                row = conn.execute("SELECT COUNT(*) as cnt FROM peers").fetchone()
        return row["cnt"]

    def count_by_tier(self, tier: int) -> int:
        """Count peers in a tier."""
        with self._lock:
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) as cnt FROM peers WHERE tier = ?", (tier,)
                ).fetchone()
        return row["cnt"]
