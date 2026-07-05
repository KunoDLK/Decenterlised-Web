"""
peer_book.py — SQLite Peer Directory

Persistent directory of all known peers with tiering, last-seen tracking, cleanup.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from file_registry import FileRegistry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TIER_2_RECENT_DAYS: int = 7
CONSECUTIVE_FAILS_DEMOTE: int = 5
PEER_BOOK_CLEANUP_DAYS: int = 30


# ---------------------------------------------------------------------------
# PeerBook
# ---------------------------------------------------------------------------


class PeerBook:
    """SQLite-backed peer directory with tier-based prioritisation."""

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
                    is_bootstrap    INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_peers_tier ON peers(tier, last_seen DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_peers_last_seen ON peers(last_seen)"
            )
            conn.commit()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

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
