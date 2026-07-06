"""
file_registry.py — Decentralised File Registry

Local copy of the network-wide file registry, SQLite-backed, gossiped between peers.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional, TYPE_CHECKING

from protocol import FileRegistryEntry, ReplicaEntry
from identity import AuthorIdentity

if TYPE_CHECKING:
    from storage import StorageManager

_log = logging.getLogger("registry")

# ---------------------------------------------------------------------------
# FileRegistry
# ---------------------------------------------------------------------------


class FileRegistry:
    """SQLite-backed file registry with in-memory cache."""

    def __init__(self, data_dir: str, node_id: str) -> None:
        self.db_path = str(Path(data_dir) / "registry.db")
        self.node_id = node_id
        self.entries: dict[str, FileRegistryEntry] = {}
        self._lock = threading.RLock()
        self.storage: Optional["StorageManager"] = None

        Path(data_dir).mkdir(parents=True, exist_ok=True)

        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    file_id             TEXT PRIMARY KEY,
                    file_name           TEXT NOT NULL,
                    file_size           INTEGER NOT NULL,
                    mime_type           TEXT NOT NULL,
                    author_id           TEXT NOT NULL,
                    author_public_key   BLOB NOT NULL,
                    replica_count       INTEGER NOT NULL DEFAULT 0,
                    author_signature    BLOB NOT NULL,
                    timestamp           REAL NOT NULL,
                    previous_file_id    TEXT DEFAULT '',
                    is_deleted          INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS replicas (
                    file_id     TEXT NOT NULL,
                    node_id     TEXT NOT NULL,
                    added_at    REAL NOT NULL,
                    is_local    INTEGER DEFAULT 0,
                    PRIMARY KEY (file_id, node_id)
                )
                """
            )
            # ---- Transfers table (replaces UploadState/DownloadState) ----
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS transfers (
                    transfer_id     TEXT PRIMARY KEY,
                    file_id         TEXT NOT NULL,
                    peer_id         TEXT NOT NULL,
                    direction       TEXT NOT NULL DEFAULT 'download',
                    total_chunks    INTEGER NOT NULL DEFAULT 0,
                    current_chunk   INTEGER NOT NULL DEFAULT 0,
                    state           TEXT NOT NULL DEFAULT 'active',
                    created_at      REAL NOT NULL,
                    completed_at    REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS transfer_chunks (
                    transfer_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    received    INTEGER NOT NULL DEFAULT 0,
                    sent_at     REAL,
                    acked_at    REAL,
                    retries     INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (transfer_id, chunk_index)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_author ON files(author_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_replica ON files(replica_count)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_replicas_node ON replicas(node_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_transfers_file ON transfers(file_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_transfers_peer ON transfers(peer_id)"
            )
            conn.commit()

        self._load_cache()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _load_cache(self) -> None:
        """Load all non-deleted entries into memory."""
        with self._lock:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM files WHERE is_deleted = 0"
                ).fetchall()
            self.entries.clear()
            for row in rows:
                entry = self._row_to_entry(dict(row))
                self.entries[entry.file_id] = entry

    def _row_to_entry(self, row: dict) -> FileRegistryEntry:
        """Convert a DB row dict to a FileRegistryEntry."""
        with self._get_conn() as conn:
            rep_rows = conn.execute(
                "SELECT node_id, added_at FROM replicas WHERE file_id = ?",
                (row["file_id"],),
            ).fetchall()
        replicas = [
            ReplicaEntry(node_id=r["node_id"], added_at=r["added_at"])
            for r in rep_rows
        ]
        return FileRegistryEntry(
            file_id=row["file_id"],
            file_name=row["file_name"],
            file_size=row["file_size"],
            mime_type=row["mime_type"],
            author_id=row["author_id"],
            author_public_key=bytes(row["author_public_key"]),
            replica_count=row["replica_count"],
            author_signature=bytes(row["author_signature"]),
            replicas=replicas,
            timestamp=row["timestamp"],
            previous_file_id=row["previous_file_id"] or "",
            is_deleted=bool(row["is_deleted"]),
        )

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def add(self, entry: FileRegistryEntry) -> None:
        """INSERT OR REPLACE entry and its replicas."""
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO files
                    (file_id, file_name, file_size, mime_type, author_id,
                     author_public_key, replica_count, author_signature,
                     timestamp, previous_file_id, is_deleted)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry.file_id,
                        entry.file_name,
                        entry.file_size,
                        entry.mime_type,
                        entry.author_id,
                        entry.author_public_key,
                        entry.replica_count,
                        entry.author_signature,
                        entry.timestamp,
                        entry.previous_file_id,
                        1 if entry.is_deleted else 0,
                    ),
                )
                for r in entry.replicas:
                    conn.execute(
                        "INSERT OR IGNORE INTO replicas (file_id, node_id, added_at) VALUES (?, ?, ?)",
                        (entry.file_id, r.node_id, r.added_at),
                    )
                conn.commit()
            self.entries[entry.file_id] = entry
        _log.info("Registry add: %s (%s, %d bytes, %d replicas)",
                    entry.file_id[:12], entry.file_name, entry.file_size, entry.replica_count)

    def update(self, entry: FileRegistryEntry) -> None:
        """Update only if incoming timestamp > existing."""
        existing = self.entries.get(entry.file_id)
        if existing is None or entry.timestamp > existing.timestamp:
            self.add(entry)

    def get(self, file_id: str) -> Optional[FileRegistryEntry]:
        """Get entry from cache."""
        return self.entries.get(file_id)

    def get_all(self) -> list[FileRegistryEntry]:
        """All non-deleted entries."""
        return [e for e in self.entries.values() if not e.is_deleted]

    def get_by_author(self, author_id: str) -> list[FileRegistryEntry]:
        """Files by a specific author."""
        return [e for e in self.entries.values() if e.author_id == author_id]

    def mark_deleted(self, file_id: str) -> None:
        """Mark file as deleted."""
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE files SET is_deleted = 1 WHERE file_id = ?",
                    (file_id,),
                )
                conn.commit()
            self.entries.pop(file_id, None)
        _log.info("Registry mark deleted: %s", file_id[:12])

    # ------------------------------------------------------------------
    # Replica management
    # ------------------------------------------------------------------

    def increment_replica(self, file_id: str, node_id: str) -> None:
        """Add a replica and increment count."""
        now = time.time()
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO replicas (file_id, node_id, added_at) VALUES (?, ?, ?)",
                    (file_id, node_id, now),
                )
                conn.execute(
                    "UPDATE files SET replica_count = replica_count + 1 WHERE file_id = ?",
                    (file_id,),
                )
                conn.commit()
            entry = self.entries.get(file_id)
            if entry:
                entry.replica_count += 1
                entry.replicas.append(ReplicaEntry(node_id=node_id, added_at=now))

    def decrement_replica(self, file_id: str, node_id: str) -> None:
        """Remove a replica and decrement count (min 0)."""
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "DELETE FROM replicas WHERE file_id = ? AND node_id = ?",
                    (file_id, node_id),
                )
                conn.execute(
                    "UPDATE files SET replica_count = MAX(0, replica_count - 1) WHERE file_id = ?",
                    (file_id,),
                )
                conn.commit()
            entry = self.entries.get(file_id)
            if entry:
                entry.replica_count = max(0, entry.replica_count - 1)
                entry.replicas = [
                    r for r in entry.replicas if r.node_id != node_id
                ]

    def remove_peer_replicas(self, node_id: str) -> list[str]:
        """Remove all replicas hosted by a peer. Returns affected file_ids."""
        affected: list[str] = []
        with self._lock:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT file_id FROM replicas WHERE node_id = ?", (node_id,)
                ).fetchall()
                affected = [r["file_id"] for r in rows]
                conn.execute(
                    "DELETE FROM replicas WHERE node_id = ?", (node_id,)
                )
                for fid in affected:
                    conn.execute(
                        "UPDATE files SET replica_count = MAX(0, replica_count - 1) WHERE file_id = ?",
                        (fid,),
                    )
                conn.commit()
            for fid in affected:
                entry = self.entries.get(fid)
                if entry:
                    entry.replica_count = max(0, entry.replica_count - 1)
                    entry.replicas = [
                        r for r in entry.replicas if r.node_id != node_id
                    ]
        if affected:
            _log.info("Removed replicas for peer %s from %d files", node_id[:12], len(affected))
        return affected

    # ------------------------------------------------------------------
    # Gossip / sync
    # ------------------------------------------------------------------

    def compute_hash(self) -> str:
        """SHA-256 of all (file_id, timestamp) pairs sorted."""
        with self._lock:
            pairs = sorted(
                (e.file_id, e.timestamp) for e in self.entries.values()
            )
        data = "".join(f"{fid}:{ts}" for fid, ts in pairs).encode("utf-8")
        return hashlib.sha256(data).hexdigest()

    def get_delta(self, their_hash: str) -> list[FileRegistryEntry]:
        """Return delta: empty if hashes match; full if new peer; last-24h otherwise."""
        if their_hash == self.compute_hash():
            return []
        # New peer (empty/None hash) → full sync
        if not their_hash:
            return self.get_all()
        # Hashes differ → return entries modified in the last 24 hours
        since = time.time() - 86400
        return self.get_entries_since(since)

    def get_entries_since(self, timestamp: float) -> list[FileRegistryEntry]:
        """Return all non-deleted entries with timestamp > the given value."""
        return [
            e for e in self.entries.values()
            if not e.is_deleted and e.timestamp > timestamp
        ]

    def merge_delta(self, entries: list[FileRegistryEntry]) -> None:
        """Merge entries from another peer (latest timestamp wins)."""
        for entry in entries:
            existing = self.entries.get(entry.file_id)
            if existing is None or entry.timestamp > existing.timestamp:
                self.add(entry)

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    @staticmethod
    def verify_author_signature(entry: FileRegistryEntry) -> bool:
        """Verify the author signature on a registry entry."""
        import struct

        pb = (
            __import__("wire").PayloadBuilder()
            .add_string(entry.file_id)
            .add_string(entry.file_name)
            .add_uint64(entry.file_size)
            .add_string(entry.mime_type)
            .add_string(entry.author_id)
            .add_uint64(int(entry.timestamp * 1_000_000))
        )
        signed_data = pb.build()
        return AuthorIdentity.verify(
            signed_data, entry.author_signature, entry.author_public_key
        )

    # ------------------------------------------------------------------
    # Transfer management (replaces UploadState / DownloadState in udp_engine)
    # ------------------------------------------------------------------

    def create_transfer(
        self,
        transfer_id: str,
        file_id: str,
        peer_id: str,
        direction: str = "download",
        total_chunks: int = 0,
    ) -> None:
        """Create a new transfer record."""
        now = time.time()
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO transfers
                       (transfer_id, file_id, peer_id, direction, total_chunks,
                        current_chunk, state, created_at)
                       VALUES (?, ?, ?, ?, ?, 0, 'active', ?)""",
                    (transfer_id, file_id, peer_id, direction, total_chunks, now),
                )
                conn.commit()

    def get_transfer(self, transfer_id: str) -> Optional[dict]:
        """Get a transfer record."""
        with self._lock:
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT * FROM transfers WHERE transfer_id=?",
                    (transfer_id,),
                ).fetchone()
        return dict(row) if row else None

    def update_transfer_progress(
        self, transfer_id: str, chunk_index: int, total_chunks: int
    ) -> None:
        """Record a received chunk and update progress."""
        now = time.time()
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO transfer_chunks
                       (transfer_id, chunk_index, received, sent_at)
                       VALUES (?, ?, 1, ?)""",
                    (transfer_id, chunk_index, now),
                )
                conn.execute(
                    """UPDATE transfers SET total_chunks=MAX(total_chunks, ?),
                       current_chunk=? WHERE transfer_id=?""",
                    (total_chunks, chunk_index + 1, transfer_id),
                )
                conn.commit()

    def get_transfer_progress(self, transfer_id: str) -> tuple[int, int, set[int]]:
        """Return (total_chunks, current_chunk, set of received chunk indices)."""
        with self._lock:
            with self._get_conn() as conn:
                t = conn.execute(
                    "SELECT total_chunks, current_chunk FROM transfers WHERE transfer_id=?",
                    (transfer_id,),
                ).fetchone()
                if t is None:
                    return (0, 0, set())
                chunks = conn.execute(
                    "SELECT chunk_index FROM transfer_chunks WHERE transfer_id=? AND received=1",
                    (transfer_id,),
                ).fetchall()
        received = {c["chunk_index"] for c in chunks}
        return (t["total_chunks"], t["current_chunk"], received)

    def is_transfer_complete(self, transfer_id: str) -> bool:
        """Check if all chunks have been received."""
        total, _, received = self.get_transfer_progress(transfer_id)
        return total > 0 and len(received) >= total

    def mark_transfer_complete(self, transfer_id: str) -> None:
        """Mark a transfer as complete."""
        now = time.time()
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE transfers SET state='complete', completed_at=? WHERE transfer_id=?",
                    (now, transfer_id),
                )
                conn.commit()

    def mark_transfer_failed(self, transfer_id: str) -> None:
        """Mark a transfer as failed."""
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    "UPDATE transfers SET state='failed' WHERE transfer_id=?",
                    (transfer_id,),
                )
                conn.commit()

    def increment_chunk_retry(self, transfer_id: str, chunk_index: int) -> int:
        """Increment retry count for a chunk. Returns new count."""
        with self._lock:
            with self._get_conn() as conn:
                conn.execute(
                    """INSERT INTO transfer_chunks (transfer_id, chunk_index, received, retries)
                       VALUES (?, ?, 0, 1)
                       ON CONFLICT(transfer_id, chunk_index) DO UPDATE SET
                       retries=transfer_chunks.retries + 1""",
                    (transfer_id, chunk_index),
                )
                conn.commit()
                row = conn.execute(
                    "SELECT retries FROM transfer_chunks WHERE transfer_id=? AND chunk_index=?",
                    (transfer_id, chunk_index),
                ).fetchone()
        return row["retries"] if row else 0

    def cleanup_transfers(self, max_age: float = 3600) -> int:
        """Remove old transfer records. Returns count removed."""
        cutoff = time.time() - max_age
        with self._lock:
            with self._get_conn() as conn:
                c1 = conn.execute(
                    "DELETE FROM transfer_chunks WHERE transfer_id IN "
                    "(SELECT transfer_id FROM transfers WHERE created_at < ?)",
                    (cutoff,),
                )
                c2 = conn.execute(
                    "DELETE FROM transfers WHERE created_at < ?",
                    (cutoff,),
                )
                conn.commit()
        return c2.rowcount

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def total_unique_file_size(self) -> int:
        """Sum of all unique file sizes."""
        return sum(e.file_size for e in self.entries.values() if not e.is_deleted)

    def count(self) -> int:
        """Number of non-deleted files."""
        return len([e for e in self.entries.values() if not e.is_deleted])

    def cleanup_old_versions(self, max_age_seconds: float = 86400) -> int:
        """Remove old versions where a newer one exists via previous_file_id chain."""
        removed = 0
        now = time.time()
        with self._lock:
            # Find old versions: entries where a newer version exists
            # and timestamp < now - max_age_seconds
            new_ids: set[str] = {
                e.previous_file_id
                for e in self.entries.values()
                if e.previous_file_id
            }
            for fid in list(new_ids):
                entry = self.entries.get(fid)
                if entry and entry.timestamp < now - max_age_seconds:
                    with self._get_conn() as conn:
                        conn.execute(
                            "DELETE FROM files WHERE file_id = ?", (fid,)
                        )
                        conn.execute(
                            "DELETE FROM replicas WHERE file_id = ?", (fid,)
                        )
                        conn.commit()
                    self.entries.pop(fid, None)
                    removed += 1
        return removed

    def get_version_chain(self, file_id: str) -> list[str]:
        """Follow previous_file_id links to build the full version chain (oldest first)."""
        chain: list[str] = []
        current = file_id
        visited: set[str] = set()

        # Walk back to oldest
        temp: list[str] = []
        while current and current not in visited:
            visited.add(current)
            entry = self.entries.get(current)
            if entry is None:
                break
            temp.append(current)
            current = entry.previous_file_id
            if not current:
                break

        # Reverse to get oldest first
        chain = list(reversed(temp))
        return chain
