"""
storage.py — Disk Storage & Quota Manager

Manage files on disk, track storage usage, enforce quotas.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Optional

_log = logging.getLogger("storage")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_STORAGE_MB: int = 500
TEMPORARY_REPLICA_TTL: float = 3600.0  # 1 hour


# ---------------------------------------------------------------------------
# StorageManager
# ---------------------------------------------------------------------------


class StorageManager:
    """Manage files on disk with quota tracking."""

    def __init__(self, data_dir: str, total_mb: int = 0) -> None:
        self.files_dir = str(Path(data_dir) / "files")
        self.total_configured_mb = total_mb if total_mb > 0 else DEFAULT_STORAGE_MB
        self.author_id: Optional[str] = None
        self._lock = threading.Lock()

        Path(self.files_dir).mkdir(parents=True, exist_ok=True)

    @property
    def _metadata_path(self) -> str:
        return str(Path(self.files_dir) / ".metadata.json")

    def _load_metadata(self) -> dict:
        """Load metadata JSON."""
        path = self._metadata_path
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
        return {"own_files": [], "replica_files": [], "temporary_files": {}}

    def _save_metadata(self, meta: dict) -> None:
        """Save metadata JSON."""
        with open(self._metadata_path, "w") as f:
            json.dump(meta, f, indent=2)

    # ------------------------------------------------------------------
    # Store
    # ------------------------------------------------------------------

    def store_own_file(
        self, file_id: str, data: bytes, file_name: str, mime_type: str
    ) -> str:
        """Write file to disk as own file."""
        filepath = str(Path(self.files_dir) / file_id)
        with open(filepath, "wb") as f:
            f.write(data)

        with self._lock:
            meta = self._load_metadata()
            if file_id not in meta["own_files"]:
                meta["own_files"].append(file_id)
            self._save_metadata(meta)

        _log.info("Store own: %s (%s, %s, %d bytes)", file_id[:12], file_name, mime_type, len(data))
        return filepath

    def store_replica(self, file_id: str, data: bytes) -> str:
        """Write file to disk as a replica."""
        filepath = str(Path(self.files_dir) / file_id)
        with open(filepath, "wb") as f:
            f.write(data)

        with self._lock:
            meta = self._load_metadata()
            if file_id not in meta["replica_files"]:
                meta["replica_files"].append(file_id)
            # Remove from temporary if it was there
            meta["temporary_files"].pop(file_id, None)
            self._save_metadata(meta)

        _log.info("Store replica: %s (%d bytes)", file_id[:12], len(data))
        return filepath

    def store_temporary_replica(
        self, file_id: str, data: bytes, tab_id: str
    ) -> str:
        """Write file as temporary replica (user opened/downloaded)."""
        filepath = str(Path(self.files_dir) / file_id)
        with open(filepath, "wb") as f:
            f.write(data)

        with self._lock:
            meta = self._load_metadata()
            meta["temporary_files"][file_id] = {
                "expires_at": time.time() + TEMPORARY_REPLICA_TTL,
                "tab_id": tab_id,
            }
            self._save_metadata(meta)

        _log.info("Store temporary: %s (%d bytes, tab=%s)", file_id[:12], len(data), tab_id)
        return filepath

    def promote_temporary(self, file_id: str) -> None:
        """Move from temporary to regular replica."""
        with self._lock:
            meta = self._load_metadata()
            if file_id in meta["temporary_files"]:
                del meta["temporary_files"][file_id]
                if file_id not in meta["replica_files"]:
                    meta["replica_files"].append(file_id)
                self._save_metadata(meta)
                _log.info("Promoted temporary → replica: %s", file_id[:12])

    def cleanup_expired_temporary(self) -> list[str]:
        """Promote expired temporary replicas. Returns promoted file_ids."""
        now = time.time()
        promoted: list[str] = []
        with self._lock:
            meta = self._load_metadata()
            expired = [
                fid
                for fid, info in meta["temporary_files"].items()
                if info["expires_at"] < now
            ]
            for fid in expired:
                del meta["temporary_files"][fid]
                if fid not in meta["replica_files"]:
                    meta["replica_files"].append(fid)
                promoted.append(fid)
            if promoted:
                self._save_metadata(meta)
        return promoted

    # ------------------------------------------------------------------
    # Read / Check / Delete
    # ------------------------------------------------------------------

    def has_file(self, file_id: str) -> bool:
        """True if file exists on disk."""
        return os.path.isfile(str(Path(self.files_dir) / file_id))

    def read_file(self, file_id: str) -> bytes:
        """Read file content. Raises FileNotFoundError if missing."""
        filepath = str(Path(self.files_dir) / file_id)
        if not os.path.isfile(filepath):
            raise FileNotFoundError(f"File not found: {file_id}")
        with open(filepath, "rb") as f:
            return f.read()

    def delete_file(self, file_id: str) -> bool:
        """Delete file from disk. Returns True if deleted."""
        filepath = str(Path(self.files_dir) / file_id)
        if os.path.isfile(filepath):
            os.remove(filepath)
            with self._lock:
                meta = self._load_metadata()
                meta["own_files"] = [f for f in meta["own_files"] if f != file_id]
                meta["replica_files"] = [
                    f for f in meta["replica_files"] if f != file_id
                ]
                meta["temporary_files"].pop(file_id, None)
                self._save_metadata(meta)
            _log.info("Deleted file: %s", file_id[:12])
            return True
        return False

    # ------------------------------------------------------------------
    # Quota
    # ------------------------------------------------------------------

    def used_for_own_files(self) -> int:
        """Bytes used by own published files."""
        total = 0
        meta = self._load_metadata()
        for fid in meta["own_files"]:
            filepath = str(Path(self.files_dir) / fid)
            if os.path.isfile(filepath):
                total += os.path.getsize(filepath)
        return total

    def used_for_replicas(self) -> int:
        """Bytes used by replica files."""
        total = 0
        meta = self._load_metadata()
        for fid in meta["replica_files"]:
            filepath = str(Path(self.files_dir) / fid)
            if os.path.isfile(filepath):
                total += os.path.getsize(filepath)
        for fid in meta["temporary_files"]:
            filepath = str(Path(self.files_dir) / fid)
            if os.path.isfile(filepath):
                total += os.path.getsize(filepath)
        return total

    def available_bytes(self) -> int:
        """Free bytes under quota."""
        return max(0, self.total_configured_bytes() - self.used_for_own_files() - self.used_for_replicas())

    def total_configured_bytes(self) -> int:
        """Total configured quota in bytes."""
        return self.total_configured_mb * 1024 * 1024

    def is_over_quota(self) -> bool:
        """True if usage exceeds quota."""
        return self.used_for_own_files() + self.used_for_replicas() > self.total_configured_bytes()

    def get_storage_breakdown(self) -> dict:
        """Return {own, replicas, available, total} in bytes."""
        own = self.used_for_own_files()
        replicas = self.used_for_replicas()
        total = self.total_configured_bytes()
        return {
            "own": own,
            "replicas": replicas,
            "available": max(0, total - own - replicas),
            "total": total,
        }

    def set_quota(self, mb: int) -> None:
        """Update quota in MB."""
        self.total_configured_mb = mb

    def get_files_sorted_by_size(self) -> list[tuple[str, int]]:
        """Return (file_id, size) sorted largest first for all local files."""
        files: list[tuple[str, int]] = []
        meta = self._load_metadata()
        for fid in set(
            meta["own_files"] + meta["replica_files"] + list(meta["temporary_files"].keys())
        ):
            filepath = str(Path(self.files_dir) / fid)
            if os.path.isfile(filepath):
                files.append((fid, os.path.getsize(filepath)))
        files.sort(key=lambda x: x[1], reverse=True)
        return files
