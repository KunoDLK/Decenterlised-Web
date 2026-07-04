"""
replication.py — Rebalancing & Diversity Logic

Calculate network target, decide which files to replicate/delete, maximise diversity.
"""

from __future__ import annotations

import statistics
import time
from typing import Optional, TYPE_CHECKING

from protocol import ReplicationSolicitPayload

if TYPE_CHECKING:
    from file_registry import FileRegistry, FileRegistryEntry
    from storage import StorageManager
    from peer_book import PeerBook
    from udp_engine import UDPEngine

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REBALANCE_INTERVAL: float = 60.0


# ---------------------------------------------------------------------------
# ReplicationManager
# ---------------------------------------------------------------------------


class ReplicationManager:
    """Manages file replication, diversity, and rebalancing."""

    def __init__(
        self,
        file_registry: "FileRegistry",
        storage: "StorageManager",
        peer_book: "PeerBook",
        udp_engine: "UDPEngine",
    ) -> None:
        self.file_registry = file_registry
        self.storage = storage
        self.peer_book = peer_book
        self.udp_engine = udp_engine
        self.rebalance_gate: bool = False
        self.estimated_network_target: int = 3
        self.received_targets: list[int] = []
        self.tier1_contacted: set[str] = set()
        self.tier1_total: int = 0

    # ------------------------------------------------------------------
    # Network target
    # ------------------------------------------------------------------

    def calculate_network_target(self) -> int:
        """floor(Σ storage_of_all_known_peers / Σ unique_file_size)."""
        # Estimate total network storage from peer count * average storage
        peer_count = self.peer_book.count()
        if peer_count == 0:
            return 3
        # Conservative: assume each peer has 500MB
        total_storage = peer_count * 500 * 1024 * 1024
        total_files = self.file_registry.total_unique_file_size()
        if total_files == 0:
            return max(3, peer_count // 2)
        target = total_storage // max(total_files, 1)
        return max(3, min(target, 10))  # clamp between 3 and 10

    def receive_target_estimate(self, value: int) -> None:
        """Record an estimate from a peer, recalculate median."""
        self.received_targets.append(value)
        if self.received_targets:
            self.estimated_network_target = int(
                statistics.median(self.received_targets)
            )
        self.estimated_network_target = max(3, self.estimated_network_target)

    def open_gate(self) -> None:
        """Allow rebalancing."""
        self.rebalance_gate = True

    def should_rebalance(self) -> bool:
        """True if gate open AND (connected >= 3 OR all Tier1 contacted)."""
        if not self.rebalance_gate:
            return False
        connected = len(self.udp_engine.get_connected_peers())
        all_tier1_done = (
            self.tier1_total > 0
            and len(self.tier1_contacted) >= self.tier1_total
        )
        return connected >= 3 or all_tier1_done

    # ------------------------------------------------------------------
    # Replication levels
    # ------------------------------------------------------------------

    def get_under_replicated(self) -> list["FileRegistryEntry"]:
        """Files where replica_count < networkTarget - 1."""
        target = self.estimated_network_target
        return [
            e
            for e in self.file_registry.get_all()
            if e.replica_count < target - 1
        ]

    def get_over_replicated(self) -> list["FileRegistryEntry"]:
        """Files where replica_count > networkTarget + 1, locally stored."""
        target = self.estimated_network_target
        return [
            e
            for e in self.file_registry.get_all()
            if e.replica_count > target + 1 and self.storage.has_file(e.file_id)
        ]

    def get_at_target_low(self) -> list["FileRegistryEntry"]:
        """Files at replica_count == networkTarget - 1."""
        target = self.estimated_network_target
        return [
            e for e in self.file_registry.get_all() if e.replica_count == target - 1
        ]

    def get_at_target_high(self) -> list["FileRegistryEntry"]:
        """Files at replica_count == networkTarget + 1, locally stored."""
        target = self.estimated_network_target
        return [
            e
            for e in self.file_registry.get_all()
            if e.replica_count == target + 1 and self.storage.has_file(e.file_id)
        ]

    def get_at_target_exact(self) -> list["FileRegistryEntry"]:
        """Files at replica_count == networkTarget, locally stored, not own."""
        target = self.estimated_network_target
        meta = self.storage._load_metadata()
        own = set(meta.get("own_files", []))
        return [
            e
            for e in self.file_registry.get_all()
            if e.replica_count == target
            and self.storage.has_file(e.file_id)
            and e.file_id not in own
        ]

    # ------------------------------------------------------------------
    # Diversity
    # ------------------------------------------------------------------

    def diversity_score(self, file_id: str) -> float:
        """Compute diversity: 1.0 - (|intersection| / |existing_replicas|)."""
        entry = self.file_registry.get(file_id)
        if entry is None or entry.replica_count == 0:
            return 0.0
        replica_nodes = {r.node_id for r in entry.replicas}
        connected_nodes = set(self.udp_engine.get_connected_peers())
        overlap = len(replica_nodes & connected_nodes)
        return 1.0 - (overlap / max(entry.replica_count, 1))

    def rank_by_diversity(
        self, candidates: list["FileRegistryEntry"]
    ) -> list["FileRegistryEntry"]:
        """Sort by diversity_score descending."""
        return sorted(candidates, key=lambda e: self.diversity_score(e.file_id), reverse=True)

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------

    def select_files_to_replicate(self, limit_bytes: int) -> list[str]:
        """Select under-replicated files to replicate, ranked by diversity."""
        under = self.get_under_replicated()
        ranked = self.rank_by_diversity(under)
        selected: list[str] = []
        used = 0
        for entry in ranked:
            if used + entry.file_size <= limit_bytes:
                selected.append(entry.file_id)
                used += entry.file_size
            if used >= limit_bytes:
                break
        # If space remains, consider at_target_low
        if used < limit_bytes:
            low = self.rank_by_diversity(self.get_at_target_low())
            for entry in low:
                if used + entry.file_size <= limit_bytes:
                    selected.append(entry.file_id)
                    used += entry.file_size
        return selected

    def select_files_to_delete(self, needed_bytes: int) -> list[str]:
        """Select files to delete to free needed_bytes.
        Priority: over-replicated > at_target_high > at_target_exact.
        Never own files. Never sole replica holder.
        """
        from file_registry import FileRegistryEntry

        selected: list[str] = []
        freed = 0
        meta = self.storage._load_metadata()
        own = set(meta.get("own_files", []))

        candidates: list[FileRegistryEntry] = []

        # Over-replicated (most over-replicated first)
        over = sorted(
            self.get_over_replicated(),
            key=lambda e: e.replica_count,
            reverse=True,
        )
        candidates.extend(over)

        # At target high
        candidates.extend(self.get_at_target_high())

        # At target exact (only if critically vulnerable files exist)
        has_critical = any(
            e.replica_count <= max(1, self.estimated_network_target // 2)
            for e in self.file_registry.get_all()
        )
        if has_critical or freed < needed_bytes:
            candidates.extend(self.get_at_target_exact())

        for entry in candidates:
            if entry.file_id in own:
                continue
            if entry.replica_count <= 1:
                continue  # Last replica — don't delete
            if entry.file_id in selected:
                continue
            selected.append(entry.file_id)
            freed += entry.file_size
            if freed >= needed_bytes:
                break

        return selected

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    def execute_rebalance(self) -> None:
        """Main rebalance logic."""
        if not self.should_rebalance():
            return

        available = self.storage.available_bytes()
        under = self.get_under_replicated()

        if under and available > 0:
            to_replicate = self.select_files_to_replicate(available)
            for fid in to_replicate:
                self.solicit_replication(fid)

        if under and self.storage.is_over_quota():
            needed = sum(
                e.file_size
                for e in under
                if not self.storage.has_file(e.file_id)
            )
            to_delete = self.select_files_to_delete(needed)
            for fid in to_delete:
                self.storage.delete_file(fid)
                self.file_registry.decrement_replica(
                    fid, self.file_registry.node_id
                )
            # Now try replicating again
            available = self.storage.available_bytes()
            to_replicate = self.select_files_to_replicate(available)
            for fid in to_replicate:
                self.solicit_replication(fid)

        # If over-replicated and no under-replicated, free some space
        if not under:
            over = self.get_over_replicated()
            if over:
                for entry in over[:3]:  # delete up to 3 over-replicated
                    meta = self.storage._load_metadata()
                    own = set(meta.get("own_files", []))
                    if entry.file_id not in own and entry.replica_count > 1:
                        self.storage.delete_file(entry.file_id)
                        self.file_registry.decrement_replica(
                            entry.file_id, self.file_registry.node_id
                        )

    def solicit_replication(self, file_id: str) -> None:
        """Broadcast replication solicitation."""
        entry = self.file_registry.get(file_id)
        if entry is None:
            return
        payload = ReplicationSolicitPayload(
            file_id=entry.file_id,
            file_name=entry.file_name,
            file_size=entry.file_size,
            author_id=entry.author_id,
        )
        from protocol import MsgType, MessageBuilder

        self.udp_engine.broadcast(
            MsgType.REPLICATION_SOLICIT, MessageBuilder.replication_solicit(payload)
        )

    def consider_solicit(self, payload: ReplicationSolicitPayload) -> bool:
        """Consider a replication solicitation. Returns True if accepting."""
        if self.storage.has_file(payload.file_id):
            return False
        available = self.storage.available_bytes()
        if payload.file_size > available:
            return False
        # Accept — download and store
        data = self.udp_engine.download_file(payload.file_id)
        self.storage.store_replica(payload.file_id, data)
        self.file_registry.increment_replica(
            payload.file_id, self.file_registry.node_id
        )
        return True

    def on_peer_disconnected(self, node_id: str) -> None:
        """Handle peer disconnection: remove replicas, trigger rebalance."""
        self.file_registry.remove_peer_replicas(node_id)
        if self.rebalance_gate:
            self.execute_rebalance()
