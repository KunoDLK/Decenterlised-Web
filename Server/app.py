"""
app.py — Main Entry Point (Scheduler-Driven Architecture)

Central orchestrator: parses CLI args, initialises all modules, wires them
together via the unified Scheduler, launches TUI and/or web server.

All periodic work is driven by the scheduler — no scattered timer threads.
All per-connection state lives in the peer_book DB — the UDP engine is thin.
"""

from __future__ import annotations

import argparse
import atexit
import hashlib
import json
import logging
import os
import signal
import struct
import sys
import threading
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Optional

import log_utils
import wire
from identity import (
    NodeIdentity,
    AuthorIdentity,
    sha256_hex,
    public_key_to_base64,
    public_key_from_base64,
)
from reliable import ReliabilityManager
from protocol import (
    MsgType,
    MessageBuilder,
    MessageParser,
    FilePublishPayload,
    FileUpdatePayload,
    FileDeletePayload,
    FileRegistryEntry,
    ReplicaEntry,
    HelloPayload,
    PeerListResponsePayload,
    PeerEntry,
    FileRegistryResponsePayload,
    FileAnnouncePayload,
    FileChunkAckPayload,
    FileChunkPayload,
    FileRequestPayload,
    ReplicationSolicitPayload,
    ReplicationAckPayload,
    ConnectIntroducePayload,
    ConnectRequestPayload,
    ConnectAckPayload,
    ShareFileQueryPayload,
    ShareFileResponsePayload,
    PingPayload,
)
from stun import StunError
from udp_engine import UDPEngine
from peer_book import PeerBook
from file_registry import FileRegistry
from storage import StorageManager
from replication import ReplicationManager
from scheduler import Scheduler, Action, ActionType, Priority
from config import PeerConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BOOTSTRAP_PEERS: list[tuple[str, str, str, int]] = [
    # Format: (node_id, public_key_base64, ip, port)
]

MIN_PUBLISH_BYTES: int = 1_048_576  # 1MB
MAX_CONCURRENT_HOLE_PUNCH: int = 10


# ---------------------------------------------------------------------------
# EventBus
# ---------------------------------------------------------------------------


class EventBus:
    """Simple pub/sub for WebSocket notifications."""

    def __init__(self) -> None:
        self._subscribers: dict[str, set[Callable]] = defaultdict(set)

    def subscribe(self, event_type: str, callback: Callable[[dict], None]) -> None:
        self._subscribers[event_type].add(callback)

    def unsubscribe(self, event_type: str, callback: Callable[[dict], None]) -> None:
        self._subscribers[event_type].discard(callback)

    def emit(self, event_type: str, **data: Any) -> None:
        data["type"] = event_type
        for cb in self._subscribers.get(event_type, set()):
            try:
                cb(data)
            except Exception:
                pass
        for cb in self._subscribers.get("*", set()):
            try:
                cb(data)
            except Exception:
                pass


# ===================================================================
# App
# ===================================================================


class App:
    """Central orchestrator — scheduler-driven, DB-backed state."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.data_dir = os.path.expanduser(args.data_dir)
        self.port = args.port + args.tui_port_offset
        self.web_port = args.web_port + args.tui_port_offset if args.web_port > 0 else 0
        self.web_host = args.web_host
        self.no_tui = args.no_tui
        self.no_lan = args.no_lan
        self._stopped = False

        # Build config from args
        self.config = PeerConfig(
            udp_port=self.port,
            web_port=self.web_port,
            web_host=self.web_host,
            no_tui=self.no_tui,
            no_lan=self.no_lan,
            storage_limit_mb=args.storage_limit,
            data_dir=self.data_dir,
            log_file=args.log,
            udp_trace_file=args.udp_trace,
            tui_port_offset=args.tui_port_offset,
        )

        Path(self.data_dir).mkdir(parents=True, exist_ok=True)

        # Logging
        if self.config.log_file:
            log_utils.configure(self.data_dir, self.config.log_file, self.no_tui)
        if self.config.udp_trace_file:
            log_utils.configure_udp_trace(self.data_dir, self.config.udp_trace_file)
        self._log = logging.getLogger("app")

        # Lock file
        self._acquire_lock()

        # Identity
        self.node_identity = NodeIdentity.load_or_create(self.data_dir)
        self.author_identity: Optional[AuthorIdentity] = None
        self.author_mode: str = "browse_only"

        # Event bus
        self.event_bus = EventBus()

        # DB-backed modules
        self.peer_book = PeerBook(self.data_dir)
        self.file_registry = FileRegistry(self.data_dir, self.node_identity.node_id)
        self.storage = StorageManager(self.data_dir, self.config.storage_limit_mb)
        self.file_registry.storage = self.storage

        # Reliability (event-driven, no polling)
        self.reliable = ReliabilityManager(
            ack_timeout=self.config.ack_timeout_base,
            max_retries=self.config.max_retries,
            ack_timeout_max=self.config.ack_timeout_max,
            ack_timeout_multiplier=self.config.ack_timeout_multiplier,
        )

        # Unified scheduler (replaces all timer threads)
        self.scheduler = Scheduler(debug=getattr(args, "debug", False))

        # UDP engine (thin — just socket + recv + send)
        self.udp_engine = UDPEngine(
            port=self.port,
            node_identity=self.node_identity,
            reliable=self.reliable,
            peer_book=self.peer_book,
            scheduler=self.scheduler,
            max_chunk_size=self.config.max_chunk_size,
        )
        self.udp_engine.set_packet_handler(self._handle_packet)

        # Replication (uses scheduler)
        self.replication = ReplicationManager(
            self.file_registry,
            self.storage,
            self.peer_book,
            self.udp_engine,
            self.scheduler,
        )

        # Register scheduler action handlers
        self._register_scheduler_handlers()

        # Web app / TUI (created later)
        self.web_app = None
        self.tui = None

    # ==================================================================
    # Packet handler — called by UDP engine recv thread
    # ==================================================================

    def _handle_packet(
        self, wm: "wire.WireMessage", from_addr: tuple[str, int], udp: UDPEngine
    ) -> None:
        """Dispatch an incoming decoded packet. Updates DB, queues actions."""
        msg_type = wm.msg_type
        sender_prefix = wm.sender_id_prefix.hex()
        sender_id = udp.resolve_node_id(from_addr) or sender_prefix
        self.peer_book.mark_seen(sender_id)
        if self.reliable.is_duplicate(sender_id, wm.seq_num):
            return
        try:
            if msg_type == MsgType.HELLO:
                self._on_hello(wm, sender_id, from_addr)
            elif msg_type == MsgType.PING:
                self._on_ping(wm, sender_id)
            elif msg_type == MsgType.PEER_LIST_REQUEST:
                self._on_peer_list_request(sender_id)
            elif msg_type == MsgType.PEER_LIST_RESPONSE:
                self._on_peer_list_response(wm)
            elif msg_type == MsgType.FILE_REGISTRY_QUERY:
                self._on_file_registry_query(sender_id)
            elif msg_type == MsgType.FILE_REGISTRY_RESPONSE:
                self._on_file_registry_response(wm)
            elif msg_type == MsgType.FILE_REGISTRY_PUSH:
                self._on_file_registry_push(wm, sender_id)
            elif msg_type == MsgType.FILE_REQUEST:
                self._on_file_request(wm, sender_id)
            elif msg_type == MsgType.FILE_CHUNK:
                self._on_file_chunk(wm, sender_id)
            elif msg_type == MsgType.FILE_CHUNK_ACK:
                self._on_file_chunk_ack(wm, sender_id)
            elif msg_type == MsgType.FILE_ANNOUNCE:
                self._on_file_announce(wm)
            elif msg_type == MsgType.REPLICATION_SOLICIT:
                self._on_replication_solicit(wm, sender_id)
            elif msg_type == MsgType.REPLICATION_ACK:
                self._on_replication_ack(wm)
            elif msg_type == MsgType.FILE_PUBLISH:
                self._on_file_publish(wm, sender_id)
            elif msg_type == MsgType.FILE_UPDATE:
                self._on_file_update(wm, sender_id)
            elif msg_type == MsgType.FILE_DELETE:
                self._on_file_delete(wm)
            elif msg_type == MsgType.CONNECT_REQUEST:
                self._on_connect_request(wm, from_addr)
            elif msg_type == MsgType.CONNECT_INTRODUCE:
                self._on_connect_introduce(wm)
            elif msg_type == MsgType.CONNECT_ACK:
                self._on_connect_ack(wm)
            elif msg_type == MsgType.SHARE_FILE_QUERY:
                self._on_share_file_query(wm, sender_id)
            elif msg_type == MsgType.SHARE_FILE_RESPONSE:
                self._on_share_file_response(wm)
            elif msg_type == MsgType.GOODBYE:
                self._on_goodbye(sender_id)
        except Exception:
            self._log.exception("Error handling packet type=%d from %s", msg_type, sender_id[:12])

    # ---- Packet sub-handlers (update DB, queue actions) ----

    def _on_hello(self, wm, sender_id: str, from_addr: tuple[str, int]) -> None:
        p = MessageParser.hello(wm.payload)
        sign_data = (
            p.node_id.encode() + p.public_ip.encode()
            + struct.pack(">H", p.public_port)
            + struct.pack(">Q", int(p.uptime_since * 1_000_000))
        )
        if not NodeIdentity.verify(sign_data, p.signature, p.public_key):
            self._log.warning("HELLO signature fail from %s:%d", *from_addr)
            return

        # Check if this is a new peer BEFORE updating state
        cs = self.peer_book.get_connection_state(p.node_id)
        was_new = cs is None or cs["state"] != "CONNECTED"

        self.peer_book.add_or_update(
            p.node_id, p.public_key, p.public_ip, p.public_port, p.uptime_since
        )
        self.peer_book.set_connection_state(
            p.node_id, "CONNECTED", from_addr[0], from_addr[1]
        )
        self.udp_engine._addr_cache[from_addr] = p.node_id

        if was_new:
            self._log.info("New peer %s from %s:%d", p.node_id[:12], *from_addr)
            self.event_bus.emit("peer_connected", node_id=p.node_id)
            self.scheduler.enqueue_at_front(
                Action.critical(ActionType.SEND_HELLO_REPLY, peer_id=p.node_id)
            )
        self.scheduler.cancel_by_type(
            ActionType.CHECK_PING_RESPONSE, {"peer_id": p.node_id}
        )

    def _on_ping(self, wm, sender_id: str) -> None:
        self.peer_book.record_ping_received(sender_id)
        self.scheduler.cancel_by_type(
            ActionType.CHECK_PING_RESPONSE, {"peer_id": sender_id}
        )

    def _on_peer_list_request(self, sender_id: str) -> None:
        if sender_id:
            self.scheduler.enqueue_at_front(
                Action.critical(
                    ActionType.SEND_HELLO_REPLY, peer_id=sender_id,
                    sub_action="peer_list_response",
                )
            )

    def _on_peer_list_response(self, wm) -> None:
        p = MessageParser.peer_list_response(wm.payload)
        for peer in p.peers:
            self.peer_book.add_or_update(
                peer.node_id, b"", peer.public_ip, peer.public_port, peer.uptime_since
            )
        self.replication.receive_target_estimate(p.estimated_network_target)

    def _on_file_registry_query(self, sender_id: str) -> None:
        if sender_id:
            self.scheduler.enqueue_at_front(
                Action.critical(
                    ActionType.SEND_HELLO_REPLY, peer_id=sender_id,
                    sub_action="file_registry_response",
                )
            )

    def _on_file_registry_response(self, wm) -> None:
        p = MessageParser.file_registry_response(wm.payload)
        self.file_registry.merge_delta(p.entries)
        self.replication.receive_target_estimate(p.estimated_network_target)

    def _on_file_registry_push(self, wm, sender_id: str) -> None:
        p = MessageParser.file_registry_push(wm.payload)
        self.file_registry.update(p.entry)
        from protocol import FileRegistryPushPayload
        push = FileRegistryPushPayload(entry=p.entry)
        self.udp_engine.broadcast_except(
            MsgType.FILE_REGISTRY_PUSH,
            MessageBuilder.file_registry_push(push),
            sender_id or "",
        )

    def _on_file_request(self, wm, sender_id: str) -> None:
        p = MessageParser.file_request(wm.payload)
        if not sender_id:
            return
        if self.storage.has_file(p.file_id):
            self.scheduler.enqueue_at_front(
                Action.critical(
                    ActionType.SEND_CHUNK, file_id=p.file_id, peer_id=sender_id,
                )
            )

    def _on_file_chunk(self, wm, sender_id: str) -> None:
        p = MessageParser.file_chunk(wm.payload)
        transfer_id = f"dl:{p.file_id}:{sender_id}"

        # Store chunk data on disk (stateless — no in-memory buffer)
        self.storage.store_chunk(p.file_id, p.chunk_index, p.data)

        # Update transfer progress in DB
        self.file_registry.update_transfer_progress(
            transfer_id, p.chunk_index, p.total_chunks
        )

        # Send ACK
        if sender_id:
            ack = FileChunkAckPayload(file_id=p.file_id, chunk_index=p.chunk_index)
            self.udp_engine.send_to(
                sender_id, MsgType.FILE_CHUNK_ACK,
                MessageBuilder.file_chunk_ack(ack),
            )

        # Check if download is complete (all chunks received)
        total, _, received = self.file_registry.get_transfer_progress(transfer_id)
        if total > 0 and len(received) >= total:
            # Assemble chunks into final file
            try:
                data = self.storage.assemble_chunks(p.file_id, total)
                # Integrity check
                entry = self.file_registry.get(p.file_id)
                if entry is not None:
                    expected = hashlib.sha256(
                        data + entry.author_id.encode() + str(entry.timestamp).encode()
                    ).hexdigest()
                    if expected == p.file_id:
                        self.storage.store_temporary_replica(p.file_id, data, "")
                        self.file_registry.increment_replica(p.file_id, self.node_identity.node_id)
                        self.file_registry.mark_transfer_complete(transfer_id)
                        self._log.info("Download complete: %s (%d bytes, integrity OK)",
                                        p.file_id[:12], len(data))
                        # Broadcast file announce
                        announce = FileAnnouncePayload(
                            file_id=p.file_id, node_id=self.node_identity.node_id,
                            is_temporary=True, signature=b"",
                        )
                        self.udp_engine.broadcast(
                            MsgType.FILE_ANNOUNCE, MessageBuilder.file_announce(announce)
                        )
                        self.event_bus.emit("download_progress", file_id=p.file_id,
                                            progress=1.0, status="complete")
                    else:
                        self._log.error("Download %s: integrity check failed!", p.file_id[:12])
                        self.file_registry.mark_transfer_failed(transfer_id)
                else:
                    # No registry entry — still store the data
                    self.storage.store_temporary_replica(p.file_id, data, "")
                    self.file_registry.mark_transfer_complete(transfer_id)
                    self._log.info("Download complete: %s (%d bytes, no registry entry)",
                                    p.file_id[:12], len(data))
            except FileNotFoundError:
                self._log.warning("Download %s: chunk assembly failed, missing chunks",
                                  p.file_id[:12])

    def _on_file_chunk_ack(self, wm, sender_id: str) -> None:
        p = MessageParser.file_chunk_ack(wm.payload)
        if sender_id:
            transfer_id = f"ul:{p.file_id}:{sender_id}"
            self.scheduler.cancel_by_type(
                ActionType.CHECK_CHUNK_ACK,
                {"transfer_id": transfer_id, "chunk_index": p.chunk_index},
            )
            self.scheduler.enqueue_at_front(
                Action.critical(
                    ActionType.SEND_CHUNK, file_id=p.file_id, peer_id=sender_id,
                    chunk_index=p.chunk_index + 1,
                )
            )

    def _on_file_announce(self, wm) -> None:
        p = MessageParser.file_announce(wm.payload)
        self.file_registry.increment_replica(p.file_id, p.node_id)

    def _on_replication_solicit(self, wm, sender_id: str) -> None:
        p = MessageParser.replication_solicit(wm.payload)
        if self.replication.consider_solicit(p) and sender_id:
            ack = ReplicationAckPayload(
                file_id=p.file_id, node_id=self.node_identity.node_id
            )
            self.udp_engine.send_to(
                sender_id, MsgType.REPLICATION_ACK,
                MessageBuilder.replication_ack(ack),
            )

    def _on_replication_ack(self, wm) -> None:
        p = MessageParser.replication_ack(wm.payload)
        self.file_registry.increment_replica(p.file_id, p.node_id)

    def _on_file_publish(self, wm, sender_id: str) -> None:
        p = MessageParser.file_publish(wm.payload)
        pb = wire.PayloadBuilder()
        pb.add_string(p.file_id); pb.add_string(p.file_name)
        pb.add_uint64(p.file_size); pb.add_string(p.mime_type)
        pb.add_string(p.author_id); pb.add_uint64(int(p.timestamp * 1_000_000))
        if not AuthorIdentity.verify(pb.build(), p.author_signature, p.author_public_key):
            return
        entry = FileRegistryEntry(
            file_id=p.file_id, file_name=p.file_name, file_size=p.file_size,
            mime_type=p.mime_type, author_id=p.author_id,
            author_public_key=p.author_public_key, replica_count=1,
            author_signature=p.author_signature,
            replicas=[ReplicaEntry(node_id=sender_id, added_at=p.timestamp)],
            timestamp=p.timestamp,
        )
        self.file_registry.add(entry)
        self.event_bus.emit("file_added", file_id=p.file_id)
        if not self.storage.has_file(p.file_id) and self.storage.available_bytes() >= p.file_size:
            self._log.info("Replicating %s from %s", p.file_id[:12], sender_id[:12])
            self.scheduler.enqueue(
                Action.normal(ActionType.SOLICIT_REPLICATION,
                              file_id=p.file_id, from_peer=sender_id)
            )

    def _on_file_update(self, wm, sender_id: str) -> None:
        p = MessageParser.file_update(wm.payload)
        pb = wire.PayloadBuilder()
        pb.add_string(p.file_id); pb.add_string(p.previous_file_id)
        pb.add_string(p.file_name); pb.add_uint64(p.file_size)
        pb.add_string(p.mime_type); pb.add_string(p.author_id)
        pb.add_uint64(int(p.timestamp * 1_000_000))
        if not AuthorIdentity.verify(pb.build(), p.author_signature, p.author_public_key):
            return
        existing = self.file_registry.get(p.previous_file_id)
        if existing and existing.author_id != p.author_id:
            return
        entry = FileRegistryEntry(
            file_id=p.file_id, file_name=p.file_name, file_size=p.file_size,
            mime_type=p.mime_type, author_id=p.author_id,
            author_public_key=p.author_public_key, replica_count=1,
            author_signature=p.author_signature,
            replicas=[ReplicaEntry(node_id=sender_id, added_at=p.timestamp)],
            timestamp=p.timestamp, previous_file_id=p.previous_file_id,
        )
        self.file_registry.add(entry)
        self.event_bus.emit("file_updated", file_id=p.file_id)

    def _on_file_delete(self, wm) -> None:
        p = MessageParser.file_delete(wm.payload)
        pb = wire.PayloadBuilder()
        pb.add_string(p.file_id); pb.add_string(p.author_id)
        pb.add_uint64(int(p.timestamp * 1_000_000))
        if not AuthorIdentity.verify(pb.build(), p.author_signature, p.author_public_key):
            return
        existing = self.file_registry.get(p.file_id)
        if existing and existing.author_id != p.author_id:
            return
        self.file_registry.mark_deleted(p.file_id)
        self.storage.delete_file(p.file_id)
        self.event_bus.emit("file_deleted", file_id=p.file_id)

    def _on_connect_request(self, wm, from_addr: tuple[str, int]) -> None:
        p = MessageParser.connect_request(wm.payload)
        if self.peer_book.is_connected(p.target_node_id):
            intro = ConnectIntroducePayload(
                target_node_id=p.target_node_id,
                introducer_node_id=self.node_identity.node_id,
                requester_node_id=p.requester_node_id,
                requester_ip=p.requester_ip,
                requester_port=p.requester_port,
                is_initiator=True,
            )
            self.scheduler.enqueue_at_front(
                Action.critical(
                    ActionType.SEND_CONNECT_INTRODUCE,
                    target_id=p.target_node_id, payload=intro,
                )
            )

    def _on_connect_introduce(self, wm) -> None:
        p = MessageParser.connect_introduce(wm.payload)
        ack = ConnectAckPayload(peer_node_id=p.requester_node_id)
        self.udp_engine.send_to(
            p.introducer_node_id, MsgType.CONNECT_ACK,
            MessageBuilder.connect_ack(ack),
        )
        self.scheduler.enqueue(
            Action.high(
                ActionType.HOLE_PUNCH_PEER,
                peer_id=p.requester_node_id, ip=p.requester_ip,
                port=p.requester_port, delay=(0 if p.is_initiator else 0.2),
            )
        )

    def _on_connect_ack(self, wm) -> None:
        pass  # Handled by action chain

    def _on_share_file_query(self, wm, sender_id: str) -> None:
        p = MessageParser.share_file_query(wm.payload)
        if not sender_id:
            return
        entry = self.file_registry.get(p.file_id)
        if entry is None:
            return
        host_ids = [r.node_id for r in entry.replicas[:3]]
        resp = ShareFileResponsePayload(
            file_id=p.file_id, file_hash=p.file_id, suggested_peers=host_ids,
        )
        self.udp_engine.send_to(
            sender_id, MsgType.SHARE_FILE_RESPONSE,
            MessageBuilder.share_file_response(resp),
        )

    def _on_share_file_response(self, wm) -> None:
        pass

    def _on_goodbye(self, sender_id: str) -> None:
        self._log.info("Peer disconnected: %s", sender_id[:12])
        self.peer_book.set_connection_state(sender_id, "DISCONNECTED")
        self.file_registry.remove_peer_replicas(sender_id)
        self.reliable.discard_all_for_peer(sender_id)
        self.event_bus.emit("peer_disconnected", node_id=sender_id)

    # ==================================================================
    # Scheduler action handlers
    # ==================================================================

    def _register_scheduler_handlers(self) -> None:
        """Register all action handlers with the scheduler."""
        h = self.scheduler.register
        h(ActionType.SEND_HELLO_REPLY, self._act_send_hello_reply)
        h(ActionType.SEND_CHUNK, self._act_send_chunk)
        h(ActionType.SEND_FILE_CHUNK_ACK, self._act_send_file_chunk_ack)
        h(ActionType.SEND_CONNECT_INTRODUCE, self._act_send_connect_introduce)
        h(ActionType.CHECK_RETRANSMIT, self._act_check_retransmit)
        h(ActionType.CHECK_CHUNK_ACK, self._act_check_chunk_ack)
        h(ActionType.CHECK_PING_RESPONSE, self._act_check_ping_response)
        h(ActionType.CHECK_HOLE_PUNCH, self._act_check_hole_punch)
        h(ActionType.CHECK_DOWNLOAD_COMPLETE, self._act_check_download_complete)
        h(ActionType.REBALANCE, self._act_rebalance)
        h(ActionType.EXCHANGE_REGISTRY, self._act_exchange_registry)
        h(ActionType.REQUEST_PEER_LIST, self._act_request_peer_list)
        h(ActionType.HOLE_PUNCH_PEER, self._act_hole_punch_peer)
        h(ActionType.LAN_BROADCAST, self._act_lan_broadcast)
        h(ActionType.PING_PEER, self._act_ping_peer)
        h(ActionType.CLEANUP_TEMP, self._act_cleanup_temp)
        h(ActionType.GC_OLD_VERSIONS, self._act_gc_old_versions)
        h(ActionType.PEER_CLEANUP, self._act_peer_cleanup)
        h(ActionType.LIVENESS_CHECK, self._act_liveness_check)
        h(ActionType.SOLICIT_REPLICATION, self._act_solicit_replication)

    # ---- Critical action handlers ----

    def _act_send_hello_reply(self, action: Action) -> None:
        peer_id = action.params.get("peer_id", "")
        sub = action.params.get("sub_action", "")
        if sub == "peer_list_response":
            self._send_peer_list_response(peer_id)
        elif sub == "file_registry_response":
            self._send_file_registry_response(peer_id)
        else:
            hello = MessageBuilder.hello(self.udp_engine._build_hello_payload())
            self.udp_engine.send_to(peer_id, MsgType.HELLO, hello)

    def _act_send_chunk(self, action: Action) -> None:
        file_id = action.params.get("file_id", "")
        peer_id = action.params.get("peer_id", "")
        chunk_index = action.params.get("chunk_index", 0)
        if not self.storage.has_file(file_id):
            return
        data = self.storage.read_file(file_id)
        chunks = self.udp_engine.chunk_data(data)
        total = len(chunks)
        if chunk_index >= total:
            return
        chunk = FileChunkPayload(
            file_id=file_id, chunk_index=chunk_index,
            total_chunks=total, data=chunks[chunk_index],
        )
        self.udp_engine.send_to(
            peer_id, MsgType.FILE_CHUNK, MessageBuilder.file_chunk(chunk)
        )
        self.scheduler.enqueue(
            Action.high(
                ActionType.CHECK_CHUNK_ACK,
                delay=self.config.chunk_ack_timeout,
                file_id=file_id, peer_id=peer_id,
                chunk_index=chunk_index,
                transfer_id=f"ul:{file_id}:{peer_id}",
            )
        )

    def _act_send_file_chunk_ack(self, action: Action) -> None:
        pass  # ACKs are sent inline in _on_file_chunk

    def _act_send_connect_introduce(self, action: Action) -> None:
        target_id = action.params.get("target_id", "")
        intro = action.params.get("payload")
        if intro:
            self.udp_engine.send_to(
                target_id, MsgType.CONNECT_INTRODUCE,
                MessageBuilder.connect_introduce(intro),
            )

    # ---- High action handlers ----

    def _act_check_retransmit(self, action: Action) -> None:
        peer_id = action.params.get("peer_id", "")
        seq_num = action.params.get("seq_num", 0)
        msg, new_expiry = self.reliable.mark_retry(peer_id, seq_num)
        if msg is None:
            return
        self.udp_engine.send_to(peer_id, msg.msg_type, msg.payload)
        delay = max(0, new_expiry - time.monotonic())
        self.scheduler.enqueue(
            Action.high(
                ActionType.CHECK_RETRANSMIT, delay=delay,
                peer_id=peer_id, seq_num=seq_num,
            )
        )

    def _act_check_chunk_ack(self, action: Action) -> None:
        file_id = action.params.get("file_id", "")
        peer_id = action.params.get("peer_id", "")
        chunk_index = action.params.get("chunk_index", 0)
        transfer_id = action.params.get("transfer_id", "")
        retries = self.file_registry.increment_chunk_retry(transfer_id, chunk_index)
        if retries >= self.config.max_retries:
            self.file_registry.mark_transfer_failed(transfer_id)
            return
        if self.storage.has_file(file_id):
            data = self.storage.read_file(file_id)
            chunks = self.udp_engine.chunk_data(data)
            if chunk_index < len(chunks):
                chunk = FileChunkPayload(
                    file_id=file_id, chunk_index=chunk_index,
                    total_chunks=len(chunks), data=chunks[chunk_index],
                )
                self.udp_engine.send_to(
                    peer_id, MsgType.FILE_CHUNK, MessageBuilder.file_chunk(chunk)
                )
                self.scheduler.enqueue(
                    Action.high(
                        ActionType.CHECK_CHUNK_ACK,
                        delay=self.config.chunk_ack_timeout * (retries + 1),
                        file_id=file_id, peer_id=peer_id,
                        chunk_index=chunk_index, transfer_id=transfer_id,
                    )
                )

    def _act_check_ping_response(self, action: Action) -> None:
        peer_id = action.params.get("peer_id", "")
        cs = self.peer_book.get_connection_state(peer_id)
        if cs is None:
            return
        if cs["last_seen"] >= cs["last_ping_sent"]:
            return  # Response received
        missed = action.params.get("missed_count", 0) + 1
        if missed >= self.config.max_missed_pings:
            self._log.info("Peer %s timed out after %d missed pings", peer_id[:12], missed)
            self.peer_book.set_connection_state(peer_id, "DISCONNECTED")
            self.file_registry.remove_peer_replicas(peer_id)
            self.reliable.discard_all_for_peer(peer_id)
            self.event_bus.emit("peer_disconnected", node_id=peer_id)

    def _act_check_hole_punch(self, action: Action) -> None:
        peer_id = action.params.get("peer_id", "")
        cs = self.peer_book.get_connection_state(peer_id)
        if cs is None or cs["state"] == "CONNECTED":
            return
        blocked = self.peer_book.increment_hole_punch_attempts(peer_id)
        if blocked:
            self._log.warning("Peer %s direct blocked after %d attempts",
                              peer_id[:12], self.config.max_direct_attempts)

    def _act_check_download_complete(self, action: Action) -> None:
        pass  # Check handled inline in _on_file_chunk

    # ---- Normal action handlers ----

    def _act_rebalance(self, action: Action) -> None:
        self.replication.calculate_network_target()
        self.replication.execute_rebalance()
        self.scheduler.enqueue(
            Action.normal(ActionType.REBALANCE, delay=self.config.rebalance_interval)
        )

    def _act_exchange_registry(self, action: Action) -> None:
        for nid in self.peer_book.get_connected_peers():
            self.udp_engine.send_to(
                nid, MsgType.FILE_REGISTRY_QUERY,
                MessageBuilder.file_registry_query(),
            )

    def _act_request_peer_list(self, action: Action) -> None:
        for nid in self.peer_book.get_connected_peers():
            self.udp_engine.send_to(
                nid, MsgType.PEER_LIST_REQUEST,
                MessageBuilder.peer_list_request(),
            )

    def _act_hole_punch_peer(self, action: Action) -> None:
        peer_id = action.params.get("peer_id", "")
        ip = action.params.get("ip", "")
        port = action.params.get("port", 0)
        delay = action.params.get("delay", 0)
        if delay:
            time.sleep(delay)
        self.peer_book.set_connection_state(peer_id, "PUNCHING", ip, port)
        hello = MessageBuilder.hello(self.udp_engine._build_hello_payload())
        encoded = wire.encode(
            1, MsgType.HELLO, self.node_identity.node_id, 0, hello
        )
        for _ in range(self.config.hole_punch_packets):
            self.udp_engine.send_raw(encoded, (ip, port))
            time.sleep(self.config.hole_punch_interval)
        self.scheduler.enqueue(
            Action.high(
                ActionType.CHECK_HOLE_PUNCH,
                delay=self.config.hole_punch_timeout,
                peer_id=peer_id,
            )
        )

    def _act_lan_broadcast(self, action: Action) -> None:
        connected = len(self.peer_book.get_connected_peers())
        if connected < self.config.lan_broadcast_min_peers:
            self.udp_engine.lan_broadcast()
        self.scheduler.enqueue(
            Action.low(ActionType.LAN_BROADCAST, delay=self.config.lan_broadcast_interval)
        )

    # ---- Low action handlers ----

    def _act_ping_peer(self, action: Action) -> None:
        peer_id = action.params.get("peer_id", "")
        cs = self.peer_book.get_connection_state(peer_id)
        if cs is None or cs["state"] != "CONNECTED":
            return
        ping = MessageBuilder.ping(PingPayload(node_id=self.node_identity.node_id))
        self.udp_engine.send_to(peer_id, MsgType.PING, ping)
        self.peer_book.record_ping_sent(peer_id)
        self.scheduler.enqueue(
            Action.high(
                ActionType.CHECK_PING_RESPONSE,
                delay=self.config.ping_response_timeout,
                peer_id=peer_id, missed_count=action.params.get("missed_count", 0),
            )
        )
        self.scheduler.enqueue(
            Action.low(
                ActionType.PING_PEER, delay=self.config.keepalive_interval,
                peer_id=peer_id,
            )
        )

    def _act_cleanup_temp(self, action: Action) -> None:
        promoted = self.storage.cleanup_expired_temporary()
        if promoted:
            self.replication.execute_rebalance()
        self.scheduler.enqueue(
            Action.low(ActionType.CLEANUP_TEMP, delay=self.config.cleanup_temp_interval)
        )

    def _act_gc_old_versions(self, action: Action) -> None:
        self.file_registry.cleanup_old_versions()
        self.scheduler.enqueue(
            Action.low(ActionType.GC_OLD_VERSIONS, delay=self.config.gc_old_versions_interval)
        )

    def _act_peer_cleanup(self, action: Action) -> None:
        self.peer_book.cleanup(self.config.peer_cleanup_max_age_days)
        self.scheduler.enqueue(
            Action.low(ActionType.PEER_CLEANUP, delay=self.config.peer_cleanup_interval)
        )

    def _act_liveness_check(self, action: Action) -> None:
        for nid in self.peer_book.get_connected_peers():
            if not self.peer_book.is_alive(nid, self.config.peer_timeout):
                self._log.info("Peer %s liveness timeout", nid[:12])
                self.peer_book.set_connection_state(nid, "DISCONNECTED")
                self.file_registry.remove_peer_replicas(nid)
                self.reliable.discard_all_for_peer(nid)
                self.event_bus.emit("peer_disconnected", node_id=nid)
        self.scheduler.enqueue(
            Action.low(ActionType.LIVENESS_CHECK, delay=self.config.keepalive_interval)
        )

    def _act_solicit_replication(self, action: Action) -> None:
        file_id = action.params.get("file_id", "")
        from_peer = action.params.get("from_peer", "")
        if not file_id:
            return
        try:
            data = self.download_file(file_id)
            self.storage.store_replica(file_id, data)
            self.file_registry.increment_replica(file_id, self.node_identity.node_id)
            self._log.info("Replica stored: %s from %s", file_id[:12], from_peer[:12])
            self.event_bus.emit("file_added", file_id=file_id)
        except Exception as e:
            self._log.warning("Replication failed for %s: %s", file_id[:12], e)

    # ---- Helper sends ----

    def _send_peer_list_response(self, peer_id: str) -> None:
        peers = []
        for nid in self.peer_book.get_connected_peers():
            cs = self.peer_book.get_connection_state(nid)
            if cs:
                pb_row = self.peer_book.get(nid)
                uptime = pb_row["uptime_since"] if pb_row else 0
                peers.append(PeerEntry(
                    node_id=nid, public_ip=cs["address_ip"],
                    public_port=cs["address_port"], uptime_since=uptime,
                ))
        target = self.replication.calculate_network_target()
        resp = PeerListResponsePayload(peers=peers, estimated_network_target=target, signature=b"")
        self.udp_engine.send_to(
            peer_id, MsgType.PEER_LIST_RESPONSE,
            MessageBuilder.peer_list_response(resp),
        )

    def _send_file_registry_response(self, peer_id: str) -> None:
        entries = self.file_registry.get_all()
        target = self.replication.calculate_network_target()
        resp = FileRegistryResponsePayload(entries=entries, estimated_network_target=target)
        self.udp_engine.send_to(
            peer_id, MsgType.FILE_REGISTRY_RESPONSE,
            MessageBuilder.file_registry_response(resp),
        )

    # ==================================================================
    # Public API
    # ==================================================================

    def login(self, username: str, password: str) -> AuthorIdentity:
        """Derive author identity and check publishing rules."""
        author = AuthorIdentity.derive(username, password)
        self.author_identity = author
        self.storage.author_id = author.author_id

        # Check remote login policy
        existing = self.file_registry.get_by_author(author.author_id)
        if existing:
            # Author has published files — full access
            self.author_mode = "full"
        else:
            # Check if any peer hosting their files is online
            # For now: if we have space, allow publishing
            if self.storage.available_bytes() >= MIN_PUBLISH_BYTES:
                self.author_mode = "full"
            else:
                self.author_mode = "browse_only"

        return author

    def author_can_publish(self) -> bool:
        """True if author can publish new files."""
        return self.author_mode != "browse_only"

    def publish_file(self, data: bytes, file_name: str, mime_type: str) -> str:
        """Publish a file to the network."""
        if not self.author_identity:
            raise ValueError("Not logged in")
        if not self.author_can_publish():
            raise ValueError("Browse-only mode — cannot publish")

        timestamp = time.time()
        timestamp_us = round(timestamp * 1_000_000) / 1_000_000.0
        file_id = hashlib.sha256(
            data + self.author_identity.author_id.encode() + str(timestamp_us).encode()
        ).hexdigest()

        self.storage.store_own_file(file_id, data, file_name, mime_type)
        self._log.info("Published file: %s (%s, %d bytes) id=%s",
                        file_name, mime_type, len(data), file_id[:12])

        pb = wire.PayloadBuilder()
        pb.add_string(file_id); pb.add_string(file_name)
        pb.add_uint64(len(data)); pb.add_string(mime_type)
        pb.add_string(self.author_identity.author_id)
        pb.add_uint64(int(timestamp_us * 1_000_000))
        signature = self.author_identity.sign(pb.build())

        entry = FileRegistryEntry(
            file_id=file_id, file_name=file_name, file_size=len(data),
            mime_type=mime_type, author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            replica_count=1, author_signature=signature,
            replicas=[ReplicaEntry(node_id=self.node_identity.node_id, added_at=timestamp_us)],
            timestamp=timestamp_us,
        )
        self.file_registry.add(entry)

        payload = FilePublishPayload(
            file_id=file_id, file_name=file_name, file_size=len(data),
            mime_type=mime_type, author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            timestamp=timestamp, author_signature=signature,
        )
        self.udp_engine.broadcast(MsgType.FILE_PUBLISH, MessageBuilder.file_publish(payload))
        self.event_bus.emit("file_added", file_id=file_id)
        return file_id

    def download_file(self, file_id: str) -> bytes:
        """Download a file from the network (blocking, for use by scheduler actions)."""
        entry = self.file_registry.get(file_id)
        if entry is None:
            raise ValueError(f"File {file_id} not in registry")

        # Find a connected peer hosting this file
        host_id = None
        for replica in entry.replicas:
            if self.peer_book.is_connected(replica.node_id):
                host_id = replica.node_id
                break
        if host_id is None:
            raise ValueError(f"No connected peer hosts file {file_id}")

        transfer_id = f"dl:{file_id}:{host_id}"
        self.file_registry.create_transfer(transfer_id, file_id, host_id, "download", 0)

        self.udp_engine.send_to(
            host_id, MsgType.FILE_REQUEST,
            MessageBuilder.file_request(FileRequestPayload(file_id=file_id)),
        )

        # Wait for completion (blocking)
        deadline = time.monotonic() + self.config.download_timeout
        while time.monotonic() < deadline:
            if self.file_registry.is_transfer_complete(transfer_id):
                break
            time.sleep(0.1)

        if not self.file_registry.is_transfer_complete(transfer_id):
            self.file_registry.mark_transfer_failed(transfer_id)
            raise TimeoutError(f"Download of {file_id} incomplete")

        self.file_registry.mark_transfer_complete(transfer_id)
        if not self.storage.has_file(file_id):
            raise FileNotFoundError(f"Downloaded file {file_id} not on disk")
        data = self.storage.read_file(file_id)
        # Integrity check
        expected = hashlib.sha256(
            data + entry.author_id.encode() + str(entry.timestamp).encode()
        ).hexdigest()
        if expected != file_id:
            raise ValueError(f"Integrity check failed for {file_id}")
        return data

    def open_file(self, file_id: str) -> bytes:
        """Open a file (download + track as temporary)."""
        return self.download_file(file_id)

    def update_file(self, file_id: str, new_data: bytes) -> str:
        """Update a file (author only)."""
        if not self.author_identity:
            raise ValueError("Not logged in")
        existing = self.file_registry.get(file_id)
        if existing is None:
            raise ValueError("File not found")
        if existing.author_id != self.author_identity.author_id:
            raise ValueError("Not the author")

        timestamp = time.time()
        timestamp_us = round(timestamp * 1_000_000) / 1_000_000.0
        new_file_id = hashlib.sha256(
            new_data + self.author_identity.author_id.encode() + str(timestamp_us).encode()
        ).hexdigest()

        self.storage.store_own_file(new_file_id, new_data, existing.file_name, existing.mime_type)

        pb = wire.PayloadBuilder()
        pb.add_string(new_file_id); pb.add_string(file_id)
        pb.add_string(existing.file_name); pb.add_uint64(len(new_data))
        pb.add_string(existing.mime_type); pb.add_string(self.author_identity.author_id)
        pb.add_uint64(int(timestamp_us * 1_000_000))
        signature = self.author_identity.sign(pb.build())

        payload = FileUpdatePayload(
            file_id=new_file_id, previous_file_id=file_id,
            file_name=existing.file_name, file_size=len(new_data),
            mime_type=existing.mime_type, author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            timestamp=timestamp_us, author_signature=signature,
        )
        self.udp_engine.broadcast(MsgType.FILE_UPDATE, MessageBuilder.file_update(payload))

        entry = FileRegistryEntry(
            file_id=new_file_id, file_name=existing.file_name, file_size=len(new_data),
            mime_type=existing.mime_type, author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            replica_count=1, author_signature=signature,
            replicas=[ReplicaEntry(node_id=self.node_identity.node_id, added_at=timestamp_us)],
            timestamp=timestamp_us, previous_file_id=file_id,
        )
        self.file_registry.add(entry)
        self.event_bus.emit("file_updated", file_id=new_file_id)
        return new_file_id

    def delete_file(self, file_id: str) -> None:
        """Delete a file (author only)."""
        if not self.author_identity:
            raise ValueError("Not logged in")
        existing = self.file_registry.get(file_id)
        if existing is None:
            return
        if existing.author_id != self.author_identity.author_id:
            raise ValueError("Not the author")

        timestamp = time.time()
        pb = wire.PayloadBuilder()
        pb.add_string(file_id); pb.add_string(self.author_identity.author_id)
        pb.add_uint64(int(timestamp * 1_000_000))
        signature = self.author_identity.sign(pb.build())

        payload = FileDeletePayload(
            file_id=file_id, author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            timestamp=timestamp, author_signature=signature,
        )
        self.udp_engine.broadcast(MsgType.FILE_DELETE, MessageBuilder.file_delete(payload))
        self.file_registry.mark_deleted(file_id)
        self.storage.delete_file(file_id)
        self.event_bus.emit("file_deleted", file_id=file_id)

    def create_share_link(self, file_id: str) -> str:
        """Create a share link for a file."""
        entry = self.file_registry.get(file_id)
        if entry is None:
            raise ValueError("File not found")
        host_ids = [r.node_id for r in entry.replicas[:3]]
        peer_str = ",".join(host_ids[:3])
        return (
            f"http://{self.web_host}:{self.web_port}/?"
            f"file={file_id}&hash={file_id}&peers={peer_str}"
        )

    def connect_to_peer(self, node_id: str, pubkey_b64: str, ip: str, port: int) -> bool:
        """Connect to a peer via direct hole punch (sends hello packets inline,
        then lets the scheduler handle the response and follow-up)."""
        pubkey = public_key_from_base64(pubkey_b64)
        # Add to peer book
        self.peer_book.add_or_update(node_id, pubkey, ip, port, time.time())
        self.peer_book.set_connection_state(node_id, "PUNCHING", ip, port)

        # Send hello packets directly (don't block the scheduler)
        hello = MessageBuilder.hello(self.udp_engine._build_hello_payload())
        encoded = wire.encode(1, MsgType.HELLO, self.node_identity.node_id, 0, hello)
        for _ in range(self.config.hole_punch_packets):
            try:
                self.udp_engine.send_raw(encoded, (ip, port))
            except Exception:
                pass
            time.sleep(self.config.hole_punch_interval)

        # Queue a check for whether the punch succeeded
        self.scheduler.enqueue(
            Action.high(
                ActionType.CHECK_HOLE_PUNCH,
                delay=self.config.hole_punch_timeout,
                peer_id=node_id,
            )
        )
        return True

    def connect_via_url(self, url: str) -> bool:
        """Parse a connection URL and connect."""
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        node_id = params.get("join", [None])[0]
        pk_b64 = params.get("pk", [None])[0]
        addr = params.get("addr", [None])[0]
        if not node_id or not pk_b64 or not addr:
            raise ValueError("Invalid connection URL")
        ip, port_str = addr.rsplit(":", 1)
        return self.connect_to_peer(node_id, pk_b64, ip, int(port_str))

    # ==================================================================
    # Lifecycle
    # ==================================================================

    def start(self) -> None:
        """Start the node."""
        self._log.info("Node ID: %s", self.node_identity.node_id)
        self._log.info("Listening on UDP port %s", self.port)
        print(f"Node ID: {self.node_identity.node_id}")
        print(f"Listening on UDP port {self.port}")

        # Start UDP engine (spawns recv thread)
        self.udp_engine.start()
        self._log.info("Public address: %s:%s",
                        self.udp_engine.public_ip, self.udp_engine.public_port)
        print(f"Public address: {self.udp_engine.public_ip}:{self.udp_engine.public_port}")

        # Seed periodic scheduler actions
        self._seed_periodic_actions()

        # Run reconnection sequence (queues scheduler actions)
        self._reconnect()

        # Launch web server
        if self.web_port > 0:
            self._start_web_server()

        # Start the scheduler in a daemon thread
        scheduler_thread = threading.Thread(target=self.scheduler.run, daemon=True, name="scheduler")
        scheduler_thread.start()

        # Launch TUI or wait
        if not self.no_tui:
            try:
                from tui import TUI
                self.tui = TUI(self)
            except (ImportError, ModuleNotFoundError) as e:
                print(f"TUI not available ({e}). Falling back to headless mode.")
                self.tui = None

            if self.tui:
                print("Starting TUI... (press 'q' to quit)")
                try:
                    self.tui.run()
                finally:
                    self.stop()
            else:
                try:
                    while self.scheduler.running:
                        time.sleep(1)
                except KeyboardInterrupt:
                    pass
                finally:
                    self.stop()
        else:
            try:
                while self.scheduler.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
            finally:
                self.stop()

    # ==================================================================
    # Data-dir locking
    # ==================================================================

    def _acquire_lock(self) -> None:
        """Prevent two instances from sharing the same data directory."""
        lock_path = os.path.join(self.data_dir, ".lock")
        if os.path.exists(lock_path):
            try:
                with open(lock_path) as f:
                    old_pid = int(f.read().strip())
                os.kill(old_pid, 0)  # signal 0 = just check if process exists
                print(f"ERROR: Data dir '{self.data_dir}' is locked by PID {old_pid}")
                print("       Use --data-dir to specify a different directory.")
                sys.exit(1)
            except (ValueError, ProcessLookupError, OSError):
                # PID is stale — overwrite
                pass
        with open(lock_path, "w") as f:
            f.write(str(os.getpid()))

    def _release_lock(self) -> None:
        lock_path = os.path.join(self.data_dir, ".lock")
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
        except OSError:
            pass

    def stop(self) -> None:
        """Graceful shutdown."""
        if self._stopped:
            return
        self._stopped = True
        self._log.info("Shutting down...")
        print("Shutting down...")
        if self.tui:
            self.tui.stop()
        self.scheduler.stop()
        self.udp_engine.stop()
        self._release_lock()
        self._log.info("Goodbye.")
        print("Goodbye.")
        time.sleep(0.1)
        sys.stdout.flush()
        sys.stderr.flush()

    def _reconnect(self) -> None:
        """Queue startup reconnection as scheduler actions."""
        self._log.info("=== Reconnect: queuing startup actions ===")

        peers = self.peer_book.get_all_ordered()
        if not peers:
            self._log.info("Reconnect: no peers in book, adding bootstrap")
            for node_id, pk_b64, ip, port in BOOTSTRAP_PEERS:
                self.peer_book.add_or_update(
                    node_id, public_key_from_base64(pk_b64), ip, port,
                    time.time(), is_bootstrap=True,
                )
            peers = self.peer_book.get_all_ordered()

        self._log.info("Reconnect: %d peers in book", len(peers))

        # Queue hole punches for Tier 1 peers
        for tier in [1, 2, 3]:
            tier_peers = [p for p in peers if p["tier"] == tier]
            limit = MAX_CONCURRENT_HOLE_PUNCH if tier == 1 else 5
            for peer in tier_peers[:limit]:
                self.scheduler.enqueue(
                    Action.normal(
                        ActionType.HOLE_PUNCH_PEER,
                        peer_id=peer["node_id"],
                        ip=peer["public_ip"],
                        port=peer["public_port"],
                        pubkey=bytes(peer["public_key"]),
                    )
                )

        # Queue registry exchange (delayed to give hole punches time)
        self.scheduler.enqueue(
            Action.normal(ActionType.EXCHANGE_REGISTRY, delay=2.0)
        )
        # Queue peer list request
        self.scheduler.enqueue(
            Action.normal(ActionType.REQUEST_PEER_LIST, delay=3.0)
        )
        # Open rebalance gate and queue first rebalance
        self.replication.open_gate()
        self.scheduler.enqueue(
            Action.normal(ActionType.REBALANCE, delay=4.0)
        )
        # LAN broadcast
        if not self.no_lan:
            self.scheduler.enqueue(
                Action.low(ActionType.LAN_BROADCAST, delay=5.0)
            )

        self._log.info("=== Reconnect: actions queued ===")

    def _seed_periodic_actions(self) -> None:
        """Queue initial periodic scheduler actions (replaces old timer threads)."""
        s = self.scheduler

        # Rebalance
        s.enqueue(Action.normal(ActionType.REBALANCE, delay=self.config.rebalance_interval))

        # Cleanup temp files
        s.enqueue(Action.low(ActionType.CLEANUP_TEMP, delay=self.config.cleanup_temp_interval))

        # GC old versions
        s.enqueue(Action.low(ActionType.GC_OLD_VERSIONS, delay=self.config.gc_old_versions_interval))

        # Peer cleanup
        s.enqueue(Action.low(ActionType.PEER_CLEANUP, delay=self.config.peer_cleanup_interval))

        # Liveness check
        s.enqueue(Action.low(ActionType.LIVENESS_CHECK, delay=self.config.keepalive_interval))

        # LAN broadcast (only if enabled)
        if not self.no_lan:
            s.enqueue(Action.low(ActionType.LAN_BROADCAST, delay=self.config.lan_broadcast_interval))

        # Kick off keepalive pings for already-connected peers (none at startup,
        # but PING_PEER actions get seeded when peers connect via _on_hello)

        self._log.info("Periodic actions seeded in scheduler")

    def _start_web_server(self) -> None:
        """Launch Flask web server in a thread."""
        from web import create_app
        from web.ws import init_ws

        self.web_app = create_app(self, self.web_port, self.web_host)
        init_ws(self.web_app)

        def _run():
            self.web_app.run(
                host=self.web_host,
                port=self.web_port,
                debug=False,
                use_reloader=False,
            )

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        print(f"Web UI: http://{self.web_host}:{self.web_port}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Decentralised File Storage Network")
    parser.add_argument(
        "--user", "-u",
        default=os.environ.get("DECWEB_USER"),
        help="Author username",
    )
    parser.add_argument(
        "--pass", "-p",
        dest="password",
        default=os.environ.get("DECWEB_PASS"),
        help="Author password",
    )
    parser.add_argument("--port", "-P", type=int, default=9000, help="UDP listen port")
    parser.add_argument("--no-tui", action="store_true", help="Disable terminal UI")
    parser.add_argument("--web-port", type=int, default=9001, help="Web UI port (0 = disable)")
    parser.add_argument("--web-host", default="127.0.0.1", help="Web UI bind address")
    parser.add_argument(
        "--data-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
        help="Data directory",
    )
    parser.add_argument("--storage-limit", type=int, default=500, help="Max storage in MB")
    parser.add_argument("--no-lan", action="store_true", help="Disable LAN broadcast")
    parser.add_argument(
        "--log", type=str, default=None,
        help="Log filename inside data dir (e.g. app.log); omit to disable file logging",
    )
    parser.add_argument(
        "--udp-trace", type=str, default=None,
        help="UDP hex-dump trace filename inside data dir (e.g. packets.hex)",
    )
    parser.add_argument(
        "--tui-port-offset", type=int, default=0,
        help="Offset added to all ports for multi-instance testing",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Enable scheduler queue debug logging",
    )

    args = parser.parse_args()
    app = App(args)

    # Safety net: ensure lock is always released
    atexit.register(app._release_lock)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _s, _f: app.stop() or sys.exit(0))

    # Auto-login if credentials provided
    if args.user and args.password:
        try:
            app.login(args.user, args.password)
            print(f"Logged in as @{args.user} (author_id: {app.author_identity.author_id})")
        except Exception as e:
            print(f"Login failed: {e}")

    try:
        app.start()
    except KeyboardInterrupt:
        app.stop()


if __name__ == "__main__":
    main()
