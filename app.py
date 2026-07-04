"""
app.py — Main Entry Point

Central orchestrator: parses CLI args, initialises all modules, wires them together,
launches TUI and/or web server.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Optional

from identity import (
    NodeIdentity,
    AuthorIdentity,
    sha256_hex,
    public_key_to_base64,
    public_key_from_base64,
)
from wire import PayloadBuilder
from reliable import ReliabilityManager
from protocol import (
    MsgType,
    MessageBuilder,
    MessageParser,
    ProtocolRouter,
    FilePublishPayload,
    FileUpdatePayload,
    FileDeletePayload,
    FileRegistryEntry,
    HelloPayload,
    PeerListResponsePayload,
    FileRegistryResponsePayload,
    FileAnnouncePayload,
    ReplicationSolicitPayload,
    ConnectIntroducePayload,
    ConnectRequestPayload,
    ShareFileQueryPayload,
    ShareFileResponsePayload,
)
from stun import StunError
from udp_engine import UDPEngine
from peer_book import PeerBook
from file_registry import FileRegistry
from storage import StorageManager
from replication import ReplicationManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BOOTSTRAP_PEERS: list[tuple[str, str, str, int]] = [
    # Format: (node_id, public_key_base64, ip, port)
    # Empty by default — rely on LAN broadcast + QR sharing
]

MIN_PUBLISH_BYTES: int = 1_048_576  # 1MB


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
        # Notify specific subscribers
        for cb in self._subscribers.get(event_type, set()):
            try:
                cb(data)
            except Exception:
                pass
        # Notify wildcard subscribers
        for cb in self._subscribers.get("*", set()):
            try:
                cb(data)
            except Exception:
                pass


# ===================================================================
# App
# ===================================================================


class App:
    """Central orchestrator for the decentralised file storage node."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.data_dir = os.path.expanduser(args.data_dir)
        self.port = args.port + args.tui_port_offset
        self.web_port = args.web_port + args.tui_port_offset if args.web_port > 0 else 0
        self.web_host = args.web_host
        self.no_tui = args.no_tui
        self.no_lan = args.no_lan
        self.storage_limit_mb = args.storage_limit

        # Ensure data directory exists
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)

        # Load/create NodeIdentity
        self.node_identity = NodeIdentity.load_or_create(self.data_dir)

        # Author identity (set during login)
        self.author_identity: Optional[AuthorIdentity] = None
        self.author_mode: str = "browse_only"

        # Event bus
        self.event_bus = EventBus()

        # Initialise modules in dependency order
        self.peer_book = PeerBook(self.data_dir)
        self.file_registry = FileRegistry(self.data_dir, self.node_identity.node_id)
        self.storage = StorageManager(self.data_dir, self.storage_limit_mb)

        # Link storage to file_registry for tier calculations
        self.file_registry.storage = self.storage

        self.reliable = ReliabilityManager()
        self.replication = ReplicationManager(
            self.file_registry, self.storage, self.peer_book, None  # udp_engine set later
        )

        self.protocol_router = ProtocolRouter(
            self.node_identity.node_id,
            None,  # udp_engine set later
            self.peer_book,
            self.file_registry,
            self.storage,
            self.replication,
            self.reliable,
        )

        self.udp_engine = UDPEngine(
            self.port,
            self.node_identity,
            self.protocol_router,
            self.reliable,
            self.peer_book,
            self.file_registry,
            self.storage,
        )

        # Wire cross-references
        self.protocol_router.udp_engine = self.udp_engine
        self.replication.udp_engine = self.udp_engine

        # Register protocol handlers
        self._register_handlers()

        # Web app (created later if needed)
        self.web_app = None

        # TUI
        self.tui = None

    # ==================================================================
    # Protocol handlers
    # ==================================================================

    def _register_handlers(self) -> None:
        r = self.protocol_router

        r.register(MsgType.HELLO, self._handle_hello)
        r.register(MsgType.PING, self._handle_ping)
        r.register(MsgType.PEER_LIST_REQUEST, self._handle_peer_list_request)
        r.register(MsgType.PEER_LIST_RESPONSE, self._handle_peer_list_response)
        r.register(MsgType.FILE_REGISTRY_QUERY, self._handle_file_registry_query)
        r.register(MsgType.FILE_REGISTRY_RESPONSE, self._handle_file_registry_response)
        r.register(MsgType.FILE_REGISTRY_PUSH, self._handle_file_registry_push)
        r.register(MsgType.FILE_REQUEST, self._handle_file_request)
        r.register(MsgType.FILE_CHUNK, self._handle_file_chunk)
        r.register(MsgType.FILE_CHUNK_ACK, self._handle_file_chunk_ack)
        r.register(MsgType.FILE_ANNOUNCE, self._handle_file_announce)
        r.register(MsgType.REPLICATION_SOLICIT, self._handle_replication_solicit)
        r.register(MsgType.REPLICATION_ACK, self._handle_replication_ack)
        r.register(MsgType.FILE_PUBLISH, self._handle_file_publish)
        r.register(MsgType.FILE_UPDATE, self._handle_file_update)
        r.register(MsgType.FILE_DELETE, self._handle_file_delete)
        r.register(MsgType.CONNECT_REQUEST, self._handle_connect_request)
        r.register(MsgType.CONNECT_INTRODUCE, self._handle_connect_introduce)
        r.register(MsgType.CONNECT_ACK, self._handle_connect_ack)
        r.register(MsgType.SHARE_FILE_QUERY, self._handle_share_file_query)
        r.register(MsgType.SHARE_FILE_RESPONSE, self._handle_share_file_response)
        r.register(MsgType.GOODBYE, self._handle_goodbye)

    # ---- Hello / Ping / Goodbye ----

    def _handle_hello(self, wire_msg, p: HelloPayload, from_addr) -> None:
        # Verify signature
        import struct
        sign_data = (
            p.node_id.encode()
            + p.public_ip.encode()
            + struct.pack(">H", p.public_port)
            + struct.pack(">Q", int(p.uptime_since * 1_000_000))
        )
        if not NodeIdentity.verify(sign_data, p.signature, p.public_key):
            return

        self.peer_book.add_or_update(
            p.node_id, p.public_key, p.public_ip, p.public_port, p.uptime_since
        )

        from connection import new_connection, mark_connected

        conn = self.udp_engine.connections.get(p.node_id)
        if conn is None:
            conn = new_connection(p.node_id, p.public_key, from_addr)
            self.udp_engine.connections[p.node_id] = conn

        mark_connected(conn)
        conn.uptime_since = p.uptime_since
        self.udp_engine._addr_to_node_id[from_addr] = p.node_id

        self.event_bus.emit("peer_connected", node_id=p.node_id)

        # Send our hello back
        hello_payload = MessageBuilder.hello(self.udp_engine._build_hello_payload())
        self.udp_engine.send_to(p.node_id, MsgType.HELLO, hello_payload)

    def _handle_ping(self, wire_msg, p, from_addr) -> None:
        sender_id = self.udp_engine.resolve_node_id(from_addr)
        if sender_id:
            self.peer_book.mark_seen(sender_id)
            conn = self.udp_engine.connections.get(sender_id)
            if conn:
                conn.last_seen = time.time()
            # Reply with ping
            from protocol import PingPayload
            self.udp_engine.send_to(
                sender_id,
                MsgType.PING,
                MessageBuilder.ping(PingPayload(node_id=self.node_identity.node_id)),
            )

    def _handle_goodbye(self, wire_msg, p, from_addr) -> None:
        sender_id = self.udp_engine.resolve_node_id(from_addr)
        if sender_id:
            self.peer_book.mark_offline(sender_id)
            self.file_registry.remove_peer_replicas(sender_id)
            conn = self.udp_engine.connections.pop(sender_id, None)
            if conn:
                from connection import mark_disconnected
                mark_disconnected(conn)
            self.event_bus.emit("peer_disconnected", node_id=sender_id)

    # ---- Peer list ----

    def _handle_peer_list_request(self, wire_msg, p, from_addr) -> None:
        from protocol import PeerEntry
        peers = []
        for nid in self.udp_engine.get_connected_peers():
            conn = self.udp_engine.connections.get(nid)
            if conn:
                peers.append(
                    PeerEntry(
                        node_id=nid,
                        public_ip=conn.address[0],
                        public_port=conn.address[1],
                        uptime_since=conn.uptime_since,
                    )
                )
        target = self.replication.calculate_network_target()
        resp = PeerListResponsePayload(
            peers=peers,
            estimated_network_target=target,
            signature=b"",  # TODO: sign
        )
        sender_id = self.udp_engine.resolve_node_id(from_addr)
        if sender_id:
            self.udp_engine.send_to(
                sender_id,
                MsgType.PEER_LIST_RESPONSE,
                MessageBuilder.peer_list_response(resp),
            )

    def _handle_peer_list_response(self, wire_msg, p: PeerListResponsePayload, from_addr) -> None:
        for peer in p.peers:
            self.peer_book.add_or_update(
                peer.node_id, b"", peer.public_ip, peer.public_port, peer.uptime_since
            )
        self.replication.receive_target_estimate(p.estimated_network_target)

    # ---- File registry ----

    def _handle_file_registry_query(self, wire_msg, p, from_addr) -> None:
        entries = self.file_registry.get_all()
        target = self.replication.calculate_network_target()
        resp = FileRegistryResponsePayload(entries=entries, estimated_network_target=target)
        sender_id = self.udp_engine.resolve_node_id(from_addr)
        if sender_id:
            self.udp_engine.send_to(
                sender_id,
                MsgType.FILE_REGISTRY_RESPONSE,
                MessageBuilder.file_registry_response(resp),
            )

    def _handle_file_registry_response(self, wire_msg, p: FileRegistryResponsePayload, from_addr) -> None:
        self.file_registry.merge_delta(p.entries)
        self.replication.receive_target_estimate(p.estimated_network_target)

    def _handle_file_registry_push(self, wire_msg, p, from_addr) -> None:
        self.file_registry.update(p.entry)
        sender_id = self.udp_engine.resolve_node_id(from_addr)
        # Propagate to other peers
        self.udp_engine.broadcast_except(
            MsgType.FILE_REGISTRY_PUSH,
            MessageBuilder.file_registry_push(p),
            sender_id or "",
        )

    # ---- File transfer ----

    def _handle_file_request(self, wire_msg, p, from_addr) -> None:
        sender_id = self.udp_engine.resolve_node_id(from_addr)
        if not sender_id:
            return
        if self.storage.has_file(p.file_id):
            data = self.storage.read_file(p.file_id)
            self.udp_engine.upload_file(sender_id, p.file_id, data)

    def _handle_file_chunk(self, wire_msg, p, from_addr) -> None:
        ds = self.udp_engine.pending_downloads.get(p.file_id)
        if ds is None:
            ds = __import__("udp_engine").DownloadState(p.file_id, "")
            self.udp_engine.pending_downloads[p.file_id] = ds

        ds.total_chunks = p.total_chunks
        ds.received[p.chunk_index] = p.data

        # Send chunk ACK
        sender_id = self.udp_engine.resolve_node_id(from_addr)
        if sender_id:
            from protocol import FileChunkAckPayload
            self.udp_engine.send_to(
                sender_id,
                MsgType.FILE_CHUNK_ACK,
                MessageBuilder.file_chunk_ack(
                    FileChunkAckPayload(file_id=p.file_id, chunk_index=p.chunk_index)
                ),
            )

        # Check completion
        if len(ds.received) == ds.total_chunks:
            chunks = [ds.received[i] for i in range(ds.total_chunks)]
            data = b"".join(chunks)
            # Verify hash
            actual = hashlib.sha256(data).hexdigest()
            if actual == p.file_id:
                self.storage.store_temporary_replica(p.file_id, data, "")
                self.file_registry.increment_replica(p.file_id, self.node_identity.node_id)
                # Broadcast file announce
                from protocol import FileAnnouncePayload
                announce = FileAnnouncePayload(
                    file_id=p.file_id,
                    node_id=self.node_identity.node_id,
                    is_temporary=True,
                    signature=b"",
                )
                self.udp_engine.broadcast(
                    MsgType.FILE_ANNOUNCE,
                    MessageBuilder.file_announce(announce),
                )
            ds.download_complete.set()

        # Progress
        if ds.total_chunks > 0:
            progress = len(ds.received) / ds.total_chunks
            self.event_bus.emit(
                "download_progress",
                file_id=p.file_id,
                progress=progress,
                status="downloading",
            )

    def _handle_file_chunk_ack(self, wire_msg, p, from_addr) -> None:
        sender_id = self.udp_engine.resolve_node_id(from_addr)
        if not sender_id:
            return
        state = self.udp_engine.upload_queue.get((sender_id, p.file_id))
        if state:
            state.current_chunk = p.chunk_index + 1
            if state.current_chunk < state.total_chunks:
                self.udp_engine._send_chunk(state, state.current_chunk)
            else:
                # Upload complete
                self.udp_engine.upload_queue.pop((sender_id, p.file_id), None)

    # ---- File announce / replication ----

    def _handle_file_announce(self, wire_msg, p: FileAnnouncePayload, from_addr) -> None:
        self.file_registry.increment_replica(p.file_id, p.node_id)

    def _handle_replication_solicit(self, wire_msg, p: ReplicationSolicitPayload, from_addr) -> None:
        accepted = self.replication.consider_solicit(p)
        if accepted:
            sender_id = self.udp_engine.resolve_node_id(from_addr)
            if sender_id:
                from protocol import ReplicationAckPayload
                self.udp_engine.send_to(
                    sender_id,
                    MsgType.REPLICATION_ACK,
                    MessageBuilder.replication_ack(
                        ReplicationAckPayload(
                            file_id=p.file_id, node_id=self.node_identity.node_id
                        )
                    ),
                )

    def _handle_replication_ack(self, wire_msg, p, from_addr) -> None:
        self.file_registry.increment_replica(p.file_id, p.node_id)

    # ---- File publish / update / delete ----

    def _handle_file_publish(self, wire_msg, p: FilePublishPayload, from_addr) -> None:
        import struct
        pb = PayloadBuilder()
        pb.add_string(p.file_id)
        pb.add_string(p.file_name)
        pb.add_uint64(p.file_size)
        pb.add_string(p.mime_type)
        pb.add_string(p.author_id)
        pb.add_uint64(int(p.timestamp * 1_000_000))
        if not AuthorIdentity.verify(pb.build(), p.author_signature, p.author_public_key):
            return
        entry = FileRegistryEntry(
            file_id=p.file_id,
            file_name=p.file_name,
            file_size=p.file_size,
            mime_type=p.mime_type,
            author_id=p.author_id,
            author_public_key=p.author_public_key,
            replica_count=1,
            author_signature=p.author_signature,
            replicas=[],
            timestamp=p.timestamp,
        )
        self.file_registry.add(entry)
        self.event_bus.emit("file_added", file_id=p.file_id)

    def _handle_file_update(self, wire_msg, p: FileUpdatePayload, from_addr) -> None:
        import struct
        pb = PayloadBuilder()
        pb.add_string(p.file_id)
        pb.add_string(p.previous_file_id)
        pb.add_string(p.file_name)
        pb.add_uint64(p.file_size)
        pb.add_string(p.mime_type)
        pb.add_string(p.author_id)
        pb.add_uint64(int(p.timestamp * 1_000_000))
        if not AuthorIdentity.verify(pb.build(), p.author_signature, p.author_public_key):
            return
        existing = self.file_registry.get(p.previous_file_id)
        if existing and existing.author_id != p.author_id:
            return  # Not the original author
        entry = FileRegistryEntry(
            file_id=p.file_id,
            file_name=p.file_name,
            file_size=p.file_size,
            mime_type=p.mime_type,
            author_id=p.author_id,
            author_public_key=p.author_public_key,
            replica_count=1,
            author_signature=p.author_signature,
            replicas=[],
            timestamp=p.timestamp,
            previous_file_id=p.previous_file_id,
        )
        self.file_registry.add(entry)
        self.event_bus.emit("file_updated", file_id=p.file_id)

    def _handle_file_delete(self, wire_msg, p: FileDeletePayload, from_addr) -> None:
        import struct
        pb = PayloadBuilder()
        pb.add_string(p.file_id)
        pb.add_string(p.author_id)
        pb.add_uint64(int(p.timestamp * 1_000_000))
        if not AuthorIdentity.verify(pb.build(), p.author_signature, p.author_public_key):
            return
        existing = self.file_registry.get(p.file_id)
        if existing and existing.author_id != p.author_id:
            return
        self.file_registry.mark_deleted(p.file_id)
        self.storage.delete_file(p.file_id)
        self.event_bus.emit("file_deleted", file_id=p.file_id)

    # ---- Connect / hole punch ----

    def _handle_connect_request(self, wire_msg, p: ConnectRequestPayload, from_addr) -> None:
        # If we're connected to target_node_id, relay as CONNECT_INTRODUCE
        target_conn = self.udp_engine.connections.get(p.target_node_id)
        if target_conn and target_conn.is_connected:
            # Determine who should initiate
            # The requester (A) asked us (B) to intro them to C
            # A's NAT might be symmetric, so C should fire first
            intro = ConnectIntroducePayload(
                target_node_id=p.target_node_id,
                introducer_node_id=self.node_identity.node_id,
                requester_node_id=p.requester_node_id,
                requester_ip=p.requester_ip,
                requester_port=p.requester_port,
                is_initiator=True,  # C fires first
            )
            self.udp_engine.send_to(
                p.target_node_id,
                MsgType.CONNECT_INTRODUCE,
                MessageBuilder.connect_introduce(intro),
            )
        else:
            # Relay unavailable — send failure ACK
            ack = __import__("protocol").ConnectAckPayload(peer_node_id="")
            self.udp_engine.send_to(
                p.requester_node_id,
                MsgType.CONNECT_ACK,
                MessageBuilder.connect_ack(ack),
                addr=from_addr,
            )

    def _handle_connect_introduce(self, wire_msg, p: ConnectIntroducePayload, from_addr) -> None:
        # We are the target (C). Start hole punching to requester (A).
        # Send CONNECT_ACK back via introducer
        ack = __import__("protocol").ConnectAckPayload(
            peer_node_id=p.requester_node_id
        )
        self.udp_engine.send_to(
            p.introducer_node_id,
            MsgType.CONNECT_ACK,
            MessageBuilder.connect_ack(ack),
        )

        if p.is_initiator:
            # Fire first — send hello packets
            pb = self.peer_book.get(p.requester_node_id)
            pubkey = pb["public_key"] if pb else b"\x00" * 32
            self.udp_engine.hole_punch(
                p.requester_node_id, p.requester_ip, p.requester_port, pubkey
            )
        else:
            # Wait 200ms then send
            time.sleep(0.2)
            pb = self.peer_book.get(p.requester_node_id)
            pubkey = pb["public_key"] if pb else b"\x00" * 32
            self.udp_engine.hole_punch(
                p.requester_node_id, p.requester_ip, p.requester_port, pubkey
            )

    def _handle_connect_ack(self, wire_msg, p, from_addr) -> None:
        event = self.udp_engine.pending_assisted.get(p.peer_node_id)
        if event:
            event.set()

    # ---- Share ----

    def _handle_share_file_query(self, wire_msg, p, from_addr) -> None:
        sender_id = self.udp_engine.resolve_node_id(from_addr)
        if not sender_id:
            return
        entry = self.file_registry.get(p.file_id)
        if entry is None:
            return
        # Find up to 3 peers with longest uptime hosting this file
        host_ids = [r.node_id for r in entry.replicas[:3]]
        resp = ShareFileResponsePayload(
            file_id=p.file_id,
            file_hash=p.file_id,
            suggested_peers=host_ids,
        )
        self.udp_engine.send_to(
            sender_id,
            MsgType.SHARE_FILE_RESPONSE,
            MessageBuilder.share_file_response(resp),
        )

    def _handle_share_file_response(self, wire_msg, p: ShareFileResponsePayload, from_addr) -> None:
        entry = self.udp_engine.pending_share_responses.get(p.file_id)
        if entry:
            event, _ = entry
            self.udp_engine.pending_share_responses[p.file_id] = (event, p)
            event.set()

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
        file_id = hashlib.sha256(
            data + self.author_identity.author_id.encode() + str(timestamp).encode()
        ).hexdigest()

        # Store locally
        self.storage.store_own_file(file_id, data, file_name, mime_type)

        # Sign with author key
        pb = PayloadBuilder()
        pb.add_string(file_id)
        pb.add_string(file_name)
        pb.add_uint64(len(data))
        pb.add_string(mime_type)
        pb.add_string(self.author_identity.author_id)
        pb.add_uint64(int(timestamp * 1_000_000))
        signature = self.author_identity.sign(pb.build())

        # Build registry entry
        entry = FileRegistryEntry(
            file_id=file_id,
            file_name=file_name,
            file_size=len(data),
            mime_type=mime_type,
            author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            replica_count=1,
            author_signature=signature,
            replicas=[],
            timestamp=timestamp,
        )
        self.file_registry.add(entry)

        # Broadcast
        payload = FilePublishPayload(
            file_id=file_id,
            file_name=file_name,
            file_size=len(data),
            mime_type=mime_type,
            author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            timestamp=timestamp,
            author_signature=signature,
        )
        self.udp_engine.broadcast(
            MsgType.FILE_PUBLISH, MessageBuilder.file_publish(payload)
        )

        self.event_bus.emit("file_added", file_id=file_id)
        return file_id

    def download_file(self, file_id: str) -> bytes:
        """Download a file from the network."""
        return self.udp_engine.download_file(file_id)

    def open_file(self, file_id: str) -> bytes:
        """Open a file (download + track as temporary)."""
        return self.udp_engine.download_file(file_id)

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
        new_file_id = hashlib.sha256(
            new_data + self.author_identity.author_id.encode() + str(timestamp).encode()
        ).hexdigest()

        self.storage.store_own_file(new_file_id, new_data, existing.file_name, existing.mime_type)

        pb = PayloadBuilder()
        pb.add_string(new_file_id)
        pb.add_string(file_id)  # previous_file_id
        pb.add_string(existing.file_name)
        pb.add_uint64(len(new_data))
        pb.add_string(existing.mime_type)
        pb.add_string(self.author_identity.author_id)
        pb.add_uint64(int(timestamp * 1_000_000))
        signature = self.author_identity.sign(pb.build())

        payload = FileUpdatePayload(
            file_id=new_file_id,
            previous_file_id=file_id,
            file_name=existing.file_name,
            file_size=len(new_data),
            mime_type=existing.mime_type,
            author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            timestamp=timestamp,
            author_signature=signature,
        )
        self.udp_engine.broadcast(
            MsgType.FILE_UPDATE, MessageBuilder.file_update(payload)
        )

        entry = FileRegistryEntry(
            file_id=new_file_id,
            file_name=existing.file_name,
            file_size=len(new_data),
            mime_type=existing.mime_type,
            author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            replica_count=1,
            author_signature=signature,
            replicas=[],
            timestamp=timestamp,
            previous_file_id=file_id,
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
        pb = PayloadBuilder()
        pb.add_string(file_id)
        pb.add_string(self.author_identity.author_id)
        pb.add_uint64(int(timestamp * 1_000_000))
        signature = self.author_identity.sign(pb.build())

        payload = FileDeletePayload(
            file_id=file_id,
            author_id=self.author_identity.author_id,
            author_public_key=self.author_identity.public_key_bytes,
            timestamp=timestamp,
            author_signature=signature,
        )
        self.udp_engine.broadcast(
            MsgType.FILE_DELETE, MessageBuilder.file_delete(payload)
        )

        self.file_registry.mark_deleted(file_id)
        self.storage.delete_file(file_id)
        self.event_bus.emit("file_deleted", file_id=file_id)

    def create_share_link(self, file_id: str) -> str:
        """Create a share link for a file."""
        entry = self.file_registry.get(file_id)
        if entry is None:
            raise ValueError("File not found")

        # Query a connected peer for suggested hosts
        connected = self.udp_engine.get_connected_peers()
        if not connected:
            return f"decentralised://{file_id}"

        relay = connected[0]
        event = threading.Event()
        self.udp_engine.pending_share_responses[file_id] = (event, None)

        self.udp_engine.send_to(
            relay,
            MsgType.SHARE_FILE_QUERY,
            MessageBuilder.share_file_query(ShareFileQueryPayload(file_id=file_id)),
        )

        if event.wait(timeout=5.0):
            _, resp = self.udp_engine.pending_share_responses.pop(file_id, (None, None))
            if resp:
                peer_str = ",".join(resp.suggested_peers[:3])
                return (
                    f"http://{self.web_host}:{self.web_port}/?"
                    f"file={file_id}&hash={resp.file_hash}&peers={peer_str}"
                )

        return f"decentralised://{file_id}"

    def connect_to_peer(
        self, node_id: str, pubkey_b64: str, ip: str, port: int
    ) -> bool:
        """Connect to a peer."""
        pubkey = public_key_from_base64(pubkey_b64)
        return self.udp_engine.hole_punch(node_id, ip, port, pubkey)

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
        port = int(port_str)

        return self.connect_to_peer(node_id, pk_b64, ip, port)

    # ==================================================================
    # Lifecycle
    # ==================================================================

    def start(self) -> None:
        """Start the node."""
        print(f"Node ID: {self.node_identity.node_id}")
        print(f"Listening on UDP port {self.port}")

        # Start UDP engine
        self.udp_engine.start()
        print(f"Public address: {self.udp_engine.public_ip}:{self.udp_engine.public_port}")

        # Run reconnection sequence
        self._reconnect()

        # Start periodic tasks
        self._start_periodic_tasks()

        # Launch web server
        if self.web_port > 0:
            self._start_web_server()

        # Launch TUI
        if not self.no_tui:
            from tui import TUI
            self.tui = TUI(self)
            print("Starting TUI... (press 'q' to quit)")
            self.tui.run()
        else:
            # Just wait
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
            finally:
                self.stop()

    def stop(self) -> None:
        """Graceful shutdown."""
        print("Shutting down...")
        if self.tui:
            self.tui.stop()
        self.udp_engine.stop()
        print("Goodbye.")

    def _reconnect(self) -> None:
        """Run the startup reconnection sequence."""
        # Phase 0: Get all known peers
        peers = self.peer_book.get_all_ordered()
        if not peers:
            for node_id, pk_b64, ip, port in BOOTSTRAP_PEERS:
                self.peer_book.add_or_update(
                    node_id,
                    public_key_from_base64(pk_b64),
                    ip,
                    port,
                    time.time(),
                    is_bootstrap=True,
                )
            peers = self.peer_book.get_all_ordered()

        # Phase 1: Connect to Tier 1 peers
        tier1 = [p for p in peers if p["tier"] == 1]
        self.replication.tier1_total = len(tier1)

        for peer in tier1[:MAX_CONCURRENT_HOLE_PUNCH]:
            self.replication.tier1_contacted.add(peer["node_id"])
            try:
                self.udp_engine.hole_punch(
                    peer["node_id"],
                    peer["public_ip"],
                    peer["public_port"],
                    bytes(peer["public_key"]),
                )
            except Exception:
                self.peer_book.record_failure(peer["node_id"])

        # Phase 2: Exchange file registries with connected peers
        time.sleep(1)
        for nid in self.udp_engine.get_connected_peers():
            try:
                self.udp_engine.send_to(
                    nid,
                    MsgType.FILE_REGISTRY_QUERY,
                    MessageBuilder.file_registry_query(),
                )
            except Exception:
                pass

        # Phase 3: Open rebalance gate
        self.replication.open_gate()
        self.replication.execute_rebalance()

        # Phase 4: Try Tier 2, then Tier 3
        for tier in [2, 3]:
            tier_peers = [p for p in peers if p["tier"] == tier]
            for peer in tier_peers[:5]:
                self.replication.tier1_contacted.add(peer["node_id"])
                try:
                    self.udp_engine.hole_punch(
                        peer["node_id"],
                        peer["public_ip"],
                        peer["public_port"],
                        bytes(peer["public_key"]),
                    )
                except Exception:
                    pass

        # Request peer lists from connected peers
        for nid in self.udp_engine.get_connected_peers():
            try:
                self.udp_engine.send_to(
                    nid,
                    MsgType.PEER_LIST_REQUEST,
                    MessageBuilder.peer_list_request(),
                )
            except Exception:
                pass

        # Phase 5: LAN broadcast
        if not self.no_lan:
            self.udp_engine.lan_broadcast()

    def _start_periodic_tasks(self) -> None:
        """Start all background periodic tasks."""

        def _rebalance_loop():
            while self.udp_engine.running:
                time.sleep(60)
                if self.udp_engine.running:
                    try:
                        self.replication.calculate_network_target()
                        self.replication.execute_rebalance()
                    except Exception:
                        pass

        def _cleanup_loop():
            while self.udp_engine.running:
                time.sleep(300)
                if self.udp_engine.running:
                    try:
                        promoted = self.storage.cleanup_expired_temporary()
                        if promoted:
                            self.replication.execute_rebalance()
                    except Exception:
                        pass

        def _gc_loop():
            while self.udp_engine.running:
                time.sleep(1800)
                if self.udp_engine.running:
                    try:
                        self.file_registry.cleanup_old_versions()
                    except Exception:
                        pass

        def _peer_cleanup_loop():
            while self.udp_engine.running:
                time.sleep(3600)
                if self.udp_engine.running:
                    try:
                        self.peer_book.cleanup()
                    except Exception:
                        pass

        def _lan_loop():
            while self.udp_engine.running and not self.no_lan:
                time.sleep(30)
                if self.udp_engine.running:
                    connected = len(self.udp_engine.get_connected_peers())
                    if connected < 2:
                        self.udp_engine.lan_broadcast()

        def _liveness_loop():
            while self.udp_engine.running:
                time.sleep(30)
                if self.udp_engine.running:
                    try:
                        self.udp_engine.check_liveness()
                    except Exception:
                        pass

        for target in [
            _rebalance_loop,
            _cleanup_loop,
            _gc_loop,
            _peer_cleanup_loop,
            _lan_loop,
            _liveness_loop,
        ]:
            t = threading.Thread(target=target, daemon=True)
            t.start()

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
        default=os.path.expanduser("~/.decentralised-web"),
        help="Data directory",
    )
    parser.add_argument("--storage-limit", type=int, default=500, help="Max storage in MB")
    parser.add_argument("--no-lan", action="store_true", help="Disable LAN broadcast")
    parser.add_argument(
        "--tui-port-offset", type=int, default=0,
        help="Offset added to all ports for multi-instance testing",
    )

    args = parser.parse_args()
    app = App(args)

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
