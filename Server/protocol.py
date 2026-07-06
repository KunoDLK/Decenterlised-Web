"""
protocol.py — Protocol Message Definitions & Router

Defines all message types, typed payload dataclasses,
MessageBuilder/MessageParser for serialisation, and ProtocolRouter for dispatch.
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TYPE_CHECKING

from wire import PayloadBuilder, PayloadReader

if TYPE_CHECKING:
    from reliable import ReliabilityManager
    from udp_engine import UDPEngine
    from peer_book import PeerBook
    from file_registry import FileRegistry
    from storage import StorageManager
    from replication import ReplicationManager

# ===================================================================
# Message Type IDs
# ===================================================================


class MsgType:
    HELLO = 0x0001
    PING = 0x0002
    ACK = 0x0003
    PEER_LIST_REQUEST = 0x0010
    PEER_LIST_RESPONSE = 0x0011
    FILE_REGISTRY_QUERY = 0x0020
    FILE_REGISTRY_RESPONSE = 0x0021
    FILE_REGISTRY_PUSH = 0x0022
    FILE_REQUEST = 0x0030
    FILE_CHUNK = 0x0031
    FILE_CHUNK_ACK = 0x0032
    FILE_ANNOUNCE = 0x0040
    REPLICATION_SOLICIT = 0x0041
    REPLICATION_ACK = 0x0042
    FILE_PUBLISH = 0x0050
    FILE_UPDATE = 0x0051
    FILE_DELETE = 0x0052
    CONNECT_REQUEST = 0x0060
    CONNECT_INTRODUCE = 0x0061
    CONNECT_ACK = 0x0062
    SHARE_FILE_QUERY = 0x0070
    SHARE_FILE_RESPONSE = 0x0071
    GOODBYE = 0x00FF


# ===================================================================
# Typed Payload Dataclasses
# ===================================================================


@dataclass
class HelloPayload:
    node_id: str  # 16-char hex
    public_key: bytes  # 32 bytes
    public_ip: str
    public_port: int
    uptime_since: float
    signature: bytes  # 64 bytes
    last_registry_update: float = 0.0  # epoch seconds of last registry change
    last_peer_update: float = 0.0      # epoch seconds of last peer list change


@dataclass
class PingPayload:
    node_id: str
    last_registry_update: float = 0.0
    last_peer_update: float = 0.0


@dataclass
class AckPayload:
    acked_msg_type: int
    ack_seq_num: int


@dataclass
class PeerEntry:
    node_id: str
    public_ip: str
    public_port: int
    uptime_since: float


@dataclass
class PeerListRequestPayload:
    pass


@dataclass
class PeerListResponsePayload:
    peers: list[PeerEntry]
    estimated_network_target: int
    signature: bytes  # 64 bytes


@dataclass
class ReplicaEntry:
    node_id: str
    added_at: float


@dataclass
class FileRegistryEntry:
    file_id: str
    file_name: str
    file_size: int
    mime_type: str
    author_id: str
    author_public_key: bytes  # 32 bytes
    replica_count: int
    author_signature: bytes  # 64 bytes
    replicas: list[ReplicaEntry] = field(default_factory=list)
    timestamp: float = 0.0
    previous_file_id: str = ""
    is_deleted: bool = False


@dataclass
class FileRegistryQueryPayload:
    pass


@dataclass
class FileRegistryResponsePayload:
    entries: list[FileRegistryEntry]
    estimated_network_target: int


@dataclass
class FileRegistryPushPayload:
    entry: FileRegistryEntry


@dataclass
class FileRequestPayload:
    file_id: str


@dataclass
class FileChunkPayload:
    file_id: str
    chunk_index: int
    total_chunks: int
    data: bytes


@dataclass
class FileChunkAckPayload:
    file_id: str
    chunk_index: int


@dataclass
class FileAnnouncePayload:
    file_id: str
    node_id: str
    is_temporary: bool = False
    signature: bytes = b""


@dataclass
class ReplicationSolicitPayload:
    file_id: str
    file_name: str
    file_size: int
    author_id: str


@dataclass
class ReplicationAckPayload:
    file_id: str
    node_id: str


@dataclass
class FilePublishPayload:
    file_id: str
    file_name: str
    file_size: int
    mime_type: str
    author_id: str
    author_public_key: bytes
    timestamp: float
    author_signature: bytes


@dataclass
class FileUpdatePayload:
    file_id: str
    previous_file_id: str
    file_name: str
    file_size: int
    mime_type: str
    author_id: str
    author_public_key: bytes
    timestamp: float
    author_signature: bytes


@dataclass
class FileDeletePayload:
    file_id: str
    author_id: str
    author_public_key: bytes
    timestamp: float
    author_signature: bytes


@dataclass
class ConnectRequestPayload:
    target_node_id: str
    requester_node_id: str
    requester_ip: str
    requester_port: int


@dataclass
class ConnectIntroducePayload:
    target_node_id: str
    introducer_node_id: str
    requester_node_id: str
    requester_ip: str
    requester_port: int
    is_initiator: bool


@dataclass
class ConnectAckPayload:
    peer_node_id: str


@dataclass
class GoodbyePayload:
    node_id: str


@dataclass
class ShareFileQueryPayload:
    file_id: str


@dataclass
class ShareFileResponsePayload:
    file_id: str
    file_hash: str
    suggested_peers: list[str]


# ===================================================================
# MessageBuilder — typed payloads → bytes
# ===================================================================


class MessageBuilder:
    """Encode typed payload dataclasses into binary payload bytes."""

    @staticmethod
    def hello(p: HelloPayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.node_id)
            .add_fixed_bytes(p.public_key)
            .add_string(p.public_ip)
            .add_uint16(p.public_port)
            .add_uint64(int(p.uptime_since * 1_000_000))
            .add_fixed_bytes(p.signature)
            .add_uint64(int(p.last_registry_update * 1_000_000))
            .add_uint64(int(p.last_peer_update * 1_000_000))
            .build()
        )

    @staticmethod
    def ping(p: PingPayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.node_id)
            .add_uint64(int(p.last_registry_update * 1_000_000))
            .add_uint64(int(p.last_peer_update * 1_000_000))
            .build()
        )

    @staticmethod
    def ack(p: AckPayload) -> bytes:
        return (
            PayloadBuilder()
            .add_uint16(p.acked_msg_type)
            .add_uint32(p.ack_seq_num)
            .build()
        )

    @staticmethod
    def peer_list_request() -> bytes:
        return b""

    @staticmethod
    def peer_list_response(p: PeerListResponsePayload) -> bytes:
        pb = PayloadBuilder()
        pb.add_uint16(len(p.peers))
        for peer in p.peers:
            pb.add_string(peer.node_id)
            pb.add_string(peer.public_ip)
            pb.add_uint16(peer.public_port)
            pb.add_uint64(int(peer.uptime_since * 1_000_000))
        pb.add_uint32(p.estimated_network_target)
        pb.add_fixed_bytes(p.signature)
        return pb.build()

    @staticmethod
    def file_registry_query() -> bytes:
        return b""

    @staticmethod
    def file_registry_response(p: FileRegistryResponsePayload) -> bytes:
        pb = PayloadBuilder()
        pb.add_uint16(len(p.entries))
        for e in p.entries:
            MessageBuilder._encode_registry_entry(pb, e)
        pb.add_uint32(p.estimated_network_target)
        return pb.build()

    @staticmethod
    def file_registry_push(p: FileRegistryPushPayload) -> bytes:
        pb = PayloadBuilder()
        MessageBuilder._encode_registry_entry(pb, p.entry)
        return pb.build()

    @staticmethod
    def file_request(p: FileRequestPayload) -> bytes:
        return PayloadBuilder().add_string(p.file_id).build()

    @staticmethod
    def file_chunk(p: FileChunkPayload) -> bytes:
        encoded_fid = p.file_id.encode("utf-8")
        return (
            PayloadBuilder()
            .add_uint32(len(encoded_fid))
            .add_fixed_bytes(encoded_fid)
            .add_uint32(p.chunk_index)
            .add_uint32(p.total_chunks)
            .add_bytes(p.data)
            .build()
        )

    @staticmethod
    def file_chunk_ack(p: FileChunkAckPayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.file_id)
            .add_uint32(p.chunk_index)
            .build()
        )

    @staticmethod
    def file_announce(p: FileAnnouncePayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.file_id)
            .add_string(p.node_id)
            .add_uint8(1 if p.is_temporary else 0)
            .add_fixed_bytes(p.signature)
            .build()
        )

    @staticmethod
    def replication_solicit(p: ReplicationSolicitPayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.file_id)
            .add_string(p.file_name)
            .add_uint64(p.file_size)
            .add_string(p.author_id)
            .build()
        )

    @staticmethod
    def replication_ack(p: ReplicationAckPayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.file_id)
            .add_string(p.node_id)
            .build()
        )

    @staticmethod
    def file_publish(p: FilePublishPayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.file_id)
            .add_string(p.file_name)
            .add_uint64(p.file_size)
            .add_string(p.mime_type)
            .add_string(p.author_id)
            .add_fixed_bytes(p.author_public_key)
            .add_uint64(int(p.timestamp * 1_000_000))
            .add_fixed_bytes(p.author_signature)
            .build()
        )

    @staticmethod
    def file_update(p: FileUpdatePayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.file_id)
            .add_string(p.previous_file_id)
            .add_string(p.file_name)
            .add_uint64(p.file_size)
            .add_string(p.mime_type)
            .add_string(p.author_id)
            .add_fixed_bytes(p.author_public_key)
            .add_uint64(int(p.timestamp * 1_000_000))
            .add_fixed_bytes(p.author_signature)
            .build()
        )

    @staticmethod
    def file_delete(p: FileDeletePayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.file_id)
            .add_string(p.author_id)
            .add_fixed_bytes(p.author_public_key)
            .add_uint64(int(p.timestamp * 1_000_000))
            .add_fixed_bytes(p.author_signature)
            .build()
        )

    @staticmethod
    def connect_request(p: ConnectRequestPayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.target_node_id)
            .add_string(p.requester_node_id)
            .add_string(p.requester_ip)
            .add_uint16(p.requester_port)
            .build()
        )

    @staticmethod
    def connect_introduce(p: ConnectIntroducePayload) -> bytes:
        return (
            PayloadBuilder()
            .add_string(p.target_node_id)
            .add_string(p.introducer_node_id)
            .add_string(p.requester_node_id)
            .add_string(p.requester_ip)
            .add_uint16(p.requester_port)
            .add_uint8(1 if p.is_initiator else 0)
            .build()
        )

    @staticmethod
    def connect_ack(p: ConnectAckPayload) -> bytes:
        return PayloadBuilder().add_string(p.peer_node_id).build()

    @staticmethod
    def share_file_query(p: ShareFileQueryPayload) -> bytes:
        return PayloadBuilder().add_string(p.file_id).build()

    @staticmethod
    def share_file_response(p: ShareFileResponsePayload) -> bytes:
        pb = PayloadBuilder()
        pb.add_string(p.file_id)
        pb.add_string(p.file_hash)
        pb.add_uint8(len(p.suggested_peers))
        for peer_id in p.suggested_peers:
            pb.add_string(peer_id)
        return pb.build()

    @staticmethod
    def goodbye(p: GoodbyePayload) -> bytes:
        return PayloadBuilder().add_string(p.node_id).build()

    # Helper
    @staticmethod
    def _encode_registry_entry(pb: PayloadBuilder, e: FileRegistryEntry) -> None:
        pb.add_string(e.file_id)
        pb.add_string(e.file_name)
        pb.add_uint64(e.file_size)
        pb.add_string(e.mime_type)
        pb.add_string(e.author_id)
        pb.add_fixed_bytes(e.author_public_key)
        pb.add_uint32(e.replica_count)
        pb.add_fixed_bytes(e.author_signature)
        pb.add_uint16(len(e.replicas))
        for r in e.replicas:
            pb.add_string(r.node_id)
            pb.add_uint64(int(r.added_at * 1_000_000))
        pb.add_uint64(int(e.timestamp * 1_000_000))
        pb.add_string(e.previous_file_id)
        pb.add_uint8(1 if e.is_deleted else 0)


# ===================================================================
# MessageParser — bytes → typed payloads
# ===================================================================


class MessageParser:
    """Parse binary payload bytes into typed dataclasses."""

    @staticmethod
    def hello(data: bytes) -> HelloPayload:
        r = PayloadReader(data)
        node_id = r.read_string()
        public_key = r.read_fixed_bytes(32)
        public_ip = r.read_string()
        public_port = r.read_uint16()
        uptime_since = r.read_uint64() / 1_000_000.0
        signature = r.read_fixed_bytes(64)
        # V2 extension fields (default to 0 if payload too short)
        last_registry_update = 0.0
        last_peer_update = 0.0
        try:
            last_registry_update = r.read_uint64() / 1_000_000.0
            last_peer_update = r.read_uint64() / 1_000_000.0
        except (IndexError, struct.error):
            pass
        return HelloPayload(
            node_id=node_id,
            public_key=public_key,
            public_ip=public_ip,
            public_port=public_port,
            uptime_since=uptime_since,
            signature=signature,
            last_registry_update=last_registry_update,
            last_peer_update=last_peer_update,
        )

    @staticmethod
    def ping(data: bytes) -> PingPayload:
        r = PayloadReader(data)
        node_id = r.read_string()
        last_registry_update = 0.0
        last_peer_update = 0.0
        try:
            last_registry_update = r.read_uint64() / 1_000_000.0
            last_peer_update = r.read_uint64() / 1_000_000.0
        except (IndexError, struct.error):
            pass
        return PingPayload(
            node_id=node_id,
            last_registry_update=last_registry_update,
            last_peer_update=last_peer_update,
        )

    @staticmethod
    def ack(data: bytes) -> AckPayload:
        r = PayloadReader(data)
        return AckPayload(
            acked_msg_type=r.read_uint16(),
            ack_seq_num=r.read_uint32(),
        )

    @staticmethod
    def peer_list_request(data: bytes) -> PeerListRequestPayload:
        return PeerListRequestPayload()

    @staticmethod
    def peer_list_response(data: bytes) -> PeerListResponsePayload:
        r = PayloadReader(data)
        count = r.read_uint16()
        peers: list[PeerEntry] = []
        for _ in range(count):
            peers.append(
                PeerEntry(
                    node_id=r.read_string(),
                    public_ip=r.read_string(),
                    public_port=r.read_uint16(),
                    uptime_since=r.read_uint64() / 1_000_000.0,
                )
            )
        target = r.read_uint32()
        sig = r.read_fixed_bytes(64) if r.remaining() >= 64 else b""
        return PeerListResponsePayload(
            peers=peers,
            estimated_network_target=target,
            signature=sig,
        )

    @staticmethod
    def file_registry_query(data: bytes) -> FileRegistryQueryPayload:
        return FileRegistryQueryPayload()

    @staticmethod
    def file_registry_response(data: bytes) -> FileRegistryResponsePayload:
        r = PayloadReader(data)
        count = r.read_uint16()
        entries = [MessageParser._decode_registry_entry(r) for _ in range(count)]
        target = r.read_uint32()
        return FileRegistryResponsePayload(
            entries=entries, estimated_network_target=target
        )

    @staticmethod
    def file_registry_push(data: bytes) -> FileRegistryPushPayload:
        r = PayloadReader(data)
        return FileRegistryPushPayload(
            entry=MessageParser._decode_registry_entry(r)
        )

    @staticmethod
    def file_request(data: bytes) -> FileRequestPayload:
        r = PayloadReader(data)
        return FileRequestPayload(file_id=r.read_string())

    @staticmethod
    def file_chunk(data: bytes) -> FileChunkPayload:
        r = PayloadReader(data)
        fid_len = r.read_uint32()
        fid = r.read_fixed_bytes(fid_len).decode("utf-8")
        return FileChunkPayload(
            file_id=fid,
            chunk_index=r.read_uint32(),
            total_chunks=r.read_uint32(),
            data=r.read_bytes(),
        )

    @staticmethod
    def file_chunk_ack(data: bytes) -> FileChunkAckPayload:
        r = PayloadReader(data)
        return FileChunkAckPayload(
            file_id=r.read_string(),
            chunk_index=r.read_uint32(),
        )

    @staticmethod
    def file_announce(data: bytes) -> FileAnnouncePayload:
        r = PayloadReader(data)
        fid = r.read_string()
        nid = r.read_string()
        is_temp = r.read_uint8() == 1
        sig = r.read_fixed_bytes(64) if r.remaining() >= 64 else b""
        return FileAnnouncePayload(
            file_id=fid, node_id=nid, is_temporary=is_temp, signature=sig
        )

    @staticmethod
    def replication_solicit(data: bytes) -> ReplicationSolicitPayload:
        r = PayloadReader(data)
        return ReplicationSolicitPayload(
            file_id=r.read_string(),
            file_name=r.read_string(),
            file_size=r.read_uint64(),
            author_id=r.read_string(),
        )

    @staticmethod
    def replication_ack(data: bytes) -> ReplicationAckPayload:
        r = PayloadReader(data)
        return ReplicationAckPayload(
            file_id=r.read_string(),
            node_id=r.read_string(),
        )

    @staticmethod
    def file_publish(data: bytes) -> FilePublishPayload:
        r = PayloadReader(data)
        return FilePublishPayload(
            file_id=r.read_string(),
            file_name=r.read_string(),
            file_size=r.read_uint64(),
            mime_type=r.read_string(),
            author_id=r.read_string(),
            author_public_key=r.read_fixed_bytes(32),
            timestamp=r.read_uint64() / 1_000_000.0,
            author_signature=r.read_fixed_bytes(64),
        )

    @staticmethod
    def file_update(data: bytes) -> FileUpdatePayload:
        r = PayloadReader(data)
        return FileUpdatePayload(
            file_id=r.read_string(),
            previous_file_id=r.read_string(),
            file_name=r.read_string(),
            file_size=r.read_uint64(),
            mime_type=r.read_string(),
            author_id=r.read_string(),
            author_public_key=r.read_fixed_bytes(32),
            timestamp=r.read_uint64() / 1_000_000.0,
            author_signature=r.read_fixed_bytes(64),
        )

    @staticmethod
    def file_delete(data: bytes) -> FileDeletePayload:
        r = PayloadReader(data)
        return FileDeletePayload(
            file_id=r.read_string(),
            author_id=r.read_string(),
            author_public_key=r.read_fixed_bytes(32),
            timestamp=r.read_uint64() / 1_000_000.0,
            author_signature=r.read_fixed_bytes(64),
        )

    @staticmethod
    def connect_request(data: bytes) -> ConnectRequestPayload:
        r = PayloadReader(data)
        return ConnectRequestPayload(
            target_node_id=r.read_string(),
            requester_node_id=r.read_string(),
            requester_ip=r.read_string(),
            requester_port=r.read_uint16(),
        )

    @staticmethod
    def connect_introduce(data: bytes) -> ConnectIntroducePayload:
        r = PayloadReader(data)
        return ConnectIntroducePayload(
            target_node_id=r.read_string(),
            introducer_node_id=r.read_string(),
            requester_node_id=r.read_string(),
            requester_ip=r.read_string(),
            requester_port=r.read_uint16(),
            is_initiator=r.read_uint8() == 1,
        )

    @staticmethod
    def connect_ack(data: bytes) -> ConnectAckPayload:
        r = PayloadReader(data)
        return ConnectAckPayload(peer_node_id=r.read_string())

    @staticmethod
    def share_file_query(data: bytes) -> ShareFileQueryPayload:
        r = PayloadReader(data)
        return ShareFileQueryPayload(file_id=r.read_string())

    @staticmethod
    def share_file_response(data: bytes) -> ShareFileResponsePayload:
        r = PayloadReader(data)
        fid = r.read_string()
        fhash = r.read_string()
        count = r.read_uint8()
        peers = [r.read_string() for _ in range(count)]
        return ShareFileResponsePayload(
            file_id=fid, file_hash=fhash, suggested_peers=peers
        )

    @staticmethod
    def goodbye(data: bytes) -> GoodbyePayload:
        r = PayloadReader(data)
        return GoodbyePayload(node_id=r.read_string())

    # Helper
    @staticmethod
    def _decode_registry_entry(r: PayloadReader) -> FileRegistryEntry:
        file_id = r.read_string()
        file_name = r.read_string()
        file_size = r.read_uint64()
        mime_type = r.read_string()
        author_id = r.read_string()
        author_public_key = r.read_fixed_bytes(32)
        replica_count = r.read_uint32()
        author_signature = r.read_fixed_bytes(64)
        rep_count = r.read_uint16()
        replicas: list[ReplicaEntry] = []
        for _ in range(rep_count):
            replicas.append(
                ReplicaEntry(
                    node_id=r.read_string(),
                    added_at=r.read_uint64() / 1_000_000.0,
                )
            )
        timestamp = r.read_uint64() / 1_000_000.0
        previous_file_id = r.read_string()
        is_deleted = r.read_uint8() == 1
        return FileRegistryEntry(
            file_id=file_id,
            file_name=file_name,
            file_size=file_size,
            mime_type=mime_type,
            author_id=author_id,
            author_public_key=author_public_key,
            replica_count=replica_count,
            author_signature=author_signature,
            replicas=replicas,
            timestamp=timestamp,
            previous_file_id=previous_file_id,
            is_deleted=is_deleted,
        )


# ===================================================================
# ProtocolRouter — central message dispatcher
# ===================================================================


class ProtocolRouter:
    """Central message dispatcher. Routes incoming WireMessages to handlers."""

    def __init__(
        self,
        node_id: str,
        udp_engine: "UDPEngine",
        peer_book: "PeerBook",
        file_registry: "FileRegistry",
        storage: "StorageManager",
        replication: "ReplicationManager",
        reliable: "ReliabilityManager",
    ) -> None:
        self.node_id = node_id
        self.udp_engine = udp_engine
        self.peer_book = peer_book
        self.file_registry = file_registry
        self.storage = storage
        self.replication = replication
        self.reliable = reliable
        self.handlers: dict[int, Callable] = {}

    def register(self, msg_type: int, handler: Callable) -> None:
        """Register a handler for a message type.

        Handler signature: handler(wire_msg: WireMessage, parsed_payload: Any, from_addr: tuple[str, int])
        """
        self.handlers[msg_type] = handler

    def route(self, wire_msg: "WireMessage", from_addr: tuple[str, int]) -> None:
        """Route an incoming wire message.

        1. ACK message → delegate to reliable.ack_received()
        2. If needs ACK → send ACK via udp_engine
        3. If duplicate → skip
        4. Parse payload → dispatch to registered handler
        """
        from wire import WireMessage

        # Handle ACK messages
        if wire_msg.msg_type == MsgType.ACK:
            # Parse ACK from payload
            ack = MessageParser.ack(wire_msg.payload)
            # We need the sender's node_id - resolve from addr
            sender_id = self.udp_engine.resolve_node_id(from_addr)
            if sender_id:
                self.reliable.ack_received(sender_id, ack.ack_seq_num)
            return

        # Resolve sender
        sender_id = self.udp_engine.resolve_node_id(from_addr)

        # Send ACK if needed
        if sender_id and self.reliable.needs_ack(wire_msg.msg_type):
            ack_payload = self.reliable.build_ack(
                sender_id, wire_msg.msg_type, wire_msg.seq_num
            )
            self.udp_engine.send_to(
                sender_id, MsgType.ACK, ack_payload, addr=from_addr
            )

        # Duplicate check
        if sender_id and self.reliable.is_duplicate(sender_id, wire_msg.seq_num):
            return

        # Parse and dispatch
        handler = self.handlers.get(wire_msg.msg_type)
        if handler is None:
            return  # Unknown message type — silently drop (forward-compatible)

        parsed = self._parse_payload(wire_msg.msg_type, wire_msg.payload)
        handler(wire_msg, parsed, from_addr)

    def _parse_payload(self, msg_type: int, payload: bytes) -> Any:
        """Parse payload bytes into typed dataclass based on message type."""
        parsers: dict[int, Callable] = {
            MsgType.HELLO: MessageParser.hello,
            MsgType.PING: MessageParser.ping,
            MsgType.PEER_LIST_REQUEST: MessageParser.peer_list_request,
            MsgType.PEER_LIST_RESPONSE: MessageParser.peer_list_response,
            MsgType.FILE_REGISTRY_QUERY: MessageParser.file_registry_query,
            MsgType.FILE_REGISTRY_RESPONSE: MessageParser.file_registry_response,
            MsgType.FILE_REGISTRY_PUSH: MessageParser.file_registry_push,
            MsgType.FILE_REQUEST: MessageParser.file_request,
            MsgType.FILE_CHUNK: MessageParser.file_chunk,
            MsgType.FILE_CHUNK_ACK: MessageParser.file_chunk_ack,
            MsgType.FILE_ANNOUNCE: MessageParser.file_announce,
            MsgType.REPLICATION_SOLICIT: MessageParser.replication_solicit,
            MsgType.REPLICATION_ACK: MessageParser.replication_ack,
            MsgType.FILE_PUBLISH: MessageParser.file_publish,
            MsgType.FILE_UPDATE: MessageParser.file_update,
            MsgType.FILE_DELETE: MessageParser.file_delete,
            MsgType.CONNECT_REQUEST: MessageParser.connect_request,
            MsgType.CONNECT_INTRODUCE: MessageParser.connect_introduce,
            MsgType.CONNECT_ACK: MessageParser.connect_ack,
            MsgType.SHARE_FILE_QUERY: MessageParser.share_file_query,
            MsgType.SHARE_FILE_RESPONSE: MessageParser.share_file_response,
            MsgType.GOODBYE: MessageParser.goodbye,
        }
        parser = parsers.get(msg_type)
        if parser:
            return parser(payload)
        return payload  # return raw bytes if unknown
