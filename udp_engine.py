"""
udp_engine.py — UDP Socket & Hole Punching

Core networking module: UDP socket, send/recv loop, hole punching, keepalive, peer-assisted connect.
"""

from __future__ import annotations

import hashlib
import logging
import socket
import threading
import time
from typing import Optional, TYPE_CHECKING

import wire
from connection import (
    ConnectionState,
    is_alive,
    mark_connected,
    mark_disconnected,
    mark_assisted,
    increment_attempts,
    new_connection,
)
from protocol import (
    MsgType,
    MessageBuilder,
    MessageParser,
    HelloPayload,
    PingPayload,
    GoodbyePayload,
    FileRequestPayload,
    FileChunkPayload,
    FileChunkAckPayload,
    ConnectRequestPayload,
    ConnectAckPayload,
    ShareFileQueryPayload,
    ShareFileResponsePayload,
)
from stun import get_public_address, StunError

if TYPE_CHECKING:
    from protocol import ProtocolRouter
    from reliable import ReliabilityManager
    from peer_book import PeerBook
    from file_registry import FileRegistry
    from storage import StorageManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_CHUNK_SIZE: int = 16384
HOLE_PUNCH_PACKETS: int = 3
HOLE_PUNCH_INTERVAL: float = 0.1
HOLE_PUNCH_TIMEOUT: float = 5.0
KEEPALIVE_INTERVAL: float = 30.0
MAX_CONCURRENT_HOLE_PUNCH: int = 10
LAN_BROADCAST_INTERVAL: float = 30.0


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


class UploadState:
    """Track a chunked upload in progress."""

    __slots__ = (
        "peer_id",
        "file_id",
        "chunks",
        "total_chunks",
        "current_chunk",
        "retries",
        "last_sent",
    )

    def __init__(
        self, peer_id: str, file_id: str, chunks: list[bytes], total_chunks: int
    ) -> None:
        self.peer_id = peer_id
        self.file_id = file_id
        self.chunks = chunks
        self.total_chunks = total_chunks
        self.current_chunk = 0
        self.retries: dict[int, int] = {}
        self.last_sent: dict[int, float] = {}


class DownloadState:
    """Track a chunked download in progress."""

    __slots__ = (
        "file_id",
        "total_chunks",
        "received",
        "peer_id",
        "started_at",
        "download_complete",
    )

    def __init__(self, file_id: str, peer_id: str) -> None:
        self.file_id = file_id
        self.total_chunks = 0
        self.received: dict[int, bytes] = {}
        self.peer_id = peer_id
        self.started_at = time.time()
        self.download_complete = threading.Event()


# ===================================================================
# UDPEngine
# ===================================================================


class UDPEngine:
    """Core UDP networking engine."""

    def __init__(
        self,
        port: int,
        node_identity,
        protocol_router: "ProtocolRouter",
        reliable: "ReliabilityManager",
        peer_book: "PeerBook",
        file_registry: "FileRegistry",
        storage: "StorageManager",
    ) -> None:
        self.port = port
        self.node_identity = node_identity
        self.protocol_router = protocol_router
        self.reliable = reliable
        self.peer_book = peer_book
        self.file_registry = file_registry
        self.storage = storage
        self._log = logging.getLogger("udp")

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", port))

        self.public_ip: str = "0.0.0.0"
        self.public_port: int = port
        self.uptime_since: float = time.time()

        self.connections: dict[str, ConnectionState] = {}
        self._addr_to_node_id: dict[tuple[str, int], str] = {}
        self._lock = threading.RLock()

        self.running = False
        self.recv_thread: Optional[threading.Thread] = None

        self.pending_downloads: dict[str, DownloadState] = {}
        self.pending_assisted: dict[str, threading.Event] = {}
        self.pending_share_responses: dict[
            str, tuple[threading.Event, Optional[ShareFileResponsePayload]]
        ] = {}
        self.upload_queue: dict[tuple[str, str], UploadState] = {}

        self._timer_threads: list[threading.Thread] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the UDP engine."""
        try:
            self.public_ip, self.public_port = get_public_address(self.sock)
        except StunError:
            # Fallback: use local IP
            try:
                self.public_ip = socket.gethostbyname(socket.gethostname())
            except Exception:
                self.public_ip = "127.0.0.1"

        self._log.info("UDP engine started on 0.0.0.0:%d (public %s:%d)",
                        self.port, self.public_ip, self.public_port)
        self.running = True
        self.recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.recv_thread.start()

        self._start_timer("keepalive", KEEPALIVE_INTERVAL, self._keepalive_ping)
        self._start_timer("retransmit", 0.1, self._retransmit_check)

    def stop(self) -> None:
        """Graceful shutdown."""
        self.running = False
        self._log.info("UDP engine stopping, sending GOODBYE to %d peers",
                        sum(1 for c in self.connections.values() if c.is_connected))
        # Send goodbye to all connected peers
        for conn in list(self.connections.values()):
            if conn.is_connected:
                try:
                    self.send_to(
                        conn.node_id,
                        MsgType.GOODBYE,
                        MessageBuilder.goodbye(
                            GoodbyePayload(node_id=self.node_identity.node_id)
                        ),
                    )
                except Exception:
                    pass
        try:
            self.sock.close()
        except Exception:
            pass
        self._log.info("UDP engine stopped")

    # ------------------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------------------

    def _recv_loop(self) -> None:
        """Background recv thread."""
        self.sock.settimeout(0.5)
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
                try:
                    wm = wire.decode(data)
                    self._addr_to_node_id[addr] = wm.sender_id_prefix.hex()
                    self._log.debug(
                        "Rcvd %d bytes | type=%d seq=%d | %s:%d → %s",
                        len(data), wm.msg_type, wm.seq_num,
                        addr[0], addr[1], wm.sender_id_prefix.hex()[:12],
                    )
                    self.protocol_router.route(wm, addr)
                except wire.WireError:
                    self._log.debug("Rcvd %d bytes malformed wire | %s:%d", len(data), *addr)
                    continue
            except socket.timeout:
                continue
            except OSError:
                if not self.running:
                    break

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    def send_to(
        self,
        peer_id: str,
        msg_type: int,
        payload: bytes,
        addr: Optional[tuple[str, int]] = None,
    ) -> int:
        """Send a message to a peer. Returns seq_num used."""
        seq_num = self.reliable.next_seq(peer_id)
        encoded = wire.encode(
            wire.PROTOCOL_VERSION,
            msg_type,
            self.node_identity.node_id,
            seq_num,
            payload,
        )

        if addr is None:
            with self._lock:
                conn = self.connections.get(peer_id)
                if conn is None:
                    return seq_num
                addr = conn.address

        self.sock.sendto(encoded, addr)

        self._log.debug(
            "Sent %d bytes | type=%d seq=%d | → %s:%d peer=%s",
            len(encoded), msg_type, seq_num, addr[0], addr[1], peer_id[:12],
        )

        if self.reliable.needs_ack(msg_type):
            self.reliable.track_pending(
                peer_id, seq_num, payload, msg_type, critical=True
            )

        return seq_num

    def broadcast(self, msg_type: int, payload: bytes) -> None:
        """Send to all connected peers."""
        for conn in list(self.connections.values()):
            if conn.is_connected:
                self.send_to(conn.node_id, msg_type, payload)

    def broadcast_except(
        self, msg_type: int, payload: bytes, exclude_id: str
    ) -> None:
        """Send to all connected except exclude_id."""
        for conn in list(self.connections.values()):
            if conn.is_connected and conn.node_id != exclude_id:
                self.send_to(conn.node_id, msg_type, payload)

    # ------------------------------------------------------------------
    # Hole punching
    # ------------------------------------------------------------------

    def hole_punch(
        self, target_id: str, target_ip: str, target_port: int, target_pubkey: bytes
    ) -> bool:
        """Direct hole punch to a peer. Returns True if connected."""
        addr = (target_ip, target_port)
        conn = new_connection(target_id, target_pubkey, addr)

        with self._lock:
            self.connections[target_id] = conn

        # Build hello payload
        hello_payload = MessageBuilder.hello(self._build_hello_payload())

        # Send 3 hello packets
        for _ in range(HOLE_PUNCH_PACKETS):
            if not self.running:
                return False
            try:
                self.send_to(target_id, MsgType.HELLO, hello_payload, addr=addr)
            except OSError:
                pass
            time.sleep(HOLE_PUNCH_INTERVAL)

        # Wait for hello response
        if conn.hello_received.wait(timeout=HOLE_PUNCH_TIMEOUT):
            return True

        increment_attempts(conn)
        if conn.direct_blocked:
            mark_disconnected(conn)
        return False

    def peer_assisted_connect(self, target_id: str, relay_id: str) -> bool:
        """Connect to target through a relay peer."""
        event = threading.Event()
        self.pending_assisted[target_id] = event

        delays = [1.0, 2.0, 4.0]
        for delay in delays:
            if not self.running:
                break

            # Send CONNECT_REQUEST to relay
            req = ConnectRequestPayload(
                target_node_id=target_id,
                requester_node_id=self.node_identity.node_id,
                requester_ip=self.public_ip,
                requester_port=self.public_port,
            )
            self.send_to(
                relay_id,
                MsgType.CONNECT_REQUEST,
                MessageBuilder.connect_request(req),
            )

            if event.wait(timeout=delay + 5.0):
                # Got CONNECT_ACK — start mutual hole punch
                self.pending_assisted.pop(target_id, None)
                # Send 3 hello packets
                hello_payload = MessageBuilder.hello(self._build_hello_payload())
                for _ in range(HOLE_PUNCH_PACKETS):
                    try:
                        self.send_to(
                            target_id,
                            MsgType.HELLO,
                            hello_payload,
                            addr=self.connections.get(target_id).address
                            if target_id in self.connections
                            else None,
                        )
                    except Exception:
                        pass
                    time.sleep(HOLE_PUNCH_INTERVAL)
                return True

        self.pending_assisted.pop(target_id, None)
        return False

    def _build_hello_payload(self) -> HelloPayload:
        """Build a HelloPayload for this node."""
        import struct

        pb = wire.PayloadBuilder()
        pb.add_string(self.node_identity.node_id)
        pb.add_fixed_bytes(self.node_identity.public_key_bytes)
        pb.add_string(self.public_ip)
        pb.add_uint16(self.public_port)
        pb.add_uint64(int(self.uptime_since * 1_000_000))
        # Sign: node_id + public_ip + port + uptime
        sign_data = (
            self.node_identity.node_id.encode()
            + self.public_ip.encode()
            + struct.pack(">H", self.public_port)
            + struct.pack(">Q", int(self.uptime_since * 1_000_000))
        )
        sig = self.node_identity.sign(sign_data)
        pb.add_fixed_bytes(sig)

        return MessageParser.hello(pb.build())

    # ------------------------------------------------------------------
    # File transfer
    # ------------------------------------------------------------------

    def download_file(self, file_id: str) -> bytes:
        """Download a file from a peer hosting it. Returns file content."""
        # Find a peer hosting this file
        entry = self.file_registry.get(file_id)
        if entry is None:
            raise ValueError(f"File {file_id} not in registry")

        host_id = None
        for replica in entry.replicas:
            if replica.node_id in self.connections:
                host_id = replica.node_id
                break

        if host_id is None:
            raise ValueError(f"No connected peer hosts file {file_id}")

        # Create download state
        ds = DownloadState(file_id, host_id)
        self.pending_downloads[file_id] = ds

        # Send FILE_REQUEST
        self.send_to(
            host_id,
            MsgType.FILE_REQUEST,
            MessageBuilder.file_request(FileRequestPayload(file_id=file_id)),
        )

        # Wait for completion
        ds.download_complete.wait(timeout=60.0)

        if ds.total_chunks == 0 or len(ds.received) != ds.total_chunks:
            self.pending_downloads.pop(file_id, None)
            raise TimeoutError(f"Download of {file_id} incomplete")

        # Reassemble
        chunks = [ds.received[i] for i in range(ds.total_chunks)]
        data = b"".join(chunks)

        # Verify hash
        expected_hash = file_id
        actual_hash = hashlib.sha256(data).hexdigest()
        if actual_hash != expected_hash:
            self.pending_downloads.pop(file_id, None)
            raise ValueError(
                f"Hash mismatch for {file_id}: expected {expected_hash}, got {actual_hash}"
            )

        self.pending_downloads.pop(file_id, None)
        return data

    def upload_file(self, peer_id: str, file_id: str, data: bytes) -> None:
        """Upload a file to a peer in chunks."""
        chunks = [
            data[i : i + MAX_CHUNK_SIZE]
            for i in range(0, len(data), MAX_CHUNK_SIZE)
        ]
        total_chunks = len(chunks)
        state = UploadState(peer_id, file_id, chunks, total_chunks)
        self.upload_queue[(peer_id, file_id)] = state

        # Send first chunk
        self._send_chunk(state, 0)

    def _send_chunk(self, state: UploadState, chunk_index: int) -> None:
        """Send a single chunk."""
        if chunk_index >= state.total_chunks:
            return
        payload = MessageBuilder.file_chunk(
            FileChunkPayload(
                file_id=state.file_id,
                chunk_index=chunk_index,
                total_chunks=state.total_chunks,
                data=state.chunks[chunk_index],
            )
        )
        self.send_to(state.peer_id, MsgType.FILE_CHUNK, payload)
        state.last_sent[chunk_index] = time.time()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def get_connected_peers(self) -> list[str]:
        """List of connected node_ids."""
        with self._lock:
            return [
                nid
                for nid, conn in self.connections.items()
                if conn.is_connected
            ]

    def resolve_node_id(self, addr: tuple[str, int]) -> Optional[str]:
        """Resolve (ip, port) to node_id."""
        return self._addr_to_node_id.get(addr)

    def lan_broadcast(self) -> None:
        """Send hello to 255.255.255.255:port."""
        try:
            hello_payload = MessageBuilder.hello(self._build_hello_payload())
            encoded = wire.encode(
                wire.PROTOCOL_VERSION,
                MsgType.HELLO,
                self.node_identity.node_id,
                0,
                hello_payload,
            )
            self.sock.sendto(encoded, ("255.255.255.255", self.port))
        except Exception:
            pass

    def check_liveness(self) -> None:
        """Check all connections, remove dead ones."""
        dead: list[str] = []
        with self._lock:
            for node_id, conn in list(self.connections.items()):
                if not is_alive(conn):
                    dead.append(node_id)

        for node_id in dead:
            with self._lock:
                conn = self.connections.pop(node_id, None)
                if conn:
                    conn.address = conn.address  # keep for ref
            self.peer_book.mark_offline(node_id)
            self.file_registry.remove_peer_replicas(node_id)

    # ------------------------------------------------------------------
    # Internal: timers
    # ------------------------------------------------------------------

    def _start_timer(
        self, name: str, interval: float, callback
    ) -> None:
        """Start a periodic timer thread."""

        def _loop() -> None:
            while self.running:
                time.sleep(interval)
                if self.running:
                    try:
                        callback()
                    except Exception:
                        pass

        t = threading.Thread(target=_loop, daemon=True, name=f"timer-{name}")
        t.start()
        self._timer_threads.append(t)

    def _keepalive_ping(self) -> None:
        """Send PING to all connected peers."""
        payload = MessageBuilder.ping(
            PingPayload(node_id=self.node_identity.node_id)
        )
        for conn in list(self.connections.values()):
            if conn.is_connected:
                try:
                    self.send_to(conn.node_id, MsgType.PING, payload)
                except Exception:
                    pass

    def _retransmit_check(self) -> None:
        """Check for expired messages and retransmit."""
        expired = self.reliable.get_expired()
        for msg in expired:
            try:
                self.send_to(
                    msg.peer_id, msg.msg_type, msg.payload
                )
            except Exception:
                pass
