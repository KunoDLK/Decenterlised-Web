"""
udp_engine.py — Thin UDP Socket Layer

Stateless networking: no connection dicts, no timers, no upload/download state.
All state lives in peer_book DB + file_registry transfers table.
All periodic work is driven by the scheduler.

Responsibilities:
  - Bind UDP socket, recv loop
  - Decode wire format
  - Dispatch received packets to a registered callback
  - Encode + send packets (address lookup from peer_book DB)
  - LAN broadcast helper
"""

from __future__ import annotations

import logging
import socket
import struct
import threading
import time
from typing import Callable, Optional, TYPE_CHECKING

import log_utils
import wire
from protocol import MsgType, MessageBuilder, MessageParser, HelloPayload, GoodbyePayload

if TYPE_CHECKING:
    from reliable import ReliabilityManager
    from peer_book import PeerBook
    from scheduler import Scheduler

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_DATAGRAM: int = 65535

# ===================================================================
# Packet handler signature
# ===================================================================

PacketHandler = Callable[
    ["wire.WireMessage", tuple[str, int], "UDPEngine"], None
]


# ===================================================================
# UDPEngine
# ===================================================================


class UDPEngine:
    """Thin UDP networking layer.  Stateless — all state is in the DB."""

    def __init__(
        self,
        port: int,
        node_identity,
        reliable: "ReliabilityManager",
        peer_book: "PeerBook",
        scheduler: "Scheduler",
        max_chunk_size: int = 8192,
    ) -> None:
        self.port = port
        self.node_identity = node_identity
        self.reliable = reliable
        self.peer_book = peer_book
        self.scheduler = scheduler
        self.max_chunk_size = max_chunk_size
        self._log = logging.getLogger("udp")

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 256 * 1024)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256 * 1024)
        self.sock.bind(("0.0.0.0", port))

        self.public_ip: str = "0.0.0.0"
        self.public_port: int = port
        self.uptime_since: float = time.time()

        self.running: bool = False
        self.recv_thread: Optional[threading.Thread] = None

        # Registered callback for incoming packets
        self._on_packet: Optional[PacketHandler] = None

        # addr → node_id cache for fast lookup in recv hot path
        self._addr_cache: dict[tuple[str, int], str] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Bind and start the receive thread."""
        from stun import get_public_address, StunError

        try:
            self.public_ip, self.public_port = get_public_address(self.sock)
        except StunError:
            try:
                self.public_ip = socket.gethostbyname(socket.gethostname())
            except Exception:
                self.public_ip = "127.0.0.1"

        self._log.info(
            "UDP engine on 0.0.0.0:%d (public %s:%d)",
            self.port, self.public_ip, self.public_port,
        )
        self.running = True
        self.recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.recv_thread.start()

    def stop(self) -> None:
        """Graceful shutdown: send GOODBYE to connected peers, close socket."""
        self.running = False
        connected = self.peer_book.get_connected_peers()
        self._log.info("UDP stopping, GOODBYE to %d peers", len(connected))
        goodbye = MessageBuilder.goodbye(
            GoodbyePayload(node_id=self.node_identity.node_id)
        )
        for nid in connected:
            try:
                self.send_to(nid, MsgType.GOODBYE, goodbye)
            except Exception:
                pass
        try:
            self.sock.close()
        except Exception:
            pass
        self._log.info("UDP engine stopped")

    # ------------------------------------------------------------------
    # Packet handler registration
    # ------------------------------------------------------------------

    def set_packet_handler(self, handler: PacketHandler) -> None:
        """Register the callback for incoming decoded packets."""
        self._on_packet = handler

    # ------------------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------------------

    def _recv_loop(self) -> None:
        """Single recv thread. Decodes wire, dispatches to handler, wakes scheduler."""
        self.sock.settimeout(0.5)
        while self.running:
            try:
                data, addr = self.sock.recvfrom(MAX_DATAGRAM)
                log_utils.udp_trace("RECV", data, addr)
                try:
                    wm = wire.decode(data)
                except wire.WireError:
                    self._log.debug("Rcvd %d bytes malformed | %s:%d", len(data), *addr)
                    continue

                # Cache addr → node_id for fast lookup
                prefix = wm.sender_id_prefix.hex()
                if addr not in self._addr_cache:
                    self._addr_cache[addr] = prefix

                self._log.debug(
                    "Rcvd %d bytes | type=%d seq=%d | %s:%d → %s",
                    len(data), wm.msg_type, wm.seq_num,
                    addr[0], addr[1], prefix[:12],
                )

                # Dispatch to registered handler
                if self._on_packet:
                    self._on_packet(wm, addr, self)

                # Signal scheduler to process any queued actions
                self.scheduler.wake()

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
        """Send an encoded message to a peer. Returns seq_num.

        Looks up the peer's address from peer_book DB if not provided.
        """
        seq_num = self.reliable.next_seq(peer_id)
        encoded = wire.encode(
            wire.PROTOCOL_VERSION,
            msg_type,
            self.node_identity.node_id,
            seq_num,
            payload,
        )

        if addr is None:
            cs = self.peer_book.get_connection_state(peer_id)
            if cs is None or not cs["address_ip"]:
                self._log.warning(
                    "send_to: no address for peer %s — dropping type=%d",
                    peer_id[:12], msg_type,
                )
                return seq_num
            addr = (cs["address_ip"], cs["address_port"])

        self.sock.sendto(encoded, addr)
        log_utils.udp_trace("SENT", encoded, addr)

        self._log.debug(
            "Sent %d bytes | type=%d seq=%d | → %s:%d peer=%s",
            len(encoded), msg_type, seq_num, addr[0], addr[1], peer_id[:12],
        )

        # Track for retransmit (event-driven via scheduler)
        expiry = self.reliable.track_sent(peer_id, seq_num, payload, msg_type)
        if expiry is not None:
            from scheduler import Action, ActionType
            self.scheduler.enqueue(
                Action.high(
                    ActionType.CHECK_RETRANSMIT,
                    delay=expiry - time.monotonic(),
                    peer_id=peer_id,
                    seq_num=seq_num,
                )
            )

        return seq_num

    def send_raw(self, data: bytes, addr: tuple[str, int]) -> None:
        """Send raw bytes to an address (used for hole punch hello packets)."""
        self.sock.sendto(data, addr)
        log_utils.udp_trace("SENT", data, addr)

    def broadcast(self, msg_type: int, payload: bytes) -> None:
        """Send to all connected peers (from peer_book DB)."""
        for nid in self.peer_book.get_connected_peers():
            self.send_to(nid, msg_type, payload)

    def broadcast_except(
        self, msg_type: int, payload: bytes, exclude_id: str
    ) -> None:
        """Send to all connected peers except exclude_id."""
        for nid in self.peer_book.get_connected_peers():
            if nid != exclude_id:
                self.send_to(nid, msg_type, payload)

    # ------------------------------------------------------------------
    # Address resolution
    # ------------------------------------------------------------------

    def resolve_node_id(self, addr: tuple[str, int]) -> Optional[str]:
        """Resolve (ip, port) to node_id.

        Checks the in-memory cache first (populated by recv loop),
        then falls back to peer_book DB.
        """
        cached = self._addr_cache.get(addr)
        if cached:
            return cached
        return self.peer_book.resolve_node_id(addr)

    def _build_hello_payload(self) -> HelloPayload:
        """Build a signed HelloPayload for this node."""
        pb = wire.PayloadBuilder()
        pb.add_string(self.node_identity.node_id)
        pb.add_fixed_bytes(self.node_identity.public_key_bytes)
        pb.add_string(self.public_ip)
        pb.add_uint16(self.public_port)
        pb.add_uint64(int(self.uptime_since * 1_000_000))
        sign_data = (
            self.node_identity.node_id.encode()
            + self.public_ip.encode()
            + struct.pack(">H", self.public_port)
            + struct.pack(">Q", int(self.uptime_since * 1_000_000))
        )
        sig = self.node_identity.sign(sign_data)
        pb.add_fixed_bytes(sig)
        return MessageParser.hello(pb.build())

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

    # ------------------------------------------------------------------
    # File helpers (used by scheduler action handlers)
    # ------------------------------------------------------------------

    def chunk_data(self, data: bytes) -> list[bytes]:
        """Split data into max_chunk_size pieces."""
        return [
            data[i : i + self.max_chunk_size]
            for i in range(0, len(data), self.max_chunk_size)
        ]

    def get_connected_peers(self) -> list[str]:
        """List of connected node_ids (from peer_book DB)."""
        return self.peer_book.get_connected_peers()

