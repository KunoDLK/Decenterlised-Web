"""
stun.py — STUN Client (RFC 5389)

Discover public IP:port via STUN Binding Request.
"""

from __future__ import annotations

import os
import socket
import struct
import time

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STUN_SERVERS: list[tuple[str, int]] = [
    ("stun.l.google.com", 19302),
    ("stun1.l.google.com", 19302),
]
STUN_TIMEOUT: float = 3.0  # seconds per server attempt

# STUN magic cookie (RFC 5389)
MAGIC_COOKIE: int = 0x2112A442

# STUN message types
BINDING_REQUEST: int = 0x0001
BINDING_RESPONSE_SUCCESS: int = 0x0101

# STUN attributes
ATTR_XOR_MAPPED_ADDRESS: int = 0x0020


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class StunError(Exception):
    """STUN query failed (all servers timed out or malformed response)."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_public_address(
    sock: socket.socket, timeout: float = STUN_TIMEOUT
) -> tuple[str, int]:
    """Discover the public (IP, port) of the UDP socket via STUN.

    Args:
        sock: A bound UDP socket (SOCK_DGRAM).
        timeout: Max seconds to wait per STUN server.

    Returns:
        (public_ip: str, public_port: int)

    Raises:
        StunError: If all STUN servers timeout or return malformed responses.
    """
    # Build RFC 5389 Binding Request
    transaction_id = os.urandom(16)
    request = _build_binding_request(transaction_id)

    original_timeout = sock.gettimeout()

    for server_addr in STUN_SERVERS:
        try:
            sock.sendto(request, server_addr)
            sock.settimeout(timeout)

            start = time.monotonic()
            while time.monotonic() - start < timeout:
                try:
                    data, _addr = sock.recvfrom(2048)
                    result = _parse_binding_response(data, transaction_id)
                    if result is not None:
                        return result
                except socket.timeout:
                    break
        except OSError:
            continue
        finally:
            try:
                sock.settimeout(original_timeout)
            except OSError:
                pass

    raise StunError("All STUN servers timed out or returned invalid responses")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_binding_request(transaction_id: bytes) -> bytes:
    """Build STUN Binding Request (20-byte header)."""
    return struct.pack(
        ">H H 4s 8s",
        BINDING_REQUEST,  # message type
        0,  # message length (no attributes)
        struct.pack(">I", MAGIC_COOKIE),  # magic cookie
        transaction_id,  # 12 bytes after cookie = full 16-byte ID
    )


def _parse_binding_response(
    data: bytes, expected_tid: bytes
) -> tuple[str, int] | None:
    """Parse STUN Binding Response, extract XOR-MAPPED-ADDRESS.

    Returns (ip, port) or None if not a valid success response.
    """
    if len(data) < 20:
        return None

    msg_type, msg_len, magic_cookie_bytes, tid_rest = struct.unpack(
        ">H H 4s 8s", data[:16]
    )

    # Verify it's a Binding Success Response
    if msg_type != BINDING_RESPONSE_SUCCESS:
        return None

    # Verify magic cookie
    magic = struct.unpack(">I", magic_cookie_bytes)[0]
    if magic != MAGIC_COOKIE:
        return None

    # Verify transaction ID
    received_tid = magic_cookie_bytes + tid_rest
    if received_tid != expected_tid:
        return None

    # Parse attributes
    offset = 20
    end = 20 + msg_len

    while offset + 4 <= end and offset < len(data):
        attr_type, attr_len = struct.unpack(">H H", data[offset : offset + 4])
        offset += 4

        if offset + attr_len > len(data):
            break

        if attr_type == ATTR_XOR_MAPPED_ADDRESS and attr_len >= 8:
            # Parse XOR-MAPPED-ADDRESS
            # [reserved 1B][family 1B][xor-port 2B][xor-ip 4B]
            family = data[offset + 1]
            if family == 0x01:  # IPv4
                xor_port = struct.unpack(">H", data[offset + 2 : offset + 4])[0]
                xor_ip = struct.unpack(">I", data[offset + 4 : offset + 8])[0]

                # XOR with magic cookie to get real values
                port = xor_port ^ (MAGIC_COOKIE >> 16)
                ip_int = xor_ip ^ MAGIC_COOKIE

                ip = ".".join(
                    str((ip_int >> (24 - 8 * i)) & 0xFF) for i in range(4)
                )
                return (ip, port)

            # IPv6 not supported for now

        offset += attr_len

    return None
