"""
wire.py — Binary Wire Format

Wire Format: [1B version][2B msg_type][8B sender_id_prefix][4B payload_len][4B seq_num][payload]
Total header: 19 bytes.
"""

from __future__ import annotations

import struct
from binascii import unhexlify
from dataclasses import dataclass
from typing import List

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HEADER_FORMAT: str = ">B H 8s I I"
HEADER_SIZE: int = struct.calcsize(HEADER_FORMAT)  # 19
PROTOCOL_VERSION: int = 0x01


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class WireError(Exception):
    """Wire format decode/encode error."""


# ---------------------------------------------------------------------------
# WireMessage
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WireMessage:
    """Decoded wire message."""

    version: int
    msg_type: int
    sender_id_prefix: bytes  # 8 bytes
    payload_len: int
    seq_num: int
    payload: bytes


# ---------------------------------------------------------------------------
# Wire-level encode / decode
# ---------------------------------------------------------------------------


def encode(
    version: int,
    msg_type: int,
    sender_id: str,
    seq_num: int,
    payload: bytes,
) -> bytes:
    """Encode a message into 19-byte header + payload.

    Args:
        version: Protocol version (e.g. 0x01).
        msg_type: Message type ID (uint16).
        sender_id: 16-char hex node_id.
        seq_num: Sequence number (uint32).
        payload: Raw payload bytes.

    Returns:
        Full wire-format bytes.
    """
    sender_prefix = unhexlify(sender_id)  # 16 hex → 8 bytes
    header = struct.pack(HEADER_FORMAT, version, msg_type, sender_prefix, len(payload), seq_num)
    return header + payload


def decode(data: bytes) -> WireMessage:
    """Decode raw bytes into a WireMessage.

    Raises WireError if data < 19 bytes or payload_len > remaining.
    """
    if len(data) < HEADER_SIZE:
        raise WireError(f"Data too short: need {HEADER_SIZE} bytes, got {len(data)}")

    version, msg_type, sender_id_prefix, payload_len, seq_num = struct.unpack(
        HEADER_FORMAT, data[:HEADER_SIZE]
    )

    remaining = len(data) - HEADER_SIZE
    if payload_len > remaining:
        raise WireError(f"payload_len ({payload_len}) > remaining bytes ({remaining})")

    payload = data[HEADER_SIZE : HEADER_SIZE + payload_len]
    return WireMessage(
        version=version,
        msg_type=msg_type,
        sender_id_prefix=sender_id_prefix,
        payload_len=payload_len,
        seq_num=seq_num,
        payload=payload,
    )


# ---------------------------------------------------------------------------
# Payload-level string/int helpers
# ---------------------------------------------------------------------------


def encode_payload_strings(*fields: str) -> bytes:
    """Encode strings as [2B len][UTF-8 bytes] per string."""
    parts: list[bytes] = []
    for s in fields:
        encoded = s.encode("utf-8")
        if len(encoded) > 0xFFFF:
            raise WireError(f"String too long for uint16 prefix: {len(encoded)} bytes")
        parts.append(struct.pack(">H", len(encoded)))
        parts.append(encoded)
    return b"".join(parts)


def decode_payload_strings(data: bytes, count: int) -> List[str]:
    """Decode count length-prefixed strings from bytes."""
    results: list[str] = []
    offset = 0
    for _ in range(count):
        if offset + 2 > len(data):
            raise WireError("Unexpected end of data reading string length")
        strlen = struct.unpack(">H", data[offset : offset + 2])[0]
        offset += 2
        if offset + strlen > len(data):
            raise WireError(f"Unexpected end of data reading string of length {strlen}")
        results.append(data[offset : offset + strlen].decode("utf-8"))
        offset += strlen
    return results


def encode_payload_ints(*fields: int, sizes: list[int]) -> bytes:
    """Encode integers with specified byte sizes."""
    format_map = {1: ">B", 2: ">H", 4: ">I", 8: ">Q"}
    parts: list[bytes] = []
    for value, size in zip(fields, sizes):
        fmt = format_map[size]
        parts.append(struct.pack(fmt, value))
    return b"".join(parts)


def decode_payload_ints(data: bytes, sizes: list[int]) -> List[int]:
    """Decode integers with specified byte sizes."""
    format_map = {1: ">B", 2: ">H", 4: ">I", 8: ">Q"}
    results: list[int] = []
    offset = 0
    for size in sizes:
        fmt = format_map[size]
        results.append(struct.unpack(fmt, data[offset : offset + size])[0])
        offset += size
    return results


# ===================================================================
# PayloadBuilder — stateful payload construction
# ===================================================================


class PayloadBuilder:
    """Build message payloads piece by piece."""

    def __init__(self) -> None:
        self._parts: list[bytes] = []

    def add_string(self, s: str) -> "PayloadBuilder":
        """Append [2B len][UTF-8]."""
        encoded = s.encode("utf-8")
        self._parts.append(struct.pack(">H", len(encoded)))
        self._parts.append(encoded)
        return self

    def add_uint8(self, n: int) -> "PayloadBuilder":
        self._parts.append(struct.pack(">B", n))
        return self

    def add_uint16(self, n: int) -> "PayloadBuilder":
        self._parts.append(struct.pack(">H", n))
        return self

    def add_uint32(self, n: int) -> "PayloadBuilder":
        self._parts.append(struct.pack(">I", n))
        return self

    def add_uint64(self, n: int) -> "PayloadBuilder":
        self._parts.append(struct.pack(">Q", n))
        return self

    def add_bytes(self, b: bytes) -> "PayloadBuilder":
        """Append raw bytes with uint32 length prefix."""
        self._parts.append(struct.pack(">I", len(b)))
        self._parts.append(b)
        return self

    def add_fixed_bytes(self, b: bytes) -> "PayloadBuilder":
        """Append raw bytes without length prefix."""
        self._parts.append(b)
        return self

    def build(self) -> bytes:
        """Return assembled payload bytes."""
        return b"".join(self._parts)


# ===================================================================
# PayloadReader — stateful payload parsing
# ===================================================================


class PayloadReader:
    """Read typed fields from payload bytes."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    def read_string(self) -> str:
        """Read [2B len][UTF-8] → str."""
        strlen = self.read_uint16()
        s = self._data[self._offset : self._offset + strlen].decode("utf-8")
        self._offset += strlen
        return s

    def read_uint8(self) -> int:
        val = struct.unpack(">B", self._data[self._offset : self._offset + 1])[0]
        self._offset += 1
        return val

    def read_uint16(self) -> int:
        val = struct.unpack(">H", self._data[self._offset : self._offset + 2])[0]
        self._offset += 2
        return val

    def read_uint32(self) -> int:
        val = struct.unpack(">I", self._data[self._offset : self._offset + 4])[0]
        self._offset += 4
        return val

    def read_uint64(self) -> int:
        val = struct.unpack(">Q", self._data[self._offset : self._offset + 8])[0]
        self._offset += 8
        return val

    def read_bytes(self) -> bytes:
        """Read [4B len][data] → bytes."""
        length = self.read_uint32()
        b = self._data[self._offset : self._offset + length]
        self._offset += length
        return b

    def read_fixed_bytes(self, n: int) -> bytes:
        """Read exactly n bytes."""
        b = self._data[self._offset : self._offset + n]
        self._offset += n
        return b

    def remaining(self) -> int:
        """Bytes left unread."""
        return len(self._data) - self._offset
