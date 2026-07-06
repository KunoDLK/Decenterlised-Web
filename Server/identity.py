"""
identity.py — Identity & Cryptography

Two-layer identity system:
  1. NodeIdentity  – random Ed25519 keypair (persisted per node).
  2. AuthorIdentity – deterministic Ed25519 keypair from username+password via PBKDF2 (session-only).
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Final

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.exceptions import InvalidSignature

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PBKDF2_SALT: Final[bytes] = b"decentralised-web-v1"
PBKDF2_ITERATIONS: Final[int] = 600_000
PBKDF2_HASH: Final[str] = "sha256"
NODE_IDENTITY_FILE: Final[str] = "node_identity.json"

# ---------------------------------------------------------------------------
# Free functions
# ---------------------------------------------------------------------------


def sha256_hex(data: bytes) -> str:
    """SHA-256 hash as 64-char hex string."""
    digest = hashes.Hash(hashes.SHA256())
    digest.update(data)
    return digest.finalize().hex()


def node_id_from_pubkey(pubkey: bytes) -> str:
    """Derive 16-char hex node_id: SHA-256(pubkey)[:16].hex()."""
    return sha256_hex(pubkey)[:16]


def public_key_to_base64(pubkey: bytes) -> str:
    """Encode 32-byte Ed25519 pubkey to URL-safe base64 (no padding)."""
    return base64.urlsafe_b64encode(pubkey).rstrip(b"=").decode("ascii")


def public_key_from_base64(b64: str) -> bytes:
    """Decode URL-safe base64 to 32-byte Ed25519 pubkey."""
    missing = len(b64) % 4
    if missing:
        b64 += "=" * (4 - missing)
    return base64.urlsafe_b64decode(b64)


# ===================================================================
# NodeIdentity
# ===================================================================


class NodeIdentity:
    """Random Ed25519 keypair — one per node installation.

    Persisted to ``<data_dir>/node_identity.json``.
    """

    __slots__ = ("private_key", "public_key", "node_id")

    def __init__(
        self,
        private_key: Ed25519PrivateKey,
        public_key: Ed25519PublicKey,
        node_id: str,
    ) -> None:
        self.private_key = private_key
        self.public_key = public_key
        self.node_id = node_id

    @classmethod
    def generate(cls) -> "NodeIdentity":
        """Generate fresh random Ed25519 keypair via os.urandom(32)."""
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        node_id = node_id_from_pubkey(
            public_key.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            )
        )
        return cls(private_key, public_key, node_id)

    @classmethod
    def load_or_create(cls, data_dir: str) -> "NodeIdentity":
        """Load from disk or generate & persist a new identity."""
        path = Path(data_dir) / NODE_IDENTITY_FILE
        if path.exists():
            with open(path, "r") as f:
                data = json.load(f)
            private_key = Ed25519PrivateKey.from_private_bytes(
                base64.urlsafe_b64decode(data["private_key"] + "==")
            )
            public_key = private_key.public_key()
            node_id = node_id_from_pubkey(
                public_key.public_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PublicFormat.Raw,
                )
            )
            return cls(private_key, public_key, node_id)
        else:
            identity = cls.generate()
            identity.save(data_dir)
            return identity

    def save(self, data_dir: str) -> None:
        """Persist to node_identity.json."""
        path = Path(data_dir) / NODE_IDENTITY_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        raw_private = self.private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        raw_public = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        with open(path, "w") as f:
            json.dump(
                {
                    "private_key": base64.urlsafe_b64encode(raw_private)
                    .rstrip(b"=")
                    .decode("ascii"),
                    "public_key": base64.urlsafe_b64encode(raw_public)
                    .rstrip(b"=")
                    .decode("ascii"),
                },
                f,
                indent=2,
            )

    def sign(self, message: bytes) -> bytes:
        """Sign message with node's Ed25519 private key. Returns 64-byte signature."""
        return self.private_key.sign(message)

    @staticmethod
    def verify(message: bytes, signature: bytes, public_key: bytes) -> bool:
        """Verify Ed25519 signature against 32-byte public key."""
        try:
            pubkey = Ed25519PublicKey.from_public_bytes(public_key)
            pubkey.verify(signature, message)
            return True
        except InvalidSignature:
            return False

    @property
    def public_key_bytes(self) -> bytes:
        """32-byte raw public key."""
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )


# ===================================================================
# AuthorIdentity
# ===================================================================


class AuthorIdentity:
    """Deterministic Ed25519 keypair derived from username+password.

    Never persisted — derived fresh each session via PBKDF2.
    """

    __slots__ = ("username", "private_key", "public_key", "author_id")

    def __init__(
        self,
        username: str,
        private_key: Ed25519PrivateKey,
        public_key: Ed25519PublicKey,
        author_id: str,
    ) -> None:
        self.username = username
        self.private_key = private_key
        self.public_key = public_key
        self.author_id = author_id

    @classmethod
    def derive(cls, username: str, password: str) -> "AuthorIdentity":
        """Derive identity: PBKDF2(password, PBKDF2_SALT, 600K iters) → 32B seed → Ed25519 key."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=PBKDF2_SALT,
            iterations=PBKDF2_ITERATIONS,
        )
        seed = kdf.derive(password.encode("utf-8"))
        private_key = Ed25519PrivateKey.from_private_bytes(seed)
        public_key = private_key.public_key()
        author_id = node_id_from_pubkey(
            public_key.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            )
        )
        return cls(username, private_key, public_key, author_id)

    def sign(self, payload: bytes) -> bytes:
        """Sign payload with author's Ed25519 private key. Returns 64-byte signature."""
        return self.private_key.sign(payload)

    @staticmethod
    def verify(message: bytes, signature: bytes, public_key: bytes) -> bool:
        """Verify Ed25519 signature against 32-byte public key."""
        try:
            pubkey = Ed25519PublicKey.from_public_bytes(public_key)
            pubkey.verify(signature, message)
            return True
        except InvalidSignature:
            return False

    @property
    def public_key_bytes(self) -> bytes:
        """32-byte raw public key."""
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
