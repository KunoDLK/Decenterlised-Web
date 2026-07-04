# Decentralised File Storage Network — Detailed Implementation Plan

## Table of Contents
- [Decentralised File Storage Network — Detailed Implementation Plan](#decentralised-file-storage-network--detailed-implementation-plan)
  - [Table of Contents](#table-of-contents)
  - [1. Project Structure](#1-project-structure)
  - [2. Dependency Graph](#2-dependency-graph)
  - [3. Module-by-Module Specification](#3-module-by-module-specification)
    - [3.1 `identity.py` — Identity \& Cryptography](#31-identitypy--identity--cryptography)
      - [Constants](#constants)
      - [Class: `NodeIdentity`](#class-nodeidentity)
      - [Class: `AuthorIdentity`](#class-authoridentity)
      - [Free Functions](#free-functions)
    - [3.2 `wire.py` — Binary Wire Format](#32-wirepy--binary-wire-format)
      - [Wire Format (repeated from spec)](#wire-format-repeated-from-spec)
      - [Class: `WireMessage`](#class-wiremessage)
      - [Free Functions](#free-functions-1)
      - [Helper: `PayloadBuilder` class](#helper-payloadbuilder-class)
      - [Helper: `PayloadReader` class](#helper-payloadreader-class)
    - [3.3 `reliable.py` — Reliability Layer](#33-reliablepy--reliability-layer)
      - [Class: `ReliabilityManager`](#class-reliabilitymanager)
      - [`PendingMessage` dataclass](#pendingmessage-dataclass)
    - [3.4 `protocol.py` — Protocol Message Definitions \& Router](#34-protocolpy--protocol-message-definitions--router)
      - [Constants — Message Type IDs](#constants--message-type-ids)
      - [Dataclasses — Typed Message Payloads](#dataclasses--typed-message-payloads)
      - [Class: `MessageBuilder`](#class-messagebuilder)
      - [Class: `MessageParser`](#class-messageparser)
      - [Class: `ProtocolRouter`](#class-protocolrouter)
    - [3.5 `stun.py` — STUN Client](#35-stunpy--stun-client)
      - [Constants](#constants-1)
      - [Free Function](#free-function)
    - [3.6 `udp_engine.py` — UDP Socket \& Hole Punching](#36-udp_enginepy--udp-socket--hole-punching)
      - [Class: `UDPEngine`](#class-udpengine)
      - [`ConnectionState` dataclass](#connectionstate-dataclass)
      - [`UploadState` dataclass](#uploadstate-dataclass)
      - [`DownloadState` dataclass](#downloadstate-dataclass)
    - [3.7 `connection.py` — Per-Peer Connection State](#37-connectionpy--per-peer-connection-state)
      - [Free Functions](#free-functions-2)
    - [3.8 `peer_book.py` — SQLite Peer Directory](#38-peer_bookpy--sqlite-peer-directory)
      - [Database Schema](#database-schema)
      - [Class: `PeerBook`](#class-peerbook)
    - [3.9 `file_registry.py` — Decentralised File Registry](#39-file_registrypy--decentralised-file-registry)
      - [Database Schema](#database-schema-1)
      - [Class: `FileRegistry`](#class-fileregistry)
    - [3.10 `storage.py` — Disk Storage \& Quota Manager](#310-storagepy--disk-storage--quota-manager)
      - [Class: `StorageManager`](#class-storagemanager)
      - [Metadata file](#metadata-file)
    - [3.11 `replication.py` — Rebalancing \& Diversity Logic](#311-replicationpy--rebalancing--diversity-logic)
      - [Class: `ReplicationManager`](#class-replicationmanager)
    - [3.12 `tui.py` — Terminal UI](#312-tuipy--terminal-ui)
      - [Class: `TUI`](#class-tui)
      - [TUI Input Key Bindings](#tui-input-key-bindings)
    - [3.13 `web/__init__.py` — Flask App Factory](#313-web__init__py--flask-app-factory)
      - [Function: `create_app(node: App, web_port: int, web_host: str)`](#function-create_appnode-app-web_port-int-web_host-str)
    - [3.14 `web/routes.py` — HTTP Routes](#314-webroutespy--http-routes)
      - [Blueprint: `main`](#blueprint-main)
      - [Auth decorator](#auth-decorator)
    - [3.15 `web/ws.py` — WebSocket Handler](#315-webwspy--websocket-handler)
      - [WebSocket endpoint: `/ws`](#websocket-endpoint-ws)
      - [Implementation notes:](#implementation-notes)
    - [3.16 `web/templates/index.html` — Main UI](#316-webtemplatesindexhtml--main-ui)
    - [3.17 `web/templates/login.html` — Login Page](#317-webtemplatesloginhtml--login-page)
    - [3.18 `web/static/style.css` — Dark Mode Styles](#318-webstaticstylecss--dark-mode-styles)
    - [3.19 `web/static/app.js` — Frontend Logic](#319-webstaticappjs--frontend-logic)
      - [Global State](#global-state)
      - [Functions](#functions)
      - [Event Listeners](#event-listeners)
      - [WebSocket Reconnection](#websocket-reconnection)
    - [3.20 `app.py` — Main Entry Point](#320-apppy--main-entry-point)
      - [Class: `App`](#class-app)
      - [CLI Argument Parsing (argparse)](#cli-argument-parsing-argparse)
      - [`EventBus` class (simple pub/sub)](#eventbus-class-simple-pubsub)
      - [`main()` function](#main-function)
  - [4. Data Flow Diagrams](#4-data-flow-diagrams)
    - [4.1 Message Reception Flow](#41-message-reception-flow)
    - [4.2 File Download Flow](#42-file-download-flow)
    - [4.3 File Publish Flow](#43-file-publish-flow)
    - [4.4 Reconnection Sequence Flow](#44-reconnection-sequence-flow)
  - [5. Startup \& Reconnection Sequence](#5-startup--reconnection-sequence)
  - [6. File Lifecycle](#6-file-lifecycle)
    - [State Machine](#state-machine)
    - [Replica States (Local Storage)](#replica-states-local-storage)
  - [7. Error Handling Strategy](#7-error-handling-strategy)
  - [Appendix A: Bootstrap Peer Configuration](#appendix-a-bootstrap-peer-configuration)
  - [Appendix B: Constants Summary](#appendix-b-constants-summary)
  - [Appendix C: Connection URL Format](#appendix-c-connection-url-format)
  - [Appendix D: Threading Model](#appendix-d-threading-model)

---

## 1. Project Structure

```
Decenterlised-Web/
├── app.py                    # Main entry point
├── identity.py               # Node + Author identity, Ed25519, PBKDF2
├── wire.py                   # Binary wire format encode/decode
├── reliable.py               # Sequence numbers, ACKs, retransmit
├── protocol.py               # Message type constants, payload builders, router
├── stun.py                   # STUN client (RFC 5389)
├── udp_engine.py             # UDP socket, hole punching, send/recv loop
├── connection.py             # Per-peer connection state machine
├── peer_book.py              # SQLite peer directory + tiering
├── file_registry.py          # SQLite file registry + gossip sync
├── storage.py                # Disk file storage + quota tracking
├── replication.py            # Rebalancing & diversity logic
├── tui.py                    # Rich-based terminal UI
├── web/
│   ├── __init__.py           # Flask app factory
│   ├── routes.py             # HTTP routes (login, file serve, QR)
│   ├── ws.py                 # WebSocket handler (auth-gated)
│   ├── templates/
│   │   ├── index.html        # Main UI (requires auth)
│   │   └── login.html        # Login form
│   └── static/
│       ├── style.css         # Dark mode CSS
│       ├── app.js            # Frontend JS: auth, WS, DOM
│       ├── qrcode.min.js     # QR code generation library
│       └── jsqr.min.js       # QR code scanning library
└── requirements.txt          # flask, flask-sock, cryptography, rich
```

**Data directory** (`~/.decentralised-web/`):
```
~/.decentralised-web/
├── config.json               # User settings (network name, storage quota, etc.)
├── node_identity.json        # Random Ed25519 keypair (Node ID)
├── peers.db                  # SQLite peer book
├── registry.db               # SQLite file registry
└── files/
    ├── <fileId1>             # Stored file (flat binary)
    ├── <fileId2>
    └── .metadata.json        # Own/replica/temporary file tracking
```

---

## 2. Dependency Graph

```
                    ┌──────────┐
                    │  app.py  │  (main, ties everything together)
                    └────┬─────┘
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
    ┌─────────┐    ┌──────────┐    ┌─────────┐
    │  tui.py │    │ udp_engine│    │  web/   │
    └────┬────┘    └────┬─────┘    └────┬────┘
         │              │               │
         └──────┬───────┘───────────────┘
                │
    ┌───────────┼───────────┬──────────┬──────────┐
    ▼           ▼           ▼          ▼          ▼
┌────────┐ ┌────────┐ ┌─────────┐ ┌────────┐ ┌───────────┐
│identity│ │protocol│ │connection│ │peer_book│ │file_registry│
└───┬────┘ └───┬────┘ └────┬────┘ └───┬────┘ └─────┬─────┘
    │          │           │          │             │
    └──────────┼───────────┴──────────┴─────────────┘
               │
       ┌───────┼───────┬──────────┐
       ▼       ▼       ▼          ▼
   ┌──────┐┌──────┐┌────────┐┌───────────┐
   │wire  ││reliable││stun   ││replication│
   │.py   ││.py    ││.py    ││.py        │
   └──────┘└──────┘└────────┘└─────┬─────┘
                                    │
                               ┌────┴────┐
                               │storage  │
                               │.py      │
                               └─────────┘
```

---

## 3. Module-by-Module Specification

---

### 3.1 `identity.py` — Identity & Cryptography

**Purpose**: Two-layer identity: random node keypair (persisted) + deterministic author keypair (PBKDF2-derived, session-only).

#### Constants

```python
PBKDF2_SALT = b"decentralised-web-v1"
PBKDF2_ITERATIONS = 600_000
PBKDF2_HASH = "sha256"
NODE_IDENTITY_FILE = "node_identity.json"  # relative to data_dir
```

#### Class: `NodeIdentity`

Represents the random keypair generated once per node installation.

| Field | Type | Description |
|---|---|---|
| `private_key` | `Ed25519PrivateKey` | 32-byte Ed25519 private key |
| `public_key` | `Ed25519PublicKey` | 32-byte Ed25519 public key |
| `node_id` | `str` (16 chars) | First 16 hex chars of SHA-256(public_key_bytes) |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `generate()` | `() -> NodeIdentity` | — | New `NodeIdentity` | Generate fresh random Ed25519 keypair via `os.urandom(32)` |
| `load_or_create(data_dir: str)` | `(str) -> NodeIdentity` | `data_dir`: path to `~/.decentralised-web` | `NodeIdentity` | If `node_identity.json` exists, load & deserialise. Else `generate()`, persist as JSON, return. |
| `save(data_dir: str)` | `(str) -> None` | `data_dir` | — | Write `{ "private_key": base64, "public_key": base64 }` to `node_identity.json` |
| `sign(message: bytes)` | `(bytes) -> bytes` | `message`: raw bytes to sign | 64-byte Ed25519 signature | Sign with node's private key |
| `verify(message: bytes, signature: bytes, public_key: bytes)` | `(bytes, bytes, bytes) -> bool` | `message`, `signature`, `public_key` (32 bytes) | `True` if valid | Static method. Verify Ed25519 signature. |

**Persistence format** (`node_identity.json`):
```json
{
  "private_key": "<base64-encoded 32 bytes>",
  "public_key": "<base64-encoded 32 bytes>"
}
```

#### Class: `AuthorIdentity`

Derived from username + password. Never persisted — derived fresh each session.

| Field | Type | Description |
|---|---|---|
| `username` | `str` | The username |
| `private_key` | `Ed25519PrivateKey` | 32-byte Ed25519 private key |
| `public_key` | `Ed25519PublicKey` | 32-byte Ed25519 public key |
| `author_id` | `str` (16 chars) | First 16 hex chars of SHA-256(public_key_bytes) |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `derive(username: str, password: str)` | `(str, str) -> AuthorIdentity` | `username`: plaintext username; `password`: plaintext password | `AuthorIdentity` | 1. PBKDF2-HMAC-SHA256(password, salt=PBKDF2_SALT, iterations=600K) → 32-byte seed. 2. Use seed as Ed25519 private key bytes. 3. Derive public key. 4. Compute `author_id = SHA-256(pubkey)[:16].hex()`. |
| `sign(payload: bytes)` | `(bytes) -> bytes` | `payload`: raw bytes | 64-byte Ed25519 signature | Sign with author's private key |
| `verify(message: bytes, signature: bytes, public_key: bytes)` | `(bytes, bytes, bytes) -> bool` | `message`, `signature`, `public_key` | `True` if valid | Static method. Same as NodeIdentity.verify |

#### Free Functions

| Function | Signature | Input | Output | Description |
|---|---|---|---|---|
| `sha256_hex(data: bytes)` | `(bytes) -> str` | Raw bytes | 64-char hex string | SHA-256 hash |
| `node_id_from_pubkey(pubkey: bytes)` | `(bytes) -> str` | 32-byte Ed25519 public key | 16-char hex string | First 16 chars of SHA-256(pubkey) |
| `public_key_to_base64(pubkey: bytes)` | `(bytes) -> str` | 32-byte Ed25519 public key | Base64 string (no padding) | Encode for URLs / QR codes |
| `public_key_from_base64(b64: str)` | `(str) -> bytes` | Base64 string | 32-byte Ed25519 public key | Decode |

---

### 3.2 `wire.py` — Binary Wire Format

**Purpose**: Encode/decode all protocol messages into the compact binary format. Every message on the wire uses this format.

#### Wire Format (repeated from spec)

```
[1B protocol version] [2B message type] [8B sender nodeId prefix]
[4B payload length] [4B sequence number]
[payload bytes]
```

Total header: 19 bytes.

#### Class: `WireMessage`

| Field | Type | Bytes | Description |
|---|---|---|---|
| `version` | `int` (uint8) | 1 | Currently `0x01` |
| `msg_type` | `int` (uint16) | 2 | Message type ID (e.g. `0x01` for `hello`) |
| `sender_id_prefix` | `bytes` | 8 | First 8 bytes of sender's node_id (as raw bytes, not hex) |
| `payload_len` | `int` (uint32) | 4 | Length of payload in bytes |
| `seq_num` | `int` (uint32) | 4 | Monotonically increasing per peer-pair |
| `payload` | `bytes` | variable | Message-specific binary payload |

#### Free Functions

| Function | Signature | Input | Output | Description |
|---|---|---|---|---|
| `encode(version: int, msg_type: int, sender_id: str, seq_num: int, payload: bytes)` | `(int, int, str, int, bytes) -> bytes` | `version` (1), `msg_type` (e.g. 0x01), `sender_id` (16-char hex), `seq_num`, `payload` | Raw wire bytes (19 + len(payload)) | Pack with `struct.pack('>B H 8s I I', ...)`. `sender_id` hex → unhexlify to 8 bytes. |
| `decode(data: bytes)` | `(bytes) -> WireMessage` | Raw bytes (min 19) | `WireMessage` namedtuple | Unpack with `struct.unpack('>B H 8s I I', data[:19])`. Raise `WireError` if len < 19 or payload_len > remaining bytes. |
| `encode_payload_strings(*fields: str)` | `(str...) -> bytes` | Variable number of strings | Length-prefixed concatenation: `[2B len][utf8 bytes]` per string | Used for fields like nodeId, fileId, fileName |
| `decode_payload_strings(data: bytes, count: int)` | `(bytes, int) -> list[str]` | Raw payload bytes, expected number of strings | List of decoded UTF-8 strings | Read `[2B len][utf8 bytes]` `count` times |
| `encode_payload_ints(*fields: int, sizes: list[int])` | `(int..., list[int]) -> bytes` | Integers + their byte sizes (1,2,4,8) | Packed bytes | `struct.pack` with appropriate format chars |
| `decode_payload_ints(data: bytes, sizes: list[int])` | `(bytes, list[int]) -> list[int]` | Raw bytes, list of byte-sizes | List of decoded ints | `struct.unpack` |

#### Helper: `PayloadBuilder` class

Stateful builder for constructing message payloads piece by piece.

| Method | Description |
|---|---|
| `add_string(s: str)` | Append `[2B len][utf8]` |
| `add_uint8(n: int)` | Append 1 byte |
| `add_uint16(n: int)` | Append 2 bytes (big-endian) |
| `add_uint32(n: int)` | Append 4 bytes (big-endian) |
| `add_uint64(n: int)` | Append 8 bytes (big-endian) |
| `add_bytes(b: bytes)` | Append raw bytes (length-prefixed with uint32) |
| `add_fixed_bytes(b: bytes)` | Append raw bytes (no length prefix) |
| `build()` | `-> bytes` — return assembled payload |

#### Helper: `PayloadReader` class

Stateful reader for parsing payloads.

| Method | Description |
|---|---|
| `read_string()` | `-> str` — read `[2B len][utf8]` |
| `read_uint8()` | `-> int` |
| `read_uint16()` | `-> int` |
| `read_uint32()` | `-> int` |
| `read_uint64()` | `-> int` |
| `read_bytes()` | `-> bytes` — read `[4B len][data]` |
| `read_fixed_bytes(n: int)` | `-> bytes` — read exactly n bytes |
| `remaining()` | `-> int` — bytes left unread |

---

### 3.3 `reliable.py` — Reliability Layer

**Purpose**: Sequence number tracking, ACK generation, retransmit timers. Sits between `protocol.py` and `udp_engine.py`.

#### Class: `ReliabilityManager`

Per-`udp_engine` singleton. Manages sequence numbers per peer-pair and pending ACKs.

| Field | Type | Description |
|---|---|---|
| `pending_acks` | `dict[(str, int), PendingMessage]` | Key: `(peer_node_id, seq_num)`. Messages awaiting ACK. |
| `peer_seq_out` | `dict[str, int]` | Next outgoing seq_num per peer. |
| `peer_seq_in` | `dict[str, int]` | Last received seq_num per peer (for duplicate detection). |
| `received_seqs` | `dict[str, set[int]]` | Received sequence numbers per peer (sliding window of last 256). |
| `on_retry_failed` | `Callable` | Callback when max retries exhausted for a message. |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `next_seq(peer_id: str)` | `(str) -> int` | `peer_id`: target peer's node_id | Next seq_num | Increment & return `peer_seq_out[peer_id]` |
| `is_duplicate(peer_id: str, seq_num: int)` | `(str, int) -> bool` | `peer_id`, `seq_num` | `True` if duplicate | Check if seq_num ≤ last seen (accounting for wraparound). If new, store in received set & update `peer_seq_in`. |
| `track_pending(peer_id: str, seq_num: int, payload: bytes, msg_type: int, critical: bool)` | `(str, int, bytes, int, bool) -> None` | `peer_id`, `seq_num`, `payload`, `msg_type`, `critical` (if True, requires ACK) | — | If critical: store in `pending_acks` with retry count 0, expiry = now + 500ms. |
| `ack_received(peer_id: str, seq_num: int)` | `(str, int) -> None` | `peer_id`, `seq_num` | — | Remove from `pending_acks`. |
| `get_expired()` | `() -> list[PendingMessage]` | — | List of messages whose ACK timer expired | Iterate `pending_acks`, return those past expiry. Increment retry count. If retries ≥ 5, call `on_retry_failed`. |
| `needs_ack(msg_type: int)` | `(int) -> bool` | Message type ID | `True` if message requires ACK | Returns True for: `file_chunk` (0x31), `file_registry_query` (0x20), `file_registry_response` (0x21), `file_registry_push` (0x22), `file_publish` (0x50), `file_update` (0x51), `file_delete` (0x52), `share_file_query` (0x70), `share_file_response` (0x71). |
| `build_ack(peer_id: str, acked_msg_type: int, ack_seq: int)` | `(str, int, int) -> bytes` | `peer_id`, the original message type being ACK'd, the seq_num being acknowledged | Encoded `ack` message payload | `[2B acked_msg_type][4B ack_seq_num]`. ACK wrapper message type = 0x03. |

#### `PendingMessage` dataclass

```python
@dataclass
class PendingMessage:
    peer_id: str
    seq_num: int
    payload: bytes
    msg_type: int
    retry_count: int
    expiry: float        # monotonic time when retry is due
    created_at: float
```

---

### 3.4 `protocol.py` — Protocol Message Definitions & Router

**Purpose**: Define all message types, build typed payloads, parse payloads into typed objects, route incoming messages to handlers.

#### Constants — Message Type IDs

```python
class MsgType:
    HELLO               = 0x0001
    PING                = 0x0002
    ACK                 = 0x0003   # Generic ACK for reliable messages
    PEER_LIST_REQUEST   = 0x0010
    PEER_LIST_RESPONSE  = 0x0011
    FILE_REGISTRY_QUERY     = 0x0020
    FILE_REGISTRY_RESPONSE  = 0x0021
    FILE_REGISTRY_PUSH      = 0x0022
    FILE_REQUEST        = 0x0030
    FILE_CHUNK          = 0x0031
    FILE_CHUNK_ACK      = 0x0032
    FILE_ANNOUNCE       = 0x0040
    REPLICATION_SOLICIT = 0x0041
    REPLICATION_ACK     = 0x0042
    FILE_PUBLISH        = 0x0050
    FILE_UPDATE         = 0x0051
    FILE_DELETE         = 0x0052
    CONNECT_REQUEST     = 0x0060
    CONNECT_INTRODUCE   = 0x0061
    CONNECT_ACK         = 0x0062
    SHARE_FILE_QUERY    = 0x0070
    SHARE_FILE_RESPONSE = 0x0071
    GOODBYE             = 0x00FF
```

#### Dataclasses — Typed Message Payloads

```python
@dataclass
class HelloPayload:
    node_id: str          # 16-char hex
    public_key: bytes     # 32 bytes
    public_ip: str        # "203.0.113.5"
    public_port: int      # uint16
    uptime_since: float   # unix timestamp (float64)
    signature: bytes      # 64 bytes — signs all fields above with node's private key

@dataclass
class PingPayload:
    node_id: str

@dataclass
class AckPayload:
    acked_msg_type: int    # uint16 — the original message type being ACK'd
    ack_seq_num: int

@dataclass
class PeerEntry:
    node_id: str
    public_ip: str
    public_port: int
    uptime_since: float

@dataclass
class PeerListRequestPayload:
    pass  # empty

@dataclass
class PeerListResponsePayload:
    peers: list[PeerEntry]
    estimated_network_target: int
    signature: bytes      # 64 bytes — signs the serialised payload with node's private key

@dataclass
class ReplicaEntry:
    node_id: str          # 16-char hex
    added_at: float       # unix timestamp when replica was added

@dataclass
class FileRegistryEntry:
    file_id: str          # SHA-256 hex
    file_name: str
    file_size: int        # bytes
    mime_type: str
    author_id: str        # 16-char hex
    author_public_key: bytes  # 32 bytes
    replica_count: int
    author_signature: bytes   # 64 bytes
    replicas: list[ReplicaEntry]
    timestamp: float      # unix timestamp
    previous_file_id: str # empty if original

@dataclass
class FileRegistryQueryPayload:
    pass  # empty — "give me everything"

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
    chunk_index: int       # uint32
    total_chunks: int      # uint32
    data: bytes            # max 16384 bytes

@dataclass
class FileChunkAckPayload:
    file_id: str
    chunk_index: int

@dataclass
class FileAnnouncePayload:
    file_id: str
    node_id: str
    is_temporary: bool = False  # True if this is a temporary replica (user opened/downloaded)
    signature: bytes = b""      # 64 bytes — signs (file_id, node_id, is_temporary) with node's private key

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
    author_public_key: bytes  # 32 bytes
    timestamp: float
    author_signature: bytes   # 64 bytes — signs all fields above

@dataclass
class FileUpdatePayload:
    file_id: str              # new file_id
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
    target_node_id: str       # who A wants to connect to
    requester_node_id: str    # A's node_id
    requester_ip: str
    requester_port: int

@dataclass
class ConnectIntroducePayload:
    target_node_id: str       # who should receive the intro (C)
    introducer_node_id: str   # B's node_id (the relay)
    requester_node_id: str    # A's node_id
    requester_ip: str
    requester_port: int
    is_initiator: bool        # True if the receiving peer (C) should fire first (A's NAT is symmetric)

@dataclass
class ConnectAckPayload:
    peer_node_id: str         # who is being acknowledged

@dataclass
class GoodbyePayload:
    node_id: str

@dataclass
class ShareFileQueryPayload:
    file_id: str

@dataclass
class ShareFileResponsePayload:
    file_id: str
    file_hash: str        # SHA-256 hex of file content
    suggested_peers: list[str]  # up to 3 node_ids with longest uptime
```

#### Class: `MessageBuilder`

Static methods to encode typed payloads into bytes.

| Method | Signature | Input | Output |
|---|---|---|---|
| `hello(p: HelloPayload)` | `(HelloPayload) -> bytes` | Typed payload | Binary payload bytes |
| `ping(p: PingPayload)` | `(PingPayload) -> bytes` | | |
| `ack(p: AckPayload)` | `(AckPayload) -> bytes` | | |
| `peer_list_request()` | `() -> bytes` | — | Empty payload |
| `peer_list_response(p: PeerListResponsePayload)` | `(PeerListResponsePayload) -> bytes` | | |
| `file_registry_query()` | `() -> bytes` | — | Empty payload |
| `file_registry_response(p: FileRegistryResponsePayload)` | `(FileRegistryResponsePayload) -> bytes` | | |
| `file_registry_push(p: FileRegistryPushPayload)` | `(FileRegistryPushPayload) -> bytes` | | |
| `file_request(p: FileRequestPayload)` | `(FileRequestPayload) -> bytes` | | |
| `file_chunk(p: FileChunkPayload)` | `(FileChunkPayload) -> bytes` | | Binary: `[4B fileIdLen][fileId bytes][4B chunkIdx][4B totalChunks][4B dataLen][data]`. Note: fileId uses 4B length prefix (not 2B like `add_string`) to match spec wire format. |
| `file_chunk_ack(p: FileChunkAckPayload)` | `(FileChunkAckPayload) -> bytes` | | |
| `file_announce(p: FileAnnouncePayload)` | `(FileAnnouncePayload) -> bytes` | | |
| `replication_solicit(p: ReplicationSolicitPayload)` | `(ReplicationSolicitPayload) -> bytes` | | |
| `replication_ack(p: ReplicationAckPayload)` | `(ReplicationAckPayload) -> bytes` | | |
| `file_publish(p: FilePublishPayload)` | `(FilePublishPayload) -> bytes` | | |
| `file_update(p: FileUpdatePayload)` | `(FileUpdatePayload) -> bytes` | | |
| `file_delete(p: FileDeletePayload)` | `(FileDeletePayload) -> bytes` | | |
| `connect_request(p: ConnectRequestPayload)` | `(ConnectRequestPayload) -> bytes` | | |
| `connect_introduce(p: ConnectIntroducePayload)` | `(ConnectIntroducePayload) -> bytes` | | |
| `connect_ack(p: ConnectAckPayload)` | `(ConnectAckPayload) -> bytes` | | |
| `share_file_query(p: ShareFileQueryPayload)` | `(ShareFileQueryPayload) -> bytes` | | |
| `share_file_response(p: ShareFileResponsePayload)` | `(ShareFileResponsePayload) -> bytes` | | |
| `goodbye(p: GoodbyePayload)` | `(GoodbyePayload) -> bytes` | | |

#### Class: `MessageParser`

Static methods to parse binary payloads into typed objects.

| Method | Signature | Input | Output |
|---|---|---|---|
| `hello(data: bytes)` | `(bytes) -> HelloPayload` | Binary payload | `HelloPayload` |
| `ping(data: bytes)` | `(bytes) -> PingPayload` | | |
| `ack(data: bytes)` | `(bytes) -> AckPayload` | | |
| `peer_list_request(data: bytes)` | `(bytes) -> PeerListRequestPayload` | | (always empty) |
| `peer_list_response(data: bytes)` | `(bytes) -> PeerListResponsePayload` | | |
| `file_registry_query(data: bytes)` | `(bytes) -> FileRegistryQueryPayload` | | (always empty) |
| `file_registry_response(data: bytes)` | `(bytes) -> FileRegistryResponsePayload` | | |
| `file_registry_push(data: bytes)` | `(bytes) -> FileRegistryPushPayload` | | |
| `file_request(data: bytes)` | `(bytes) -> FileRequestPayload` | | |
| `file_chunk(data: bytes)` | `(bytes) -> FileChunkPayload` | | |
| `file_chunk_ack(data: bytes)` | `(bytes) -> FileChunkAckPayload` | | |
| `file_announce(data: bytes)` | `(bytes) -> FileAnnouncePayload` | | |
| `replication_solicit(data: bytes)` | `(bytes) -> ReplicationSolicitPayload` | | |
| `replication_ack(data: bytes)` | `(bytes) -> ReplicationAckPayload` | | |
| `file_publish(data: bytes)` | `(bytes) -> FilePublishPayload` | | |
| `file_update(data: bytes)` | `(bytes) -> FileUpdatePayload` | | |
| `file_delete(data: bytes)` | `(bytes) -> FileDeletePayload` | | |
| `connect_request(data: bytes)` | `(bytes) -> ConnectRequestPayload` | | |
| `connect_introduce(data: bytes)` | `(bytes) -> ConnectIntroducePayload` | | |
| `connect_ack(data: bytes)` | `(bytes) -> ConnectAckPayload` | | |
| `share_file_query(data: bytes)` | `(bytes) -> ShareFileQueryPayload` | | |
| `share_file_response(data: bytes)` | `(bytes) -> ShareFileResponsePayload` | | |
| `goodbye(data: bytes)` | `(bytes) -> GoodbyePayload` | | |

#### Class: `ProtocolRouter`

Central message dispatcher. One instance per node.

| Field | Type | Description |
|---|---|---|
| `handlers` | `dict[int, Callable]` | `msg_type -> handler_function` |
| `node_id` | `str` | This node's ID (for filtering) |
| `udp_engine` | `UDPEngine` | Reference to send replies |
| `peer_book` | `PeerBook` | Reference for lookups |
| `file_registry` | `FileRegistry` | Reference for registry ops |
| `storage` | `StorageManager` | Reference for file ops |
| `replication` | `ReplicationManager` | Reference for rebalancing |
| `reliable` | `ReliabilityManager` | Reference for ACK tracking |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `register(msg_type: int, handler: Callable)` | `(int, Callable) -> None` | `msg_type`, `handler(wire_msg, parsed_payload, from_addr)` | — | Register a handler for a message type |
| `route(wire_msg: WireMessage, from_addr: tuple[str, int])` | `(WireMessage, tuple) -> None` | Decoded wire message, source `(ip, port)` | — | 1. Check if ACK message → delegate to `reliable.ack_received()`. 2. If message requires ACK → build & send ACK via udp_engine. 3. If duplicate (via `reliable.is_duplicate()`) → skip. 4. Parse payload via `MessageParser`. 5. Dispatch to registered handler. |

**Required handler registrations** (set up at startup):

| MsgType | Handler | What it does |
|---|---|---|
| `HELLO` | `_handle_hello` | Verify signature, add/update peer in peer_book, mark as connected, send own hello back, trigger `peer_list_request`, then trigger registry hash exchange via `file_registry_query`. |
| `PING` | `_handle_ping` | Update last_seen, reply with ping |
| `PEER_LIST_REQUEST` | `_handle_peer_list_request` | Gather connected peers from connection manager → `PeerListResponsePayload` → send |
| `PEER_LIST_RESPONSE` | `_handle_peer_list_response` | Verify node signature against sender's public key (from peer_book or hello). Merge peers into peer_book, store `estimated_network_target`. |
| `FILE_REGISTRY_QUERY` | `_handle_file_registry_query` | Gather all local registry entries → `FileRegistryResponsePayload` → send |
| `FILE_REGISTRY_RESPONSE` | `_handle_file_registry_response` | Merge entries into local registry (latest timestamp wins), store `estimated_network_target`. Trigger rebalancing check if in startup phase. |
| `FILE_REGISTRY_PUSH` | `_handle_file_registry_push` | Merge single entry into local registry. Propagate to other connected peers (except sender). |
| `FILE_REQUEST` | `_handle_file_request` | Check `storage.has_file(file_id)`. If yes, begin chunked send via udp_engine using `file_chunk` messages. |
| `FILE_CHUNK` | `_handle_file_chunk` | Look up `DownloadState` in `pending_downloads[file_id]` (create if first chunk — set `total_chunks` and initialise `received` dict). Store chunk data in `received[chunk_index]`. Send `FILE_CHUNK_ACK`. Compute `progress = len(received) / total_chunks`, emit `download_progress` event via EventBus. If all chunks received (`len(received) == total_chunks`): reassemble in order, verify SHA-256, store to disk via `storage.store_replica()` or `storage.store_temporary_replica()`, set `download_complete` Event. |
| `FILE_CHUNK_ACK` | `_handle_file_chunk_ack` | Advance chunk send pointer for the in-progress upload. |
| `FILE_ANNOUNCE` | `_handle_file_announce` | Verify node signature against sender's known public key. Increment replica_count in local registry for that file. Propagate via gossip. |
| `REPLICATION_SOLICIT` | `_handle_replication_solicit` | Delegate to `replication.consider_solicit()`. |
| `REPLICATION_ACK` | `_handle_replication_ack` | Increment replica_count. |
| `FILE_PUBLISH` | `_handle_file_publish` | Verify author signature. Add to local registry. Gossip. |
| `FILE_UPDATE` | `_handle_file_update` | Verify author matches original. Update registry entry. |
| `FILE_DELETE` | `_handle_file_delete` | Verify author matches original. Mark deleted in registry. |
| `CONNECT_REQUEST` | `_handle_connect_request` | If connected to `target_node_id`, relay as `CONNECT_INTRODUCE` to target. If NOT connected to target, send a `CONNECT_ACK` back to requester with a failure flag (`peer_node_id` set to empty string — signal that relay is unavailable). Requester treats empty `peer_node_id` as relay failure and removes this relay from consideration. |
| `CONNECT_INTRODUCE` | `_handle_connect_introduce` | If `is_initiator`: immediately start hole punching to requester (send 3 hello packets). Else: start listening and send hellos after 200ms delay. Send `CONNECT_ACK` back via introducer to confirm receipt. |
| `CONNECT_ACK` | `_handle_connect_ack` | Signal the pending `threading.Event` for the assisted connection keyed by `peer_node_id`. |
| `SHARE_FILE_QUERY` | `_handle_share_file_query` | Look up file in registry. Find up to 3 peers hosting it with longest uptime. Build `ShareFileResponsePayload` → send. |
| `SHARE_FILE_RESPONSE` | `_handle_share_file_response` | Store response in `pending_share_responses[file_id]` and set the associated `threading.Event`. |
| `GOODBYE` | `_handle_goodbye` | Mark peer as offline in peer_book. Decrement replica counts for files they hosted. |

---

### 3.5 `stun.py` — STUN Client

**Purpose**: Discover public IP:port via STUN (RFC 5389 Binding Request).

#### Constants

```python
STUN_SERVERS = [
    ("stun.l.google.com", 19302),
    ("stun1.l.google.com", 19302),
]
STUN_TIMEOUT = 3.0  # seconds
```

#### Free Function

| Function | Signature | Input | Output | Description |
|---|---|---|---|---|
| `get_public_address(sock: socket.socket, timeout: float = STUN_TIMEOUT)` | `(socket.socket, float) -> tuple[str, int]` | A bound UDP socket, timeout | `(public_ip, public_port)` | 1. Build RFC 5389 Binding Request (20-byte header: `0x00 0x01 0x00 0x00` + 16-byte transaction ID from `os.urandom`). 2. Send to each STUN server in list. 3. Wait for first Binding Response. 4. Parse XOR-MAPPED-ADDRESS attribute (type `0x0020`). 5. Return (ip, port). Raise `StunError` if all servers timed out. |

---

### 3.6 `udp_engine.py` — UDP Socket & Hole Punching

**Purpose**: The core networking module. Owns the UDP socket, handles send/recv loop, hole punching, keepalive pings, and peer-assisted connection.

#### Class: `UDPEngine`

| Field | Type | Description |
|---|---|---|
| `sock` | `socket.socket` | Bound UDP socket (`SOCK_DGRAM`) |
| `port` | `int` | Local UDP port |
| `node_identity` | `NodeIdentity` | This node's identity (for signing hello) |
| `public_ip` | `str` | Discovered via STUN |
| `public_port` | `int` | Discovered via STUN |
| `uptime_since` | `float` | `time.time()` at startup |
| `connections` | `dict[str, ConnectionState]` | `node_id -> ConnectionState` |
| `protocol_router` | `ProtocolRouter` | Dispatches received messages |
| `reliable` | `ReliabilityManager` | ACK/retransmit tracking |
| `peer_book` | `PeerBook` | Peer directory |
| `running` | `bool` | Set False to stop the recv loop |
| `recv_thread` | `threading.Thread` | Background recv loop |
| `pending_downloads` | `dict[str, DownloadState]` | `file_id -> DownloadState` for in-progress downloads |
| `pending_assisted` | `dict[str, threading.Event]` | `target_id -> Event` signalled when CONNECT_ACK received |
| `pending_share_responses` | `dict[str, tuple[threading.Event, ShareFileResponsePayload]]` | `file_id -> (Event, response)` for awaiting share query responses |
| `upload_queue` | `dict[(str, str), UploadState]` | `(peer_id, file_id) -> UploadState` for chunked sends |
| `_addr_to_node_id` | `dict[tuple[str, int], str]` | Reverse mapping: `(ip, port) -> node_id` |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `__init__(port: int, node_identity: NodeIdentity, ...)` | Constructor | `port`, `node_identity`, refs to other modules | — | Create UDP socket, bind to `0.0.0.0:port`, set `SO_REUSEADDR`. |
| `start()` | `() -> None` | — | — | 1. Run STUN query → store `public_ip`, `public_port`. 2. Set `running = True`. 3. Start `recv_thread` (target = `_recv_loop`). 4. Start keepalive timer thread. 5. Start retransmit timer thread. |
| `stop()` | `() -> None` | — | — | Set `running = False`. Send `goodbye` to all connected peers. Close socket. |
| `_recv_loop()` | `() -> None` | — (runs in thread) | — | Loop: `sock.recvfrom(65535)` → decode via `wire.decode()` → dispatch to `protocol_router.route()`. Catch `WireError`, log. |
| `send_to(peer_id: str, msg_type: int, payload: bytes, addr: tuple[str, int] = None)` | `(str, int, bytes, tuple?) -> int` | `peer_id`, `msg_type`, `payload`, optional `addr` override | Sequence number used | 1. Get next seq_num from `reliable.next_seq(peer_id)`. 2. Encode via `wire.encode()`. 3. If `addr` is None, look up from `connections[peer_id]`. 4. `sock.sendto(encoded, addr)`. 5. If message needs ACK, `reliable.track_pending()`. |
| `hole_punch(target_id: str, target_ip: str, target_port: int, target_pubkey: bytes)` | `(str, str, int, bytes) -> bool` | Target's node_id, IP, port, public key | `True` if connection established | 1. Create `ConnectionState` in PUNCHING state. 2. Send 3 `hello` packets at 100ms intervals via `send_to()`. 3. Wait up to 5s for a `hello` response from that peer (tracked via `connections[target_id].hello_received` Event). 4. If received: mark CONNECTED, update peer_book, return True. 5. If timeout: return False. |
| `peer_assisted_connect(target_id: str, relay_id: str)` | `(str, str) -> bool` | `target_id`: peer to reach; `relay_id`: mutual peer to relay through | `True` if connected | 1. Create `threading.Event` in `pending_assisted[target_id]`. 2. Loop with backoff delays [1.0, 2.0, 4.0]: send `CONNECT_REQUEST` to relay, wait for Event with timeout = delay + 5s. 3. If Event is set (CONNECT_ACK received): start mutual hole punch — if this node is designated initiator, send 3 `hello` packets first; else wait 200ms then send. 4. Return result. 5. If all 3 retries exhausted, remove Event and return False. |
| `download_file(file_id: str)` | `(str) -> bytes` | `file_id` | File content as bytes | 1. Find a peer hosting this file in registry. 2. Send `FILE_REQUEST` to that peer. 3. Create a `DownloadState` in `pending_downloads[file_id]`. 4. Wait on `download_complete` Event. 5. Return reassembled bytes. |
| `upload_file(peer_id: str, file_id: str, data: bytes)` | `(str, str, bytes) -> None` | `peer_id`, `file_id`, raw file bytes | — | 1. Chunk data into 16KB pieces. 2. Create `UploadState` in `upload_queue`. 3. Send first chunk. 4. Chunk ACKs advance the pointer; retransmit on timeout. |
| `broadcast(msg_type: int, payload: bytes)` | `(int, bytes) -> None` | `msg_type`, `payload` | — | Send to all CONNECTED peers in `connections`. |
| `broadcast_except(msg_type: int, payload: bytes, exclude_id: str)` | `(int, bytes, str) -> None` | As above + excluded peer_id | — | Send to all except `exclude_id`. |
| `get_connected_peers()` | `() -> list[str]` | — | List of connected `node_id`s | Filter `connections` for CONNECTED state. |
| `lan_broadcast()` | `() -> None` | — | — | Send `hello` to `255.255.255.255:port`. |
| `check_liveness()` | `() -> None` | — | — | Iterate `connections`. For each peer where `!is_alive(conn)`: call `mark_disconnected()`, `peer_book.mark_offline(node_id)`, `file_registry.remove_peer_replicas(node_id)`, `replication.on_peer_disconnected(node_id)`. Emit `peer_disconnected` event. Remove from `connections`. |
| `resolve_node_id(addr: tuple[str, int])` | `(tuple) -> str | None` | `(ip, port)` | `node_id` or None | Look up in `_addr_to_node_id`. Updated in `_recv_loop` whenever a message arrives. |

#### `ConnectionState` dataclass

```python
@dataclass
class ConnectionState:
    node_id: str
    public_key: bytes
    address: tuple[str, int]      # (ip, port)
    state: str                    # "PUNCHING" | "CONNECTED" | "ASSISTED" | "DISCONNECTED" | "UNREACHABLE"
    hello_received: threading.Event
    last_seen: float
    uptime_since: float           # peer's reported uptime
    hole_punch_attempts: int
    direct_blocked: bool          # True if direct hole punch failed
```

#### `UploadState` dataclass

```python
@dataclass
class UploadState:
    peer_id: str
    file_id: str
    chunks: list[bytes]
    total_chunks: int
    current_chunk: int
    retries: dict[int, int]       # chunk_index -> retry_count
    last_sent: dict[int, float]
```

#### `DownloadState` dataclass

```python
@dataclass
class DownloadState:
    file_id: str
    total_chunks: int              # known after first chunk arrives
    received: dict[int, bytes]     # chunk_index -> chunk data
    peer_id: str                   # the peer we're downloading from
    started_at: float
    download_complete: threading.Event
```

---

### 3.7 `connection.py` — Per-Peer Connection State

**Purpose**: Thin module — the `ConnectionState` dataclass and helper for managing connection lifecycle events. Most logic lives in `udp_engine.py`. This module provides:

#### Free Functions

| Function | Signature | Input | Output | Description |
|---|---|---|---|---|
| `new_connection(node_id: str, pubkey: bytes, addr: tuple[str, int])` | `(str, bytes, tuple) -> ConnectionState` | `node_id`, `pubkey`, `addr` | `ConnectionState` in PUNCHING state | Factory |
| `mark_connected(conn: ConnectionState)` | `(ConnectionState) -> None` | Connection | — | Set state to CONNECTED, set `hello_received` Event, update `last_seen` |
| `mark_assisted(conn: ConnectionState)` | `(ConnectionState) -> None` | Connection | — | Set state to ASSISTED |
| `mark_disconnected(conn: ConnectionState)` | `(ConnectionState) -> None` | Connection | — | Set state to DISCONNECTED |
| `is_alive(conn: ConnectionState, timeout: float = 90.0)` | `(ConnectionState, float) -> bool` | Connection, timeout seconds | `True` if last_seen within timeout | Check if peer is considered still connected |
| `increment_attempts(conn: ConnectionState)` | `(ConnectionState) -> None` | Connection | — | Increment `hole_punch_attempts`. If ≥ 5, set `direct_blocked = True`. |

---

### 3.8 `peer_book.py` — SQLite Peer Directory

**Purpose**: Persistent directory of all known peers with tiering, last-seen tracking, and cleanup.

#### Database Schema

```sql
CREATE TABLE IF NOT EXISTS peers (
    node_id         TEXT PRIMARY KEY,   -- 16-char hex
    public_key      BLOB NOT NULL,      -- 32 bytes
    public_ip       TEXT NOT NULL,
    public_port     INTEGER NOT NULL,
    uptime_since    REAL,               -- unix timestamp (float)
    last_seen       REAL,               -- unix timestamp
    tier            INTEGER NOT NULL DEFAULT 3,  -- 1=Critical, 2=Recent, 3=General
    consecutive_fails INTEGER DEFAULT 0,
    first_seen      REAL NOT NULL,
    is_bootstrap    INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_peers_tier ON peers(tier, last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_peers_last_seen ON peers(last_seen);
```

#### Class: `PeerBook`

| Field | Type | Description |
|---|---|---|
| `db_path` | `str` | Path to `peers.db` |
| `file_registry` | `FileRegistry` | Reference for tier calculation |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `__init__(data_dir: str)` | `(str) -> None` | `data_dir`: base data directory | — | Open/create `peers.db`, run schema migration |
| `add_or_update(node_id: str, public_key: bytes, ip: str, port: int, uptime_since: float, is_bootstrap: bool = False)` | `(str, bytes, str, int, float, bool) -> None` | Peer details | — | INSERT OR REPLACE. Reset `consecutive_fails` to 0. Update `last_seen` to now. |
| `get(node_id: str)` | `(str) -> dict | None` | `node_id` | Row as dict or None | SELECT by node_id |
| `get_by_tier(tier: int, limit: int = 100)` | `(int, int) -> list[dict]` | `tier` (1-3), `limit` | List of peer dicts ordered by last_seen DESC | |
| `get_all_ordered()` | `() -> list[dict]` | — | All peers, sorted by tier ASC, last_seen DESC | For reconnection sequence |
| `mark_seen(node_id: str)` | `(str) -> None` | `node_id` | — | Update `last_seen = now()`, reset `consecutive_fails = 0` |
| `mark_offline(node_id: str)` | `(str) -> None` | `node_id` | — | Does NOT remove. Just sets last_seen (already done). |
| `record_failure(node_id: str)` | `(str) -> None` | `node_id` | — | Increment `consecutive_fails`. If ≥ 5, demote tier by 1 (min 3). |
| `recalculate_tiers(file_registry: FileRegistry)` | `(FileRegistry) -> None` | Reference to FileRegistry | — | **Tier 1**: peers that are author of any file in local storage OR host a replica of a file this node authored. **Tier 2**: last_seen within 7 days AND not Tier 1. **Tier 3**: all others. Update all rows. |
| `cleanup(max_age_days: float = 30)` | `(float) -> int` | `max_age_days` | Number removed | DELETE peers where `last_seen < now - max_age_days` AND `tier >= 3` AND `consecutive_fails >= 10` AND `is_bootstrap = 0`. |
| `count()` | `() -> int` | — | Total peer count | |
| `count_by_tier(tier: int)` | `(int) -> int` | `tier` | Count | |

---

### 3.9 `file_registry.py` — Decentralised File Registry

**Purpose**: Local copy of the network-wide file registry, gossiped between peers. SQLite-backed.

#### Database Schema

```sql
CREATE TABLE IF NOT EXISTS files (
    file_id             TEXT PRIMARY KEY,   -- SHA-256 hex
    file_name           TEXT NOT NULL,
    file_size           INTEGER NOT NULL,   -- bytes
    mime_type           TEXT NOT NULL,
    author_id           TEXT NOT NULL,      -- 16-char hex
    author_public_key   BLOB NOT NULL,      -- 32 bytes
    replica_count       INTEGER NOT NULL DEFAULT 0,
    author_signature    BLOB NOT NULL,      -- 64 bytes
    timestamp           REAL NOT NULL,      -- unix timestamp
    previous_file_id    TEXT DEFAULT '',
    is_deleted          INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS replicas (
    file_id     TEXT NOT NULL,
    node_id     TEXT NOT NULL,              -- 16-char hex
    added_at    REAL NOT NULL,
    is_local    INTEGER DEFAULT 0,          -- 1 if stored on this node
    PRIMARY KEY (file_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_files_author ON files(author_id);
CREATE INDEX IF NOT EXISTS idx_files_replica ON files(replica_count);
CREATE INDEX IF NOT EXISTS idx_replicas_node ON replicas(node_id);
```

#### Class: `FileRegistry`

| Field | Type | Description |
|---|---|---|
| `db_path` | `str` | Path to `registry.db` |
| `entries` | `dict[str, FileRegistryEntry]` | In-memory cache (fast lookups) |
| `node_id` | `str` | This node's ID |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `__init__(data_dir: str, node_id: str)` | `(str, str) -> None` | `data_dir`, `node_id` | — | Open/create `registry.db`, load all entries into memory cache |
| `add(entry: FileRegistryEntry)` | `(FileRegistryEntry) -> None` | Registry entry | — | INSERT OR REPLACE into `files` table. For each replica in `entry.replicas`, INSERT OR IGNORE into `replicas`. Update in-memory cache. |
| `update(entry: FileRegistryEntry)` | `(FileRegistryEntry) -> None` | Updated entry | — | Only update if `entry.timestamp > existing.timestamp`. Same as add otherwise. |
| `get(file_id: str)` | `(str) -> FileRegistryEntry | None` | `file_id` | Entry or None | From in-memory cache |
| `get_all()` | `() -> list[FileRegistryEntry]` | — | All non-deleted entries | Filter `is_deleted = 0` |
| `get_by_author(author_id: str)` | `(str) -> list[FileRegistryEntry]` | `author_id` | Author's files | |
| `mark_deleted(file_id: str)` | `(str) -> None` | `file_id` | — | Set `is_deleted = 1` in DB and remove from cache |
| `increment_replica(file_id: str, node_id: str)` | `(str, str) -> None` | `file_id`, `node_id` | — | INSERT OR IGNORE into `replicas`. Increment `replica_count` by 1. |
| `decrement_replica(file_id: str, node_id: str)` | `(str, str) -> None` | `file_id`, `node_id` | — | DELETE from `replicas`. Decrement `replica_count` by 1 (min 0). |
| `remove_peer_replicas(node_id: str)` | `(str) -> list[str]` | `node_id` | List of affected `file_id`s | When a peer disconnects: find all replicas they hosted, remove them, decrement counts. Return affected file_ids for rebalancing check. |
| `compute_hash()` | `() -> str` | — | SHA-256 hash of all (file_id, timestamp) pairs sorted | For gossip comparison — quick check if registries differ |
| `get_delta(their_hash: str)` | `(str) -> list[FileRegistryEntry]` | Hash from another peer | Entries newer than what they have, or all if hash differs | If hashes match → empty list. Else → return all entries (full sync). Future: implement delta sync. |
| `merge_delta(entries: list[FileRegistryEntry])` | `(list[FileRegistryEntry]) -> None` | Entries from another peer | — | For each entry: if not in local registry, add. If exists and incoming timestamp > local timestamp, update. |
| `verify_author_signature(entry: FileRegistryEntry)` | `(FileRegistryEntry) -> bool` | Entry with signature | `True` if valid | Reconstruct signed payload: `{file_id, file_name, file_size, mime_type, author_id, timestamp}` as deterministic binary → verify against `author_public_key` and `author_signature`. |
| `total_unique_file_size()` | `() -> int` | — | Sum of all unique file sizes | For `networkTarget` calculation |
| `count()` | `() -> int` | — | Number of non-deleted files | |
| `cleanup_old_versions(max_age_seconds: float = 86400)` | `(float) -> int` | `max_age_seconds` (default 24h) | Number removed | Find files where a newer version exists (traced via `previous_file_id` chain) AND the old version's `timestamp < now - max_age_seconds`. Remove entries from registry and return count. The calling code handles disk deletion. | |
| `get_version_chain(file_id: str)` | `(str) -> list[str]` | Current `file_id` | List of all `file_id`s in the version chain (oldest first) | Follow `previous_file_id` links to build the full chain. |

---

### 3.10 `storage.py` — Disk Storage & Quota Manager

**Purpose**: Manage files on disk, track storage usage, enforce quotas.

#### Class: `StorageManager`

| Field | Type | Description |
|---|---|---|
| `files_dir` | `str` | `~/.decentralised-web/files/` |
| `total_configured_mb` | `int` | Default 500MB |
| `author_id` | `str | None` | This node's logged-in author (for tracking own files) |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `__init__(data_dir: str, total_mb: int)` | `(str, int) -> None` | `data_dir`, `total_mb` | — | Create `files/` dir if needed. Set `total_configured_mb = total_mb`. Note: if `total_mb` is 0 or unset, use `max(used_for_own_files() / (1024*1024), DEFAULT_STORAGE_MB)` — dynamic default. |
| `store_own_file(file_id: str, data: bytes, file_name: str, mime_type: str)` | `(str, bytes, str, str) -> str` | `file_id`, raw bytes, name, MIME type | Absolute file path | Write to `files/<file_id>`. Track in metadata. |
| `store_replica(file_id: str, data: bytes)` | `(str, bytes) -> str` | `file_id`, raw bytes | File path | Write to `files/<file_id>`. Mark as replica (not own). |
| `store_temporary_replica(file_id: str, data: bytes, tab_id: str)` | `(str, bytes, str) -> str` | `file_id`, raw bytes, tab identifier | File path | Write to `files/<file_id>`. Add to `temporary_files` in metadata with `expires_at = now + 3600` (1h timeout) and `tab_id`. |
| `promote_temporary(file_id: str)` | `(str) -> None` | `file_id` | — | Remove from `temporary_files`, add to `replica_files`. Called when tab closes or timeout elapses. |
| `cleanup_expired_temporary()` | `() -> list[str]` | — | List of file_ids promoted | Find all temporary files where `expires_at < now`. Call `promote_temporary()` for each. After promotion, trigger `replication.execute_rebalance()`. Return promoted file_ids. |
| `has_file(file_id: str)` | `(str) -> bool` | `file_id` | `True` if file exists on disk | |
| `read_file(file_id: str)` | `(str) -> bytes` | `file_id` | File content | Read from `files/<file_id>`. Raise `FileNotFoundError` if missing. |
| `delete_file(file_id: str)` | `(str) -> bool` | `file_id` | `True` if deleted | Remove from disk. Return False if file didn't exist. |
| `used_for_own_files()` | `() -> int` | — | Bytes used by own published files | Sum of files whose `file_id` appears in own file list |
| `used_for_replicas()` | `() -> int` | — | Bytes used by replica files | Total stored minus own |
| `available_bytes()` | `() -> int` | — | Free bytes under quota | `total_configured_bytes - used_for_own_files() - used_for_replicas()` |
| `total_configured_bytes()` | `() -> int` | — | `total_configured_mb * 1024 * 1024` | |
| `is_over_quota()` | `() -> bool` | — | `True` if used > configured | |
| `get_storage_breakdown()` | `() -> dict` | — | `{ "own": int, "replicas": int, "available": int, "total": int }` | For UI display |
| `set_quota(mb: int)` | `(int) -> None` | New quota in MB | — | Update `total_configured_mb` |
| `get_files_sorted_by_size()` | `() -> list[tuple[str, int]]` | — | `[(file_id, size), ...]` sorted largest first | For rebalancing (find largest to delete first) |

#### Metadata file

`~/.decentralised-web/files/.metadata.json`:
```json
{
  "own_files": ["fileId1", "fileId2"],
  "replica_files": ["fileId3"],
  "temporary_files": {
    "fileId4": {"expires_at": 1712345678.0, "tab_id": "abc123"}
  }
}
```

---

### 3.11 `replication.py` — Rebalancing & Diversity Logic

**Purpose**: Calculate network target, decide which files to replicate/delete, maximise diversity.

#### Class: `ReplicationManager`

| Field | Type | Description |
|---|---|---|
| `file_registry` | `FileRegistry` | Reference |
| `storage` | `StorageManager` | Reference |
| `peer_book` | `PeerBook` | Reference |
| `udp_engine` | `UDPEngine` | Reference (for sending solicitations) |
| `rebalance_gate` | `bool` | `True` when rebalancing is allowed (post-reconnection assessment) |
| `estimated_network_target` | `int` | Merged from gossip (median of received estimates) |
| `received_targets` | `list[int]` | Collected `estimated_network_target` values from peers |
| `tier1_contacted` | `set[str]` | Set of Tier 1 node_ids that have been contacted (regardless of success) |
| `tier1_total` | `int` | Total number of Tier 1 peers known at startup |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `calculate_network_target()` | `() -> int` | — | Computed target | `floor(Σ storage_of_all_known_peers / Σ unique_file_size)`. Uses cached values from registry. |
| `receive_target_estimate(value: int)` | `(int) -> None` | Estimate from a peer | — | Append to `received_targets`. Recalculate median → `estimated_network_target`. |
| `open_gate()` | `() -> None` | — | — | Set `rebalance_gate = True`. Called after startup Phase 2 completes. |
| `should_rebalance()` | `() -> bool` | — | `True` if gate open AND (connected peers ≥ 3 OR all Tier 1 peers contacted) | |
| `get_under_replicated()` | `() -> list[FileRegistryEntry]` | — | Files where `replica_count < networkTarget - 1` | |
| `get_over_replicated()` | `() -> list[FileRegistryEntry]` | — | Files where `replica_count > networkTarget + 1`, limited to locally stored files | |
| `get_at_target_low()` | `() -> list[FileRegistryEntry]` | — | Files at `replica_count == networkTarget - 1` | Bottom edge of tolerance band |
| `get_at_target_high()` | `() -> list[FileRegistryEntry]` | — | Files at `replica_count == networkTarget + 1`, locally stored | Top edge of tolerance band |
| `get_at_target_exact()` | `() -> list[FileRegistryEntry]` | — | Files at `replica_count == networkTarget`, locally stored, not own files | Exact target — last resort for deletion to make room for under-replicated files |
| `diversity_score(file_id: str)` | `(str) -> float` | `file_id` | Score 0.0–1.0 | 1. Get set of node_ids hosting this file. 2. Get set of node_ids this node is connected to. 3. `score = 1.0 - (|intersection| / |existing_replicas|)`. Higher score = less overlap = more diverse. |
| `rank_by_diversity(candidates: list[FileRegistryEntry])` | `(list[FileRegistryEntry]) -> list[FileRegistryEntry]` | Candidate files | Sorted list (highest diversity first) | Compute diversity score for each, sort descending. |
| `select_files_to_replicate(limit_bytes: int)` | `(int) -> list[str]` | Available bytes | List of file_ids to replicate | 1. Get under-replicated files. 2. Rank by diversity. 3. Greedily select until total size ≤ limit_bytes. 4. If space remains, consider `get_at_target_low()`. |
| `select_files_to_delete(needed_bytes: int)` | `(int) -> list[str]` | Bytes needed to free | List of file_ids to delete | 1. Get over-replicated files (locally stored). 2. Sort by highest replica_count first (most over-replicated). 3. Greedily select until total size ≥ needed_bytes. 4. If still not enough, consider `get_at_target_high()`. 5. If still not enough, consider `get_at_target_exact()` (only if an under-replicated file is critically vulnerable: `replica_count <= max(1, floor(networkTarget / 2))`). 6. Never select own published files. 7. Never select files where this node is the only replica holder. |
| `execute_rebalance()` | `() -> None` | — | — | 1. If `!should_rebalance()` → return. 2. Compute `available = storage.available_bytes()`. 3. If any file is under-replicated and `available > 0`: select & replicate. 4. If storage is full and under-replicated files exist: `select_files_to_delete()`, delete, then replicate. 5. If over-replicated files exist and no under-replicated files: consider deleting to free space. |
| `solicit_replication(file_id: str)` | `(str) -> None` | `file_id` | — | Build `ReplicationSolicitPayload`, broadcast to connected peers. |
| `consider_solicit(payload: ReplicationSolicitPayload)` | `(ReplicationSolicitPayload) -> bool` | Solicitation | `True` if accepting | 1. Check if file already stored locally → return False. 2. Check if space available. 3. If yes: request file via `udp_engine.download_file()`, store, send `REPLICATION_ACK`. |
| `on_peer_disconnected(node_id: str)` | `(str) -> None` | `node_id` | — | Remove peer's replicas from registry. Trigger `execute_rebalance()` if gate open. |

---

### 3.12 `tui.py` — Terminal UI

**Purpose**: Rich-based interactive terminal UI with live peer list, file search, tabs, keyboard shortcuts.

#### Class: `TUI`

| Field | Type | Description |
|---|---|---|
| `node` | `App` | Reference to main app (for calling methods) |
| `console` | `rich.console.Console` | Rich console |
| `layout` | `rich.layout.Layout` | Main layout |
| `running` | `bool` | Control flag |

| Method | Signature | Description |
|---|---|---|
| `__init__(app: App)` | Constructor | Create Rich console, set up layout regions |
| `run()` | `() -> None` | Main loop. Uses `rich.live.Live` with auto-refresh. Reads keyboard input via `get_key()`. |
| `render()` | `() -> Renderable` | Build the full TUI renderable (peer table, file table, search bar, status bar, help bar) |
| `render_peer_table()` | `() -> Table` | Rich Table with columns: Node ID (truncated), Username, Status (🟢/🟡/🔴), Uptime, Address |
| `render_file_table()` | `() -> Table` | Rich Table with columns: Icon, File Name, Size, Replicas, Author, Actions. Filtered by search string. |
| `render_my_files_table()` | `() -> Table` | As above, filtered to files authored by logged-in user. Extra actions: [u]pdate, [x]delete. |
| `render_storage_tab()` | `() -> Table` | Rich Table with columns: File ID (truncated), Type (Own/Replica/Temporary), Size, Status. Shows all locally stored files with their category. |
| `render_peer_book_tab()` | `() -> Table` | Rich Table with columns: Node ID, Tier, Last Seen, Address, Status. Paginated list of all known peers from `peer_book.get_all_ordered()`. |
| `render_status_bar()` | `() -> Panel` | Storage bar + health indicator + peer/file counts |
| `handle_key(key: str)` | `(str) -> None` | Dispatch keyboard input |
| `do_search(char: str)` | `(str) -> None` | Append to search string, filter file list |
| `do_download(file_id: str)` | `(str) -> None` | Trigger file download via `node.download_file()` |
| `do_publish()` | `() -> None` | Prompt for file path → read file → `node.publish_file()` |
| `do_connect()` | `() -> None` | Prompt for peer address → parse → `node.connect_to_peer()` |
| `do_login()` | `() -> None` | Prompt for username + password → `node.login()` |
| `do_update(file_id: str)` | `(str) -> None` | Prompt for new file → `node.update_file()` |
| `do_delete(file_id: str)` | `(str) -> None` | Confirm → `node.delete_file()` |
| `do_replicate(file_id: str)` | `(str) -> None` | Trigger replication for selected file via `node.replication.solicit_replication(file_id)`. |
| `switch_tab(tab: int)` | `(int) -> None` | Switch between [1] Network Files, [2] My Files, [3] Storage, [4] Peers |

#### TUI Input Key Bindings

| Key | Action |
|---|---|
| `q` | Quit |
| `h` | Show help |
| `s` | Show stats/status |
| `p` | Publish file |
| `c` | Connect to peer |
| `l` | Login |
| `m` | Switch to My Files tab |
| `1-4` | Switch to tab 1-4 |
| `d` | Download selected file |
| `r` | Trigger replication for selected file |
| `u` | Update selected file (My Files tab) |
| `x` | Delete selected file (My Files tab) |
| `↑/↓` | Navigate file list |
| `Enter` | Expand/collapse selected file |
| `Esc` | Clear search / go back |
| Any printable | Append to search filter |

---

### 3.13 `web/__init__.py` — Flask App Factory

#### Function: `create_app(node: App, web_port: int, web_host: str)`

| Signature | Input | Output | Description |
|---|---|---|---|
| `(App, int, str) -> Flask` | Reference to main `App` instance, port, host | Configured Flask app | 1. Create Flask app. 2. Register blueprints/routes. 3. Set secret key from `os.urandom(32)` (session cookies). 4. Store `node` reference in `app.config['node']`. 5. Return app. |

---

### 3.14 `web/routes.py` — HTTP Routes

#### Blueprint: `main`

| Route | Methods | Auth | Purpose |
|---|---|---|---|
| `/` | GET | Required | Serve `index.html` if authenticated, else redirect to `/login` |
| `/login` | GET | None | Serve `login.html` |
| `/api/login` | POST | None | Accept JSON `{ "username": str, "password": str }`. Derive `AuthorIdentity`. Verify password by checking if derived key matches stored author (no stored password — if no files exist yet, any derivation succeeds). Set Flask session `{ "username", "author_id", "authenticated": True }`. Return `{ "success": true, "author_id": "..." }`. |
| `/api/logout` | POST | Required | Clear session. Return `{ "success": true }`. |
| `/api/status` | GET | Required | Return JSON: `{ "node_id", "author_id", "peers_connected": int, "files_count": int, "storage": {...}, "health": "healthy"/"reconnecting"/... }` |
| `/api/files` | GET | Required | Return all registry entries as JSON array |
| `/api/files/my` | GET | Required | Return files authored by logged-in user |
| `/api/files/<file_id>` | GET | Required | Get single file entry |
| `/api/files/<file_id>/download` | GET | Required | Download file: `node.open_file(file_id)` → serve with appropriate Content-Type and Content-Disposition |
| `/api/files/<file_id>/open` | GET | Required | Open file (same as download but with inline Content-Disposition) |
| `/api/files/upload` | POST | Required | Accept multipart file upload. Read file data, determine MIME type → `node.publish_file(data, file_name, mime_type)` → return `{ "file_id": "..." }` |
| `/api/files/<file_id>/update` | POST | Required | Accept multipart file upload. `node.update_file(file_id, new_data)` → return `{ "file_id": "..." }` |
| `/api/files/<file_id>/delete` | DELETE | Required | `node.delete_file(file_id)` → return `{ "success": true }` |
| `/api/files/<file_id>/close` | POST | Required | `storage.promote_temporary(file_id)`. Return `{ "success": true }`. |
| `/api/peers` | GET | Required | Return list of known peers with status |
| `/api/peers/connect` | POST | Required | Accept `{ "url": "connection URL" }`. Parse URL → `node.connect_to_peer(id, pk, ip, port)`. Return `{ "success": true }` |
| `/api/network-name` | GET | Required | Return `{ "name": str }` from config file (default: auto-generated). |
| `/api/network-name` | POST | Required | Accept `{ "name": str }`. Save to `config.json`. Return `{ "success": true }`. |
| `/api/qr` | GET | Required | Return `{ "url": "<connection URL>" }` for this node's QR code |
| `/api/storage/config` | GET | Required | Return `{ "total_mb": int, "used_own": int, "used_replicas": int, "available": int }` |
| `/api/storage/config` | POST | Required | Accept `{ "total_mb": int }`. Validate: if `total_mb * 1024 * 1024 < used_for_own_files()`, return 400 with `{ "error": "Quota too low", "min_required_mb": ceil(used_for_own_files() / 1024 / 1024) }`. Else update quota. |
| `/api/share/<file_id>` | GET | Required | Generate share URL: `node.create_share_link(file_id)` → return `{ "url": "...", "qr_url": "..." }` |

#### Auth decorator

```python
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            return jsonify({"error": "Authentication required"}), 401
        return f(*args, **kwargs)
    return decorated
```

---

### 3.15 `web/ws.py` — WebSocket Handler

**Purpose**: Real-time push updates to the web UI via WebSocket (flask-sock).

#### WebSocket endpoint: `/ws`

| Event (Server → Client) | Payload | Trigger |
|---|---|---|
| `peer_update` | `{ "node_id": str, "status": str, "address": str, "uptime": float }` | Peer connects, disconnects, or pings |
| `file_update` | `{ "action": "added"/"updated"/"deleted", "entry": FileRegistryEntry }` | Registry changes |
| `storage_update` | `{ "used_own": int, "used_replicas": int, "available": int }` | Storage changes |
| `health_update` | `{ "status": str, "peers_connected": int }` | Health state change |
| `download_progress` | `{ "file_id": str, "progress": float, "status": "downloading"/"complete"/"error" }` | File download progress |

| Event (Client → Server) | Payload | Action |
|---|---|---|
| `download` | `{ "file_id": str }` | Trigger file download |
| `open` | `{ "file_id": str }` | Trigger file open |
| `share` | `{ "file_id": str }` | Generate share link, send back as `share_response` |
| `connect_peer` | `{ "url": str }` | Parse and connect to peer |
| `publish` | `{ "file_name": str }` | (Not the binary — binary goes via HTTP POST. This just notifies of intent.) |
| `tab_closed` | `{ "file_id": str }` | Notify server that the opened tab was closed. Server calls `storage.promote_temporary(file_id)` and triggers rebalancing. |

#### Implementation notes:
- WebSocket connection is established after successful login.
- Server holds a set of connected WebSocket clients and broadcasts to all on state changes.
- The `node` object has an `event_bus` (simple pub/sub) that the WebSocket handler subscribes to.
- **Health debouncing**: `health_update` events are debounced — a pending health state must persist for 5 seconds before being broadcast. A debounce timer in the WebSocket handler tracks the pending state; if the state changes again within 5s, the timer resets. This prevents brief disconnect/reconnect cycles from causing UI flicker.

---

### 3.16 `web/templates/index.html` — Main UI

Full HTML structure for the authenticated UI. Jinja2 template.

**Sections**:
1. **Title bar**: Network name, author username, [Share Network] button, Node ID, [Logout]
2. **Left panel** (collapsible): QR code, copy link, paste peer address, scan QR, hide panel
3. **Center panel — Tabs**: [My Files] [Browse] with search box and [+ Upload]. [Settings] tab for storage quota configuration.
4. **File rows**: Collapsed (icon, name, type, size, replicas, expand arrow), Expanded (author, peers, [Open] [Download] [Share/Update/Delete])
5. **Bottom bar**: Storage progress bar, health indicator
6. **Settings tab**: Storage quota slider/input with fair contribution warning if quota < own files size. Network name field.

Requires: `style.css`, `app.js`, `qrcode.min.js`, WebSocket.

---

### 3.17 `web/templates/login.html` — Login Page

Simple centered login form with dark background.

```html
<form id="login-form">
  <input type="text" id="username" placeholder="Username" required>
  <input type="password" id="password" placeholder="Password" required>
  <button type="submit">Login</button>
  <p id="error-msg" class="error"></p>
</form>
```

On submit: POST `/api/login` → on success, redirect to `/`. On failure, show error.

---

### 3.18 `web/static/style.css` — Dark Mode Styles

**Design system**:
- Background: `#1a1a2e` (deep navy)
- Surface: `#16213e` (dark blue)
- Card: `#0f3460` (medium blue)
- Accent: `#e94560` (coral red)
- Text: `#eee` (light grey)
- Text secondary: `#a0a0b0`
- Success: `#00c853` (green)
- Warning: `#ff9800` (amber)
- Error: `#ff1744` (red)
- Font: `system-ui, -apple-system, sans-serif`

**Layout**: CSS Grid for the main 3-column structure (left panel, center, right gutter). Flexbox for rows. Smooth transitions for expand/collapse. Responsive down to 768px (hides side panel).

**Components styled**:
- `.title-bar` — full width, flex, space-between
- `.side-panel` — 280px, collapsible with transition
- `.side-panel.collapsed` — width 0, overflow hidden
- `.center-panel` — flex-grow, scrollable
- `.tab-bar` — horizontal tabs with active indicator
- `.file-row` — hover highlight, click to expand
- `.file-row.expanded` — shows detail section
- `.file-details` — hidden by default, flex column
- `.bottom-bar` — full width, sticky bottom
- `.storage-bar` — progress bar with gradient fill
- `.health-indicator` — coloured dot + text
- `.qr-container` — centered, white background for QR readability
- `.btn` — styled buttons: `.btn-primary`, `.btn-danger`, `.btn-ghost`
- `.search-box` — rounded input with icon
- `.login-form` — centered card, max-width 400px
- `.error` — red text for error messages
- `.status-dot` — small coloured circle (green/amber/red)
- Animations: `@keyframes fadeIn`, `@keyframes slideIn`

---

### 3.19 `web/static/app.js` — Frontend Logic

**Purpose**: All client-side interactivity. Vanilla JS (no framework).

#### Global State

```javascript
const state = {
    ws: null,                    // WebSocket connection
    authorId: null,
    files: [],                   // Cached file entries
    connectedPeers: [],
    searchQuery: '',
    activeTab: 'browse',        // 'browse' | 'myfiles'
    expandedFileId: null,
    sidePanelVisible: true,
    storage: { own: 0, replicas: 0, available: 0, total: 0 },
    health: 'reconnecting'
};
```

#### Functions

| Function | Description |
|---|---|
| `init()` | Called on page load. Check auth via `/api/status`. If unauthenticated, redirect to `/login`. Connect WebSocket. Load initial data. |
| `connectWebSocket()` | Create `WebSocket` to `ws://<host>:<port>/ws`. Set `onmessage`, `onclose`, `onerror` handlers. |
| `handleWSMessage(event)` | Parse JSON. Switch on `event.type`: `peer_update` → update peer list; `file_update` → refresh file list; `storage_update` → update storage bar; `health_update` → update health indicator. |
| `loadFiles()` | GET `/api/files` → populate `state.files` → `renderFileList()` |
| `loadPeers()` | GET `/api/peers` → populate `state.connectedPeers` → `renderPeerPanel()` |
| `loadStorage()` | GET `/api/storage/config` → update `state.storage` → `renderBottomBar()` |
| `renderFileList()` | Clear and rebuild file rows based on `state.activeTab` and `state.searchQuery`. Add click handlers for expand/collapse. |
| `renderPeerPanel()` | Update peer list in side panel. Auto-collapse side panel if 2+ peers connected. |
| `renderBottomBar()` | Update storage progress bar (colour-coded segments) and health indicator. |
| `expandFile(fileId)` | Toggle `state.expandedFileId`. Show/hide file details. Fetch peer info for replicas. |
| `downloadFile(fileId)` | Send WebSocket `{ type: "download", file_id: fileId }`. Track progress. |
| `openFile(fileId)` | Send WebSocket `{ type: "open", file_id: fileId }`. Open in new tab when complete. |
| `shareFile(fileId)` | Send WebSocket `{ type: "share", file_id: fileId }`. On `share_response`, show modal with URL and QR code. |
| `uploadFile(file)` | POST multipart to `/api/files/upload`. On success, refresh file list. |
| `updateFile(fileId, file)` | POST multipart to `/api/files/<id>/update`. |
| `deleteFile(fileId)` | Confirm → DELETE `/api/files/<id>/delete`. |
| `connectPeer(url)` | POST `/api/peers/connect` with `{ url }`. |
| `scanQR()` | Access `getUserMedia` for camera, use `jsQR` or browser QR scanner. On scan → `connectPeer(url)`. |
| `copyShareLink()` | Copy the connection URL from the QR panel to clipboard. |
| `pastePeerAddress()` | Read clipboard → if it looks like a connection URL → `connectPeer(url)`. |
| `switchTab(tab)` | Set `state.activeTab`, re-render file list. |
| `filterFiles()` | Apply `state.searchQuery` to `state.files`, re-render. |
| `logout()` | POST `/api/logout` → redirect to `/login`. |
| `showQRModal(url)` | Create modal overlay with QR code canvas (using `qrcode.min.js`) and copy button. |
| `onFileRowClick(fileId)` | Toggle expand. If already expanded, collapse. |
| `setStorageQuota(mb)` | POST `/api/storage/config` with `{ total_mb: mb }`. On 400, show warning: "Cannot set quota below your own files size (X MB)." On success, refresh storage. |
| `showSettingsTab()` | Render the Settings tab: quota input, fair contribution warning, network name field. |

#### Event Listeners

- Upload input change → `uploadFile(this.files[0])`
- Search input keyup → update `state.searchQuery`, call `filterFiles()`
- Tab clicks → `switchTab()`
- [Share Network] click → show side panel
- [✕ Hide] click → collapse side panel
- [📋 Copy Link] click → `copyShareLink()`
- [📎 Paste Peer] click → `pastePeerAddress()`
- [📷 Scan Peer] click → `scanQR()`
- [Logout] click → `logout()`
- `beforeunload` / `pagehide` events → send `tab_closed` WebSocket message for any open files

#### WebSocket Reconnection

On `onclose`/`onerror`: attempt reconnect with exponential backoff (1s, 2s, 4s, 8s, max 30s). Show "Reconnecting…" in health indicator.

---

### 3.20 `app.py` — Main Entry Point

**Purpose**: Parse CLI args, initialise all modules, wire them together, launch TUI and/or web server.

#### Class: `App`

The central orchestrator. All modules reference each other through this.

| Field | Type | Description |
|---|---|---|
| `data_dir` | `str` | `~/.decentralised-web` |
| `node_identity` | `NodeIdentity` | This node's random identity |
| `author_identity` | `AuthorIdentity | None` | Logged-in author (None if not logged in) |
| `author_mode` | `str` | `"full"` or `"browse_only"` — set during login based on remote login rules |
| `MIN_PUBLISH_BYTES` | `int` (class constant) | `1048576` (1MB) — minimum free storage required on a remote node to allow publishing by a new author |
| `udp_engine` | `UDPEngine` | Network layer |
| `protocol_router` | `ProtocolRouter` | Message dispatcher |
| `reliable` | `ReliabilityManager` | ACK/retransmit |
| `peer_book` | `PeerBook` | Peer directory |
| `file_registry` | `FileRegistry` | File registry |
| `storage` | `StorageManager` | Disk storage |
| `replication` | `ReplicationManager` | Rebalancing |
| `tui` | `TUI | None` | Terminal UI (None if `--no-tui`) |
| `web_app` | `Flask | None` | Flask app (None if `--web-port 0`) |
| `event_bus` | `EventBus` | Simple pub/sub for WebSocket notifications |

| Method | Signature | Input | Output | Description |
|---|---|---|---|---|
| `__init__(args: argparse.Namespace)` | `(Namespace) -> None` | Parsed CLI args | — | 1. Resolve `data_dir`. 2. Apply `tui_port_offset` to `port` and `web_port`. 3. Load/create `NodeIdentity`. 4. Derive `AuthorIdentity` if credentials provided. 5. Initialise all modules in dependency order. 6. Wire cross-references. 7. Register all protocol handlers. |
| `start()` | `() -> None` | — | — | 1. Start UDP engine. 2. Run startup/reconnection sequence (§9b). 3. If `--no-tui` is false, launch TUI. 4. If `--web-port > 0`, launch Flask in a thread. 5. Enter main event loop. |
| `stop()` | `() -> None` | — | — | Graceful shutdown: stop UDP engine, stop Flask, close DBs. |
| `login(username: str, password: str)` | `(str, str) -> AuthorIdentity` | Credentials | Author identity | 1. Derive `AuthorIdentity`. 2. Store in `self.author_identity`. 3. Check remote login policy: (a) if author has existing files in local registry → this is a "home" or "returning" login; (b) if no local files, check `file_registry.get_by_author(author_id)` — if any exist AND at least one peer hosting those files is currently connected (intersect with `udp_engine.get_connected_peers()`), author has contributed and is online; (c) if author has never published OR no previous node is online → check `storage.available_bytes() >= MIN_PUBLISH_BYTES`, if yes allow publishing (remote node hosts), else set `self.author_mode = "browse_only"`. 4. Return identity. |
| `author_can_publish()` | `() -> bool` | — | `True` if author can publish new files | Returns False only if `author_mode == "browse_only"`. Publishers on remote nodes are allowed if author has contributed before OR the remote node has spare storage to host a file. |
| `publish_file(file_path_or_data, file_name, mime_type)` | `(str|bytes, str, str) -> str` | File path or raw bytes, name, MIME type | `file_id` | 1. Read file data. 2. Generate `file_id = SHA-256(data + author_id + timestamp)`. 3. Store on disk. 4. Sign with author key. 5. Build `file_publish` → broadcast. 6. Add to local registry. 7. Emit `file_update` event. |
| `download_file(file_id: str)` | `(str) -> bytes` | `file_id` | File content | Delegate to `udp_engine.download_file()`. On success: store replica, announce, emit event. |
| `open_file(file_id: str)` | `(str) -> bytes` | `file_id` | File content | Same as download but with different event emission for UI tracking. |
| `update_file(file_id: str, new_data: bytes)` | `(str, bytes) -> str` | Existing file_id, new file data | New `file_id` | 1. Verify author matches. 2. Generate new file_id. 3. Build `file_update` → broadcast. 4. Update registry. |
| `delete_file(file_id: str)` | `(str) -> None` | `file_id` | — | 1. Verify author matches. 2. Build `file_delete` → broadcast. 3. Mark deleted in registry. 4. Delete from disk. |
| `create_share_link(file_id: str)` | `(str) -> str` | `file_id` | Share URL | 1. Send `SHARE_FILE_QUERY` to a connected peer. 2. Receive `SHARE_FILE_RESPONSE` with `suggested_peers` (node_ids). 3. For each node_id, resolve address via `peer_book.get(node_id)`. 4. Generate URL with node info + file hash + up to 3 peers (longest uptime, now resolved to ip:port). |
| `connect_to_peer(node_id: str, pubkey_b64: str, ip: str, port: int)` | `(str, str, str, int) -> bool` | Peer details | Success | Delegate to `udp_engine.hole_punch()`. |
| `connect_via_url(url: str)` | `(str) -> bool` | Connection URL | Success | Parse URL parameters → `connect_to_peer()`. |

#### CLI Argument Parsing (argparse)

```python
parser = argparse.ArgumentParser(description="Decentralised File Storage Network")
parser.add_argument('--user', '-u', default=os.environ.get('DECWEB_USER'), help='Author username')
parser.add_argument('--pass', '-p', dest='password', default=os.environ.get('DECWEB_PASS'), help='Author password')
parser.add_argument('--port', '-P', type=int, default=9000, help='UDP listen port')
parser.add_argument('--no-tui', action='store_true', help='Disable terminal UI')
parser.add_argument('--web-port', type=int, default=9001, help='Web UI port (0 = disable)')
parser.add_argument('--web-host', default='127.0.0.1', help='Web UI bind address')
parser.add_argument('--data-dir', default=os.path.expanduser('~/.decentralised-web'), help='Data directory')
parser.add_argument('--storage-limit', type=int, default=500, help='Max storage in MB for replicas')
parser.add_argument('--no-lan', action='store_true', help='Disable LAN broadcast discovery')
parser.add_argument('--tui-port-offset', type=int, default=0, help='Offset added to all ports for multi-instance testing')
```

#### `EventBus` class (simple pub/sub)

```python
class EventBus:
    def __init__(self):
        self._subscribers = defaultdict(set)

    def subscribe(self, event_type: str, callback: Callable):
        self._subscribers[event_type].add(callback)

    def unsubscribe(self, event_type: str, callback: Callable):
        self._subscribers[event_type].discard(callback)

    def emit(self, event_type: str, **data):
        for cb in self._subscribers.get(event_type, set()):
            cb(data)
```

Events emitted: `peer_connected`, `peer_disconnected`, `file_added`, `file_updated`, `file_deleted`, `storage_changed`, `health_changed`, `download_progress`.

#### `main()` function

```python
def main():
    args = parser.parse_args()
    app = App(args)
    try:
        app.start()
    except KeyboardInterrupt:
        app.stop()
```

---

## 4. Data Flow Diagrams

### 4.1 Message Reception Flow

```
UDP Socket recvfrom()
    │
    ▼
wire.decode(raw_bytes) → WireMessage
    │
    ▼
protocol_router.route(wire_msg, from_addr)
    │
    ├── Is ACK message? → reliable.ack_received()
    │
    ├── Requires ACK? → build & send ACK via udp_engine.send_to()
    │
    ├── Duplicate? (reliable.is_duplicate) → drop
    │
    └── MessageParser.<type>(payload) → typed payload
        │
        ▼
    Registered handler (e.g. _handle_file_publish)
        │
        ├── Verify signature (if applicable)
        ├── Update file_registry
        ├── Update peer_book (if applicable)
        ├── Propagate via gossip (broadcast_except)
        └── Emit event to EventBus → WebSocket → browser UI
```

### 4.2 File Download Flow

```
Browser: click [Download]
    │
    ▼
WebSocket: { type: "download", file_id: "abc123" }
    │
    ▼
App.download_file("abc123")
    │
    ▼
FileRegistry.get("abc123") → find replica host (e.g. "d4e5f6")
    │
    ▼
UDPEngine.download_file("abc123")
    │
    ├── Send FILE_REQUEST to "d4e5f6"
    │
    └── Create pending_downloads["abc123"] = DownloadState
        │
        ▼ (chunks arrive asynchronously)
    _handle_file_chunk()
        │
        ├── Send FILE_CHUNK_ACK
        ├── Store chunk in DownloadState.received[chunk_index]
        └── If all chunks received:
            ├── Reassemble → verify SHA-256
            ├── Storage.store_temporary_replica(file_id, data, tab_id)
            ├── Set DownloadState.download_complete Event
            ├── FileRegistry.increment_replica("abc123", own_node_id)
            ├── Broadcast FILE_ANNOUNCE (is_temporary=True)
            └── Emit event → WebSocket → browser
                │
                ▼
            Browser receives download_progress: { status: "complete" }
            → Opens file or triggers download
```

### 4.3 File Publish Flow

```
Browser: select file → [Upload]
    │
    ▼
POST /api/files/upload (multipart)
    │
    ▼
App.publish_file(data, file_name, mime_type)
    │
    ├── file_id = SHA-256(data + author_id + timestamp)
    ├── Storage.store_own_file(file_id, data)
    ├── Sign: author_identity.sign(file_id + file_name + ...)
    ├── Build FilePublishPayload
    │
    ├── FileRegistry.add(entry)  ← local
    │
    └── UDPEngine.broadcast(FILE_PUBLISH, payload)
        │
        ▼ (peers receive)
    _handle_file_publish()
        ├── Verify author_identity.verify(payload, signature, pubkey)
        ├── FileRegistry.add(entry)
        └── Gossip to other peers
```

### 4.4 Reconnection Sequence Flow

```
App.start()
    │
    ├── Phase 0: PeerBook.get_all_ordered()
    │   └── If empty → use hardcoded bootstrap peers
    │
    ├── Phase 1: For each Tier 1 peer (max 10 concurrent):
    │   └── UDPEngine.hole_punch(node_id, ip, port)
    │       ├── Success → mark CONNECTED, update peer_book
    │       └── Fail → record_failure, if ≥3 connected peers have
    │           this target → peer_assisted_connect()
    │
    ├── Phase 2: For each connected peer:
    │   └── Send FILE_REGISTRY_QUERY
    │       └── On FILE_REGISTRY_RESPONSE:
    │           ├── FileRegistry.merge_delta(entries)
    │           └── Replication.receive_target_estimate(estimated_network_target)
    │
    ├── Phase 3: Replication.open_gate()
    │   └── Replication.execute_rebalance()
    │
    ├── Phase 4: Try Tier 2, then Tier 3 peers
    │   └── Send PEER_LIST_REQUEST to connected peers
    │       └── On PEER_LIST_RESPONSE → merge into peer_book
    │
    ├── Phase 5: UDPEngine.lan_broadcast() (unless --no-lan)
    │
    ├── Phase 6: Start all periodic tasks (full list in §5 step 8):
    │   ├── keepalive_ping (every 30s)
    │   ├── retransmit_check (every 100ms)
    │   ├── rebalance_periodic (every 60s)
    │   ├── liveness_check (every 30s)
    │   ├── temporary_replica_cleanup (every 300s)
    │   ├── old_version_gc (every 1800s)
    │   ├── lan_broadcast_periodic (every 30s)
    │   └── peer_book_cleanup (every 3600s)
```

---

## 5. Startup & Reconnection Sequence

Detailed as pseudocode in `App.start()`:

```
1. LOAD identity: NodeIdentity.load_or_create(data_dir)
2. DERIVE author identity if --user and --pass provided
3. INITIALISE modules:
   a. peer_book = PeerBook(data_dir)
   b. file_registry = FileRegistry(data_dir, node_id)
   c. storage = StorageManager(data_dir, storage_limit_mb)
   d. reliable = ReliabilityManager()
   e. replication = ReplicationManager(file_registry, storage, peer_book, udp_engine)
   f. protocol_router = ProtocolRouter(node_id, ...)
   g. udp_engine = UDPEngine(port, node_identity, ...)
4. REGISTER all protocol handlers on protocol_router
5. START udp_engine (STUN lookup + recv thread)
6. RECALCULATE peer_book tiers
7. RECONNECTION:
   a. peers = peer_book.get_all_ordered()
   b. if not peers: peers = BOOTSTRAP_PEERS  # hardcoded
      # Also use bootstrap peers if all tier connection attempts fail
      # (tracked via a flag set if Phase 1/4 yield no connections)
   c. PHASE 1: parallel hole punch to Tier 1 peers
   d. wait for at least 1 connection or all attempts exhausted
   e. PHASE 2: FILE_REGISTRY_QUERY to all connected
   f. wait for all responses (timeout 10s)
   g. PHASE 3: replication.open_gate() + execute_rebalance()
   h. PHASE 4: connect to Tier 2, then Tier 3
   i. request PEER_LIST_RESPONSE from connected peers
   j. PHASE 5: lan_broadcast()
8. START periodic tasks:
   a. keepalive_ping() every 30s
   b. retransmit_check() every 100ms (reliable.get_expired())
   c. rebalance_periodic() every 60s:
      1. replication.calculate_network_target() — compute own estimate
      2. replication.execute_rebalance() — evaluate and act
   d. lan_broadcast_periodic() every 30s if < 2 connected peers
   e. peer_book_cleanup() every 3600s
   f. liveness_check() every 30s (check for dead peers, trigger disconnect cleanup)
   g. temporary_replica_cleanup() every 300s (promote expired temporary replicas)
   h. old_version_gc() every 1800s (remove old file versions)
9. LAUNCH UI:
   a. if not --no-tui: TUI.run()
   b. if --web-port > 0: Flask in thread
10. WAIT for shutdown signal (KeyboardInterrupt or TUI quit)
11. STOP: udp_engine.stop(), close DBs, exit
```

---

## 6. File Lifecycle

### State Machine

```
                    ┌─────────┐
                    │  NONE   │  (file not known to this node)
                    └────┬────┘
                         │ file_publish received / user uploads
                         ▼
                    ┌─────────┐
                    │PUBLISHED│  (in registry, may or may not be stored locally)
                    └────┬────┘
                         │ file_update received (signed by author)
                         ▼
                    ┌─────────┐
                    │ UPDATED │  (new file_id, linked via previous_file_id)
                    └────┬────┘
                         │ file_delete received (signed by author)
                         ▼
                    ┌─────────┐
                    │ DELETED │  (is_deleted=1, kept for tombstone period)
                    └─────────┘
```

### Replica States (Local Storage)

```
                    ┌──────────┐
                    │NOT STORED│
                    └────┬─────┘
                         │ user downloads/opens, or replication solicits, or rebalancing
                         ▼
                    ┌──────────┐
                    │TEMPORARY │  (opened/viewed by user, tab still open)
                    └────┬─────┘
                         │ tab closed / timeout
                         ▼
                    ┌──────────┐
                    │ REGULAR  │  (permanent replica, subject to rebalancing)
                    └────┬─────┘
                         │ rebalancing: over-replicated
                         ▼
                    ┌──────────┐
                    │ DELETED  │  (removed from disk, replica_count decreased)
                    └──────────┘
```

---

## 7. Error Handling Strategy

| Layer | Error | Handling |
|---|---|---|
| **wire.py** | Malformed packet (< 19 bytes) | Log warning, drop packet |
| **wire.py** | Payload length mismatch | Log warning, drop packet |
| **wire.py** | Unknown message type | Log warning, drop (forward-compatible) |
| **reliable.py** | ACK timeout (5 retries) | Call `on_retry_failed` callback, log error |
| **reliable.py** | Duplicate sequence number | Silently drop |
| **stun.py** | All STUN servers timeout | Raise `StunError`. App logs error, uses local IP. Network limited to LAN. |
| **udp_engine.py** | Hole punch fails after 5 attempts | Mark `direct_blocked = True`. Try peer-assisted if available. If also fails, mark peer unreachable. |
| **udp_engine.py** | `sendto()` raises `OSError` | Log error, mark peer as potentially disconnected |
| **udp_engine.py** | Chunk transfer interrupted | Clean up partial file, re-request from different peer |
| **file_registry.py** | Signature verification fails | Log warning, reject entry, don't propagate |
| **file_registry.py** | Timestamp conflict (equal timestamps) | Keep existing entry (first-write-wins) |
| **storage.py** | Disk full | Log error, refuse new replicas, alert UI |
| **storage.py** | File not found on disk but in registry | Remove from local replica list, decrement count |
| **storage.py** | SHA-256 mismatch after download | Delete corrupted file, re-download from different peer |
| **peer_book.py** | SQLite error | Log, continue with in-memory cache |
| **tui.py** | Terminal resize | Rich handles automatically via `Live` |
| **web/routes.py** | Invalid login | Return 401 with error message |
| **web/ws.py** | WebSocket disconnect | Attempt reconnect with backoff (client-side) |
| **app.py** | Unhandled exception in recv loop | Log exception, continue loop (don't crash) |
| **app.py** | Fatal error on startup | Log, exit with code 1 |

---

## Appendix A: Bootstrap Peer Configuration

```python
BOOTSTRAP_PEERS = [
    # Format: (node_id, public_key_base64, ip, port)
    # These would be maintained by the project maintainers
    # For development: empty list (rely on LAN broadcast + QR)
]
```

---

## Appendix B: Constants Summary

| Constant | Value | Used In |
|---|---|---|
| `PROTOCOL_VERSION` | `0x01` | `wire.py` |
| `PBKDF2_SALT` | `b"decentralised-web-v1"` | `identity.py` |
| `PBKDF2_ITERATIONS` | `600_000` | `identity.py` |
| `MAX_CHUNK_SIZE` | `16384` (16KB) | `udp_engine.py` |
| `ACK_TIMEOUT` | `0.5` (500ms) | `reliable.py` |
| `MAX_RETRIES` | `5` | `reliable.py` |
| `HOLE_PUNCH_PACKETS` | `3` | `udp_engine.py` |
| `HOLE_PUNCH_INTERVAL` | `0.1` (100ms) | `udp_engine.py` |
| `HOLE_PUNCH_TIMEOUT` | `5.0` (5s) | `udp_engine.py` |
| `KEEPALIVE_INTERVAL` | `30.0` (30s) | `udp_engine.py` |
| `PEER_TIMEOUT` | `90.0` (90s, 3 missed pings) | `connection.py` |
| `DEFAULT_PORT` | `9000` | `app.py` |
| `DEFAULT_WEB_PORT` | `9001` | `app.py` |
| `DEFAULT_STORAGE_MB` | `500` | `storage.py` |
| `MIN_PUBLISH_BYTES` | `1048576` (1MB) | `app.py` |
| `REBALANCE_INTERVAL` | `60.0` (60s) | `replication.py` |
| `PEER_BOOK_CLEANUP_DAYS` | `30` | `peer_book.py` |
| `MAX_CONCURRENT_HOLE_PUNCH` | `10` | `udp_engine.py` |
| `LAN_BROADCAST_INTERVAL` | `30.0` | `udp_engine.py` |
| `TIER_2_RECENT_DAYS` | `7` | `peer_book.py` |
| `CONSECUTIVE_FAILS_DEMOTE` | `5` | `peer_book.py` |

---

## Appendix C: Connection URL Format

Connection URLs encode enough information for a peer to initiate a UDP hole punch to another peer. The `<bootstrapper>` part is the HTTP address of any known node's web UI — it is the entry point a browser visits to display the join page.

**Format:**
```
https://<any-known-peer-ip>:<web-port>/?join=<nodeId>&pk=<base64PublicKey>&addr=<publicIP>:<publicPort>
```

- `<any-known-peer-ip>:<web-port>`: The web UI address of any peer in the network (e.g., `192.168.1.5:9001`). This is just where the browser loads the page from — it does NOT need to be the peer being joined. The actual P2P connection target is in the `addr` parameter. Note: in development, Flask uses HTTP (`http://`) since SSL is not configured; the scheme adapts to the deployment environment.
- `nodeId`: 16-char hex node ID of the peer to connect to
- `pk`: Base64-encoded 32-byte Ed25519 public key of the peer (no padding, URL-safe)
- `addr`: `ip:port` of the peer — this is the actual UDP hole-punch target

**How it works:**
1. Alice generates her connection URL (with her own `nodeId`, `pk`, and `addr`)
2. Alice encodes it as a QR code or sends the URL to Bob
3. Bob's browser loads the URL → his local Python backend parses the parameters → initiates UDP hole punch to `addr`
4. The `<bootstrapper>` host is just the HTTP entry point — any online peer can serve this role
5. For QR codes shown in the web UI, `<bootstrapper>` is the local node's own web address

Share link extension:
```
...&file=<fileId>&hash=<fileHash>&peers=<p1>,<p2>,<p3>
```

---

## Appendix D: Threading Model

```
Main Thread:
├── TUI render loop (if not --no-tui)
│
├── UDP Recv Thread: udp_engine._recv_loop()
│   └── Calls protocol_router.route() for each message
│       └── Handlers run on this thread (must be thread-safe)
│
├── Keepalive Timer Thread: every 30s
│   └── Sends PING to all connected peers
│
├── Retransmit Timer Thread: every 100ms
│   └── reliable.get_expired() → retransmit
│
├── Rebalance Timer Thread: every 60s
│   └── replication.execute_rebalance()
│
├── Liveness Check Timer Thread: every 30s
│   └── udp_engine.check_liveness()
│
├── Temporary Replica Cleanup Timer: every 300s
│   └── storage.cleanup_expired_temporary()
│
├── Old Version GC Timer: every 1800s
│   └── file_registry.cleanup_old_versions()
│
├── Peer Book Cleanup Timer: every 3600s
│   └── peer_book.cleanup()
│
├── LAN Broadcast Timer: every 30s (if < 2 peers)
│   └── udp_engine.lan_broadcast()
│
└── Flask Thread (if --web-port > 0):
    └── HTTP request handling + WebSocket
```

**Thread safety notes**:
- `file_registry.entries` (in-memory dict) protected by `threading.RLock`
- `peer_book` SQLite connection: use WAL mode, each thread gets its own connection, or use a single connection with `threading.Lock`
- `reliable.pending_acks`: `threading.Lock`
- `udp_engine.connections`: `threading.RLock`
- EventBus: `threading.Lock` on subscriber sets

---

*End of Implementation Plan*
