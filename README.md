# Decentralised Web — Peer-to-Peer File Storage Network

A self-organizing, decentralised file storage network. Each peer runs a Python process with a terminal UI and optional web UI. Files are replicated dynamically across the network with automatic rebalancing.

## Project Structure

```
Decenterlised-Web/
├── Server/                     # Source code
│   ├── app.py                  # Entry point — CLI, wiring, lifecycle
│   ├── config.py               # All tunable parameters in one place
│   ├── scheduler.py            # Unified priority action scheduler
│   ├── udp_engine.py           # Thin UDP socket layer (recv + send)
│   ├── protocol.py             # Binary message types, builder, parser
│   ├── reliable.py             # Sequence numbers, ACKs, retransmit
│   ├── identity.py             # Node + Author Ed25519 keypairs
│   ├── peer_book.py            # SQLite peer directory + connection state
│   ├── file_registry.py        # SQLite file registry + transfer tracking
│   ├── storage.py              # Disk file store + quota management
│   ├── replication.py          # Rebalancing, diversity, target calculation
│   ├── connection.py           # Compatibility shim (state moved to peer_book)
│   ├── wire.py                 # Binary wire format encode/decode
│   ├── stun.py                 # STUN client for public address discovery
│   ├── log_utils.py            # Logging + UDP trace helpers
│   ├── tui.py                  # Rich-based terminal UI
│   ├── web/                    # Flask web UI (optional)
│   │   ├── __init__.py         # App factory
│   │   ├── routes.py           # HTTP routes
│   │   ├── ws.py               # WebSocket handler
│   │   ├── templates/          # Jinja2 templates
│   │   └── static/             # CSS, JS
│   └── requirements.txt
├── Tests/                      # Test suite
│   ├── test_integration.py     # 89 integration tests (15 scenarios)
│   └── test_tui.py             # TUI render & interaction tests
├── Docs/                       # Design documents
│   ├── plan-decentralizedFileStorageNetwork.prompt.md
│   └── implementation-plan.md
└── README.md                   # This file
```

## Quick Start

```bash
# Install dependencies
cd Server
pip install -r requirements.txt

# Run a node (TUI mode)
python app.py -u alice -p secret

# Run headless
python app.py -u alice -p secret --no-tui

# Run with web UI on port 9001
python app.py -u alice -p secret --web-port 9001 --web-host 0.0.0.0

# Run two instances locally for testing
python app.py -u alice -p secret --port 9000
python app.py -u bob -p pass  --port 9001 --tui-port-offset 1
```

## CLI Arguments

| Argument | Default | Purpose |
|---|---|---|
| `-u` / `--user` | `DECWEB_USER` env | Author username |
| `-p` / `--pass` | `DECWEB_PASS` env | Author password |
| `-P` / `--port` | `9000` | UDP listen port |
| `--no-tui` | `false` | Disable terminal UI |
| `--web-port` | `9001` | Web UI port (0 = disabled) |
| `--web-host` | `127.0.0.1` | Web UI bind address |
| `--data-dir` | `Server/data` | Data directory |
| `--storage-limit` | `500` (MB) | Max storage for replicas |
| `--no-lan` | `false` | Disable LAN broadcast |
| `--log` | — | Log filename (e.g. `app.log`) |
| `--udp-trace` | — | UDP hex-dump trace file |
| `--debug` | `false` | Scheduler queue debug logging |

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      Python Peer Process                          │
│                                                                   │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌────────────┐     │
│  │ Identity │  │ UDP Engine│  │  Peer    │  │   File     │     │
│  │ (Ed25519)│  │ (thin     │  │  Book    │  │   Store    │     │
│  │ KDF user │  │  socket)  │  │ (SQLite) │  │  (on disk) │     │
│  │ + pass)  │  └─────┬─────┘  └──────────┘  └────────────┘     │
│  └──────────┘        │                                           │
│                      │  ┌──────────────────────────────┐        │
│  ┌──────────┐  ┌─────┴──┴──────┐  ┌──────────┐        │        │
│  │  File    │  │   Scheduler   │  │ Storage  │        │        │
│  │ Registry │  │ (priority     │  │ Manager  │        │        │
│  │ (SQLite) │  │  heap)        │  │ (quota)  │        │        │
│  └──────────┘  └───────────────┘  └──────────┘        │        │
│                                                         │        │
│  ┌──────────────────────────────────────────────────┐   │        │
│  │  Terminal UI (rich)            [--no-tui]        │   │        │
│  │  Peers: 4 online  Files: 12  Storage: 45%       │   │        │
│  └──────────────────────────────────────────────────┘   │        │
│                                                         │        │
│  ┌──────────────────────────────────────────────────┐   │        │
│  │  Flask HTTP + WebSocket  (port 9001, optional)   │   │        │
│  │  Dark-mode web UI with auth, upload, QR codes    │   │        │
│  └──────────────────────┬───────────────────────────┘   │        │
└─────────────────────────┼───────────────────────────────┘
                          │ HTTP + WebSocket
┌─────────────────────────┴───────────────────────────────┐
│  Browser Frontend  (login with author credentials)       │
│  File List │ QR Code │ Upload │ Settings │ Storage      │
└─────────────────────────────────────────────────────────┘
```

## How It Works

### Identity (Two Layers)

| Layer | Derivation | Persisted | Used For |
|---|---|---|---|
| **Node ID** | Random Ed25519 keypair | `node_identity.json` | P2P protocol messages, peer discovery |
| **Author ID** | PBKDF2(username+password) → Ed25519 | In-memory (session) | Signing file operations (publish, update, delete) |

Same credentials on any device = same author identity. Node identity is per-installation.

### Peer Discovery

1. **QR Code** — Scan another peer's QR to connect instantly
2. **LAN Broadcast** — Auto-discover peers on the same network
3. **Bootstrap Peers** — Hardcoded well-known peers for initial join
4. **Gossip** — Exchange peer lists with connected peers

### UDP Transport

- Raw UDP sockets — no WebRTC ceremony
- **Hole punching** via STUN (`stun.l.google.com:19302`)
- **Peer-assisted connection** for symmetric NAT fallback (~15% of NATs)
- **Keepalive** pings every 30s to refresh NAT mappings
- **Reliability layer**: sequence numbers, ACKs, exponential-backoff retransmit (up to 5 retries)

### Wire Format

```
[1B version][2B msg_type][8B sender_id][4B payload_len][4B seq_num][payload]
```

19-byte header. Compact binary encoding — not JSON.

### File Operations

- **Publish**: `SHA-256(data + authorId + timestamp)` as fileId, signed by author
- **Download**: Chunked transfer with ACK per chunk (max 8KB/chunk), integrity verified
- **Update**: New fileId linked to previous via `previous_file_id` chain
- **Delete**: Author-signed, removes from registry + local storage

### Replication & Rebalancing

- **Network target**: `floor(Σ storage / Σ unique_file_size)`, gossiped, median-merged
- **±1 tolerance band** around target prevents thrashing
- **Diversity prioritisation**: prefer files hosted by peers not already in your peer set
- **Rebalancing gate**: don't act until connected to ≥3 peers or all Tier 1 contacted
- **Originator protection**: file publisher always keeps their own files

### Scheduler Architecture

All periodic work runs through a single priority heap instead of scattered timer threads:

| Priority | Examples |
|---|---|
| **CRITICAL (0)** | HELLO reply, send chunk, connect introduce |
| **HIGH (1)** | Retransmit check, chunk ACK check, ping response check |
| **NORMAL (2)** | Rebalance, registry exchange, hole punch, peer list |
| **LOW (3)** | Keepalive pings, cleanup, GC, liveness check |

The scheduler is fully configurable via `Server/config.py`.

## Running Tests

```bash
# Integration tests (89 tests, 15 scenarios)
python Tests/test_integration.py
python Tests/test_integration.py --verbose
python Tests/test_integration.py --keep-dirs

# TUI tests
python Tests/test_tui.py
```

## Configuration

All tunable parameters in `Server/config.py`:

```python
from config import PeerConfig

config = PeerConfig(
    keepalive_interval=30.0,        # PING interval (seconds)
    ping_response_timeout=2.0,      # wait for PING response
    peer_timeout=90.0,              # 3 missed pings = dead
    ack_timeout_base=0.5,           # initial retransmit timeout
    ack_timeout_max=4.0,            # max after backoff
    max_retries=5,                  # max retransmit attempts
    rebalance_interval=60.0,        # rebalance cycle
    hole_punch_packets=3,           # hello packets per punch
    hole_punch_timeout=5.0,         # wait for hello response
    max_chunk_size=8192,            # bytes per UDP datagram
    storage_limit_mb=500,           # max disk usage
    # ... 25+ parameters total
)
```

## Dependencies

- Python 3.10+
- `cryptography` — Ed25519 keypairs, PBKDF2 key derivation
- `rich` — Terminal UI
- `flask` + `flask-sock` — Web UI (optional)

No external P2P libraries. Raw UDP sockets only.

## Scope

- ✅ UDP hole punching + peer-assisted fallback
- ✅ Dynamic replication with diversity-aware rebalancing
- ✅ Terminal UI + optional web UI
- ✅ Deterministic author identity (portable across devices)
- ✅ Gossip-based registry sync (eventual consistency)
- ✅ Configurable scheduler (priority heap, 20+ action types)
- ❌ Encryption at rest / in transit (plain UDP)
- ❌ TURN relay for symmetric NAT edge cases
- ❌ Economic incentives / proof-of-storage
- ❌ Malicious node detection
