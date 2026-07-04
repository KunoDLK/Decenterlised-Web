# Decentralized File Storage Network (Python + UDP Hole Punching)

**TL;DR** — A self-organizing decentralized storage network. Each peer runs a Python process with a terminal UI (peer list, file search, status) and optionally a web UI on a separate port. Peers discover each other via single-scan QR codes, broadcast UDP (LAN), or hardcoded bootstrap peers. Identity is derived from a username + password (deterministic keypair — same credentials on any device = same identity). Files replicated dynamically.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                    Python Peer Process                        │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌────────────┐ │
│  │ Identity │  │ UDP Engine│  │ Peer     │  │ File       │ │
│  │(KDF from │  │ (P2P)     │  │ Book     │  │ Store      │ │
│  │user+pass)│  └─────┬─────┘  │ (SQLite) │  │(on disk)   │ │
│  └──────────┘        │         └──────────┘  └────────────┘ │
│                      │                                        │
│  ┌──────────┐  ┌─────┴─────┐  ┌──────────┐                  │
│  │ File     │  │ Protocol  │  │ Storage  │                  │
│  │ Registry │  │ Router    │  │ Manager  │                  │
│  └──────────┘  └───────────┘  └──────────┘                  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Terminal UI (default)         [--no-tui to disable] │   │
│  │  ┌────────────────────────────────────────────────┐  │   │
│  │  │  Peers: 4 online    Files: 12    Storage: 45%  │  │   │
│  │  │  > search: photo                                │  │   │
│  │  │  📄 photo.jpg  2.4MB  👁 3  author: alice      │  │   │
│  │  │  🖼️  photo2.png 1.2MB  👁 5  author: bob       │  │   │
│  │  └────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Flask HTTP Server (optional, port configurable)     │   │
│  │  ┌──────────────┐  ┌──────────────────────────────┐  │   │
│  │  │ Static HTML  │  │ WebSocket (real-time UI)     │  │   │
│  │  │ /CSS/JS      │  │ /ws  (admin auth required)   │  │   │
│  │  └──────────────┘  └──────────────────────────────┘  │   │
│  └──────────────────────┬───────────────────────────────┘   │
└─────────────────────────┼───────────────────────────────────┘
                          │ HTTP + WebSocket (separate port)
┌─────────────────────────┴───────────────────────────────────┐
│  Browser Frontend  (dark mode UI, login with admin creds)    │
│  UI: File List │ QR Code │ Upload │ Settings │ Storage      │
└─────────────────────────────────────────────────────────────┘
```

**Dependencies:** Python 3.10+, Flask, flask-sock (WebSocket), `cryptography` (Ed25519 + KDF), `rich` (terminal UI). No external P2P libraries — raw UDP sockets.

### 1. Identity (Two Separate Layers)

#### 1a. Node Identity (Peer ID — Random)

Each Python process has its own **node identity**: a random Ed25519 keypair generated on first run, persisted to `~/.decentralised-web/node_identity.json`. This keypair:
- Identifies the node on the P2P network (Node ID = first 16 chars of SHA-256 of public key)
- Signs protocol-level messages (`hello`, `file_announce`, `peer_list_response`, etc.)
- Has **no relationship** to who is operating the node or what files it hosts
- Regenerated if the identity file is deleted (node gets a new ID)

This is the identity other peers see: "node a1b2c3 is online, uptime 2h, hosting files X, Y, Z."

#### 1b. File Author Identity (Username + Password — Deterministic)

A separate identity derived from **username + password** via PBKDF2-HMAC-SHA256 (600K iterations, salt `decentralised-web-v1`) seeding an Ed25519 keypair. This:
- Is the same on any device — log in with `-u alice -p secret` anywhere, get the same author identity
- Signs **file operations only**: `file_publish`, `file_update`, `file_delete`
- Receivers verify signatures against the author's public key to confirm authorship
- Has **no relationship** to the node identity — alice can publish files from any node
- Author public key is included in file registry entries so anyone can verify

#### 1c. Web UI Authentication (Username + Password — Same as Author)

The web UI login form accepts the same username + password. On successful login:
- The browser is authenticated to manage the server (upload files, trigger downloads, configure storage)
- File operations performed via the web UI are signed with the **author's** derived keypair, not the node's key
- This means: log into a remote node's web UI as alice → upload file → it's signed as alice → network recognises alice as the author → later, alice can push an update from any other node

#### 1d. Remote Login Rules

When a user logs into a **remote node** (a node whose local node identity is not their own):

**Publishing new files** — allowed only if:
- At least one node where the user previously logged in is currently **online** (the user is contributing to the network), OR
- The remote node will host the file (it becomes the contributor on the user's behalf)

**Modifying / deleting files** — always allowed:
- The user can prove authorship via their Ed25519 signature on the update/delete message
- The operation is valid regardless of which node initiates it
- If the user's home node was offline and comes back, it queries the network, discovers the file was updated, and syncs the latest version. No files are ever frozen.

**Browse-only fallback:** If the user has never contributed (0 files on network) AND the remote node has no spare storage to host a new file, the login succeeds but only browse/download mode is available.

**Summary of the three identities:**

| Identity | How derived | Persisted | Used for |
|---|---|---|---|
| **Node ID** | Random Ed25519 | `node_identity.json` | P2P protocol messages, peer discovery |
| **Author ID** | PBKDF2(username+password) → Ed25519 | In-memory only (session) | Signing file operations (publish, update, delete) |
| **Web auth** | Same username+password | Flask session cookie | Authenticating to web UI to manage server |

### 2. Peer Discovery

#### 2a. QR Code Discovery (Single Scan)
- Each node generates a connection URL: `https://<bootstrapper>/?join=<nodeId>&pk=<base64NodePublicKey>&addr=<publicIP>:<publicPort>`
- `nodeId` and `nodePublicKey` are the node's random peer identity (not the author identity)
- The `addr` parameter is the node's public IP:port (obtained via STUN — see §3b)
- URL encoded as QR code. ~150 bytes — easily scannable
- Another user scans the QR → browser sends the peer info to the local Python backend via WebSocket → backend initiates UDP hole punching to the target address
- **Single scan suffices**: the scanning peer sends the first UDP packet. The existing node was already listening on its known port. The existing node's reply completes the punchthrough. No two-phase handshake needed.

#### 2b. Broadcast LAN Discovery
- On startup, the node sends a UDP broadcast to `255.255.255.255:<port>` with its Node ID, public key, and local IP
- All peers on the same LAN receive it and respond directly (no NAT, no hole punching)
- Peers discovered via broadcast are auto-connected — no QR, no user action
- Broadcasts repeat every 30 seconds while the node has < 2 connected peers

#### 2c. Bootstrap Peers
- Hardcoded list of well-known bootstrap peer addresses: `(ip, port, nodeId, publicKey)`
- On startup, attempt UDP hole punch to each bootstrap peer
- Once connected to any peer, request their known peer list via gossip
- Bootstrap peers are just regular nodes that happen to be long-running. No special software needed.
- This enables **fully automatic joining** — if at least one bootstrap peer is online, a fresh node rejoins the network with zero user action
- QR codes become a fallback for when all bootstrap peers are offline

#### 2d. Gossip Peer Discovery
- Once connected to any peer, periodically request their known peer list
- Protocol message: `peer_list_request` → `peer_list_response`
- **Only share actively connected peers** — sharing stale addresses is pointless since hole punching requires both sides to be online
- `peer_list_response` returns array of `{ nodeId, publicIP, publicPort, uptimeSince }`
- Each node maintains a **peer book** (SQLite) of all known peers
- On startup, iterate through peer book and attempt hole punch to each
- Connection URLs use the same format as QR discovery: `https://<bootstrapper>/?join=<nodeId>&pk=<base64PublicKey>&addr=<ip>:<port>`
- Share links (Browse tab → Share button) extend this: append `&file=<fileId>&hash=<fileHash>&peers=<p1>,<p2>,<p3>`

### 3. UDP Transport

#### 3a. P2P over UDP
- Single UDP socket bound to `0.0.0.0:<port>` handles all peer communication
- No connection setup ceremony — just `sendto()` and `recvfrom()`
- Each peer identified by `(ip, port)` tuple + verified via Node ID in messages

#### 3b. Hole Punching Flow
1. On startup: query **STUN server** (`stun.l.google.com:19302`) to discover public IP:port
2. Store public address — this is what goes into QR codes and peer lists
3. To connect to a known peer at `(targetIP, targetPort)`:
   - Send 3 consecutive UDP packets to `(targetIP, targetPort)` with 100ms gaps
   - Payload: a `hello` message with own Node ID, public key, and address
   - Target receives at least one → reads source address from `recvfrom()` → sends reply
   - Reply arrives back through the punched hole → connection established
4. Keepalive: send a small ping every 30 seconds to keep NAT mapping alive5. If no reply after 5 seconds, the hole punch is recorded as a failure for this peer pair

#### 3c. Peer-Assisted Connection (Symmetric NAT Fallback)

~15% of NATs are symmetric — they assign a different port per destination, so the port learned via STUN differs from the port used to punch to a peer. Direct hole punching fails. The fallback: use a mutually connected peer as a coordinator.

```
A is connected to B. C is connected to B.
A cannot directly punch to C (A's NAT is symmetric).

A ──connect_request(C's nodeId)──► B
                                   B ──connect_introduce(A's nodeId, A's address)──► C
C ──connect_ack(B's nodeId)──────► B
B ──connect_ack(A's nodeId)──────► A

A and C now both try hole punching to each other simultaneously:
  (C is the initiator since A's NAT is the problematic one —
   C firing first opens C's NAT hole for A's reply)

C ──hello packet──► A (A's NAT drops this — symmetric)
A ◄──                           
A ──hello packet──► C (C's NAT has hole open from C's outbound → gets through!)
C ◄────────────────
       ...or vice versa. One gets through, then both ways work.

A ══════════ Direct UDP connection established ═══════════► C
                    (B is no longer involved)
```

**Decision logic for when to use peer-assisted connection:**
- After 3 hole punch attempts to a peer fail → mark the peer pair as "direct blocked"
- Query connected peers: "are you connected to target peer?"
- If yes → request peer-assisted introduction
- After introduction, both sides attempt mutual hole punching
- If peer-assisted also fails → peer pair is unreachable. Accept the loss (~5% of NAT combinations).
- On next startup, retry direct first. Only fall back to assisted if direct fails again.
#### 3d. Reliability Layer
UDP drops packets (~1% on healthy networks). A thin reliability layer sits above raw UDP:

- Every message has a **sequence number** (monotonically increasing per peer pair)
- Critical messages (file chunks, registry sync) require an **ACK**. Sender retransmits after 500ms if no ACK, up to 5 retries
- Non-critical messages (pings, gossip updates) are fire-and-forget — loss is acceptable
- Duplicate detection via sequence numbers — re-sent packets are silently dropped

#### 3e. Wire Format
All messages use a compact binary format (not JSON — smaller, faster to parse):

```
[1B protocol version] [2B message type] [8B sender nodeId prefix]
[4B payload length] [4B sequence number]
[payload bytes]
```

Total overhead: 19 bytes per message. Message types use the same names as the protocol table below, encoded as uint16 IDs.

### 4. Protocol Messages

All messages use the binary wire format from §3e. Message types are uint16 IDs. Payloads use a compact binary encoding (not JSON) — integer lengths, fixed-width fields, length-prefixed strings.

| Message Type | ID | Direction | Purpose |
|---|---|---|---|
| `hello` | 0x01 | A↔B | Initial hole-punch packet: nodeId, publicKey, node's public address, uptimeSince |
| `ping` | 0x02 | A↔B | Keepalive (every 30s). Also serves as hole-punch refresh |
| `peer_list_request` | 0x10 | A→B | "Give me your known peers" |
| `peer_list_response` | 0x11 | B→A | Array of `{ nodeId, publicIP, publicPort, uptimeSince }` + `estimatedNetworkTarget` (see §6a) |
| `file_registry_query` | 0x20 | A→B | "What files exist?" (reconnect health assessment) |
| `file_registry_response` | 0x21 | B→A | Array of `{ fileId, name, size, authorId, replicas: [nodeId,...], estimatedNetworkTarget }` |
| `file_registry_push` | 0x22 | A→B | Unsolicited update: replica count changed |
| `file_request` | 0x30 | A→B | "Send me file X" — includes fileId |
| `file_chunk` | 0x31 | B→A | `[4B fileIdLen][fileId][4B chunkIdx][4B totalChunks][data]` — binary chunk, max 16KB data. Requires ACK. |
| `file_chunk_ack` | 0x32 | A→B | "Received chunk N for file X" — triggers next chunk or retransmit |
| `file_announce` | 0x40 | A→B | "I have file X and I'm willing to serve it" |
| `replication_solicit` | 0x41 | A→B | "File X needs more replicas — can you mirror?" |
| `replication_ack` | 0x42 | B→A | "I'm replicating file X" |
| `file_publish` | 0x50 | A→B | "I'm publishing a new file" (signed by author) |
| `file_update` | 0x51 | A→B | "I'm updating my file" (signed by original author) |
| `file_delete` | 0x52 | A→B | "I'm deleting my file" (signed by original author) |
| `goodbye` | 0xFF | A→B | "I'm going offline" |
| `connect_request` | 0x60 | A→B | "B, tell C I want to connect. Here's my address." (symmetric NAT fallback) |
| `connect_introduce` | 0x61 | B→C | "A wants to connect. Here's A's address." (relayed by B) |
| `connect_ack` | 0x62 | C→B, B→A | "Acknowledged. Initiating mutual hole punch." |

Message types 0x1000-0x1FFF reserved for future use.

**Delivery guarantees:**
- `file_chunk` and `file_chunk_ack`: reliable (ACK + retransmit, up to 5 retries with 500ms timeout)
- All `registry_*` messages: reliable (ACK + retransmit)
- `hello`, `ping`, `goodbye`, gossip, `file_announce`: best-effort (loss acceptable, periodic resends cover gaps)

### 5. File Registry (Decentralized)

- Each node maintains a local file registry (in-memory + SQLite backup in `~/.decentralised-web/registry.db`)
- Registry entries: `{ fileId, fileName, fileSize, mimeType, authorId, authorPublicKey, replicaCount, authorSignature, replicas: [{ nodeId, addedAt }] }`
- **Gossip sync**: When connecting to a peer, exchange registry hashes → if different, exchange full or delta registry
- **Replica counting**: Nodes gossip `file_announce` messages. Other nodes increment/decrement their local `replicaCount` for that file. On peer disconnect, decrement counts for files that peer hosted.
- **Consensus**: No strict consensus — eventual consistency via gossip is acceptable for a demo. Conflict resolution: latest timestamp wins for updates to the same file.

### 6. Dynamic Replication Logic

#### 6a. Network-Wide Target Calculation

Each node continuously estimates the sustainable replica count from its partial view of the network:

```
networkTarget = floor(Σ contributingStorage / Σ uniqueFileSize)
```

This is gossiped in `peer_list_response` as `estimatedNetworkTarget`. Each node merges received estimates (median) to converge on a shared value.

**Rebalancing thresholds** (with ±1 tolerance band):
| Condition | Action |
|---|---|
| `replicaCount > networkTarget + 1` | Over-replicated → candidate for local deletion |
| `replicaCount == networkTarget ± 1` | Healthy → no action |
| `replicaCount < networkTarget - 1` | Under-replicated → candidate for replication |

The tolerance band ensures the network doesn't thrash — files can be ±1 from the target without triggering rebalancing, since files will never all fit neatly at exactly the same replica count.

#### 6b. Rebalancing Decision

When a node has spare storage capacity:
1. Scan registry for files where `replicaCount < networkTarget - 1` (truly under-replicated)
2. Among those, **prioritise files hosted by peers this node does NOT already share files with** (maximises diversity — see §6c)
3. If no under-replicated files exist but spare capacity remains: store files at `replicaCount == networkTarget - 1` (bottom edge of tolerance band) with same diversity preference

When a node's storage is full and it detects an under-replicated file:
1. Find locally stored files with `replicaCount > networkTarget + 1`
2. Delete the most over-replicated file (highest replicaCount first)
3. Replicate the under-replicated file in the freed space
4. If no over-replicated files exist but the under-replicated file is more vulnerable: delete a file at `replicaCount == networkTarget` to make room

**Originator protection**: The original publisher of a file always keeps it regardless of replica count.

#### 6c. Storage Diversity (Spare Capacity)

When allocating spare capacity beyond the fair-share obligation, nodes maximise network resilience by spreading replicas across diverse peer sets:

- **For each under-replicated file**, examine the set of peers already hosting replicas
- **Prefer files whose existing replica-holders have minimal overlap** with the node's current connected peers
- This ensures that if two random nodes go offline, the impact is spread across different files rather than concentrated — worst case: 1 file falls off the network, not many

In practice: when choosing which under-replicated file to mirror, compute a diversity score for each candidate file — lower overlap with current peer set = higher score. Pick the highest-scoring file.

#### 6d. Rebalancing Gate (Don't Act on Stale Data)

A node must NOT start rebalancing until it has an accurate picture of the network:
- Minimum connected peers before rebalancing: either 3+ peers, or all Tier 1 peers have been contacted (see §9 reconnection)
- After reconnection, wait until `file_registry_query` responses are received from all connected peers before evaluating which files to delete or replicate
- This prevents a node from incorrectly deleting files it thinks are over-replicated when it simply hasn't heard from all peers yet

### 7. Storage Manager

- Files stored on disk at `~/.decentralised-web/files/<fileId>`
- **Storage quota tracking**: Total configured storage (default: equal to total size of published files). Configurable in UI settings panel.
- **Storage breakdown**: 
  - `usedForOwnFiles`: size of files this node published
  - `usedForReplicas`: size of files replicated from others
  - `available`: remaining configured quota
- When `usedForReplicas` exceeds `totalConfigured - usedForOwnFiles`, trigger rebalancing
- **Fair contribution**: Enforced by the UI — a warning is shown if the user tries to configure less storage than their published files. For the tech demo, we assume honesty.
- Disk space is vastly larger than browser storage — practical limits are GBs not MBs. Default storage cap: 500MB.

### 8. File Operations

#### Publishing a file:
1. User selects file via the HTML frontend → uploaded to Python backend
2. Backend reads file, generates `fileId = SHA-256(fileContent + authorNodeId + timestamp)`
3. Store file on disk at `~/.decentralised-web/files/<fileId>`
4. Sign `{ fileId, fileName, fileSize, mimeType, authorId, timestamp }` with author's Ed25519 private key
5. Broadcast `file_publish` to all connected peers via UDP
6. Peers verify signature → add to registry → gossip

#### Downloading / Opening a file:
1. User clicks a file in the registry → expands row → clicks **[Open File]** or **[Download]**
2. Frontend sends request to Python backend via WebSocket
3. Backend sends `file_request` to a peer that has a replica
4. Peer responds with `file_chunk` messages (reliable — ACK + retransmit per chunk). Each chunk: 12-byte header + up to 16KB file data. Chunks re-requested if ACK not received within 500ms (up to 5 retries).
5. Receiving backend reassembles chunks → verifies SHA-256 hash → stores on disk
6. Backend announces `file_announce` to all peers — becomes a **temporary peer** hosting the file
7. File served to browser: Open = serve as HTTP response with correct MIME type; Download = serve with `Content-Disposition: attachment`

#### Temporary Peer Storage & Replica Lifecycle:
When a user opens or downloads a file, they become a temporary peer for that file. This means:
- **Popular files** naturally accumulate many replicas as users access them
- When the opened tab is closed (or download usage times out), the file transitions from "temporary" to "regular replica"
- The rebalancing logic then evaluates: if `replicaCount > networkTarget + 1`, the file may be deleted to free space. If `replicaCount` is within target range, the file stays as a permanent replica contribution
- This creates a natural "cache" effect: frequently accessed files have high replica counts, rarely accessed files maintain baseline replication

#### Creating a Share Link (Browse tab):
1. User clicks **[Share]** on an expanded file row
2. Node sends `share_file_query` to a connected peer
3. Peer responds with `share_file_response`: file hash + up to 3 suggested peers (ordered by reliability: uptime, lastSeen recency)
4. Node generates URL: `https://<bootstrapper>/?join=<ownNodeId>&pk=<pk>&addr=<ip>:<port>&file=<fileId>&hash=<fileHash>&peers=<p1>,<p2>,<p3>`
5. URL is copied to clipboard and optionally displayed as a QR code in a small popup
6. Limiting to 3 peers (longest uptime) keeps the QR code simple and scannable

#### Updating a file:
1. Author selects existing file + new version
2. New `fileId` generated, but linked to original via `previousFileId` field
3. Signed with same author key → peers verify author matches original → update registry
4. Old replicas eventually garbage-collected

### 9. Persistence & Reconnection

#### 9a. Peer Book Tiering

The peer book (SQLite database at `~/.decentralised-web/peers.db`) assigns each peer a relevance tier:

| Tier | Criteria | Rationale |
|---|---|---|
| **Tier 1 — Critical** | Peer is author of a file in your local storage, OR peer hosts a replica of a file you authored | These peers determine the health of files you care about |
| **Tier 2 — Recent** | Last seen within 7 days, connected successfully before | Likely still online, good for general network view |
| **Tier 3 — General** | All other known peers | Fallback for bootstrapping into the network |

Tiers are recalculated whenever the local file registry changes.

#### 9b. Reconnection Sequence

On startup (process restart):

1. **Load peer book** from SQLite → sort by tier (Tier 1 first, then Tier 2, then Tier 3)
2. **Phase 0 — Bootstrap**: If peer book is empty or all tiers exhausted, try hardcoded bootstrap peers (§2c)
3. **Phase 1 — Critical peers**: Attempt direct UDP hole punch to all Tier 1 peers in parallel (max 10 concurrent). Each attempt: 3 hello packets at 100ms intervals, wait 5s. If direct punch fails for a peer, check if any currently connected peer also knows that target → if yes, request peer-assisted connection (§3c). Use exponential backoff for assisted retries (1s, 2s, 4s, then give up on that peer).
4. **Phase 2 — Assess**: Once at least one Tier 1 peer responds (or all Tier 1 exhausted), send `file_registry_query` to all connected peers. Collect responses. Now the node has an accurate picture of replica counts.
5. **Phase 3 — Rebalance** (gated — see §6d): Only now evaluate rebalancing. Do NOT rebalance on stale data.
6. **Phase 4 — Broaden**: Attempt Tier 2 and Tier 3 peers. Request their peer lists to discover new peers.
7. **Phase 5 — Broadcast**: Send LAN broadcast discovery packet (§2b) to find local peers.

**No QR needed for reconnection** as long as any peer in the book (or a bootstrap peer) is online. QR is only for brand-new nodes or when the entire known network is offline.

#### 9c. Peer Book Maintenance

- On successful connection: update `lastSeen` timestamp. Record the peer's current `(publicIP, publicPort, uptimeSince)`
- On `peer_list_response`: merge new peers into peer book (deduplicate by nodeId)
- On `goodbye` from a peer: mark as offline, keep in peer book
- Periodic cleanup: remove peers not seen in 30+ days (configurable)
- Peers that fail to connect 5+ consecutive attempts: demote one tier
- **UDP NAT mapping expiry**: keepalive pings every 30 seconds refresh the hole. If a peer misses 3 consecutive pings (90s), consider disconnected.

### 10. Terminal UI (TUI — Default Interface)

By default, running `python app.py` launches an interactive terminal UI. This is the primary interface for headless/server operation and local development (running multiple instances on the same machine for testing).

#### 10a. TUI Layout

```
┌── Decentralised Web ──────────────────── peers: 4 ──── files: 12 ── storage: 45% ──┐
│                                                                                    │
│  [Connected Peers]                                                                 │
│  a1b2c3  alice      🟢 online   2h 14m uptime    203.0.113.5:49152                │
│  d4e5f6  bob        🟢 online   45m uptime       198.51.100.3:52833               │
│  g7h8i9  carol      🟡 assisted 12m uptime       (via alice)                      │
│  j0k1l2  dave       🔴 offline  last seen 3h ago                                  │
│                                                                                    │
│  [Search: photo________________]  (type to filter files)                          │
│                                                                                    │
│  📄 photo.jpg      2.4MB  👁 3   author: alice    [d]ownload                       │
│  🖼️  photo2.png     1.2MB  👁 5   author: bob      [d]ownload                       │
│  📊 data.json       12KB  👁 1⚠️ author: carol    [d]ownload  [r]eplicate          │
│                                                                                    │
│  [My Published Files] (tab: m)                                                     │
│  📄 report.pdf      156KB  👁 2   [u]pdate  [x]delete                             │
│  🖼️  cat.png        1.2MB  👁 4   [u]pdate  [x]delete                             │
│                                                                                    │
│  [h]elp  [q]uit  [s]tats  [p]ublish file  [c]onnect to peer  [l]ogin              │
└────────────────────────────────────────────────────────────────────────────────────┘
```

#### 10b. TUI Features

- **Real-time updates**: peer list, file registry, and storage stats refresh live via the same event loop as the UDP engine
- **Search**: type to filter network files. Press Enter to select, `d` to download, `r` to trigger replication
- **Tabs**: `[1]` Network Files, `[2]` My Published Files, `[3]` Local Storage, `[4]` Peer Book
- **Publish**: `p` opens a file picker (via `tkinter` or CLI path input) → file is published to the network
- **Manual connect**: `c` prompts for a peer address (ip:port or connection URL) to manually join
- **Login**: `l` prompts for username + password → derives author keypair → enables file publishing/updating as that author. Without login, the node can browse and download but cannot publish or update files.
- **Disable TUI**: `python app.py --no-tui` runs in headless mode (pure logging output). Useful for servers.
- **Framework**: Built with `rich` library for cross-platform terminal UI (tables, panels, live updates)
- **Multiple instances**: Run `python app.py --port 9001 --tui-port-offset 100` to shift all ports, allowing multiple instances on the same machine without port conflicts

#### 10c. CLI Arguments

| Argument | Default | Purpose |
|---|---|---|
| `--user` / `-u` | (env: `DECWEB_USER`) | Author username for signing file operations. Optional — without it, the node can browse/download but not publish. |
| `--pass` / `-p` | (env: `DECWEB_PASS`) | Author password for signing file operations. |
| `--port` / `-P` | 9000 | UDP listen port for P2P |
| `--no-tui` | false | Disable terminal UI (headless mode) |
| `--web-port` | 9001 | Port for the Flask web UI (0 = disabled) |
| `--web-host` | `127.0.0.1` | Bind address for web UI (`0.0.0.0` for LAN access) |
| `--data-dir` | `~/.decentralised-web` | Data directory for SQLite DBs + file store |
| `--storage-limit` | 500 (MB) | Maximum disk storage for peer replicas |
| `--no-lan` | false | Disable LAN broadcast discovery |

### 11. Web UI (Dark Mode — Optional)

The web UI runs on a **separate port** (default: 9001) and serves the same visual interface described below. Set `--web-port 0` to disable entirely.

#### 11a. Authentication

- On first load, the web UI shows a **login form** (username + password)
- Credentials derive the **author keypair** (see §1b) — this is NOT the node's identity
- On successful login: WebSocket connection established, UI renders the full file management interface
- File operations performed via the web UI are signed with the author's key
- The web UI acts as a **remote control** for the server — managing files hosted on that server, triggering downloads, viewing the network
- Session persists via Flask session cookie. Logout available from the title bar.
- Without login: browse-only mode (view files, see peers, cannot publish/update/delete). Login enables full access per the remote login rules in §1d.

#### 11b. Layout Structure

```
┌──────────────────────────────────────────────────────────────────┐
│  🏷️ Kuno's Net  alice [📡 Share Network]         [👤 a1b2] [⏻] │  ← Title bar
├──────────┬───────────────────────────────────────────────────────┤
│          │  [ My Files ]  [ Browse ]      [+ Upload]  🔍 Search  │
│ ┌──────┐ │  ┌──────────────────────────────────────────────────┐ │
│ │🔗Join│ │  │ 📄 report.pdf          PDF   156KB   👁 2  [▼]   │ │
│ │      │ │  │   ├─ Author: a1b2c3                               │ │
│ │  QR  │ │  │   ├─ Peers: a1b2c3 (author), d4e5f6               │ │
│ │ Code │ │  │   ├─ [Open File] [Download]                       │ │
│ │      │ │  │   └─ [Share] (Browse tab only)                    │ │
│ │[📋Copy]│  │                                                    │ │
│ │      │ │  │ 🖼️  cat.png            PNG   1.2MB   👁 5  [▶]    │ │
│ │[📎Paste│  │ 🎵  song.mp3           MP3   4.8MB   👁 3  [▶]    │ │
│ │ Peer] │  │ 📊  data.json          JSON   12KB   👁 1  ⚠️ [▶] │ │
│ │      │ │  └──────────────────────────────────────────────────┘ │
│ │[📷Scan││                                                       │
│ │ Peer] ││                                                       │
│ │      │ │                                                       │
│ │[✕Hide]│ │                                                       │
├──────────┴───────────────────────────────────────────────────────┤
│  💾 Storage: ████████░░ 8MB / 20MB  │  4 own  │  2 peer replicas │  ← Bottom bar
│  🟢 Healthy — 4 peers connected                                   │
└──────────────────────────────────────────────────────────────────┘
```

#### 11c. Title Bar (top, full width)

| Element | Position | Behavior |
|---|---|---|
| **Network name** (e.g. "Kuno's Net") | Left | User-configurable display name. Defaults to auto-generated but editable |
| **Author username** | Left, after name | Shows logged-in author (e.g. "alice"). Blank if not logged in. Click to re-login |
| **Share Network** button | Left, after username | Reopens the QR code panel if collapsed |
| **Node ID** | Right | Truncated node ID (first 8 chars), click to copy. This is the random node identity |
| **Logout** button | Right, after Node ID | Clears author session, reverts to browse-only mode |

#### 11d. Peer Join/Share Panel (left side, collapsible)

- Default: **open** on first visit (no peers connected yet)
- **Collapses automatically** once the node has 2+ connected peers. Clicking **[📡 Share Network]** in the title bar reopens it

Contains three ways to join/share:

```
┌──────────────────┐
│  🔗 Join Network │
│                  │
│  ┌────────────┐  │
│  │            │  │  ← QR code for others to scan
│  │  QR Code   │  │    (encodes your connection URL)
│  │            │  │
│  └────────────┘  │
│  [📋 Copy Link]  │  ← Copies connection URL to clipboard
│                  │
│  ── Join a peer ─│
│  [📎 Paste Peer  │  ← Pastes a peer's connection URL
│   Address]       │    from clipboard, auto-connects
│                  │
│  [📷 Scan QR     │  ← Opens camera to scan a peer's
│   of Peer]       │    QR code and auto-connect
│                  │
│  [✕ Hide Panel]  │
└──────────────────┘
```

**Your QR code:** Encodes `https://<bootstrapper>/?join=<nodeId>&pk=<base64PublicKey>&addr=<ip>:<port>` (~180 chars). Shows the address of any bootstrap peer so the scanning device knows where to join. Regenerated when IP/port changes. **[📋 Copy Link]** copies this URL to clipboard.

**Paste Peer Address:** User receives a connection URL from another peer (via messaging app, email, etc.). Pasting into this field triggers: parse URL → extract `?join`, `?pk`, `?addr` → Python backend initiates UDP hole punch to the target address. No QR scan needed.

**Scan QR of Peer:** Uses the device camera (`getUserMedia` with `video` constraint) to scan another peer's QR code. On successful scan: parse URL → Python backend sends UDP hello packets to the target `addr` → existing peer replies → hole punched → connection established. Single scan, single direction. No response QR needed — the existing node is already listening on its known port.

#### 11e. Center Panel — File Lists (tabs)

Two tabs sharing the same layout and controls:

| Tab | Shows | Extra action |
|---|---|---|
| **My Files** | Files the user has published | Update, Delete |
| **Browse** | All known network files | Share (creates shareable link with file hash + peers) |

**Row display (collapsed):**
```
📄 report.pdf          PDF   156KB   👁 2   [▼]
```
Shows: icon, file name, file type, size, replica count (with warning ⚠️ if under-replicated), expand arrow.

**Row display (expanded):**
```
📄 report.pdf          PDF   156KB   👁 2   [▲]
  ├─ Author: a1b2c3
  ├─ Added: 2026-07-03
  ├─ Peers hosting this file:
  │   a1b2c3 (author) 🟢 online
  │   d4e5f6          🟢 online
  ├─ [Open File]  [Download]
  └─ [Share] (Browse tab only)
```

**Actions on expand:**
- **Open File**: Opens file in a new browser tab. Temporarily adds the file to the user's peer storage (becomes a replica host). Popular files accumulate many temporary peers.
- **Download**: Same as Open but triggers a browser download. Also temporarily adds to peer storage.
- **Share** (Browse tab only): Generates a shareable URL containing the site URL + node ID + file hash + up to 3 peers with the longest uptime (highest `uptimeSince` values). Keeps the URL/QR compact.
- **Update** (My Files, own files only): Replace with new version (signed).
- **Delete** (My Files, own files only): Remove from network.

**Search box**: Filters rows by file name as you type. Works identically on both tabs.

**[+ Upload File] button**: In the tab header area (next to the search box). Triggers a native file picker (`<input type="file">`). On selection, the file is published per §8. Supported on both tabs — on Browse tab, uploads appear in "My Files" after publishing.

#### 11f. Bottom Bar — Storage Health (full width)

Shows a compact overview of the node's storage and network health:

```
💾 Storage: ████████░░ 8MB / 20MB  │  4 own files  │  2 peer replicas
🟢 Healthy — 4 peers connected
```

**Storage bar**: Visual progress bar showing used vs configured storage. Segments colour-coded by category (own files vs replicas).

**Health indicator** (colour + text):

| State | Colour | Meaning |
|---|---|---|
| **Reconnecting…** | ⬜ Grey | Loading peer book, attempting connections, no registry data yet |
| **Healthy** | 🟢 Green | Connected, files have replicas within network target ±1 |
| **Below standard** | 🟠 Amber | At least one stored file has fewer replicas than `networkTarget - 1` |
| **No peers** | 🔴 Red | Zero connected peers — files are inaccessible to the network |
| **Many peers** | 🔵 Blue | Connected to 10+ peers; files well above network target |

Transitions are debounced — don't flash red/amber on a brief disconnect.

#### 11g. Temporary Peer Storage (Open/Download behaviour)

When a user clicks **Open File** or **Download**:
1. File is downloaded via UDP from a known peer (reliable chunks with ACK)
2. Stored on disk at `~/.decentralised-web/files/<fileId>`
3. Node announces `file_announce` — becomes a peer hosting that file
4. File is marked as **temporary** (not a permanent replica commitment)

When the file's browser tab is closed (detected via `pagehide`/`beforeunload` on the opened tab, or via a timeout if download):
1. File is downgraded from "temporary" to "regular peer replica"
2. Rebalancing logic evaluates:
   - If `replicaCount > networkTarget + 1`: delete this file (too many copies)
   - If `replicaCount <= networkTarget + 1`: keep it — it's needed
   - Priority: other over-replicated files may be deleted first to make room for under-replicated ones

This means **popular files naturally accumulate many replicas** as users open them, then gracefully shed excess copies. Files that are rarely accessed maintain just the baseline replica count.

---

## Files to Create

| File | Purpose |
|---|---|
| `app.py` | Main entry point — CLI arg parsing, TUI launch, UDP engine, optional Flask server |
| `identity.py` | Random node keypair generation (persisted), PBKDF2 author key derivation (session-only), signing, verification |
| `tui.py` | Terminal UI using `rich` — live peer list, file search, tabs, keyboard shortcuts |
| `udp_engine.py` | UDP socket, hole punching, STUN query, send/recv, keepalive pings, peer-assisted connection |
| `reliable.py` | Reliability layer: sequence numbers, ACKs, retransmit, duplicate detection |
| `protocol.py` | Binary message encoding/decoding, message routing, gossip logic |
| `peer_book.py` | SQLite-backed peer directory, tiering, cleanup |
| `connection.py` | Per-peer connection state, hole-punch lifecycle, ping tracking |
| `file_registry.py` | Local file registry with gossip sync, SQLite-backed |
| `storage.py` | Disk file storage, quota tracking, rebalancing triggers |
| `replication.py` | Replica counting, solicitation, rebalancing logic |
| `web/` | Flask web UI (optional, on separate port) |
| `web/app.py` | Flask routes: serve HTML, WebSocket endpoint (auth-gated), file download endpoint |
| `web/templates/index.html` | Login form + main UI layout (dark mode) |
| `web/templates/login.html` | Username + password login form |
| `web/static/style.css` | Dark mode styling |
| `web/static/app.js` | Frontend JS: login, WebSocket connection, DOM manipulation, QR display |
| `web/static/qrcode.min.js` | QR code generation library (single-file include) |
| `requirements.txt` | Python dependencies: flask, flask-sock, cryptography, rich |

---

## Verification Plan

1. **TUI launch**: Run `python app.py -u alice -p secret` → TUI appears, node generates identity, starts listening.
2. **Multi-instance**: Run two instances with `--port 9001` and `--port 9002` on same machine. Verify they connect via LAN broadcast or manual `c` connect.
3. **QR Discovery**: Device A shows QR (web UI or copied link). Device B scans → UDP hole punch → connection within 1 second.
4. **Deterministic identity**: Run with `-u alice -p secret` on two different machines. Verify they produce the same Node ID and public key.
5. **Web UI login**: Open `http://localhost:9001` → login form → enter server's credentials → WebSocket connects → full UI renders.
6. **File publish**: Upload file via TUI (`p`) or web UI. Verify it appears in other peers' file lists.
7. **Download**: Click download in web UI or press `d` in TUI → file transfers via UDP with reliable chunks.
8. **Replication + rebalancing**: As per original verification plan.

---

## Decisions

- **PBKDF2-derived Ed25519** keypair from username + password — portable identity, no key files to lose or sync. Same credentials on any device = same node.
- **Terminal UI by default** (`rich` library) — enables running multiple instances locally, headless server mode via `--no-tui`.
- **Web UI on separate port** with admin login — remote management, same credentials as the server identity.
- **UDP with reliability layer** — no WebRTC ceremony. Hole punching takes <500ms vs 2-5s for ICE+DTLS.
- **Binary wire format** — 19-byte header, compact.
- **SQLite** for peer book and file registry.
- **Disk storage** for files — GB-scale, default 500MB cap.
- **STUN only** — same Google STUN. ~85% success, peer-assisted fallback for symmetric NAT.
- **Bootstrap peers + LAN broadcast + peer book** — auto-join without QR in most scenarios. QR is fallback.
- **Eventual consistency** — gossip + timestamps.
- **Honest node assumption** — tech demo.

---

## Out of Scope (explicitly)
- Encryption of file contents at rest or in transit (plain UDP)
- TURN relay for symmetric NATs
- Proof-of-storage or economic incentives
- Malicious node detection
- Versioned file history beyond simple updates
- Multi-file directories/folders
- NAT port prediction for symmetric NAT edge cases
