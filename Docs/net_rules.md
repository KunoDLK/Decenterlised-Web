# Decentralised Web — P2P Communication Protocol Specification

## Wire Format

```
[version: 1 byte] [msg_type: 2 bytes BE] [sender_id_prefix: 8 bytes] [seq_num: 4 bytes BE] [payload: N bytes]
```

---

## Message Types

### Control Messages

| Type | ID | Payload | Reliable? |
|---|---|---|---|
| `HELLO` | `0x0001` | node_id, pubkey(32B), ip, port, uptime, sig(64B), last_registry_update, last_peer_update | No |
| `PING` | `0x0002` | node_id, last_registry_update, last_peer_update | No |
| `ACK` | `0x0003` | acked_msg_type, ack_seq_num | No |
| `GOODBYE` | `0x00FF` | (empty) | No |

### Peer Discovery

| Type | ID | Purpose |
|---|---|---|
| `PEER_LIST_REQUEST` | `0x0010` | Request peer list from connected peer |
| `PEER_LIST_RESPONSE` | `0x0011` | Response with `peers[]` + `estimated_network_target` |

### File Registry

| Type | ID | Purpose |
|---|---|---|
| `FILE_REGISTRY_QUERY` | `0x0020` | Request registry delta | ✅ |
| `FILE_REGISTRY_RESPONSE` | `0x0021` | Response with `entries[]` | ✅ |
| `FILE_REGISTRY_PUSH` | `0x0022` | Push single entry update | ✅ |

### File Operations

| Type | ID | Purpose |
|---|---|---|
| `FILE_PUBLISH` | `0x0050` | Publish new file to network | ✅ |
| `FILE_UPDATE` | `0x0051` | Update existing file | ✅ |
| `FILE_DELETE` | `0x0052` | Delete file | ✅ |
| `FILE_ANNOUNCE` | `0x0040` | Announce replica available | No |
| `FILE_REQUEST` | `0x0030` | Request file download | No |
| `FILE_CHUNK` | `0x0031` | File chunk data (max 8192 bytes) | ✅ |
| `FILE_CHUNK_ACK` | `0x0032` | Acknowledge single chunk receipt | No |

### Replication

| Type | ID | Purpose |
|---|---|---|
| `REPLICATION_SOLICIT` | `0x0041` | Solicit replication of a file | No |
| `REPLICATION_ACK` | `0x0042` | Acknowledge replication | No |

### NAT Traversal

| Type | ID | Purpose |
|---|---|---|
| `CONNECT_REQUEST` | `0x0060` | Request peer-assisted connect | No |
| `CONNECT_INTRODUCE` | `0x0061` | Introducer forwards connect to target | No |
| `CONNECT_ACK` | `0x0062` | Connect acknowledgement | No |

### File Sharing (Web UI)

| Type | ID | Purpose |
|---|---|---|
| `SHARE_FILE_QUERY` | `0x0070` | Query file availability | ✅ |
| `SHARE_FILE_RESPONSE` | `0x0071` | Response with `suggested_peers[]` | ✅ |

> ✅ = Reliable delivery: auto-ACK sent by UDP engine on receipt, retransmit if ACK not received.

---

## Timing & Intervals

### Keepalive / Liveness

| Parameter | Value | Description |
|---|---|---|
| `keepalive_interval` | **30 seconds** | Send PING to each connected peer |
| `ping_response_timeout` | **2 seconds** | Wait for PING response before counting as missed |
| `peer_timeout` | **90 seconds** | 3 × missed pings = peer considered dead |
| `max_missed_pings` | **3** | Disconnect after this many consecutive misses |

### LAN Broadcast

| Parameter | Value | Description |
|---|---|---|
| `lan_broadcast_interval` | **30 seconds** | Broadcast HELLO to `255.255.255.255:self.port` |
| `lan_broadcast_min_peers` | **2** | Advisory threshold (broadcast always runs) |

### Reliability / Retransmit

| Parameter | Value | Description |
|---|---|---|
| `ack_timeout_base` | **0.5 seconds** | Initial retransmit timeout |
| `ack_timeout_max` | **4.0 seconds** | Maximum timeout after exponential backoff |
| `ack_timeout_multiplier` | **2.0×** | Multiplier per retry attempt |
| `max_retries` | **5** | Give up after this many attempts |
| `sliding_window_size` | **256** | Duplicate detection window per peer |

### File Transfer

| Parameter | Value | Description |
|---|---|---|
| `max_chunk_size` | **8192 bytes** | Maximum bytes per FILE_CHUNK datagram |
| `chunk_ack_timeout` | **0.5 seconds** | Wait for chunk ACK before retransmit |
| `download_timeout` | **60 seconds** | Maximum time for a full file download |

### Hole Punching

| Parameter | Value | Description |
|---|---|---|
| `hole_punch_packets` | **3** | Send this many HELLO bursts for hole punch |
| `hole_punch_interval` | **0.1 seconds** | Gap between punch packets |
| `hole_punch_timeout` | **5.0 seconds** | Wait for hole punch HELLO response |
| `max_direct_attempts` | **5** | Before marking peer as `direct_blocked` |

### Replication / Rebalance

| Parameter | Value | Description |
|---|---|---|
| `rebalance_interval` | **60 seconds** | Time between rebalance cycles |
| `network_target_min` | **3** | Minimum replica count per file |
| `network_target_max` | **10** | Maximum replica count per file |
| `replica_tolerance_band` | **±1** | Healthy band around target — no rebalance action within it (prevents thrashing) |
| `rebalance_min_peers` | **3** | Minimum connected peers before rebalancing may act (gate) |
| `temporary_replica_ttl` | **3600 seconds** | 1 hour before temp replica becomes permanent |
| `min_publish_bytes` | **1 MB** | Minimum bytes available to allow publishing |

### Periodic Maintenance

| Parameter | Value | Description |
|---|---|---|
| `cleanup_temp_interval` | **300 seconds** | Clean up temporary replicas (5 min) |
| `gc_old_versions_interval` | **1800 seconds** | GC old file versions (30 min) |
| `peer_cleanup_interval` | **3600 seconds** | Remove stale peers (1 hour) |
| `peer_cleanup_max_age_days` | **30 days** | Max peer age before removal |

---

## Event → Response Rules

### 1. Receiving HELLO (`0x0001`)

**On arrival:**
- `mark_seen(sender_id)` — update liveness timestamp
- **NEVER** drop HELLO as duplicate — it is a liveness signal
- Verify signature: `sign(node_id || ip || port || uptime)` against `public_key`
- If signature fails → drop packet, log warning
- `add_or_update(peer)` in peer book database
- `set_connection_state(CONNECTED, from_ip, from_port)`
- Cache `from_addr → node_id` mapping in UDP engine
- Clean up any loopback-discovery placeholder for this address

**If peer is NEW** (was not previously `CONNECTED`):
- Log `"New peer <id> from <ip>:<port>"`
- `_mark_peer_updated()` — bump local peer update timestamp
- Emit `peer_connected` event to UI
- Queue `SEND_HELLO_REPLY` at **CRITICAL** priority (front of queue)
- Queue `PING_PEER` at **LOW** priority, delay = `keepalive_interval` (30s)

**If peer has newer registry** (`peer.last_registry_update > self.last_registry_update`):
- Queue `EXCHANGE_REGISTRY` at **NORMAL** priority, 1s delay

**Always:**
- Cancel any pending `CHECK_PING_RESPONSE` for this peer

---

### 2. Receiving PING (`0x0002`)

**On arrival:**
- `mark_seen(sender_id)` — update liveness timestamp
- **NEVER** drop PING as duplicate
- `record_ping_received(sender_id)` — update `last_seen` and `consecutive_fails = 0`
- Cancel `CHECK_PING_RESPONSE` for this peer

**If peer has newer registry** (`ping.last_registry_update > self.last_registry_update`):
- Queue `EXCHANGE_REGISTRY` at **NORMAL** priority, 0.5s delay

**If peer has newer peer list** (`ping.last_peer_update > self.last_peer_update`):
- Queue `REQUEST_PEER_LIST` at **NORMAL** priority, 0.5s delay

---

### 3. Receiving ACK (`0x0003`)

- Parse `acked_msg_type` and `ack_seq_num` from payload
- Call `reliable.ack_received(sender_id, ack_seq_num)` — pops pending retransmit entry
- **NEVER** drop ACK as duplicate
- When scheduler later runs `CHECK_RETRANSMIT` for this msg, `mark_retry()` returns `None` (already acked) → handler exits without re-enqueuing

---

### 4. Sending PING Keepalive (`_act_ping_peer`)

**Runs every `keepalive_interval` (30s) per connected peer:**
- If peer is not `CONNECTED` → skip (return early)
- Build PING payload with `node_id`, `last_registry_update`, `last_peer_update`
- Send PING to peer
- `record_ping_sent(peer_id)` — update `last_ping_sent` timestamp
- Queue `CHECK_PING_RESPONSE` at **HIGH** priority, `ping_response_timeout` (2s) delay, with `missed_count`
- Re-queue `PING_PEER` at **LOW** priority, `keepalive_interval` (30s) delay

---

### 5. PING Response Check (`_act_check_ping_response`)

**Runs `ping_response_timeout` (2s) after PING sent:**
- If `peer.last_seen >= peer.last_ping_sent` → response was received, do nothing
- Otherwise: `missed_count += 1`
- If `missed_count >= max_missed_pings` (3):
  - Set connection state to `DISCONNECTED`
  - `remove_peer_replicas(peer_id)` from file registry
  - `discard_all_for_peer(peer_id)` — clear pending reliable messages
  - `_mark_peer_updated()`, emit `peer_disconnected` event

---

### 6. Liveness Check (`_act_liveness_check`)

**Runs every `keepalive_interval` (30s):**
- Iterate all peers with state `CONNECTED`
- If `time.time() - peer.last_seen > peer_timeout` (90s):
  - Log `"Peer <id> liveness timeout"`
  - Set connection state to `DISCONNECTED`
  - `remove_peer_replicas(peer_id)`
  - `discard_all_for_peer(peer_id)` — clear pending reliables
  - `_mark_peer_updated()`, emit `peer_disconnected` event
- Re-queue self at **LOW** priority, `keepalive_interval` (30s) delay

---

### 7. LAN Broadcast (`_act_lan_broadcast`)

**Runs every `lan_broadcast_interval` (30s), always:**
- Send HELLO to `255.255.255.255:self.port`
- HELLO includes `last_registry_update` and `last_peer_update` timestamps
- Re-queue self at **LOW** priority, `lan_broadcast_interval` (30s) delay

---

### 8. Loopback Discovery (startup only)

**When port conflicts detected** (`skipped_ports` not empty):
- For each skipped port: create placeholder peer at `127.0.0.1:<port>`
- Queue `HOLE_PUNCH_PEER` at **NORMAL** priority to probe

**When `--tui-port-offset` is non-zero:**
- Also probe `127.0.0.1:(port - offset)` — the base port without offset
- Creates placeholder peer, queues `HOLE_PUNCH_PEER`

---

### 9. File Publish (user upload via web UI)

**`publish_file(data, file_name, mime_type)`:**
1. Compute `file_id = SHA256(data || author_id || timestamp)`
2. Store own file to disk (`store_own_file`)
3. Create `FileRegistryEntry` with `replica_count = 1`, add to registry
4. `_mark_registry_updated()` — bump registry timestamp
5. Broadcast `FILE_PUBLISH` to **all connected peers** (`broadcast()`)
6. Emit `file_added` event to UI

---

### 10. Receiving FILE_PUBLISH (`0x0050`)

- Verify author signature: `sign(file_id || file_name || file_size || mime_type || author_id || timestamp)`
- If signature fails → drop
- Add entry to file registry (`replica_count = 1`)
- `_mark_registry_updated()`
- Emit `file_added` event
- If we do **not** have the file locally AND have enough available storage:
  - Log `"Replicating <file_id> from <sender_id>"`
  - Queue `SOLICIT_REPLICATION` at **NORMAL** priority
- ✅ Reliable delivery: auto-ACK sent, retransmit if no ACK

---

### 11. Solicit Replication (`_act_solicit_replication`)

- Triggered automatically by scheduler actions (for example from `FILE_PUBLISH` handling or rebalance decisions); never requires manual user interaction.
- If `storage.has_file(file_id)` → just call `increment_replica`, skip download
- Otherwise: call `download_file(file_id)` — **blocking** download
- On success: `store_replica(file_id, data)`, `increment_replica`, broadcast `FILE_ANNOUNCE` so the network immediately counts this node as a host
- On failure: log warning
- Emit `file_added` event

---

### 12. File Download (`download_file`)

1. Look up file in registry → find first connected peer hosting it
2. If no connected peer hosts the file → raise `ValueError`
3. Create transfer record: `transfer_id = "dl:<file_id>:<host_id>"`
4. Send `FILE_REQUEST` to host peer
5. Block and wait up to `download_timeout` (60s):
   - Chunks arrive as `FILE_CHUNK` → stored to disk via `store_chunk()`
   - Each chunk triggers `FILE_CHUNK_ACK` back to sender
   - Transfer progress tracked in registry DB
6. When `len(received) >= total` (all chunks arrived):
   - Assemble chunks from disk via `assemble_chunks()`
   - **Integrity check:** `SHA256(data || author_id || timestamp) == file_id`
   - On success:
     - `store_temporary_replica(file_id, data)`
     - `increment_replica(file_id, self.node_id)`
     - `mark_transfer_complete(transfer_id)`
     - Broadcast `FILE_ANNOUNCE` to all connected peers
     - Emit `download_progress(status="complete")` to UI
   - On failure: `mark_transfer_failed(transfer_id)`

---

### 13. Sending File Chunks (`_act_send_chunk`)

**Triggered by `FILE_REQUEST` or `FILE_CHUNK_ACK`:**
- Read file from storage
- Split into `max_chunk_size` (8192B) pieces
- Send `FILE_CHUNK` for current `chunk_index`
- Queue `CHECK_CHUNK_ACK` at **HIGH** priority, `chunk_ack_timeout` (0.5s) delay
- When `FILE_CHUNK_ACK` received → cancel pending `CHECK_CHUNK_ACK`, enqueue next chunk at **CRITICAL**
- If no ACK after timeout → increment retry count, resend chunk (up to `max_retries` = 5)
- ✅ Reliable delivery with auto-ACK + chunk-level ACK

---

### 14. Receiving FILE_CHUNK (`0x0031`)

- Store chunk data to disk: `store_chunk(file_id, chunk_index, data)`
- Update transfer progress in registry
- Send `FILE_CHUNK_ACK` back to sender
- Check if download complete (`len(received) >= total`):
  - If already have file (`storage.has_file`) → just `mark_transfer_complete`, return
  - Assemble chunks, verify integrity, store temporary replica, broadcast `FILE_ANNOUNCE`

---

### 15. Receiving FILE_ANNOUNCE (`0x0040`)

- `increment_replica(file_id, node_id)` — count announcing node as a replica host
- `_mark_registry_updated()`

---

### 16. Registry Exchange (`_act_exchange_registry`)

**Runs on startup, or when peer PING/HELLO indicates newer data:**
- Send `FILE_REGISTRY_QUERY` carrying the local `last_registry_update` timestamp (and, when available, a registry digest/hash) to connected peers
- The peer replies with only the **delta** — entries changed since the requester's timestamp — never the full registry. This keeps sync cost bounded as the network's file count grows (scales with churn, not total files)
- ✅ Reliable delivery (auto-ACK)

**On receiving `FILE_REGISTRY_QUERY`:**
- If the peer's digest already matches the local registry → respond with an empty delta (nothing to transfer)
- Otherwise compute and send `FILE_REGISTRY_RESPONSE` with only the entries newer than the requester's `last_registry_update`

**On receiving `FILE_REGISTRY_RESPONSE`:**
- `merge_delta(entries)` — merge peer's registry entries into local DB (**latest-timestamp-wins** on conflicting updates to the same file)
- `_mark_registry_updated()`
- Merge the peer's `estimated_network_target` via **median** of received estimates (see Replication & Network Target)

**On receiving `FILE_REGISTRY_PUSH`:**
- Update single entry in registry
- Broadcast `FILE_REGISTRY_PUSH` to all other connected peers (except sender)

---

### 17. Peer List Exchange

- Send `PEER_LIST_REQUEST` to connected peers
- Peer responds with `PEER_LIST_RESPONSE` containing:
  - `peers[]`: list of `(node_id, public_ip, public_port, uptime_since)` — **only actively connected peers are shared**. Stale/offline addresses are omitted, since hole punching requires both sides online, so gossiping dead peers wastes bandwidth and slows convergence
  - `estimated_network_target`: replication target value
- On receive: `add_or_update()` each peer (**deduplicate by node_id**), merge `estimated_network_target` via **median** of received values

---

### 18. File Update (`update_file`)

- Only author can update
- Compute new `file_id = SHA256(new_data || author_id || timestamp)`
- Store new file, add to registry with `previous_file_id` link
- `_mark_registry_updated()`
- Broadcast `FILE_UPDATE` to all connected peers
- ✅ Reliable delivery

**On receiving `FILE_UPDATE`:**
- Verify author signature
- Verify author matches existing file's author
- Add new entry with `previous_file_id` link
- `_mark_registry_updated()`

---

### 19. File Delete (`delete_file`)

- Only author can delete
- `mark_deleted(file_id)` in registry
- `_mark_registry_updated()`
- Remove file from storage
- Broadcast `FILE_DELETE` to all connected peers
- ✅ Reliable delivery

**On receiving `FILE_DELETE`:**
- Verify author signature
- Verify author matches existing file's author
- `mark_deleted(file_id)`, remove from storage
- `_mark_registry_updated()`

---

### 20. Hole Punch (`_act_hole_punch_peer`)

1. Set peer connection state to `PUNCHING` with target `(ip, port)`
2. Send `hole_punch_packets` (3) HELLO packets directly to `(ip, port)`
3. Space them `hole_punch_interval` (0.1s) apart
4. Queue `CHECK_HOLE_PUNCH` at **HIGH** priority, `hole_punch_timeout` (5s) delay
5. If peer responds with HELLO → transitions to `CONNECTED` naturally via `_on_hello`
6. If no response, `increment_hole_punch_attempts()`:
   - After `max_direct_attempts` (5) → marked `direct_blocked`
   - Preserves `CONNECTED` state if peer connected via other means (never demotes)

---

### 21. Retransmit / ACK Mechanism

**Reliable message types:** `FILE_CHUNK` (0x31), `FILE_REGISTRY_QUERY` (0x20), `FILE_REGISTRY_RESPONSE` (0x21), `FILE_REGISTRY_PUSH` (0x22), `FILE_PUBLISH` (0x50), `FILE_UPDATE` (0x51), `FILE_DELETE` (0x52), `SHARE_FILE_QUERY` (0x70), `SHARE_FILE_RESPONSE` (0x71)

**Sending a reliable message:**
- `track_sent(peer_id, seq_num, payload, msg_type)` → stores in `pending_acks` dict
- Returns expiry time = `time.monotonic() + ack_timeout_base` (0.5s)
- Queue `CHECK_RETRANSMIT` at **HIGH** priority, delay = `expiry - now`

**When ACK received:**
- `ack_received(peer_id, seq_num)` → pops from `pending_acks`
- Scheduler later runs `CHECK_RETRANSMIT` → `mark_retry()` returns `(None, None)` → exits cleanly

**When no ACK received (retransmit):**
- `mark_retry()` checks `pending_acks`
- If already acked → returns `None` (clean exit)
- If max retries exceeded → pops from `pending_acks`, returns `None` (give up)
- Otherwise: exponential backoff:
  - Attempt 1: 0.5s → 2: 1.0s → 3: 2.0s → 4: 4.0s (cap) → 5: give up
- Resend original message, re-queue `CHECK_RETRANSMIT`

**Auto-ACK:** In `_recv_loop`, after dispatching to handler, if received message type is in `RELIABLE_MSG_TYPES`, automatically send ACK back to sender.

---

### 22. Rebalance (`_act_rebalance`)

**Rebalancing gate (never act on stale data):**
- Skip the cycle unless the node has an accurate network picture: either **≥ `rebalance_min_peers` (3) connected peers**, OR all Tier 1 peers have been contacted (see Peer Book Tiering)
- Wait until `FILE_REGISTRY_RESPONSE` has been received from all connected peers before evaluating deletions/replications — this prevents deleting files that only *appear* over-replicated because not every peer has reported yet

**Runs every `rebalance_interval` (60s), once the gate passes:**
1. `calculate_network_target()` — `floor(Σ contributing_storage / Σ unique_file_size)`, clamped to `network_target_min` (3) .. `network_target_max` (10); converge with peers via **median** of gossiped `estimated_network_target`
2. Classify each file against the target using the **±1 tolerance band** (`replica_tolerance_band`) to prevent thrashing:
   - `replica_count > network_target + 1` → **over-replicated** → candidate for local deletion
   - `network_target - 1 ≤ replica_count ≤ network_target + 1` → **healthy** → no action
   - `replica_count < network_target - 1` → **under-replicated** → candidate for replication
3. `execute_rebalance()`:
  - **Under-replicated + spare capacity:** choose files to mirror with a **storage-diversity preference** — prefer files whose existing replica-holders overlap least with this node's connected peers, so a single peer outage drops at most one file. Automatically send `REPLICATION_SOLICIT` without user action; the selected peer then auto-runs `_act_solicit_replication` to download and host the file. On `REPLICATION_ACK` → `increment_replica(file_id, node_id)`
   - **Under-replicated + storage full:** delete the most over-replicated local file (highest `replica_count`, `> network_target + 1`) to free space, then replicate the vulnerable file
   - **Originator protection:** the original publisher always keeps its own file regardless of replica count — never deleted during rebalance
4. Re-queue self at **NORMAL** priority, `rebalance_interval` (60s) delay

---

### 23. Reconnect Sequence (startup)

Peers are ranked by tier from the peer book (see Peer Book Tiering). Reconnection proceeds in phases so the node never rebalances on an incomplete picture — critical for correctness as the network scales.

1. **Load peer book**, sort by tier (Tier 1 → Tier 2 → Tier 3)
2. **Phase 0 — Bootstrap:** if the book is empty or all tiers are exhausted, queue `HOLE_PUNCH_PEER` to hardcoded bootstrap peers
3. **Phase 1 — Critical peers:** queue `HOLE_PUNCH_PEER` to all **Tier 1** peers (author of a file you store, or host of a replica of a file you authored). If a direct punch fails and a connected peer knows the target → request peer-assisted connection (CONNECT_REQUEST)
4. **Phase 2 — Assess:** once ≥1 Tier 1 peer responds (or Tier 1 is exhausted), queue `EXCHANGE_REGISTRY` and `REQUEST_PEER_LIST` (NORMAL, staggered delays) to all connected peers and collect registry responses
5. **Phase 3 — Rebalance:** only now, and only if the rebalancing gate passes (Rule 22), evaluate deletions/replications
6. **Phase 4 — Broaden:** queue `HOLE_PUNCH_PEER` to Tier 2 then Tier 3 peers; request their peer lists to discover new peers
7. **Phase 5 — Broadcast:** queue initial `LAN_BROADCAST` (LOW, 5s delay) to find local peers

---

### 24. Peer Disconnect / Goodbye

**On receiving `GOODBYE` (`0x00FF`):**
- Log `"Peer disconnected: <id>"`
- Set connection state to `DISCONNECTED`
- `remove_peer_replicas(peer_id)` — remove all replicas hosted by this peer
- `discard_all_for_peer(peer_id)` — clear pending reliable messages
- `_mark_peer_updated()`
- Emit `peer_disconnected` event to UI

**On liveness timeout:**
- Same sequence as GOODBYE

---

### 25. Connect via URL (Join Link)

**`connect_to_peer(node_id, pubkey_b64, ip, port)`:**
1. Parse base64 public key
2. `add_or_update` peer in book
3. Set state to `PUNCHING`
4. Send `hole_punch_packets` (3) HELLOs inline (not via scheduler)
5. Queue `CHECK_HOLE_PUNCH` (HIGH, 5s)
6. If peer responds with HELLO → `_on_hello` transitions to `CONNECTED`

---

## Peer Book Tiering

The peer book (SQLite) assigns each peer a relevance tier so reconnection and rebalancing scale gracefully — critical peers are contacted first, and the node reaches an accurate picture quickly without waiting on the whole network.

| Tier | Criteria | Rationale |
|---|---|---|
| **Tier 1 — Critical** | Peer is author of a file in local storage, OR peer hosts a replica of a file this node authored | Determines the health of files this node cares about |
| **Tier 2 — Recent** | Last seen within 7 days and previously connected successfully | Likely still online; good for general network view |
| **Tier 3 — General** | All other known peers | Fallback for bootstrapping into the network |

- Tiers are recalculated whenever the local file registry changes
- On `PEER_LIST_RESPONSE`: merge new peers (deduplicate by node_id)
- On `GOODBYE` or timeout: mark offline, keep in book
- Peers failing to connect 5+ consecutive times: demote one tier
- Periodic cleanup (`peer_cleanup_interval`, 1h): remove peers not seen in `peer_cleanup_max_age_days` (30 days)

---

## Replication & Network Target

Each node continuously estimates the sustainable replica count from its partial view, so the network converges without central coordination:

```
network_target = floor(Σ contributing_storage / Σ unique_file_size)
```

- Clamped to `network_target_min` (3) .. `network_target_max` (10)
- Gossiped in `PEER_LIST_RESPONSE` and `FILE_REGISTRY_RESPONSE` as `estimated_network_target`
- Nodes merge received estimates via **median** to converge on a shared value despite partial views
- Rebalancing acts only on the **±1 tolerance band** (Rule 22) so files at `network_target ± 1` are considered healthy and never trigger churn
- **Diversity:** when spare capacity allows mirroring, prefer under-replicated files whose replica-holders overlap least with this node's current peers, spreading outage risk across the network
- **Originator protection:** a publisher never deletes its own file during rebalance

---

## Priority System

| Priority | Level | Used For |
|---|---|---|
| `CRITICAL` | 0 | HELLO replies (`SEND_HELLO_REPLY`), chunk sends (`SEND_CHUNK`), connect introduces (`SEND_CONNECT_INTRODUCE`) |
| `HIGH` | 1 | Retransmit checks (`CHECK_RETRANSMIT`), chunk ACK checks (`CHECK_CHUNK_ACK`), PING response checks (`CHECK_PING_RESPONSE`), hole punch checks (`CHECK_HOLE_PUNCH`) |
| `NORMAL` | 2 | Rebalance (`REBALANCE`), registry exchange (`EXCHANGE_REGISTRY`), peer list request (`REQUEST_PEER_LIST`), hole punch (`HOLE_PUNCH_PEER`), replication solicit (`SOLICIT_REPLICATION`) |
| `LOW` | 3 | PING keepalives (`PING_PEER`), cleanup (`CLEANUP_TEMP`), GC (`GC_OLD_VERSIONS`), peer cleanup (`PEER_CLEANUP`), liveness check (`LIVENESS_CHECK`), LAN broadcast (`LAN_BROADCAST`) |

---

## Duplicate Filtering

- **HELLO (`0x0001`), PING (`0x0002`), ACK (`0x0003`):** NEVER filtered as duplicates — always processed (they are liveness/control signals)
- **All other message types:** Filtered by sliding window (256 entries per peer) keyed on `(peer_id, seq_num)`
  - If `seq_num` already in received set → drop (duplicate)
  - If `seq_num` is behind the window → drop
  - Otherwise → record and process

---

## Update Timestamp Signalling

Each node tracks two timestamps:

| Timestamp | Updated When |
|---|---|
| `last_registry_update` | File published, updated, deleted, announced, or registry merged from peer |
| `last_peer_update` | Peer connects, disconnects, or times out |

These timestamps are embedded in **every HELLO and PING** payload. When a node receives a HELLO or PING:

- If `peer.last_registry_update > self.last_registry_update` → peer has newer files, queue `EXCHANGE_REGISTRY`
- If `peer.last_peer_update > self.last_peer_update` → peer knows about more peers, queue `REQUEST_PEER_LIST`

This allows nodes to naturally discover new data without polling — the next keepalive automatically triggers an exchange.

---

## Download Guard (Deduplication)

- Before starting any download (`_act_solicit_replication` or `_on_file_chunk` assembly):
  - Check `storage.has_file(file_id)` — if already have the file locally, skip download
  - This prevents duplicate downloads when FILE_PUBLISH, FILE_ANNOUNCE, or registry exchanges re-trigger replication
- Web UI download button also checks `has_file()` before queueing

---

## Architecture Notes

- **Scheduler-driven:** All periodic work and responses flow through a single priority heap — no scattered timer threads
- **DB-backed state:** Peer book, file registry, and transfer progress are all persisted in SQLite
- **Thread-safe:** `enqueue()` / `cancel()` / `wake()` can be called from recv thread or scheduler thread
- **Stateless UDP engine:** No connection state in the network layer — all state in the DB
