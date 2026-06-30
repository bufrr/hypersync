# Hyperliquid P2P Sync Protocol

Reverse-engineered from a running Hyperliquid non-validator node (`hl-visor` / `hl-node`, closed-source Rust binaries) by packet capture, binary-string analysis, and live probing on our own infrastructure. The transport is **plaintext**, so no MITM/decryption was needed. This document describes the wire protocol in enough byte-level detail to build a gateway that lets a node sync **entirely through it** — including replaying the bootstrap from cache and answering the client-block RPC (this is what `hypersync` does).

> Scope: the gossip transport between nodes (state + block sync), not the EVM/exchange application layer. Inner bincode layouts are mapped only as far as a gateway needs (it forwards block bytes verbatim; the node verifies signatures end-to-end). Claims are marked **[confirmed]** (byte-verified on the wire) or **[inferred]** (from binary symbols / behaviour).

---

## 1. Transport & ports

- **Plaintext TCP** [confirmed]. A 70k+ packet capture showed no TLS; `hl-node` links only `lz4_flex` (no noise/rustls/x25519/AEAD). The only crypto is block signatures (secp256k1 / BLS) — integrity, not confidentiality.
- The node publishes ports **4000–4010**. Two matter for sync:
  - **4001** — heavy channel: the `abci_state` snapshot, the EVM-KVs snapshot, and the live block stream.
  - **4002** — gossip RPC: short request/response (height query, **client-block range query**, peer discovery).
- A relay/gateway should cover the whole 4000–4010 range. (Forwarding only 4001 once made the node's 4002 height query time out, so it never bootstrapped.)

## 2. Frame format [confirmed]

Every message is length-prefixed:

```
[u32 big-endian L][1 byte type][L bytes payload]
```

- **`L` counts ONLY the payload.** The 1 type byte is separate ⇒ total wire size = `4 + 1 + L`.
- `type = 0` — control: greeting, status/error codes, **client-block range request**.
- `type = 1` — data: `abci_state`, EVM KVs, blocks, **client-block batch response**.

## 3. HL compact varint [confirmed]

Integers in the bincode payloads use a compact varint (verified against real blocks and requests):

| lead byte | meaning |
|---|---|
| `< 0xfb` | the value itself (single byte) |
| `0xfb` | `u16` little-endian follows |
| `0xfc` | `u32` little-endian follows |
| `0xfd` | `u64` little-endian follows |
| `0xfe` | `u128` little-endian follows |

Consensus rounds (~1.35e9) are therefore emitted as `fc <u32 LE>`.

## 4. Greeting / handshake [confirmed]

The **dialing** side sends an 8-byte greeting right after TCP connect:

```
00 00 00 03 00 <send_abci> 00 00      = [L=3][type=0][ send_abci, broadcast_group=0, id=0 ]
```

decoding to `TcpGreeting { send_abci: bool, broadcast_group: BroadcastGroup, id: NodeId }`.

- `send_abci = 0x01` → "send me the full snapshot" — `00 00 00 03 00 01 00 00`. Used by a node booting from scratch. The peer then streams **abci_state → EVM KVs → live blocks** (§7).
- `send_abci = 0x00` → "live blocks only" — `00 00 00 03 00 00 00 00`. Used by a node that already has state, or by a relay collecting the live stream.

A serving peer starts streaming immediately. A peer that declines replies with a 1-byte status frame `[L=1][type=0][code]` and closes (`Rate limited by peer`, etc.).

**Sync-direction rule [confirmed].** A node ingests blocks **only from peers it dialed (outbound)**. On an inbound connection it *serves* the dialer; it does not accept pushed blocks. A gateway must therefore be an *outbound* peer of the node (listed in `root_node_ips`) — pushing into the node's listening port does nothing.

## 5. Compression [confirmed]

Large `type = 1` payloads are `lz4_flex` **`compress_prepend_size`**:

```
[u32 little-endian uncompressed_size][LZ4 block]
```

`lz4_flex`-compressed bytes are standard LZ4 block format, so a gateway can recompress with the same library and the node accepts it.

## 6. The two block serializations (critical) [confirmed]

The same logical block appears on the wire in **two different bincode serializations**. They are NOT interchangeable — this is the single most important detail for a gateway.

| | LIVE / consensus block (on 4001) | CLIENT-BLOCK element (in a 4002 response batch) |
|---|---|---|
| reached via | `send_abci:false` live stream / bootstrap tail | client-block range query response |
| leading byte | `0x00` discriminant, then the block | (none per element; the batch has one leading `0x00`) |
| **round offset** | decompressed **`[0x5e]`** = `0xfc`, u32 LE at `[0x5f..0x63]` | element_start **`+ 0x5d`** = `0xfc`, u32 LE at `+0x5e` |
| body | `{ signed_action_bundles, round, parent_round, abci_block, resps }` — the full block incl. `abci_block` (here ~200 KB) | `ClientBlock { proof, txs, commit_proof:{ child_qc, grandchild_qc } }` — compact (here ~7 KB) |
| size | varies with tx volume | varies; usually smaller |

They share only a ~2.6 KB common header (round, parent_round, qc, validator-set hash); after it they diverge in fields and length. **You cannot byte-transform a live block into a client-block element** — the element carries `commit_proof` (child/grandchild QCs) that only exists once *later* rounds commit, so it cannot be produced from a single live block in isolation. A gateway must obtain client-block elements from the client-block RPC, not from the live stream.

## 7. Bootstrap stream — `send_abci:true` on 4001 [confirmed]

A from-scratch node dials with `send_abci:true`; the peer streams, in order, on the one connection:

1. **abci_state** — one type=1 frame, ~**950 MB** lz4→msgpack (HyperCore consensus/exchange state; visible field names `exchange`, `locus`, `ctx`, matching the on-disk `.rmp`). The node logs `reading bytes for abci_stream recv greeting: X/950…`.
2. **EVM KVs** — the HyperEVM state checkpoint (`hyperliquid_data/evm_db_hub_slow/checkpoint/<height>/EvmState`), ~**3.5–3.7 GB**, streamed as a run of type=1 chunks of **variable size (≈0.2–1.2 MB)** right after the abci_state. The node logs `evm kvs` and, on failure, `failed to receive evm kvs: TcpRead::bincode err: LimitExceeded`.
3. **live blocks** — the normal live stream begins.

Total bootstrap ≈ **4.5 GB**. **The abci_state→EVM-KVs→live boundaries are internal to the (closed) framing and are NOT detectable from the wire** — EVM-KVs chunks and live blocks are both type=1 with overlapping sizes. Consequence for a gateway: don't try to parse the boundary. Either (a) relay the `send_abci:true` connection transparently and let the node find the boundaries, or (b) capture the byte stream verbatim and replay it verbatim (§12).

When the snapshot is certified and committed, the node writes `hyperliquid_data/visor_abci_state.json` (~225 B, `VisorAbciState { scheduled_freeze_height, consensus_time, wall_clock_time, reference_lag }`) — **this file appearing is the signal that bootstrap finished.**

## 8. Gossip RPC — port 4002 [confirmed]

Request/response, one short-lived TCP connection per exchange. Wrapped as `GossipRpcRequest { RpcRequest { content } }` / `GossipRpcResponse`.

### 8a. Client-block RANGE query (the catch-up workhorse)

**Request** (node→peer, type=0). Example captured frame:
```
00 00 00 0b  00  00 fc 8e 26 81 50  fc f1 26 81 50
└── L=11 ─┘ typ  └tag┘└start_round ┘└ end_round  ┘
```
Payload layout:
| off | field | encoding |
|---|---|---|
| +0 | variant tag | `0x00` (range-query variant) |
| +1 | `start_round` | varint `0xfc`+u32 LE |
| +6 | `end_round` | varint `0xfc`+u32 LE |

A **closed range `[start_round, end_round]`** (NOT the `BlocksAndTxs{after_round,last_block_hash}` variant — no 32-byte hash is on the wire for catch-up). Observed catch-up walks request contiguous ~100-round windows (`next.start == prev.end + 1`).

**Response** (peer→node, type=1, one frame). Payload = `lz4 compress_prepend_size` of:
```
[0x00 tag][count varint = N][element_0][element_1]…[element_{N-1}]
```
- `0x00` = `RpcResponse::ClientBlocks` discriminant.
- `count` = N = `end_round - start_round + 1`.
- elements (§6) are concatenated with **no per-element length prefix** (self-delimiting bincode); the consumer walks them, deriving N from the count / range. Each element's round at `element_start + 0x5d`.

The node logs `querying client blocks [X, Y]` → `got N client blocks first=X last=Y`.

### 8b. Other RPCs / errors
- **height query**: `querying height` → `got heights [ip→height]` (the node asks peers their height before deciding what to fetch).
- **query_peers**: peer discovery; the response is a peer table (IP list).
- **Error frames** [confirmed]: a type=0 frame whose payload is `0x03` + an ASCII reason, e.g. `client block round too large` (you requested rounds ahead of the peer's *committed* client-block store, which lags the live tip) or `Peer-only request`. A gateway answering from a cache should likewise only serve rounds it has, and return a comparable type=0 error otherwise.

## 9. Full node lifecycle after startup

1. **Read config** `~/override_gossip_config.json` (§11). `root_node_ips` are dialed; `try_new_peers` controls discovery.
2. **Dial roots**, send greetings; over 4002 run `query_peers` / `querying height` to learn peers and their heights.
3. If no local state: pick a peer and dial `send_abci:true` → receive **abci_state + EVM KVs** (§7). State is **rate-limited per source IP** (§10); the node retries other peers until one serves the full snapshot within its deadline.
4. **Finalize**: certify/commit the snapshot, write `visor_abci_state.json`.
5. **Catch-up**: walk `client blocks [X,Y]` range queries (§8a) from the snapshot round to the committed tip; apply each batch in order (`got N client blocks` → `applied block …`). Catch-up runs ahead of live until it nears the tip (then a request returns `client block round too large` and it waits).
6. **Steady state**: receive live blocks (type=1) on 4001 and apply them; `new app hashes reached quorum [(height, QuorumAppHash{…})]` confirms consensus. A synced non-validator keeps only ~1 active upstream in practice.
7. **Restart**: a fresh start re-runs 3–6 (re-fetches the ~4.5 GB snapshot). Internal symbols for the non-validator path: `node_bootstrap`, `nv_stream_apply_execution_state`, `nv_stream_forward_client_blocks`, `split_client_blocks`, `process_client_block`.

## 10. Rate limiting [confirmed]

- Serving the **abci_state** snapshot is **rate-limited per source IP** (binary string `abci state request rate limited`). Repeated full-state fetches from one IP get refused (a tiny type=0 status frame, or a slow/partial serve). Live blocks (`send_abci:false`) and client-block range queries are far less restricted.
- All containers behind one NAT share the host IP's quota. Recovery is per-(IP, peer) over ~tens of minutes; **rotating to a fresh peer is faster than waiting.** Observed serving rates range from ~50–74 MB/s (fast) down to peers that serve the header then stall — pick fast ones for a bootstrap.
- A peer's *committed client-block store lags the live tip*; requesting rounds beyond it returns `client block round too large` (§8b).

## 11. Config files

`~/override_gossip_config.json`:
```
GossipConfigInner {
  root_node_ips,      // peers dialed on startup/bootstrap (hot-reload does NOT re-dial new roots)
  try_new_peers,      // discover/dial peers beyond the root list
  chain,              // "Mainnet" / "Testnet"
  reserved_peer_ips,  // peers always allowed to connect IN
  n_gossip_peers,     // target outbound peer count (default 8; limited by rate-limits in practice)
  split_client_blocks // stream uncommitted mempool txs (needs all hops to enable it)
}
```
To point a node at a gateway: `{"root_node_ips":[{"Ip":"<gw>"}],"try_new_peers":false,"chain":"Mainnet"}` and restart. `~/override_public_ip_address` (optional, often absent) overrides the node's OWN advertised public IP — it is the node's identity, **not** a peer list. Ports 4001/4002 must be publicly reachable or peers deprioritize the node.

## 12. Gateway implications — how hypersync provides all of this

A node points `root_node_ips` at the gateway and connects to nothing else; the gateway fronts a pool of real peers (kept fresh + ranked by `peerd`, which discovers via `gossipRootIps` + a running node's logs and probes live-serving — never the rate-limited state).

- **Transparent failover (default).** The gateway relays the node ↔ a peer that is *currently serving the abci_state* (it peeks the first frame: a serving peer sends the >4 MB state; a declining one sends a tiny status frame → try the next). All of the node's connections are pinned to that one **active** peer, otherwise the 4002 client-block RPC is refused `Peer-only request` (the peer doesn't recognize a node it never served). Round-robin selection means each node/gateway restart rotates to a fresh state-server, dodging the per-IP rate-limit. The node finds the §7 boundaries itself; the gateway needn't understand them.
- **Cache cold-start (`--push`).** The gateway captures the full §7 bootstrap **verbatim** from a fast peer (speed-gate >10 MB/s; stop when the byte-rate collapses to the live trickle for ~12 s = bulk end) and holds it in RAM. On a node's `send_abci:true` it **replays the cached bytes** — the node cold-starts entirely from cache, **no peer state fetch, no rate-limit, no transparent state path**. Refreshed periodically. Verified: a node connected only to the gateway, 3.7 GB EvmState, finalized, syncing.
- **Client-block catch-up.** A cache-cold-started node has no peer relationship, so the gateway issues the node's §8a range query to a real peer **on the node's behalf** and forwards the response verbatim (the node verifies signatures). It does **not** synthesize elements from live blocks (§6 makes that impossible).
- **Multi-source live merge (`--push`).** The gateway connects to several pool peers (`send_abci:false`), de-duplicates by round (round at `0x5e`, §6), and forwards the **fastest** copy of each block — gap-free, lower latency.

Because blocks are signature-verified end-to-end by the node, the gateway forwards/relays block bytes byte-for-byte and never needs to re-sign or fully decode them.

## 13. Consensus messages (validators)

Seen in the binary; a non-validator forwards but does not vote: `BlockTimeout`, `Tc` (timeout cert), `Qc` (quorum cert), `Heartbeat`/`HeartbeatAck`, `Tx`, `NextProposer`, `jailed_validators`, `round_to_jailed_validators`. Validation-error symbols on `ClientBlock` confirm its fields: `ClientBlockQcRound`, `ClientBlockQcHash`, `ClientBlockTx`, `ClientBlockTxHashes`, `ClientBlockTime`, `ClientBlockMissingCommitProof`, `ChildQcCommitProof`, `GrandchildQcCommitProof`.
