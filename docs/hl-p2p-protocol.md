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
- **What a non-validator actually dials** [confirmed 2026-11-15, stock mainnet node, ~40 min captured]: only **4001** and **4002**. It never connected to **4000**, directly or through a gateway, and opened nothing on 4003–4010. It also dials one hard-coded-looking host on **4005** (`35.79.176.46`, not in `gossipRootIps` or any peer list) about every 2.2 s; that host never answered on any port, and the attempt bypasses `root_node_ips`, so it leaves from the node's own address even behind a gateway. Probably a validator-channel connect a non-validator can't complete.

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
| **round offset** | **variable since 2026-10**: the first `0xfc R 0xfc R-1 0xfc R-2` varint triple (round, parent, grandparent; seen at decompressed `0x71..0xd1`), found by scanning — see `protocol::find_block_round`. Before 2026-10 it was fixed at decompressed **`[0x5e]`** = `0xfc`, u32 LE at `[0x5f..0x63]` | element_start **`+ 0x5d`** = `0xfc`, u32 LE at `+0x5e` (pre-2026-10 observation; the gateway does not parse client-block rounds) |
| body | `{ signed_action_bundles, round, parent_round, abci_block, resps }` — the full block incl. `abci_block` (here ~200 KB) | `ClientBlock { proof, txs, commit_proof:{ child_qc, grandchild_qc } }` — compact (here ~7 KB) |
| size | varies with tx volume | varies; usually smaller |

They share only a ~2.6 KB common header (round, parent_round, qc, validator-set hash); after it they diverge in fields and length. **You cannot byte-transform a live block into a client-block element** — the element carries `commit_proof` (child/grandchild QCs) that only exists once *later* rounds commit, so it cannot be produced from a single live block in isolation. A gateway must obtain client-block elements from the client-block RPC, not from the live stream.

## 7. Bootstrap stream — `send_abci:true` on 4001 [confirmed]

A from-scratch node dials with `send_abci:true`; the peer streams, in order, on the one connection:

1. **abci_state** — one type=1 frame, ~**950 MB** in mid-2026 / ~**1.29 GB** in 2026-10, lz4→msgpack (HyperCore consensus/exchange state; visible field names `exchange`, `locus`, `ctx`, matching the on-disk `.rmp`). The node logs `reading bytes for abci_stream recv greeting: X/950…`.
2. **EVM KVs** — the HyperEVM state checkpoint (`hyperliquid_data/evm_db_hub_slow/checkpoint/<height>/EvmState`), ~**3.5–3.7 GB** in mid-2026 (the rest of the ~5.8 GB total in 2026-10), streamed as a run of type=1 chunks of **variable size (≈0.2–1.2 MB)** right after the abci_state. The node logs `evm kvs` and, on failure, `failed to receive evm kvs: TcpRead::bincode err: LimitExceeded`.
3. **live blocks** — the normal live stream begins.

Total bootstrap ≈ **5.8 GB** on mainnet in 2026-10 (abci_state ~1.29 GB + EVM KVs; it was ~4.5 GB in mid-2026 and keeps growing). **The abci_state→EVM-KVs→live boundaries are internal to the (closed) framing and are NOT detectable from the wire** — EVM-KVs chunks and live blocks are both type=1 with overlapping sizes. Consequence for a gateway: don't try to parse the boundary. Either (a) relay the `send_abci:true` connection transparently and let the node find the boundaries, or (b) capture the byte stream verbatim and replay it verbatim (§12).

### 7a. What a bootstrap looks like in practice [confirmed 2026-11-15]

Measured on a fresh mainnet node (Linode 16-core / 64 GB, Tokyo) bootstrapping through the gateway, with a capture on both sides:

| Phase | On the wire | In the node's log | Duration at ~24 MB/s |
|---|---|---|---|
| Greeting | node → peer: 8 bytes `00 00 00 03 00 01 00 00`. **The node never sends another byte on this connection**, through the whole bootstrap and the live stream after it. | `connecting to peer`, `connected to abci stream from <peer>:4001` | — |
| abci_state | peer → node: one type=1 frame, **1,397–1,399 MB** (two servers, 2026-11-15) | `reading bytes for abci_stream recv greeting: X/Y` every 4 MB | ~60 s |
| Load | peer keeps sending EVM KVs; **the node stops reading** while it parses the state, so its socket fills and every write to it stalls | `received abci greeting from <peer>:4001`, then `computed l1 hash: L1Hash(0x…)` ~1 s later | the pause measured 57 s (fresh node) to 145 s (after a wipe) |
| EVM KVs | the rest, ~4.4 GB of type=1 chunks | **no progress lines at all**: nothing is logged for the whole phase, however long it takes | ~3 min |
| Finalize | — | `visor_abci_state.json` appears; hl-visor stops logging `could not load VisorAbciState` | seconds |
| Catch-up | 4002: one connection per 11-byte range query, 100 rounds per answer (~13–22 MB), ~5 MB/s; 4001 already carries live blocks | `starting bootstrap @@ [start_round …] @ [end_round …]`, `querying client blocks [X, Y]`, `added client block batch during bootstrap`, `applied block N` | ~10 min to the tip (~15 min after the abci_state) |
| Live | 4001: ~1.3–1.9 MB/s inbound; 0 bytes outbound on every port; 4002 idle | `applied block N` several per second, `new abci digests reached quorum` | — |

Things that go wrong in the EVM KV phase, and why it matters to anything between the node and the peer:

- **There is no resume.** If the connection ends anywhere before the EVM KVs are through, the node logs `Evm Kvs read error: early eof` (or `could not read abci state from <peer>: early eof` earlier on) and starts over with a new `send_abci:true` connection, downloading the abci_state again. A relay that cuts the node during its load pause therefore produces an endless re-download loop (hypersync before 2026-11-09: a flat 30 s node-write bound).
- **A server can serve the abci_state fast and then trickle the EVM KVs.** One peer delivered the abci_state normally and then sent the EVM KVs at **0.26 MB/s** for over 10 minutes (~2 h to finish) without ever going silent; the node logged nothing and waited. The next server delivered 24 MB/s. Silence-based timeouts don't catch this; only a rate floor does.
- Because the node logs nothing during this phase, the only way to see progress is from the outside: bytes on the 4001 connection (`ss -tni`), or the gateway's relay counters.
- Peers refuse or trickle to an IP whose own public 4001/4002 don't answer (§8c, §11): a fresh IP with nothing listening got `early eof` right after its greeting from 49 of 96 peers; with probe answers up, only 1.

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
- **Structured error replies** [confirmed 2026-10-26]: a peer can also answer a range query with a short bincode error instead of the ASCII form. Seen for `client block round too large`: a 13-byte payload `00 01 06 fc <requested round u32 LE> fc <committed tip u32 LE>` (hl-node logs it as `client block round too large: <requested> > <tip>`). It must be treated like the ASCII error, not as a batch: a lagging peer returns it at once and would otherwise outrace a slower peer's real batch.
- **Structured Peer-only** [confirmed 2026-10-28]: mainnet peers now refuse a range query from an IP with no 4001 relationship with the 3-byte payload `00 01 00` (error, variant 0) instead of the ASCII `Peer-only request`. The same peers serve the range once a 4001 session is open, and answer a future range with the structured round-too-large (variant 6).

### 8c. What peers send to a node's public ports [confirmed 2026-10-28]

One hour of everything except the gateway reaching node1's public 4001/4002 (node1 dials only the
gateway, so all of it is other peers' doing):

| Who | Count | What |
|---|---|---|
| QuickNode sentry (one of three) | 344 + 344, every ~10.5 s | 4001: connect, close without a byte. 4002: `query_peers` (`00 00 00 01 00 01`). Nothing else. |
| Pool peers | ~4,200 + ~4,300 | the same reachability check and `query_peers` |
| Pool peers | 243 | 4002 height query `00 00 00 01 00 02`; answered `02 01 fc <u32> fc <height u32> …` to anyone |
| Pool peers | 20 | 4002 range queries (§8a); node1 served 128 MB |
| Pool peers | 94 | 4001 greetings, 29 of them `send_abci:true`; node1 accepted the live ones (status `03`) and uploaded 6.5 GB in the hour |

So a node's public ports mostly answer reachability checks, and otherwise serve strangers. The
gateway answers the checks in its place for every non-whitelisted source (README "Public probe answers"):
connect-only probes are held until the peer closes, a greeting gets the peer-full status
`00 00 00 01 00 04`, `query_peers` an empty table `00 00 00 02 00 01 00`, any other RPC the
structured Peer-only (§8b). No block is served.

## 9. Full node lifecycle after startup

1. **Read config** `~/override_gossip_config.json` (§11). `root_node_ips` are dialed; `try_new_peers` controls discovery.
2. **Dial roots**, send greetings; over 4002 run `query_peers` / `querying height` to learn peers and their heights.
3. If no local state: pick a peer and dial `send_abci:true` → receive **abci_state + EVM KVs** (§7). State is **rate-limited per source IP** (§10); the node retries other peers until one serves the full snapshot within its deadline.
4. **Finalize**: certify/commit the snapshot, write `visor_abci_state.json`.
5. **Catch-up**: walk `client blocks [X,Y]` range queries (§8a) from the snapshot round to the committed tip; apply each batch in order (`got N client blocks` → `applied block …`). Catch-up runs ahead of live until it nears the tip (then a request returns `client block round too large` and it waits).
6. **Steady state**: receive live blocks (type=1) on 4001 and apply them; `new app hashes reached quorum [(height, QuorumAppHash{…})]` confirms consensus. A synced non-validator keeps only ~1 active upstream in practice.
7. **Restart**: a fresh start re-runs 3–6 (re-fetches the ~5.8 GB bootstrap). Internal symbols for the non-validator path: `node_bootstrap`, `nv_stream_apply_execution_state`, `nv_stream_forward_client_blocks`, `split_client_blocks`, `process_client_block`.

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

A node points `root_node_ips` at the gateway and connects to nothing else; the gateway fronts a pool of real peers (kept fresh + ranked by `peerd`, which discovers via `gossipRootIps` plus a `query_peers` crawl of the gossip peer graph, optionally supplemented by the node's own `tcp_lz4_stats/<date>` files when mounted/copied, and probes live-serving -- never the rate-limited state).

- **Transparent failover (default).** The gateway relays the node ↔ a peer that is *currently serving the abci_state* (it peeks the first frame: a serving peer sends the >4 MB state; a declining one sends a tiny status frame → try the next). All of the node's connections are pinned to that one **active** peer, otherwise the 4002 client-block RPC is refused `Peer-only request` (the peer doesn't recognize a node it never served). Round-robin selection means each node/gateway restart rotates to a fresh state-server, dodging the per-IP rate-limit. The node finds the §7 boundaries itself; the gateway needn't understand them.
- **Cache cold-start (`--cache`).** The gateway captures the full §7 bootstrap **verbatim** from a fast peer (speed-gate >5 MB/s; stop when the byte-rate collapses to the live trickle for ~12 s = bulk end) and holds it in memory, with a persisted cache file (`bootstrap.cache` beside the peer file, or `/tmp/hypersync-bootstrap.cache` if that directory is read-only; override with `HYPERSYNC_BOOT_CACHE`). On a node's `send_abci:true` it **replays fresh cached bytes** (<=8 minutes old) -- the node cold-starts from cache with **no peer state fetch**. The gateway refreshes cache every 4 minutes and retries failed fresh refreshes after up to 30 seconds, shortening the retry as the cache approaches expiry; stale cache is ignored and the node transparently bootstraps from a real peer, while the gateway can tee that successful fallback stream back into cache.
- **Bootstrap relay timing.** Writes to the node on a bootstrap connection may stall up to 300 s during its first 15 minutes (the §7a load pause), 30 s after that. The upstream must deliver at least 1 MB/s, measured over 120 s of time spent waiting on it (so the node's load pause doesn't count against it); past 5.2 GB, where the EVM-KV tail and the live stream may overlap, the floor drops to 400 KB/s, and past 7 GB (certainly live) pacing stops. hl-node gives the whole abci_state frame **~60 s from its own connect**, and the gateway's selection time counts against it (a steady 27 MB/s server picked 14 s after the greeting was dropped at 60.1 s, 1,282 of 1,400 MB): a candidate must show from its 64 MB prefetch rate that it can finish the frame in what is left minus 5 s, or selection moves on (`bootstrap candidate <peer> too slow for the node's abci_state deadline`). It also has a stall deadline inside the frame (it dropped a server that stalled after 532 MB within ~20 s, and one throttled to 0.5 MB/s within ~33 s: `could not read abci state ... tcp_read_exact: deadline has elapsed`); a relay that ends there counts as the server's failure too. A server cut for trickling, or dropped inside the abci_state, is skipped as a bootstrap source for 20 minutes (a priority peer for 5: it is trusted, and a node restarting mid-abci_state marks its server too), remembered or not, so the node's reconnects land elsewhere. When every candidate was cut recently, the next relay keeps a slow server with only the silence cut: hl-node never resumes a bootstrap, so cutting again would only restart it on another trickler, forever.
- **Client-block catch-up.** A cache-cold-started node has no peer relationship, so the gateway issues the node's §8a range query to real peers **on the node's behalf** and streams the response verbatim (the node verifies signatures). 4002 always uses this controlled path so the gateway can retry `Peer-only request`, rotate away from peers that cannot serve old requested ranges (`client block round too small`), and rotate through candidates that report terminal range responses such as `no client blocks to serve` or `client block round too large`; only if no batch is available within the retry budget does it forward a terminal tip/wait error frame to the node. It does **not** synthesize elements from live blocks (§6 makes that impossible). Because a node asks for the next contiguous range only after processing the last, the gateway prefetches that next range while the node works (only well behind the tip); under heavy blocks the node's sequential walk alone is slower than the chain.
- **Active live stream (`--push`).** The gateway keeps the active 4001 stream transparent for correctness and tears the session down if it stalls so the node reconnects to a fresh active. Shadow multi-source injection is currently disabled: round-only dedup does not prove proposer/fork correctness.

Because blocks are signature-verified end-to-end by the node, the gateway forwards/relays block bytes byte-for-byte and never needs to re-sign or fully decode them.

## 13. Consensus messages (validators)

Seen in the binary; a non-validator forwards but does not vote: `BlockTimeout`, `Tc` (timeout cert), `Qc` (quorum cert), `Heartbeat`/`HeartbeatAck`, `Tx`, `NextProposer`, `jailed_validators`, `round_to_jailed_validators`. Validation-error symbols on `ClientBlock` confirm its fields: `ClientBlockQcRound`, `ClientBlockQcHash`, `ClientBlockTx`, `ClientBlockTxHashes`, `ClientBlockTime`, `ClientBlockMissingCommitProof`, `ChildQcCommitProof`, `GrandchildQcCommitProof`.
