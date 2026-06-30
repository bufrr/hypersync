# Hyperliquid P2P Protocol

Reverse-engineered from a running Hyperliquid non-validator node (`hl-visor` / `hl-node`, closed-source Rust binaries) by packet capture and binary string analysis on our own infrastructure. The transport is **plaintext**, so no MITM/decryption was needed. This document describes the wire protocol well enough to build a transparent proxy / block relay (hypersync).

> Scope: the gossip transport between nodes (block sync), not the EVM/exchange application layer. Field names are taken from the binary's symbols; exact inner (bincode) layouts are only partially mapped — a relay does not need them because the node verifies blocks by signature and a relay forwards bytes verbatim.

## 1. Transport & ports

- **Plaintext TCP.** A capture of 70k+ packets showed no TLS; the `hl-node` binary links only `lz4_flex` (no noise/rustls/x25519/AEAD). The only cryptography is block signatures (secp256k1 / BLS) — integrity, not confidentiality.
- The node container publishes the port range **4000–4010**. Two ports matter for sync:
  - **4001** — heavy channel: the `abci_state` snapshot and the block stream.
  - **4002** — gossip RPC channel: lightweight request/response (height queries, client-block requests, peer discovery).
- Which port carries what depends on the connection/role; a relay should forward the whole 4000–4010 range to be safe (forwarding only 4001 made the node's height-query RPC on 4002 time out, so it never reached bootstrap).

## 2. Frame format

Every message on a connection is length-prefixed:

```
[u32 big-endian L][1 byte type][L bytes payload]
```

- Total frame size on the wire = `5 + L` (the type byte is **not** counted in `L`).
- `type = 0` — control: greeting, status/reject codes.
- `type = 1` — data: `abci_state`, blocks, RPC responses.

## 3. Greeting (handshake)

The **dialing** side sends an 8-byte greeting immediately after the TCP connect:

```
00 00 00 03 00 <send_abci> 00 00
```

i.e. `[L=3][type=0][payload = <send_abci> 00 00]`, decoding to the binary struct:

```
TcpGreeting { send_abci: bool, broadcast_group: BroadcastGroup, id: NodeId }
```

- `send_abci = 0x01` → "send me the full abci_state snapshot" (used by a node that needs to bootstrap from scratch). Bytes: `00 00 00 03 00 01 00 00`.
- `send_abci = 0x00` → "send me live blocks only" (a node that already has state, or a relay collecting the live stream). Bytes: `00 00 00 03 00 00 00 00`.

A serving peer immediately starts streaming. A peer that will not serve replies with a 1-byte status frame `[L=1][type=0][code]` (e.g. `04` / `05`) and closes.

**Sync direction rule (important).** A node ingests blocks **only from peers it dialed (outbound)**. On an inbound connection (a peer dialed *into* the node) the node *serves* the dialer; it does **not** accept blocks pushed at it. Consequence for a gateway: it must be an *outbound* peer of the node (listed in `root_node_ips` so the node dials it). Pushing blocks into the node's listening port does not work.

## 4. Compression & block encoding

- Large `type = 1` payloads are `lz4_flex` **`compress_prepend_size`**: `[u32 little-endian uncompressed_size][LZ4 block]`.
- **abci_state**: decompresses to **msgpack** (~948 MB; visible field names `exchange`, `locus`, `ctx`, … matching the on-disk `.rmp`). It is the full state snapshot a node loads first when bootstrapping.
- **Blocks** (client blocks / live blocks): decompress to a bincode-style structure. For relaying you only need the **consensus round**, which sits at a fixed offset in the decompressed block:

  ```
  decompressed[0x5e] == 0xfc   then   round = u32 LE at decompressed[0x5f..0x63]
  ```

  (`0xfc` is the bincode tag for "u32 follows".) The round increments per block and is used for de-duplication when merging multiple peers. hypersync decodes only the first `0x63` bytes (bounded LZ4 decode) to read this — it never decompresses the whole block.

## 5. Gossip RPC (port 4002)

Request/response, wrapped as `GossipRpcRequest { RpcRequest { content: RpcRequestContent } }` and `GossipRpcResponse`. Observed `RpcRequestContent` variants (from binary enums):

- **`query_peers`** — peer discovery.
- **`BlocksAndTxs { after_round, last_block_hash }`** / `QueryClientBlocks` — request client blocks after a given round (catch-up). Logs: `querying client blocks [X, Y]` → response `got N client blocks first=X last=Y`.
- **`BlockTx`** — block/transaction fetch.
- Height query: `querying height` → `got heights [ip_to_heights: Map{ip -> height}]` (the node asks peers their height before deciding what to fetch).

Status / error codes seen: `RpcNotFound`, `RpcPeer`, `RpcRoundRobin`, `PeerTimeout`, `PeerRateLimit`, `PeerNoQuorum`, `Variant/NotFound`.

## 6. ClientBlock

```
Signed { signature, content: ClientBlock }
ClientBlock { qc_round, qc_hash, txs, tx_hashes, time, commit_proof { child, grandchild_qc } }
```

Validation-error symbols (indicate the fields the node checks): `ClientBlockQcRound`, `ClientBlockQcHash`, `ClientBlockTx`, `ClientBlockTxHashes`, `ClientBlockTime`, `ClientBlockMissingCommitProof`, `ClientBlockMissingTxCommitProof`, `ChildQcCommitProof`, `GrandchildQcCommitProof`.

The node tracks `last_recv_round`; a block whose `parent_round < last_recv_round` triggers `received old client block, is peer behind?` (redundant gossip — harmless).

## 7. Bootstrap flow (interactive)

Bootstrap is an **interactive RPC sequence**, not a one-way push (a one-way replay of cached bytes fails — the node cannot get its block requests answered and never finalizes):

1. Node dials a peer and sends the greeting (`send_abci:true` if it has no state).
2. Node queries peer height (4002): `querying height` → `got heights`.
3. Peer serves the `abci_state` snapshot (4001, ~948 MB lz4→msgpack).
4. Node requests client blocks to catch up (4002): `BlocksAndTxs { after_round, … }` → `got N client blocks`, applied in order.
5. When the snapshot is certified and committed, the node writes `hyperliquid_data/visor_abci_state.json` (~225 B) — this file appearing **is the signal that bootstrap finished**. It holds `VisorAbciState { scheduled_freeze_height, consensus_time, wall_clock_time, reference_lag }`.
6. Steady state: the node receives live blocks (type=1) on 4001 and applies them; `new app hashes reached quorum [(height, QuorumAppHash{ app_hash, user->signature })]` confirms consensus.

Non-validator stream symbols: `node_bootstrap`, `nv_stream_apply_execution_state`, `nv_stream_forward_client_blocks`, `split_client_blocks` (`DefaultSplitClientBlocks`), `forward_client_blocks`, `process_client_block`.

## 8. Rate limiting

- Serving the `abci_state` snapshot is **rate-limited per source IP** (binary string `abci state request rate limited`); repeated full-state requests from one IP get rejected with a status code.
- Observed nuance with NAT: a node rejected direct connections from its own docker subnet but served the same host via the docker-proxy NAT source — i.e. the limit keys on the *observed* source address.
- Live blocks (`send_abci:false`) are far less restricted than the full snapshot.

## 9. Config (`override_gossip_config.json`)

```
GossipConfigInner {
  root_node_ips,      // peers the node dials on startup/bootstrap (a hot-reload does NOT re-dial new roots)
  try_new_peers,      // whether to discover/dial peers beyond the root list
  chain,              // "Mainnet" / ...
  reserved_peer_ips,
  n_gossip_peers,     // target outbound peer count (default 8; limited in practice by rate-limits/rejections)
  split_client_blocks
}
```

A synced non-validator typically maintains only ~1 active upstream in steady state. To make the node use a gateway, put the gateway IP in `root_node_ips` and restart (so the node dials it).

## 10. Consensus messages (validators)

Seen in the binary; a non-validator receives/forwards but does not vote: `BlockTimeout`, `Tc` (timeout cert), `Qc` (quorum cert), `Heartbeat` / `HeartbeatAck`, `Tx`, `NextProposer`, `jailed_validators`, `round_to_jailed_validators`.

## 11. Relaying implications (why hypersync works)

- A relay can be byte-transparent: it forwards the greeting and every frame both ways; the node and the real peer complete the interactive bootstrap through it. No inner decoding required (signatures are verified end-to-end by the node).
- For multi-source acceleration, parse only the frame header + the round at `0x5e`: forward each round once from whichever peer delivered it first (de-dup), pass control/abci_state/RPC frames through from the chosen "active" peer.
- Because the node only ingests from peers it dials, the gateway must be an outbound peer of the node (in `root_node_ips`).
