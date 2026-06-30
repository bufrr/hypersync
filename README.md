# hypersync

A Hyperliquid P2P sync gateway — a multi-peer, health-monitored **failover transparent proxy** that keeps a Hyperliquid node synced with low lag.

A local HL node points its `root_node_ips` at hypersync; hypersync transparently relays the node's P2P traffic to one of several real upstream peers, monitors that peer's health, and automatically fails over to another peer if it stalls or dies — so the node keeps syncing even when an individual peer degrades.

## Why

Hyperliquid non-validator nodes sync by dialing peers and streaming consensus / client blocks. A single slow or flaky peer can cause sync lag. hypersync sits in front of the node as its peer, fronting a pool of real upstream peers with automatic failover, decoupling the node from any one peer's problems.

## Modes

- `proxy <peer1,peer2,...> [--push]` — **failover transparent proxy** (production mode): listens on 4000-4010, relays to the active upstream, health-monitors it, and fails over to the next healthy peer (bad peers skipped with a cooldown; no oscillation). **Default = transparent proxy + failover only (no block push).** Add `--push` to additionally merge-push live blocks from *all* peers on 4001 (round-dedup, fastest-block-first) for lower block-reception latency.
- `relay <host>` — plain transparent multi-port relay to a single upstream.
- `serve <port> <peers>` — multi-upstream live-block merge with round-based dedup (freshest block wins, gap-free).
- `cache <port> <upstream>` — fetches the abci_state snapshot once, caches it, serves connecting nodes at local speed (beats the abci_stream deadline).
- `mock <bind> <dir> <start> <end>` — replays captured blocks for testing.
- `bench <dir> <iters>` — benchmarks the hot path (lz4 decode + round dedup).

## Build & run

```sh
cargo build --release
./target/release/hypersync proxy 1.2.3.4,5.6.7.8,9.10.11.12
```

Point the node's `override_gossip_config.json` at hypersync and restart it so the node dials the gateway:

```json
{"root_node_ips": [{"Ip": "<hypersync-host>"}], "try_new_peers": false, "chain": "Mainnet"}
```

## HL P2P protocol (reverse-engineered)

- Plaintext TCP, ports 4000-4010 (4001 = abci_state / block heavy channel, 4002 = gossip RPC).
- Frame: `[u32 BE length][1 type byte][payload]`; type 0 = control, type 1 = data.
- Greeting: `TcpGreeting { send_abci, broadcast_group, id }` (8 bytes).
- Blocks are lz4-compressed; the consensus round sits at decompressed offset `0x5e`.
- Bootstrap is **interactive RPC** (query height → request client blocks → commit). A node only ingests blocks from peers **it dials** (outbound) — so hypersync must be an outbound peer (configured in `root_node_ips`), never an inbound pusher.

## Performance

The hot path (decode + dedup) is heavily optimized vs a naive full-decompress implementation: bounded lz4 decode (only the first ~99 bytes are needed to read the round), FxHash dedup over a cache-resident window, flat ring-buffer eviction. ~**795x** throughput in the included `bench`.
