# hypersync

A Hyperliquid P2P sync gateway — a multi-peer, health-monitored **failover transparent proxy** that keeps a Hyperliquid node synced with low lag.

A local HL node points its `root_node_ips` at hypersync; hypersync transparently relays the node's P2P traffic to one of several real upstream peers, monitors that peer's health, and automatically fails over to another peer if it stalls or dies — so the node keeps syncing even when an individual peer degrades.

## Why

Hyperliquid non-validator nodes sync by dialing peers and streaming consensus / client blocks. A single slow or flaky peer can cause sync lag. hypersync sits in front of the node as its peer, fronting a pool of real upstream peers with automatic failover, decoupling the node from any one peer's problems.

## Modes

- `gateway <peers-file> [--push] [--live N]` — **full P2P gateway** (recommended): the node points `root_node_ips` at it and syncs the real mainnet ONLY through the gateway, which fronts a whole pool of real peers read from `<peers-file>` (maintained by `peerd.sh`, refreshed live). On a node bootstrap it transparently relays a peer that is *currently* serving the abci_state (peeks the first frame; rotates to a fresh state-server each time, so node/gateway restarts never hit the per-IP abci_state rate-limit). All of a node's connections are pinned to that one **active** peer so the client-block RPC isn't rejected with "Peer-only request". Default mode is transparent failover. `--push` turns on the **cache + block-push** mode: the gateway (a) captures the full bootstrap (abci_state + EVM KVs, ~4.5 GB) verbatim from a fast peer and **caches it in RAM**, so a node's cold-start is replayed entirely from cache — **no peer state fetch, no per-IP rate-limit, no transparent state path**; (b) fetches client-block catch-up from a real peer on the node's behalf and forwards it (a node that cold-started from cache has no peer relationship of its own); (c) merge-injects the fastest copy of each live block from `--live N` peers (default 5; the rest of the pool is backup, and each shadow source reconnects with backoff if it drops). A node can thus sync **entirely through the gateway**, connecting to nothing else. (Default, without `--push`, is transparent failover: the gateway relays a peer that is actually serving the abci_state, so the node streams the bootstrap straight through — the gateway never has to understand HL's closed bootstrap framing.)
- `proxy <peer1,peer2,...> [--push]` — **failover transparent proxy**: listens on 4000-4010, relays to the active upstream, health-monitors it, and fails over to the next healthy peer (bad peers skipped with a cooldown; no oscillation). Like `gateway` but with a fixed upstream list instead of the live `peerd` pool. Add `--push` to merge-push live blocks from *all* peers on 4001 (round-dedup, fastest-block-first) for lower block-reception latency.

`peerd.sh` is the companion daemon: it discovers peers (the `gossipRootIps` API + harvesting a running node's logs) and probes each for **live-block serving** (`send_abci:false`, cheap and not rate-limited — it never probes the rate-limited abci_state), writing a ranked pool to `peers.json` that `gateway` reads and refreshes.
- `relay <port> <upstream>` — plain transparent relay of ports 4000-4010 to a single upstream.
- `<port> <peer1,peer2,...>` (no subcommand) — multi-upstream live-block merge with round-based dedup (freshest block wins, gap-free).
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

The hot path (lz4 round-decode + dedup) is heavily optimized. Reading a block's consensus round needs only the first decompressed bytes, and the lz4 literal run at the block head normally covers them — so the round is read straight from the compressed literals with **no decode buffer at all** (it falls back to a bounded decode otherwise). Dedup is a lock-free, hash-free sliding window: a power-of-two `AtomicU32` array indexed by `round & mask` and updated with a single atomic swap — no mutex, no hashing, no ring-buffer eviction. ~**135M frames/s** in the included `bench` — about **4.3x** the previous already-optimized baseline (and far higher vs a naive full-decompress).
