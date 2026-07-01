# hypersync

A Hyperliquid P2P sync gateway — a multi-peer, health-monitored **failover transparent proxy** that keeps a Hyperliquid node synced with low lag.

A local HL node points its `root_node_ips` at hypersync; hypersync transparently relays the node's P2P traffic to one of several real upstream peers, monitors that peer's health, and automatically fails over to another peer if it stalls or dies — so the node keeps syncing even when an individual peer degrades.

## Why

Hyperliquid non-validator nodes sync by dialing peers and streaming consensus / client blocks. A single slow or flaky peer can cause sync lag. hypersync sits in front of the node as its peer, fronting a pool of real upstream peers with automatic failover, decoupling the node from any one peer's problems.

## Modes

- `gateway <peers-file> [--push] [--cache] [--live N]` — **full P2P gateway** (recommended): the node points `root_node_ips` at it and syncs the real mainnet ONLY through the gateway, which fronts a whole pool of real peers read from `<peers-file>` (maintained by `peerd.sh`, refreshed live). All of a node's connections are pinned to one **active** peer so the client-block RPC isn't rejected with "Peer-only request". The flags are orthogonal and compose:
  - **default (no flags)** — pure transparent failover: on bootstrap the gateway relays a peer that is *currently* serving the abci_state (peeks the first frame; rotates to a fresh state-server each time, so node/gateway restarts never hit the per-IP abci_state rate-limit), and splices live blocks + gossip RPC to that active peer. The gateway never has to understand HL's closed bootstrap framing.
  - **`--push`** — layer multi-source **block-push** on top of the transparent backbone: the active peer still relays the full stream (bootstrap + live), and the gateway *additionally* merge-injects the fastest copy of each live block from `--live N` peers (default 5; deduped by round, shadow sources reconnect with backoff). If the **active peer dies, serve_push tears the session down so the node reconnects and the gateway pins a fresh active** (proxy-style failover); shadows are best-effort accelerators only. The node keeps a real peer relationship, so its client-block RPC (4002) is spliced straight to the active peer — no fragile fetch-forward. This is the recommended production mode: transparent failover **and** push together.
  - **`--cache`** — additionally capture the full bootstrap (abci_state + EVM KVs, ~4.5 GB) verbatim from a fast peer (hedged race across the pool, early-abort on slow peers) and **cache it in RAM**, so a node's cold-start replays entirely from cache — no peer state fetch, no per-IP rate-limit. Trade-off: a cache-cold-started node has **no peer relationship of its own**, so its 4002 catch-up is fetched from a real peer on its behalf and forwarded (more fragile than a splice). Opt-in for the specific case of avoiding the ~950 MB re-fetch on node restart.
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
