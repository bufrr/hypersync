# hypersync

A Hyperliquid P2P sync gateway — a multi-peer, health-monitored **failover transparent proxy** that keeps a Hyperliquid node synced with low lag.

A local HL node points its `root_node_ips` at hypersync; hypersync transparently relays the node's P2P traffic to one of several real upstream peers, monitors that peer's health, and automatically fails over to another peer if it stalls or dies — so the node keeps syncing even when an individual peer degrades.

## Why

Hyperliquid non-validator nodes sync by dialing peers and streaming consensus / client blocks. A single slow or flaky peer can cause sync lag. hypersync sits in front of the node as its peer, fronting a pool of real upstream peers with automatic failover, decoupling the node from any one peer's problems.

## Modes

- `gateway <peers-file> [--push] [--cache] [--live N]` — **full P2P gateway** (recommended): each node points `root_node_ips` at it and syncs the real mainnet ONLY through the gateway, which fronts a whole pool of real peers read from `<peers-file>` (maintained by `peerd`, refreshed live). The gateway serves **multiple downstream nodes concurrently**: session state (active-peer pin, cache-replay cooldown, live round floor) is kept **per node, keyed by the node's source IP** — so run one node per egress IP (two nodes NAT'd through one IP would be conflated into one session). All of a node's connections are pinned to that node's **active** peer so the client-block RPC isn't rejected with "Peer-only request". The flags are orthogonal and compose:
  - **default (no flags)** — pure transparent failover: on bootstrap the gateway relays a peer that is *currently* serving the abci_state (peeks the first frame; rotates to a fresh state-server each time, so node/gateway restarts never hit the per-IP abci_state rate-limit), and splices live blocks + gossip RPC to that active peer. The gateway never has to understand HL's closed bootstrap framing.
  - **`--push`** — route 4001 live/resume traffic through `serve_push`: the active peer still relays the full stream and owns the peer relationship, and if the active stream stalls or dies the session is torn down so the node reconnects and the gateway pins a fresh active. Shadow multi-source block injection is currently disabled because round-only dedup cannot prove proposer/fork correctness; `--live N` is retained as a future capacity knob.
  - **`--cache`** — additionally capture the full bootstrap (abci_state + EVM KVs, ~4.5 GB) verbatim from a fast peer (hedged race across the pool, early-abort on slow peers) and cache it in memory plus disk. The default disk path is `bootstrap.cache` next to the peer file, falling back to `/tmp/hypersync-bootstrap.cache` if that directory is read-only; override with `HYPERSYNC_BOOT_CACHE`. Fresh cache entries (<=30 minutes old) are replayed to cold-starting nodes; the gateway refreshes cache every 10 minutes and retries failed refreshes after 2 minutes. Stale cache is ignored and the node transparently bootstraps from a real peer, while the gateway tees that successful fallback stream back into cache. Trade-off: a cache-cold-started node has **no peer relationship of its own**, so the gateway pins a live 4001 peer immediately after replay and serves the node's 4002 catch-up RPC in stages: the pinned active peer answers **alone** within a short grace budget (one upstream connection in the common case, and no other peer's response can outrace it); pool candidates — each behind its own temporary live relationship — are hedged in only when the active is absent, failed, or slow. A `Peer-only request` from the pinned active unpins it so the node's next 4001 reconnect elects a fresh peer; terminal range errors (`no client blocks to serve`, `client block round too large`) are forwarded to the node as a last resort; if every attempt fails the request socket closes and the node retries.
- `proxy <peer1,peer2,...> [--push]` — **failover transparent proxy**: listens on 4000-4010, relays to the active upstream, health-monitors it, and fails over to the next healthy peer (bad peers skipped with a cooldown; no oscillation). Like `gateway` but with a fixed upstream list instead of the live `peerd` pool. Add `--push` to use the same active-stream monitoring path as `gateway --push`; shadow multi-source injection is disabled for now.

`peerd [interval]` (default 300s) is the companion daemon: it discovers peers (the `gossipRootIps` API + reading the node's own `tcp_lz4_stats/<date>` files — the peers the node has actually exchanged data with) and probes each for **live-block serving** (`send_abci:false`, cheap and not rate-limited — it never probes the rate-limited abci_state), writing a ranked pool to `<data-dir>/peers.json` (atomically, via temp+rename) that `gateway` reads and refreshes every 30s. Probes default to 8 concurrent sockets (`PEERD_PROBE_CONCURRENCY`, clamped 1-64), and an empty live pool backs off up to `PEERD_EMPTY_BACKOFF_MAX_SECS` (default 1800s). Candidates that fail 50 consecutive probe cycles are pruned (re-discovered automatically if they come back). It's a subcommand of the same `hypersync` binary, but deliberately still runs as its **own process**, not a background task inside the gateway — a probing storm across 100+ candidates never touches the process actively serving a node, and either side can be restarted independently. Container-friendly: the stats harvest only needs a node's data volume mounted read-only (no docker.sock, no subprocess); the only runtime dep is `curl` on PATH. Config is via env vars: `HYPERSYNC_DATA` (data dir, default `./data`), `HL_STATS_DIR` (optional, **comma-separated** — one `tcp_lz4_stats` dir per co-located node; unset — the normal case when gw+peerd run on a separate machine from the nodes — means discovery relies on the API + previously persisted candidates), `HL_SELF_IP` (optional, **comma-separated** — the public IPs of ALL your own nodes; they're legitimate routable addresses but must never enter the upstream pool, or the gateway can end up relaying your nodes to each other).

`soak-test.sh [restart-node-at-start] [samples] [interval-seconds] [log-file]` monitors a running node+gateway pair over time: applied-block rate, sync source, real error count, and container health. Real errors are filtered by *signature* (e.g. `desc: "tcp greeting ... gossip"`) rather than by source IP, since the node never accepts inbound peer connections — that class of error is always internet-scanner noise on its public ports, regardless of which IP sent it.
- `relay <upstream>` — plain transparent relay of ports 4000-4010 to a single upstream.
- `<port> <peer1,peer2,...>` (no subcommand) — multi-upstream live-block merge with round-based dedup (freshest block wins, gap-free). Testing-only: it drops control frames, so a real node never receives the peer greeting through it — pair it with `mock`.
- `cache <port> <upstream>` — fetches the abci_state snapshot once, caches it, serves connecting nodes at local speed. Testing-only: binds a single port, and a real node also needs 4002 to bootstrap — use `gateway --cache` for nodes.
- `mock <bind> <dir> <start> <end>` — replays captured blocks for testing.
- `fakenode <gw-ip[:base-port]> [--live-secs N] [--rpc] [--label NAME]` — testing-only synthetic downstream node: receives a full bootstrap (counting bytes/frames, never buffering), reads the live stream (parsing block rounds), and optionally sends a real 4002 client-block range query (a leaked "Peer-only request" rejection counts as FAIL — it means the gateway mis-routed the RPC). Prints one machine-checkable `FAKENODE ...` summary line; exit code is a bitmask (0 pass, +1 boot, +2 live, +4 rpc, 64 usage/dial). Run each fakenode as its own container so the gateway sees distinct source IPs.
- `bench <dir> <iters>` — benchmarks the hot path (lz4 decode + round dedup).

**Deployment note:** the gateway/proxy/relay listeners bind `0.0.0.0:4000-4010` inside their own network namespace with no peer allowlist — anyone who can reach those ports can trigger a ~4.5 GB bootstrap relay or a cache replay (bandwidth amplification). `--cache` keeps roughly one bootstrap in memory (~4.5 GB) and may briefly hold another during the 10-minute refresh cycle; budget RAM and upstream bandwidth accordingly. Run gateway listeners on an internal/container/private network only; never publish them to the internet. The public 4001/4002 should belong to a real HL node on the same public egress path, not to the gateway.

## Build & run

```sh
cargo build --release
./target/release/hypersync proxy 1.2.3.4,5.6.7.8,9.10.11.12
```

Point each node's `override_gossip_config.json` at hypersync and restart it so the node dials the gateway:

```json
{"root_node_ips": [{"Ip": "<hypersync-host>"}], "try_new_peers": false, "chain": "Mainnet"}
```

## Deploy with docker compose

gw + peerd run as their own compose project (`docker-compose.yml` in this repo). The gateway reads no node files; peerd can optionally read mounted/copied `tcp_lz4_stats` to improve discovery. Set `HL_SELF_IP` in `.env` to a comma-separated list of all your nodes' public IPs.

- **Co-located nodes** (same machine): node compose projects join the attachable `hypersync_gwnet` network (`external: true`) and dial the gateway's static IP `172.28.0.10`. Gateway `ports:` stay disabled, so the host's public 4001/4002 remain owned by the real HL node. Optionally add stats harvesting from the local node: `docker compose -f docker-compose.yml -f docker-compose.colocated.yml up -d`.
- **Remote nodes** (nodes on other machines): expose gateway only on a private/VPN interface and firewall 4000-4010 to the node machines. If this is the same host that also publishes a real HL node, do not let both containers bind the same host `0.0.0.0:4000-4010`; bind the real node to the public IP and gateway to the private/VPN IP. A gateway-only public IP without real HL node 4001/4002 behavior may be deprioritized by upstream peers.

Smoke-test a running gateway with a synthetic node:

```sh
docker run --rm --network hypersync_gwnet hypersync:latest fakenode 172.28.0.10 --rpc --label smoke
```

## HL P2P protocol (reverse-engineered)

- Plaintext TCP, ports 4000-4010 (4001 = abci_state / block heavy channel, 4002 = gossip RPC).
- Frame: `[u32 BE length][1 type byte][payload]`; type 0 = control, type 1 = data.
- Greeting: `TcpGreeting { send_abci, broadcast_group, id }` (8 bytes).
- Blocks are lz4-compressed; the consensus round sits at decompressed offset `0x5e`.
- Bootstrap is **interactive RPC** (query height → request client blocks → commit). A node only ingests blocks from peers **it dials** (outbound) — so hypersync must be an outbound peer (configured in `root_node_ips`), never an inbound pusher.

## Performance

The hot path (lz4 round-decode + dedup) is heavily optimized. Reading a block's consensus round needs only the first decompressed bytes, and the lz4 literal run at the block head normally covers them — so the round is read straight from the compressed literals with **no decode buffer at all** (it falls back to a bounded decode otherwise). Dedup is a lock-free, hash-free sliding window: a power-of-two `AtomicU32` array indexed by `round & mask` and updated with a single atomic swap — no mutex, no hashing, no ring-buffer eviction. ~**135M frames/s** in the included `bench` — about **4.3x** the previous already-optimized baseline (and far higher vs a naive full-decompress).
