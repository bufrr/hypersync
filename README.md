# hypersync

A Hyperliquid P2P sync gateway — a multi-peer, health-monitored **failover transparent proxy** that keeps a Hyperliquid node synced with low lag.

A local HL node points its `root_node_ips` at hypersync; hypersync transparently relays the node's P2P traffic to one of several real upstream peers, monitors that peer's health, and automatically fails over to another peer if it stalls or dies — so the node keeps syncing even when an individual peer degrades.

## Why

Hyperliquid non-validator nodes sync by dialing peers and streaming consensus / client blocks. A single slow or flaky peer can cause sync lag. hypersync sits in front of the node as its peer, fronting a pool of real upstream peers with automatic failover, decoupling the node from any one peer's problems.

## Modes

- `gateway <peers-file> [--push] [--cache] [--live N]` — **full P2P gateway** (recommended): each node points `root_node_ips` at it and syncs the real mainnet ONLY through the gateway, which fronts a whole pool of real peers read from `<peers-file>` (maintained by `peerd`, refreshed live). The gateway serves **multiple downstream nodes concurrently**: session state (active-peer pin, cache-replay cooldown, live round floor) is kept **per node, keyed by the node's source IP** — so run one node per egress IP (two nodes NAT'd through one IP would be conflated into one session). All of a node's connections are pinned to that node's **active** peer so the client-block RPC isn't rejected with "Peer-only request". The flags are orthogonal and compose:
  - **default (no flags)** — pure transparent failover: on bootstrap the gateway relays a peer that is *currently* serving the abci_state (peeks the first frame; rotates to a fresh state-server each time, so node/gateway restarts never hit the per-IP abci_state rate-limit), and splices live blocks + gossip RPC to that active peer. The gateway never has to understand HL's closed bootstrap framing.
  - **`--push`** — route 4001 live/resume traffic through `serve_push`: the active peer still relays the full stream and owns the peer relationship, and if the active stream stalls or dies the session is torn down so the node reconnects and the gateway pins a fresh active. Shadow multi-source block injection is currently disabled because round-only dedup cannot prove proposer/fork correctness; `--live N` is retained as a future capacity knob. With `HYPERSYNC_FAILOVER_STANDBYS` set, the gateway switches the node to a standby peer inside the same session instead of tearing it down (see [Hot-standby failover](#hot-standby-failover)).
  - **`--cache`** — additionally capture the full bootstrap (abci_state + EVM KVs, ~5.8 GB in 2026-10) verbatim from a fast peer (hedged race across the pool, early-abort on slow peers) and cache it in memory plus disk. The default disk path is `bootstrap.cache` next to the peer file, falling back to `/tmp/hypersync-bootstrap.cache` if that directory is read-only; override with `HYPERSYNC_BOOT_CACHE`. Fresh cache entries (<=8 minutes old) are replayed to cold-starting nodes; the gateway refreshes cache every 4 minutes and retries failed fresh refreshes after up to 30 seconds, shortening the retry as the cache approaches expiry. Stale cache is ignored and the node transparently bootstraps from a real peer, while the gateway tees that successful fallback stream back into cache. Trade-off: a cache-cold-started node has **no peer relationship of its own**, so the gateway pins a live 4001 peer immediately after replay and serves the node's 4002 catch-up RPC in stages: the pinned active peer answers **alone** within a short grace budget (one upstream connection in the common case, and no other peer's response can outrace it); pool candidates — each behind its own temporary live relationship — are hedged in only when the active is absent, failed, or slow. A `Peer-only request` from the pinned active unpins it so the node's next 4001 reconnect elects a fresh peer; terminal range errors (`no client blocks to serve`, `client block round too large`) are forwarded to the node as a last resort; if every attempt fails the request socket closes and the node retries. **Memory cost**: the cache holds one bootstrap in memory (~6 GB) and every refresh race downloads one to three more in parallel, so the gateway swings between ~6 and ~19 GB. On a 64 GB host shared with a ~35 GB hl-node this pushed host memory past hl-visor's ~95% low-memory threshold and caused repeated hl-node child restarts (2026-10-12 soak), so the bundled `docker-compose.yml` runs the gateway **without** `--cache` (gateway RSS then stays in the hundreds of MB). Enable it only with that headroom to spare. The benefit: without a cache, any hl-node child restart (e.g. an hl-visor binary update) must find an external peer willing to serve the full abci_state right then, which can take minutes when most of the pool declines or rate-limits (the gateway does remember recent state-servers and tries them first, but a warm cache removes the dependency entirely).
  - **Hot standbys** (`HYPERSYNC_FAILOVER_STANDBYS=N`, needs `--push`). Each node session also streams live blocks from N standby peers. When the active peer stalls, dies or falls behind, the gateway moves the node's feed to a standby without closing the node's 4001 connection. See [Hot-standby failover](#hot-standby-failover).
- `proxy <peer1,peer2,...> [--push]` — **failover transparent proxy**: listens on 4000-4010, relays to the active upstream, health-monitors it, and fails over to the next healthy peer (bad peers skipped with a cooldown; no oscillation). Like `gateway` but with a fixed upstream list instead of the live `peerd` pool. Add `--push` to use the same active-stream monitoring path as `gateway --push`; shadow multi-source injection is disabled for now.

`peerd [interval]` (default 300s) is the companion daemon: it discovers peers (the `gossipRootIps` API + a `query_peers` crawl of the gossip peer graph, with the node's own `tcp_lz4_stats/<date>` files as an optional supplementary source) and probes candidates for **live-block serving** (`send_abci:false`, cheap and not rate-limited — it never probes the rate-limited abci_state), writing a ranked pool to `<data-dir>/peers.json` (atomically, via temp+rename) that `gateway` reads and refreshes every 30s. Probes default to 8 concurrent sockets (`PEERD_PROBE_CONCURRENCY`, clamped 1-64) and at most 256 candidates per cycle (`PEERD_PROBE_MAX_PER_CYCLE`, clamped 32-5000): previous top live peers are kept hot, while the rest of the candidate set is covered by rotation. An empty live pool backs off up to `PEERD_EMPTY_BACKOFF_MAX_SECS` (default 1800s). Candidates that fail 50 consecutive probe cycles are pruned (re-discovered automatically if they come back). It's a subcommand of the same `hypersync` binary, but deliberately still runs as its **own process**, not a background task inside the gateway — a probing storm across 100+ candidates never touches the process actively serving a node, and either side can be restarted independently. Container-friendly: the stats harvest only needs a node's data volume mounted read-only (no docker.sock, no subprocess); the only runtime dep is `curl` on PATH. Config is via env vars: `HYPERSYNC_DATA` (data dir, default `./data`), `HL_STATS_DIR` (optional, **comma-separated** — one `tcp_lz4_stats` dir per co-located node; unset — the normal case when gw+peerd run on a separate machine from the nodes — means discovery relies on the API + previously persisted candidates), `HL_SELF_IP` (optional, **comma-separated** — the public IPs of ALL your own nodes; they're legitimate routable addresses but must never enter the upstream pool, or the gateway can end up relaying your nodes to each other).

`soak-test.sh [restart-node-at-start] [samples] [interval-seconds] [log-file]` monitors a running node+gateway pair over time: applied-block rate, sync source, real error count, and container health. Real errors are filtered by *signature* (e.g. `desc: "tcp greeting ... gossip"`) rather than by source IP, since the node never accepts inbound peer connections — that class of error is always internet-scanner noise on its public ports, regardless of which IP sent it.
- `relay <upstream>` — plain transparent relay of ports 4000-4010 to a single upstream.
- `<port> <peer1,peer2,...>` (no subcommand) — multi-upstream live-block merge with round-based dedup (freshest block wins, gap-free). Testing-only: it drops control frames, so a real node never receives the peer greeting through it — pair it with `mock`.
- `cache <port> <upstream>` — fetches the abci_state snapshot once, caches it, serves connecting nodes at local speed. Testing-only: binds a single port, and a real node also needs 4002 to bootstrap — use `gateway --cache` for nodes.
- `mock <bind> <dir> <start> <end>` — replays captured blocks for testing.
- `fakenode <gw-ip[:base-port]> [--live-secs N] [--rpc] [--label NAME]` — testing-only synthetic downstream node: receives a full bootstrap (counting bytes/frames, never buffering), reads the live stream (parsing block rounds), and optionally sends a real 4002 client-block range query (a leaked "Peer-only request" rejection counts as FAIL — it means the gateway mis-routed the RPC). Prints one machine-checkable `FAKENODE ...` summary line; exit code is a bitmask (0 pass, +1 boot, +2 live, +4 rpc, 64 usage/dial). Run each fakenode as its own container so the gateway sees distinct source IPs.
- `bench <dir> <iters>` — benchmarks the hot path (lz4 decode + round dedup).

**Deployment note:** the gateway/proxy/relay listeners bind `0.0.0.0:4000-4010` inside their own network namespace with no peer allowlist — anyone who can reach those ports can trigger a ~5.8 GB bootstrap relay or a cache replay (bandwidth amplification). `--cache` keeps one bootstrap in memory (~6 GB) and holds up to three more in flight during each 4-minute refresh race (observed peak ~19 GB); budget RAM and upstream bandwidth accordingly. Run gateway listeners on an internal/container/private network only; never publish them to the internet. The public 4001/4002 should belong to a real HL node on the same public egress path, not to the gateway.

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

## Hot-standby failover

With `--push` and no standbys, the gateway ends a node's 4001 session after the active peer sends nothing for 10 s. The node then reconnects and the gateway elects a new peer. On a mainnet node in Tokyo that cycle took about 11 s from the last block to the next one, and a peer that kept sending blocks a few hundred ms late was never replaced.

Set `HYPERSYNC_FAILOVER_STANDBYS=N` to give every node session N standby peers. Each standby streams live blocks into a buffer that the gateway does not forward. When the active peer fails or falls behind, the gateway switches the node to a standby on the node's existing TCP connection.

### Why a switch is safe

The node rejects a block whose parent it never received, so the gateway can't start forwarding from wherever the standby happens to be. It replays the standby's buffered blocks starting at the block whose parent is the last block the node received, then follows parent links. If the standby's buffer has a gap, the gateway doesn't switch to it.

Live blocks for a round are byte-identical on every peer. On 2026-10-20 we compared 429 rounds from 6 peers and found no difference in the decompressed blocks or in the wire frames. The gateway hashes every block and checks each standby's copy against the block the active peer delivered for that round. One mismatch drops the standby. A standby needs 3 matching blocks before it can replace a live active peer, and 1 when the active peer has already died.

Only one upstream feeds the node at any time. The gateway never mixes frames from two peers, so the round-only dedup problem that keeps `ENABLE_SHADOW_PUSH` off doesn't apply.

### When the gateway switches

- The active stream errors, closes or sends nothing for 5 s. The gateway switches at once to a standby that can continue the chain.
- A verified standby has held the node's next block for `HYPERSYNC_FAILOVER_STALL_MS` and the active peer still hasn't sent it.
- A verified standby's median lead over the active peer, across the last 40 blocks both delivered, is above `HYPERSYNC_FAILOVER_LAG_MS`. The gateway makes this kind of switch at most once per `HYPERSYNC_FAILOVER_DWELL_MS`.

On a stall the gateway picks the standby with the best median lead, then the one holding the most of the chain. After a switch it repins the node's 4002 RPC to the new active peer, closes the old one, keeps it out for 120 s and dials a replacement standby. If the active peer dies and no standby can continue the chain within 3 s, the session ends the same way it does without standbys.

### Choosing standbys

The gateway fills a standby slot by dialing up to 4 candidates at once, priority peers first and then the peerd pool, and keeps the first one that accepts the live greeting. The first to accept is usually the nearest. Every `HYPERSYNC_FAILOVER_ROTATE_SECS` it drops the standby that trails the active peer the most, if that standby trails by more than `HYPERSYNC_FAILOVER_ROTATE_MS`, and skips it for 30 minutes. Priority peers are never rotated out.

### Settings

| Variable | Default | Effect |
|---|---|---|
| `HYPERSYNC_FAILOVER_STANDBYS` | `0` | Standby peers per node session (max 16). `0` keeps the behavior described under `--push`. |
| `HYPERSYNC_FAILOVER_STALL_MS` | `400` | How long a standby may hold the next block before the gateway switches to it. |
| `HYPERSYNC_FAILOVER_LAG_MS` | `150` | Median lead a standby needs over the active peer to trigger a lag switch. |
| `HYPERSYNC_FAILOVER_DWELL_MS` | `10000` | Minimum time between lag switches. Stall and dead-peer switches ignore it. |
| `HYPERSYNC_FAILOVER_ROTATE_MS` | `100` | A standby trailing the active peer by more than this gets replaced. `0` turns rotation off. |
| `HYPERSYNC_FAILOVER_ROTATE_SECS` | `60` | How often the gateway checks for a standby to rotate out. |
| `HYPERSYNC_FAILOVER_STATUS_DIR` | unset | If set, the gateway writes `failover-<node-ip>.json` there every second with the active peer, switch count, last switch and, per standby, matched blocks, median lead and buffered blocks. |

Each standby is one more live stream from the gateway's IP, about 1 to 2 MB/s. The buffers hold only blocks the node hasn't received yet, so memory use stays in the low MB.

## HL P2P protocol (reverse-engineered)

- Plaintext TCP, ports 4000-4010 (4001 = abci_state / block heavy channel, 4002 = gossip RPC).
- Frame: `[u32 BE length][1 type byte][payload]`; type 0 = control, type 1 = data.
- Greeting: `TcpGreeting { send_abci, broadcast_group, id }` (8 bytes).
- Blocks are lz4-compressed (u32-LE size prefix). Since 2026-10 the round is no longer at a fixed offset: the decompressed header carries the round and its parents as `0xfc R 0xfc R-1 0xfc R-2` within the first 0x400 bytes, and the gateway takes the first such triple (`src/protocol.rs`). Before that the round sat at offset `0x5e`.
- Bootstrap is **interactive RPC** (query height → request client blocks → commit). A node only ingests blocks from peers **it dials** (outbound) — so hypersync must be an outbound peer (configured in `root_node_ips`), never an inbound pusher.

## Performance

The hot path (lz4 round-decode + dedup) is heavily optimized. Reading a block's consensus round needs only the block header, so the gateway decodes just the first 0x400 decompressed bytes into a fixed stack buffer (blocks run to ~250 KB decompressed) and falls back to a full decompress only if that bounded decode fails. Dedup is a lock-free, hash-free sliding window: a power-of-two `AtomicU32` array indexed by `round & mask` and updated with a single atomic swap — no mutex, no hashing, no ring-buffer eviction. ~**135M frames/s** in the included `bench` (measured with the pre-2026-10 fixed-offset decoder; not re-measured since) — about **4.3x** the previous already-optimized baseline (and far higher vs a naive full-decompress).
