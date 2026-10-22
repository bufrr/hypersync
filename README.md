# hypersync

A P2P sync gateway for Hyperliquid nodes. Your nodes peer only with hypersync; hypersync picks healthy upstream peers for them, watches those peers, and replaces any peer that stalls, lags or misbehaves.

## How it works

```text
public HL peers ─┐
priority peers ──┼─▶ hypersync gateway ─▶ your hl-node(s)
                 │         ▲
             peerd ── peers.json (live peers + network round tip)
```

- **gateway** relays each node's bootstrap, live blocks (4001) and client-block RPC (4002) through one vetted upstream peer per node, and serves several nodes at once (one session per node source IP).
- **peerd** discovers peers (`gossipRootIps` API + a `query_peers` crawl), probes which ones serve live blocks, and writes `peers.json` for the gateway every cycle.
- **Priority peers** (e.g. dedicated sentries) are tried first; the public pool is the backup.
- A node only ingests blocks from peers it dials, so each node points `root_node_ips` at the gateway and dials nothing else.

## Quick start (docker compose)

```sh
echo 'HL_SELF_IP=<public IPs of all your nodes, comma-separated>' > .env   # see Configuration
docker compose up -d    # gateway + peerd
```

Point each node at the gateway in `override_gossip_config.json` and restart it:

```json
{"root_node_ips": [{"Ip": "172.28.0.10"}], "try_new_peers": false, "chain": "Mainnet"}
```

- **Same host:** the node's compose project joins the external `hypersync_gwnet` network and dials `172.28.0.10`.
- **Other hosts:** publish the gateway's 4000-4010 only on a private/VPN address, firewalled to your nodes. If a real node on the same host owns the public 4000-4010, bind that node to the public IP and the gateway to the private one.
- Smoke test: `docker run --rm --network hypersync_gwnet hypersync:latest fakenode 172.28.0.10 --rpc --label smoke`

> **Never publish the gateway on the internet.** Anyone who reaches it can trigger a ~5.8 GB bootstrap relay. The public 4001/4002 on your egress IP should belong to a real hl-node; upstream peers deprioritise IPs whose 4001/4002 don't answer.

## Modes

| Command | Use |
|---|---|
| `gateway <peers.json> [--push] [--cache] [--live N]` | The gateway. Compose runs `--push` without `--cache`. |
| `peerd [interval-secs]` | Peer discovery and probing (default 300 s; compose uses 120). Run as its own container. |
| `proxy <ip1,ip2,...> [--push]` | Failover proxy over a fixed upstream list instead of peerd's pool. |
| `relay <ip>` | Plain relay of 4000-4010 to one upstream. |
| `fakenode <gw-ip> [--rpc] [--live-secs N] [--label X]` | Synthetic node for testing a gateway. |
| `cache`, `mock`, `bench`, `<port> <peers>` | Testing only. |

Gateway flags:

- **`--push`** (recommended): live blocks pass through the gateway's checks (below) instead of a raw splice, and a bad upstream ends the session so the node reconnects to a fresh one.
- **`--cache`**: keep the full bootstrap (~5.8 GB) in memory and on disk and replay it to cold-starting nodes, avoiding the per-IP abci_state rate limit. Holds up to ~3 bootstraps at once (~17 GB: the cache plus a refresh capture and a fallback tap), which pushed a 64 GB host shared with an hl-node past hl-visor's low-memory restart threshold — so it is off by default. A capture is installed only if it ends in live blocks, but a hostile state peer could still append a few fake ones to an incomplete state; enable it only with state peers you trust. Without it, a node restart bootstraps from a peer that is serving state right now (the gateway remembers recent state servers).
- **`--live N`**: reserved for multi-source push (disabled).

## Configuration

Set in `.env`; compose passes them through.

| Variable | Default | Meaning |
|---|---|---|
| `HL_SELF_IP` | – | Comma-separated public IPs of **all** your nodes; never used as upstreams. |
| `HYPERSYNC_IP_BLACKLIST` | – | Comma-separated IPs never connected to (see [IP blacklist](#ip-blacklist)). |
| `HYPERSYNC_PRIORITY_PEERS` | – | Comma-separated priority peer IPs. Logged as `prio-N`, never by IP. |
| `HYPERSYNC_PRIORITY_MAX_SESSIONS` | `0` | Max node sessions per priority peer (`0` = unlimited). |
| `HYPERSYNC_PRIORITY_STAGE_MS` | `1500` | How long priority peers race alone before the pool joins. |
| `HYPERSYNC_PRIORITY_BOOTSTRAP` | `fallback` | Bootstrap from priority peers: `fallback` (after the pool), `prefer`, or `never`. |
| `HYPERSYNC_FAILOVER_STANDBYS` | `0` | Hot standbys per node session (see below). |
| `HYPERSYNC_BOOT_CACHE` | next to peers.json | Disk path for `--cache`. |
| `HYPERSYNC_DATA` | `./data` | peerd's data dir (`peers.json`, candidates). |
| `HL_STATS_DIR` | – | Optional, comma-separated `tcp_lz4_stats` dirs of co-located nodes (extra discovery source). |
| `PEERD_PROBE_CONCURRENCY` / `PEERD_PROBE_MAX_PER_CYCLE` | `8` / `256` | Probe parallelism and candidates per cycle. |
| `PEERD_CRAWL_DEPTH` / `PEERD_CRAWL_MAX_QUERIES` / `PEERD_CRAWL_CONCURRENCY` | `3` / `200` / `8` | `query_peers` crawl bounds. |
| `PEERD_EMPTY_BACKOFF_MAX_SECS` | `1800` | Max backoff when no live peer is found and there is no previous pool. |

## Upstream health checks

**Picking a peer** (on every node 4001 connect):

- Priority peers race first. Every upstream session the gateway opens on a priority peer — live, fallback, bootstrap, 4002 prewarm, hot standby — reserves one of its `HYPERSYNC_PRIORITY_MAX_SESSIONS` slots before dialing; a full peer is skipped without penalty. A node's own side connections (4000, 4002-4010) to its active peer belong to that session and take no extra slot. The pool joins after the stage delay, 4 at a time.
- The first peer whose first live block is plausible and within 1,500 rounds of the network tip wins — all within 3.5 s, because hl-node abandons a connect ~5 s after greeting. If none qualifies, a fallback races up to 8 peers in the time left.
- A priority peer that errors, refuses (`peer full`) or hangs is cooled down for 30 s.

**Dropping a peer** (`--push`). The session ends and the node reconnects when the active peer:

- sends nothing for 10 s, or no higher block for 30 s;
- sends a block more than 20,000 rounds above its previous one;
- falls more than 1,500 rounds behind the network tip, or sends an implausible round;
- stops reading the node's outbound traffic for 5 s.

Live frames over 8 MB end the session (blocks average ~200 KB), at most 64 MB of frames are queued per node, and writes to a node are bounded to 30 s, so neither a hostile peer nor a stuck node can make a session hold more.

**Network round tip.** peerd publishes the median round of the largest cluster of probed peers, so a minority of peers (even a few colluding ones) can't move it; a majority of the probed peers lying still could. The gateway replaces its tip with each fresh reading and resets it to unknown (with a `WARN`) if none arrives for 15 minutes, rather than freezing on a stale value.

**Pool.** Re-read every 30 s; a missing or empty `peers.json` keeps the previous pool.

**Client blocks (4002).** The node's active peer answers alone for 2 s, then up to 8 others are hedged in. 15 s per request, at most 8 requests at once.

## IP blacklist

IPv4 addresses hypersync never connects to, however they are advertised (gossip roots, the crawl, `peers.json`, the priority list, a CLI upstream). Exact match only.

- Sources: `HYPERSYNC_IP_BLACKLIST` plus `ip_blacklist.txt` in the data dir (`data/ip_blacklist.txt` in compose, shared by gateway and peerd): one IP per line, `#` comments allowed.
- The gateway re-reads the file every 30 s and peerd every cycle, so edits apply without a restart (`proxy`/`relay`/`cache` read `$HYPERSYNC_DATA` once at start).
- Every upstream dial goes through the check. Candidates are also filtered so selection never wastes a dial or a priority slot on them.
- A session already on a newly blacklisted peer ends within ~5 s of the reload (with hot standbys, it switches to a standby instead). peerd drops it from its candidates and rewrites `peers.json`.

## Hot-standby failover

With `HYPERSYNC_FAILOVER_STANDBYS=N` (needs `--push`), each node session also streams live blocks from N standby peers into a buffer. When the active peer fails or falls behind, the gateway switches the node to a standby **on the same TCP connection**, instead of ending the session (which took ~11 s from last block to next).

- **Safe switch:** the gateway replays the standby's buffered blocks starting from the block whose parent is the node's last block, following parent links; a standby with a gap isn't used. Every standby block is hashed after decompression and checked against the active's block for the same round (identical on all peers). A mismatch resets the standby's verification and 3 in a row drop it; a standby needs 3 matches before it can take over, even when the active has died. The active stays the reference, so an active that sends wrong blocks can still push its standbys out. Only one upstream feeds the node at a time.
- **When:** the active errors, closes or is silent for 5 s, sends no higher block for 30 s, or jumps more than 20,000 rounds; a verified standby has held the next block for `STALL_MS`; or a standby's median lead over the last 40 blocks exceeds `LAG_MS` (at most once per `DWELL_MS`).
- **After a switch:** 4002 is repinned to the new active, the old one is dropped for 120 s and a replacement standby is dialed (up to 4 at once, priority first). If no standby can continue within 3 s, the session ends as without standbys.

| Variable | Default | Effect |
|---|---|---|
| `HYPERSYNC_FAILOVER_STANDBYS` | `0` | Standbys per session (max 16). |
| `HYPERSYNC_FAILOVER_STALL_MS` | `400` | Standby hold time before a stall switch. |
| `HYPERSYNC_FAILOVER_LAG_MS` | `150` | Median lead that triggers a lag switch. |
| `HYPERSYNC_FAILOVER_DWELL_MS` | `10000` | Minimum time between lag switches. |
| `HYPERSYNC_FAILOVER_ROTATE_MS` | `100` | Replace a standby trailing the active by more than this (`0` = off; priority peers never rotate). |
| `HYPERSYNC_FAILOVER_ROTATE_SECS` | `60` | Rotation check interval. |
| `HYPERSYNC_FAILOVER_STATUS_DIR` | unset | Write `failover-<node-ip>.json` status there every second. |

Each standby is one more live stream (~1-2 MB/s) from the gateway's IP, buffers at most 32 MB, and on a priority peer takes one of its session slots. `HYPERSYNC_FAILOVER_STATUS_DIR` is created if missing.

## HL P2P protocol notes

- Plaintext TCP on 4000-4010: 4001 = bootstrap and live blocks, 4002 = gossip RPC (client blocks, `query_peers`).
- Frames are `[u32 BE length][type][payload]`; type 0 = control, 1 = data. The 8-byte greeting carries `send_abci` and the broadcast group.
- Blocks are lz4 (u32-LE size prefix). Since 2026-10 the round is found by scanning the first 0x400 decompressed bytes for `0xfc R 0xfc R-1 0xfc R-2` (it used to sit at 0x5e).
- A full bootstrap is ~5.8 GB (abci_state ~1.3 GB + EVM KVs, 2026-10) and abci_state is rate-limited per source IP.

More detail: [`docs/hl-p2p-protocol.md`](docs/hl-p2p-protocol.md), [`docs/deploy-existing-hlnode.md`](docs/deploy-existing-hlnode.md).

## Development

```sh
cargo build --release
cargo test --release
cargo clippy --release --all-targets -- -D warnings
```

`soak-test.sh` monitors a running node + gateway over time (block rate, sync source, errors). The gateway pins glibc's mmap threshold at 1 MiB so large 4002 buffers are returned to the OS; steady-state memory is a few MB.
