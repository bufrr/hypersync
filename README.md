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
- **peerd** discovers peers (`gossipRootIps` API + a `query_peers` crawl), probes which ones serve live blocks (a live greeting, ended as soon as 2 blocks arrive, so a live peer costs a few hundred KB, not seconds of its stream), and writes `peers.json` for the gateway every cycle.
- **Priority peers** (e.g. dedicated sentries) are tried first; the public pool is the backup.
- A node only ingests blocks from peers it dials, so each node points `root_node_ips` at the gateway and dials nothing else.
- The gateway's public listeners answer upstream probe traffic from non-whitelisted sources on 4001/4002, so the nodes need no public ports (below).

## Quick start (docker compose)

```sh
echo 'HL_SELF_IP=<public IPs of all your nodes, comma-separated>' > .env   # see Configuration
echo 'HYPERSYNC_NODE_WHITELIST=172.28.0.128/25' >> .env  # co-located nodes on gwnet
docker compose up -d    # gateway + peerd
```

Point each node at the gateway in `override_gossip_config.json` and restart it:

```json
{"root_node_ips": [{"Ip": "172.28.0.10"}], "try_new_peers": false, "chain": "Mainnet"}
```

- **Same host:** the node's compose project joins the external `hypersync_gwnet` network and dials `172.28.0.10`. Whitelist its gwnet address, or the default dynamic pool `172.28.0.128/25`.
- **Other hosts:** add each remote node's public IPv4 source address or CIDR to `HYPERSYNC_NODE_WHITELIST`; the node dials the gateway host's public IPv4 address directly, with no VPN required. Use IPv4: under Compose an IPv6 connection reaches the gateway through Docker's proxy from `172.28.0.1`, so a node's IPv6 address can never be whitelisted. `HYPERSYNC_PUBLIC_BIND` optionally limits the host address on which the gateway publishes 4000-4010.
- Smoke test: `docker run --rm --network hypersync_gwnet hypersync:latest fakenode 172.28.0.10 --rpc --label smoke`

> **Whitelist only your nodes.** A whitelisted source can trigger a ~5.8 GB bootstrap relay. The gateway refuses to start with an empty whitelist. The gwnet bridge gateway `172.28.0.1` must never be covered: Docker's userland proxy makes forwarded IPv6 and hairpin connections arrive from that address, so covering it would whitelist the whole internet. The gateway therefore rejects (logs and never installs) a `/0` or any entry containing its own default gateway, such as `172.28.0.0/24`. Non-whitelisted sources receive only the [public probe answers](#public-probe-answers).

## Modes

| Command | Use |
|---|---|
| `gateway <peers.json> [--push] [--cache]` | The gateway. Compose runs `--push` without `--cache`. |
| `peerd [interval-secs]` | Peer discovery and probing (default 300 s; compose uses 120). Run as its own container. |
| `proxy <ip1,ip2,...> [--push]` | Failover proxy over a fixed upstream list instead of peerd's pool. |
| `relay <ip>` | Plain relay of 4000-4010 to one upstream. |
| `fakenode <gw-ip> [--rpc] [--live-secs N] [--label X]` | Synthetic node for testing a gateway. |
| `cache`, `mock`, `bench`, `<port> <peers>` | Testing only. |

Gateway flags:

- **`--push`** (recommended): live blocks pass through the gateway's checks (below) instead of a raw splice, and a bad upstream ends the session so the node reconnects to a fresh one.
- **`--cache`**: keep the full bootstrap (~5.8 GB) in memory and on disk and replay it to cold-starting nodes, avoiding the per-IP abci_state rate limit. Holds up to ~3 bootstraps at once (~17 GB: the cache plus a refresh capture and a fallback tap), which pushed a 64 GB host shared with an hl-node past hl-visor's low-memory restart threshold — so it is off by default. A capture is installed only if it ends in live blocks, but a hostile state peer could still append a few fake ones to an incomplete state; enable it only with state peers you trust. Without it, a node restart bootstraps from a peer that is serving state right now (the gateway remembers recent state servers).

## Public probe answers

Upstream peers, QuickNode's sentries included, check the IP that peers with them: they open its
4001 and close it without a byte, and ask its 4002 for `query_peers`. Some pool peers also greet
4001 to pull a live stream or ask 4002 for heights. The gateway answers these probes whenever the
connection's source IP is not in the node whitelist:

- 4001: a probe is held until the peer closes it; a greeting gets the "peer full" status, then the
  connection closes. No block is ever sent.
- 4002: `query_peers` gets an empty peer table; anything else gets the structured Peer-only error.
- 4000 and 4003-4010: the connection closes immediately without a read.

A non-whitelisted connection never creates node session state, dials an upstream or receives a
block. Every 10 minutes the gateway logs public-answer counters, including other-port closes and
probes from priority peers, under `[gw] [public]`. The gateway never logs a stranger's address per
connection.

The whitelist is the union of `HYPERSYNC_NODE_WHITELIST` (comma-separated) and
`node_whitelist.txt` next to the peers file (`data/node_whitelist.txt` in compose), one entry per
line with `#` comments allowed. IPv4/IPv6 addresses and CIDRs are accepted; CIDR host bits are
masked. Under Compose only IPv4 entries can ever match a remote node (see Quick start). Both
sources are re-read every 30 seconds. Removals affect new connections only; existing node sessions
are left alone. An entry that would serve the whole internet is rejected with a `REJECTED` log
line: a `/0` (also refused at startup when it is in the environment variable) or one containing
the gateway's default gateway (`172.28.0.1` in compose; a NAT router's address on a bare host). An
empty initial whitelist, counting only the entries actually installed, stops startup; an empty set
after a reload logs a warning.

Because the probe answers come from the gateway process, a gateway restart or crash loop also
leaves upstream probes unanswered until it is back.

## Configuration

Set in `.env`; compose passes them through.

| Variable | Default | Meaning |
|---|---|---|
| `HL_SELF_IP` | – | Comma-separated public IPs of **all** your nodes; never used as upstreams. |
| `HYPERSYNC_NODE_WHITELIST` | – | Comma-separated node source IPs/CIDRs; union with `node_whitelist.txt`. At least one entry is required. |
| `HYPERSYNC_PUBLIC_BIND` | all host addresses | Optional host IP on which Compose publishes the gateway's 4000-4010. |
| `HYPERSYNC_IP_BLACKLIST` | – | Comma-separated IPs never connected to (see [IP blacklist](#ip-blacklist)). |
| `HYPERSYNC_PRIORITY_PEERS` | – | Comma-separated priority peer IPs. Logged as `prio-N`, never by IP. |
| `HYPERSYNC_PRIORITY_PEERS_EXPECTED` | – | How many priority peers there must be (e.g. `3` for QuickNode). Set, a missing, short or partly invalid list stops startup instead of quietly leaving nodes on the pool. |
| `HYPERSYNC_PRIORITY_MAX_SESSIONS` | `0` | Max node sessions per priority peer (`0` = unlimited). |
| `HYPERSYNC_PRIORITY_EXCLUSIVE` | `1` | Keep nodes on the priority peers (QuickNode-style peering): the pool serves a node only while every priority peer is unusable. `0` = staged: the pool joins after `HYPERSYNC_PRIORITY_STAGE_MS`. |
| `HYPERSYNC_PRIORITY_STAGE_MS` | `1500` | Staged tier only: how long priority peers race alone before the pool joins. |
| `HYPERSYNC_PRIORITY_BOOTSTRAP` | `prefer` if exclusive, else `fallback` | Bootstrap from priority peers: `prefer` (first), `fallback` (after the pool, even on an exclusive tier), or `never`. |
| `HYPERSYNC_FAILOVER_STANDBYS` | `0` | Hot standbys per node session (see below). |
| `HYPERSYNC_PACE_REFERENCES` | `1` | Pace references, which let even a single live stream notice its peer falling behind: 3 pool peers' rounds, polled with a 4002 height query every 2 s (~15 bytes/s each). With fewer than 2 working the lag guards pause (`WARN only N pace reference(s) available`). `0` turns them off. |
| `HYPERSYNC_SHARED_LIVE` | `1` | `0` gives every node its own upstream 4001 live stream instead of sharing one (below). |
| `HYPERSYNC_SIBLING_SYNC` | `1` | `0` always takes a node's bootstrap state and 4002 catch-up from upstream peers instead of first from a synced node behind the gateway (below). |
| `HYPERSYNC_BOOT_CACHE` | next to peers.json | Disk path for `--cache`. |
| `HYPERSYNC_DATA` | `./data` | peerd's data dir (`peers.json`, candidates). |
| `HL_STATS_DIR` | – | Optional, comma-separated `tcp_lz4_stats` dirs of co-located nodes (extra discovery source). |
| `PEERD_PROBE_CONCURRENCY` / `PEERD_PROBE_MAX_PER_CYCLE` | `8` / `256` | Probe parallelism and candidates per cycle. |
| `PEERD_CRAWL_DEPTH` / `PEERD_CRAWL_MAX_QUERIES` / `PEERD_CRAWL_CONCURRENCY` | `3` / `200` / `8` | `query_peers` crawl bounds. |
| `PEERD_EMPTY_BACKOFF_MAX_SECS` | `1800` | Max backoff when no live peer is found and there is no previous pool. |

## Upstream health checks

**Picking a peer** (on every node 4001 connect):

- **Exclusive tier (default).** A node stays on the priority peers while any of them is usable; the pool serves it — live blocks, 4002, the 4001 fallback, abci_state, hot standbys — only while every priority peer is cooling down (after an error, a hang or a lag cut), at its session cap, or blacklisted. A slow priority peer is waited for for the whole selection window, never outraced by the pool. For abci_state the priority peers get an 8 s head start, after which the pool may take free slots next to a still-pending priority attempt (one stuck download can take longer than the whole window). A session that did land on the pool returns to a priority peer as below.
- Priority peers race first. Every upstream session the gateway opens on a priority peer — live, fallback, bootstrap, 4002 prewarm, hot standby — reserves one of its `HYPERSYNC_PRIORITY_MAX_SESSIONS` slots before dialing; a full peer is skipped without penalty. A node's own side connections (4000, 4002-4010) to its active peer belong to that session and take no extra slot. On a staged tier (`HYPERSYNC_PRIORITY_EXCLUSIVE=0`) the pool joins after the stage delay, 4 at a time.
- The first peer whose first live block is plausible and within 1,500 rounds of the network tip wins — all within 3.5 s, because hl-node abandons a connect ~5 s after greeting. If none qualifies, a fallback races up to 8 peers in the time left.
- A priority peer that errors, refuses (`peer full`) or hangs is cooled down for 30 s. On an exclusive tier, one that merely stays silent through a live selection or a return-to-priority probe (timed out, no error) is skipped only for 4 s, so the node's next connect tries it again — silence from all of them at once is usually a local stall. A cooldown is only ever extended, never shortened.

**Dropping a peer** (`--push`). The session ends and the node reconnects when the active peer:

- sends nothing for 10 s, or no higher block for 30 s;
- sends a block more than 20,000 rounds above its previous one;
- falls more than 1,500 rounds behind the network tip, or sends an implausible round;
- stops, or nearly, while the gateway's other streams advance: 45 rounds (~3 s) behind the freshest round at least two OTHER upstream peers agree on (the pace references' polled rounds, other live streams; a stream never counts as its own reference), and another 30 rounds behind 3 s later (cut ~6 s after it stops, instead of the 10 s idle timeout or the guard below). A healthy stream that dips and recovers, or keeps pace a fixed distance behind, isn't cut (recorded streams of healthy peers never trailed by more than 26 rounds);
- stays more than 100 rounds (~7 s) behind that freshest round for 5 s (cut ~12 s after it starts falling behind) — a peer that still sends, just slowly. "Agree" means at least two different peers (other streams, the pace references' polled rounds) have reached it, so one bad peer can't make honest sessions look lagging, and rounds more than ~20 min past peerd's network tip never count (nor any while that tip is unknown). Time when the node itself isn't taking blocks counts against the peer neither here nor for the 30 s stall;
- stops reading the node's outbound traffic for 5 s.

The peer is then skipped for 30 s (priority peers via their own cooldown), so the reconnect lands on a different one unless every peer is cooling down.

**Returning to priority peers.** A session that ended up on a pool peer (e.g. after a network-wide pause cooled the priority peers down) moves back once a priority peer serves again: after the session has run 60 s and the node has finished catching up (no client-block batch for 60 s, or a catch-up still running after 3 minutes, which isn't converging), the gateway probes the available priority peers in turn, least-loaded first, with the same first-block vetting as selection and, once one passes, ends the session so the node reconnects to it. A failed probe cools that peer down; each node makes at most one probe attempt per 5 minutes, since the reconnect costs it a few seconds of blocks. With hot standbys the switch happens on the same connection instead (below).

**Stuck catch-up.** hl-node drops the live blocks that arrive while it fetches a missing range over 4002, so the next live block is missing its parent again; with a slow 4002 answer it can loop on small ranges and stay a fixed distance behind (~70 s seen) for as long as the session lasts. In gateway push mode without hot standbys (the default), a session whose node's catch-up has sat at one distance for 3 minutes is ended, at most once per node per 5 minutes, so the node reconnects and clears the gap as one large range (about a minute). Sitting at one distance: its client-block batches (range queries answered with a batch) keep coming without a 60 s gap, every batch of the last 3 minutes was within a 150-round (~10 s) band of the others behind the gateway's freshest live round (from each request's start round), without closing in, and the node was asking for the loop's small ranges (~20-40 rounds) rather than full 100-round windows. A catch-up that moves is left alone: one closing in (e.g. after a restart from an old snapshot, including one that first lost ground under heavy blocks), and one falling further behind or merely keeping pace with heavy blocks in 100-round windows, which a new session would only restart. An unknown freshest round, or a jump in it, also holds the cut back. Hot-standby sessions and `proxy --push` don't have this watchdog.

Live frames over 8 MB end the session (blocks average ~200 KB), at most 64 MB of frames are queued per node, and writes to a node are bounded to 30 s, so neither a hostile peer nor a stuck node can make a session hold more. The one exception is a bootstrap connection: during its first 15 minutes a write may stall up to 300 s, because a node stops reading while it loads the received abci_state before it reads the EVM KVs (57-145 s measured on a 16-core node), and the 64 MB state prefetch is a single write that a slow link needs well over 30 s for. After those 15 minutes the 30 s bound applies again.

**Network round tip.** peerd publishes the median round of the largest cluster of probed peers, so a minority of peers (even a few colluding ones) can't move it; a majority of the probed peers lying still could. The gateway replaces its tip with each fresh reading and resets it to unknown (with a `WARN`) if none arrives for 15 minutes, rather than freezing on a stale value.

**Shared live stream** (`--push`, default; `HYPERSYNC_SHARED_LIVE=0` turns it off). Every node behind the gateway is fed from ONE upstream 4001 live stream, so an upstream peer sees the gateway as a single node however many sit behind it. This works because after its 8-byte greeting a node sends nothing on 4001, and every node is sent the same thing: the peer's accept frame, then the same blocks. The first node to start a live session selects the peer; later nodes join the running stream, get blocks from that point on, and fetch the gap over 4002 as on any new session. All of them pin their 4002 to the stream's peer, which knows them by the gateway's source IP. The stream is judged once by the checks above (a failure ends every node's session and they reconnect onto one new stream; the peer is cooled down once) and takes one priority slot. It returns to a priority peer only while every node on it is caught up. Each node has its own 64 MB queue: one that stops reading is dropped on its own (`node write backlog`) without holding the others back. A stream with no node left lingers 30 s for reconnects. The pace references poll other peers' rounds over 4002 instead of streaming blocks, so the blocks come from upstream exactly once: upstream 4001 traffic is this one stream (~2.2 MB/s on 2026-11-16) however many nodes are behind the gateway. No peer gets two long-lived streams from the gateway. Served per node as before: a node greeting with other bytes than the stream's, hot-standby sessions, the live session after a `--cache` replay, and a node whose selection found no peer. Log lines: `live hub start`/`live hub ended`, and each node's `4001 push session start ... mode=shared nodes=N`.

**Sibling sync** (with the shared live stream; `HYPERSYNC_SIBLING_SYNC=0` turns it off). Nodes behind the gateway serve each other over the LAN first, with no upstream traffic and no upstream rate limit; any failure falls back to upstream peers exactly as without siblings.

- **Bootstrap state.** A cold-starting node's abci_state + EVM KVs come from a sibling that has been on the shared stream for 2 minutes, isn't catching up, and answers a 4002 height query within 300 rounds (~20 s) of the stream's round. A remote node publishes no ports, so its height query fails and it is never asked. hl-node prepares the state on each request, so a sibling may take 30 s to send its first byte (13.5-15 s measured, once over 20 s while busy; then 309 MB/s); the whole sibling attempt is bounded to 35 s. When it fails within 8 s, upstream peers are asked in the same attempt; when it fails later, too little of the node's ~60 s deadline is left for an upstream server, so the gateway closes the connection: hl-node retries within ~2 s and that retry goes straight upstream. A sibling that declines, is too slow, or ends the relay short of a whole bootstrap isn't asked again for 10 minutes; nor is one whose bootstrap ended at full size when the node asks for another bootstrap within 5 minutes (bytes alone can't prove a bootstrap took: a sibling may close the connection after it). Log line: `bootstrapping again Ns after sibling <node>'s bootstrap`. A sibling's state is never written to the `--cache`. Log lines: `node bootstrap via <node> tier=sibling`, `no sibling node served abci_state`, `bootstrap via sibling <node> ended at N MB`, `sibling <node> not a bootstrap source: ...`.
- **4002 catch-up.** A range query goes to one such sibling first (3 s to answer); anything but a type-1 batch (refused, round too large or too small, any other status, an error, a timeout) skips that sibling for 60 s and sends the request upstream in the same request. Height queries and `query_peers` always go upstream. Log lines: `4002 client blocks via <node> tier=sibling`, `4002 sibling <node>: <why>; asking upstream`.

The node's 4002 stays pinned to the shared stream's peer, never to a sibling.

**Pool.** Re-read every 30 s; a missing or empty `peers.json` keeps the previous pool.

**Bootstrap relay.** A state server that sends nothing for 60 s ends the relay so the node retries elsewhere; one that ends before delivering the whole abci_state is no longer tried first. One that keeps sending below 1 MB/s (measured over 120 s of waiting on it, so a node pausing to load the abci_state doesn't count) is cut too, and skipped as a bootstrap source for 20 minutes, 5 for a priority peer (it can still serve live blocks), so the node's reconnect lands on another server. Past 5.2 GB (the EVM KV tail, or the live stream once the bootstrap is done) the floor drops to 400 KB/s, and past 7 GB pacing stops. A server the node itself gives up on before the abci_state is through (hl-node drops a stalled state read after ~20-30 s) is skipped the same way. If every candidate was cut recently, the gateway keeps a slow server rather than restart the bootstrap forever: it logs `keeping a slow server this time` and only the 60 s silence cut applies.

**Client blocks (4002).** The node's active peer answers alone for 2 s (on an exclusive tier only a priority active; a pool active gets no head start while a priority peer is usable, since a pool peer's 4002 can lag the tip), then up to 8 others are hedged in — on an exclusive tier the other priority peers first, and the pool only once every one of them has failed that request or 6 s have passed (a priority peer silent that long cools down). 15 s per request, at most 8 requests at once.

## IP blacklist

IPv4 addresses hypersync never connects to, however they are advertised (gossip roots, the crawl, `peers.json`, the priority list, a CLI upstream). Exact match only.

- Sources: `HYPERSYNC_IP_BLACKLIST` plus `ip_blacklist.txt` in the data dir (`data/ip_blacklist.txt` in compose, shared by gateway and peerd): one IP per line, `#` comments allowed.
- The gateway re-reads the file every 30 s and peerd every cycle, so edits apply without a restart (`proxy`/`relay`/`cache` read `$HYPERSYNC_DATA` once at start).
- Every upstream dial goes through the check. Candidates are also filtered so selection never wastes a dial or a priority slot on them.
- A session already on a newly blacklisted peer ends within ~5 s of the reload (with hot standbys, it switches to a standby instead). peerd drops it from its candidates and rewrites `peers.json`.

## Hot-standby failover

With `HYPERSYNC_FAILOVER_STANDBYS=N` (needs `--push`), each node session also streams live blocks from N standby peers into a buffer. When the active peer fails or falls behind, the gateway switches the node to a standby **on the same TCP connection**, instead of ending the session (which took ~11 s from last block to next).

- **Safe switch:** the gateway replays the standby's buffered blocks starting from the block whose parent is the node's last block, following parent links; a standby with a gap isn't used. Every standby block is hashed after decompression and checked against the active's block for the same round (identical on all peers). A mismatch resets the standby's verification and 3 in a row drop it; a standby needs 3 matches before it can take over, even when the active has died. The active stays the reference, so an active that sends wrong blocks can still push its standbys out. Only one upstream feeds the node at a time.
- **When:** the active errors, closes or is silent for 5 s, sends no higher block for 30 s, or jumps more than 20,000 rounds; a verified standby has held the next block for `STALL_MS`; the active is a pool peer and a verified priority standby can continue the chain (at most once per `DWELL_MS`); or a standby's median lead over the last 40 blocks exceeds `LAG_MS` (at most once per `DWELL_MS`). A priority active is only left for speed by another priority peer.
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
