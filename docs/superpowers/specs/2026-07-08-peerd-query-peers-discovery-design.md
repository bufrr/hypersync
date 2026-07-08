# peerd `query_peers` peer discovery — design

## Goal

Replace peerd's dependency on the local HL node's `tcp_lz4_stats` files as the primary
peer-discovery source with a self-contained crawler that speaks the Hyperliquid gossip
`query_peers` RPC directly. peerd should build a rich candidate pool from `gossipRootIps` alone,
without needing a co-located node's data volume mounted.

`tcp_lz4_stats` (`HL_STATS_DIR`) is retained as an **optional supplementary** source, not the
backbone.

## Background

Today `run_peerd` (`src/peerd.rs`) gathers candidates each cycle from three sources: the
`gossipRootIps` API, the node's own `tcp_lz4_stats/<date>` files (`HL_STATS_DIR`), and persisted
`peer_candidates.txt`. The stats-file source is the strongest one but ties discovery to having a
real co-located node. Without it, only a handful of root IPs plus history remain, and `peer.md`
documents the resulting `live=0` collapses.

## `query_peers` wire format (reverse-engineered)

Confirmed on live mainnet (`observations/query_peers-4002-20260708.pcap`). The 4002 RPC needs
**no `TcpGreeting`** — connect and send the framed request directly.

**Request** (to `<peer>:4002`):

```
00 00 00 01  00  01
[u32 BE L=1][type=0][tag=0x01]
```

**Response** — one `type=0` control frame (gossip RPC replies share the control channel; the
client-block RPC is the exception that uses type=1 lz4 data); payload:

```
0x01  <count: HL compact varint>  { 0x00  a b c d  0x01 0x01 } × count
```

- `0x01` = the `Peers` response variant tag.
- `count` = number of entries, HL compact varint (§3 of `docs/hl-p2p-protocol.md`); observed as a
  single byte (tables are small, node default `n_gossip_peers = 8`).
- Each entry is 7 bytes: `0x00` (IPv4 address discriminant) + 4 address octets + a constant
  `0x01 0x01` trailer. Only the 4 octets matter for discovery.
- Errors arrive as `type=0` frames: `0x03 <len> <ascii reason>`.

**Decisive property: `query_peers` is not peer-gated.** In the capture, 42 requests returned peer
tables and 0 were refused, while `query_height` and `client_blocks` to the same peers returned
`Peer-only request`. peerd can therefore crawl the graph standalone — it does not depend on the
reciprocity / inbound-4001 behaviour that limits live-probing.

## Components

### `query_peers(ip) -> Vec<String>` (new, in `src/peerd.rs`)
- Connect `ip:4002` (connect timeout ~4s), write the 6-byte request (write timeout ~3s).
- Read one frame: 5-byte header, then payload with a hard size cap (e.g. 64 KB — peer tables are
  tiny). Bounded read timeouts.
- Parse: require leading `0x01`; read varint count; loop reading a discriminant byte — if `0x00`,
  read 4 octets + 2 trailer bytes and keep the IP; on any unknown discriminant or short read,
  stop and return what was parsed so far.
- Filter through the existing `is_routable`; caller excludes `self_ips`.
- Return `Vec<String>`. All failures degrade to an empty vec (no panics).

### Crawl step (new, replaces the `HL_STATS_DIR` role in `run_peerd`)
Bounded multi-hop BFS per cycle:
- **Seed frontier** = `gossipRootIps` results ∪ persisted `peer_candidates.txt`.
- **Expand** hop by hop up to `PEERD_CRAWL_DEPTH`: query the current frontier concurrently
  (semaphore, `PEERD_CRAWL_CONCURRENCY`), collect newly-seen routable IPs as the next frontier,
  track a `visited` set so no IP is queried twice, and stop early when the per-cycle query budget
  `PEERD_CRAWL_MAX_QUERIES` is reached.
- Merge every discovered IP into the `candidates` set (same set the probe stage consumes).

### Retained sources
- `gossipRootIps` API call: unchanged (also used as BFS seed).
- `HL_STATS_DIR` / `read_node_peers`: kept, but now optional/supplementary — merged into
  candidates if configured, no longer the backbone. `read_node_peers` stays.
- The 4001 live-probe → tip-cluster → rank → `peers.json` write path: unchanged.

## Config (new env vars)
- `PEERD_CRAWL_DEPTH` — max BFS hops per cycle. Default 3, clamp 1–6.
- `PEERD_CRAWL_MAX_QUERIES` — max `query_peers` calls per cycle. Default 200, clamp 10–2000.
- `PEERD_CRAWL_CONCURRENCY` — concurrent `query_peers` sockets. Default 8, clamp 1–64
  (reuses the existing `env_usize` helper).

Existing vars (`HYPERSYNC_DATA`, `HL_STATS_DIR`, `HL_SELF_IP`, `PEERD_PROBE_CONCURRENCY`,
`PEERD_EMPTY_BACKOFF_MAX_SECS`) are unchanged.

## Data flow (per cycle)
1. `gossipRootIps` API → seed IPs.
2. BFS crawl via `query_peers` → discovered IPs merged into `candidates`.
3. (optional) `HL_STATS_DIR` harvest → merged into `candidates`.
4. Persist `candidates` to `peer_candidates.txt` (atomic).
5. Live-probe candidates on 4001, cluster tip, rank, write `peers.json` (unchanged).

## Error handling
- Every network op is timeout-bounded; failures return empty / are skipped, never panic.
- Response length is capped before allocation.
- Parser is defensive: stops at the first malformed/unknown entry and returns partial results.
- A fully failed crawl (all queries error) simply yields no new candidates; the existing
  keep-previous-pool and empty-pool backoff logic still protects `peers.json`.

## Testing
- **Unit — parser**: feed the real captured payloads (n=10 from `64.31.48.111`, n=8 from
  `135.181.138.99`, n=1, n=0) and assert the decoded IP lists. Assert malformed/truncated input
  returns partial/empty without panic.
- **Unit — request builder**: assert the bytes equal `00 00 00 01 00 01`.
- **Unit — BFS bounds**: with a stubbed query function, assert depth cap, query-budget cap, and
  `visited` dedup are all honoured.
- **Manual/live**: run `peerd` against mainnet and confirm the candidate set grows from
  `gossipRootIps` alone (no `HL_STATS_DIR`), and `peers.json` populates.

## Out of scope
- Inbound-dial harvesting and gateway live-peer write-back (separate future enhancements).
- Parsing the `0x01 0x01` entry trailer (unused for discovery).
- Any change to the gateway or the 4001 probe/rank logic.
