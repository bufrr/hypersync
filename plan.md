# Plan: peerd `query_peers` peer discovery

Replace peerd's reliance on the local HL node's `tcp_lz4_stats` files with a self-contained
crawler that speaks the Hyperliquid gossip `query_peers` RPC. Full design:
`docs/superpowers/specs/2026-07-08-peerd-query-peers-discovery-design.md`.

## Reverse-engineered wire format (confirmed live, `observations/query_peers-4002-20260708.pcap`)
- 4002 gossip RPC needs **no** `TcpGreeting`.
- Request:  `00 00 00 01 00 01` = `[u32 BE L=1][type=0][tag=0x01]`.
- Response: `type=0` control frame; payload = `0x01 [count varint] { 0x00 a b c d 0x01 0x01 }×count`.
- `query_peers` is **not peer-gated** (42/42 answered, 0 refused), unlike query_height /
  client_blocks — so peerd can crawl standalone.

## Steps

1. [done] RE `query_peers` from a live capture; save pcap under `observations/`.
2. [done] Design doc written + approved (bounded multi-hop BFS; `tcp_lz4_stats` kept optional).
3. [done] `src/peerd.rs` — pure helpers:
   - `QUERY_PEERS_REQUEST` const, `MAX_QUERY_PEERS_RESP` cap.
   - `read_hl_varint` (compact varint decode).
   - `parse_query_peers_response` (defensive parser: leading `0x01`, varint count, 7-byte
     entries, `is_routable` filter; partial/empty on malformed input, never panics).
4. [done] `src/peerd.rs` — async client + crawl:
   - `query_peers(ip)` — connect 4002, send request, read one bounded frame, parse; empty on any
     failure.
   - `crawl_peers_with(seeds, self_ips, depth, max_queries, concurrency, query)` — generic
     bounded BFS (depth cap, per-cycle query budget, visited-dedup, semaphore concurrency).
   - `crawl_peers(...)` — concrete wrapper calling `query_peers`.
5. [done] Wire into `run_peerd`:
   - New env vars `PEERD_CRAWL_DEPTH` (3, 1–6), `PEERD_CRAWL_MAX_QUERIES` (200, 10–2000),
     `PEERD_CRAWL_CONCURRENCY` (8, 1–64).
   - Crawl step 1b seeded from roots + persisted candidates; merged into `candidates`
     (excluding `self_ips`); `crawl_new` counter.
   - `tcp_lz4_stats` demoted to supplementary (kept).
6. [done] Add `crawl_new` to the peerd cycle log line.
7. [done] Unit tests (all in `src/peerd.rs`):
   - `parse_query_peers_response` against the real captured payloads (n=10 from 64.31.48.111,
     n=8 from 135.181.138.99, n=1, n=0), plus truncated/malformed/error-frame/unroutable inputs.
   - request-builder byte assertion.
   - `read_hl_varint` (single-byte + 0xfb/0xfc/0xfd/0xfe + truncated paths).
   - `crawl_peers_with` bounds via a stubbed query fn (depth cap, query budget, visited dedup,
     self-IP exclusion).
8. [done] Verify: `cargo fmt`, `cargo clippy -D warnings`, `cargo test` — 46 passed.
9. [done] Live-validate (2026-07-08):
   - Host run, no `HL_STATS_DIR`: cycle 1 candidates=551 crawl_new=524 live=461 from
     `gossipRootIps` alone; `peers.json` populated (`/tmp/peerd-live`).
   - Docker compose: `peerd` cycle 1 candidates=556 live=460, cycle 2 candidates=593
     crawl_new=37 live=479. `gw` brought up on the crawl-built `peers.json`; local hl-node
     (`override_gossip_config.json` -> 172.28.0.10, backup at
     `~/node/override_gossip_config.json.bak-pregwtest`) bootstrapped through gw (abci_state
     973 MB + client-block catch-up) and applies live blocks at the network tip; one clean
     active-peer failover observed. Local node also answers `query_peers` (empty table,
     `try_new_peers:false`).

## Out of scope
- Inbound-dial harvesting; gateway live-peer write-back (future).
- Parsing the `0x01 0x01` entry trailer.
- Gateway / 4001 probe & rank changes.
