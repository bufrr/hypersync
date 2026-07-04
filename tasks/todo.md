# Multi-node gateway + standalone gw/peerd compose (2026-07-04)

Goal: one gateway serves multiple hl-nodes (sessions keyed by downstream source IP); gw+peerd run
as their own compose project (deployable on a separate machine — gw reads no node files); tested
end-to-end on this host.

## Implementation
- [x] NodeState/NodeRegistry (active pin, replay cooldown, live floor, last_seen) + bounded
      TTL/cap eviction (pure fn, unit-tested); registry keyed by source IP at accept
- [x] Re-thread run_gateway: per-node active/cooldown/floor on all paths (bootstrap replay
      decision, transparent fallback, live/resume, 4002, misc ports); per-node `[gw] [ip]` logs
- [x] `has_active_session` gate now means "THIS node" (node A live no longer suppresses node B's
      cache replay)
- [x] Capture single-flight: tokio Semaphore permit held by the tap task; loser relays untapped
- [x] CacheSink struct (blob+timestamp+disk path) — clippy arg-count + 4 call sites simplified
- [x] peerd: HL_STATS_DIR / HL_SELF_IP accept comma-separated lists (multi-node inputs)
- [x] fakenode testing subcommand (bootstrap count / live rounds / real 4002 range query;
      machine-checkable summary line; exit bitmask)
- [x] docker-compose.yml (gwnet 172.28.0.0/24, gw static 172.28.0.10, mem 16g, no ports by
      default; remote mode = GW_BIND_IP ports block) + docker-compose.colocated.yml (stats mount)
- [x] README: multi-node semantics, peerd env lists, compose deployment, fakenode
- [x] cargo fmt / clippy -D warnings / test — 25/25 green

## E2E verification (this host, sudo -n docker)
- [ ] Build image; stop old hl-gw/hl-peerd (keep containers for rollback); new project up
      (colocated overlay); verify warm cache adopted
- [ ] fakenode single smoke (boot/live/rpc PASS)
- [ ] fakenode ×2 concurrent: both full replays, distinct actives, zero "active session
      exists" suppressions, no cross-node clears, gw mem < limit
- [ ] Migrate real node: ~/node compose joins hypersync_gwnet, override → 172.28.0.10
      (bind-mounted), recreate; soak-test.sh short run (HL_GW=hypersync-gw)
- [ ] Real node + fakenode churn: node applied-rate unaffected
- [ ] 1-3h soak; then remove orphaned hl-gw/hl-peerd
- Rollback: $HC down; docker start hl-gw hl-peerd; node recreate without gwnet (old override
  re-applied by exec)

## Follow-up
- [ ] Refactor main.rs into modules (protocol/gateway/push/peerd/testing) — pure moves, after soak
- Non-goal (documented): per-node upstream anti-affinity; rr rotation spreads naturally

## Review
(to fill after e2e)
