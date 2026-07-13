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

## Code review round (28 verified findings, high-effort adversarial)
- [x] Fixed: capture-permit lifetime (released when capturing stops, not connection end);
      persist tmp-path race (unique tmp per writer); lazy dial_active pin no longer suppresses
      cache replay (NodeState.lazy_active); guard Drop stamps last_seen before decrement;
      replay-stamp reset via compare_exchange; mem_limit 16g→24g; GREET_TRUE + split_csv
      dedup; &CacheSink; fakenode simplified (3 unused flags dropped, phase fns, shared frame
      helpers, Peer-only leak = rpc FAIL, strict port parse)
- Accepted w/ rationale: same-node dual-greet TOCTOU (unrealistic); N concurrent replays
  (intended, shared Arc); per-replay blob scan (negligible at N=2-3); registry insert at
  accept (bounded, tiny); guard tuple style

## E2E found bug (not in review scope — needed live traffic)
- [x] Cache replay wedged at ~2^31 bytes: single write_all over 4.5GB blob + fast reader =
      one send() treadmills in-kernel until a 32-bit counter overflows; permanent stall,
      Send-Q=0. Fix: 8MB chunked writes (4.4GB now replays in ~1.5s). Real nodes read too
      slowly to trigger — fakenode's discard-speed reads exposed it.

## E2E verification (this host, sudo -n docker)
- [x] Build image; stop old hl-gw/hl-peerd (kept for rollback); new project up (colocated
      overlay); warm cache adopted (4517MB, age 664s); gw static 172.28.0.10; peerd cycle ok
- [x] fakenode single smoke: boot=PASS 4.4GB/1.5s, live=PASS 451 rounds, rpc=PASS (3.5MB
      data batch) — after wedge fix
- [x] fakenode ×2 concurrent: BOTH boot=PASS (concurrent FROM CACHE, zero suppressions),
      both live+rpc PASS, gw mem flat 4.2GiB/24GiB
- [x] Migrate real node: recreated on hypersync_gwnet (172.28.0.129), bind-mounted override →
      172.28.0.10; re-bootstrapped FROM CACHE (2nd attempt after node's own early close —
      cooldown reset path worked); short soak 5×120s: catch-up at ~2800-4900/120s,
      realerr=0 at final sample, gwerr=0 throughout, gw mem stable 4.4GiB
- [x] Real node + fakenode churn: churn1+churn2 both PASS (1.5s replays) during soak s04;
      node stayed connected (src=gw 4001+4002), applied-rate unaffected
- [x] 2h soak (24×300s): 24/24 samples, ZERO bad signals (realerr/gwerr/boot_timeout/oom/rc all
      clean), applied rate steady 4400±100/300s (~14.7 blocks/s), gw mem flat 4.47GiB with
      expected ~8GiB refresh transients every 10min
- [x] Orphaned hl-gw/hl-peerd removed after soak pass (rollback no longer needed)

## Follow-up
- [x] Refactor main.rs into modules — pure moves, done in a worktree during the soak, verified
      (fmt/clippy -D warnings/25 tests, line-coverage audit), merged after soak, redeployed,
      re-smoked with 2 concurrent fakenodes (both PASS), node reattached cleanly
- Non-goal (documented): per-node upstream anti-affinity; rr rotation spreads naturally

## Review

Delivered: one gateway serves N hl-nodes (per-node sessions keyed by source IP); gw+peerd run
as their own compose project deployable on a separate machine (no node-file dependencies);
fully tested on mainnet.

Bugs found and fixed along the way:
1. (review round, 28 verified findings) capture-permit lifetime, persist tmp race, lazy-pin
   replay suppression, guard Drop ordering, stamp clobber, mem_limit, plus cleanup batch.
2. (e2e only — needed live traffic) single write_all over the 4.5GB blob wedges permanently at
   ~2^31 bytes with a fast reader; fixed with 8MB chunked writes (replay now ~1.5s). Real nodes
   read too slowly to ever hit it; captured in memory for future debugging.

Observed quirk (non-blocking): fakenode sometimes parses live-round values ~770.5M (vs main
counter ~1355.7M) depending on which upstream peer its live session pins; the counter advances
at block rate, so it's a property of that peer's stream, not a relay defect. Real node syncing
is unaffected. Worth a look if fakenode round assertions ever get stricter.

Final state: hypersync-gw (172.28.0.10, static) + hypersync-peerd under compose project
"hypersync" with colocated overlay; node dials 172.28.0.10 via bind-mounted
override_gossip_config.json (survives recreation); main.rs split into 5 modules + dispatch;
main @ 899b240.

# Task: revert --push in-session active failover to session teardown (2026-07-10)

Context: 10h no-cache soak (sample 420) — in-session hot swap of the active push peer fed the
node a block from the replacement peer's tip; node saw `Client block invalid parent round`,
disconnected anyway. README:15 always specified teardown; commit 7377800 diverged.

- [x] serve_push: both failover arms (active pump death, node-outbound write failure) now break
      → session teardown; node reconnects and gateway pins a fresh vetted active
- [x] kept bounded 5s write timeout on node->active (detects half-broken peers that send blocks
      but stop draining outbound); timeout now ends the session instead of failing over
- [x] deleted hot-swap machinery: connect_next_active/connect_replacement_active,
      ActiveWriteHalf generation swap, outbound_fail channel, PushConfig.node_greeting,
      on_active_peer callback (+ serve_push_repinned slot dance); net -146 lines
- [x] updated stale comments/log strings (main.rs --push, gateway startup banner, IDLE comment)
- [x] regression test push_active_death_tears_session_down: active dies → exit reason
      "active stream error", node sees EOF (no injected frames), standby pool peer never dialed
- [x] verified: cargo build clean, clippy (1 pre-existing manual_clamp warning on main too),
      53/53 tests pass

# Task: fix 12-min recovery gap after teardown + node child restart (2026-07-12)

Context: 2026-07-11 15:19-15:32 incident (no-cache soak follow-up). Teardown itself was fine;
recovery stacked three amplifiers: (1) select_live_peer scanned the whole pool in 4-peer/3.5s
batches (~70-90s) with no total budget while hl-node abandons a fresh 4001 connection ~5s after
greeting; (2) hl-visor child restart forced a full abci_state bootstrap, and each retry re-dialed
almost the same 32 decliners (rr advanced by 1) with no memory of who serves state; (3) failures
were logged only as "no peer serving abci_state right now".

- [x] select_live_peer: single LIVE_SELECT_WAIT (3.5s) total budget, rolling 4-wide dials,
      first vetted peer wins; dry scan logs a failure summary and falls back fast
- [x] serve_push_4001_fallback: per-peer connect_and_greet budget 5s -> 1.5s so one dead
      candidate can't blow the node's remaining greeting window
- [x] bootstrap: GatewayCtx.state_servers remembers last 8 IPs that served a full abci_state
      (most recent first); tried before the rotated pool window on the next bootstrap
- [x] bootstrap retries advance rr by the 32-peer scan window instead of 1 (sweep fresh peers,
      stop hammering the same decliners/rate limits)
- [x] selection failure summaries: try_fast_bootstrap_peer errors stage-labeled (connect /
      greeting header / small_frame len / state prefetch), summarize_peer_errors buckets both
      live+bootstrap scans, e.g. "tried=32 small_frame=20 connect_fail=8 timeout=3"
- [x] README --cache bullet: recommended in production (child restart otherwise depends on
      external state-server availability)
- [x] tests: bootstrap_candidates ordering/dedup, remember_state_server LRU cap, summary
      bucketing; 56/56 pass, clippy clean (1 pre-existing manual_clamp)
