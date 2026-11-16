use crate::failover::{serve_failover, FailoverConfig, FailoverCtx};
use crate::priority::{gwlog, PrioritySession, PriorityTier};
use crate::protocol::{max_block_round_in_frames, unix_secs, MIN_COMPLETE_BOOTSTRAP_BYTES};
use crate::push::{
    bootstrap_write_timeout, push_exit_is_upstream_fault, serve_push, splice_bootstrap,
    splice_with_idle, BootstrapPace, PushConfig, Relayed, NODE_WRITE_TIMEOUT,
};
use crate::util::lock;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

// The lag guard a newly selected live peer is held to (FreshestRound::select_floor), or the guard
// would cut it seconds in. Hot-standby sessions don't use the guard, so none.
pub(super) fn select_guard(ctx: &GatewayCtx) -> Option<Arc<FreshestRound>> {
    (FailoverConfig::get().standbys == 0).then(|| ctx.freshest.clone())
}

// Run a 4001 push session with the node's 4002 RPC pinned to the same upstream: pin the active
// peer for the session's lifetime, run serve_push, log the exit; the guard clears the pin on any
// exit (incl. a panic in serve_push). Shared by every push-serving path (cache-live, live, fallback).
#[allow(clippy::too_many_arguments)]
async fn serve_push_repinned(
    down: TcpStream,
    upc: TcpStream,
    node: &Arc<NodeState>,
    priority: &Arc<PriorityTier>,
    pool: &Arc<Mutex<Vec<String>>>,
    cooldown: &PeerCooldown,
    // the priority slot live selection already took for this peer (None: take one here)
    prio_session: Option<PrioritySession>,
    mode: &str,
    mut cfg: PushConfig,
) {
    let active_ip = cfg.hosts[cfg.active_idx].clone();
    let active_peer = node.pin_active(&active_ip);
    // clears this pin on any exit; a no-op once a hot-standby switch re-pinned (newer epoch)
    let _active_guard = ActivePinGuard {
        node: node.clone(),
        peer: active_peer.clone(),
    };
    let _prio_session = prio_session.or_else(|| priority.acquire(&active_ip));
    let tier = priority.tier_of(&active_ip);
    let started = Instant::now();
    gwlog!(
        "[gw] [{}] 4001 push session start active_peer={} tier={} mode={} prio_sessions={}",
        node.ip,
        active_ip,
        tier,
        mode,
        priority.sessions(&active_ip)
    );
    // a session that landed on a pool peer moves back once a priority peer serves again (with hot
    // standbys the failover session switches to a priority standby on the same connection instead)
    let standbys = FailoverConfig::get().standbys;
    if standbys == 0 && !priority.is_empty() && !priority.contains(&active_ip) {
        cfg.end_when = Some(Box::pin(wait_priority_return(
            node.clone(),
            priority.clone(),
            cfg.net_round_tip.clone(),
            REPIN,
            cfg.freshest.clone(),
        )));
    }
    // ...and any session ends (so the node reconnects) once its catch-up has stopped converging
    if standbys == 0 {
        let stuck = wait_catchup_stuck(node.clone(), REPIN);
        cfg.end_when = Some(match cfg.end_when.take() {
            Some(other) => Box::pin(async move {
                tokio::select! {
                    reason = other => reason,
                    reason = stuck => reason,
                }
            }),
            None => Box::pin(stuck),
        });
    }
    if standbys > 0 {
        // hot-standby session: the active upstream can change mid-session, so the 4002 pin and the
        // priority session accounting follow every switch. Its standbys already race the active
        // (a lagging one is switched away from), so the freshest-round guard isn't used.
        cfg.freshest = None;
        let pin = Arc::new(Mutex::new((active_peer, _prio_session)));
        let on_switch = {
            let node = node.clone();
            let priority = priority.clone();
            let pin = pin.clone();
            Box::new(move |ip: &str, slot: Option<PrioritySession>| {
                let p = node.pin_active(ip);
                let s = slot.or_else(|| priority.try_acquire(ip));
                *lock(&pin) = (p, s);
            })
        };
        let exit_reason = serve_failover(
            down,
            upc,
            cfg,
            FailoverCtx {
                node_label: node.ip.to_string(),
                pool: pool.clone(),
                priority: priority.clone(),
                on_switch,
            },
        )
        .await;
        let (last_peer, _last_session) = {
            let mut g = lock(&pin);
            let p = g.0.clone();
            (p, g.1.take())
        };
        gwlog!(
            "[gw] [{}] 4001 push session ended active_peer={} tier={} mode={} secs={} reason={}",
            node.ip,
            last_peer.ip,
            priority.tier_of(&last_peer.ip),
            mode,
            started.elapsed().as_secs(),
            exit_reason
        );
        node.clear_active_if_current(&last_peer);
        cool_down_on_fault(priority, cooldown, &last_peer.ip, &exit_reason);
        return;
    }
    let exit_reason = serve_push(down, upc, cfg).await;
    cool_down_on_fault(priority, cooldown, &active_ip, &exit_reason);
    gwlog!(
        "[gw] [{}] 4001 push session ended active_peer={} tier={} mode={} secs={} reason={}",
        node.ip,
        active_ip,
        tier,
        mode,
        started.elapsed().as_secs(),
        exit_reason
    );
}

// A peer whose node session just ended on an upstream fault (stall, round jump, lag, EOF, ...)
// vets fine on a fresh connect but couldn't sustain the stream, so the node's reconnect must not
// land on it again straight away: priority peers take the tier's cooldown, pool peers this one.
const SESSION_FAULT_COOLDOWN: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct PeerCooldown(Mutex<HashMap<String, Instant>>);

impl PeerCooldown {
    pub(super) fn mark(&self, ip: &str, now: Instant) {
        let mut m = lock(&self.0);
        m.retain(|_, until| *until > now);
        m.insert(ip.to_string(), now + SESSION_FAULT_COOLDOWN);
    }

    // `peers` minus those cooling down, order kept; all of them if every one is cooling (a pool
    // that only has recently-faulted peers is still better than none).
    pub(super) fn usable(&self, peers: Vec<String>, now: Instant) -> Vec<String> {
        let mut m = lock(&self.0);
        m.retain(|_, until| *until > now);
        if m.is_empty() {
            return peers;
        }
        let out: Vec<String> = peers
            .iter()
            .filter(|ip| m.get(*ip).is_none_or(|until| *until <= now))
            .cloned()
            .collect();
        if out.is_empty() {
            peers
        } else {
            out
        }
    }
}

pub(super) fn cool_down_on_fault(
    priority: &PriorityTier,
    cooldown: &PeerCooldown,
    ip: &str,
    reason: &str,
) {
    if push_exit_is_upstream_fault(reason) {
        cool_down(priority, cooldown, ip);
    }
}

fn cool_down(priority: &PriorityTier, cooldown: &PeerCooldown, ip: &str) {
    if priority.contains(ip) {
        priority.mark_failed(ip);
    } else {
        cooldown.mark(ip, Instant::now());
    }
}

// 4001 fallback when live-peer selection fails: first pool peer to answer the greeting, served
// through serve_push so the node's live floor / forward gate / round plausibility still apply (a
// raw splice here would forward ungated frames right after a cache replay — the one moment that
// matters most). Selection failing usually means no peer passed the first-block vetting, so
// this trades vetting for availability but keeps the per-frame gates.
#[allow(clippy::too_many_arguments)]
async fn serve_push_4001_fallback(
    down: TcpStream,
    node: Arc<NodeState>,
    peers: &[String],
    start: usize,
    greet: [u8; 8],
    initial_last_forwarded: u32,
    ctx: &GatewayCtx,
    reason: &str,
) {
    let candidates = tiered_peers(&ctx.priority, peers, start);
    let tried = candidates.len().min(FALLBACK_MAX_PARALLEL);
    let Some((ip, upc, greeting, slot)) =
        race_fallback_upstream(&ctx.priority, candidates, greet, FALLBACK_WAIT).await
    else {
        gwlog!(
            "[gw] [{}] 4001 push fallback failed ({reason}; tried={tried} in {}ms)",
            node.ip,
            FALLBACK_WAIT.as_millis()
        );
        return;
    };
    gwlog!(
        "[gw] [{}] 4001 push fallback via {} ({reason})",
        node.ip,
        ip
    );
    serve_push_repinned(
        down,
        upc,
        &node,
        &ctx.priority,
        &ctx.pool,
        &ctx.cooldown,
        slot,
        "fallback",
        PushConfig {
            hosts: Arc::new(vec![ip.clone()]),
            active_idx: 0,
            prefetched_greeting: Some(greeting),
            prefetched_frames: Vec::new(),
            initial_last_forwarded,
            live_floor: node.live_floor.clone(),
            net_round_tip: ctx.net_round_tip.clone(),
            superseded: None,
            end_when: None,
            freshest: Some(ctx.freshest.clone()),
        },
    )
    .await;
}

// A 4001 relay (bootstrap transfer, then live blocks) that hears nothing from its upstream this
// long has a stalled peer: end it so the node reconnects elsewhere. Live blocks arrive several
// times a second and the state transfer streams continuously, so this never cuts a healthy relay.
const RELAY_UPSTREAM_IDLE: Duration = Duration::from_secs(60);

// Replay the cached bootstrap blob to the node, then hand the same connection to a live push
// session (vetted live peer if selection succeeds, else the gated fallback).
async fn replay_cache_then_live(
    mut down: TcpStream,
    node: Arc<NodeState>,
    peers: Vec<String>,
    greet: [u8; 8],
    blob: Arc<Vec<u8>>,
    ctx: GatewayCtx,
) {
    // The node already has its state from the blob; it now needs the live stream it asked for.
    // Keep its broadcast_group/id bytes (group 1 carries the mempool/tx feed) and clear only
    // send_abci, so a cache cold-start does not silently downgrade the node to blocks-only.
    let live_greet = live_greeting_for(greet);
    let now = unix_secs();
    gwlog!(
        "[gw] [{}] node cold-start FROM CACHE ({} MB), no peer state fetch",
        node.ip,
        blob.len() / 1_000_000
    );
    node.last_cache_replay_secs.store(now, Ordering::Release);
    let in_flight = ReplayInFlight(node.clone());
    // Chunked on purpose: one write_all over the whole >4GB blob wedges permanently at ~2^31
    // bytes when the reader drains faster than the send-buffer copy loop (a single send()
    // syscall then never returns to userspace before its byte count overflows). Bounding each
    // write keeps every syscall small; a real node reads too slowly to trigger it, a
    // cache-draining fakenode reliably does.
    let mut replay_err = false;
    let replay_start = Instant::now();
    for chunk in blob.chunks(8 * 1024 * 1024) {
        if !node_write(
            &mut down,
            chunk,
            bootstrap_write_timeout(replay_start, Instant::now()),
        )
        .await
        {
            replay_err = true;
            break;
        }
    }
    drop(in_flight);
    if replay_err {
        // The cooldown stamp stays: clearing it let a client that reads a few bytes and closes
        // trigger full 5.8GB replays back to back. A node restarting within the cooldown simply
        // bootstraps transparently from a peer.
        gwlog!(
            "[gw] [{}] cache replay write failed; node will re-bootstrap",
            node.ip
        );
        return;
    }
    node.last_cache_replay_secs
        .store(unix_secs(), Ordering::Release);
    let blob_for_scan = blob.clone();
    let cache_floor =
        tokio::task::spawn_blocking(move || max_block_round_in_frames(&blob_for_scan))
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
    // The floor only has to drop the live backlog the node already got from the blob, i.e. for
    // the minutes right after this replay. It comes from the cache's own tail, so it must not
    // outrank the network tip (a blob ending in fake high rounds would otherwise make this node
    // drop every real block): bounded by the tip when known, never raises the tip, and cleared
    // after CACHE_FLOOR_TTL.
    let net_tip = ctx.net_round_tip.load(Ordering::Acquire);
    let scanned_floor = cache_floor;
    let cache_floor = bounded_cache_floor(scanned_floor, net_tip);
    if scanned_floor != 0 && cache_floor == 0 {
        gwlog!(
            "[gw] [{}] cache replay floor {scanned_floor} ignored: above network tip {net_tip}",
            node.ip
        );
    }
    if cache_floor != 0 && raise_round_floor(&node.live_floor, cache_floor) {
        gwlog!(
            "[gw] [{}] cache replay live floor round={cache_floor}",
            node.ip
        );
        let floor = node.live_floor.clone();
        tokio::spawn(async move {
            tokio::time::sleep(CACHE_FLOOR_TTL).await;
            let _ = floor.compare_exchange(cache_floor, 0, Ordering::AcqRel, Ordering::Acquire);
        });
    }
    let start = ctx.rr.fetch_add(1, Ordering::Relaxed);
    if let Some(live) = select_live_peer(
        &ctx.priority,
        &peers,
        start,
        live_greet,
        cache_floor,
        ctx.net_round_tip.load(Ordering::Acquire),
        select_guard(&ctx),
    )
    .await
    {
        gwlog!(
            "[gw] [{}] selected live peer {} tier={} first_round={} greeting={}",
            node.ip,
            live.ip,
            ctx.priority.tier_of(&live.ip),
            live.first_round,
            hex_bytes(&live.greeting)
        );
        let hosts = vec![live.ip.clone()];
        serve_push_repinned(
            down,
            live.stream,
            &node,
            &ctx.priority,
            &ctx.pool,
            &ctx.cooldown,
            live.prio_session,
            "cache-live",
            PushConfig {
                hosts: Arc::new(hosts),
                active_idx: 0,
                prefetched_greeting: Some(live.greeting),
                prefetched_frames: live.prefetched_frames,
                initial_last_forwarded: cache_floor,
                live_floor: node.live_floor.clone(),
                net_round_tip: ctx.net_round_tip.clone(),
                superseded: None,
                end_when: None,
                freshest: Some(ctx.freshest.clone()),
            },
        )
        .await;
        return;
    }
    serve_push_4001_fallback(
        down,
        node,
        &peers,
        start,
        live_greet,
        cache_floor,
        &ctx,
        "cache replay live selection failed",
    )
    .await;
}

// A sibling hl-node prepares the state on each request before its first byte (2026-11-16: 13.5-
// 14.8 s for a 1.40 GB state, then 309 MB/s over the LAN; once over 20 s while it was busy with a
// slow hash computation), so it gets a longer header wait than an upstream server. The rest of a
// 1.4 GB state at LAN speed takes ~5 s, so a sibling that starts within 30 s still beats the
// node's ~60 s abci_state deadline; one that doesn't sends the node's retry upstream
// (SIBLING_LATE).
#[cfg(not(test))]
const SIBLING_HEADER_WAIT: Duration = Duration::from_secs(30);
#[cfg(not(test))]
const SIBLING_SELECT_WAIT: Duration = Duration::from_secs(35);
#[cfg(test)]
const SIBLING_HEADER_WAIT: Duration = Duration::from_millis(400);
#[cfg(test)]
const SIBLING_SELECT_WAIT: Duration = Duration::from_millis(500);
// A sibling that failed as a source (declined, too slow, ended early) isn't asked again for this
// long: the next attempts go straight upstream.
const SIBLING_SLOW_TTL: Duration = Duration::from_secs(10 * 60);
// A node asking for a bootstrap again within this long after a sibling's bootstrap relay ended
// didn't get a usable state from it (cut short, or the state didn't load): that sibling is
// skipped. Bytes delivered can't tell: a complete bootstrap is ~5.8 GB, and a sibling may close
// the connection after it (2026-11-16: one ended at 6.57 GB, the node went live).
const SIBLING_RETRY_SIGN: Duration = Duration::from_secs(5 * 60);
// A failed sibling attempt that took longer than this leaves upstream servers too little of the
// node's ~60 s abci_state deadline (a 1.4 GB state in the ~30 s left needs ~50 MB/s; upstream
// servers measured 24-34 MB/s). The gateway then closes the node's connection instead: hl-node
// retries within ~2 s with a fresh deadline, and with the failed siblings skipped
// (SIBLING_SLOW_TTL), that retry goes straight to upstream peers.
#[cfg(not(test))]
const SIBLING_LATE: Duration = Duration::from_secs(8);
#[cfg(test)]
const SIBLING_LATE: Duration = Duration::from_millis(300);

// Transparent bootstrap relay: pick a peer actually serving the abci_state right now and splice,
// tapping the stream into the cache when --cache holds the single-flight tap permit.
async fn transparent_bootstrap(
    mut down: TcpStream,
    node: Arc<NodeState>,
    peers: Vec<String>,
    greet: [u8; 8],
    ctx: GatewayCtx,
) {
    // the greeting was read just before this: the node's ~60 s abci_state deadline runs from here
    let node_greeted = Instant::now();
    // the node is getting fresh state from a peer: no earlier cache replay's floor applies
    node.live_floor.store(0, Ordering::Release);
    // Advance rr by the whole scan window: retries during an incident sweep fresh pool peers
    // instead of re-dialing the same ~32 decliners (and their rate limits) shifted by one.
    let start = ctx
        .rr
        .fetch_add(BOOTSTRAP_SELECT_MAX_PEERS, Ordering::Relaxed);
    // A synced node behind this gateway serves the state first, over the LAN: no upstream
    // traffic and no upstream rate limit. Upstream peers only when none of them can.
    let sibling = match &ctx.live_hub {
        Some(hubs) if sibling_sync_enabled() => {
            let siblings = hubs
                .bootstrap_siblings(node.ip, ctx.freshest.get(), &ctx.slow_state_servers)
                .await;
            if siblings.is_empty() {
                None
            } else {
                let picked = timeout(
                    SIBLING_SELECT_WAIT,
                    select_fast_bootstrap_peer(
                        &ctx.priority,
                        &siblings,
                        0,
                        greet,
                        node_greeted,
                        SIBLING_HEADER_WAIT,
                    ),
                )
                .await
                .ok()
                .flatten();
                if picked.is_none() {
                    let now = Instant::now();
                    for s in &siblings {
                        ctx.slow_state_servers.mark_for(s, now, SIBLING_SLOW_TTL);
                    }
                    let late = node_greeted.elapsed() > SIBLING_LATE;
                    gwlog!(
                        "[gw] [{}] no sibling node served abci_state (tried {} in {:.0}s; not asked again for {} min); {}",
                        node.ip,
                        siblings.join(","),
                        node_greeted.elapsed().as_secs_f64(),
                        SIBLING_SLOW_TTL.as_secs() / 60,
                        if late {
                            "closing so the node retries upstream with a full deadline"
                        } else {
                            "asking upstream peers"
                        }
                    );
                    if late {
                        return;
                    }
                }
                picked
            }
        }
        _ => None,
    };
    let from_sibling = sibling.is_some();
    let mut last_resort = false;
    let selected = if let Some(s) = sibling {
        s
    } else {
        let remembered = lock(&ctx.state_servers).clone();
        let candidates = bootstrap_source_list(
            ctx.priority.bootstrap,
            ctx.priority.candidates(),
            remembered,
            &peers,
            start,
        );
        let (candidates, all_slow) = ctx.slow_state_servers.filter(candidates, Instant::now());
        last_resort = all_slow;
        if last_resort {
            gwlog!(
                "[gw] [{}] every bootstrap candidate trickled recently; keeping a slow server this time (no rate cut)",
                node.ip
            );
        }
        let Some(selected) = select_fast_bootstrap_peer(
            &ctx.priority,
            &candidates,
            0,
            greet,
            node_greeted,
            STATE_HEADER_WAIT,
        )
        .await
        else {
            gwlog!("[gw] [{}] no peer serving abci_state right now", node.ip);
            return;
        };
        selected
    };
    let BootstrapPeerSelection {
        upc,
        hdr,
        payload_prefix,
        ip,
        abci_len,
        prio_session: _prio_session,
    } = selected;
    gwlog!(
        "[gw] [{}] node bootstrap via {ip} tier={} (abci_state {} bytes, prefetched {} MB); active set",
        node.ip,
        if from_sibling {
            "sibling"
        } else {
            ctx.priority.tier_of(&ip)
        },
        abci_len,
        payload_prefix.len() / 1_000_000
    );
    // never remember a priority peer (it would head every later candidate list and turn the
    // fallback policy into prefer), nor a sibling node (it isn't an upstream)
    if !from_sibling && !ctx.priority.contains(&ip) {
        remember_state_server(&ctx.state_servers, &ip);
    }
    // the node's 4002 goes upstream: a sibling's state still pins it to the shared stream's peer
    // (the node joins that stream next), or to nothing until its live session pins one
    let pin = if from_sibling {
        ctx.live_hub.as_ref().and_then(|h| h.peer())
    } else {
        Some(ip.clone())
    };
    let _active_guard = pin.map(|p| ActivePinGuard {
        node: node.clone(),
        peer: node.pin_active(&p),
    });
    // never cache a sibling's state: --cache replays only what an upstream state server served
    let tap_permit = if ctx.cache_coldstart && !from_sibling {
        ctx.capture_tap.clone().try_acquire_owned().ok()
    } else {
        None
    };
    let relayed = if let Some(permit) = tap_permit {
        splice_bootstrap_capture(
            down,
            upc,
            hdr,
            payload_prefix,
            CacheSink {
                boot_blob: ctx.boot_blob.clone(),
                boot_blob_cached_at: ctx.boot_blob_cached_at.clone(),
                disk_cache_path: ctx.disk_cache_path.clone(),
            },
            permit,
            RELAY_UPSTREAM_IDLE,
            !last_resort,
        )
        .await
    } else {
        if ctx.cache_coldstart {
            gwlog!(
                "[gw] [{}] bootstrap capture already in flight; relaying without tap",
                node.ip
            );
        }
        // Bounded by the bootstrap window, which the relay below continues: the 64MB prefetch is
        // one write, and a node on a link under ~17 Mbit/s needs more than NODE_WRITE_TIMEOUT for
        // it. A node that stops reading still can't park this task (and its pin, priority slot
        // and upstream connection) until TCP gives up.
        let started = Instant::now();
        for chunk in [&hdr[..], &payload_prefix[..]] {
            if !node_write(
                &mut down,
                chunk,
                bootstrap_write_timeout(started, Instant::now()),
            )
            .await
            {
                return;
            }
        }
        // Free the 64MB prefetch before the splice: the bootstrap connection can stay open long
        // after the state transfer, and keeping it in scope held 64MB per open bootstrap
        // connection (2026-10-21 2-node soak: 68MB resident for 25+ min).
        let prefetched = (hdr.len() + payload_prefix.len()) as u64;
        drop(payload_prefix);
        let mut r = splice_bootstrap(
            down,
            upc,
            Some(RELAY_UPSTREAM_IDLE),
            started,
            (!last_resort).then(|| BootstrapPace::new(prefetched)),
        )
        .await;
        r.to_node += prefetched;
        r
    };
    // remembered above so concurrent bootstraps benefit at once; a server whose side ended the
    // relay before the whole bootstrap (abci_state + EVM KVs) was through must not head the next
    // one (a node that left mid-transfer says nothing about the server)
    if relayed.upstream_ended && relayed.to_node < MIN_COMPLETE_BOOTSTRAP_BYTES as u64 {
        forget_state_server(&ctx.state_servers, &ip);
    }
    // the node reconnects within seconds: a server cut for trickling must not win that
    // reconnect's selection race again (it serves the abci_state header as fast as any), nor a
    // later one, wherever it would come from (remembered list or pool)
    // The node gives up on a stalling abci_state itself (its read deadline, ~20-30 s without
    // progress: "could not read abci state ... deadline has elapsed"), before the 60 s idle cut and
    // long before a rate window. The node ending the relay hides that from upstream_ended, so a
    // relay that ended inside the abci_state frame counts as the server's failure too.
    let state_frame = 5 + abci_len as u64;
    let ended_in_state = !relayed.too_slow && relayed.to_node < state_frame;
    // A sibling relay that ended short of a whole bootstrap failed for sure: skip that sibling now.
    // One that ended past it may or may not have completed (bytes can't tell): remembered, and
    // judged by whether the node asks for a bootstrap again (SIBLING_RETRY_SIGN).
    if from_sibling {
        if relayed.to_node < MIN_COMPLETE_BOOTSTRAP_BYTES as u64 {
            ctx.slow_state_servers
                .mark_for(&ip, Instant::now(), SIBLING_SLOW_TTL);
            gwlog!(
                "[gw] [{}] bootstrap via sibling {ip} ended at {} MB, short of a bootstrap; not asked again for {} min",
                node.ip,
                relayed.to_node / 1_000_000,
                SIBLING_SLOW_TTL.as_secs() / 60
            );
        } else {
            *lock(&node.last_sibling_bootstrap) = Some((ip.clone(), Instant::now()));
            gwlog!(
                "[gw] [{}] bootstrap via sibling {ip} ended at {} MB",
                node.ip,
                relayed.to_node / 1_000_000
            );
        }
    } else if relayed.too_slow || ended_in_state {
        let ttl = ctx
            .slow_state_servers
            .mark(&ip, Instant::now(), ctx.priority.contains(&ip));
        forget_state_server(&ctx.state_servers, &ip);
        if ended_in_state {
            gwlog!(
                "[gw] [{}] bootstrap via {ip} ended inside the abci_state ({} of {} MB); not used for bootstraps for {} min",
                node.ip,
                relayed.to_node / 1_000_000,
                state_frame / 1_000_000,
                ttl.as_secs() / 60
            );
        }
    }
}

// A node bootstrapping again within SIBLING_RETRY_SIGN of a sibling's bootstrap relay ending:
// that sibling's bootstrap didn't take, so it is skipped (SIBLING_SLOW_TTL). The record is
// consumed either way.
fn note_bootstrap_again(node: &NodeState, ctx: &GatewayCtx) {
    let now = Instant::now();
    if let Some((sib, ended)) = lock(&node.last_sibling_bootstrap).take() {
        if now.saturating_duration_since(ended) < SIBLING_RETRY_SIGN {
            ctx.slow_state_servers.mark_for(&sib, now, SIBLING_SLOW_TTL);
            gwlog!(
                "[gw] [{}] bootstrapping again {}s after sibling {sib}'s bootstrap: it isn't asked again for {} min",
                node.ip,
                now.saturating_duration_since(ended).as_secs(),
                SIBLING_SLOW_TTL.as_secs() / 60
            );
        }
    }
}

// 4001 with a bootstrap greet (send_abci:true). With --cache + a fresh captured snapshot, serve
// it FROM CACHE (no peer state fetch, no rate-limit); then stream live from a pool peer — the
// node catches up via the client-block RPC (4002, fetch-forwarded). Otherwise fall through to a
// transparent relay so the node keeps a real peer relationship.
pub(super) async fn handle_4001_bootstrap(
    down: TcpStream,
    node: Arc<NodeState>,
    peers: Vec<String>,
    greet: [u8; 8],
    ctx: GatewayCtx,
) {
    // judged before cache or transparent is chosen: a retry served from --cache must still count
    // against the sibling whose bootstrap didn't take
    note_bootstrap_again(&node, &ctx);
    if ctx.cache_coldstart {
        if let Some(blob) = cache_replay_blob(&node, &ctx) {
            replay_cache_then_live(down, node, peers, greet, blob, ctx).await;
            return;
        }
    }
    transparent_bootstrap(down, node, peers, greet, ctx).await;
}

// 4001 live/resume channel: choose a reachable peer, make it THIS node's active session peer (so
// its client-block RPC on 4002 hits the same peer), forward the greeting, and relay. This is what
// (re)establishes `active`. Push mode vets the peer's first live block and merge-serves; plain
// mode transparently splices the first reachable peer.
// A node's live session on the shared stream: its 4002 is pinned to the stream's peer for the
// session, and it ends (so the node reconnects) on its own catch-up watchdog as any push session
// does. The stream itself is judged, cooled down and logged by LiveHub, once for all its nodes.
async fn serve_hub_session(
    down: TcpStream,
    node: Arc<NodeState>,
    hub: Arc<Hub>,
    sub: crate::push::Subscription,
    ctx: &GatewayCtx,
) {
    let _active_guard = ActivePinGuard {
        node: node.clone(),
        peer: node.pin_active(&hub.ip),
    };
    let _member = hub.member(&node);
    let tier = ctx.priority.tier_of(&hub.ip);
    let started = Instant::now();
    gwlog!(
        "[gw] [{}] 4001 push session start active_peer={} tier={} mode=shared nodes={} prio_sessions={}",
        node.ip,
        hub.ip,
        tier,
        hub.nodes(),
        ctx.priority.sessions(&hub.ip)
    );
    let (reason, stats) = crate::push::serve_subscriber(
        down,
        &hub.greeting,
        sub,
        node.live_floor.clone(),
        Some(Box::pin(wait_catchup_stuck(node.clone(), REPIN))),
    )
    .await;
    gwlog!(
        "[gw] [{}] 4001 push session ended active_peer={} tier={} mode=shared secs={} reason={} stats=[blocks={} bytes={} drop_floor={}]",
        node.ip,
        hub.ip,
        tier,
        started.elapsed().as_secs(),
        reason,
        stats.blocks,
        stats.bytes,
        stats.dropped_floor
    );
}

pub(super) async fn handle_4001_live(
    down: TcpStream,
    node: Arc<NodeState>,
    peers: Vec<String>,
    greet: [u8; 8],
    ctx: GatewayCtx,
) {
    let start = ctx.rr.fetch_add(1, Ordering::Relaxed);
    if let (true, Some(hubs)) = (ctx.push, ctx.live_hub.clone()) {
        match hubs.join(greet, &peers, start, &ctx).await {
            Join::Joined(hub, sub) => {
                serve_hub_session(down, node, hub, sub, &ctx).await;
                return;
            }
            Join::NoPeer => {
                serve_push_4001_fallback(
                    down,
                    node,
                    &peers,
                    start,
                    greet,
                    0,
                    &ctx,
                    "shared live selection failed",
                )
                .await;
                return;
            }
            Join::OtherGreeting => gwlog!(
                "[gw] [{}] 4001 greeting {} differs from the shared stream's; serving this node on its own",
                node.ip,
                hex_bytes(&greet)
            ),
        }
    }
    if ctx.push {
        if let Some(live) = select_live_peer(
            &ctx.priority,
            &peers,
            start,
            greet,
            0,
            ctx.net_round_tip.load(Ordering::Acquire),
            select_guard(&ctx),
        )
        .await
        {
            gwlog!(
                "[gw] [{}] selected live peer {} tier={} first_round={} greeting={}",
                node.ip,
                live.ip,
                ctx.priority.tier_of(&live.ip),
                live.first_round,
                hex_bytes(&live.greeting)
            );
            // active stays transparent and owns the peer relationship used by 4002
            let hosts = vec![live.ip.clone()];
            serve_push_repinned(
                down,
                live.stream,
                &node,
                &ctx.priority,
                &ctx.pool,
                &ctx.cooldown,
                live.prio_session,
                "live",
                PushConfig {
                    hosts: Arc::new(hosts),
                    active_idx: 0,
                    prefetched_greeting: Some(live.greeting),
                    prefetched_frames: live.prefetched_frames,
                    initial_last_forwarded: 0,
                    live_floor: node.live_floor.clone(),
                    net_round_tip: ctx.net_round_tip.clone(),
                    superseded: None,
                    end_when: None,
                    freshest: Some(ctx.freshest.clone()),
                },
            )
            .await;
        } else {
            serve_push_4001_fallback(
                down,
                node,
                &peers,
                start,
                greet,
                0,
                &ctx,
                "push live selection failed",
            )
            .await;
        }
        return;
    }
    // Transparent splice. Race the candidates (priority first) like the push fallback: the first
    // peer to answer the greeting wins, a refusal (peer full) or a hung peer is skipped instead of
    // being handed to the node as its greeting, failed priority peers cool down, and the whole
    // race fits the node's ~5s live-connect deadline (one hung candidate dialed sequentially used
    // to burn all of it).
    let t0 = Instant::now();
    let candidates = tiered_peers(&ctx.priority, &peers, start);
    let tried = candidates.len().min(FALLBACK_MAX_PARALLEL);
    let Some((ip, upc, greeting, slot)) =
        race_fallback_upstream(&ctx.priority, candidates, greet, LIVE_SELECT_WAIT).await
    else {
        gwlog!(
            "[gw] [{}] 4001 splice: no upstream answered (tried={tried})",
            node.ip
        );
        return;
    };
    let _active_guard = ActivePinGuard {
        node: node.clone(),
        peer: node.pin_active(&ip),
    };
    let _prio_session = slot;
    gwlog!(
        "[gw] [{}] 4001 splice start peer={} tier={} connect_ms={} prio_sessions={}",
        node.ip,
        ip,
        ctx.priority.tier_of(&ip),
        t0.elapsed().as_millis(),
        ctx.priority.sessions(&ip)
    );
    let mut down = down;
    if !matches!(
        timeout(NODE_WRITE_TIMEOUT, down.write_all(&greeting)).await,
        Ok(Ok(()))
    ) {
        return;
    }
    let Relayed {
        to_node,
        to_peer,
        upstream_ended,
        ..
    } = splice_with_idle(down, upc, Some(RELAY_UPSTREAM_IDLE)).await;
    if upstream_ended {
        cool_down(&ctx.priority, &ctx.cooldown, &ip);
    }
    gwlog!(
        "[gw] [{}] 4001 splice ended peer={} tier={} secs={} bytes_to_node={} bytes_to_peer={}",
        node.ip,
        ip,
        ctx.priority.tier_of(&ip),
        t0.elapsed().as_secs(),
        to_node,
        to_peer
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::priority::BootstrapPolicy;

    #[test]
    fn a_session_fault_cools_the_peer_down_for_the_next_selection() {
        let pool: Vec<String> = ["1.1.1.1", "2.2.2.2", "3.3.3.3"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let cooldown = PeerCooldown::default();
        let priority = PriorityTier::new(
            vec!["9.9.9.9".into()],
            0,
            Duration::from_millis(1500),
            BootstrapPolicy::Fallback,
        );
        // the node leaving is not the peer's fault
        cool_down_on_fault(&priority, &cooldown, "2.2.2.2", "node outbound ended");
        cool_down_on_fault(
            &priority,
            &cooldown,
            "2.2.2.2",
            "node outbound read failed: x",
        );
        for internal_or_node in ["node writer closed", "event channel closed"] {
            cool_down_on_fault(&priority, &cooldown, "2.2.2.2", internal_or_node);
        }
        assert_eq!(cooldown.usable(pool.clone(), Instant::now()), pool);
        // a stalled stream is: the reconnect skips the peer until the cooldown runs out
        cool_down_on_fault(
            &priority,
            &cooldown,
            "2.2.2.2",
            "active stream error: no block progress for 30s",
        );
        let now = Instant::now();
        assert_eq!(cooldown.usable(pool.clone(), now), ["1.1.1.1", "3.3.3.3"]);
        // a pool of only cooling peers is still used
        assert_eq!(cooldown.usable(vec!["2.2.2.2".into()], now), ["2.2.2.2"]);
        // a priority peer takes the tier's own cooldown
        cool_down_on_fault(
            &priority,
            &cooldown,
            "9.9.9.9",
            "active stream error: early eof",
        );
        assert!(priority.candidates().is_empty());
        assert_eq!(cooldown.usable(pool.clone(), now), ["1.1.1.1", "3.3.3.3"]);
        // once the cooldown has run out the peer is back, and its entry is gone
        let later = now + SESSION_FAULT_COOLDOWN + Duration::from_secs(1);
        assert_eq!(cooldown.usable(pool.clone(), later), pool);
        assert!(lock(&cooldown.0).is_empty());
    }
}
