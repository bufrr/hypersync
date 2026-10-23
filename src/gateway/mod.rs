mod cache;
mod client_blocks;
mod node;
mod pace;
mod select;
mod session;

use crate::peerd::{extract_round_tip, read_node_peers};
use crate::priority::{gwlog, PriorityTier};
use crate::protocol::unix_secs;
use crate::push::{splice, FreshestRound, FRESHEST_STALE, PEER_LAG_GRACE, PEER_LAG_ROUNDS};
use crate::util::lock;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

// every submodule `use super::*`s: its siblings' pub(super) items resolve through these globs
#[allow(unused_imports)]
use self::{cache::*, client_blocks::*, node::*, pace::*, select::*, session::*};

// ---- full P2P gateway ----

// Connect to the session's active peer (the one serving the node's bootstrap) on `port`, waiting
// briefly for the bootstrap to set it; falls back to a round-robin pool peer if none is set yet.
async fn dial_active(
    node: &NodeState,
    priority: &PriorityTier,
    peers: &[String],
    rr: &Arc<AtomicUsize>,
    port: u16,
) -> Option<TcpStream> {
    // brief wait in case a concurrent bootstrap is about to set the active peer
    let mut target = None;
    for _ in 0..15 {
        if let Some(a) = node.active_snapshot() {
            target = Some(a);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // no active yet (e.g. a resuming node after a gateway restart, with no fresh bootstrap): establish
    // one now so EVERY connection of this node converges on the same upstream (client-block RPC needs
    // it). Marked lazy: this pin must not suppress a cache replay if the node is actually cold-starting
    // and its 4001 bootstrap greet simply hasn't arrived yet.
    // An existing pin is used even with an empty pool, and a priority-only deployment (empty
    // pool) still has candidates: the priority tier — as does an exclusive tier while any of its
    // peers is usable.
    let target = match target {
        Some(t) => t,
        None => {
            let t = if peers.is_empty() || !priority.pool_allowed() {
                priority.candidates().into_iter().next()?
            } else {
                peers[rr.fetch_add(1, Ordering::Relaxed) % peers.len()].clone()
            };
            node.pin_lazy_active(&t)
        }
    };
    let up = format!("{}:{}", target.ip, port);
    match timeout(Duration::from_secs(5), crate::blacklist::connect(&up)).await {
        Ok(Ok(c)) => {
            c.set_nodelay(true).ok();
            Some(c)
        }
        // do NOT clear active on a transient failure (that flaps the active peer and makes the
        // client-block RPC land on a peer that doesn't know this node -> "Peer-only request").
        // The active peer is (re)set by the node's 4001 block-stream connection instead.
        _ => None,
    }
}

// Shared gateway state threaded into the per-port connection handlers (Arcs + flags, cheap to
// clone per connection).
#[derive(Clone)]
struct GatewayCtx {
    rr: Arc<AtomicUsize>,
    boot_blob: Arc<Mutex<Option<Arc<Vec<u8>>>>>,
    boot_blob_cached_at: Arc<AtomicU64>,
    disk_cache_path: Option<PathBuf>,
    capture_tap: Arc<tokio::sync::Semaphore>,
    net_round_tip: Arc<AtomicU32>,
    // IPs that recently served a full abci_state (most recent first, capped). Tried before the
    // rotated pool window on the next bootstrap: when most of the pool declines to serve state
    // (the common failure), a known server cuts recovery from minutes of rescanning to seconds.
    state_servers: Arc<Mutex<Vec<String>>>,
    priority: Arc<PriorityTier>,
    // the live peer pool (peerd's file, refreshed every 30s) — failover standbys are drawn from it
    pool: Arc<Mutex<Vec<String>>>,
    // pool peers whose last node session ended on an upstream fault, skipped for a while
    cooldown: Arc<PeerCooldown>,
    // the freshest round any upstream stream of this gateway delivered (push sessions and the
    // pace reference stream feed it; a session lagging it ends — see push::FreshestRound)
    freshest: Arc<FreshestRound>,
    cache_coldstart: bool,
    push: bool,
}

// peerd's round_tip from the peer file, only if peerd wrote it recently: the tip caps plausible
// rounds at tip + NET_TIP_SLACK_ROUNDS (~4h), so a file left over from a gateway that was down for
// weeks would make every real round look implausible.
const PEER_FILE_TIP_MAX_AGE: Duration = Duration::from_secs(15 * 60);

// The upstream pool in a peer file: priority peers (raced as their own tier) and blacklisted IPs
// removed.
fn read_pool(priority: &PriorityTier, path: &str) -> Vec<String> {
    crate::blacklist::filtered(priority.strip(read_node_peers(path)))
}

// The gateway's ip_blacklist.txt lives next to its peer file (/pd in compose, peerd's data dir).
fn peer_file_dir(path: &str) -> PathBuf {
    match Path::new(path).parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

// The refresher's tip update for one peer-file read (None = leave the tip as it is). peerd
// re-measures the tip every cycle, so a fresh reading REPLACES the old one: a bad cycle lasts one
// cycle instead of pinning the tip forever. With no fresh reading for PEER_FILE_TIP_MAX_AGE (peerd
// stalled, kept_previous, or publishing round_tip:null) the tip drops to 0 = unknown: floor-only
// plausibility and no lag guard. A frozen tip would otherwise fall behind mainnet and, ~4h later,
// make every real round exceed tip + NET_TIP_SLACK_ROUNDS and starve all live serving.
fn next_net_tip(fresh: Option<u32>, since_fresh: Duration) -> Option<u32> {
    match fresh {
        Some(tip) => Some(tip),
        None if since_fresh > PEER_FILE_TIP_MAX_AGE => Some(0),
        None => None,
    }
}

fn fresh_peer_file_tip(path: &str) -> Option<u32> {
    let age = std::fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .elapsed()
        .ok()?;
    if age > PEER_FILE_TIP_MAX_AGE {
        return None;
    }
    extract_round_tip(&std::fs::read_to_string(path).ok()?)
}

// Full P2P gateway. Every downstream node connects ONLY to the gateway; the gateway provides all of
// HL's sync P2P backed by MULTIPLE upstream peers (the priority tier first, then the pool from the
// node's own peer file, a startup path arg):
//   - bootstrap (abci_state + EVM KVs, ~5.8GB in 2026-10): relayed transparently from a peer that is
//     serving it right now; with --cache also captured and replayed from memory/disk on cold-start;
//   - live blocks (4001): one vetted active peer, transparent splice or (--push) round-gated push;
//   - client-block RPC (4002): fetched from the active peer (hedged to others) and forwarded;
//     other gossip ports are transparently spliced.
// If the active peer has a problem the session tears down and the node's reconnect elects a fresh
// peer from the (continuously refreshed) pool.
pub(crate) async fn run_gateway(node_peer_file: String, push: bool, cache_coldstart: bool) {
    let priority = Arc::new(PriorityTier::from_env());
    let blacklist_dir = peer_file_dir(&node_peer_file);
    crate::blacklist::reload("gw", &blacklist_dir);
    let pool: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(read_pool(&priority, &node_peer_file)));
    // cached verbatim bootstrap (abci_state + EVM KVs) for cold-start without a peer state fetch
    let boot_blob: Arc<Mutex<Option<Arc<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let boot_blob_cached_at = Arc::new(AtomicU64::new(0));
    let disk_cache_path = cache_coldstart.then(|| bootstrap_cache_path(&node_peer_file));
    if let Some(path) = &disk_cache_path {
        remove_orphan_cache_tmps(path);
        match load_bootstrap_cache(path, unix_secs()) {
            Ok(Some((blob, cached_at))) => {
                gwlog!(
                    "[gw] bootstrap cache loaded from {} ({} MB, age={}s)",
                    path.display(),
                    blob.len() / 1_000_000,
                    unix_secs().saturating_sub(cached_at)
                );
                *lock(&boot_blob) = Some(Arc::new(blob));
                boot_blob_cached_at.store(cached_at, Ordering::Release);
            }
            Ok(None) => {
                gwlog!("[gw] no disk bootstrap cache at {}", path.display());
            }
            Err(e) => {
                gwlog!(
                    "[gw] disk bootstrap cache ignored at {}: {e}",
                    path.display()
                );
            }
        }
    }
    let rr = Arc::new(AtomicUsize::new(0)); // round-robin so successive bootstraps pick fresh peers
                                            // per-downstream-node session state (active peer pin, replay cooldown, live floor), keyed by
                                            // source IP. rr rotation naturally spreads different nodes across upstream peers.
    let nodes = Arc::new(NodeRegistry::default());
    // Single-flight for the transparent-fallback cache tap: concurrent fallback bootstraps must not
    // stack multiple multi-GB tap buffers or race the disk cache file; the loser relays untapped.
    let capture_tap = Arc::new(tokio::sync::Semaphore::new(1));
    // Best-known network round tip (0 = unknown). Set from peerd's corroborated round_tip in
    // peers.json (see next_net_tip: each fresh reading replaces the last, no reading for 15 min
    // resets it to 0); --cache scans may also raise it between readings. Reference for both the
    // tip-relative round-plausibility ceiling and the ACTIVE_ROUND_MAX_LAG freshness guard, which a
    // too-high tip tightens — hence peerd publishes a round at least two peers back.
    let net_round_tip = Arc::new(AtomicU32::new(0));
    if let Some(tip) = fresh_peer_file_tip(&node_peer_file) {
        net_round_tip.store(tip, Ordering::Release);
    }
    gwlog!(
        "[gw] full P2P gateway: pool from {} ({} peers); bootstrap={}; live={}",
        node_peer_file,
        lock(&pool).len(),
        if cache_coldstart {
            "cache-coldstart(+transparent fallback)"
        } else {
            "transparent"
        },
        if push {
            "block-push(round-gated active peer)"
        } else {
            "transparent splice"
        }
    );
    gwlog!("[gw] upstream tiers: {}", priority.describe());

    // pool refresher: re-read the node's peer file (peerd keeps it fresh) + the network tip
    {
        let pool = pool.clone();
        let path = node_peer_file.clone();
        let net_round_tip = net_round_tip.clone();
        let priority = priority.clone();
        tokio::spawn(async move {
            let mut last_fresh_tip = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                crate::blacklist::reload("gw", &blacklist_dir);
                let p = read_pool(&priority, &path);
                if p.is_empty() {
                    // missing/unreadable/empty file (peerd itself never writes an empty pool over
                    // a good one): keep serving from the last pool rather than dropping every
                    // upstream — minus anything blacklisted since
                    let mut pool = lock(&pool);
                    pool.retain(|ip| !crate::blacklist::contains(ip));
                    if !pool.is_empty() {
                        gwlog!(
                            "[gw] WARN peer file {path} has no peers; keeping the previous pool"
                        );
                    }
                } else {
                    *lock(&pool) = p;
                }
                let fresh = fresh_peer_file_tip(&path);
                if fresh.is_some() {
                    last_fresh_tip = Instant::now();
                }
                if let Some(tip) = next_net_tip(fresh, last_fresh_tip.elapsed()) {
                    let old = net_round_tip.swap(tip, Ordering::AcqRel);
                    if tip == 0 && old != 0 {
                        gwlog!(
                            "[gw] WARN no fresh round_tip from peerd for {}s; round tip unknown (floor-only round checks)",
                            last_fresh_tip.elapsed().as_secs()
                        );
                    }
                }
            }
        });
    }

    if cache_coldstart {
        // bootstrap capture: fetch + cache the full bootstrap (abci_state + EVM KVs) verbatim from a
        // serving state-server, so a node can cold-start from cache (no per-IP state rate-limit). Refresh.
        {
            let boot_blob = boot_blob.clone();
            let boot_blob_cached_at = boot_blob_cached_at.clone();
            let pool = pool.clone();
            let disk_cache_path = disk_cache_path.clone();
            let capture_peer_file = node_peer_file.clone();
            let priority = priority.clone();
            tokio::spawn(async move {
                // abci_state is rate-limited per (source-IP, peer) pair, so re-hitting the same
                // top-ranked peers exhausts them. Until we have a cache, start from peerd's best
                // ranked peers so cold cache can fill quickly. Once cached, refresh before the
                // freshness window expires and seed the refresh rotation from the clock so a fresh
                // gateway doesn't always begin at the same rank.
                let mut cycle = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as usize)
                    .unwrap_or(0);
                let mut fails = 0u32;
                loop {
                    // Until we have ANY cached blob the node can't cold-start, so the initial
                    // capture is urgent: search a wide window and retry fast. Once cached, the old
                    // blob stays valid, so refreshes use a small rotated window before the replay
                    // freshness window expires (gentle on peers, lets per-peer abci_state limits
                    // recover).
                    let have_cache = lock(&boot_blob).is_some();
                    let now = unix_secs();
                    let cached_at = boot_blob_cached_at.load(Ordering::Acquire);
                    let have_fresh_cache = have_cache && is_bootstrap_cache_fresh(now, cached_at);
                    if have_cache && !have_fresh_cache {
                        // past the freshness window it can never be replayed again: free the
                        // ~6GB now instead of holding it alongside the refresh captures
                        *lock(&boot_blob) = None;
                    }
                    if have_fresh_cache {
                        let next_refresh = cached_at.saturating_add(CACHE_REFRESH_SECS);
                        if now < next_refresh {
                            tokio::time::sleep(Duration::from_secs(
                                next_refresh
                                    .saturating_sub(now)
                                    .min(CACHE_REFRESH_RETRY_SECS),
                            ))
                            .await;
                            continue;
                        }
                        let live_count = lock(&pool).len();
                        if should_delay_cache_refresh_for_live_pool(now, cached_at, live_count) {
                            let cache_age = now.saturating_sub(cached_at);
                            let seconds_left =
                                MAX_BOOTSTRAP_CACHE_AGE_SECS.saturating_sub(cache_age);
                            let delay = fresh_cache_retry_delay_secs(now, cached_at);
                            gwlog!(
                                "[gw] bootstrap cache refresh skipped: only {live_count} validated live peer(s); cache expires in {seconds_left}s; retry in {delay}s"
                            );
                            tokio::time::sleep(Duration::from_secs(delay)).await;
                            continue;
                        } else if live_count < MIN_LIVE_PEERS_FOR_CACHE_REFRESH {
                            let cache_age = now.saturating_sub(cached_at);
                            let seconds_left =
                                MAX_BOOTSTRAP_CACHE_AGE_SECS.saturating_sub(cache_age);
                            gwlog!(
                                "[gw] bootstrap cache refresh proceeding with only {live_count} validated live peer(s); cache expires in {seconds_left}s"
                            );
                        }
                    }
                    let window = if have_fresh_cache { 24 } else { 64 };
                    let peers: Vec<String> = {
                        let live = lock(&pool).clone();
                        // the cache refresh never uses priority peers, whatever the policy
                        let full = priority.strip(bootstrap_capture_peers(
                            &capture_peer_file,
                            live,
                            !have_fresh_cache,
                        ));
                        let n = full.len();
                        if n == 0 {
                            Vec::new()
                        } else {
                            let take = window.min(n);
                            let rotate_window = have_fresh_cache || fails > 0;
                            let off = if rotate_window {
                                cycle.wrapping_mul(take) % n
                            } else {
                                0
                            };
                            (0..take).map(|k| full[(off + k) % n].clone()).collect()
                        }
                    };
                    cycle = cycle.wrapping_add(1);
                    // Refreshes still need a small hedge: a single slow-but-readable state server
                    // can push the cache past the freshness window. Limit fresh-cache refreshes to
                    // two concurrent captures so one slow peer does not block the next candidate.
                    let max_inflight = if have_fresh_cache {
                        FRESH_CACHE_REFRESH_MAX_INFLIGHT
                    } else {
                        COLD_CACHE_REFRESH_MAX_INFLIGHT
                    };
                    let capture_deadline_secs = bootstrap_capture_deadline_secs(have_fresh_cache);
                    let raced = match timeout(
                        Duration::from_secs(capture_deadline_secs),
                        capture_bootstrap_raced(peers, max_inflight, Duration::from_secs(40)),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => {
                            gwlog!(
                                "[gw] bootstrap race: exceeded {capture_deadline_secs}s total deadline, aborting"
                            );
                            None
                        }
                    };
                    let raced = raced.filter(|(blob, ip)| {
                        let ok = has_live_tail(blob);
                        if !ok {
                            gwlog!(
                                "[gw] bootstrap capture via {ip} rejected: {} MB but no live-block tail (cut off mid-bulk)",
                                blob.len() / 1_000_000
                            );
                        }
                        ok
                    });
                    let got = if let Some((blob, ip)) = raced {
                        gwlog!(
                            "[gw] bootstrap captured: {} MB via {} (raced)",
                            blob.len() / 1_000_000,
                            ip
                        );
                        let blob = Arc::new(blob);
                        *lock(&boot_blob) = Some(blob.clone());
                        boot_blob_cached_at.store(unix_secs(), Ordering::Release);
                        if let Some(path) = disk_cache_path.clone() {
                            tokio::task::spawn_blocking(move || {
                                match persist_bootstrap_cache(&path, blob.as_slice()) {
                                    Ok(()) => {
                                        gwlog!(
                                            "[gw] bootstrap cache persisted to {}",
                                            path.display()
                                        );
                                    }
                                    Err(e) => {
                                        gwlog!(
                                            "[gw] bootstrap cache persist failed at {}: {e}",
                                            path.display()
                                        );
                                    }
                                }
                            });
                        }
                        true
                    } else {
                        false
                    };
                    let delay = if got {
                        fails = 0;
                        CACHE_REFRESH_SECS
                    } else if have_fresh_cache {
                        fresh_cache_retry_delay_secs(unix_secs(), cached_at)
                    } else {
                        // A failed initial capture may already have consumed quota/bandwidth across
                        // a full rotated window. Back off in minutes, not seconds, so --cache does
                        // not keep burning the same source-IP quotas and hundreds of GB.
                        fails = (fails + 1).min(4);
                        match fails {
                            1 => 60,
                            2 => 120,
                            3 => 300,
                            _ => 600,
                        }
                    };
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                }
            });
        }
    }

    let ctx = GatewayCtx {
        rr,
        boot_blob,
        boot_blob_cached_at,
        disk_cache_path,
        capture_tap,
        net_round_tip: net_round_tip.clone(),
        state_servers: Arc::new(Mutex::new(Vec::new())),
        priority: priority.clone(),
        pool: pool.clone(),
        cooldown: Arc::new(PeerCooldown::default()),
        freshest: Arc::new(FreshestRound::new(
            PEER_LAG_ROUNDS,
            PEER_LAG_GRACE,
            FRESHEST_STALE,
            net_round_tip.clone(),
        )),
        cache_coldstart,
        push,
    };
    // pace reference streams, so a lagging upstream is caught even with a single node session
    // (HYPERSYNC_REFERENCE_STREAM=0 turns them off)
    if push && std::env::var("HYPERSYNC_REFERENCE_STREAM").map_or(true, |v| v.trim() != "0") {
        let in_use = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let cooled = Arc::new(Mutex::new(std::collections::HashMap::new()));
        for start in 0..PACE_REFERENCES {
            tokio::spawn(run_pace_reference(
                ctx.pool.clone(),
                ctx.freshest.clone(),
                ctx.net_round_tip.clone(),
                in_use.clone(),
                cooled.clone(),
                start,
            ));
        }
    }

    // listeners on 4000-4010. The node connects ONLY here; the gateway transparently relays each
    // connection to a real upstream peer. For the heavy channel (4001) bootstrap (send_abci:true)
    // it picks a peer that is ACTUALLY serving the abci_state right now (peeks the first frame:
    // a serving peer sends the >4MB state, a rate-limited one sends a tiny status frame -> try next),
    // so the node streams the complete abci_state + EVM KVs + live blocks straight from a fresh peer.
    // Round-robin start means node/gateway restarts rotate to a different fresh state-server (no
    // per-peer abci_state rate-limit). The whole pool behind the gateway is the failover set.
    let mut handles = Vec::new();
    for port in 4000u16..=4010 {
        let pool = pool.clone();
        let nodes = nodes.clone();
        let ctx = ctx.clone();
        handles.push(tokio::spawn(async move {
            let l = match TcpListener::bind(("0.0.0.0", port)).await {
                Ok(l) => l,
                Err(e) => {
                    gwlog!("[gw] bind :{port}: {e}");
                    return;
                }
            };
            loop {
                let (down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let pool = pool.clone();
                let nodes = nodes.clone();
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    let mut down = down;
                    down.set_nodelay(true).ok();
                    let (node, _conn_guard) = nodes.get_or_insert(addr.ip(), unix_secs());
                    let peers = ctx.cooldown.usable(lock(&pool).clone(), Instant::now());
                    if peers.is_empty() && ctx.priority.is_empty() {
                        return;
                    }
                    if port == 4001 {
                        let mut greet = [0u8; 8];
                        if !matches!(
                            timeout(Duration::from_secs(20), down.read_exact(&mut greet)).await,
                            Ok(Ok(_))
                        ) {
                            return;
                        }
                        gwlog!(
                            "[gw] [{}] 4001 node greeting {}",
                            node.ip,
                            hex_bytes(&greet)
                        );
                        if greet[5] == 1 {
                            handle_4001_bootstrap(down, node, peers, greet, ctx).await;
                        } else {
                            handle_4001_live(down, node, peers, greet, ctx).await;
                        }
                    } else if should_fetch_forward_client_blocks(port) {
                        // 4002 client-block RPC is request/response. Always fetch-forward it through
                        // the current active peer so we can detect "Peer-only request" and wait for a
                        // fresh active instead of leaking the rejection to the node.
                        serve_client_blocks(
                            down,
                            node.clone(),
                            ctx.priority.clone(),
                            peers,
                            ctx.rr.clone(),
                        )
                        .await;
                    } else {
                        // Other gossip channels stay transparently spliced to the node's active peer.
                        let Some(upc) =
                            dial_active(&node, &ctx.priority, &peers, &ctx.rr, port).await
                        else {
                            return;
                        };
                        splice(down, upc).await;
                    }
                });
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

pub(crate) use self::client_blocks::{
    response_has_client_block_round_too_large, response_has_client_block_round_too_small,
    response_has_no_client_blocks, response_is_peer_only,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn net_tip_follows_fresh_readings_and_resets_when_they_stop() {
        let fresh_a_while = Duration::from_secs(60);
        let stale = PEER_FILE_TIP_MAX_AGE + Duration::from_secs(1);
        // a fresh reading replaces the tip, including a LOWER one (a bad cycle lasts one cycle)
        assert_eq!(
            next_net_tip(Some(1_490_000_000), fresh_a_while),
            Some(1_490_000_000)
        );
        assert_eq!(
            next_net_tip(Some(1_400_000_000), stale),
            Some(1_400_000_000)
        );
        // no reading for a short while: keep the current tip
        assert_eq!(next_net_tip(None, fresh_a_while), None);
        // no reading for longer than the file-age limit: unknown, never frozen
        assert_eq!(next_net_tip(None, stale), Some(0));
    }

    #[test]
    fn peer_file_tip_is_ignored_once_stale() {
        let dir = std::env::temp_dir().join(format!("hs-tip-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("peers.json");
        std::fs::write(
            &path,
            r#"{"live_servers":["1.1.1.1"],"round_tip":1483973942,"candidate_fallback":false}"#,
        )
        .unwrap();
        let p = path.to_string_lossy().to_string();
        assert_eq!(fresh_peer_file_tip(&p), Some(1_483_973_942));
        let old = std::time::SystemTime::now() - PEER_FILE_TIP_MAX_AGE - Duration::from_secs(60);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(old)
            .unwrap();
        assert_eq!(fresh_peer_file_tip(&p), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
