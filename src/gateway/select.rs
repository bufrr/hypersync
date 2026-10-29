use crate::priority::{gwlog, BootstrapPolicy, PrioritySession, PriorityTier};
use crate::protocol::{
    block_round, is_plausible_mainnet_round, read_live_frame_with_timeout, response_is_peer_full,
    round_lag_from_tip, ACTIVE_ROUND_MAX_LAG, GREET_FALSE, MAX_LIVE_FRAME,
};
use crate::push::{
    FreshestRound, MAX_ACTIVE_ROUND_JUMP, NODE_CATCHUP_STUCK, RETURNING_TO_PRIORITY,
};
use crate::util::lock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

const BOOTSTRAP_PREFETCH_BYTES: usize = 64_000_000;

// Largest plausible single bootstrap frame (the abci_state is ~1.3GB in 2026-10).
pub(super) const MAX_ABCI_STATE_FRAME: usize = 1_500_000_000;

const BOOTSTRAP_PREFETCH_SECS: u64 = 4;

const BOOTSTRAP_SELECT_DEADLINE_SECS: u64 = 24;

const BOOTSTRAP_SELECT_MAX_INFLIGHT: usize = 4;

pub(super) const BOOTSTRAP_SELECT_MAX_PEERS: usize = 32;

// An exclusive tier's priority peers race alone this long; after it the pool may take free slots
// even while a priority attempt is still pending. A stuck priority attempt can last ~26s (connect,
// greeting and prefetch timeouts), longer than the whole selection window, so without a bound one
// hung QuickNode peer would keep the pool out of every bootstrap retry.
#[cfg(not(test))]
const BOOTSTRAP_PRIORITY_HEAD_START: Duration = Duration::from_secs(8);
#[cfg(test)]
const BOOTSTRAP_PRIORITY_HEAD_START: Duration = Duration::from_millis(300);

pub(super) struct BootstrapPeerSelection {
    pub(super) upc: TcpStream,
    pub(super) hdr: [u8; 5],
    pub(super) payload_prefix: Vec<u8>,
    pub(super) ip: String,
    pub(super) abci_len: usize,
    // a multi-GB relay holds a connection on the peer: its priority slot (None for pool peers)
    pub(super) prio_session: Option<PrioritySession>,
}

// Stage-labels a timeout+io result so selection failures can be summarized by cause instead of
// being swallowed ("no peer serving abci_state" alone hides whether peers declined, timed out,
// or closed early).
fn stage<T>(
    r: Result<std::io::Result<T>, tokio::time::error::Elapsed>,
    what: &str,
) -> Result<T, String> {
    match r {
        Err(_) => Err(format!("{what} timeout")),
        Ok(Err(e)) => Err(format!("{what}: {e}")),
        Ok(Ok(v)) => Ok(v),
    }
}

// Bucket a peer-attempt failure string for the selection summary logs.
fn peer_error_class(e: &str) -> &'static str {
    if e.contains("peer full") {
        "peer_full"
    } else if e.contains("lagged") || e.contains("behind the freshest") {
        "lagged"
    } else if e.contains("not serving abci_state") {
        "small_frame"
    } else if e.contains("no live block") {
        "no_block"
    } else if e.contains("connect") {
        "connect_fail"
    } else if e.contains("state prefetch") {
        "prefetch_fail"
    } else if e.contains("failed to fill whole buffer") || e.contains("early eof") {
        "eof"
    } else if e.contains("timeout") || e.contains("deadline has elapsed") {
        "timeout"
    } else {
        "other"
    }
}

fn summarize_peer_errors(tried: usize, errs: &[String]) -> String {
    let mut counts: Vec<(&'static str, usize)> = Vec::new();
    for e in errs {
        let class = peer_error_class(e);
        match counts.iter_mut().find(|(c, _)| *c == class) {
            Some((_, n)) => *n += 1,
            None => counts.push((class, 1)),
        }
    }
    counts.sort_by_key(|c| std::cmp::Reverse(c.1));
    let mut out = format!("tried={tried}");
    for (class, n) in counts {
        out.push_str(&format!(" {class}={n}"));
    }
    out
}

pub(super) fn hex_bytes(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// A peer declining a send_abci greeting answers with a small status frame. Read it (bounded) so
// the log says why: the status code, or text such as "abci state request rate limited".
pub(super) async fn decline_detail(s: &mut TcpStream, typ: u8, len: usize) -> String {
    if len == 0 || len > 256 {
        return format!("type={typ} len={len}");
    }
    let mut p = vec![0u8; len];
    match timeout(Duration::from_secs(2), s.read_exact(&mut p)).await {
        Ok(Ok(_)) => format!(
            "type={typ} len={len} code={:02x} text={:?}",
            p[0],
            String::from_utf8_lossy(&p[..len.min(80)])
        ),
        _ => format!("type={typ} len={len} payload=unread"),
    }
}

async fn try_fast_bootstrap_peer(
    ip: String,
    greet: [u8; 8],
) -> Result<BootstrapPeerSelection, String> {
    let mut upc = stage(
        timeout(
            Duration::from_secs(5),
            crate::blacklist::connect(&addr_4001(&ip)),
        )
        .await,
        "connect",
    )?;
    upc.set_nodelay(true).ok();
    stage(
        timeout(Duration::from_secs(5), upc.write_all(&greet)).await,
        "greet write",
    )?;

    let mut hdr = [0u8; 5];
    stage(
        timeout(Duration::from_secs(12), upc.read_exact(&mut hdr)).await,
        "greeting header",
    )?;
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    if len <= 4_000_000 {
        return Err(format!(
            "not serving abci_state ({})",
            decline_detail(&mut upc, hdr[4], len).await
        ));
    }
    // same bound as fetch_bootstrap / the tap (abci_state is ~1.3GB): a bigger first frame is a
    // broken or hostile peer, and relaying its length would hang the node's bootstrap on it
    if len > MAX_ABCI_STATE_FRAME {
        return Err(format!("implausible abci_state len={len}"));
    }
    let prefetch_len = len.min(BOOTSTRAP_PREFETCH_BYTES);
    let mut payload_prefix = vec![0u8; prefetch_len];
    stage(
        timeout(
            Duration::from_secs(BOOTSTRAP_PREFETCH_SECS),
            upc.read_exact(&mut payload_prefix),
        )
        .await,
        "state prefetch",
    )?;

    Ok(BootstrapPeerSelection {
        upc,
        hdr,
        payload_prefix,
        ip,
        abci_len: len,
        prio_session: None,
    })
}

pub(super) async fn select_fast_bootstrap_peer(
    priority: &Arc<PriorityTier>,
    peers: &[String],
    start: usize,
    greet: [u8; 8],
) -> Option<BootstrapPeerSelection> {
    let n = peers.len();
    if n == 0 {
        return None;
    }
    let limit = n.min(BOOTSTRAP_SELECT_MAX_PEERS);
    let started = Instant::now();
    let deadline = started + Duration::from_secs(BOOTSTRAP_SELECT_DEADLINE_SECS);
    let mut launched = 0usize;
    let mut in_flight = 0usize;
    let mut prio_in_flight = 0usize;
    let mut set = tokio::task::JoinSet::new();
    let mut fails: Vec<String> = Vec::new();
    let pool_after = started + BOOTSTRAP_PRIORITY_HEAD_START;

    loop {
        let mut pool_held = false;
        while launched < limit && in_flight < BOOTSTRAP_SELECT_MAX_INFLIGHT {
            let ip = peers[(start + launched) % n].clone();
            let is_priority = priority.contains(&ip);
            // an exclusive tier's pool candidates wait until every priority attempt has failed,
            // or the priority head start is over
            if !is_priority
                && prio_in_flight > 0
                && priority.exclusive()
                && Instant::now() < pool_after
            {
                pool_held = true;
                break;
            }
            // a priority peer is only dialed with a free slot (HYPERSYNC_PRIORITY_MAX_SESSIONS)
            let slot = if is_priority {
                match priority.try_acquire(&ip) {
                    Some(s) => Some(s),
                    None => {
                        launched += 1;
                        fails.push(PRIORITY_CAP_REACHED.to_string());
                        continue;
                    }
                }
            } else {
                None
            };
            set.spawn(async move {
                let r = try_fast_bootstrap_peer(ip, greet).await.map(|mut s| {
                    s.prio_session = slot;
                    s
                });
                (is_priority, r)
            });
            launched += 1;
            in_flight += 1;
            prio_in_flight += usize::from(is_priority);
        }
        if in_flight == 0 {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        // a held-back pool wakes the race when the head start ends
        let wait = if pool_held {
            remaining.min(pool_after.saturating_duration_since(Instant::now()))
        } else {
            remaining
        };
        match timeout(wait, set.join_next()).await {
            Ok(Some(joined)) => {
                in_flight = in_flight.saturating_sub(1);
                match joined {
                    Ok((_, Ok(selected))) => {
                        set.abort_all();
                        return Some(selected);
                    }
                    Ok((is_priority, Err(e))) => {
                        prio_in_flight -= usize::from(is_priority);
                        fails.push(e);
                    }
                    Err(e) => {
                        // a failed task can't say which tier it was; never let it hold the pool back
                        prio_in_flight = prio_in_flight.min(in_flight);
                        fails.push(format!("join: {e}"));
                    }
                }
            }
            Ok(None) => break,
            // the head start ended: launch the pool next to the pending priority attempts
            Err(_) if pool_held && Instant::now() < deadline => {}
            Err(_) => break,
        }
    }
    set.abort_all();
    gwlog!(
        "[gw] bootstrap selection: no state-serving peer in {:.1}s ({})",
        started.elapsed().as_secs_f64(),
        summarize_peer_errors(launched, &fails)
    );
    None
}

async fn read_live_greeting_with_timeout(
    s: &mut TcpStream,
    wait: Duration,
) -> std::io::Result<Vec<u8>> {
    read_live_frame_with_timeout(s, wait, 1000).await
}

pub(super) struct LivePeerSession {
    pub(super) ip: String,
    pub(super) stream: TcpStream,
    pub(super) greeting: Vec<u8>,
    pub(super) prefetched_frames: Vec<Vec<u8>>,
    pub(super) first_round: u32,
    // the priority slot taken before dialing (None for pool peers); serve_push_repinned keeps it
    pub(super) prio_session: Option<PrioritySession>,
}

// Non-block frames read while vetting (control / no-round data) are kept, in order, and handed to
// the push session ahead of the vetted block instead of being dropped; bounded so a peer streaming
// only non-block data can't grow the buffer.
const VET_KEEP_MAX_FRAMES: usize = 64;

const VET_KEEP_MAX_BYTES: usize = 8_000_000;

pub(super) const LIVE_SELECT_WAIT: Duration = Duration::from_millis(3500);

const LIVE_SELECT_MAX_PARALLEL: usize = 4;

// A candidate's first live block becomes the session's round baseline, so one far above the
// network tip would be forwarded and then make every honest block look like a stall. The tip lags
// by at most ~15 min (~13k rounds at ~15 rounds/s) before it resets to unknown, well inside this
// margin, so a stale tip can't reject honest peers.
const VET_MAX_ROUNDS_ABOVE_TIP: u32 = 2 * MAX_ACTIVE_ROUND_JUMP;

fn first_round_far_above_tip(round: u32, net_tip: u32) -> bool {
    net_tip != 0 && round > net_tip.saturating_add(VET_MAX_ROUNDS_ABOVE_TIP)
}

async fn open_live_peer_session_with_greet(
    ip: String,
    greet: [u8; 8],
    wait: Duration,
    min_round_exclusive: u32,
    net_tip: u32,
    // the gateway's lag guard; its floor is read when the first block arrives
    freshest: Option<Arc<FreshestRound>>,
) -> std::io::Result<LivePeerSession> {
    let connect_ip = ip.clone();
    let (stream, greeting, prefetched_frames, first_round) = timeout(wait, async move {
        let (mut s, greeting) = connect_and_greet_4001(&connect_ip, greet, wait).await?;
        let deadline = Instant::now() + wait;
        let mut kept: Vec<Vec<u8>> = Vec::new();
        let mut kept_bytes = 0usize;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(std::io::Error::other("no live block before deadline"));
            }
            let frame = read_live_frame_with_timeout(&mut s, remaining, MAX_LIVE_FRAME).await?;
            let round = if frame.len() >= 5 && frame[4] == 1 {
                block_round(&frame[5..])
            } else {
                None
            };
            let Some(round) = round else {
                if kept.len() < VET_KEEP_MAX_FRAMES && kept_bytes + frame.len() <= VET_KEEP_MAX_BYTES
                {
                    kept_bytes += frame.len();
                    kept.push(frame);
                }
                continue;
            };
            {
                if !is_plausible_mainnet_round(round, net_tip)
                    || round <= min_round_exclusive
                    || first_round_far_above_tip(round, net_tip)
                {
                    continue;
                }
                // A fresh 4001 stream starts at the peer's own tip, so a lagged first block means a
                // lagged peer, and it cannot make up ACTIVE_ROUND_MAX_LAG rounds inside the select
                // window. Fail immediately: select_live_peer shares one deadline across
                // LIVE_SELECT_MAX_PARALLEL slots, and looping here would pin a slot until the
                // deadline and starve the rest of the pool.
                if let Some(lag) = round_lag_from_tip(round, net_tip, ACTIVE_ROUND_MAX_LAG) {
                    return Err(std::io::Error::other(format!(
                        "live block lagged round={round} net_tip={net_tip} lag={lag} max_lag={ACTIVE_ROUND_MAX_LAG}"
                    )));
                }
                // behind the gateway's freshest stream by more than the lag guard allows
                // (FreshestRound::select_floor): it would be cut seconds into the session
                let floor = freshest.as_ref().map_or(0, |f| f.select_floor());
                if round < floor {
                    return Err(std::io::Error::other(format!(
                        "live block behind the freshest stream round={round} floor={floor}"
                    )));
                }
                kept.push(frame);
                return Ok::<_, std::io::Error>((s, greeting, kept, round));
            }
        }
    })
    .await??;
    Ok(LivePeerSession {
        ip,
        stream,
        greeting,
        prefetched_frames,
        first_round,
        prio_session: None,
    })
}

// Connect to a peer's 4001, send `greet`, and read+vet its greeting (peer-full rejected).
// The single shared prefix for every live-relationship opener.
// "ip" normally; "ip:port" (custom ports, tests on ephemeral ports) is used as given
fn addr_4001(ip: &str) -> String {
    if ip.contains(':') {
        ip.to_string()
    } else {
        format!("{ip}:4001")
    }
}

pub(super) async fn connect_and_greet_4001(
    ip: &str,
    greet: [u8; 8],
    wait: Duration,
) -> std::io::Result<(TcpStream, Vec<u8>)> {
    let mut s = timeout(wait, crate::blacklist::connect(&addr_4001(ip))).await??;
    s.set_nodelay(true).ok();
    timeout(wait, s.write_all(&greet)).await??;
    let greeting = read_live_greeting_with_timeout(&mut s, wait).await?;
    if response_is_peer_full(&greeting) {
        return Err(std::io::Error::other("peer full"));
    }
    Ok((s, greeting))
}

pub(super) async fn open_peer_relationship(ip: &str, wait: Duration) -> std::io::Result<TcpStream> {
    Ok(connect_and_greet_4001(ip, GREET_FALSE, wait).await?.0)
}

const PRIORITY_CAP_REACHED: &str = "priority session cap reached";

// A priority attempt takes its session slot BEFORE dialing (try_acquire), so concurrent
// selections for several nodes can't overrun HYPERSYNC_PRIORITY_MAX_SESSIONS; a full peer counts
// as a failed attempt. The slot travels with the session, or is released when the attempt fails
// or loses the race.
#[allow(clippy::too_many_arguments)]
async fn live_attempt(
    priority: Arc<PriorityTier>,
    ip: String,
    is_priority: bool,
    greet: [u8; 8],
    wait: Duration,
    min_round_exclusive: u32,
    net_tip: u32,
    freshest: Option<Arc<FreshestRound>>,
) -> (String, bool, std::io::Result<LivePeerSession>) {
    let slot = if is_priority {
        match priority.try_acquire(&ip) {
            Some(s) => Some(s),
            None => {
                return (
                    ip,
                    is_priority,
                    Err(std::io::Error::other(PRIORITY_CAP_REACHED)),
                )
            }
        }
    } else {
        None
    };
    let r = open_live_peer_session_with_greet(
        ip.clone(),
        greet,
        wait,
        min_round_exclusive,
        net_tip,
        freshest.clone(),
    )
    .await
    .map(|mut s| {
        s.prio_session = slot;
        s
    });
    (ip, is_priority, r)
}

// Bounded by ONE LIVE_SELECT_WAIT total: hl-node abandons a fresh 4001 connection ~5s after
// sending its greeting, so the node is only saved by a greeting that arrives inside that window.
// Scanning the whole pool in batches (up to ~70-90s on a bad pool) just burns the node's deadline
// against an already-dead downstream — the first vetted peer wins, and on a dry scan the caller's
// fallback still has time to greet the node.
//
// Two stages share that window. Stage A races every eligible priority peer at once. Stage B (the
// pool, LIVE_SELECT_MAX_PARALLEL slots) opens when the priority stage budget runs out or every
// priority attempt has failed; priority attempts still in flight keep racing alongside it. An
// exclusive tier has no stage budget: the pool opens only once every priority peer has failed (or
// none was usable to begin with), so a slow priority peer is never outraced by the pool. With no
// priority peers this is the plain pool race.
pub(super) async fn select_live_peer(
    priority: &Arc<PriorityTier>,
    peers: &[String],
    start: usize,
    greet: [u8; 8],
    min_round_exclusive: u32,
    net_tip: u32,
    freshest: Option<Arc<FreshestRound>>,
) -> Option<LivePeerSession> {
    let prio = priority.candidates();
    if peers.is_empty() && prio.is_empty() {
        return None;
    }
    let started = Instant::now();
    let deadline = started + LIVE_SELECT_WAIT;
    let pool_opens = if prio.is_empty() {
        started
    } else if priority.exclusive() {
        deadline
    } else {
        started + priority.stage
    };
    let mut set = tokio::task::JoinSet::new();
    let mut prio_in_flight = 0usize;
    let mut pool_in_flight = 0usize;
    let mut launched = 0usize;
    let mut fails: Vec<String> = Vec::new();
    let mut prio_pending: Vec<String> = prio.clone();
    for ip in prio.iter().cloned() {
        set.spawn(live_attempt(
            priority.clone(),
            ip,
            true,
            greet,
            LIVE_SELECT_WAIT,
            min_round_exclusive,
            net_tip,
            freshest.clone(),
        ));
        prio_in_flight += 1;
    }
    loop {
        let now = Instant::now();
        let pool_open = now >= pool_opens || prio_in_flight == 0;
        if pool_open {
            while launched < peers.len() && pool_in_flight < LIVE_SELECT_MAX_PARALLEL {
                let wait = deadline.saturating_duration_since(Instant::now());
                if wait.is_zero() {
                    break;
                }
                let ip = peers[(start + launched) % peers.len()].clone();
                set.spawn(live_attempt(
                    priority.clone(),
                    ip,
                    false,
                    greet,
                    wait,
                    min_round_exclusive,
                    net_tip,
                    freshest.clone(),
                ));
                launched += 1;
                pool_in_flight += 1;
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if set.is_empty() || remaining.is_zero() {
            break;
        }
        // Also wake at the stage boundary, so the pool opens on time while priority attempts hang.
        let wake = if pool_open {
            remaining
        } else {
            pool_opens.saturating_duration_since(now).min(remaining)
        };
        match timeout(wake, set.join_next()).await {
            Ok(Some(Ok((_, _, Ok(session))))) => {
                set.abort_all();
                return Some(session);
            }
            Ok(Some(Ok((ip, is_priority, Err(e))))) => {
                if is_priority {
                    prio_in_flight = prio_in_flight.saturating_sub(1);
                    prio_pending.retain(|p| p != &ip);
                    // a full peer is busy with our other nodes, not broken: no cooldown; one that
                    // only timed out may be waiting on a local stall (PriorityTier::mark_hung)
                    if e.kind() == std::io::ErrorKind::TimedOut {
                        priority.mark_hung(&ip);
                    } else if e.to_string() != PRIORITY_CAP_REACHED {
                        priority.mark_failed(&ip);
                    }
                    gwlog!(
                        "[gw] priority {ip} live attempt failed after {}ms (our sessions there={}): {e}",
                        started.elapsed().as_millis(),
                        priority.sessions(&ip)
                    );
                } else {
                    pool_in_flight = pool_in_flight.saturating_sub(1);
                }
                fails.push(e.to_string());
            }
            Ok(Some(Err(e))) => fails.push(format!("join: {e}")),
            Ok(None) => break,
            Err(_) if pool_open => break,
            Err(_) => {
                if Instant::now() >= pool_opens && pool_opens < deadline {
                    gwlog!(
                        "[gw] priority stage expired after {}ms with {prio_in_flight} attempt(s) pending; opening the pool",
                        started.elapsed().as_millis()
                    );
                }
            }
        }
    }
    set.abort_all();
    // A priority peer that neither errored nor delivered a vetted block in the whole window is as
    // good as failed. Cool it down too, or the caller's fallback (which has only what is left of
    // the node's ~5s) re-dials it first and spends the node's deadline on it (2026-10-16: three
    // hung priority peers x 1.5s each ran the node's live connect out after both low-memory
    // restarts).
    for ip in &prio_pending {
        priority.mark_hung(ip);
    }
    gwlog!(
        "[gw] live selection: no vetted peer in {:.1}s (prio_tried={} prio_hung={} {})",
        started.elapsed().as_secs_f64(),
        prio.len(),
        prio_pending.len(),
        summarize_peer_errors(launched + prio.len(), &fails)
    );
    None
}

// Priority candidates first (least-loaded), then the pool rotated from `start`, deduped. An
// exclusive tier with a usable priority peer offers only the priority peers.
pub(super) fn tiered_peers(priority: &PriorityTier, pool: &[String], start: usize) -> Vec<String> {
    let prio = priority.candidates();
    if priority.exclusive() && !prio.is_empty() {
        return prio;
    }
    ordered_candidates(prio, pool, start)
}

// hl-node abandons a fresh 4001 connection this long after sending its greeting ("connecting to
// peer" -> "abci_stream ... timed out" is 5.0s in the node log).
const NODE_LIVE_CONNECT_DEADLINE: Duration = Duration::from_secs(5);

// What a failed LIVE_SELECT_WAIT leaves the 4001 fallback, less a margin for relaying the greeting.
pub(super) const FALLBACK_WAIT: Duration = NODE_LIVE_CONNECT_DEADLINE
    .saturating_sub(LIVE_SELECT_WAIT)
    .saturating_sub(Duration::from_millis(200));

pub(super) const FALLBACK_MAX_PARALLEL: usize = 8;

// The winner of a fallback race: (ip, connection, its greeting, its priority slot).
type FallbackUpstream = (String, TcpStream, Vec<u8>, Option<PrioritySession>);

// Race connect+greet against the first FALLBACK_MAX_PARALLEL candidates (priority first) and keep
// the first that answers. Sequential dials can't fit the node's remaining budget: one hung peer
// alone would take all of it. Priority peers that error are cooled down.
pub(super) async fn race_fallback_upstream(
    priority: &Arc<PriorityTier>,
    candidates: Vec<String>,
    greet: [u8; 8],
    wait: Duration,
) -> Option<FallbackUpstream> {
    let deadline = Instant::now() + wait;
    let mut set = tokio::task::JoinSet::new();
    for ip in candidates.into_iter().take(FALLBACK_MAX_PARALLEL) {
        // a priority peer is only dialed with a free slot, held by the winning connection
        let slot = if priority.contains(&ip) {
            match priority.try_acquire(&ip) {
                Some(s) => Some(s),
                None => continue,
            }
        } else {
            None
        };
        set.spawn(async move {
            let r = connect_and_greet_4001(&ip, greet, wait).await;
            (ip, r, slot)
        });
    }
    let mut won = None;
    while let Ok(Some(joined)) = timeout(
        deadline.saturating_duration_since(Instant::now()),
        set.join_next(),
    )
    .await
    {
        match joined {
            Ok((ip, Ok((upc, greeting)), slot)) => {
                won = Some((ip, upc, greeting, slot));
                break;
            }
            Ok((ip, Err(e), _)) if priority.contains(&ip) => {
                priority.mark_failed(&ip);
                gwlog!("[gw] 4001 fallback priority {ip} failed: {e}");
            }
            _ => {}
        }
    }
    set.abort_all();
    won
}

const STATE_SERVER_MEMORY: usize = 8;

pub(super) fn forget_state_server(list: &Mutex<Vec<String>>, ip: &str) {
    lock(list).retain(|x| x != ip);
}

pub(super) fn remember_state_server(list: &Mutex<Vec<String>>, ip: &str) {
    let mut l = lock(list);
    l.retain(|x| x != ip);
    l.insert(0, ip.to_string());
    l.truncate(STATE_SERVER_MEMORY);
}

// `head` first (known state servers, or priority peers), then the pool rotated from `start`,
// deduped. A remembered state server that is now rate-limited answers with a tiny status frame
// and fails the attempt fast, so heading the list costs little even when stale.
pub(super) fn ordered_candidates(head: Vec<String>, pool: &[String], start: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(head.len() + pool.len());
    for ip in head {
        // the pool is already filtered; a remembered state server may have been blacklisted since
        if !out.contains(&ip) && !crate::blacklist::contains(&ip) {
            out.push(ip);
        }
    }
    for k in 0..pool.len() {
        let ip = &pool[(start + k) % pool.len()];
        if !out.contains(ip) {
            out.push(ip.clone());
        }
    }
    out
}

// Where a node's abci_state may come from (HYPERSYNC_PRIORITY_BOOTSTRAP). abci_state is
// rate-limited per source IP, and every node behind the gateway shares one IP, so by default the
// priority peers serve it only after every pool candidate in the scan window has failed: they
// are placed last, inside the BOOTSTRAP_SELECT_MAX_PEERS window so they are still reached.
pub(super) fn bootstrap_source_list(
    policy: BootstrapPolicy,
    priority: Vec<String>,
    remembered: Vec<String>,
    pool: &[String],
    start: usize,
) -> Vec<String> {
    match policy {
        BootstrapPolicy::Never => ordered_candidates(remembered, pool, start),
        BootstrapPolicy::Prefer => {
            let mut head = priority;
            head.extend(remembered);
            ordered_candidates(head, pool, start)
        }
        BootstrapPolicy::Fallback => {
            let mut out = ordered_candidates(remembered, pool, start);
            out.retain(|ip| !priority.contains(ip));
            out.truncate(BOOTSTRAP_SELECT_MAX_PEERS.saturating_sub(priority.len()));
            out.extend(priority);
            out
        }
    }
}

// ---- returning to the priority tier ----
//
// A node session on a POOL peer never re-selects while it stays healthy, e.g. one the unvetted
// fallback picked while a network-wide pause had every priority peer cooling down. Without hot
// standbys it can't switch upstream mid-stream either (the new peer's stream starts at its own
// tip), so it ends itself once a priority peer is demonstrably serving again, and the node's
// reconnect re-selects (priority first). That reconnect costs the node a few seconds of blocks,
// hence the session-age floor, the catch-up wait and the per-node attempt limit.
pub(super) struct RepinTiming {
    // the session's age floor
    pub(super) min_session: Duration,
    // ...and the node must have stopped catching up: no client-block response forwarded to it for
    // this long (a catching-up node pulls a 4002 batch every few seconds and reconnects 4001 on its
    // own, so ending its session then is pointless churn)
    pub(super) catchup_quiet: Duration,
    // ...unless that catch-up has run this long without a quiet gap: it isn't converging (the
    // pool's 4002 horizon can sit a fixed distance behind the tip), so moving on is the fix
    pub(super) catchup_max: Duration,
    pub(super) check_every: Duration,
    // at most one probe attempt (every available priority peer in turn) per node this often,
    // whatever its outcome (a failed probe also cools that peer down like a failed selection)
    pub(super) retry: Duration,
}

pub(super) const REPIN: RepinTiming = RepinTiming {
    min_session: Duration::from_secs(60),
    catchup_quiet: CATCHUP_RUN_GAP,
    catchup_max: Duration::from_secs(3 * 60),
    check_every: Duration::from_secs(15),
    retry: Duration::from_secs(5 * 60),
};

// Resolves with the session's exit reason once the node should move back to a priority peer: a
// priority candidate (not cooling, below its session cap) passes the same first-block vetting as
// live selection. The probe takes the candidate's session slot and releases it with the probe
// connection when it returns (or when this future is dropped), before the node reconnects.
pub(super) async fn wait_priority_return(
    node: Arc<NodeState>,
    priority: Arc<PriorityTier>,
    net_tip: Arc<AtomicU32>,
    timing: RepinTiming,
    // the push session's lag guard: a candidate behind it would be cut right after the return
    freshest: Option<Arc<FreshestRound>>,
) -> String {
    tokio::time::sleep(timing.min_session).await;
    let watch_start = Instant::now();
    loop {
        let now = Instant::now();
        // the node's current catch-up run, counted from this session's watch at the earliest
        let caught_up = node
            .client_blocks_run_since(now, timing.catchup_quiet)
            .is_none_or(|since| {
                now.saturating_duration_since(since.max(watch_start)) >= timing.catchup_max
            });
        let candidates = if caught_up {
            priority.candidates()
        } else {
            Vec::new()
        };
        if !candidates.is_empty() && node.claim_repin_attempt(Instant::now(), timing.retry) {
            // every available priority peer in turn, not just the first: one still broken (e.g.
            // restarting after an upgrade) would otherwise take every probe while another serves
            let mut probed = false;
            for ip in candidates {
                let tip = net_tip.load(Ordering::Acquire);
                let (_, _, probe) = live_attempt(
                    priority.clone(),
                    ip.clone(),
                    true,
                    GREET_FALSE,
                    LIVE_SELECT_WAIT,
                    0,
                    tip,
                    freshest.clone(),
                )
                .await;
                match probe {
                    Ok(_serving) => return format!("{RETURNING_TO_PRIORITY} ({ip} is serving)"),
                    // another session took its last slot since candidates(): nothing wrong with
                    // the peer, and no probe happened
                    Err(e) if e.to_string() == PRIORITY_CAP_REACHED => {}
                    Err(e) => {
                        probed = true;
                        gwlog!(
                            "[gw] [{}] priority peer {ip} still not serving: {e}",
                            node.ip
                        );
                        if e.kind() == std::io::ErrorKind::TimedOut {
                            priority.mark_hung(&ip);
                        } else {
                            priority.mark_failed(&ip);
                        }
                    }
                }
            }
            // no probe ran (every candidate filled up meanwhile): this node may try again next check
            if !probed {
                node.forget_repin_attempt();
            }
        }
        tokio::time::sleep(timing.check_every).await;
    }
}

// Resolves (once per node per `retry`) when the node's catch-up has sat at one distance for
// `catchup_max` (batches keep coming, but every distance behind the freshest live round stayed
// within CATCHUP_PROGRESS_ROUNDS; NodeState::client_blocks_stalled), so the node reconnects. hl-node drops the live blocks that arrive while it fetches a
// missing range over 4002, so the next live block is missing its parent again: with ~1.7 s per
// fetch it re-fetches only what the network produced meanwhile and stays a fixed distance behind
// (~70 s seen) for as long as the session lasts. A fresh session hands it one large range instead,
// fetched 100 rounds at a time, which it clears in about a minute.
pub(super) async fn wait_catchup_stuck(node: Arc<NodeState>, timing: RepinTiming) -> String {
    tokio::time::sleep(timing.min_session).await;
    let watch_start = Instant::now();
    loop {
        let now = Instant::now();
        if let Some((true, behind)) =
            node.client_blocks_stalled(now, timing.catchup_quiet, timing.catchup_max)
        {
            // watched for a full window in this session, too
            if now.saturating_duration_since(watch_start) >= timing.catchup_max
                && node.claim_catchup_cut(now, timing.retry)
            {
                return format!(
                    "{NODE_CATCHUP_STUCK} (no progress for {}s, {} rounds behind)",
                    timing.catchup_max.as_secs(),
                    behind.map_or("?".to_string(), |b| b.to_string())
                );
            }
        }
        tokio::time::sleep(timing.check_every).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vetting_skips_first_blocks_far_above_a_known_tip() {
        let tip = 1_496_000_000;
        // unknown tip: nothing to compare against
        assert!(!first_round_far_above_tip(
            tip + 10 * VET_MAX_ROUNDS_ABOVE_TIP,
            0
        ));
        // a stale tip (honest peers ~15 min = ~13k rounds ahead of it) is still accepted
        assert!(!first_round_far_above_tip(tip + 15_000, tip));
        // deliberately not tighter: the tip can trail by ~15 min plus peerd's cycle age, and a
        // high first block costs one reconnect (the node validates blocks itself), whereas a
        // too-tight bound would reject every honest peer whenever peerd's tip lags
        assert!(!first_round_far_above_tip(tip + 30_000, tip));
        assert!(!first_round_far_above_tip(
            tip + VET_MAX_ROUNDS_ABOVE_TIP,
            tip
        ));
        assert!(first_round_far_above_tip(
            tip + VET_MAX_ROUNDS_ABOVE_TIP + 1,
            tip
        ));
    }

    #[test]
    fn bootstrap_source_policy_places_priority_peers() {
        let pool: Vec<String> = ["1.1.1.1", "2.2.2.2"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let prio = vec!["9.9.9.1".to_string(), "9.9.9.2".to_string()];
        let remembered = vec!["2.2.2.2".to_string()];
        let never = bootstrap_source_list(
            BootstrapPolicy::Never,
            prio.clone(),
            remembered.clone(),
            &pool,
            0,
        );
        assert_eq!(never, ["2.2.2.2", "1.1.1.1"]);
        let prefer = bootstrap_source_list(
            BootstrapPolicy::Prefer,
            prio.clone(),
            remembered.clone(),
            &pool,
            0,
        );
        assert_eq!(prefer, ["9.9.9.1", "9.9.9.2", "2.2.2.2", "1.1.1.1"]);
        let fallback = bootstrap_source_list(
            BootstrapPolicy::Fallback,
            prio.clone(),
            remembered,
            &pool,
            0,
        );
        assert_eq!(fallback, ["2.2.2.2", "1.1.1.1", "9.9.9.1", "9.9.9.2"]);
        // a big pool is cut so the priority peers still fit inside the selection window
        let big: Vec<String> = (0..100)
            .map(|i| format!("10.0.{}.{}", i / 250, i % 250 + 1))
            .collect();
        let fallback = bootstrap_source_list(BootstrapPolicy::Fallback, prio, Vec::new(), &big, 0);
        assert_eq!(fallback.len(), BOOTSTRAP_SELECT_MAX_PEERS);
        assert_eq!(
            &fallback[BOOTSTRAP_SELECT_MAX_PEERS - 2..],
            ["9.9.9.1", "9.9.9.2"]
        );
    }

    fn live_block_frame(round: u32) -> Vec<u8> {
        let payload = crate::protocol::test_block_payload(round, 2);
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.push(1);
        frame.extend_from_slice(&payload);
        frame
    }

    #[derive(Clone, Copy)]
    enum FakePeer {
        // greeting + one live block, then hold
        Healthy,
        // accept, say nothing
        Silent,
        // answer the greeting with the 0x04 peer-full status
        Full,
        // like Healthy, but only after this many ms
        Slow(u64),
        // like Healthy, starting at this round
        At(u32),
    }

    // A fake upstream 4001 on an ephemeral 127.0.0.1 port; returns ("127.0.0.1:port", task).
    // Ephemeral ports keep these tests running on hosts where a real node owns :4001.
    async fn fake_live_peer(kind: FakePeer) -> (String, tokio::task::JoinHandle<()>) {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let h = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut greet = [0u8; 8];
                    if s.read_exact(&mut greet).await.is_err() {
                        return;
                    }
                    match kind {
                        FakePeer::Slow(ms) => {
                            tokio::time::sleep(Duration::from_millis(ms)).await;
                            let _ = s.write_all(&[0, 0, 0, 1, 0, 3]).await;
                            let _ = s.write_all(&live_block_frame(1_400_000_000)).await;
                        }
                        FakePeer::Healthy => {
                            let _ = s.write_all(&[0, 0, 0, 1, 0, 3]).await;
                            let _ = s.write_all(&live_block_frame(1_400_000_000)).await;
                        }
                        FakePeer::At(round) => {
                            let _ = s.write_all(&[0, 0, 0, 1, 0, 3]).await;
                            let _ = s.write_all(&live_block_frame(round)).await;
                        }
                        FakePeer::Full => {
                            let _ = s.write_all(&[0, 0, 0, 1, 0, 4]).await;
                        }
                        FakePeer::Silent => {}
                    }
                    tokio::time::sleep(Duration::from_secs(10)).await;
                });
            }
        });
        (addr, h)
    }

    // An address that refuses connections (a port that was just free).
    async fn refused_addr() -> String {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        l.local_addr().unwrap().to_string()
    }

    fn test_tier(peers: &[&str], stage_ms: u64) -> Arc<PriorityTier> {
        test_tier_capped(peers, stage_ms, 0)
    }

    fn test_tier_capped(peers: &[&str], stage_ms: u64, max: usize) -> Arc<PriorityTier> {
        Arc::new(PriorityTier::new(
            peers.iter().map(|s| s.to_string()).collect(),
            max,
            Duration::from_millis(stage_ms),
            BootstrapPolicy::Fallback,
        ))
    }

    fn exclusive_tier(peers: &[&str], max: usize) -> Arc<PriorityTier> {
        Arc::new(
            PriorityTier::new(
                peers.iter().map(|s| s.to_string()).collect(),
                max,
                Duration::from_millis(300),
                BootstrapPolicy::Prefer,
            )
            .with_exclusive(true),
        )
    }

    // A priority peer starting more than the lag guard allows behind the freshest stream is
    // refused at selection (and cools down) instead of being cut seconds into the session: seen
    // 2026-10-28 at the US open, a QuickNode sentry ~1,200 rounds behind was picked again and cut
    // after 5s, twice, each time costing the node a reconnect.
    #[tokio::test]
    async fn a_priority_peer_behind_the_freshest_stream_is_not_selected() {
        let (behind, h1) = fake_live_peer(FakePeer::Healthy).await; // round 1_400_000_000
        let (fresh, h2) = fake_live_peer(FakePeer::At(1_400_001_200)).await;
        let pool = vec![fresh.clone()];
        // two other streams at 1_400_001_200: the floor is 100 below
        let freshest = Arc::new(FreshestRound::new(
            100,
            Duration::from_secs(5),
            Duration::from_secs(30),
            Arc::new(AtomicU32::new(1_400_001_000)),
        ));
        let (fa, fb) = (freshest.feed("a"), freshest.feed("b"));
        fa.offer(1_400_001_200);
        fb.offer(1_400_001_200);
        let floor = Some(freshest.clone());

        // no freshest round known: the priority peer is taken as before
        let tier = exclusive_tier(&[&behind], 0);
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, behind);
        drop(s);

        // behind the floor: refused and cooled down, so the exclusive tier opens the pool
        let tier = exclusive_tier(&[&behind], 0);
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, floor.clone())
            .await
            .unwrap();
        assert_eq!(s.ip, fresh);
        assert!(tier.pool_allowed(), "the lagging priority peer cools down");
        drop(s);

        // a peer at or above the floor is taken
        let tier = exclusive_tier(&[&fresh], 0);
        let s = select_live_peer(&tier, &[], 0, GREET_FALSE, 0, 0, floor.clone())
            .await
            .unwrap();
        assert_eq!(s.ip, fresh);
        drop(s);

        // one of the two streams behind the floor ends: no freshest round, no floor
        drop(fb);
        let tier = exclusive_tier(&[&behind], 0);
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, floor)
            .await
            .unwrap();
        assert_eq!(s.ip, behind);
        drop((s, fa));
        for h in [h1, h2] {
            h.abort();
        }
    }

    // QuickNode-style peering: a node stays on the priority peers while any of them serves —
    // a slow one is waited for past the stage, never outraced by the pool — and the pool takes
    // over only once every priority peer has failed or is unusable.
    #[tokio::test]
    async fn an_exclusive_tier_uses_the_pool_only_when_every_priority_peer_fails() {
        let (slow, h1) = fake_live_peer(FakePeer::Slow(1000)).await;
        let (o1, h2) = fake_live_peer(FakePeer::Healthy).await;
        let (silent, h3) = fake_live_peer(FakePeer::Silent).await;
        let refused = refused_addr().await;
        let pool = vec![o1.clone()];

        // a priority peer answering well past the 300ms stage still beats a healthy pool peer
        let tier = exclusive_tier(&[&slow], 0);
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, slow);
        assert!(!tier.pool_allowed());
        drop(s);

        // every priority peer refuses: the pool opens at once
        let tier = exclusive_tier(&[&refused], 0);
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, o1);
        assert!(
            tier.pool_allowed(),
            "the only priority peer is cooling down"
        );
        drop(s);

        // one priority peer refuses, the other hangs: no pool while the hung one might answer;
        // once the window has cooled it down too, the next selection may use the pool
        let tier = exclusive_tier(&[&refused, &silent], 0);
        assert!(select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .is_none());
        assert!(tier.pool_allowed());
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, o1);
        drop(s);

        // a priority peer at its session cap is unusable too
        let tier = exclusive_tier(&[&slow], 1);
        let held = tier.try_acquire(&slow).unwrap();
        assert!(tier.pool_allowed());
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, o1);
        drop((s, held));
        for h in [h1, h2, h3] {
            h.abort();
        }
    }

    // A peer that answers a bootstrap greeting with a 5MB abci_state frame header and payload.
    async fn fake_state_peer() -> (String, tokio::task::JoinHandle<()>) {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let h = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut greet = [0u8; 8];
                    if s.read_exact(&mut greet).await.is_err() {
                        return;
                    }
                    let len: u32 = 5_000_000;
                    let mut hdr = len.to_be_bytes().to_vec();
                    hdr.push(1);
                    let _ = s.write_all(&hdr).await;
                    let _ = s.write_all(&vec![0u8; len as usize]).await;
                    tokio::time::sleep(Duration::from_secs(10)).await;
                });
            }
        });
        (addr, h)
    }

    // QuickNode first, but bounded: a priority peer that accepts and never answers holds the pool
    // back only for the head start, not for the whole bootstrap window.
    #[tokio::test]
    async fn a_hung_priority_peer_holds_the_bootstrap_pool_back_only_briefly() {
        let (silent, h1) = fake_live_peer(FakePeer::Silent).await;
        let (state, h2) = fake_state_peer().await;
        let tier = exclusive_tier(&[&silent], 0);
        let t0 = Instant::now();
        let sel = select_fast_bootstrap_peer(
            &tier,
            &[silent.clone(), state.clone()],
            0,
            crate::protocol::GREET_TRUE,
        )
        .await
        .expect("the pool serves the state");
        assert_eq!(sel.ip, state);
        assert!(
            t0.elapsed() >= BOOTSTRAP_PRIORITY_HEAD_START,
            "{:?}",
            t0.elapsed()
        );
        assert!(t0.elapsed() < Duration::from_secs(5), "{:?}", t0.elapsed());
        for h in [h1, h2] {
            h.abort();
        }
    }

    #[test]
    fn an_exclusive_tier_offers_the_fallback_only_its_usable_priority_peers() {
        let pool: Vec<String> = vec!["198.51.100.1".into(), "198.51.100.2".into()];
        let tier = exclusive_tier(&["203.0.113.1", "203.0.113.2"], 0);
        assert_eq!(
            tiered_peers(&tier, &pool, 0),
            vec!["203.0.113.1".to_string(), "203.0.113.2".to_string()]
        );
        tier.mark_failed("203.0.113.1");
        assert_eq!(
            tiered_peers(&tier, &pool, 0),
            vec!["203.0.113.2".to_string()]
        );
        tier.mark_failed("203.0.113.2");
        assert_eq!(
            tiered_peers(&tier, &pool, 0),
            pool,
            "every priority peer is down"
        );
        // a staged (non-exclusive) tier keeps the pool behind its priority peers
        let staged = test_tier(&["203.0.113.1"], 300);
        assert_eq!(
            tiered_peers(&staged, &pool, 0),
            vec![
                "203.0.113.1".to_string(),
                "198.51.100.1".to_string(),
                "198.51.100.2".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn live_selection_prefers_priority_then_falls_back_to_pool() {
        let (p1, h1) = fake_live_peer(FakePeer::Healthy).await;
        let (o1, h2) = fake_live_peer(FakePeer::Healthy).await;
        let (slow, h3) = fake_live_peer(FakePeer::Silent).await;
        let refused = refused_addr().await;
        let pool = vec![o1.clone()];

        // healthy priority peer wins even though the pool is healthy too
        let tier = test_tier(&[&p1], 300);
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, p1);
        assert!(
            s.prio_session.is_some(),
            "priority slot travels with the session"
        );
        assert_eq!(tier.sessions(&p1), 1);
        drop(s);
        assert_eq!(tier.sessions(&p1), 0, "slot released with the session");

        // refused priority peer: pool wins without waiting out the stage, and the priority peer
        // goes into cooldown
        let tier = test_tier(&[&refused], 2000);
        let t0 = Instant::now();
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, o1);
        assert!(
            t0.elapsed() < Duration::from_millis(1500),
            "{:?}",
            t0.elapsed()
        );
        assert!(
            tier.candidates().is_empty(),
            "failed priority peer must cool down"
        );

        // silent priority peer: the pool opens at the stage boundary and wins
        let tier = test_tier(&[&slow], 300);
        let t0 = Instant::now();
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, o1);
        assert!(
            t0.elapsed() >= Duration::from_millis(300),
            "{:?}",
            t0.elapsed()
        );

        // empty tier = plain pool race
        let tier = test_tier(&[], 300);
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, o1);
        for h in [h1, h2, h3] {
            h.abort();
        }
    }

    #[tokio::test]
    async fn live_selection_respects_the_priority_cap_without_cooling_down() {
        let (p1, h1) = fake_live_peer(FakePeer::Healthy).await;
        let (o1, h2) = fake_live_peer(FakePeer::Healthy).await;
        let pool = vec![o1.clone()];
        let tier = test_tier_capped(&[&p1], 300, 1);
        let held = tier.try_acquire(&p1).unwrap(); // another node holds the only slot
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0, None)
            .await
            .unwrap();
        assert_eq!(s.ip, o1, "a full priority peer is skipped");
        assert_eq!(tier.sessions(&p1), 1, "the cap was never exceeded");
        drop(held);
        assert_eq!(
            tier.candidates(),
            vec![p1.clone()],
            "being full is not a failure: no cooldown"
        );
        for h in [h1, h2] {
            h.abort();
        }
    }

    #[tokio::test]
    async fn failed_live_selection_cools_down_hung_priority_peers() {
        let (silent, h) = fake_live_peer(FakePeer::Silent).await;
        // a priority peer that accepts and greets nothing never errors; when the window runs out
        // it must still be cooled down so the fallback doesn't re-dial it first
        let tier = test_tier(&[&silent], 300);
        assert!(select_live_peer(&tier, &[], 0, GREET_FALSE, 0, 0, None)
            .await
            .is_none());
        assert!(
            tier.candidates().is_empty(),
            "hung priority peer must cool down"
        );
        h.abort();
    }

    #[tokio::test]
    async fn fallback_race_is_not_held_up_by_a_hung_peer() {
        let (silent, h1) = fake_live_peer(FakePeer::Silent).await;
        let (ok, h2) = fake_live_peer(FakePeer::Healthy).await;
        let (full, h3) = fake_live_peer(FakePeer::Full).await;
        let refused = refused_addr().await;
        let tier = test_tier(&[&refused, &full], 300);
        let t0 = Instant::now();
        let (ip, _, greeting, _) = race_fallback_upstream(
            &tier,
            vec![refused.clone(), full.clone(), silent.clone(), ok.clone()],
            GREET_FALSE,
            FALLBACK_WAIT,
        )
        .await
        .unwrap();
        assert_eq!(ip, ok);
        assert_eq!(greeting, [0, 0, 0, 1, 0, 3]);
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "{:?}",
            t0.elapsed()
        );
        // the refused and the peer-full priority peers both cool down (peer-full may still be
        // in flight when the healthy peer wins; give it a moment)
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !tier.candidates().contains(&refused),
            "refused priority peer must cool down"
        );

        // nothing answers: gives up at the budget instead of walking the list
        let t0 = Instant::now();
        assert!(
            race_fallback_upstream(&tier, vec![silent.clone()], GREET_FALSE, FALLBACK_WAIT)
                .await
                .is_none()
        );
        assert!(t0.elapsed() < FALLBACK_WAIT + Duration::from_millis(300));
        // a peer-full answer is never returned as a greeting
        assert!(
            race_fallback_upstream(&tier, vec![full.clone()], GREET_FALSE, FALLBACK_WAIT)
                .await
                .is_none()
        );
        for h in [h1, h2, h3] {
            h.abort();
        }
    }

    #[test]
    fn bootstrap_candidates_prefer_remembered_state_servers() {
        let pool: Vec<String> = ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        // remembered heads the list, pool rotates from start, remembered ip deduped from pool
        let out = ordered_candidates(vec!["3.3.3.3".to_string(), "9.9.9.9".to_string()], &pool, 1);
        assert_eq!(out, ["3.3.3.3", "9.9.9.9", "2.2.2.2", "4.4.4.4", "1.1.1.1"]);
        // no memory -> plain rotation
        let out = ordered_candidates(Vec::new(), &pool, 2);
        assert_eq!(out, ["3.3.3.3", "4.4.4.4", "1.1.1.1", "2.2.2.2"]);
    }

    #[test]
    fn remember_state_server_dedups_most_recent_first_and_caps() {
        let list = Mutex::new(Vec::new());
        for i in 0..(STATE_SERVER_MEMORY + 3) {
            remember_state_server(&list, &format!("10.0.0.{i}"));
        }
        remember_state_server(&list, "10.0.0.5"); // re-serve moves to front, no duplicate
        let l = lock(&list);
        assert_eq!(l.len(), STATE_SERVER_MEMORY);
        assert_eq!(l[0], "10.0.0.5");
        assert_eq!(l.iter().filter(|ip| *ip == "10.0.0.5").count(), 1);
    }

    #[test]
    fn selection_failure_summary_buckets_by_cause() {
        let errs: Vec<String> = [
            "not serving abci_state (len=57)",
            "not serving abci_state (len=13)",
            "connect timeout",
            "connect: Connection refused (os error 111)",
            "greeting header: failed to fill whole buffer",
            "greeting header timeout",
            "peer full",
            "no live block before deadline",
            "deadline has elapsed",
            "live block lagged round=10 net_tip=20 lag=10 max_lag=5",
            "live block behind the freshest stream round=10 floor=20",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let s = summarize_peer_errors(12, &errs);
        assert!(s.starts_with("tried=12"), "{s}");
        for part in [
            "small_frame=2",
            "connect_fail=2",
            "eof=1",
            "timeout=2",
            "peer_full=1",
            "no_block=1",
            "lagged=2",
        ] {
            assert!(s.contains(part), "missing {part} in {s}");
        }
    }

    fn fast_repin() -> RepinTiming {
        RepinTiming {
            min_session: Duration::from_millis(150),
            catchup_quiet: Duration::from_millis(300),
            catchup_max: Duration::from_secs(2),
            check_every: Duration::from_millis(50),
            retry: Duration::from_secs(10),
        }
    }

    // ...but not to one that starts behind the session's freshest stream by more than its lag
    // guard allows: the guard would cut it seconds after the return
    #[tokio::test]
    async fn a_pool_session_does_not_return_to_a_lagging_priority_peer() {
        let (p, h) = fake_live_peer(FakePeer::Healthy).await; // round 1_400_000_000
        let tier = test_tier_capped(&[&p], 300, 1);
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        let tip = Arc::new(AtomicU32::new(1_400_001_000));
        let freshest = Arc::new(FreshestRound::new(
            100,
            Duration::from_secs(5),
            Duration::from_secs(30),
            tip.clone(),
        ));
        let (a, b) = (freshest.feed("a"), freshest.feed("b"));
        a.offer(1_400_001_200);
        b.offer(1_400_001_200);
        assert_eq!(freshest.select_floor(), 1_400_001_100);
        let wait = timeout(
            Duration::from_secs(2),
            wait_priority_return(node, tier.clone(), tip, fast_repin(), Some(freshest)),
        )
        .await;
        assert!(wait.is_err(), "no return to a peer behind the floor");
        assert!(tier.pool_allowed(), "the lagging priority peer cools down");
        drop((a, b));
        h.abort();
    }

    #[tokio::test]
    async fn a_pool_session_returns_once_a_priority_peer_serves() {
        let (p, h) = fake_live_peer(FakePeer::Healthy).await;
        let tier = test_tier_capped(&[&p], 300, 1);
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        let t0 = Instant::now();
        let reason = timeout(
            Duration::from_secs(5),
            wait_priority_return(
                node.clone(),
                tier.clone(),
                Arc::new(AtomicU32::new(0)),
                fast_repin(),
                None,
            ),
        )
        .await
        .expect("the session ends once the priority peer serves");
        assert!(
            t0.elapsed() >= Duration::from_millis(150),
            "not before the age floor"
        );
        assert!(reason.starts_with(RETURNING_TO_PRIORITY), "{reason}");
        assert!(
            !crate::push::push_exit_is_upstream_fault(&reason),
            "the pool peer it leaves did nothing wrong"
        );
        assert_eq!(
            tier.sessions(&p),
            0,
            "probe slot released before the reconnect"
        );
        h.abort();
    }

    #[tokio::test]
    async fn no_return_while_priority_peers_are_unavailable_or_failing() {
        let tip = || Arc::new(AtomicU32::new(0));
        let node = || Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        let (p, h) = fake_live_peer(FakePeer::Healthy).await;
        // cooling down: no candidate, no probe, the session goes on
        let cooling = test_tier(&[&p], 300);
        cooling.mark_failed(&p);
        let n = node();
        assert!(timeout(
            Duration::from_millis(600),
            wait_priority_return(n.clone(), cooling, tip(), fast_repin(), None)
        )
        .await
        .is_err());
        assert!(
            n.claim_repin_attempt(Instant::now(), fast_repin().retry),
            "no attempt spent"
        );
        // full (other nodes hold its session cap): same
        let full = test_tier_capped(&[&p], 300, 1);
        let _held = full.try_acquire(&p).unwrap();
        assert!(timeout(
            Duration::from_millis(600),
            wait_priority_return(node(), full.clone(), tip(), fast_repin(), None)
        )
        .await
        .is_err());
        assert_eq!(full.sessions(&p), 1, "the probe never took the last slot");
        h.abort();
        // a candidate that doesn't serve: one probe, then it cools down like a failed selection
        // and this node won't probe again within the retry period
        let (refuses, h2) = fake_live_peer(FakePeer::Full).await;
        let tier = test_tier(&[&refuses], 300);
        let n = node();
        assert!(timeout(
            Duration::from_millis(800),
            wait_priority_return(n.clone(), tier.clone(), tip(), fast_repin(), None)
        )
        .await
        .is_err());
        assert!(
            tier.candidates().is_empty(),
            "a failed probe cools the peer down"
        );
        assert!(!n.claim_repin_attempt(Instant::now(), fast_repin().retry));
        h2.abort();
    }

    #[tokio::test]
    async fn no_return_while_the_node_is_still_catching_up() {
        let (p, h) = fake_live_peer(FakePeer::Healthy).await;
        let tier = test_tier(&[&p], 300);
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        // client-block batches keep arriving: the node is catching up
        let feeder = {
            let node = node.clone();
            tokio::spawn(async move {
                loop {
                    node.mark_client_blocks_response(Instant::now(), None, 100);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        };
        let wait = tokio::spawn(wait_priority_return(
            node.clone(),
            tier.clone(),
            Arc::new(AtomicU32::new(0)),
            fast_repin(),
            None,
        ));
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(
            !wait.is_finished(),
            "no re-pin while client blocks keep flowing"
        );
        assert_eq!(tier.sessions(&p), 0, "and no probe");
        // catch-up done: once it has been quiet for catchup_quiet, the session returns
        feeder.abort();
        let reason = timeout(Duration::from_secs(5), wait)
            .await
            .expect("returns once the node has caught up")
            .unwrap();
        assert!(reason.starts_with(RETURNING_TO_PRIORITY), "{reason}");
        assert!(
            !node.claim_repin_attempt(Instant::now(), fast_repin().retry),
            "only the probe that ran counted as the node's attempt"
        );
        h.abort();
    }

    #[tokio::test]
    async fn a_return_probe_tries_every_priority_peer_in_turn() {
        let (refuses, h1) = fake_live_peer(FakePeer::Full).await;
        let (serves, h2) = fake_live_peer(FakePeer::Healthy).await;
        let tier = test_tier(&[&refuses, &serves], 300);
        assert_eq!(tier.candidates()[0], refuses, "the broken one comes first");
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        let reason = timeout(
            Duration::from_secs(5),
            wait_priority_return(
                node.clone(),
                tier.clone(),
                Arc::new(AtomicU32::new(0)),
                fast_repin(),
                None,
            ),
        )
        .await
        .expect("the second priority peer serves on the same attempt");
        assert!(reason.contains(&serves), "{reason}");
        assert!(
            !node.claim_repin_attempt(Instant::now(), fast_repin().retry),
            "one attempt covered both peers"
        );
        assert_eq!(tier.sessions(&serves), 0, "probe slot released");
        h1.abort();
        h2.abort();
    }

    #[tokio::test]
    async fn a_catch_up_that_never_converges_ends_the_session_once() {
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        let feeder = {
            let node = node.clone();
            tokio::spawn(async move {
                // the stuck loop: batches keep coming at a constant distance behind the tip
                loop {
                    // the stuck loop: small ranges at a constant distance
                    node.mark_client_blocks_response(Instant::now(), Some(900), 27);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        };
        let t0 = Instant::now();
        let reason = timeout(
            Duration::from_secs(5),
            wait_catchup_stuck(node.clone(), fast_repin()),
        )
        .await
        .expect("ends once the catch-up has made no progress for catchup_max");
        assert!(reason.contains("900 rounds behind"), "{reason}");
        assert!(
            t0.elapsed() >= Duration::from_secs(2),
            "not before catchup_max"
        );
        assert!(reason.starts_with(NODE_CATCHUP_STUCK), "{reason}");
        assert!(
            !crate::push::push_exit_is_upstream_fault(&reason),
            "the upstream did nothing wrong"
        );
        // the reconnect's session doesn't end again within `retry`
        assert!(timeout(
            Duration::from_secs(3),
            wait_catchup_stuck(node.clone(), fast_repin())
        )
        .await
        .is_err());
        feeder.abort();
        // a long catch-up that keeps closing in on the tip is never cut
        let converging = Arc::new(NodeState::new("10.0.0.7".parse().unwrap(), 1));
        let feeder = {
            let node = converging.clone();
            tokio::spawn(async move {
                let mut behind = 60_000u32;
                loop {
                    node.mark_client_blocks_response(Instant::now(), Some(behind), 100);
                    behind = behind.saturating_sub(200);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        };
        assert!(timeout(
            Duration::from_secs(3),
            wait_catchup_stuck(converging, fast_repin())
        )
        .await
        .is_err());
        feeder.abort();
        // a node that catches up normally (quiet gaps) is never cut
        let calm = Arc::new(NodeState::new("10.0.0.8".parse().unwrap(), 1));
        calm.mark_client_blocks_response(Instant::now(), None, 100);
        assert!(timeout(
            Duration::from_secs(3),
            wait_catchup_stuck(calm, fast_repin())
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn a_catch_up_that_never_ends_stops_holding_the_return_back() {
        let (p, h) = fake_live_peer(FakePeer::Healthy).await;
        let tier = test_tier(&[&p], 300);
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        // client-block batches never stop (a pool 4002 horizon stuck behind the tip)
        let feeder = {
            let node = node.clone();
            tokio::spawn(async move {
                loop {
                    node.mark_client_blocks_response(Instant::now(), None, 100);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        };
        let t0 = Instant::now();
        let reason = timeout(
            Duration::from_secs(5),
            wait_priority_return(
                node.clone(),
                tier.clone(),
                Arc::new(AtomicU32::new(0)),
                fast_repin(),
                None,
            ),
        )
        .await
        .expect("returns once the catch-up has run catchup_max");
        assert!(
            t0.elapsed() >= Duration::from_secs(2),
            "not before catchup_max"
        );
        assert!(reason.starts_with(RETURNING_TO_PRIORITY), "{reason}");
        feeder.abort();
        h.abort();
    }
}
