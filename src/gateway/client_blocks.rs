use crate::priority::{gwlog, PrioritySession, PriorityTier};
use crate::push::NODE_WRITE_TIMEOUT;
use crate::util::lock;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

// Also in its structured (bincode) form, which mainnet peers send today: exactly 00 01 00 (error,
// variant 0). A batch payload starts with its 4-byte LE uncompressed size, so it can't be 3 bytes.
pub(crate) fn response_is_peer_only(payload: &[u8]) -> bool {
    payload == STRUCTURED_PEER_ONLY
        || payload
            .windows(b"Peer-only request".len())
            .any(|w| w == b"Peer-only request")
}

const STRUCTURED_PEER_ONLY: [u8; 3] = [0x00, 0x01, 0x00];

pub(crate) fn response_has_no_client_blocks(payload: &[u8]) -> bool {
    payload
        .windows(b"no client blocks to serve".len())
        .any(|w| w == b"no client blocks to serve")
}

pub(crate) fn response_has_client_block_round_too_large(payload: &[u8]) -> bool {
    payload
        .windows(b"client block round too large".len())
        .any(|w| w == b"client block round too large")
}

// The same error in its structured (bincode) form, as a short frame: 0x00, 0x01 (error), 0x06, then
// the requested round and the peer's committed tip, each 0xfc + u32 LE. A real batch payload is an
// LZ4 block with its 4-byte LE uncompressed size first; one starting 00 01 06 fc would declare
// ~4.2 GB, impossible for a 13-byte payload, so the exact layout can't match a batch.
pub(crate) fn response_is_structured_round_too_large(payload: &[u8]) -> bool {
    payload.len() == 13 && payload[..4] == [0x00, 0x01, 0x06, 0xfc] && payload[8] == 0xfc
}

pub(crate) fn response_has_client_block_round_too_small(payload: &[u8]) -> bool {
    payload
        .windows(b"client block round too small".len())
        .any(|w| w == b"client block round too small")
}

fn client_block_peer_candidates(
    head: Vec<String>,
    peers: &[String],
    rr: &AtomicUsize,
    limit: usize,
) -> Vec<String> {
    if limit == 0 {
        return Vec::new();
    }
    let mut out: Vec<String> = head.into_iter().take(limit).collect();
    if peers.is_empty() {
        return out;
    }
    let start = rr.fetch_add(limit.max(1), Ordering::Relaxed);
    let mut seen = HashSet::new();
    for k in 0..peers.len() {
        if out.len() >= limit {
            break;
        }
        let ip = peers[(start + k) % peers.len()].clone();
        if !out.contains(&ip) && seen.insert(ip.clone()) {
            out.push(ip);
        }
    }
    out
}

// A 4002 response frame together with its share of CLIENT_BLOCK_RESPONSE_BYTES: the budget is
// held for as long as the frame exists (kept as a fallback answer, being written to the node),
// not just while it is read.
struct BudgetedFrame {
    bytes: Vec<u8>,
    _budget: tokio::sync::SemaphorePermit<'static>,
}

impl std::ops::Deref for BudgetedFrame {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

enum ClientBlockUpstreamResponse {
    Frames(BudgetedFrame),
    PeerOnly,
    RangeTooSmall(BudgetedFrame),
    TerminalError(BudgetedFrame),
}

async fn fetch_client_blocks_from_peer(
    ip: String,
    req: Arc<Vec<u8>>,
    prewarm_live: bool,
) -> (String, std::io::Result<ClientBlockUpstreamResponse>) {
    let result = async {
        let _live_guard = if prewarm_live {
            Some(open_peer_relationship(&ip, Duration::from_secs(4)).await?)
        } else {
            None
        };
        let mut upc = timeout(
            Duration::from_secs(4),
            crate::blacklist::connect(&format!("{ip}:4002")),
        )
        .await??;
        upc.set_nodelay(true).ok();
        timeout(Duration::from_secs(4), upc.write_all(req.as_slice())).await??;

        let mut rh = [0u8; 5];
        timeout(Duration::from_secs(6), upc.read_exact(&mut rh)).await??;
        let rl = u32::from_be_bytes([rh[0], rh[1], rh[2], rh[3]]) as usize;
        // A client-block batch is ~100 rounds (mostly 8-20MB, 57MB seen); cap well above that but
        // far below a memory hazard.
        if rl > MAX_CLIENT_BLOCK_RESPONSE {
            return Err(std::io::Error::other("oversized client-block response"));
        }
        // Every in-flight response buffer draws on one gateway-wide byte budget: up to 8 fetches
        // x 9 attempts could otherwise buffer ~4.6GB at once.
        let budget = timeout(
            Duration::from_secs(6),
            CLIENT_BLOCK_RESPONSE_BYTES.acquire_many((5 + rl) as u32),
        )
        .await
        .map_err(|_| std::io::Error::other("client-block response byte budget exhausted"))?
        .map_err(|_| std::io::Error::other("client-block response budget closed"))?;
        // read straight into the frame that is forwarded (no second copy)
        let mut frame = vec![0u8; 5 + rl];
        frame[..5].copy_from_slice(&rh);
        timeout(Duration::from_secs(12), upc.read_exact(&mut frame[5..])).await??;
        let rp = &frame[5..];
        if response_is_peer_only(rp) {
            return Ok(ClientBlockUpstreamResponse::PeerOnly);
        }
        let terminal = response_has_no_client_blocks(rp)
            || response_has_client_block_round_too_large(rp)
            || response_is_structured_round_too_large(rp);
        let too_small = response_has_client_block_round_too_small(rp);
        let frame = BudgetedFrame {
            bytes: frame,
            _budget: budget,
        };
        Ok(if terminal {
            ClientBlockUpstreamResponse::TerminalError(frame)
        } else if too_small {
            ClientBlockUpstreamResponse::RangeTooSmall(frame)
        } else {
            ClientBlockUpstreamResponse::Frames(frame)
        })
    }
    .await;
    (ip, result)
}

// A node opens 4002 right after its 4001 greet, so the real active pin can be milliseconds
// away — wait briefly instead of racing candidates against an imminent pin.
const ACTIVE_4002_PIN_WAIT: Duration = Duration::from_millis(1500);

// How long the pinned active gets to answer ALONE before candidates are hedged in. Keeps the
// common case at one upstream connection and guarantees a healthy active can't be outraced by
// a cross-peer response.
const ACTIVE_4002_GRACE: Duration = Duration::from_millis(2000);
// On an exclusive priority tier, how long into a request the priority peers have before the pool
// is hedged anyway: one that hasn't answered by then counts as failed for the request and cools
// down (a hung one would otherwise hold every request past its budget, the pool never reached).
// Priority answers take ~2s; this leaves the pool most of the request budget.
const PRIORITY_4002_ROUND: Duration = Duration::from_secs(6);

// Does the pinned active answer ALONE first? Not a pool active (a fallback session) on an
// exclusive tier while a priority peer is usable: its requests go to the priority round like any
// other, the pool only after it. A pool peer can serve 4002 from a horizon well behind the tip,
// holding the node that far behind for as long as it stays the 4002 source.
fn active_answers_first(priority: &PriorityTier, ip: &str) -> bool {
    priority.contains(ip) || priority.pool_allowed()
}

// A client-block range query (docs/hl-p2p-protocol.md §8a): a type-0 frame whose 11-byte payload
// is the 0x00 tag and two 0xfc-prefixed u32 rounds. Returns its closed range [start, end].
fn client_block_range(req: &[u8]) -> Option<(u32, u32)> {
    let range = req.len() == 16
        && req[..5] == [0, 0, 0, 11, 0]
        && req[5] == 0
        && req[6] == 0xfc
        && req[11] == 0xfc;
    range.then(|| {
        (
            u32::from_le_bytes([req[7], req[8], req[9], req[10]]),
            u32::from_le_bytes([req[12], req[13], req[14], req[15]]),
        )
    })
}

fn client_block_range_request(start: u32, end: u32) -> Vec<u8> {
    let mut req = vec![0, 0, 0, 11, 0, 0, 0xfc];
    req.extend_from_slice(&start.to_le_bytes());
    req.push(0xfc);
    req.extend_from_slice(&end.to_le_bytes());
    req
}

// ---- next-range prefetch ----
//
// A catching-up node walks contiguous ranges one request at a time: it asks for [e+1, e+W] only
// once it has processed [s, e]. Under heavy blocks (30-40 MB per 100 rounds, ~4.5 s from the
// upstream plus ~3.5 s in the node) that is ~12.5 rounds/s, below mainnet's ~14.8, so a node that
// fell behind never caught up (2026-10-28: 800+ s behind for an hour). Once a batch is answered,
// the next range is fetched while the node works through this one, and served when asked.

// a prefetch not taken by then is dropped, with its batch and memory budget (the node stalled or
// went elsewhere; it re-asks anyway)
const PREFETCH_MAX_AGE: Duration = Duration::from_secs(20);
// prefetches running at once, gateway-wide: their own limit, so they never take the fetch slots
// real requests need (4 x a 64 MB batch is also well inside the shared response byte budget)
static PREFETCH_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);
static PREFETCH_IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
// only while the next range ends this far below the freshest live round: peers serve committed
// client blocks, which trail the live tip, and refuse past them ("round too large")
const PREFETCH_TIP_MARGIN: u32 = 300;
// the widest range prefetched (nodes ask for 100 rounds)
const PREFETCH_MAX_WIDTH: u32 = 1_000;

// The range to prefetch after [start, end] was answered, if the node is still behind enough.
fn next_prefetch_range(start: u32, end: u32, freshest: u32) -> Option<(u32, u32)> {
    let width = end.checked_sub(start)?.checked_add(1)?;
    if width > PREFETCH_MAX_WIDTH {
        return None;
    }
    let next_start = end.checked_add(1)?;
    let next_end = end.checked_add(width)?;
    (freshest.checked_sub(next_end)? >= PREFETCH_TIP_MARGIN).then_some((next_start, next_end))
}

// One node's prefetched next range; the fetch is aborted when this is dropped unused.
//
// The fetch is the node's own next request made early, so its side effects are the ones that
// request would have: a priority peer that fails it cools down, a hedge winner is remembered, an
// active answering Peer-only is unpinned. A new pin cancels it (NodeState::pin_active).
pub(super) struct Prefetch {
    id: u64,
    start: u32,
    end: u32,
    // the node's pin generation when the fetch began: a batch fetched under one session isn't
    // served into another (an unpin, e.g. the active refusing it as Peer-only, keeps it valid:
    // the fetch hedged elsewhere exactly as the node's request would have)
    generation: u64,
    at: Instant,
    task: Option<tokio::task::JoinHandle<Result<BudgetedFrame, String>>>,
}

impl Drop for Prefetch {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

impl Prefetch {
    fn serves(&self, start: u32, end: u32, generation: u64, now: Instant) -> bool {
        self.start == start
            && self.end == end
            && self.generation == generation
            && now.saturating_duration_since(self.at) <= PREFETCH_MAX_AGE
    }
}

// The node's prefetch for exactly [start, end] under its current pin generation, if any.
// Whatever was prefetched is taken out either way: a node asking for anything else has moved on.
fn take_prefetch(node: &NodeState, start: u32, end: u32) -> Option<Prefetch> {
    let p = lock(&node.prefetch).take()?;
    p.serves(start, end, node.pin_generation(), Instant::now())
        .then_some(p)
}

// Drop prefetch `id` if it is still waiting to be taken.
fn expire_prefetch(node: &std::sync::Weak<NodeState>, id: u64) {
    let Some(node) = node.upgrade() else {
        return;
    };
    let mut slot = lock(&node.prefetch);
    if slot.as_ref().is_some_and(|p| p.id == id) {
        let stale = slot.take();
        drop(slot);
        drop(stale);
    }
}

// Fetch [start, end] for the node in the background, replacing any earlier prefetch; dropped
// after PREFETCH_MAX_AGE if not taken. Skipped when PREFETCH_SLOTS are all busy.
fn start_prefetch(
    node: &Arc<NodeState>,
    priority: &Arc<PriorityTier>,
    peers: &[String],
    rr: &Arc<AtomicUsize>,
    (start, end): (u32, u32),
) {
    let Ok(slot) = PREFETCH_SLOTS.try_acquire() else {
        return;
    };
    let generation = node.pin_generation();
    let id = PREFETCH_IDS.fetch_add(1, Ordering::Relaxed);
    let (n, prio, peers, rr) = (node.clone(), priority.clone(), peers.to_vec(), rr.clone());
    let task = tokio::spawn(async move {
        let _slot = slot;
        let req = Arc::new(client_block_range_request(start, end));
        timeout(
            CLIENT_BLOCK_FETCH_DEADLINE,
            fetch_client_blocks_raced(&n, &prio, &peers, &rr, req),
        )
        .await
        .unwrap_or_else(|_| Err("prefetch deadline".to_string()))
    });
    let p = Prefetch {
        id,
        start,
        end,
        generation,
        at: Instant::now(),
        task: Some(task),
    };
    {
        // installed under the slot's lock, after re-checking the generation: pin_active bumps the
        // generation before it takes this lock, so a re-pin either is seen here or drops it there
        let mut slot = lock(&node.prefetch);
        if node.pin_generation() != generation {
            drop(slot);
            drop(p);
            return;
        }
        *slot = Some(p);
    }
    let weak = Arc::downgrade(node);
    tokio::spawn(async move {
        tokio::time::sleep(PREFETCH_MAX_AGE).await;
        expire_prefetch(&weak, id);
    });
}

// how often a request waiting on a prefetch checks for a re-pin
const PREFETCH_PIN_CHECK: Duration = Duration::from_millis(100);

// Wait (within the request's budget) for a prefetched batch; None when it failed, isn't a batch,
// or the node was re-pinned meanwhile (then the fetch is aborted at once rather than awaited), so
// the request is fetched as usual.
async fn await_prefetch(mut p: Prefetch, node: &NodeState) -> Option<BudgetedFrame> {
    let started = Instant::now();
    let deadline = started + CLIENT_BLOCK_FETCH_DEADLINE;
    let mut task = p.task.take()?;
    let joined = loop {
        if node.pin_generation() != p.generation || Instant::now() >= deadline {
            task.abort();
            return None;
        }
        if let Ok(joined) = timeout(PREFETCH_PIN_CHECK, &mut task).await {
            break joined;
        }
    };
    let frame = match joined {
        Ok(Ok(frame)) if frame.get(4) == Some(&1) => frame,
        _ => return None,
    };
    if node.pin_generation() != p.generation {
        return None;
    }
    gwlog!(
        "[gw] [{}] 4002 prefetched range {}..={} served (waited {}ms)",
        node.ip,
        p.start,
        p.end,
        started.elapsed().as_millis()
    );
    Some(frame)
}

async fn wait_active_session(node: &NodeState, budget: Duration) -> Option<ActivePeer> {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(peer) = node.active_session_snapshot() {
            return Some(peer);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// Record one race result. Returns Some(frame) when this attempt wins outright.
fn note_client_block_result(
    node: &NodeState,
    tag: Option<&ActivePeer>,
    ip: &str,
    result: std::io::Result<ClientBlockUpstreamResponse>,
    failures: &mut Vec<String>,
    terminal: &mut Option<BudgetedFrame>,
    too_small: &mut Option<BudgetedFrame>,
) -> Option<BudgetedFrame> {
    if let Some(peer) = tag {
        if !node.active_is_current(peer) {
            failures.push(format!("{}: stale active epoch {}", peer.ip, peer.epoch));
            return None;
        }
    }
    match result {
        Ok(ClientBlockUpstreamResponse::Frames(frame)) => return Some(frame),
        Ok(ClientBlockUpstreamResponse::TerminalError(frame)) => {
            if terminal.is_none() {
                *terminal = Some(frame);
            }
            failures.push(format!("{ip}: terminal"));
        }
        Ok(ClientBlockUpstreamResponse::PeerOnly) => {
            if let Some(peer) = tag {
                // the pinned peer refuses client-block RPC: unpin it so the node's next 4001
                // reconnect elects a fresh active instead of re-hitting the same peer forever
                node.clear_active_if_current(peer);
                failures.push(format!("{}: peer-only (active unpinned)", peer.ip));
            } else {
                failures.push(format!("{ip}: peer-only"));
            }
        }
        Ok(ClientBlockUpstreamResponse::RangeTooSmall(frame)) => {
            // another peer may still hold the range; kept only as the last-resort answer
            if too_small.is_none() {
                *too_small = Some(frame);
            }
            failures.push(format!("{ip}: range-too-small"));
        }
        Err(e) => {
            failures.push(format!("{ip}: {e}"));
        }
    }
    None
}

// One line per answered 4002 request: which upstream, which tier, how big, how long since the
// node asked. A terminal error frame (e.g. "client block round too large: X > tip") is logged
// with its text, which also reveals the upstream's own tip.
fn log_client_blocks(
    node: &NodeState,
    priority: &PriorityTier,
    ip: &str,
    stage: &str,
    frame: &[u8],
    started: Instant,
) {
    let detail = if frame.len() < 300 {
        format!(
            " text={:?}",
            String::from_utf8_lossy(&frame[5.min(frame.len())..])
        )
    } else {
        String::new()
    };
    gwlog!(
        "[gw] [{}] 4002 client blocks via {} tier={} stage={} bytes={} ms={}{}",
        node.ip,
        ip,
        priority.tier_of(ip),
        stage,
        frame.len(),
        started.elapsed().as_millis(),
        detail
    );
}

async fn fetch_client_blocks_raced(
    node: &NodeState,
    priority: &Arc<PriorityTier>,
    peers: &[String],
    rr: &AtomicUsize,
    req: Arc<Vec<u8>>,
) -> Result<BudgetedFrame, String> {
    let started = Instant::now();
    // a remembered hedge winner (see NodeState::rpc_peer) stands in for a missing or deferred pin
    // right away (a remembered pool peer only while the pool may serve this node at all)
    let remembered = lock(&node.rpc_peer)
        .clone()
        .filter(|ip| !crate::blacklist::contains(ip))
        .filter(|ip| priority.contains(ip) || priority.pool_allowed());
    let pinned = if remembered.is_some() {
        node.active_session_snapshot()
    } else {
        wait_active_session(node, ACTIVE_4002_PIN_WAIT).await
    };
    // a pool active that may not answer first keeps its direct attempt for the pool phase
    let (active, mut deferred) = match pinned {
        Some(peer) if !active_answers_first(priority, &peer.ip) => (None, Some(peer)),
        pinned => (pinned, None),
    };
    let deferred_pin = deferred.clone();
    // A prewarmed attempt opens its own live relationship, i.e. one more connection on that
    // peer: a priority peer is only used with a free slot, held for the attempt.
    let prewarm_slot = |ip: &str| -> Option<Option<PrioritySession>> {
        if priority.contains(ip) {
            priority.try_acquire(ip).map(Some)
        } else {
            Some(None)
        }
    };
    let first = match (&active, &remembered) {
        (Some(peer), _) => Some((Some(peer.clone()), peer.ip.clone(), false, "active", None)),
        (None, Some(ip)) => {
            prewarm_slot(ip).map(|slot| (None, ip.clone(), true, "remembered", slot))
        }
        (None, None) => None,
    };
    let mut attempts = tokio::task::JoinSet::new();
    let mut failures = Vec::new();
    let mut terminal = None;
    let mut too_small = None;
    let mut terminal_from = String::new();
    let mut too_small_from = String::new();

    // Stage 1: the pinned active (or the remembered RPC peer) ALONE within a grace budget (one
    // upstream connection in the common case; no candidate can outrace it with a cross-peer
    // response).
    let first_ip = first.as_ref().map(|f| f.1.clone());
    if let Some((tag, ip, prewarm, stage, slot)) = first {
        let req_for_first = req.clone();
        attempts.spawn(async move {
            let _slot = slot;
            let result = fetch_client_blocks_from_peer(ip.clone(), req_for_first, prewarm)
                .await
                .1;
            (tag, ip, result)
        });
        match timeout(ACTIVE_4002_GRACE, attempts.join_next()).await {
            Ok(Some(Ok((tag, ip, result)))) => {
                if let Some(frame) = note_client_block_result(
                    node,
                    tag.as_ref(),
                    &ip,
                    result,
                    &mut failures,
                    &mut terminal,
                    &mut too_small,
                ) {
                    log_client_blocks(node, priority, &ip, stage, &frame, started);
                    return Ok(frame);
                }
                // a last-resort answer from the first attempt: remember who gave it for the log
                if terminal.is_some() {
                    terminal_from = ip.clone();
                }
                if too_small.is_some() {
                    too_small_from = ip;
                }
            }
            Ok(_) => {}
            // grace expired: the first attempt stays in the set and keeps racing below
            Err(_) => {}
        }
    }

    // Stage 2: hedge across candidates — only reached when the active is absent, failed, or
    // slow. Each candidate opens its own temporary live relationship (prewarm) first. An exclusive
    // priority tier hedges its own peers first and the pool only once all of them have failed this
    // request without a definitive (range) answer.
    let prio = priority.candidates();
    let pool_waits = priority.exclusive() && !prio.is_empty();
    let mut round = if pool_waits {
        let n = prio.len();
        client_block_peer_candidates(prio, &[], rr, n)
    } else {
        client_block_peer_candidates(prio, peers, rr, 8)
    };
    let mut pool_hedged = !pool_waits;
    loop {
        // the pool may serve now: the deferred pool active answers over its own session, as in
        // stage 1, next to the pool candidates
        if pool_hedged {
            if let Some(peer) = deferred.take() {
                let req = req.clone();
                attempts.spawn(async move {
                    let ip = peer.ip.clone();
                    let result = fetch_client_blocks_from_peer(ip.clone(), req, false)
                        .await
                        .1;
                    (Some(peer), ip, result)
                });
            }
        }
        // the priority peers hedged in this round that haven't answered yet
        let mut prio_pending: Vec<String> = Vec::new();
        for ip in round {
            if first_ip.as_deref() == Some(ip.as_str())
                || deferred_pin.as_ref().is_some_and(|peer| peer.ip == ip)
            {
                continue;
            }
            let Some(slot) = prewarm_slot(&ip) else {
                continue; // priority peer at its session cap
            };
            if !pool_hedged {
                prio_pending.push(ip.clone());
            }
            let req = req.clone();
            attempts.spawn(async move {
                let _slot = slot;
                let result = fetch_client_blocks_from_peer(ip.clone(), req, true).await.1;
                (None, ip, result)
            });
        }

        let round_deadline = (!pool_hedged).then_some(started + PRIORITY_4002_ROUND);
        let mut round_timed_out = false;
        loop {
            let joined = match round_deadline {
                Some(d) => match timeout(
                    d.saturating_duration_since(Instant::now()),
                    attempts.join_next(),
                )
                .await
                {
                    Ok(Some(j)) => j,
                    Ok(None) => break,
                    Err(_) => {
                        // hung for this request: the full failure cooldown (a 4002 answer was due ~4s ago;
                        // unlike a live connect, this isn't the node's first seconds after a restart)
                        for ip in &prio_pending {
                            priority.mark_failed(ip);
                        }
                        round_timed_out = true;
                        gwlog!(
                            "[gw] [{}] 4002 priority peers silent after {}s (hung={}); hedging the pool",
                            node.ip,
                            PRIORITY_4002_ROUND.as_secs(),
                            prio_pending.len()
                        );
                        break;
                    }
                },
                None => match attempts.join_next().await {
                    Some(j) => j,
                    None => break,
                },
            };
            let Ok((tag, ip, result)) = joined else {
                continue;
            };
            prio_pending.retain(|p| p != &ip);
            let had_terminal = terminal.is_some();
            let had_too_small = too_small.is_some();
            if let Some(frame) = note_client_block_result(
                node,
                tag.as_ref(),
                &ip,
                result,
                &mut failures,
                &mut terminal,
                &mut too_small,
            ) {
                attempts.abort_all();
                let stage = if tag.is_some() { "active" } else { "hedge" };
                let remember = tag.is_none()
                    && match &deferred_pin {
                        // the pool pin stays: a priority winner serves its next batches first
                        // (until the next 4001 session re-pins, which forgets it)
                        Some(pin) => priority.contains(&ip) && node.active_is_current(pin),
                        None => node.active_session_snapshot().is_none(),
                    };
                if remember {
                    *lock(&node.rpc_peer) = Some(ip.clone());
                }
                log_client_blocks(node, priority, &ip, stage, &frame, started);
                return Ok(frame);
            }
            if !had_terminal && terminal.is_some() {
                terminal_from = ip.clone();
            }
            if !had_too_small && too_small.is_some() {
                too_small_from = ip;
            }
        }
        if pool_hedged
            || terminal.is_some()
            || too_small.is_some()
            || (peers.is_empty() && deferred.is_none())
        {
            break;
        }
        pool_hedged = true;
        if !round_timed_out {
            gwlog!(
                "[gw] [{}] 4002 every priority peer failed this request; hedging the pool",
                node.ip
            );
        }
        round = client_block_peer_candidates(Vec::new(), peers, rr, 8);
    }
    if let Some(frame) = terminal {
        log_client_blocks(node, priority, &terminal_from, "terminal", &frame, started);
        return Ok(frame);
    }
    // every peer said the range is older than what it keeps: tell the node so (it would otherwise
    // only see the socket close and may retry the same range)
    if let Some(frame) = too_small {
        log_client_blocks(node, priority, &too_small_from, "terminal", &frame, started);
        return Ok(frame);
    }
    Err(failures.join("; "))
}

// Serve the node's client-block RPC (port 4002), staged: the real 4001 active peer answers ALONE
// within a grace budget (mixing client-block responses from another peer into a live session may
// cross hardfork/state-view boundaries, and the common case must cost one upstream connection);
// pool candidates are hedged in — each behind its own temporary live relationship — only when the
// active is absent, failed, or slow. Terminal range errors are forwarded to the node as a last
// resort; on total failure the socket closes and the node retries/reconnects.
pub(super) async fn serve_client_blocks(
    down: TcpStream,
    node: Arc<NodeState>,
    priority: Arc<PriorityTier>,
    peers: Vec<String>,
    rr: Arc<AtomicUsize>,
    freshest: Arc<crate::push::FreshestRound>,
) {
    let mut down = down;
    let mut hdr = [0u8; 5];
    if !matches!(
        timeout(Duration::from_secs(120), down.read_exact(&mut hdr)).await,
        Ok(Ok(_))
    ) {
        return;
    }
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    if len > 65536 {
        return;
    }
    let mut payload = vec![0u8; len];
    if !matches!(
        timeout(Duration::from_secs(30), down.read_exact(&mut payload)).await,
        Ok(Ok(_))
    ) {
        return;
    }
    let mut req = hdr.to_vec();
    req.extend_from_slice(&payload);
    let range = client_block_range(&req);

    let prefetched = match range.and_then(|(s, e)| take_prefetch(&node, s, e)) {
        Some(p) => await_prefetch(p, &node).await,
        None => None,
    };
    let fetched = match prefetched {
        Some(frame) => Ok(frame),
        None => {
            // Gateway-wide bound on concurrent fetches: each one can hold up to ~9 upstream
            // attempts with multi-MB responses. A catch-up node sends one request at a time (plus
            // one prefetch), so 8 is far above normal.
            let Ok(Ok(_slot)) =
                timeout(Duration::from_secs(5), CLIENT_BLOCK_FETCH_SLOTS.acquire()).await
            else {
                gwlog!("[gw] [{}] 4002 client-block fetch busy; closing", node.ip);
                return;
            };
            timeout(
                CLIENT_BLOCK_FETCH_DEADLINE,
                fetch_client_blocks_raced(&node, &priority, &peers, &rr, Arc::new(req)),
            )
            .await
            .unwrap_or_else(|_| {
                Err(format!(
                    "deadline {}s",
                    CLIENT_BLOCK_FETCH_DEADLINE.as_secs()
                ))
            })
        }
    };
    match fetched {
        Ok(frame) => {
            // only a batch answering a range query is catch-up: not a tip/terminal error frame
            // (type 0), nor a height query or query_peers
            if let Some((start, end)) = range.filter(|_| frame.get(4) == Some(&1)) {
                let tip = freshest.get();
                node.mark_client_blocks_response(
                    Instant::now(),
                    tip.map(|t| t.saturating_sub(start)),
                );
                // the node's next request, fetched while it processes this batch
                if let Some(next) = tip.and_then(|t| next_prefetch_range(start, end, t)) {
                    start_prefetch(&node, &priority, &peers, &rr, next);
                }
            }
            let _ = timeout(NODE_WRITE_TIMEOUT, down.write_all(&frame)).await;
        }
        Err(failure) => {
            gwlog!(
                "[gw] [{}] 4002 client-block fetch failed via {}",
                node.ip,
                failure
            );
        }
    }
}

// One 4002 request's whole budget: the active grace plus a hedge round normally answer within a
// few seconds (median ~2s, 20MB batches); past this the node is better off retrying.
const CLIENT_BLOCK_FETCH_DEADLINE: Duration = Duration::from_secs(15);

static CLIENT_BLOCK_FETCH_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);

const MAX_CLIENT_BLOCK_RESPONSE: usize = 64_000_000;

static CLIENT_BLOCK_RESPONSE_BYTES: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(512_000_000);

pub(super) fn should_fetch_forward_client_blocks(port: u16) -> bool {
    port == 4002
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::response_is_peer_full;

    #[test]
    fn client_block_fetch_forward_is_always_used_for_4002() {
        assert!(should_fetch_forward_client_blocks(4002));
        assert!(!should_fetch_forward_client_blocks(4001));
        assert!(!should_fetch_forward_client_blocks(4003));
    }

    #[test]
    fn peer_only_response_is_detected_without_parsing_rpc() {
        assert!(response_is_peer_only(br#"{"Error":"Peer-only request"}"#));
        assert!(!response_is_peer_only(br#"{"Ok":{"blocks":[]}}"#));
        // structured form, captured 2026-10-28 from pool peers asked for a range without a 4001
        // relationship (the same peers served the range once one was open)
        assert!(response_is_peer_only(&[0x00, 0x01, 0x00]));
        assert!(!response_is_peer_only(&[0x00, 0x01, 0x00, 0x00]));
        assert!(!response_is_peer_only(&[0x00, 0x01, 0x06]));
    }

    #[test]
    fn peer_full_response_is_detected_without_parsing_rpc() {
        assert!(response_is_peer_full(br#"{"Error":"Peer full"}"#));
        assert!(response_is_peer_full(&[0, 0, 0, 1, 0, 4]));
        assert!(!response_is_peer_full(&[0, 0, 0, 1, 0, 3]));
        assert!(!response_is_peer_full(&[0, 0, 0, 3, 0, 0, 0, 0]));
        assert!(!response_is_peer_full(br#"{"Ok":{"round":1}}"#));
    }

    #[test]
    fn no_client_blocks_response_is_not_treated_as_success() {
        assert!(response_has_no_client_blocks(
            br#"{"Error":"no client blocks to serve"}"#
        ));
        assert!(!response_has_no_client_blocks(br#"{"Ok":{"blocks":[1]}}"#));
    }

    #[test]
    fn client_block_round_too_large_response_is_not_treated_as_success() {
        assert!(response_has_client_block_round_too_large(
            br#"{"Error":"client block round too large: 1354298843 > 769127346"}"#
        ));
        assert!(!response_has_client_block_round_too_large(
            br#"{"Ok":{"blocks":[1]}}"#
        ));
    }

    #[test]
    fn client_block_round_too_small_response_is_not_treated_as_success() {
        assert!(response_has_client_block_round_too_small(
            br#"{"Error":"client block round too small: 1354354141 <= 1354362691"}"#
        ));
        assert!(!response_has_client_block_round_too_small(
            br#"{"Ok":{"blocks":[1]}}"#
        ));
    }

    #[test]
    fn client_block_candidates_put_priority_first() {
        let pool: Vec<String> = ["1.1.1.1", "2.2.2.2", "3.3.3.3"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let rr = AtomicUsize::new(0);
        let out = client_block_peer_candidates(
            vec!["2.2.2.2".to_string(), "9.9.9.9".to_string()],
            &pool,
            &rr,
            3,
        );
        assert_eq!(out, ["2.2.2.2", "9.9.9.9", "1.1.1.1"]);
        let out = client_block_peer_candidates(Vec::new(), &[], &rr, 3);
        assert!(out.is_empty());
    }

    #[test]
    fn a_structured_round_too_large_reply_is_terminal() {
        // captured from a priority peer whose committed store lagged: requested 1502221505,
        // committed tip 1502220797
        let reply = [
            0x00, 0x01, 0x06, 0xfc, 0xc1, 0x14, 0x8a, 0x59, 0xfc, 0xfd, 0x11, 0x8a, 0x59,
        ];
        assert!(response_is_structured_round_too_large(&reply));
        assert!(!response_is_structured_round_too_large(&reply[..12]));
        let mut other_variant = reply;
        other_variant[2] = 0x05;
        assert!(!response_is_structured_round_too_large(&other_variant));
        // a batch starts with its LE uncompressed size
        let batch = [
            0x80, 0x96, 0x98, 0x00, 0x1f, 0x00, 0x01, 0xfc, 0, 0, 0, 0, 0,
        ];
        assert!(!response_is_structured_round_too_large(&batch));
    }

    #[tokio::test]
    async fn a_lagging_peers_instant_error_does_not_beat_the_actives_batch() {
        const ERR: &[u8] = &[
            0x00, 0x01, 0x06, 0xfc, 0xc1, 0x14, 0x8a, 0x59, 0xfc, 0xfd, 0x11, 0x8a, 0x59,
        ];
        let (active, lagging) = ("127.0.0.44", "127.0.0.45");
        // the active answers past its 2 s grace (heavy blocks); the hedged peer errors at once
        let (Some(t1), Some(t2)) = (
            fake_upstream_after(active, Some(true), b"real-batch", 2600).await,
            fake_upstream(lagging, Some(true), ERR).await,
        ) else {
            eprintln!("skipped: loopback 4001/4002 taken");
            return;
        };
        let tier = Arc::new(
            PriorityTier::new(
                vec![active.to_string(), lagging.to_string()],
                0,
                Duration::from_millis(300),
                crate::priority::BootstrapPolicy::Fallback,
            )
            .with_exclusive(true),
        );
        let node = NodeState::new("10.0.0.9".parse().unwrap(), 1);
        node.pin_active(active);
        let rr = AtomicUsize::new(0);
        let frame = fetch_client_blocks_raced(&node, &tier, &[], &rr, Arc::new(vec![1, 2, 3]))
            .await
            .expect("the active's batch is served");
        assert!(frame.ends_with(b"real-batch"));
        for t in t1.into_iter().chain(t2) {
            t.abort();
        }
    }

    #[test]
    fn only_range_queries_count_as_catch_up() {
        // the captured frame from docs/hl-p2p-protocol.md §8a
        let range = [
            0, 0, 0, 11, 0, 0, 0xfc, 0x8e, 0x26, 0x81, 0x50, 0xfc, 0xf1, 0x26, 0x81, 0x50,
        ];
        let (start, end) = (
            u32::from_le_bytes([0x8e, 0x26, 0x81, 0x50]),
            u32::from_le_bytes([0xf1, 0x26, 0x81, 0x50]),
        );
        assert_eq!(client_block_range(&range), Some((start, end)));
        assert_eq!(client_block_range_request(start, end), range);
        let mut other_tag = range;
        other_tag[5] = 1;
        assert_eq!(client_block_range(&other_tag), None);
        assert_eq!(client_block_range(&range[..15]), None);
        assert_eq!(client_block_range(&[0, 0, 0, 1, 0, 2]), None);
    }

    #[test]
    fn the_next_range_is_prefetched_only_well_behind_the_tip() {
        // the node's next 100 rounds, while they end 300+ below the freshest round
        assert_eq!(
            next_prefetch_range(1_000, 1_099, 1_499),
            Some((1_100, 1_199))
        );
        assert_eq!(next_prefetch_range(1_000, 1_099, 1_498), None);
        // malformed or huge ranges, and overflow at the top of u32
        assert_eq!(next_prefetch_range(1_099, 1_000, u32::MAX), None);
        assert_eq!(next_prefetch_range(0, 5_000, u32::MAX), None);
        assert_eq!(next_prefetch_range(u32::MAX - 50, u32::MAX, u32::MAX), None);
        // the margin holds at the top of u32 too
        assert_eq!(
            next_prefetch_range(u32::MAX - 250, u32::MAX - 151, u32::MAX),
            None
        );
    }

    fn test_prefetch(id: u64, generation: u64, at: Instant) -> Prefetch {
        Prefetch {
            id,
            start: 100,
            end: 199,
            generation,
            at,
            task: None,
        }
    }

    // a type-1 batch holding its share of the response byte budget, after `delay_ms`
    fn delayed_batch(delay_ms: u64) -> tokio::task::JoinHandle<Result<BudgetedFrame, String>> {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            let budget = CLIENT_BLOCK_RESPONSE_BYTES.acquire_many(6).await.unwrap();
            Ok(BudgetedFrame {
                bytes: vec![0, 0, 0, 1, 1, 0],
                _budget: budget,
            })
        })
    }

    #[test]
    fn a_prefetch_serves_only_its_range_under_the_same_pin_generation_while_fresh() {
        let now = Instant::now();
        assert!(test_prefetch(1, 3, now).serves(100, 199, 3, now));
        assert!(!test_prefetch(1, 3, now).serves(100, 198, 3, now));
        assert!(!test_prefetch(1, 3, now).serves(100, 199, 4, now));
        let old = now - PREFETCH_MAX_AGE - Duration::from_secs(1);
        assert!(!test_prefetch(1, 3, old).serves(100, 199, 3, now));
    }

    // A started prefetch is installed under the current pin generation, and a re-pin drops it.
    // (The window between reading the generation and installing is closed by the re-check under
    // the slot's lock; it is too narrow to hit reliably from a test.)
    #[tokio::test]
    async fn a_started_prefetch_carries_its_generation_and_a_re_pin_drops_it() {
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        node.pin_active("1.2.3.4");
        let tier = exclusive_tier("127.0.0.1");
        let rr = Arc::new(AtomicUsize::new(0));
        let before = node.pin_generation();
        start_prefetch(&node, &tier, &[], &rr, (100, 199));
        assert!(lock(&node.prefetch)
            .as_ref()
            .is_some_and(|p| p.generation == before));
        node.pin_active("5.6.7.8");
        assert!(lock(&node.prefetch).is_none());
    }

    #[test]
    fn a_new_pin_cancels_a_prefetch_and_an_unpin_keeps_it() {
        let node = NodeState::new("10.0.0.9".parse().unwrap(), 1);
        let pin = node.pin_active("1.2.3.4");
        // the active refusing the fetch as Peer-only unpins it: the hedged batch stays usable
        *lock(&node.prefetch) = Some(test_prefetch(1, node.pin_generation(), Instant::now()));
        node.clear_active_if_current(&pin);
        assert!(take_prefetch(&node, 100, 199).is_some());
        // a new session drops it at once
        *lock(&node.prefetch) = Some(test_prefetch(2, node.pin_generation(), Instant::now()));
        node.pin_active("1.2.3.4");
        assert!(lock(&node.prefetch).is_none());
        // asking for another range takes it out too
        *lock(&node.prefetch) = Some(test_prefetch(3, node.pin_generation(), Instant::now()));
        assert!(take_prefetch(&node, 200, 299).is_none());
        assert!(lock(&node.prefetch).is_none(), "taken out either way");
    }

    // An unused prefetch is dropped on its own (with its batch and byte budget), not only when the
    // node asks again; a later prefetch in the slot is left alone.
    #[tokio::test]
    async fn an_abandoned_prefetch_expires_and_releases_its_batch() {
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        let task = delayed_batch(0);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            task.is_finished(),
            "the batch was fetched and is being held"
        );
        let mut p = test_prefetch(7, node.pin_generation(), Instant::now());
        p.task = Some(task);
        *lock(&node.prefetch) = Some(p);
        expire_prefetch(&Arc::downgrade(&node), 8);
        assert!(lock(&node.prefetch).is_some(), "another id: not this one");
        expire_prefetch(&Arc::downgrade(&node), 7);
        assert!(lock(&node.prefetch).is_none());
    }

    // A re-pin while the node waits on an in-flight prefetch: the old session's batch isn't served.
    #[tokio::test]
    async fn a_re_pin_during_the_wait_discards_the_prefetched_batch() {
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        node.pin_active("1.2.3.4");
        let mut p = test_prefetch(1, node.pin_generation(), Instant::now());
        p.task = Some(delayed_batch(300));
        *lock(&node.prefetch) = Some(p);
        let taken = take_prefetch(&node, 100, 199).expect("valid when taken");
        let n = node.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            n.pin_active("5.6.7.8");
        });
        let t0 = Instant::now();
        assert!(await_prefetch(taken, &node).await.is_none());
        assert!(
            t0.elapsed() < Duration::from_millis(280),
            "the old fetch is abandoned at the re-pin, not awaited"
        );
        // without a re-pin the same wait serves it
        let mut p = test_prefetch(2, node.pin_generation(), Instant::now());
        p.task = Some(delayed_batch(100));
        assert!(await_prefetch(p, &node).await.is_some());
    }

    // End to end: after the node's batch for one range, the gateway asks the upstream for the next
    // range by itself, and the node's request for it is answered from that without a second ask.
    #[tokio::test]
    async fn a_catching_up_node_gets_its_next_range_prefetched() {
        let up = "127.0.0.47";
        let Ok(l) = tokio::net::TcpListener::bind((up, 4002)).await else {
            eprintln!("skipped: loopback 4002 taken");
            return;
        };
        let seen: Arc<std::sync::Mutex<Vec<(u32, u32)>>> = Arc::default();
        let log = seen.clone();
        let upstream = tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                let log = log.clone();
                tokio::spawn(async move {
                    let mut req = [0u8; 16];
                    if s.read_exact(&mut req).await.is_ok() {
                        let r = client_block_range(&req).unwrap();
                        lock(&log).push(r);
                        // a type-1 batch whose payload names the range it answers
                        let mut frame = 11u32.to_be_bytes().to_vec();
                        frame.push(1);
                        frame.extend_from_slice(&req[5..]);
                        let _ = s.write_all(&frame).await;
                    }
                });
            }
        });
        let tier = exclusive_tier(up);
        let node = Arc::new(NodeState::new("10.0.0.9".parse().unwrap(), 1));
        node.pin_active(up);
        let tip = Arc::new(std::sync::atomic::AtomicU32::new(1_000_000));
        let freshest = Arc::new(crate::push::FreshestRound::new(
            100,
            Duration::from_secs(5),
            Duration::from_secs(30),
            tip,
        ));
        let (fa, fb) = (freshest.feed("a"), freshest.feed("b"));
        fa.offer(1_000_000);
        fb.offer(1_000_000);
        let rr = Arc::new(AtomicUsize::new(0));
        let ask = |start: u32| {
            let (node, tier, freshest, rr) =
                (node.clone(), tier.clone(), freshest.clone(), rr.clone());
            async move {
                let gw = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = gw.local_addr().unwrap();
                let serve = tokio::spawn(async move {
                    let (down, _) = gw.accept().await.unwrap();
                    serve_client_blocks(down, node, tier, Vec::new(), rr, freshest).await;
                });
                let mut c = TcpStream::connect(addr).await.unwrap();
                c.write_all(&client_block_range_request(start, start + 99))
                    .await
                    .unwrap();
                let mut out = vec![0u8; 16];
                timeout(Duration::from_secs(10), c.read_exact(&mut out))
                    .await
                    .unwrap()
                    .unwrap();
                serve.await.unwrap();
                out
            }
        };
        let first = ask(990_000).await;
        assert_eq!(
            client_block_range(&[&[0, 0, 0, 11, 0][..], &first[5..]].concat()),
            Some((990_000, 990_099))
        );
        // the gateway asked for the next range on its own
        for _ in 0..50 {
            if lock(&seen).contains(&(990_100, 990_199)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(*lock(&seen), [(990_000, 990_099), (990_100, 990_199)]);
        // ...and the node's request for it is answered from that, not asked again
        let second = ask(990_100).await;
        assert_eq!(&second[7..11], &990_100u32.to_le_bytes());
        tokio::time::sleep(Duration::from_millis(200)).await;
        let seen = lock(&seen).clone();
        assert_eq!(seen.iter().filter(|r| r.0 == 990_100).count(), 1);
        assert!(
            seen.contains(&(990_200, 990_299)),
            "the next one is prefetched"
        );
        drop((fa, fb));
        upstream.abort();
    }

    #[test]
    fn a_pool_active_answers_first_only_while_the_pool_is_allowed() {
        let tier = |exclusive| {
            PriorityTier::new(
                vec!["9.9.9.9".to_string()],
                0,
                Duration::from_millis(300),
                crate::priority::BootstrapPolicy::Fallback,
            )
            .with_exclusive(exclusive)
        };
        let exclusive = tier(true);
        assert!(active_answers_first(&exclusive, "9.9.9.9"));
        assert!(
            !active_answers_first(&exclusive, "1.1.1.1"),
            "a usable priority peer goes first"
        );
        exclusive.mark_failed("9.9.9.9");
        assert!(
            active_answers_first(&exclusive, "1.1.1.1"),
            "every priority peer unusable: the pool active serves"
        );
        assert!(active_answers_first(&tier(false), "1.1.1.1"));
    }

    // A fake upstream on a loopback alias: 4001 answers the greeting (`live_ok`: as serving, else
    // as peer full; None: no 4001 at all) and 4002 answers any request with `tag` as one frame.
    // None if the ports are taken (e.g. a real node on this host owns :4001/:4002).
    async fn fake_upstream(
        ip: &str,
        live_ok: Option<bool>,
        tag: &'static [u8],
    ) -> Option<Vec<tokio::task::JoinHandle<()>>> {
        fake_upstream_after(ip, live_ok, tag, 0).await
    }

    // ...answering 4002 only after `delay_ms`
    async fn fake_upstream_after(
        ip: &str,
        live_ok: Option<bool>,
        tag: &'static [u8],
        delay_ms: u64,
    ) -> Option<Vec<tokio::task::JoinHandle<()>>> {
        let mut tasks = Vec::new();
        if let Some(ok) = live_ok {
            let l = tokio::net::TcpListener::bind((ip, 4001)).await.ok()?;
            tasks.push(tokio::spawn(async move {
                while let Ok((mut s, _)) = l.accept().await {
                    tokio::spawn(async move {
                        let mut greet = [0u8; 8];
                        if s.read_exact(&mut greet).await.is_ok() {
                            let _ = s.write_all(&[0, 0, 0, 1, 0, if ok { 3 } else { 4 }]).await;
                        }
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    });
                }
            }));
        }
        let l = tokio::net::TcpListener::bind((ip, 4002)).await.ok()?;
        tasks.push(tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let mut req = [0u8; 64];
                    if s.read(&mut req).await.unwrap_or(0) > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        let mut frame = (tag.len() as u32).to_be_bytes().to_vec();
                        frame.push(0);
                        frame.extend_from_slice(tag);
                        let _ = s.write_all(&frame).await;
                    }
                    tokio::time::sleep(Duration::from_secs(10)).await;
                });
            }
        }));
        Some(tasks)
    }

    fn exclusive_tier(prio: &str) -> Arc<PriorityTier> {
        Arc::new(
            PriorityTier::new(
                vec![prio.to_string()],
                0,
                Duration::from_millis(300),
                crate::priority::BootstrapPolicy::Fallback,
            )
            .with_exclusive(true),
        )
    }

    #[tokio::test]
    async fn a_pool_active_gets_4002_only_after_the_priority_peers() {
        let (prio, pool) = ("127.0.0.42", "127.0.0.43");
        // the pool active has no 4001 listener: it can only answer over its own session (direct)
        let (Some(t1), Some(t2)) = (
            fake_upstream(prio, Some(true), b"from-prio").await,
            fake_upstream(pool, None, b"from-pool").await,
        ) else {
            eprintln!("skipped: loopback 4001/4002 taken");
            return;
        };
        let node = NodeState::new("10.0.0.9".parse().unwrap(), 1);
        let tier = exclusive_tier(prio);
        node.pin_active(pool);
        let rr = AtomicUsize::new(0);
        let frame = fetch_client_blocks_raced(&node, &tier, &[], &rr, Arc::new(vec![1, 2, 3]))
            .await
            .expect("the priority peer serves");
        assert!(frame.ends_with(b"from-prio"));
        assert_eq!(
            lock(&node.rpc_peer).as_deref(),
            Some(prio),
            "remembered while the pool pin stays"
        );
        // the priority peer stops serving (4001 says full): the pool active still answers, over
        // its own session, once the priority round has failed
        for t in t1 {
            t.abort();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let Some(t1) = fake_upstream(prio, Some(false), b"from-prio").await else {
            eprintln!("skipped: loopback 4001/4002 taken");
            return;
        };
        *lock(&node.rpc_peer) = None;
        let frame = fetch_client_blocks_raced(&node, &tier, &[], &rr, Arc::new(vec![1, 2, 3]))
            .await
            .expect("the deferred pool active serves");
        assert!(frame.ends_with(b"from-pool"));
        for t in t1.into_iter().chain(t2) {
            t.abort();
        }
    }
}
