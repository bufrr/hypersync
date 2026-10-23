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

pub(crate) fn response_is_peer_only(payload: &[u8]) -> bool {
    payload
        .windows(b"Peer-only request".len())
        .any(|w| w == b"Peer-only request")
}

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
        let terminal =
            response_has_no_client_blocks(rp) || response_has_client_block_round_too_large(rp);
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
    // a remembered hedge winner (see NodeState::rpc_peer) stands in for a missing pin right away
    // (a remembered pool peer only while the pool may serve this node at all)
    let remembered = lock(&node.rpc_peer)
        .clone()
        .filter(|ip| !crate::blacklist::contains(ip))
        .filter(|ip| priority.contains(ip) || priority.pool_allowed());
    let active = if remembered.is_some() {
        node.active_session_snapshot()
    } else {
        wait_active_session(node, ACTIVE_4002_PIN_WAIT).await
    };
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
    let mut terminal_from = String::new();
    let mut too_small_from = String::new();
    loop {
        // the priority peers hedged in this round that haven't answered yet
        let mut prio_pending: Vec<String> = Vec::new();
        for ip in round {
            if first_ip.as_deref() == Some(ip.as_str()) {
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
                        // hung for this request: cool them down, like a hung live selection
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
                if tag.is_none() && node.active_session_snapshot().is_none() {
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
        if pool_hedged || terminal.is_some() || too_small.is_some() || peers.is_empty() {
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

    // Gateway-wide bound on concurrent fetches: each one can hold up to ~9 upstream attempts with
    // multi-MB responses. A catch-up node sends one request at a time, so 8 is far above normal.
    let Ok(Ok(_slot)) = timeout(Duration::from_secs(5), CLIENT_BLOCK_FETCH_SLOTS.acquire()).await
    else {
        gwlog!("[gw] [{}] 4002 client-block fetch busy; closing", node.ip);
        return;
    };
    let fetched = timeout(
        CLIENT_BLOCK_FETCH_DEADLINE,
        fetch_client_blocks_raced(&node, &priority, &peers, &rr, Arc::new(req)),
    )
    .await
    .unwrap_or_else(|_| {
        Err(format!(
            "deadline {}s",
            CLIENT_BLOCK_FETCH_DEADLINE.as_secs()
        ))
    });
    match fetched {
        Ok(frame) => {
            node.mark_client_blocks_response(Instant::now());
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
}
