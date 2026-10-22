use std::collections::{HashMap, HashSet};
use std::env;
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use crate::failover::{serve_failover, FailoverConfig, FailoverCtx};
use crate::peerd::{extract_round_tip, peer_candidates_path, read_node_peers};
use crate::priority::{gwlog, BootstrapPolicy, PrioritySession, PriorityTier};
use crate::protocol::{
    block_round, complete_bootstrap_at_frame_boundary, complete_frame_count,
    finish_header_after_partial, is_plausible_mainnet_round, max_block_round_in_frames,
    read_header_or_timeout, read_live_frame_with_timeout, response_is_peer_full,
    round_lag_from_tip, unix_secs, HeaderRead, ACTIVE_ROUND_MAX_LAG, GREET_FALSE, GREET_TRUE,
    MAX_LIVE_FRAME, MIN_COMPLETE_BOOTSTRAP_BYTES,
};
use crate::push::{
    push_exit_is_upstream_fault, serve_push, splice, splice_with_idle, AbortOnDrop, PushConfig,
    Relayed, MAX_ACTIVE_ROUND_JUMP, NODE_WRITE_TIMEOUT,
};
use crate::util::lock;

// ---- full P2P gateway ----

fn bootstrap_capture_peers(
    node_peer_file: &str,
    live_peers: Vec<String>,
    include_candidates: bool,
) -> Vec<String> {
    if !include_candidates {
        return live_peers;
    }
    let mut seen: HashSet<String> = live_peers.iter().cloned().collect();
    let mut out = live_peers;
    let candidates = peer_candidates_path(node_peer_file);
    for ip in crate::blacklist::filtered(read_node_peers(candidates.to_string_lossy().as_ref())) {
        if seen.insert(ip.clone()) {
            out.push(ip);
        }
    }
    out
}

// The shared bootstrap cache a capture writes into: in-memory blob + timestamp + disk path.
#[derive(Clone)]
struct CacheSink {
    boot_blob: Arc<Mutex<Option<Arc<Vec<u8>>>>>,
    boot_blob_cached_at: Arc<AtomicU64>,
    disk_cache_path: Option<PathBuf>,
}

// A complete bootstrap ends in the live block stream (the capture stops only once the bulk
// transfer has given way to the live trickle), so its tail has decodable block rounds. A capture
// cut off inside the EVM-KV bulk has none, however close to MIN_COMPLETE_BOOTSTRAP_BYTES it is:
// replaying it would fail the node's state load and loop every cooldown.
fn has_live_tail(blob: &[u8]) -> bool {
    max_block_round_in_frames(blob).is_some()
}

fn install_bootstrap_cache(blob: Vec<u8>, source: &str, sink: &CacheSink) {
    if !has_live_tail(&blob) {
        gwlog!(
            "[gw] bootstrap capture from {source} rejected: {} MB but no live-block tail (cut off mid-bulk)",
            blob.len() / 1_000_000
        );
        return;
    }
    let blob = Arc::new(blob);
    gwlog!(
        "[gw] bootstrap captured from {source}: {} MB",
        blob.len() / 1_000_000
    );
    *lock(&sink.boot_blob) = Some(blob.clone());
    sink.boot_blob_cached_at
        .store(unix_secs(), Ordering::Release);
    if let Some(path) = sink.disk_cache_path.clone() {
        tokio::task::spawn_blocking(move || {
            match persist_bootstrap_cache(&path, blob.as_slice()) {
                Ok(()) => {
                    gwlog!("[gw] bootstrap cache persisted to {}", path.display());
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
}

// One bounded write to a node (NODE_WRITE_TIMEOUT); false = the node is gone or not reading.
async fn node_write<W: AsyncWrite + Unpin>(w: &mut W, buf: &[u8]) -> bool {
    matches!(
        timeout(NODE_WRITE_TIMEOUT, w.write_all(buf)).await,
        Ok(Ok(()))
    )
}

// Transparent bootstrap fallback, with a write-through cache tap. This keeps the node's real peer
// relationship intact while opportunistically filling --cache from the same successful bootstrap.
async fn splice_bootstrap_capture(
    down: TcpStream,
    upc: TcpStream,
    first_hdr: [u8; 5],
    first_payload_prefix: Vec<u8>,
    sink: CacheSink,
    capture_permit: tokio::sync::OwnedSemaphorePermit,
    upstream_idle: Duration,
) -> Relayed {
    let up_ip = upc
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = upc.into_split();
    // bytes relayed to the node, and whether the upstream side ended the relay (closed, failed,
    // went silent for `upstream_idle`, or sent a malformed frame) rather than the node
    let delivered = Arc::new(AtomicU64::new(0));
    let upstream_ended = Arc::new(AtomicBool::new(false));
    // aborted on drop too: if this future is cancelled the copy task (and the capture permit it
    // holds) must not live on detached
    let h = AbortOnDrop({
        let delivered = delivered.clone();
        let upstream_ended = upstream_ended.clone();
        let upstream_gone = move || upstream_ended.store(true, Ordering::Release);
        tokio::spawn(async move {
            // Single-flight permit: released as soon as capturing stops (installed, overflowed, or
            // task end/abort) — NOT held for the rest of the relay, which may stream live blocks for
            // hours and would otherwise block every future cache tap.
            let mut capture_permit = Some(capture_permit);
            // Reserve the full capture budget up front: growing a ~4.5GB Vec by doubling re-copies
            // the whole blob at every growth step (~2x the final size in extra memcpy) and briefly
            // holds old+new allocations. The reservation is virtual memory; RSS only grows as pages
            // are actually written.
            let mut blob = Vec::with_capacity(MAX_BOOTSTRAP_CAPTURE_BYTES);
            let mut frames = 0u64;
            let mut capture = true;
            let mut first = Some((first_hdr, first_payload_prefix));
            let mut win = Instant::now();
            let mut win_bytes = 0usize;
            let mut slow = 0u32;
            let source = "transparent fallback";
            // one relay buffer for the whole bootstrap (thousands of frames), not one per frame
            let mut buf = vec![0u8; 1_048_576];
            // A peer that trickles just enough to dodge the idle limit must not keep the
            // single-flight permit (and a growing blob) forever: past this the capture is
            // dropped, the relay goes on.
            let capture_deadline = Instant::now() + MAX_TAP_CAPTURE;
            macro_rules! drop_slow_capture {
                () => {
                    if capture && Instant::now() >= capture_deadline {
                        gwlog!(
                            "[gw] transparent bootstrap cache tap took over {}s; disabling tap",
                            MAX_TAP_CAPTURE.as_secs()
                        );
                        blob = Vec::new();
                        capture = false;
                        capture_permit.take();
                    }
                };
            }

            loop {
                drop_slow_capture!();
                if !capture {
                    capture_permit.take();
                }
                let mut hdr = [0u8; 5];
                let mut payload_prefix = Vec::new();
                if let Some((h, prefix)) = first.take() {
                    hdr = h;
                    payload_prefix = prefix;
                } else if capture && complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                    match read_header_or_timeout(&mut ur, &mut hdr, Duration::from_secs(20)).await {
                        Ok(HeaderRead::Complete) => {}
                        Ok(HeaderRead::TimedOut(off)) => {
                            install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                            capture = false;
                            // bounded: a stalled upstream must not hold this task (and the tap
                            // permit) forever after the 20s header gap
                            if !matches!(
                                timeout(
                                    Duration::from_secs(10),
                                    finish_header_after_partial(&mut ur, &mut hdr, off)
                                )
                                .await,
                                Ok(Ok(()))
                            ) {
                                upstream_gone();
                                break;
                            }
                        }
                        Err(_) => {
                            install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                            upstream_gone();
                            break;
                        }
                    }
                } else if !matches!(
                    timeout(upstream_idle, ur.read_exact(&mut hdr)).await,
                    Ok(Ok(_))
                ) {
                    if capture && complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                        install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                    }
                    upstream_gone();
                    break;
                }

                let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
                if len > MAX_ABCI_STATE_FRAME {
                    upstream_gone();
                    break;
                }
                // checked before appending: the frame alone may be up to 1.5GB
                if capture && blob.len() + 5 + len > MAX_BOOTSTRAP_CAPTURE_BYTES {
                    gwlog!(
                    "[gw] transparent bootstrap cache tap would exceed max capture size; disabling tap"
                );
                    blob = Vec::new();
                    capture = false;
                }
                if !node_write(&mut dw, &hdr).await {
                    break;
                }
                delivered.fetch_add(5, Ordering::Relaxed);
                if capture {
                    blob.extend_from_slice(&hdr);
                }

                let mut remaining = len;
                if !payload_prefix.is_empty() {
                    if payload_prefix.len() > remaining {
                        upstream_gone();
                        break;
                    }
                    if !node_write(&mut dw, &payload_prefix).await {
                        break;
                    }
                    delivered.fetch_add(payload_prefix.len() as u64, Ordering::Relaxed);
                    if capture {
                        blob.extend_from_slice(&payload_prefix);
                    }
                    remaining -= payload_prefix.len();
                }
                while remaining > 0 {
                    let take = remaining.min(buf.len());
                    drop_slow_capture!();
                    let n = match timeout(upstream_idle, ur.read(&mut buf[..take])).await {
                        Ok(Ok(n)) if n > 0 => n,
                        _ => {
                            upstream_gone();
                            return;
                        }
                    };
                    if !node_write(&mut dw, &buf[..n]).await {
                        return;
                    }
                    delivered.fetch_add(n as u64, Ordering::Relaxed);
                    if capture {
                        blob.extend_from_slice(&buf[..n]);
                    }
                    remaining -= n;
                }
                frames += 1;

                if capture {
                    if blob.len() > MAX_BOOTSTRAP_CAPTURE_BYTES {
                        gwlog!(
                        "[gw] transparent bootstrap cache tap exceeded max capture size; disabling tap"
                    );
                        blob = Vec::new();
                        capture = false;
                        continue;
                    }
                    win_bytes += 5 + len;
                    let el = win.elapsed();
                    if el >= Duration::from_millis(1500) {
                        let rate = win_bytes as f64 / el.as_secs_f64();
                        if blob.len() > 1_500_000_000 && rate < 400_000.0 {
                            slow += 1;
                            if slow >= 8 && complete_bootstrap_at_frame_boundary(blob.len(), frames)
                            {
                                install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                                capture = false;
                            }
                        } else {
                            slow = 0;
                        }
                        win = Instant::now();
                        win_bytes = 0;
                    }
                }
            }
        })
    });
    // like splice: a source blacklisted mid-relay ends it (an unfinished capture is discarded).
    // When the upstream side ends the copy task, dropping its node writer shows the node EOF,
    // and the node closing ends the node -> upstream copy here.
    let to_peer = tokio::select! {
        r = tokio::io::copy(&mut dr, &mut uw) => r.unwrap_or(0),
        _ = crate::blacklist::wait_blacklisted(&up_ip) => {
            upstream_ended.store(true, Ordering::Release);
            0
        }
    };
    drop(h);
    Relayed {
        to_node: delivered.load(Ordering::Relaxed),
        to_peer,
        upstream_ended: upstream_ended.load(Ordering::Acquire),
    }
}

const BOOTSTRAP_PREFETCH_BYTES: usize = 64_000_000;
// Largest plausible single bootstrap frame (the abci_state is ~1.3GB in 2026-10).
const MAX_ABCI_STATE_FRAME: usize = 1_500_000_000;
const BOOTSTRAP_PREFETCH_SECS: u64 = 4;
const BOOTSTRAP_SELECT_DEADLINE_SECS: u64 = 24;
const BOOTSTRAP_SELECT_MAX_INFLIGHT: usize = 4;
const BOOTSTRAP_SELECT_MAX_PEERS: usize = 32;

struct BootstrapPeerSelection {
    upc: TcpStream,
    hdr: [u8; 5],
    payload_prefix: Vec<u8>,
    ip: String,
    abci_len: usize,
    // a multi-GB relay holds a connection on the peer: its priority slot (None for pool peers)
    prio_session: Option<PrioritySession>,
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
    } else if e.contains("lagged") {
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
    counts.sort_by(|a, b| b.1.cmp(&a.1));
    let mut out = format!("tried={tried}");
    for (class, n) in counts {
        out.push_str(&format!(" {class}={n}"));
    }
    out
}

fn hex_bytes(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// A peer declining a send_abci greeting answers with a small status frame. Read it (bounded) so
// the log says why: the status code, or text such as "abci state request rate limited".
async fn decline_detail(s: &mut TcpStream, typ: u8, len: usize) -> String {
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
            crate::blacklist::connect(&format!("{ip}:4001")),
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

async fn select_fast_bootstrap_peer(
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
    let mut set = tokio::task::JoinSet::new();
    let mut fails: Vec<String> = Vec::new();

    loop {
        while launched < limit && in_flight < BOOTSTRAP_SELECT_MAX_INFLIGHT {
            let ip = peers[(start + launched) % n].clone();
            // a priority peer is only dialed with a free slot (HYPERSYNC_PRIORITY_MAX_SESSIONS)
            let slot = if priority.contains(&ip) {
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
                try_fast_bootstrap_peer(ip, greet).await.map(|mut s| {
                    s.prio_session = slot;
                    s
                })
            });
            launched += 1;
            in_flight += 1;
        }
        if in_flight == 0 {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, set.join_next()).await {
            Ok(Some(joined)) => {
                in_flight = in_flight.saturating_sub(1);
                match joined {
                    Ok(Ok(selected)) => {
                        set.abort_all();
                        return Some(selected);
                    }
                    Ok(Err(e)) => fails.push(e),
                    Err(e) => fails.push(format!("join: {e}")),
                }
            }
            Ok(None) => break,
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

struct LivePeerSession {
    ip: String,
    stream: TcpStream,
    greeting: Vec<u8>,
    prefetched_frames: Vec<Vec<u8>>,
    first_round: u32,
    // the priority slot taken before dialing (None for pool peers); serve_push_repinned keeps it
    prio_session: Option<PrioritySession>,
}

// Non-block frames read while vetting (control / no-round data) are kept, in order, and handed to
// the push session ahead of the vetted block instead of being dropped; bounded so a peer streaming
// only non-block data can't grow the buffer.
const VET_KEEP_MAX_FRAMES: usize = 64;
const VET_KEEP_MAX_BYTES: usize = 8_000_000;

const LIVE_SELECT_WAIT: Duration = Duration::from_millis(3500);
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
async fn connect_and_greet_4001(
    ip: &str,
    greet: [u8; 8],
    wait: Duration,
) -> std::io::Result<(TcpStream, Vec<u8>)> {
    // "ip" normally; "ip:port" (custom ports, tests on ephemeral ports) is used as given
    let addr = if ip.contains(':') {
        ip.to_string()
    } else {
        format!("{ip}:4001")
    };
    let mut s = timeout(wait, crate::blacklist::connect(&addr)).await??;
    s.set_nodelay(true).ok();
    timeout(wait, s.write_all(&greet)).await??;
    let greeting = read_live_greeting_with_timeout(&mut s, wait).await?;
    if response_is_peer_full(&greeting) {
        return Err(std::io::Error::other("peer full"));
    }
    Ok((s, greeting))
}

async fn open_peer_relationship(ip: &str, wait: Duration) -> std::io::Result<TcpStream> {
    Ok(connect_and_greet_4001(ip, GREET_FALSE, wait).await?.0)
}

const PRIORITY_CAP_REACHED: &str = "priority session cap reached";

// A priority attempt takes its session slot BEFORE dialing (try_acquire), so concurrent
// selections for several nodes can't overrun HYPERSYNC_PRIORITY_MAX_SESSIONS; a full peer counts
// as a failed attempt. The slot travels with the session, or is released when the attempt fails
// or loses the race.
async fn live_attempt(
    priority: Arc<PriorityTier>,
    ip: String,
    is_priority: bool,
    greet: [u8; 8],
    wait: Duration,
    min_round_exclusive: u32,
    net_tip: u32,
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
    let r =
        open_live_peer_session_with_greet(ip.clone(), greet, wait, min_round_exclusive, net_tip)
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
// priority attempt has failed; priority attempts still in flight keep racing alongside it. With no
// priority peers this is the plain pool race.
async fn select_live_peer(
    priority: &Arc<PriorityTier>,
    peers: &[String],
    start: usize,
    greet: [u8; 8],
    min_round_exclusive: u32,
    net_tip: u32,
) -> Option<LivePeerSession> {
    let prio = priority.candidates();
    if peers.is_empty() && prio.is_empty() {
        return None;
    }
    let started = Instant::now();
    let deadline = started + LIVE_SELECT_WAIT;
    let pool_opens = if prio.is_empty() {
        started
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
                    // a full peer is busy with our other nodes, not broken: no cooldown
                    if e.to_string() != PRIORITY_CAP_REACHED {
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
                if Instant::now() >= pool_opens {
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
        priority.mark_failed(ip);
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
    cfg: PushConfig,
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
    if FailoverConfig::get().standbys > 0 {
        // hot-standby session: the active upstream can change mid-session, so the 4002 pin and the
        // priority session accounting follow every switch
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
struct PeerCooldown(Mutex<HashMap<String, Instant>>);

impl PeerCooldown {
    fn mark(&self, ip: &str, now: Instant) {
        let mut m = lock(&self.0);
        m.retain(|_, until| *until > now);
        m.insert(ip.to_string(), now + SESSION_FAULT_COOLDOWN);
    }

    // `peers` minus those cooling down, order kept; all of them if every one is cooling (a pool
    // that only has recently-faulted peers is still better than none).
    fn usable(&self, peers: Vec<String>, now: Instant) -> Vec<String> {
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

fn cool_down_on_fault(priority: &PriorityTier, cooldown: &PeerCooldown, ip: &str, reason: &str) {
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

// Priority candidates first (least-loaded), then the pool rotated from `start`, deduped.
fn tiered_peers(priority: &PriorityTier, pool: &[String], start: usize) -> Vec<String> {
    ordered_candidates(priority.candidates(), pool, start)
}

// hl-node abandons a fresh 4001 connection this long after sending its greeting ("connecting to
// peer" -> "abci_stream ... timed out" is 5.0s in the node log).
const NODE_LIVE_CONNECT_DEADLINE: Duration = Duration::from_secs(5);
// What a failed LIVE_SELECT_WAIT leaves the 4001 fallback, less a margin for relaying the greeting.
const FALLBACK_WAIT: Duration = NODE_LIVE_CONNECT_DEADLINE
    .saturating_sub(LIVE_SELECT_WAIT)
    .saturating_sub(Duration::from_millis(200));
const FALLBACK_MAX_PARALLEL: usize = 8;

// The winner of a fallback race: (ip, connection, its greeting, its priority slot).
type FallbackUpstream = (String, TcpStream, Vec<u8>, Option<PrioritySession>);

// Race connect+greet against the first FALLBACK_MAX_PARALLEL candidates (priority first) and keep
// the first that answers. Sequential dials can't fit the node's remaining budget: one hung peer
// alone would take all of it. Priority peers that error are cooled down.
async fn race_fallback_upstream(
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
        },
    )
    .await;
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ActivePeer {
    ip: String,
    epoch: u64,
}

// Pairs `pin_active` with `clear_active_if_current` regardless of how the caller's scope exits
// (early return, panic) — a pin left dangling after a failed relay can wrongly suppress cache
// replay and route 4002 RPC at a stale peer.
struct ActivePinGuard {
    node: Arc<NodeState>,
    peer: ActivePeer,
}

impl Drop for ActivePinGuard {
    fn drop(&mut self) {
        self.node.clear_active_if_current(&self.peer);
    }
}

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
    // pool) still has candidates: the priority tier.
    let target = match target {
        Some(t) => t,
        None => {
            let t = if peers.is_empty() {
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
    let remembered = lock(&node.rpc_peer)
        .clone()
        .filter(|ip| !crate::blacklist::contains(ip));
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

    // Stage 2: hedge across pool candidates — only reached when the active is absent, failed,
    // or slow. Each candidate opens its own temporary live relationship (prewarm) first.
    for ip in client_block_peer_candidates(priority.candidates(), peers, rr, 8) {
        if first_ip.as_deref() == Some(ip.as_str()) {
            continue;
        }
        let Some(slot) = prewarm_slot(&ip) else {
            continue; // priority peer at its session cap
        };
        let req = req.clone();
        attempts.spawn(async move {
            let _slot = slot;
            let result = fetch_client_blocks_from_peer(ip.clone(), req, true).await.1;
            (None, ip, result)
        });
    }

    let mut terminal_from = String::new();
    let mut too_small_from = String::new();
    while let Some(joined) = attempts.join_next().await {
        let Ok((tag, ip, result)) = joined else {
            continue;
        };
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
async fn serve_client_blocks(
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

// Capture a full bootstrap (abci_state + EVM KVs) VERBATIM from a serving state-server, frame-aligned,
// stopping when the bulk transfer ends (byte rate collapses from the bulk's tens-of-MB/s to the
// live-block trickle). Replayed to a bootstrapping node so it never pulls the snapshot from a peer
// (avoids the per-IP abci_state rate-limit). The node handles the internal abci_state/EVM-KVs/live
// framing itself, so the gateway needn't understand the (undocumented) boundary.
fn bootstrap_capture_err(
    reason: impl std::fmt::Display,
    bytes: usize,
    frames: u64,
    started: Instant,
) -> std::io::Error {
    let elapsed = started.elapsed().as_secs_f64();
    let mb = bytes as f64 / 1_000_000.0;
    let rate = if elapsed > 0.0 { mb / elapsed } else { 0.0 };
    std::io::Error::other(format!(
        "{reason}; read={mb:.1}MB frames={frames} elapsed={elapsed:.1}s avg={rate:.1}MB/s"
    ))
}

const CACHE_REPLAY_COOLDOWN_SECS: u64 = 30;
// How long a cache replay's live floor stays in force (it only covers the replay's backlog).
const CACHE_FLOOR_TTL: Duration = Duration::from_secs(10 * 60);
// hl-node's live/bootstrap reader times out in roughly five minutes once it sees the live
// stream's first block. A very old cache forces too much client-block catch-up before that
// first live round can be processed, so keep the replay window intentionally short.
const MAX_BOOTSTRAP_CACHE_AGE_SECS: u64 = 8 * 60;
const CACHE_REFRESH_SECS: u64 = 4 * 60;
const CACHE_REFRESH_RETRY_SECS: u64 = 30;
const CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS: u64 = 90;
const MIN_BOOTSTRAP_RATE_BYTES_PER_SEC: f64 = 5_000_000.0;
const MIN_LIVE_PEERS_FOR_CACHE_REFRESH: usize = 4;
// Concurrent capture buffers (~5.8GB each in 2026-10). Peak = these + the single-flight fallback
// tap + the current blob (fresh case; a stale blob is freed first): 1 + 1 + 1 fresh, 2 + 1 cold,
// i.e. ~3 bootstraps (~17.4GB) either way, inside compose's 24g mem_limit.
const FRESH_CACHE_REFRESH_MAX_INFLIGHT: usize = 1;
const COLD_CACHE_REFRESH_MAX_INFLIGHT: usize = 2;
// The full bootstrap was ~4.5GB in mid-2026 and 5.8GB by 2026-10; keep headroom so growth does
// not silently stop cache capture. Reserved as virtual memory only (see the capture loop).
#[cfg(not(test))]
const MAX_BOOTSTRAP_CAPTURE_BYTES: usize = 9_000_000_000;
// tests never capture a real bootstrap: don't reserve 9GB of address space on a small CI runner
#[cfg(test)]
const MAX_BOOTSTRAP_CAPTURE_BYTES: usize = 64 << 20;
// capture_bootstrap_raced's rate gates catch slow or stalled peers. Keep fresh refreshes bounded
// so one bad race cannot monopolize the cache loop, but give cold-cache capture enough time for a
// valid multi-GB bootstrap instead of killing it with the short fresh-refresh deadline.
const FRESH_BOOTSTRAP_REFRESH_MAX_SECS: u64 = 5 * 60;
const COLD_BOOTSTRAP_REFRESH_MAX_SECS: u64 = 15 * 60;

fn cache_parent_writable(path: &Path) -> bool {
    let Some(parent) = path.parent() else {
        return false;
    };
    let probe = parent.join(format!(".hypersync-cache-probe-{}", std::process::id()));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = std::fs::remove_file(probe);
            true
        }
        Err(_) => false,
    }
}

fn bootstrap_cache_path(node_peer_file: &str) -> PathBuf {
    if let Ok(path) = env::var("HYPERSYNC_BOOT_CACHE") {
        return PathBuf::from(path);
    }
    let peer_dir_cache = Path::new(node_peer_file)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("bootstrap.cache");
    if cache_parent_writable(&peer_dir_cache) {
        peer_dir_cache
    } else {
        PathBuf::from("/tmp/hypersync-bootstrap.cache")
    }
}

fn is_bootstrap_cache_fresh(now: u64, cached_at: u64) -> bool {
    cached_at != 0 && now.saturating_sub(cached_at) <= MAX_BOOTSTRAP_CACHE_AGE_SECS
}

fn should_delay_cache_refresh_for_live_pool(now: u64, cached_at: u64, live_count: usize) -> bool {
    if live_count >= MIN_LIVE_PEERS_FOR_CACHE_REFRESH {
        return false;
    }
    let cache_age = now.saturating_sub(cached_at);
    let seconds_left = MAX_BOOTSTRAP_CACHE_AGE_SECS.saturating_sub(cache_age);
    seconds_left > CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS
}

fn fresh_cache_retry_delay_secs(now: u64, cached_at: u64) -> u64 {
    let cache_age = now.saturating_sub(cached_at);
    let seconds_left = MAX_BOOTSTRAP_CACHE_AGE_SECS.saturating_sub(cache_age);
    if seconds_left <= CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS {
        1
    } else {
        (seconds_left - CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS).clamp(1, CACHE_REFRESH_RETRY_SECS)
    }
}

fn bootstrap_capture_deadline_secs(have_fresh_cache: bool) -> u64 {
    if have_fresh_cache {
        FRESH_BOOTSTRAP_REFRESH_MAX_SECS
    } else {
        COLD_BOOTSTRAP_REFRESH_MAX_SECS
    }
}

fn load_bootstrap_cache(path: &Path, now: u64) -> std::io::Result<Option<(Vec<u8>, u64)>> {
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let cached_at = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if !is_bootstrap_cache_fresh(now, cached_at) {
        return Err(std::io::Error::other(format!(
            "disk cache too old: {}s",
            now.saturating_sub(cached_at)
        )));
    }
    if meta.len() < MIN_COMPLETE_BOOTSTRAP_BYTES as u64 {
        return Err(std::io::Error::other(format!(
            "disk cache too small: {} bytes",
            meta.len()
        )));
    }
    let blob = std::fs::read(path)?;
    let Some(frames) = complete_frame_count(&blob) else {
        return Err(std::io::Error::other(format!(
            "disk cache has incomplete frame boundary: {} bytes",
            blob.len()
        )));
    };
    if !complete_bootstrap_at_frame_boundary(blob.len(), frames) {
        return Err(std::io::Error::other(format!(
            "disk cache incomplete: {} bytes, {} frames",
            blob.len(),
            frames
        )));
    }
    if !has_live_tail(&blob) {
        return Err(std::io::Error::other(format!(
            "disk cache has no live-block tail (cut off mid-bulk): {} bytes",
            blob.len()
        )));
    }
    Ok(Some((blob, cached_at)))
}

// persist_bootstrap_cache writes `<cache>.tmp<N>` then renames it; a crash or OOM kill in between
// leaves a ~6GB orphan, and the sequence restarts at 0, so higher-numbered ones are never reused.
// No writer is running yet at startup, so every such file is an orphan.
fn remove_orphan_cache_tmps(path: &Path) {
    let (Some(dir), Some(stem)) = (path.parent(), path.file_stem().and_then(|s| s.to_str())) else {
        return;
    };
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let prefix = format!("{stem}.tmp");
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.starts_with(&prefix)
            && name[prefix.len()..].chars().all(|c| c.is_ascii_digit())
            && std::fs::remove_file(entry.path()).is_ok()
        {
            gwlog!("[gw] removed orphaned cache temp file {name}");
        }
    }
}

fn persist_bootstrap_cache(path: &Path, blob: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // unique tmp per writer: the fallback tap and the background refresher can persist
    // concurrently, and a shared tmp name would interleave their writes
    static PERSIST_SEQ: AtomicU64 = AtomicU64::new(0);
    let tmp = path.with_extension(format!(
        "tmp{}",
        PERSIST_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = std::fs::File::create(&tmp)?;
    if let Err(e) = file.write_all(blob).and_then(|_| file.sync_all()) {
        std::fs::remove_file(&tmp).ok();
        return Err(e);
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn cache_replay_allowed(now: u64, last: u64) -> bool {
    last == 0 || now.saturating_sub(last) >= CACHE_REPLAY_COOLDOWN_SECS
}

fn should_replay_cache(now: u64, last: u64, has_active_session: bool, cached_at: u64) -> bool {
    !has_active_session
        && is_bootstrap_cache_fresh(now, cached_at)
        && cache_replay_allowed(now, last)
}

fn should_fetch_forward_client_blocks(port: u16) -> bool {
    port == 4002
}

fn raise_round_floor(floor: &Arc<AtomicU32>, round: u32) -> bool {
    let mut cur = floor.load(Ordering::Acquire);
    while round > cur {
        match floor.compare_exchange(cur, round, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(next) => cur = next,
        }
    }
    false
}

async fn fetch_bootstrap(upstream: &str) -> std::io::Result<Vec<u8>> {
    let mut s = crate::blacklist::connect(upstream).await?;
    s.set_nodelay(true).ok();
    s.write_all(&GREET_TRUE).await?;
    // see splice_bootstrap_capture: reserve up front to avoid re-copying gigabytes on Vec growth
    let mut blob: Vec<u8> = Vec::with_capacity(MAX_BOOTSTRAP_CAPTURE_BYTES);
    let t0 = Instant::now();
    let mut first = true;
    let mut win = Instant::now();
    let mut win_bytes = 0usize;
    let mut slow = 0u32;
    let mut frames = 0u64;
    loop {
        let mut hdr = [0u8; 5];
        match timeout(Duration::from_secs(20), s.read_exact(&mut hdr)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                if complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                    return Ok(blob);
                }
                return Err(bootstrap_capture_err(
                    format!("read header failed: {e}"),
                    blob.len(),
                    frames,
                    t0,
                ));
            }
            Err(_) => {
                if complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                    return Ok(blob);
                }
                return Err(bootstrap_capture_err(
                    "read header timeout",
                    blob.len(),
                    frames,
                    t0,
                ));
            }
        }
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if first {
            if len <= 4_000_000 {
                return Err(bootstrap_capture_err(
                    format!(
                        "not serving abci_state {}",
                        decline_detail(&mut s, hdr[4], len).await
                    ),
                    blob.len(),
                    frames,
                    t0,
                ));
            }
            first = false;
        }
        // Cap a single frame: the only legitimately-large frame is the ~954MB abci_state snapshot;
        // 1.5GB leaves headroom for state growth while rejecting a bogus/huge length that would
        // otherwise force a multi-GB allocation (3 of these race concurrently).
        if len > MAX_ABCI_STATE_FRAME {
            return Err(bootstrap_capture_err(
                format!("frame too large len={len}"),
                blob.len(),
                frames,
                t0,
            ));
        }
        // checked BEFORE allocating/appending: the frame alone may be up to 1.5GB
        if blob.len() + 5 + len > MAX_BOOTSTRAP_CAPTURE_BYTES {
            return Err(bootstrap_capture_err(
                format!("capture too large with next frame len={len}"),
                blob.len(),
                frames,
                t0,
            ));
        }
        let mut payload = vec![0u8; len];
        if len > 50_000_000 {
            // The big frame is the ~954MB abci_state. Read it in chunks and abort early if the
            // sustained rate is too low to ever deliver the full bootstrap before the per-frame
            // deadline. This frees the slot so the race moves to a faster state-server in ~4s
            // instead of blocking ~60s on one slow peer. (The same peers were rejected before via
            // the 60s read_exact timeout — this just rejects them ~15x sooner.)
            let mut off = 0usize;
            let fstart = Instant::now();
            let mut ok = true;
            while off < len {
                match timeout(Duration::from_secs(15), s.read(&mut payload[off..])).await {
                    Ok(Ok(n)) if n > 0 => off += n,
                    _ => {
                        ok = false;
                        break;
                    }
                }
                let el = fstart.elapsed().as_secs_f64();
                // Abort only below the speed gate's floor; 7-9MB/s peers are slow but usable for
                // one-time cache refresh, and rejecting all of them can leave cold cache empty.
                if el > 4.0 && (off as f64 / el) < MIN_BOOTSTRAP_RATE_BYTES_PER_SEC {
                    return Err(bootstrap_capture_err(
                        format!(
                            "state-server too slow early len={len} off={off} rate={:.1}MB/s",
                            (off as f64 / el) / 1_000_000.0
                        ),
                        blob.len(),
                        frames,
                        t0,
                    ));
                }
            }
            if !ok {
                return Err(bootstrap_capture_err(
                    format!("large frame read failed len={len} off={off}"),
                    blob.len(),
                    frames,
                    t0,
                ));
            }
        } else if !matches!(
            timeout(Duration::from_secs(60), s.read_exact(&mut payload)).await,
            Ok(Ok(_))
        ) {
            return Err(bootstrap_capture_err(
                format!("small frame read failed len={len}"),
                blob.len(),
                frames,
                t0,
            ));
        }
        blob.extend_from_slice(&hdr);
        blob.extend_from_slice(&payload);
        frames += 1;
        // total cap: a real bootstrap is ~5.8GB (2026-10); a peer that streams bulk forever (staying above
        // the rate-drop floor) must not inflate the capture unboundedly (x3 concurrent racers).
        if blob.len() > MAX_BOOTSTRAP_CAPTURE_BYTES {
            return Err(bootstrap_capture_err(
                "capture too large",
                blob.len(),
                frames,
                t0,
            ));
        }
        win_bytes += 5 + len;
        // speed gate: right after the abci_state snapshot, require a fast server (else the EVM-KVs
        // capture is slow and the rate-drop detector could mistake a slow tail for the end).
        if blob.len() >= 900_000_000 && blob.len() < 970_000_000 {
            let r = blob.len() as f64 / t0.elapsed().as_secs_f64();
            if r < MIN_BOOTSTRAP_RATE_BYTES_PER_SEC {
                return Err(bootstrap_capture_err(
                    format!(
                        "state-server too slow after abci_state rate={:.1}MB/s",
                        r / 1_000_000.0
                    ),
                    blob.len(),
                    frames,
                    t0,
                ));
            }
        }
        let el = win.elapsed();
        if el >= Duration::from_millis(1500) {
            let rate = win_bytes as f64 / el.as_secs_f64();
            // bulk (abci_state + EVM KVs) ends when the rate collapses to the live-block trickle.
            // Require it SUSTAINED (~12s of <400KB/s) so a mid-EVM-KVs slowdown isn't taken for the end.
            if blob.len() > 1_500_000_000 && rate < 400_000.0 {
                slow += 1;
                if slow >= 8 {
                    if complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                        return Ok(blob);
                    }
                    return Err(bootstrap_capture_err(
                        "bulk ended before minimum usable size",
                        blob.len(),
                        frames,
                        t0,
                    ));
                }
            } else {
                slow = 0;
            }
            win = Instant::now();
            win_bytes = 0;
        }
    }
}

// Race several state-servers for the bootstrap capture instead of trying them one-by-one. A
// rate-limited peer fails in <1s and is replaced immediately; a slow-but-serving peer would
// otherwise block the queue while it downloads ~900MB before the speed gate rejects it, so after
// `hedge` with no winner we add a concurrent attempt to a fresh peer (up to `max_inflight`). The
// first CLEAN capture wins and the rest are aborted, freeing bandwidth and the per-IP abci_state
// quota. Note a full 5.8GB capture outlasts the 40s `hedge` below ~145MB/s, so in practice most
// captures ARE hedged into a second concurrent download (and the hedge timer restarts after every
// failed attempt). Each in-flight attempt grows its own buffer toward the full bootstrap size.
async fn capture_bootstrap_raced(
    peers: Vec<String>,
    max_inflight: usize,
    hedge: Duration,
) -> Option<(Vec<u8>, String)> {
    if peers.is_empty() {
        return None;
    }
    let mut set: tokio::task::JoinSet<(std::io::Result<Vec<u8>>, String)> =
        tokio::task::JoinSet::new();
    let mut idx = 0usize;
    let mut started = 0usize;
    macro_rules! spawn_next {
        () => {{
            let ip = peers[idx].clone();
            idx += 1;
            started += 1;
            let up = format!("{}:4001", ip);
            set.spawn(async move { (fetch_bootstrap(&up).await, ip) });
        }};
    }
    spawn_next!();
    loop {
        if set.is_empty() && idx >= peers.len() {
            gwlog!(
                "[gw] bootstrap race: no usable state-server among {started} peers tried (all too-slow / incomplete / throttled)"
            );
            return None;
        }
        tokio::select! {
            joined = set.join_next(), if !set.is_empty() => {
                match joined {
                    Some(Ok((Ok(blob), ip))) => {
                        gwlog!(
                            "[gw] bootstrap race won by {ip} ({} MB) after {started} attempt(s)",
                            blob.len() / 1_000_000
                        );
                        set.abort_all();
                        return Some((blob, ip));
                    }
                    Some(Ok((Err(e), ip))) => {
                        gwlog!("[gw] bootstrap attempt via {ip} failed: {e}");
                    }
                    Some(Err(e)) => {
                        gwlog!("[gw] bootstrap attempt task failed: {e}");
                    }
                    None => {}
                }
                // failed/short attempt: replace immediately with a fresh candidate
                if idx < peers.len() {
                    spawn_next!();
                }
            }
            _ = tokio::time::sleep(hedge), if set.len() < max_inflight && idx < peers.len() => {
                gwlog!("[gw] bootstrap race: no winner in {}s, hedging attempt #{}", hedge.as_secs(), started + 1);
                spawn_next!();
            }
        }
    }
}

// Per-downstream-node session state, keyed by the node's source IP (each hl-node needs its own
// egress IP as seen by the gateway; NAT'ing two nodes through one IP conflates their sessions).
// State deliberately survives reconnects: nodes restart and come back with the same IP, and the
// replay cooldown / live floor must persist across that.
struct NodeState {
    ip: IpAddr,
    // the upstream peer serving THIS node's bootstrap; all of this node's connections reuse it
    // so client-block RPC (4002) isn't rejected with "Peer-only request".
    active: Mutex<Option<ActivePeer>>,
    next_active_epoch: AtomicU64,
    // Non-zero only after this gateway actually replayed boot_blob to THIS node. The short replay
    // cooldown only prevents duplicate replays from immediate reconnects while allowing normal
    // node restarts to use cache.
    last_cache_replay_secs: AtomicU64,
    // During cache cold-start, the node races ahead through 4002 client-block catch-up while the
    // 4001 live stream may still replay older backlog. Drop live block frames at/below this floor.
    // Arc so PushConfig/pump_merge keep their existing Arc<AtomicU32> shape.
    live_floor: Arc<AtomicU32>,
    last_seen_secs: AtomicU64,
    // connections currently open from this node; a long-lived live stream opens no new
    // connections for hours, so eviction must key on this, not just last_seen
    open_conns: AtomicUsize,
    // true when `active` was pinned lazily by a misc-port dial (no real 4001 session behind it).
    // A cold-starting node's gossip ports can connect before its 4001 bootstrap greet; a lazy pin
    // must not count as "has an active session" or it would suppress the node's cache replay.
    lazy_active: AtomicBool,
    // The upstream that last answered this node's client-block RPC as a hedge after the active
    // stopped serving it (Peer-only, so the pin was cleared while the 4001 session stays up).
    // Tried first, without waiting for a pin, until the next real 4001 session pins a new active;
    // otherwise every later 4002 request pays the pin wait plus the full hedge fan-out.
    rpc_peer: Mutex<Option<String>>,
    // a cache replay to this node is running (see ReplayInFlight)
    replay_in_flight: AtomicBool,
}

impl NodeState {
    fn new(ip: IpAddr, now: u64) -> Self {
        Self {
            ip,
            active: Mutex::new(None),
            next_active_epoch: AtomicU64::new(0),
            last_cache_replay_secs: AtomicU64::new(0),
            live_floor: Arc::new(AtomicU32::new(0)),
            last_seen_secs: AtomicU64::new(now),
            open_conns: AtomicUsize::new(0),
            lazy_active: AtomicBool::new(false),
            rpc_peer: Mutex::new(None),
            replay_in_flight: AtomicBool::new(false),
        }
    }

    // Pin this node's active peer from a real 4001 session (bootstrap or live/resume).
    // `lazy_active` is only ever written while holding the `active` lock (and read either under
    // the lock or after cloning the guard's contents), so readers can never observe a pin whose
    // lazy flag belongs to the previous pin.
    fn pin_active(&self, ip: &str) -> ActivePeer {
        let peer = ActivePeer {
            ip: ip.to_string(),
            epoch: self.next_active_epoch.fetch_add(1, Ordering::AcqRel) + 1,
        };
        let mut active = lock(&self.active);
        *active = Some(peer.clone());
        self.lazy_active.store(false, Ordering::Release);
        *lock(&self.rpc_peer) = None;
        peer
    }

    fn pin_lazy_active(&self, ip: &str) -> ActivePeer {
        let mut active = lock(&self.active);
        unpin_blacklisted(&mut active);
        if let Some(peer) = active.clone() {
            return peer;
        }
        let peer = ActivePeer {
            ip: ip.to_string(),
            epoch: self.next_active_epoch.fetch_add(1, Ordering::AcqRel) + 1,
        };
        *active = Some(peer.clone());
        self.lazy_active.store(true, Ordering::Release);
        peer
    }

    fn active_snapshot(&self) -> Option<ActivePeer> {
        let mut active = lock(&self.active);
        unpin_blacklisted(&mut active);
        active.clone()
    }

    fn active_session_snapshot(&self) -> Option<ActivePeer> {
        let mut active = lock(&self.active);
        unpin_blacklisted(&mut active);
        if self.lazy_active.load(Ordering::Acquire) {
            None
        } else {
            active.clone()
        }
    }

    fn active_is_current(&self, peer: &ActivePeer) -> bool {
        lock(&self.active).as_ref() == Some(peer)
    }

    fn clear_active_if_current(&self, peer: &ActivePeer) {
        let mut active = lock(&self.active);
        if active.as_ref() == Some(peer) {
            *active = None;
        }
    }

    // "This node has a live session" — a lazy misc-port pin doesn't count.
    fn has_active_session(&self) -> bool {
        let mut active = lock(&self.active);
        unpin_blacklisted(&mut active);
        active.is_some() && !self.lazy_active.load(Ordering::Acquire)
    }
}

// A pin to an IP blacklisted after it was set is dropped on sight, so no new connection of the
// node follows it (the session on it is being torn down, see blacklist::wait_blacklisted).
fn unpin_blacklisted(active: &mut Option<ActivePeer>) {
    if active
        .as_ref()
        .is_some_and(|p| crate::blacklist::contains(&p.ip))
    {
        *active = None;
    }
}

// Decrements the node's open-connection count when the connection task ends (however it ends).
struct NodeConnGuard(Arc<NodeState>);

impl Drop for NodeConnGuard {
    fn drop(&mut self) {
        // stamp BEFORE decrementing: an eviction scan between the two must never see
        // open_conns==0 paired with an hours-stale last_seen
        self.0.last_seen_secs.store(unix_secs(), Ordering::Release);
        self.0.open_conns.fetch_sub(1, Ordering::AcqRel);
    }
}

// The gateway binds 0.0.0.0 with no allowlist; scanners must not grow the registry unbounded.
const NODE_STATE_CAP: usize = 256;
// Keep state across node restarts; prune entries idle this long.
const NODE_STATE_TTL_SECS: u64 = 60 * 60;

// Which entries to evict before inserting a new key. Entries with open connections are never
// evicted. Of the rest: everything past the idle TTL goes, then (if still over cap)
// oldest-last_seen first. Tie-break on ip so eviction is deterministic.
fn node_state_evict_keys(
    entries: &[(IpAddr, u64, bool)],
    now: u64,
    cap: usize,
    ttl: u64,
) -> Vec<IpAddr> {
    let mut evict: Vec<IpAddr> = entries
        .iter()
        .filter(|(_, seen, held)| !held && now.saturating_sub(*seen) > ttl)
        .map(|(ip, _, _)| *ip)
        .collect();
    let mut evictable: Vec<(IpAddr, u64)> = entries
        .iter()
        .filter(|(_, seen, held)| !held && now.saturating_sub(*seen) <= ttl)
        .map(|(ip, seen, _)| (*ip, *seen))
        .collect();
    let kept = entries.len() - evict.len();
    if kept + 1 > cap {
        evictable.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let excess = (kept + 1 - cap).min(evictable.len());
        evict.extend(evictable.iter().take(excess).map(|(ip, _)| *ip));
    }
    evict
}

#[derive(Default)]
struct NodeRegistry {
    nodes: Mutex<HashMap<IpAddr, Arc<NodeState>>>,
}

impl NodeRegistry {
    // Returns the node's state plus a guard that holds its open-connection count; keep the guard
    // alive for the life of the connection task.
    fn get_or_insert(&self, ip: IpAddr, now: u64) -> (Arc<NodeState>, NodeConnGuard) {
        let mut nodes = lock(&self.nodes);
        if let Some(state) = nodes.get(&ip) {
            state.last_seen_secs.store(now, Ordering::Release);
            state.open_conns.fetch_add(1, Ordering::AcqRel);
            return (state.clone(), NodeConnGuard(state.clone()));
        }
        let entries: Vec<(IpAddr, u64, bool)> = nodes
            .iter()
            .map(|(ip, s)| {
                (
                    *ip,
                    s.last_seen_secs.load(Ordering::Acquire),
                    s.open_conns.load(Ordering::Acquire) > 0,
                )
            })
            .collect();
        for stale in node_state_evict_keys(&entries, now, NODE_STATE_CAP, NODE_STATE_TTL_SECS) {
            nodes.remove(&stale);
        }
        let state = Arc::new(NodeState::new(ip, now));
        state.open_conns.fetch_add(1, Ordering::AcqRel);
        nodes.insert(ip, state.clone());
        (state.clone(), NodeConnGuard(state))
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
    cache_coldstart: bool,
    push: bool,
}

const STATE_SERVER_MEMORY: usize = 8;
// Longest a transparent bootstrap's cache tap may capture. Headroom over the cache refresh's own
// limits: its 15 min cold deadline, and ~20 min for a whole bootstrap (~5.8GB) at the 5MB/s its
// rate spot-check accepts. (The tap's own sustained-rate detector, 400KB/s, only marks the end of
// the bulk transfer.)
#[cfg(not(test))]
const MAX_TAP_CAPTURE: Duration = Duration::from_secs(30 * 60);
#[cfg(test)]
const MAX_TAP_CAPTURE: Duration = Duration::from_millis(300);
// A 4001 relay (bootstrap transfer, then live blocks) that hears nothing from its upstream this
// long has a stalled peer: end it so the node reconnects elsewhere. Live blocks arrive several
// times a second and the state transfer streams continuously, so this never cuts a healthy relay.
const RELAY_UPSTREAM_IDLE: Duration = Duration::from_secs(60);

fn forget_state_server(list: &Mutex<Vec<String>>, ip: &str) {
    lock(list).retain(|x| x != ip);
}

fn remember_state_server(list: &Mutex<Vec<String>>, ip: &str) {
    let mut l = lock(list);
    l.retain(|x| x != ip);
    l.insert(0, ip.to_string());
    l.truncate(STATE_SERVER_MEMORY);
}

// `head` first (known state servers, or priority peers), then the pool rotated from `start`,
// deduped. A remembered state server that is now rate-limited answers with a tiny status frame
// and fails the attempt fast, so heading the list costs little even when stale.
fn ordered_candidates(head: Vec<String>, pool: &[String], start: usize) -> Vec<String> {
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
fn bootstrap_source_list(
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

// The live-only form of a node's 4001 greeting `[0 0 0 3][0][send_abci][broadcast_group][id]`:
// send_abci cleared, everything else (notably broadcast_group 1 = the mempool/tx feed) kept.
fn live_greeting_for(greet: [u8; 8]) -> [u8; 8] {
    let mut g = greet;
    g[5] = 0;
    g
}

// The live floor a cache replay may set: the blob's tail round, unless the network tip is known
// and the blob claims rounds beyond it (0 = no floor).
fn bounded_cache_floor(cache_floor: u32, net_tip: u32) -> u32 {
    if net_tip != 0 && cache_floor > net_tip.saturating_add(ACTIVE_ROUND_MAX_LAG) {
        0
    } else {
        cache_floor
    }
}

// Held for the duration of one cache replay to a node (set by cache_replay_blob): the replay can
// outlast CACHE_REPLAY_COOLDOWN_SECS, and has_active_session() stays false until the live session
// after it, so without this a second greet mid-replay would start a second full replay.
struct ReplayInFlight(Arc<NodeState>);

impl Drop for ReplayInFlight {
    fn drop(&mut self) {
        self.0.replay_in_flight.store(false, Ordering::Release);
    }
}

// Decide whether THIS bootstrap greet gets the cached blob replayed (fresh cache + no active
// session + replay cooldown passed); logs why a present blob is being withheld otherwise.
fn cache_replay_blob(node: &NodeState, ctx: &GatewayCtx) -> Option<Arc<Vec<u8>>> {
    let now = unix_secs();
    let last = node.last_cache_replay_secs.load(Ordering::Acquire);
    let has_active_session = node.has_active_session();
    let cached_at = ctx.boot_blob_cached_at.load(Ordering::Acquire);
    let blob = lock(&ctx.boot_blob).clone()?;
    if should_replay_cache(now, last, has_active_session, cached_at) {
        if node
            .replay_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Some(blob);
        }
        gwlog!(
            "[gw] [{}] cache replay suppressed: a replay to this node is still running; using transparent fallback",
            node.ip
        );
        return None;
    }
    if has_active_session {
        gwlog!(
            "[gw] [{}] cache replay suppressed: this node has an active session; using transparent fallback",
            node.ip
        );
    } else if !is_bootstrap_cache_fresh(now, cached_at) {
        gwlog!(
            "[gw] [{}] cache replay suppressed: cache age {}s exceeds {}s; using transparent fallback",
            node.ip,
            now.saturating_sub(cached_at),
            MAX_BOOTSTRAP_CACHE_AGE_SECS
        );
    } else {
        gwlog!(
            "[gw] [{}] cache replay suppressed: replayed {}s ago; using transparent fallback",
            node.ip,
            now.saturating_sub(last)
        );
    }
    None
}

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
    for chunk in blob.chunks(8 * 1024 * 1024) {
        if !node_write(&mut down, chunk).await {
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

// Transparent bootstrap relay: pick a peer actually serving the abci_state right now and splice,
// tapping the stream into the cache when --cache holds the single-flight tap permit.
async fn transparent_bootstrap(
    mut down: TcpStream,
    node: Arc<NodeState>,
    peers: Vec<String>,
    greet: [u8; 8],
    ctx: GatewayCtx,
) {
    // the node is getting fresh state from a peer: no earlier cache replay's floor applies
    node.live_floor.store(0, Ordering::Release);
    // Advance rr by the whole scan window: retries during an incident sweep fresh pool peers
    // instead of re-dialing the same ~32 decliners (and their rate limits) shifted by one.
    let start = ctx
        .rr
        .fetch_add(BOOTSTRAP_SELECT_MAX_PEERS, Ordering::Relaxed);
    let remembered = lock(&ctx.state_servers).clone();
    let candidates = bootstrap_source_list(
        ctx.priority.bootstrap,
        ctx.priority.candidates(),
        remembered,
        &peers,
        start,
    );
    let Some(selected) = select_fast_bootstrap_peer(&ctx.priority, &candidates, 0, greet).await
    else {
        gwlog!("[gw] [{}] no peer serving abci_state right now", node.ip);
        return;
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
        ctx.priority.tier_of(&ip),
        abci_len,
        payload_prefix.len() / 1_000_000
    );
    // never remember a priority peer: it would head every later candidate list and turn the
    // fallback policy into prefer
    if !ctx.priority.contains(&ip) {
        remember_state_server(&ctx.state_servers, &ip);
    }
    let active_peer = node.pin_active(&ip);
    let _active_guard = ActivePinGuard {
        node: node.clone(),
        peer: active_peer,
    };
    let tap_permit = if ctx.cache_coldstart {
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
        )
        .await
    } else {
        if ctx.cache_coldstart {
            gwlog!(
                "[gw] [{}] bootstrap capture already in flight; relaying without tap",
                node.ip
            );
        }
        // bounded like every other node write: a node that stops reading must not park this
        // task (and its pin, priority slot and upstream connection) until TCP gives up
        for chunk in [&hdr[..], &payload_prefix[..]] {
            if !matches!(
                timeout(NODE_WRITE_TIMEOUT, down.write_all(chunk)).await,
                Ok(Ok(()))
            ) {
                return;
            }
        }
        // Free the 64MB prefetch before the splice: the bootstrap connection can stay open long
        // after the state transfer, and keeping it in scope held 64MB per open bootstrap
        // connection (2026-10-21 2-node soak: 68MB resident for 25+ min).
        let prefetched = (hdr.len() + payload_prefix.len()) as u64;
        drop(payload_prefix);
        let mut r = splice_with_idle(down, upc, Some(RELAY_UPSTREAM_IDLE)).await;
        r.to_node += prefetched;
        r
    };
    // remembered above so concurrent bootstraps benefit at once; a server whose side ended the
    // relay before the whole bootstrap (abci_state + EVM KVs) was through must not head the next
    // one (a node that left mid-transfer says nothing about the server)
    if relayed.upstream_ended && relayed.to_node < MIN_COMPLETE_BOOTSTRAP_BYTES as u64 {
        forget_state_server(&ctx.state_servers, &ip);
    }
}

// 4001 with a bootstrap greet (send_abci:true). With --cache + a fresh captured snapshot, serve
// it FROM CACHE (no peer state fetch, no rate-limit); then stream live from a pool peer — the
// node catches up via the client-block RPC (4002, fetch-forwarded). Otherwise fall through to a
// transparent relay so the node keeps a real peer relationship.
async fn handle_4001_bootstrap(
    down: TcpStream,
    node: Arc<NodeState>,
    peers: Vec<String>,
    greet: [u8; 8],
    ctx: GatewayCtx,
) {
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
async fn handle_4001_live(
    down: TcpStream,
    node: Arc<NodeState>,
    peers: Vec<String>,
    greet: [u8; 8],
    ctx: GatewayCtx,
) {
    let start = ctx.rr.fetch_add(1, Ordering::Relaxed);
    if ctx.push {
        if let Some(live) = select_live_peer(
            &ctx.priority,
            &peers,
            start,
            greet,
            0,
            ctx.net_round_tip.load(Ordering::Acquire),
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
        net_round_tip,
        state_servers: Arc::new(Mutex::new(Vec::new())),
        priority: priority.clone(),
        pool: pool.clone(),
        cooldown: Arc::new(PeerCooldown::default()),
        cache_coldstart,
        push,
    };

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_replay_cooldown_blocks_immediate_duplicate_replay() {
        let first = 1_000_000;
        let cached_at = first;

        assert!(cache_replay_allowed(first, 0));
        assert!(!cache_replay_allowed(
            first + CACHE_REPLAY_COOLDOWN_SECS - 1,
            first
        ));
        assert!(cache_replay_allowed(
            first + CACHE_REPLAY_COOLDOWN_SECS,
            first
        ));
        assert!(!cache_replay_allowed(first - 1, first));

        assert!(should_replay_cache(first, 0, false, cached_at));
        assert!(!should_replay_cache(first, 0, true, cached_at));
        assert!(!should_replay_cache(first + 1, first, false, cached_at));
        assert!(!should_replay_cache(
            first + MAX_BOOTSTRAP_CACHE_AGE_SECS + 1,
            0,
            false,
            cached_at
        ));
        assert!(is_bootstrap_cache_fresh(
            cached_at + MAX_BOOTSTRAP_CACHE_AGE_SECS,
            cached_at
        ));
        assert!(!is_bootstrap_cache_fresh(cached_at + 15 * 60, cached_at));
    }

    #[test]
    fn low_live_pool_delays_cache_refresh_only_until_near_expiry() {
        let cached_at = 1_000_000;

        assert!(should_delay_cache_refresh_for_live_pool(
            cached_at + CACHE_REFRESH_SECS,
            cached_at,
            MIN_LIVE_PEERS_FOR_CACHE_REFRESH - 1
        ));
        assert!(!should_delay_cache_refresh_for_live_pool(
            cached_at + MAX_BOOTSTRAP_CACHE_AGE_SECS - CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS,
            cached_at,
            MIN_LIVE_PEERS_FOR_CACHE_REFRESH - 1
        ));
        assert!(!should_delay_cache_refresh_for_live_pool(
            cached_at + CACHE_REFRESH_SECS,
            cached_at,
            MIN_LIVE_PEERS_FOR_CACHE_REFRESH
        ));
    }

    #[test]
    fn fresh_cache_retry_delay_does_not_sleep_past_expiry_margin() {
        let cached_at = 1_000_000;

        assert_eq!(
            fresh_cache_retry_delay_secs(cached_at + CACHE_REFRESH_SECS, cached_at),
            CACHE_REFRESH_RETRY_SECS
        );
        assert_eq!(
            fresh_cache_retry_delay_secs(
                cached_at + MAX_BOOTSTRAP_CACHE_AGE_SECS
                    - CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS
                    - 1,
                cached_at
            ),
            1
        );
        assert_eq!(
            fresh_cache_retry_delay_secs(cached_at + MAX_BOOTSTRAP_CACHE_AGE_SECS - 10, cached_at),
            1
        );
    }

    #[test]
    fn cold_capture_deadline_is_not_the_short_fresh_refresh_deadline() {
        assert_eq!(
            bootstrap_capture_deadline_secs(true),
            FRESH_BOOTSTRAP_REFRESH_MAX_SECS
        );
        assert_eq!(
            bootstrap_capture_deadline_secs(false),
            COLD_BOOTSTRAP_REFRESH_MAX_SECS
        );
        assert!(
            bootstrap_capture_deadline_secs(false) > bootstrap_capture_deadline_secs(true),
            "cold cache fill must allow valid multi-GB bootstraps that exceed the fresh refresh cap"
        );
    }

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
    fn orphan_cache_tmps_are_removed_but_nothing_else() {
        let dir = std::env::temp_dir().join(format!("hs-tmp-clean-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = dir.join("bootstrap.cache");
        for f in [
            "bootstrap.tmp0",
            "bootstrap.tmp17",
            "bootstrap.cache",
            "bootstrap.tmpx",
            "peers.json",
        ] {
            std::fs::write(dir.join(f), b"x").unwrap();
        }
        remove_orphan_cache_tmps(&cache);
        let mut left: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        assert_eq!(
            left,
            vec!["bootstrap.cache", "bootstrap.tmpx", "peers.json"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cache_floor_never_outranks_a_known_tip() {
        let tip = 1_490_000_000;
        assert_eq!(bounded_cache_floor(tip - 7_000, tip), tip - 7_000);
        assert_eq!(
            bounded_cache_floor(tip + ACTIVE_ROUND_MAX_LAG, tip),
            tip + ACTIVE_ROUND_MAX_LAG
        );
        assert_eq!(
            bounded_cache_floor(u32::MAX - 5, tip),
            0,
            "fake high tail rounds are ignored"
        );
        assert_eq!(
            bounded_cache_floor(tip, 0),
            tip,
            "unknown tip: kept (cleared after its TTL)"
        );
    }

    #[test]
    fn a_capture_without_a_live_tail_is_not_installed() {
        let sink = CacheSink {
            boot_blob: Arc::new(Mutex::new(None)),
            boot_blob_cached_at: Arc::new(AtomicU64::new(0)),
            disk_cache_path: None,
        };
        // bulk-only frames (type 0 / undecodable) and no block rounds: cut off mid-bulk
        let mut blob = Vec::new();
        for _ in 0..8 {
            blob.extend_from_slice(&1_000u32.to_be_bytes());
            blob.push(1);
            blob.extend(std::iter::repeat_n(0x5a, 1_000));
        }
        assert!(!has_live_tail(&blob));
        install_bootstrap_cache(blob, "test", &sink);
        assert!(lock(&sink.boot_blob).is_none());
        // the same bulk followed by a few live blocks is a complete capture
        let mut complete = Vec::new();
        for r in 0..6u32 {
            let payload = crate::protocol::test_block_payload(1_490_000_000 + r, 3);
            complete.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            complete.push(1);
            complete.extend_from_slice(&payload);
        }
        assert!(has_live_tail(&complete));
    }

    #[test]
    fn raise_round_floor_is_monotonic() {
        let floor = Arc::new(AtomicU32::new(10));

        assert!(!raise_round_floor(&floor, 9));
        assert_eq!(floor.load(Ordering::Acquire), 10);
        assert!(raise_round_floor(&floor, 11));
        assert_eq!(floor.load(Ordering::Acquire), 11);
    }

    #[test]
    fn blacklisted_ips_leave_the_pool_and_the_pins() {
        let _g = crate::blacklist::TEST_LOCK.blocking_lock();
        let dir = std::env::temp_dir().join(format!("hypersync_gwbl_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("peers.json");
        std::fs::write(
            &file,
            r#"{"live_servers":["203.0.113.30","203.0.113.31","203.0.113.32"]}"#,
        )
        .unwrap();
        let path = file.to_str().unwrap();
        assert_eq!(peer_file_dir(path), dir);
        assert_eq!(peer_file_dir("peers.json"), PathBuf::from("."));
        let priority = PriorityTier::new(
            vec!["203.0.113.32".into()],
            0,
            Duration::from_millis(1500),
            BootstrapPolicy::Fallback,
        );
        let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
        node.pin_active("203.0.113.30");
        crate::blacklist::install(&["203.0.113.30"]);
        // priority peers and blacklisted IPs are both out of the pool
        assert_eq!(read_pool(&priority, path), ["203.0.113.31"]);
        // a pin made before the IP was blacklisted is dropped, so no new connection follows it
        assert!(!node.has_active_session());
        assert!(node.active_snapshot().is_none());
        assert_eq!(node.pin_lazy_active("203.0.113.31").ip, "203.0.113.31");
        // a remembered state server blacklisted since is skipped
        assert_eq!(
            ordered_candidates(vec!["203.0.113.30".into()], &["203.0.113.31".into()], 0),
            ["203.0.113.31"]
        );
        crate::blacklist::install(&[]);
        std::fs::remove_dir_all(&dir).ok();
    }

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

    // A tapped bootstrap relay whose upstream goes silent mid-frame ends: the single-flight
    // capture permit is released (it used to be held forever) and the upstream is blamed.
    #[tokio::test]
    async fn silent_upstream_ends_the_tapped_bootstrap_relay_and_frees_the_permit() {
        let up_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_addr = up_l.local_addr().unwrap();
        tokio::spawn(async move {
            let (_s, _) = up_l.accept().await.unwrap();
            // the frame's first bytes came with selection; then nothing
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let node_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_addr = node_l.local_addr().unwrap();
        let node = tokio::spawn(async move {
            let mut c = TcpStream::connect(node_addr).await.unwrap();
            let mut got = Vec::new();
            c.read_to_end(&mut got).await.unwrap();
            got.len()
        });
        let (down, _) = node_l.accept().await.unwrap();
        let upc = TcpStream::connect(up_addr).await.unwrap();
        let tap = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = tap.clone().try_acquire_owned().unwrap();
        let r = timeout(
            Duration::from_secs(5),
            splice_bootstrap_capture(
                down,
                upc,
                [0, 0, 0, 100, 1],
                vec![7u8; 10],
                CacheSink {
                    boot_blob: Arc::new(Mutex::new(None)),
                    boot_blob_cached_at: Arc::new(AtomicU64::new(0)),
                    disk_cache_path: None,
                },
                permit,
                Duration::from_millis(200),
            ),
        )
        .await
        .expect("the relay ends instead of waiting on the silent upstream");
        assert!(r.upstream_ended);
        assert_eq!(r.to_node, 15, "header + prefetched prefix reached the node");
        assert_eq!(node.await.unwrap(), 15);
        assert_eq!(tap.available_permits(), 1, "capture permit released");
    }

    // A state peer that trickles just enough to dodge the idle limit loses the cache tap (its
    // permit is released) once the capture deadline passes, while the node keeps getting bytes.
    #[tokio::test]
    async fn a_trickling_upstream_loses_the_tap_but_keeps_relaying() {
        let up_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_addr = up_l.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = up_l.accept().await.unwrap();
            // one byte every 50ms: never idle for the 200ms limit
            while s.write_all(&[1]).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        let node_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_addr = node_l.local_addr().unwrap();
        let node = tokio::spawn(async move {
            let mut c = TcpStream::connect(node_addr).await.unwrap();
            let mut buf = [0u8; 64];
            let mut got = 0usize;
            let until = Instant::now() + Duration::from_millis(800);
            while Instant::now() < until {
                if let Ok(Ok(n)) = timeout(Duration::from_millis(100), c.read(&mut buf)).await {
                    got += n;
                }
            }
            got // then the node leaves
        });
        let (down, _) = node_l.accept().await.unwrap();
        let upc = TcpStream::connect(up_addr).await.unwrap();
        let tap = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = tap.clone().try_acquire_owned().unwrap();
        let relay = tokio::spawn(splice_bootstrap_capture(
            down,
            upc,
            // a big frame the trickle never finishes
            [0, 0, 100, 0, 1],
            Vec::new(),
            CacheSink {
                boot_blob: Arc::new(Mutex::new(None)),
                boot_blob_cached_at: Arc::new(AtomicU64::new(0)),
                disk_cache_path: None,
            },
            permit,
            Duration::from_millis(200),
        ));
        tokio::time::sleep(MAX_TAP_CAPTURE + Duration::from_millis(250)).await;
        assert_eq!(tap.available_permits(), 1, "tap dropped after its deadline");
        assert!(!relay.is_finished(), "while the relay itself goes on");
        let got = node.await.unwrap();
        assert!(
            got > 5 + 5,
            "the node kept receiving the trickle ({got} bytes)"
        );
        let r = timeout(Duration::from_secs(2), relay)
            .await
            .unwrap()
            .unwrap();
        assert!(!r.upstream_ended, "the node left first");
    }

    #[test]
    fn clear_active_only_clears_matching_epoch() {
        let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
        let first = node.pin_active("1.1.1.1");
        let second = node.pin_active("1.1.1.1");

        node.clear_active_if_current(&first);
        assert_eq!(node.active_snapshot(), Some(second.clone()));

        node.clear_active_if_current(&second);
        assert!(node.active_snapshot().is_none());
    }

    #[test]
    fn active_pin_guard_clears_pin_on_early_drop() {
        let node = Arc::new(NodeState::new("10.0.0.1".parse().unwrap(), 1));
        let peer = node.pin_active("1.1.1.1");
        {
            let _guard = ActivePinGuard {
                node: node.clone(),
                peer,
            };
            assert!(node.has_active_session());
            // guard drops here, as it would on an early `return` out of transparent_bootstrap
        }
        assert!(!node.has_active_session());
    }

    #[test]
    fn active_pin_guard_leaves_a_newer_pin_untouched() {
        let node = Arc::new(NodeState::new("10.0.0.1".parse().unwrap(), 1));
        let stale = node.pin_active("1.1.1.1");
        let guard = ActivePinGuard {
            node: node.clone(),
            peer: stale,
        };
        // a fresh session pins over the guarded one before it drops (e.g. node reconnected)
        let fresh = node.pin_active("2.2.2.2");
        drop(guard);
        assert_eq!(node.active_snapshot(), Some(fresh));
    }

    #[test]
    fn lazy_active_is_not_a_real_4001_session() {
        let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
        let lazy = node.pin_lazy_active("1.1.1.1");

        assert_eq!(node.active_snapshot(), Some(lazy));
        assert!(node.active_session_snapshot().is_none());

        let real = node.pin_active("2.2.2.2");
        assert_eq!(node.active_session_snapshot(), Some(real));
    }

    #[test]
    fn node_state_evict_keys_prunes_ttl_then_oldest() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let now = 10_000u64;

        // under cap, all fresh -> nothing evicted
        let entries = vec![
            (ip("10.0.0.1"), now - 5, false),
            (ip("10.0.0.2"), now, false),
        ];
        assert!(node_state_evict_keys(&entries, now, 4, 3600).is_empty());

        // idle past ttl evicted regardless of cap
        let entries = vec![
            (ip("10.0.0.1"), now - 3601, false),
            (ip("10.0.0.2"), now, false),
        ];
        assert_eq!(
            node_state_evict_keys(&entries, now, 4, 3600),
            vec![ip("10.0.0.1")]
        );

        // at cap with all fresh -> exactly the oldest-last_seen evicted to fit the insert
        let entries = vec![
            (ip("10.0.0.1"), now - 30, false),
            (ip("10.0.0.2"), now - 10, false),
            (ip("10.0.0.3"), now - 20, false),
        ];
        assert_eq!(
            node_state_evict_keys(&entries, now, 3, 3600),
            vec![ip("10.0.0.1")]
        );

        // last_seen tie -> deterministic by ip
        let entries = vec![(ip("10.0.0.9"), now, false), (ip("10.0.0.8"), now, false)];
        assert_eq!(
            node_state_evict_keys(&entries, now, 2, 3600),
            vec![ip("10.0.0.8")]
        );

        // a node with open connections is never evicted: not by ttl (its long-lived live stream
        // opens no new connections for hours) and not by cap pressure
        let entries = vec![
            (ip("10.0.0.1"), now - 7200, true),
            (ip("10.0.0.2"), now - 30, false),
        ];
        assert_eq!(
            node_state_evict_keys(&entries, now, 2, 3600),
            vec![ip("10.0.0.2")]
        );

        // all held and over cap -> nothing evictable; the insert may exceed cap (bounded by the
        // number of concurrently open connections)
        let entries = vec![(ip("10.0.0.1"), now, true), (ip("10.0.0.2"), now, true)];
        assert!(node_state_evict_keys(&entries, now, 2, 3600).is_empty());
    }

    #[test]
    fn node_registry_reuses_state_across_reconnects() {
        let ip_a: IpAddr = "10.0.0.1".parse().unwrap();
        let ip_b: IpAddr = "10.0.0.2".parse().unwrap();
        let reg = NodeRegistry::default();
        // guard Drop stamps wall-clock last_seen, so anchor test times to the wall clock
        let t0 = unix_secs();

        let (a1, a1_guard) = reg.get_or_insert(ip_a, t0);
        let peer = a1.pin_active("1.1.1.1");

        // same ip reconnecting -> same state (active survives), last_seen bumped
        let (a2, a2_guard) = reg.get_or_insert(ip_a, t0 + 10);
        assert!(Arc::ptr_eq(&a1, &a2));
        assert_eq!(a2.active_snapshot(), Some(peer));
        assert_eq!(a2.last_seen_secs.load(Ordering::Acquire), t0 + 10);
        assert_eq!(a2.open_conns.load(Ordering::Acquire), 2);
        drop(a2_guard);
        assert_eq!(a1.open_conns.load(Ordering::Acquire), 1);

        // different ip -> fresh state
        let (b, b_guard) = reg.get_or_insert(ip_b, t0);
        assert!(!Arc::ptr_eq(&a1, &b));
        assert!(lock(&b.active).is_none());
        drop(b_guard); // stamps last_seen ~t0; ip_b now idle with no open connections

        // ip_a still has an open connection (a1_guard) -> survives TTL eviction; ip_b doesn't
        let later = t0 + NODE_STATE_TTL_SECS + 60;
        let ip_c: IpAddr = "10.0.0.3".parse().unwrap();
        let (_c, _c_guard) = reg.get_or_insert(ip_c, later);
        assert!(lock(&reg.nodes).contains_key(&ip_a));
        assert!(!lock(&reg.nodes).contains_key(&ip_b));
        assert!(lock(&reg.nodes).contains_key(&ip_c));
        drop(a1_guard);
    }

    #[test]
    fn cache_live_greeting_keeps_the_nodes_broadcast_group() {
        assert_eq!(live_greeting_for(GREET_TRUE), GREET_FALSE);
        assert_eq!(
            live_greeting_for([0, 0, 0, 3, 0, 1, 1, 0]),
            [0, 0, 0, 3, 0, 0, 1, 0]
        );
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
                        FakePeer::Healthy => {
                            let _ = s.write_all(&[0, 0, 0, 1, 0, 3]).await;
                            let _ = s.write_all(&live_block_frame(1_400_000_000)).await;
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

    #[tokio::test]
    async fn live_selection_prefers_priority_then_falls_back_to_pool() {
        let (p1, h1) = fake_live_peer(FakePeer::Healthy).await;
        let (o1, h2) = fake_live_peer(FakePeer::Healthy).await;
        let (slow, h3) = fake_live_peer(FakePeer::Silent).await;
        let refused = refused_addr().await;
        let pool = vec![o1.clone()];

        // healthy priority peer wins even though the pool is healthy too
        let tier = test_tier(&[&p1], 300);
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0)
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
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0)
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
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0)
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
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0)
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
        let s = select_live_peer(&tier, &pool, 0, GREET_FALSE, 0, 0)
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
        assert!(select_live_peer(&tier, &[], 0, GREET_FALSE, 0, 0)
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
            "lagged=1",
        ] {
            assert!(s.contains(part), "missing {part} in {s}");
        }
    }
}
