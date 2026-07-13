use std::collections::{HashMap, HashSet};
use std::env;
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use crate::peerd::{extract_round_tip, peer_candidates_path, read_node_peers};
use crate::protocol::{
    block_round, complete_bootstrap_at_frame_boundary, complete_frame_count,
    finish_header_after_partial, is_plausible_mainnet_round, max_block_round_in_frames,
    read_header_or_timeout, unix_secs, HeaderRead, GREET_FALSE, GREET_TRUE,
};
use crate::push::{serve_push, splice, PushConfig};

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
    for ip in read_node_peers(candidates.to_string_lossy().as_ref()) {
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

fn install_bootstrap_cache(blob: Vec<u8>, source: &str, sink: &CacheSink) {
    let blob = Arc::new(blob);
    eprintln!(
        "[gw] bootstrap captured from {source}: {} MB",
        blob.len() / 1_000_000
    );
    *sink.boot_blob.lock().unwrap() = Some(blob.clone());
    sink.boot_blob_cached_at
        .store(unix_secs(), Ordering::Release);
    if let Some(path) = sink.disk_cache_path.clone() {
        tokio::task::spawn_blocking(move || {
            match persist_bootstrap_cache(&path, blob.as_slice()) {
                Ok(()) => {
                    eprintln!("[gw] bootstrap cache persisted to {}", path.display());
                }
                Err(e) => {
                    eprintln!(
                        "[gw] bootstrap cache persist failed at {}: {e}",
                        path.display()
                    );
                }
            }
        });
    }
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
) {
    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = upc.into_split();
    let h = tokio::spawn(async move {
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

        loop {
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
                        if finish_header_after_partial(&mut ur, &mut hdr, off)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => {
                        install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                        break;
                    }
                }
            } else if ur.read_exact(&mut hdr).await.is_err() {
                if capture && complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                    install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                }
                break;
            }

            let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
            if len > 1_500_000_000 {
                break;
            }
            if dw.write_all(&hdr).await.is_err() {
                break;
            }
            if capture {
                blob.extend_from_slice(&hdr);
            }

            let mut remaining = len;
            let mut buf = vec![0u8; 1_048_576.min(len.max(1))];
            if !payload_prefix.is_empty() {
                if payload_prefix.len() > remaining {
                    break;
                }
                if dw.write_all(&payload_prefix).await.is_err() {
                    break;
                }
                if capture {
                    blob.extend_from_slice(&payload_prefix);
                }
                remaining -= payload_prefix.len();
            }
            while remaining > 0 {
                let take = remaining.min(buf.len());
                let n = match ur.read(&mut buf[..take]).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                if dw.write_all(&buf[..n]).await.is_err() {
                    return;
                }
                if capture {
                    blob.extend_from_slice(&buf[..n]);
                }
                remaining -= n;
            }
            frames += 1;

            if capture {
                if blob.len() > MAX_BOOTSTRAP_CAPTURE_BYTES {
                    eprintln!(
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
                        if slow >= 8 && complete_bootstrap_at_frame_boundary(blob.len(), frames) {
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
    });
    let _ = tokio::io::copy(&mut dr, &mut uw).await;
    h.abort();
}

const BOOTSTRAP_PREFETCH_BYTES: usize = 64_000_000;
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

async fn try_fast_bootstrap_peer(
    ip: String,
    greet: [u8; 8],
) -> Result<BootstrapPeerSelection, String> {
    let mut upc = stage(
        timeout(
            Duration::from_secs(5),
            TcpStream::connect(format!("{ip}:4001")),
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
        return Err(format!("not serving abci_state (len={len})"));
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
    })
}

async fn select_fast_bootstrap_peer(
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
            set.spawn(try_fast_bootstrap_peer(ip, greet));
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
    eprintln!(
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

async fn read_live_frame_with_timeout(
    s: &mut TcpStream,
    wait: Duration,
    max_len: usize,
) -> std::io::Result<Vec<u8>> {
    let mut hdr = [0u8; 5];
    timeout(wait, s.read_exact(&mut hdr)).await??;
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    if len > max_len {
        return Err(std::io::Error::other("live frame too large"));
    }
    let mut payload = vec![0u8; len];
    timeout(wait, s.read_exact(&mut payload)).await??;
    let mut frame = hdr.to_vec();
    frame.extend_from_slice(&payload);
    Ok(frame)
}

struct LivePeerSession {
    ip: String,
    stream: TcpStream,
    greeting: Vec<u8>,
    prefetched_frames: Vec<Vec<u8>>,
    first_round: u32,
}

const LIVE_SELECT_WAIT: Duration = Duration::from_millis(3500);
const LIVE_SELECT_MAX_PARALLEL: usize = 4;

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
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(std::io::Error::other("no live block before deadline"));
            }
            let frame = read_live_frame_with_timeout(&mut s, remaining, 8_000_000).await?;
            let round = if frame.len() >= 5 && frame[4] == 1 {
                block_round(&frame[5..])
            } else {
                None
            };
            if let Some(round) = round {
                if is_plausible_mainnet_round(round, net_tip) && round > min_round_exclusive {
                    return Ok::<_, std::io::Error>((s, greeting, vec![frame], round));
                }
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
    })
}

// Connect to a peer's 4001, send `greet`, and read+vet its greeting (peer-full rejected).
// The single shared prefix for every live-relationship opener.
async fn connect_and_greet_4001(
    ip: &str,
    greet: [u8; 8],
    wait: Duration,
) -> std::io::Result<(TcpStream, Vec<u8>)> {
    let mut s = timeout(wait, TcpStream::connect(format!("{ip}:4001"))).await??;
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

// Bounded by ONE LIVE_SELECT_WAIT total: hl-node abandons a fresh 4001 connection ~5s after
// sending its greeting, so the node is only saved by a greeting that arrives inside that window.
// Scanning the whole pool in batches (up to ~70-90s on a bad pool) just burns the node's deadline
// against an already-dead downstream — the first vetted peer wins, and on a dry scan the caller's
// fallback still has time to greet the node.
async fn select_live_peer(
    peers: &[String],
    start: usize,
    greet: [u8; 8],
    min_round_exclusive: u32,
    net_tip: u32,
) -> Option<LivePeerSession> {
    if peers.is_empty() {
        return None;
    }
    let started = Instant::now();
    let deadline = started + LIVE_SELECT_WAIT;
    let mut set = tokio::task::JoinSet::new();
    let mut launched = 0usize;
    let mut fails: Vec<String> = Vec::new();
    loop {
        while launched < peers.len() && set.len() < LIVE_SELECT_MAX_PARALLEL {
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break;
            }
            let ip = peers[(start + launched) % peers.len()].clone();
            set.spawn(open_live_peer_session_with_greet(
                ip,
                greet,
                wait,
                min_round_exclusive,
                net_tip,
            ));
            launched += 1;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if set.is_empty() || remaining.is_zero() {
            break;
        }
        match timeout(remaining, set.join_next()).await {
            Ok(Some(Ok(Ok(session)))) => {
                set.abort_all();
                return Some(session);
            }
            Ok(Some(Ok(Err(e)))) => fails.push(e.to_string()),
            Ok(Some(Err(e))) => fails.push(format!("join: {e}")),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    set.abort_all();
    eprintln!(
        "[gw] live selection: no vetted peer in {:.1}s ({})",
        started.elapsed().as_secs_f64(),
        summarize_peer_errors(launched, &fails)
    );
    None
}

// Run a 4001 push session with the node's 4002 RPC pinned to the same upstream: pin the active
// peer for the session's lifetime, run serve_push, log the exit, clear the pin. Shared by every
// push-serving path (cache-live, live, fallback).
async fn serve_push_repinned(
    down: TcpStream,
    upc: TcpStream,
    node: &Arc<NodeState>,
    mode: &str,
    cfg: PushConfig,
) {
    let active_ip = cfg.hosts[cfg.active_idx].clone();
    let active_peer = node.pin_active(&active_ip);
    let exit_reason = serve_push(down, upc, cfg).await;
    eprintln!(
        "[gw] [{}] 4001 push session ended active_peer={} mode={} reason={}",
        node.ip, active_ip, mode, exit_reason
    );
    node.clear_active_if_current(&active_peer);
}

// The push host set: the vetted live peer first (active backbone), padded with other pool peers
// up to the live-upstream budget for the (currently disabled) shadow block sources.
fn build_push_hosts(live_ip: &str, peers: &[String], n_live: usize) -> Vec<String> {
    let mut hosts = vec![live_ip.to_string()];
    for p in peers {
        if hosts.len() >= n_live {
            break;
        }
        if p != live_ip {
            hosts.push(p.clone());
        }
    }
    hosts
}

// 4001 fallback when live-peer selection fails: first reachable pool peer, served through
// serve_push so the node's live floor / forward gate / round plausibility still apply (a raw
// splice here would forward ungated frames right after a cache replay — the one moment that
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
    net_round_tip: Arc<AtomicU32>,
    reason: &str,
) {
    for k in 0..peers.len() {
        let ip = peers[(start + k) % peers.len()].clone();
        // Short per-peer budget: the node abandons the connection ~5s after greeting and the
        // failed live selection already spent most of that; one dead candidate at 5s here would
        // guarantee the node times out. Pool peers are live-probed, so 1.5s is plenty.
        let Ok((upc, greeting)) =
            connect_and_greet_4001(&ip, greet, Duration::from_millis(1500)).await
        else {
            continue;
        };
        eprintln!(
            "[gw] [{}] 4001 push fallback via {} ({reason})",
            node.ip, ip
        );
        serve_push_repinned(
            down,
            upc,
            &node,
            "fallback",
            PushConfig {
                hosts: Arc::new(vec![ip.clone()]),
                active_idx: 0,
                port: 4001,
                prefetched_greeting: Some(greeting),
                prefetched_frames: Vec::new(),
                initial_last_forwarded,
                live_floor: node.live_floor.clone(),
                net_round_tip,
            },
        )
        .await;
        return;
    }
    eprintln!("[gw] [{}] 4001 push fallback failed ({reason})", node.ip);
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
    peers: &[String],
    rr: &Arc<AtomicUsize>,
    port: u16,
) -> Option<TcpStream> {
    let n = peers.len();
    if n == 0 {
        return None;
    }
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
    let target = match target {
        Some(t) => t,
        None => {
            let t = peers[rr.fetch_add(1, Ordering::Relaxed) % n].clone();
            node.pin_lazy_active(&t)
        }
    };
    let up = format!("{}:{}", target.ip, port);
    match timeout(Duration::from_secs(5), TcpStream::connect(&up)).await {
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

fn response_is_peer_full(payload: &[u8]) -> bool {
    // On the live 4001 greeting path, upstream refusal is often encoded as a compact binary
    // status frame. 0x03 is the normal accepted greeting before live block frames; 0x04 is the
    // observed peer-full refusal and must not be forwarded as a usable live greeting.
    if payload.len() == 6 && payload[..4] == [0, 0, 0, 1] && payload[4] == 0 {
        return payload[5] == 4;
    }
    payload
        .windows(b"Peer full".len())
        .any(|w| w == b"Peer full")
}

fn response_has_no_client_blocks(payload: &[u8]) -> bool {
    payload
        .windows(b"no client blocks to serve".len())
        .any(|w| w == b"no client blocks to serve")
}

fn response_has_client_block_round_too_large(payload: &[u8]) -> bool {
    payload
        .windows(b"client block round too large".len())
        .any(|w| w == b"client block round too large")
}

fn response_has_client_block_round_too_small(payload: &[u8]) -> bool {
    payload
        .windows(b"client block round too small".len())
        .any(|w| w == b"client block round too small")
}

fn client_block_peer_candidates(peers: &[String], rr: &AtomicUsize, limit: usize) -> Vec<String> {
    if peers.is_empty() || limit == 0 {
        return Vec::new();
    }
    let start = rr.fetch_add(limit.max(1), Ordering::Relaxed);
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for k in 0..peers.len() {
        if out.len() >= limit {
            break;
        }
        let ip = peers[(start + k) % peers.len()].clone();
        if seen.insert(ip.clone()) {
            out.push(ip);
        }
    }
    out
}

enum ClientBlockUpstreamResponse {
    Frames(Vec<u8>),
    PeerOnly,
    RangeTooSmall,
    TerminalError(Vec<u8>),
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
            TcpStream::connect(format!("{ip}:4002")),
        )
        .await??;
        upc.set_nodelay(true).ok();
        timeout(Duration::from_secs(4), upc.write_all(req.as_slice())).await??;

        let mut rh = [0u8; 5];
        timeout(Duration::from_secs(6), upc.read_exact(&mut rh)).await??;
        let rl = u32::from_be_bytes([rh[0], rh[1], rh[2], rh[3]]) as usize;
        // A client-block batch is a handful of blocks (~100 rounds/request); cap well above that
        // but far below a memory hazard.
        if rl > 64_000_000 {
            return Err(std::io::Error::other("oversized client-block response"));
        }
        let mut rp = vec![0u8; rl];
        timeout(Duration::from_secs(12), upc.read_exact(&mut rp)).await??;
        let mut frame = rh.to_vec();
        frame.extend_from_slice(&rp);
        if response_is_peer_only(&rp) {
            return Ok(ClientBlockUpstreamResponse::PeerOnly);
        }
        if response_has_no_client_blocks(&rp) {
            return Ok(ClientBlockUpstreamResponse::TerminalError(frame));
        }
        if response_has_client_block_round_too_large(&rp) {
            return Ok(ClientBlockUpstreamResponse::TerminalError(frame));
        }
        if response_has_client_block_round_too_small(&rp) {
            return Ok(ClientBlockUpstreamResponse::RangeTooSmall);
        }
        Ok(ClientBlockUpstreamResponse::Frames(frame))
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
    result: std::io::Result<ClientBlockUpstreamResponse>,
    failures: &mut Vec<String>,
    terminal: &mut Option<Vec<u8>>,
) -> Option<Vec<u8>> {
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
            failures.push("terminal".to_string());
        }
        Ok(ClientBlockUpstreamResponse::PeerOnly) => {
            if let Some(peer) = tag {
                // the pinned peer refuses client-block RPC: unpin it so the node's next 4001
                // reconnect elects a fresh active instead of re-hitting the same peer forever
                node.clear_active_if_current(peer);
                failures.push(format!("{}: peer-only (active unpinned)", peer.ip));
            } else {
                failures.push("peer-only".to_string());
            }
        }
        Ok(ClientBlockUpstreamResponse::RangeTooSmall) => {
            failures.push("range-too-small".to_string());
        }
        Err(e) => {
            failures.push(e.to_string());
        }
    }
    None
}

async fn fetch_client_blocks_raced(
    node: &NodeState,
    peers: &[String],
    rr: &AtomicUsize,
    req: Arc<Vec<u8>>,
) -> Result<Vec<u8>, String> {
    let active = wait_active_session(node, ACTIVE_4002_PIN_WAIT).await;
    let mut attempts = tokio::task::JoinSet::new();
    let mut failures = Vec::new();
    let mut terminal = None;

    // Stage 1: the pinned active ALONE within a grace budget (one upstream connection in the
    // common case; no candidate can outrace it with a cross-peer response).
    if let Some(peer) = active.clone() {
        let req_for_active = req.clone();
        let ip = peer.ip.clone();
        let tag = peer.clone();
        attempts.spawn(async move {
            (
                Some(tag),
                fetch_client_blocks_from_peer(ip, req_for_active, false)
                    .await
                    .1,
            )
        });
        match timeout(ACTIVE_4002_GRACE, attempts.join_next()).await {
            Ok(Some(Ok((tag, result)))) => {
                if let Some(frame) = note_client_block_result(
                    node,
                    tag.as_ref(),
                    result,
                    &mut failures,
                    &mut terminal,
                ) {
                    return Ok(frame);
                }
            }
            Ok(_) => {}
            // grace expired: the active attempt stays in the set and keeps racing below
            Err(_) => {}
        }
    }

    // Stage 2: hedge across pool candidates — only reached when the active is absent, failed,
    // or slow. Each candidate opens its own temporary live relationship (prewarm) first.
    for ip in client_block_peer_candidates(peers, rr, 8) {
        if active.as_ref().is_some_and(|peer| peer.ip == ip) {
            continue;
        }
        let req = req.clone();
        attempts.spawn(async move {
            let result = fetch_client_blocks_from_peer(ip, req, true).await.1;
            (None, result)
        });
    }

    while let Some(joined) = attempts.join_next().await {
        let Ok((tag, result)) = joined else {
            continue;
        };
        if let Some(frame) =
            note_client_block_result(node, tag.as_ref(), result, &mut failures, &mut terminal)
        {
            attempts.abort_all();
            return Ok(frame);
        }
    }
    if let Some(frame) = terminal {
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

    match fetch_client_blocks_raced(&node, &peers, &rr, Arc::new(req)).await {
        Ok(frame) => {
            let _ = down.write_all(&frame).await;
        }
        Err(failure) => {
            eprintln!(
                "[gw] [{}] 4002 client-block fetch failed via {}",
                node.ip, failure
            );
        }
    }
}

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

pub(crate) const MIN_COMPLETE_BOOTSTRAP_BYTES: usize = 4_400_000_000;
pub(crate) const MIN_COMPLETE_BOOTSTRAP_FRAMES: u64 = 4_000;
const CACHE_REPLAY_COOLDOWN_SECS: u64 = 30;
// hl-node's live/bootstrap reader times out in roughly five minutes once it sees the live
// stream's first block. A very old cache forces too much client-block catch-up before that
// first live round can be processed, so keep the replay window intentionally short.
const MAX_BOOTSTRAP_CACHE_AGE_SECS: u64 = 8 * 60;
const CACHE_REFRESH_SECS: u64 = 4 * 60;
const CACHE_REFRESH_RETRY_SECS: u64 = 30;
const CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS: u64 = 90;
const MIN_BOOTSTRAP_RATE_BYTES_PER_SEC: f64 = 5_000_000.0;
const MIN_LIVE_PEERS_FOR_CACHE_REFRESH: usize = 4;
const FRESH_CACHE_REFRESH_MAX_INFLIGHT: usize = 2;
const COLD_CACHE_REFRESH_MAX_INFLIGHT: usize = 3;
const MAX_BOOTSTRAP_CAPTURE_BYTES: usize = 6_000_000_000;
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
        (seconds_left - CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS)
            .min(CACHE_REFRESH_RETRY_SECS)
            .max(1)
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
    Ok(Some((blob, cached_at)))
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
    let mut s = TcpStream::connect(upstream).await?;
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
                    format!("not serving abci_state first_len={len} type={}", hdr[4]),
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
        if len > 1_500_000_000 {
            return Err(bootstrap_capture_err(
                format!("frame too large len={len}"),
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
        // total cap: a real bootstrap is ~4.5GB; a peer that streams bulk forever (staying above
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
// quota. `hedge` is set above the typical fast-capture time, so the common case runs solo (no
// contention) and only the slow tail is hedged.
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
            eprintln!(
                "[gw] bootstrap race: no usable state-server among {started} peers tried (all too-slow / incomplete / throttled)"
            );
            return None;
        }
        tokio::select! {
            joined = set.join_next(), if !set.is_empty() => {
                match joined {
                    Some(Ok((Ok(blob), ip))) => {
                        eprintln!(
                            "[gw] bootstrap race won by {ip} ({} MB) after {started} attempt(s)",
                            blob.len() / 1_000_000
                        );
                        set.abort_all();
                        return Some((blob, ip));
                    }
                    Some(Ok((Err(e), ip))) => {
                        eprintln!("[gw] bootstrap attempt via {ip} failed: {e}");
                    }
                    Some(Err(e)) => {
                        eprintln!("[gw] bootstrap attempt task failed: {e}");
                    }
                    None => {}
                }
                // failed/short attempt: replace immediately with a fresh candidate
                if idx < peers.len() {
                    spawn_next!();
                }
            }
            _ = tokio::time::sleep(hedge), if set.len() < max_inflight && idx < peers.len() => {
                eprintln!("[gw] bootstrap race: no winner in {}s, hedging attempt #{}", hedge.as_secs(), started + 1);
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
        let mut active = self.active.lock().unwrap();
        *active = Some(peer.clone());
        self.lazy_active.store(false, Ordering::Release);
        peer
    }

    fn pin_lazy_active(&self, ip: &str) -> ActivePeer {
        let mut active = self.active.lock().unwrap();
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
        self.active.lock().unwrap().clone()
    }

    fn active_session_snapshot(&self) -> Option<ActivePeer> {
        let active = self.active.lock().unwrap();
        if self.lazy_active.load(Ordering::Acquire) {
            None
        } else {
            active.clone()
        }
    }

    fn active_is_current(&self, peer: &ActivePeer) -> bool {
        self.active.lock().unwrap().as_ref() == Some(peer)
    }

    fn clear_active_if_current(&self, peer: &ActivePeer) {
        let mut active = self.active.lock().unwrap();
        if active.as_ref() == Some(peer) {
            *active = None;
        }
    }

    // "This node has a live session" — a lazy misc-port pin doesn't count.
    fn has_active_session(&self) -> bool {
        let active = self.active.lock().unwrap();
        active.is_some() && !self.lazy_active.load(Ordering::Acquire)
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
        let mut nodes = self.nodes.lock().unwrap();
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
    cache_coldstart: bool,
    push: bool,
    n_live: usize,
}

const STATE_SERVER_MEMORY: usize = 8;

fn remember_state_server(list: &Mutex<Vec<String>>, ip: &str) {
    let mut l = list.lock().unwrap();
    l.retain(|x| x != ip);
    l.insert(0, ip.to_string());
    l.truncate(STATE_SERVER_MEMORY);
}

// Known state servers first, then the pool rotated from `start`, deduped. A remembered peer that
// is now rate-limited answers with a tiny status frame and fails the attempt fast, so heading the
// list costs little even when stale.
fn bootstrap_candidates(remembered: Vec<String>, pool: &[String], start: usize) -> Vec<String> {
    let mut out = remembered;
    for k in 0..pool.len() {
        let ip = &pool[(start + k) % pool.len()];
        if !out.contains(ip) {
            out.push(ip.clone());
        }
    }
    out
}

// Decide whether THIS bootstrap greet gets the cached blob replayed (fresh cache + no active
// session + replay cooldown passed); logs why a present blob is being withheld otherwise.
fn cache_replay_blob(node: &NodeState, ctx: &GatewayCtx) -> Option<Arc<Vec<u8>>> {
    let now = unix_secs();
    let last = node.last_cache_replay_secs.load(Ordering::Acquire);
    let has_active_session = node.has_active_session();
    let cached_at = ctx.boot_blob_cached_at.load(Ordering::Acquire);
    let blob = ctx.boot_blob.lock().unwrap().clone()?;
    if should_replay_cache(now, last, has_active_session, cached_at) {
        return Some(blob);
    }
    if has_active_session {
        eprintln!(
            "[gw] [{}] cache replay suppressed: this node has an active session; using transparent fallback",
            node.ip
        );
    } else if !is_bootstrap_cache_fresh(now, cached_at) {
        eprintln!(
            "[gw] [{}] cache replay suppressed: cache age {}s exceeds {}s; using transparent fallback",
            node.ip,
            now.saturating_sub(cached_at),
            MAX_BOOTSTRAP_CACHE_AGE_SECS
        );
    } else {
        eprintln!(
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
    blob: Arc<Vec<u8>>,
    ctx: GatewayCtx,
) {
    let now = unix_secs();
    eprintln!(
        "[gw] [{}] node cold-start FROM CACHE ({} MB), no peer state fetch",
        node.ip,
        blob.len() / 1_000_000
    );
    node.last_cache_replay_secs.store(now, Ordering::Release);
    // Chunked on purpose: one write_all over the whole >4GB blob wedges permanently at ~2^31
    // bytes when the reader drains faster than the send-buffer copy loop (a single send()
    // syscall then never returns to userspace before its byte count overflows). Bounding each
    // write keeps every syscall small; a real node reads too slowly to trigger it, a
    // cache-draining fakenode reliably does.
    let mut replay_err = false;
    for chunk in blob.chunks(8 * 1024 * 1024) {
        if down.write_all(chunk).await.is_err() {
            replay_err = true;
            break;
        }
    }
    if replay_err {
        eprintln!(
            "[gw] [{}] cache replay write failed; node will re-bootstrap",
            node.ip
        );
        // reset the cooldown only if our own stamp is still current — never clobber a
        // concurrent newer replay's
        let _ = node.last_cache_replay_secs.compare_exchange(
            now,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
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
    if cache_floor != 0 {
        raise_round_floor(&ctx.net_round_tip, cache_floor);
        if raise_round_floor(&node.live_floor, cache_floor) {
            eprintln!(
                "[gw] [{}] cache replay live floor round={cache_floor}",
                node.ip
            );
        }
    }
    let start = ctx.rr.fetch_add(1, Ordering::Relaxed);
    if let Some(live) = select_live_peer(
        &peers,
        start,
        GREET_FALSE,
        cache_floor,
        ctx.net_round_tip.load(Ordering::Acquire),
    )
    .await
    {
        eprintln!(
            "[gw] [{}] selected live peer {} first_round={}",
            node.ip, live.ip, live.first_round
        );
        let hosts = if ctx.push {
            build_push_hosts(&live.ip, &peers, ctx.n_live)
        } else {
            vec![live.ip.clone()]
        };
        serve_push_repinned(
            down,
            live.stream,
            &node,
            "cache-live",
            PushConfig {
                hosts: Arc::new(hosts),
                active_idx: 0,
                port: 4001,
                prefetched_greeting: Some(live.greeting),
                prefetched_frames: live.prefetched_frames,
                initial_last_forwarded: cache_floor,
                live_floor: node.live_floor.clone(),
                net_round_tip: ctx.net_round_tip.clone(),
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
        GREET_FALSE,
        cache_floor,
        ctx.net_round_tip.clone(),
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
    // Advance rr by the whole scan window: retries during an incident sweep fresh pool peers
    // instead of re-dialing the same ~32 decliners (and their rate limits) shifted by one.
    let start = ctx
        .rr
        .fetch_add(BOOTSTRAP_SELECT_MAX_PEERS, Ordering::Relaxed);
    let remembered = ctx.state_servers.lock().unwrap().clone();
    let candidates = bootstrap_candidates(remembered, &peers, start);
    let Some(selected) = select_fast_bootstrap_peer(&candidates, 0, greet).await else {
        eprintln!("[gw] [{}] no peer serving abci_state right now", node.ip);
        return;
    };
    let BootstrapPeerSelection {
        upc,
        hdr,
        payload_prefix,
        ip,
        abci_len,
    } = selected;
    eprintln!(
        "[gw] [{}] node bootstrap via {ip} (abci_state {} bytes, prefetched {} MB); active set",
        node.ip,
        abci_len,
        payload_prefix.len() / 1_000_000
    );
    remember_state_server(&ctx.state_servers, &ip);
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
    if let Some(permit) = tap_permit {
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
        )
        .await;
    } else {
        if ctx.cache_coldstart {
            eprintln!(
                "[gw] [{}] bootstrap capture already in flight; relaying without tap",
                node.ip
            );
        }
        if down.write_all(&hdr).await.is_err() {
            return;
        }
        if down.write_all(&payload_prefix).await.is_err() {
            return;
        }
        splice(down, upc).await;
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
            replay_cache_then_live(down, node, peers, blob, ctx).await;
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
            &peers,
            start,
            greet,
            0,
            ctx.net_round_tip.load(Ordering::Acquire),
        )
        .await
        {
            eprintln!(
                "[gw] [{}] selected live peer {} first_round={}",
                node.ip, live.ip, live.first_round
            );
            // active stays transparent and owns the peer relationship used by 4002. Shadow
            // multi-source injection is disabled for correctness.
            let hosts = build_push_hosts(&live.ip, &peers, ctx.n_live);
            serve_push_repinned(
                down,
                live.stream,
                &node,
                "live",
                PushConfig {
                    hosts: Arc::new(hosts),
                    active_idx: 0,
                    port: 4001,
                    prefetched_greeting: Some(live.greeting),
                    prefetched_frames: live.prefetched_frames,
                    initial_last_forwarded: 0,
                    live_floor: node.live_floor.clone(),
                    net_round_tip: ctx.net_round_tip.clone(),
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
                ctx.net_round_tip.clone(),
                "push live selection failed",
            )
            .await;
        }
        return;
    }
    for k in 0..peers.len() {
        let ip = peers[(start + k) % peers.len()].clone();
        let up = format!("{}:4001", ip);
        let mut upc = match timeout(Duration::from_secs(5), TcpStream::connect(&up)).await {
            Ok(Ok(c)) => c,
            _ => continue,
        };
        upc.set_nodelay(true).ok();
        if upc.write_all(&greet).await.is_err() {
            continue;
        }
        let active_peer = node.pin_active(&ip);
        splice(down, upc).await;
        node.clear_active_if_current(&active_peer);
        break;
    }
}

// Full P2P gateway. Every downstream node connects ONLY to the gateway; the gateway provides all of
// HL's sync P2P backed by MULTIPLE upstream peers (taken from the node's own peer file, a startup path arg):
//   - abci_state: fetched from a pool peer and CACHED (served to the node at local speed, so node
//     restarts never re-pull ~950MB and never hit the per-IP abci_state rate-limit);
//   - live blocks: round-merged from several pool peers (fastest-block-first, gap-free);
//   - gossip RPC (4002 etc.): transparently proxied to an active pool peer, failing over on dial error.
// If the active peer has a problem the gateway uses the next peer from the (continuously refreshed) pool.
pub(crate) async fn run_gateway(
    node_peer_file: String,
    push: bool,
    cache_coldstart: bool,
    n_live: usize,
) {
    let pool: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(read_node_peers(&node_peer_file)));
    // cached verbatim bootstrap (abci_state + EVM KVs) for cold-start without a peer state fetch
    let boot_blob: Arc<Mutex<Option<Arc<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let boot_blob_cached_at = Arc::new(AtomicU64::new(0));
    let disk_cache_path = cache_coldstart.then(|| bootstrap_cache_path(&node_peer_file));
    if let Some(path) = &disk_cache_path {
        match load_bootstrap_cache(path, unix_secs()) {
            Ok(Some((blob, cached_at))) => {
                eprintln!(
                    "[gw] bootstrap cache loaded from {} ({} MB, age={}s)",
                    path.display(),
                    blob.len() / 1_000_000,
                    unix_secs().saturating_sub(cached_at)
                );
                *boot_blob.lock().unwrap() = Some(Arc::new(blob));
                boot_blob_cached_at.store(cached_at, Ordering::Release);
            }
            Ok(None) => {
                eprintln!("[gw] no disk bootstrap cache at {}", path.display());
            }
            Err(e) => {
                eprintln!(
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
    // Best-known network round tip (monotonic; 0 until first learned). Sources: peerd's clustered
    // round_tip in peers.json and the bootstrap-cache scans. Used as the reference for the
    // tip-relative round-plausibility ceiling; a stale/absent tip only widens the ceiling.
    let net_round_tip = Arc::new(AtomicU32::new(0));
    if let Ok(contents) = std::fs::read_to_string(&node_peer_file) {
        if let Some(tip) = extract_round_tip(&contents) {
            raise_round_floor(&net_round_tip, tip);
        }
    }
    eprintln!(
        "[gw] full P2P gateway: pool from {} ({} peers); bootstrap={}; live={}; live-upstream budget={}",
        node_peer_file,
        pool.lock().unwrap().len(),
        if cache_coldstart {
            "cache-coldstart(+transparent fallback)"
        } else {
            "transparent"
        },
        if push {
            "block-push(transparent active backbone + shadow-disabled)"
        } else {
            "transparent splice"
        },
        n_live
    );

    // pool refresher: re-read the node's peer file (peerd keeps it fresh) + the network tip
    {
        let pool = pool.clone();
        let path = node_peer_file.clone();
        let net_round_tip = net_round_tip.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let p = read_node_peers(&path);
                *pool.lock().unwrap() = p;
                if let Ok(contents) = std::fs::read_to_string(&path) {
                    if let Some(tip) = extract_round_tip(&contents) {
                        raise_round_floor(&net_round_tip, tip);
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
                    let have_cache = boot_blob.lock().unwrap().is_some();
                    let now = unix_secs();
                    let cached_at = boot_blob_cached_at.load(Ordering::Acquire);
                    let have_fresh_cache = have_cache && is_bootstrap_cache_fresh(now, cached_at);
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
                        let live_count = pool.lock().unwrap().len();
                        if should_delay_cache_refresh_for_live_pool(now, cached_at, live_count) {
                            let cache_age = now.saturating_sub(cached_at);
                            let seconds_left =
                                MAX_BOOTSTRAP_CACHE_AGE_SECS.saturating_sub(cache_age);
                            let delay = fresh_cache_retry_delay_secs(now, cached_at);
                            eprintln!(
                                "[gw] bootstrap cache refresh skipped: only {live_count} validated live peer(s); cache expires in {seconds_left}s; retry in {delay}s"
                            );
                            tokio::time::sleep(Duration::from_secs(delay)).await;
                            continue;
                        } else if live_count < MIN_LIVE_PEERS_FOR_CACHE_REFRESH {
                            let cache_age = now.saturating_sub(cached_at);
                            let seconds_left =
                                MAX_BOOTSTRAP_CACHE_AGE_SECS.saturating_sub(cache_age);
                            eprintln!(
                                "[gw] bootstrap cache refresh proceeding with only {live_count} validated live peer(s); cache expires in {seconds_left}s"
                            );
                        }
                    }
                    let window = if have_fresh_cache { 24 } else { 64 };
                    let peers: Vec<String> = {
                        let live = pool.lock().unwrap().clone();
                        let full =
                            bootstrap_capture_peers(&capture_peer_file, live, !have_fresh_cache);
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
                            eprintln!(
                                "[gw] bootstrap race: exceeded {capture_deadline_secs}s total deadline, aborting"
                            );
                            None
                        }
                    };
                    let got = if let Some((blob, ip)) = raced {
                        eprintln!(
                            "[gw] bootstrap captured: {} MB via {} (raced)",
                            blob.len() / 1_000_000,
                            ip
                        );
                        let blob = Arc::new(blob);
                        *boot_blob.lock().unwrap() = Some(blob.clone());
                        boot_blob_cached_at.store(unix_secs(), Ordering::Release);
                        if let Some(path) = disk_cache_path.clone() {
                            tokio::task::spawn_blocking(move || {
                                match persist_bootstrap_cache(&path, blob.as_slice()) {
                                    Ok(()) => {
                                        eprintln!(
                                            "[gw] bootstrap cache persisted to {}",
                                            path.display()
                                        );
                                    }
                                    Err(e) => {
                                        eprintln!(
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
        cache_coldstart,
        push,
        n_live,
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
                    eprintln!("[gw] bind :{port}: {e}");
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
                    let peers = pool.lock().unwrap().clone();
                    if peers.is_empty() {
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
                        if greet[5] == 1 {
                            handle_4001_bootstrap(down, node, peers, greet, ctx).await;
                        } else {
                            handle_4001_live(down, node, peers, greet, ctx).await;
                        }
                    } else if should_fetch_forward_client_blocks(port) {
                        // 4002 client-block RPC is request/response. Always fetch-forward it through
                        // the current active peer so we can detect "Peer-only request" and wait for a
                        // fresh active instead of leaking the rejection to the node.
                        serve_client_blocks(down, node.clone(), peers, ctx.rr.clone()).await;
                    } else {
                        // Other gossip channels stay transparently spliced to the node's active peer.
                        let Some(upc) = dial_active(&node, &peers, &ctx.rr, port).await else {
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
    fn raise_round_floor_is_monotonic() {
        let floor = Arc::new(AtomicU32::new(10));

        assert!(!raise_round_floor(&floor, 9));
        assert_eq!(floor.load(Ordering::Acquire), 10);
        assert!(raise_round_floor(&floor, 11));
        assert_eq!(floor.load(Ordering::Acquire), 11);
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
        assert!(b.active.lock().unwrap().is_none());
        drop(b_guard); // stamps last_seen ~t0; ip_b now idle with no open connections

        // ip_a still has an open connection (a1_guard) -> survives TTL eviction; ip_b doesn't
        let later = t0 + NODE_STATE_TTL_SECS + 60;
        let ip_c: IpAddr = "10.0.0.3".parse().unwrap();
        let (_c, _c_guard) = reg.get_or_insert(ip_c, later);
        assert!(reg.nodes.lock().unwrap().contains_key(&ip_a));
        assert!(!reg.nodes.lock().unwrap().contains_key(&ip_b));
        assert!(reg.nodes.lock().unwrap().contains_key(&ip_c));
        drop(a1_guard);
    }

    #[test]
    fn bootstrap_candidates_prefer_remembered_state_servers() {
        let pool: Vec<String> = ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        // remembered heads the list, pool rotates from start, remembered ip deduped from pool
        let out =
            bootstrap_candidates(vec!["3.3.3.3".to_string(), "9.9.9.9".to_string()], &pool, 1);
        assert_eq!(out, ["3.3.3.3", "9.9.9.9", "2.2.2.2", "4.4.4.4", "1.1.1.1"]);
        // no memory -> plain rotation
        let out = bootstrap_candidates(Vec::new(), &pool, 2);
        assert_eq!(out, ["3.3.3.3", "4.4.4.4", "1.1.1.1", "2.2.2.2"]);
    }

    #[test]
    fn remember_state_server_dedups_most_recent_first_and_caps() {
        let list = Mutex::new(Vec::new());
        for i in 0..(STATE_SERVER_MEMORY + 3) {
            remember_state_server(&list, &format!("10.0.0.{i}"));
        }
        remember_state_server(&list, "10.0.0.5"); // re-serve moves to front, no duplicate
        let l = list.lock().unwrap();
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
        ] {
            assert!(s.contains(part), "missing {part} in {s}");
        }
    }
}
