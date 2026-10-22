use crate::peerd::{peer_candidates_path, read_node_peers};
use crate::priority::gwlog;
use crate::protocol::{
    complete_bootstrap_at_frame_boundary, complete_frame_count, finish_header_after_partial,
    max_block_round_in_frames, read_header_or_timeout, unix_secs, HeaderRead, ACTIVE_ROUND_MAX_LAG,
    GREET_TRUE, MIN_COMPLETE_BOOTSTRAP_BYTES,
};
use crate::push::{
    copy_node_to_upstream, AbortOnDrop, Relayed, NODE_WRITE_TIMEOUT, RELAY_UPSTREAM_WRITE_TIMEOUT,
};
use crate::util::lock;
use std::collections::HashSet;
use std::env;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

pub(super) fn bootstrap_capture_peers(
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
pub(super) struct CacheSink {
    pub(super) boot_blob: Arc<Mutex<Option<Arc<Vec<u8>>>>>,
    pub(super) boot_blob_cached_at: Arc<AtomicU64>,
    pub(super) disk_cache_path: Option<PathBuf>,
}

// A complete bootstrap ends in the live block stream (the capture stops only once the bulk
// transfer has given way to the live trickle), so its tail has decodable block rounds. A capture
// cut off inside the EVM-KV bulk has none, however close to MIN_COMPLETE_BOOTSTRAP_BYTES it is:
// replaying it would fail the node's state load and loop every cooldown.
pub(super) fn has_live_tail(blob: &[u8]) -> bool {
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
pub(super) async fn node_write<W: AsyncWrite + Unpin>(w: &mut W, buf: &[u8]) -> bool {
    matches!(
        timeout(NODE_WRITE_TIMEOUT, w.write_all(buf)).await,
        Ok(Ok(()))
    )
}

// Transparent bootstrap fallback, with a write-through cache tap. This keeps the node's real peer
// relationship intact while opportunistically filling --cache from the same successful bootstrap.
pub(super) async fn splice_bootstrap_capture(
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
    let mut h = AbortOnDrop({
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
    // ...and the copy task ending is waited on too: a node that never closes (half-open, a
    // non-standard client) must not keep this relay alive after the upstream side is done
    let to_peer = tokio::select! {
        (n, upstream_gone) = copy_node_to_upstream(&mut dr, &mut uw, RELAY_UPSTREAM_WRITE_TIMEOUT) => {
            if upstream_gone {
                upstream_ended.store(true, Ordering::Release);
            }
            n
        }
        _ = &mut h.0 => 0,
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
pub(super) const CACHE_FLOOR_TTL: Duration = Duration::from_secs(10 * 60);

// hl-node's live/bootstrap reader times out in roughly five minutes once it sees the live
// stream's first block. A very old cache forces too much client-block catch-up before that
// first live round can be processed, so keep the replay window intentionally short.
pub(super) const MAX_BOOTSTRAP_CACHE_AGE_SECS: u64 = 8 * 60;

pub(super) const CACHE_REFRESH_SECS: u64 = 4 * 60;

pub(super) const CACHE_REFRESH_RETRY_SECS: u64 = 30;

const CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS: u64 = 90;

const MIN_BOOTSTRAP_RATE_BYTES_PER_SEC: f64 = 5_000_000.0;

pub(super) const MIN_LIVE_PEERS_FOR_CACHE_REFRESH: usize = 4;

// Concurrent capture buffers (~5.8GB each in 2026-10). Peak = these + the single-flight fallback
// tap + the current blob (fresh case; a stale blob is freed first): 1 + 1 + 1 fresh, 2 + 1 cold,
// i.e. ~3 bootstraps (~17.4GB) either way, inside compose's 24g mem_limit.
pub(super) const FRESH_CACHE_REFRESH_MAX_INFLIGHT: usize = 1;

pub(super) const COLD_CACHE_REFRESH_MAX_INFLIGHT: usize = 2;

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

pub(super) fn bootstrap_cache_path(node_peer_file: &str) -> PathBuf {
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

pub(super) fn is_bootstrap_cache_fresh(now: u64, cached_at: u64) -> bool {
    cached_at != 0 && now.saturating_sub(cached_at) <= MAX_BOOTSTRAP_CACHE_AGE_SECS
}

pub(super) fn should_delay_cache_refresh_for_live_pool(
    now: u64,
    cached_at: u64,
    live_count: usize,
) -> bool {
    if live_count >= MIN_LIVE_PEERS_FOR_CACHE_REFRESH {
        return false;
    }
    let cache_age = now.saturating_sub(cached_at);
    let seconds_left = MAX_BOOTSTRAP_CACHE_AGE_SECS.saturating_sub(cache_age);
    seconds_left > CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS
}

pub(super) fn fresh_cache_retry_delay_secs(now: u64, cached_at: u64) -> u64 {
    let cache_age = now.saturating_sub(cached_at);
    let seconds_left = MAX_BOOTSTRAP_CACHE_AGE_SECS.saturating_sub(cache_age);
    if seconds_left <= CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS {
        1
    } else {
        (seconds_left - CACHE_REFRESH_FORCE_BEFORE_EXPIRY_SECS).clamp(1, CACHE_REFRESH_RETRY_SECS)
    }
}

pub(super) fn bootstrap_capture_deadline_secs(have_fresh_cache: bool) -> u64 {
    if have_fresh_cache {
        FRESH_BOOTSTRAP_REFRESH_MAX_SECS
    } else {
        COLD_BOOTSTRAP_REFRESH_MAX_SECS
    }
}

pub(super) fn load_bootstrap_cache(
    path: &Path,
    now: u64,
) -> std::io::Result<Option<(Vec<u8>, u64)>> {
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
pub(super) fn remove_orphan_cache_tmps(path: &Path) {
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

pub(super) fn persist_bootstrap_cache(path: &Path, blob: &[u8]) -> std::io::Result<()> {
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

pub(super) fn raise_round_floor(floor: &Arc<AtomicU32>, round: u32) -> bool {
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
pub(super) async fn capture_bootstrap_raced(
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

// Longest a transparent bootstrap's cache tap may capture. Headroom over the cache refresh's own
// limits: its 15 min cold deadline, and ~20 min for a whole bootstrap (~5.8GB) at the 5MB/s its
// rate spot-check accepts. (The tap's own sustained-rate detector, 400KB/s, only marks the end of
// the bulk transfer.)
#[cfg(not(test))]
const MAX_TAP_CAPTURE: Duration = Duration::from_secs(30 * 60);

#[cfg(test)]
const MAX_TAP_CAPTURE: Duration = Duration::from_millis(300);

// The live-only form of a node's 4001 greeting `[0 0 0 3][0][send_abci][broadcast_group][id]`:
// send_abci cleared, everything else (notably broadcast_group 1 = the mempool/tx feed) kept.
pub(super) fn live_greeting_for(greet: [u8; 8]) -> [u8; 8] {
    let mut g = greet;
    g[5] = 0;
    g
}

// The live floor a cache replay may set: the blob's tail round, unless the network tip is known
// and the blob claims rounds beyond it (0 = no floor).
pub(super) fn bounded_cache_floor(cache_floor: u32, net_tip: u32) -> u32 {
    if net_tip != 0 && cache_floor > net_tip.saturating_add(ACTIVE_ROUND_MAX_LAG) {
        0
    } else {
        cache_floor
    }
}

// Held for the duration of one cache replay to a node (set by cache_replay_blob): the replay can
// outlast CACHE_REPLAY_COOLDOWN_SECS, and has_active_session() stays false until the live session
// after it, so without this a second greet mid-replay would start a second full replay.
pub(super) struct ReplayInFlight(pub(super) Arc<NodeState>);

impl Drop for ReplayInFlight {
    fn drop(&mut self) {
        self.0.replay_in_flight.store(false, Ordering::Release);
    }
}

// Decide whether THIS bootstrap greet gets the cached blob replayed (fresh cache + no active
// session + replay cooldown passed); logs why a present blob is being withheld otherwise.
pub(super) fn cache_replay_blob(node: &NodeState, ctx: &GatewayCtx) -> Option<Arc<Vec<u8>>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::GREET_FALSE;

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

    // A tapped bootstrap relay whose upstream goes silent mid-frame ends: the single-flight
    // capture permit is released (it used to be held forever) and the upstream is blamed.
    // A node that never closes (half-open, a non-standard client) must not keep the tapped relay
    // alive once the upstream side has ended it.
    #[tokio::test]
    async fn a_half_open_node_does_not_keep_the_tapped_relay_alive() {
        let up_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_addr = up_l.local_addr().unwrap();
        tokio::spawn(async move {
            let (_s, _) = up_l.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let node_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_addr = node_l.local_addr().unwrap();
        let node = tokio::spawn(async move {
            // connected, never reads, never closes
            let _c = TcpStream::connect(node_addr).await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let (down, _) = node_l.accept().await.unwrap();
        let upc = TcpStream::connect(up_addr).await.unwrap();
        let tap = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = tap.clone().try_acquire_owned().unwrap();
        let r = timeout(
            Duration::from_secs(3),
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
        .expect("the relay ends without waiting for the node to close");
        assert!(r.upstream_ended);
        assert_eq!(tap.available_permits(), 1);
        node.abort();
    }

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
    fn cache_live_greeting_keeps_the_nodes_broadcast_group() {
        assert_eq!(live_greeting_for(GREET_TRUE), GREET_FALSE);
        assert_eq!(
            live_greeting_for([0, 0, 0, 3, 0, 1, 1, 0]),
            [0, 0, 0, 3, 0, 0, 1, 0]
        );
    }
}
