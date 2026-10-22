use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

use crate::priority::gwlog;
use crate::protocol::{
    block_round, is_plausible_mainnet_round, read_live_frame_with_timeout, response_is_peer_full,
    round_lag_from_tip, RoundDedup, RoundForwardGate, ACTIVE_ROUND_MAX_LAG, GREET_FALSE,
    MAX_LIVE_FRAME,
};

pub(crate) async fn serve(mut down: TcpStream, upstreams: Vec<String>) -> std::io::Result<()> {
    down.set_nodelay(true).ok();
    let mut g = [0u8; 8];
    timeout(Duration::from_secs(20), down.read_exact(&mut g)).await??;
    eprintln!("[gw] node greeting {:02x?}", g);

    let dedup = Arc::new(RoundDedup::new(16_384));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(4096);

    for ip in &upstreams {
        let ip = ip.clone();
        let tx = tx.clone();
        let dedup = dedup.clone();
        tokio::spawn(async move {
            match connect_greet(&ip, GREET_FALSE).await {
                Ok(s) => {
                    let _ = pump(s, ip, tx, dedup).await;
                }
                Err(e) => gwlog!("[gw] upstream {ip} connect: {e}"),
            }
        });
    }
    drop(tx);

    let mut bytes = 0u64;
    let mut nframes = 0u64;
    while let Some(buf) = rx.recv().await {
        bytes += buf.len() as u64;
        nframes += 1;
        if down.write_all(&buf).await.is_err() {
            break;
        }
        if nframes.is_multiple_of(5) {
            eprintln!("[gw] frames forwarded={}", nframes);
        }
    }
    eprintln!("[gw] end: frames={} bytes={}", nframes, bytes);
    Ok(())
}

async fn connect_greet(addr: &str, g: [u8; 8]) -> std::io::Result<TcpStream> {
    let target = if addr.contains(':') {
        addr.to_string()
    } else {
        format!("{addr}:4001")
    };
    let mut s = timeout(Duration::from_secs(5), crate::blacklist::connect(&target)).await??;
    s.set_nodelay(true).ok();
    s.write_all(&g).await?;
    Ok(s)
}

async fn pump(
    mut s: TcpStream,
    ip: String,
    tx: mpsc::Sender<Vec<u8>>,
    dedup: Arc<RoundDedup>,
) -> std::io::Result<()> {
    let mut first = true;
    loop {
        let mut hdr = [0u8; 5];
        s.read_exact(&mut hdr).await?;
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        if len > 50_000_000 {
            let mut rem = len;
            let mut buf = vec![0u8; 65536];
            while rem > 0 {
                let n = s.read(&mut buf[..rem.min(65536)]).await?;
                if n == 0 {
                    return Ok(());
                }
                rem -= n;
            }
            continue;
        }
        // single allocation: read the payload directly into the send-ready frame (see pump_merge)
        let mut frame = vec![0u8; 5 + len];
        frame[..5].copy_from_slice(&hdr);
        s.read_exact(&mut frame[5..]).await?;
        if typ != 1 {
            continue;
        }
        match block_round(&frame[5..]) {
            Some(r) => {
                if first {
                    gwlog!("[gw] upstream {ip}: first round = {r}");
                    first = false;
                }
                if dedup.is_new(r) && tx.send(frame).await.is_err() {
                    return Ok(());
                }
            }
            None => {
                if tx.send(frame).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
}

// Bidirectionally relay a node connection and its chosen upstream until either side closes.
// Returns (bytes upstream->node, bytes node->upstream). Writes to the node are bounded by
// NODE_WRITE_TIMEOUT and either direction ending ends the relay: a node that stops reading (hung,
// SIGSTOPped, half-open) would otherwise park the task — and the upstream connection, pin and
// priority session it holds — until TCP gives up, ~15 min later. The relay also ends once the
// upstream's IP is blacklisted (hot reload), so the node reconnects to an allowed peer.
pub(crate) async fn splice(down: TcpStream, upc: TcpStream) -> (u64, u64) {
    let up_ip = upc
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = upc.into_split();
    let to_node = Arc::new(AtomicU64::new(0));
    // aborted on drop: a caller that cancels this future (e.g. a proxy failover) must not leave
    // the upstream->node copy running and writing the old peer's bytes into the node
    let mut h = AbortOnDrop({
        let to_node = to_node.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 256 * 1024];
            loop {
                let n = match ur.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                match timeout(NODE_WRITE_TIMEOUT, dw.write_all(&buf[..n])).await {
                    Ok(Ok(())) => {}
                    _ => break,
                }
                to_node.fetch_add(n as u64, Ordering::Relaxed);
            }
        })
    });
    let to_peer = tokio::select! {
        r = tokio::io::copy(&mut dr, &mut uw) => r.unwrap_or(0),
        _ = &mut h.0 => 0,
        _ = crate::blacklist::wait_blacklisted(&up_ip) => 0,
    };
    drop(h);
    (to_node.load(Ordering::Relaxed), to_peer)
}

// A spawned task that is aborted when its owner is dropped (a plain JoinHandle detaches).
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// Pick the next non-bad upstream after `cur` (skips peers in cooldown). None => no healthy alternative.
fn pick_next(cur: usize, bad_until: &[AtomicU64], now_ms: u64, n: usize) -> Option<usize> {
    for off in 1..n {
        let j = (cur + off) % n;
        if bad_until[j].load(Ordering::Relaxed) <= now_ms {
            return Some(j);
        }
    }
    None
}

// The proxy's shared failover state: which upstream is active, a generation bumped on every
// switch (sessions on an older generation end), and per-upstream cooldowns.
struct ProxyState {
    hosts: Arc<Vec<String>>,
    active: AtomicUsize,
    generation: Arc<AtomicU64>,
    bad_until: Vec<AtomicU64>,
    base: std::time::Instant,
    // last upstream->node byte on a raw splice (non-push stall monitor); restarted on a switch so
    // a new upstream isn't judged by the silence of the one it replaced
    last_data_ms: Arc<AtomicU64>,
}

impl ProxyState {
    fn now_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }

    // Cool `idx` down and, if it is still the active upstream, move to the next healthy one.
    fn fail_over(&self, idx: usize, why: &str) {
        const COOLDOWN_MS: u64 = 30_000;
        let now = self.now_ms();
        self.bad_until[idx].store(now + COOLDOWN_MS, Ordering::Relaxed);
        if self.active.load(Ordering::Relaxed) != idx {
            return; // another session already moved on
        }
        match pick_next(idx, &self.bad_until, now, self.hosts.len()) {
            Some(j) => {
                self.active.store(j, Ordering::Relaxed);
                self.generation.fetch_add(1, Ordering::Relaxed);
                self.last_data_ms.store(now, Ordering::Relaxed);
                gwlog!(
                    "[proxy] {why} on {} -> failover -> {}",
                    self.hosts[idx],
                    self.hosts[j]
                );
            }
            None => gwlog!(
                "[proxy] {why} on {}, no healthy alternative",
                self.hosts[idx]
            ),
        }
    }
}

// Failover transparent proxy. Default (push=false): the local node dials the gateway (4000-4010,
// outbound) and the gateway transparently proxies to the ACTIVE upstream peer; a health monitor
// rotates to the next healthy peer on stall/dial-failure ("选择其他peer替代").
// With push=true: the heavy block channel (4001) runs serve_push — live blocks from the active peer
// are round-gated (dedup + live floor) before reaching the node, while the active connection stays
// transparent for the node's outbound + control/state/RPC frames. Shadow peers are compiled out
// (ENABLE_SHADOW_PUSH), so this does not merge blocks from other peers. serve_push itself is the
// health signal then (idle, block progress, lag, implausible rounds): an upstream-side session end
// fails the active over, and a failover ends every session of the older generation so 4001 and
// 4002 always follow the same peer.
// (Inbound push to the node is not implemented — verified: HL nodes only ingest from peers they dial.)
pub(crate) async fn run_proxy(upstreams: Vec<String>, push: bool) {
    let hosts: Vec<String> = upstreams
        .iter()
        .map(|u| match u.rsplit_once(':') {
            Some((h, _)) => h.to_string(),
            None => u.clone(),
        })
        .collect();
    if hosts.is_empty() {
        gwlog!("[proxy] no upstreams given");
        return;
    }
    let n = hosts.len();
    // last upstream->node byte on a raw splice; only meaningful without --push (with --push the
    // 4001 stream goes through serve_push and never stamps it, so a stall monitor on it would be
    // blind to the main stream and fire on quiet side ports instead)
    let last_data_ms = Arc::new(AtomicU64::new(0));
    let st = Arc::new(ProxyState {
        hosts: Arc::new(hosts),
        active: AtomicUsize::new(0),
        generation: Arc::new(AtomicU64::new(0)),
        bad_until: (0..n).map(|_| AtomicU64::new(0)).collect(),
        base: std::time::Instant::now(),
        last_data_ms: last_data_ms.clone(),
    });
    gwlog!(
        "[proxy] upstreams={:?}, active={}, push={}",
        st.hosts,
        st.hosts[0],
        push
    );

    if !push {
        let st = st.clone();
        let last_data_ms = last_data_ms.clone();
        tokio::spawn(async move {
            const STALL_MS: u64 = 20000;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = st.now_ms();
                let ld = last_data_ms.load(Ordering::Relaxed);
                if ld != 0 && now.saturating_sub(ld) > STALL_MS {
                    st.fail_over(
                        st.active.load(Ordering::Relaxed),
                        &format!("STALL {}ms", now - ld),
                    );
                    last_data_ms.store(now, Ordering::Relaxed);
                }
            }
        });
    }

    let mut handles = Vec::new();
    for p in 4000u16..=4010 {
        let st = st.clone();
        let last_data_ms = last_data_ms.clone();
        handles.push(tokio::spawn(async move {
            let l = match TcpListener::bind(("0.0.0.0", p)).await {
                Ok(l) => l,
                Err(e) => {
                    gwlog!("[proxy] bind :{p}: {e}");
                    return;
                }
            };
            loop {
                let (down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let st = st.clone();
                let last_data_ms = last_data_ms.clone();
                tokio::spawn(async move {
                    proxy_conn(down, addr, p, push, st, last_data_ms).await;
                });
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

// How a raw proxy's node -> upstream copy ended.
#[derive(Debug, PartialEq)]
enum N2uEnd {
    Node,
    UpstreamGone,
    UpstreamStalled,
}

// One node connection on port `p`, proxied to the active upstream.
async fn proxy_conn(
    down: TcpStream,
    addr: std::net::SocketAddr,
    p: u16,
    push: bool,
    st: Arc<ProxyState>,
    last_data_ms: Arc<AtomicU64>,
) {
    let cur_gen = st.generation.load(Ordering::Relaxed);
    let idx = st.active.load(Ordering::Relaxed);
    let up = format!("{}:{}", st.hosts[idx], p);
    let mut upc = match timeout(Duration::from_secs(4), crate::blacklist::connect(&up)).await {
        Ok(Ok(s)) => s,
        _ => {
            st.fail_over(idx, "dial failed");
            return;
        }
    };
    let mut down = down;
    down.set_nodelay(true).ok();
    upc.set_nodelay(true).ok();
    // 4001: forward the node's greeting and vet the upstream's first frame before anything else,
    // so a refusal (peer full) or a silent upstream rotates the active out instead of being handed
    // to the node as its greeting (and, on a raw splice, counted as healthy traffic).
    let mut first = Vec::new();
    let mut bootstrap = false;
    if p == 4001 {
        let mut greet = [0u8; 8];
        if !matches!(
            timeout(Duration::from_secs(20), down.read_exact(&mut greet)).await,
            Ok(Ok(_))
        ) {
            return;
        }
        if !matches!(
            timeout(Duration::from_secs(5), upc.write_all(&greet)).await,
            Ok(Ok(()))
        ) {
            st.fail_over(idx, "greeting write failed");
            return;
        }
        bootstrap = greet[5] == 1;
        // a live greeting reply is immediate; a serving state peer may take a few seconds to
        // start the abci_state frame
        let wait = if bootstrap {
            Duration::from_secs(12)
        } else {
            PUSH_GREETING_WAIT
        };
        match read_first_frame(&mut upc, wait, bootstrap).await {
            Ok(f) if response_is_peer_full(&f) => {
                st.fail_over(idx, "peer full");
                return;
            }
            Ok(f) => first = f,
            Err(e) => {
                st.fail_over(idx, &format!("first frame: {e}"));
                return;
            }
        }
    }
    if push && p == 4001 && !bootstrap {
        // live channel: round-gated blocks from the active peer (shadows are compiled out);
        // active stays transparent for node->peer + control/RPC.
        let exit_reason = serve_push(
            down,
            upc,
            PushConfig {
                hosts: st.hosts.clone(),
                active_idx: idx,
                port: p,
                prefetched_greeting: Some(first),
                prefetched_frames: Vec::new(),
                initial_last_forwarded: 0,
                live_floor: Arc::new(AtomicU32::new(0)),
                // proxy mode has no peerd tip: floor-only plausibility (the session's round-jump
                // and block-progress guards still apply)
                net_round_tip: Arc::new(AtomicU32::new(0)),
                superseded: Some((st.generation.clone(), cur_gen)),
            },
        )
        .await;
        gwlog!(
            "[proxy] :{p} {addr} push session ended active_peer={} reason={exit_reason}",
            st.hosts[idx]
        );
        if push_exit_is_upstream_fault(&exit_reason) {
            st.fail_over(idx, "push session failed");
        }
        return;
    }
    if !first.is_empty() {
        // the vetted greeting / start of the abci_state frame (header only for a big frame)
        if !matches!(
            timeout(NODE_WRITE_TIMEOUT, down.write_all(&first)).await,
            Ok(Ok(()))
        ) {
            return;
        }
    }
    if push {
        // bootstrap (send_abci:true) or a side port under --push: transparent, and never stamped
        // into the (disabled) raw-splice stall monitor. Mid-bootstrap multi-source injection can't
        // help (the abci_state/EVM-KV boundaries aren't detectable). It still follows failovers,
        // like the 4001 push session, so the node's 4002 never talks to a different peer than
        // its live stream.
        tokio::select! {
            _ = splice(down, upc) => {}
            _ = wait_superseded(Some((st.generation.clone(), cur_gen))) => {}
        }
        return;
    }
    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = upc.into_split();
    let ld = last_data_ms.clone();
    let base = st.base;
    // ends true when the UPSTREAM ended the stream, false when the node stopped taking it
    let mut up2 = tokio::spawn(async move {
        let mut buf = vec![0u8; 262144];
        let closed = loop {
            match ur.read(&mut buf).await {
                Ok(0) | Err(_) => break true,
                Ok(nb) => {
                    ld.store(base.elapsed().as_millis() as u64, Ordering::Relaxed);
                    match timeout(NODE_WRITE_TIMEOUT, dw.write_all(&buf[..nb])).await {
                        Ok(Ok(())) => {}
                        _ => break false,
                    }
                }
            }
        };
        (closed, Instant::now())
    });
    // node -> upstream, bounded like serve_push's: an upstream that keeps sending but stops
    // reading would otherwise park the node's RPC requests and acks behind a full socket while
    // the stall monitor (fed by up2) sees a healthy stream. Ends with whose fault it was.
    let mut n2u = tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        let end = loop {
            match dr.read(&mut buf).await {
                Ok(0) | Err(_) => break N2uEnd::Node,
                Ok(nb) => match timeout(Duration::from_secs(5), uw.write_all(&buf[..nb])).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => break N2uEnd::UpstreamGone,
                    Err(_) => break N2uEnd::UpstreamStalled,
                },
            }
        };
        (end, Instant::now())
    });
    // which direction ended first (None = superseded by a failover: nobody to blame)
    let (mut up, mut down) = (None, None);
    let first_up = tokio::select! {
        _ = wait_superseded(Some((st.generation.clone(), cur_gen))) => None,
        r = &mut up2 => {
            up = r.ok();
            Some(true)
        }
        r = &mut n2u => {
            down = r.ok();
            Some(false)
        }
    };
    if let Some(first_up) = first_up {
        // one end makes the other side close right after, so give the other direction a moment
        // to end too, then let the earlier end decide (never re-poll the handle that finished)
        const SETTLE: Duration = Duration::from_millis(100);
        if first_up {
            down = timeout(SETTLE, &mut n2u).await.ok().and_then(Result::ok);
        } else {
            up = timeout(SETTLE, &mut up2).await.ok().and_then(Result::ok);
        }
        if let Some(why) = raw_proxy_fault(p, up, down) {
            st.fail_over(idx, why);
        }
    }
    up2.abort();
    n2u.abort();
}

// Why a raw proxy connection was the upstream's fault, if it was, from the direction(s) that had
// ended and when. The EARLIER end decides: an upstream that closes its stream makes the node close
// right after (it sees EOF), and a node that leaves makes the upstream close after it. A stalled
// upstream is at fault on any port; one that closed or reset only on 4001 — a side-port peer may
// close after answering an RPC.
fn raw_proxy_fault(
    p: u16,
    up: Option<(bool, Instant)>,
    down: Option<(N2uEnd, Instant)>,
) -> Option<&'static str> {
    let upstream_first = match (&up, &down) {
        (Some((_, u)), Some((_, d))) => u <= d,
        (Some(_), None) => true,
        _ => false,
    };
    if upstream_first {
        let (closed, _) = up?;
        return (p == 4001 && closed).then_some("upstream closed the stream");
    }
    match down?.0 {
        N2uEnd::UpstreamStalled => Some("upstream stopped reading"),
        N2uEnd::UpstreamGone if p == 4001 => Some("upstream write failed"),
        N2uEnd::UpstreamGone | N2uEnd::Node => None,
    }
}

// The upstream's first 4001 frame after the node's greeting. Live: the whole frame, at most 1000
// bytes (a greeting or a refusal status). Bootstrap: just the 5-byte header of the abci_state
// frame (relayed as-is by the splice that follows), which must be a real state frame — anything
// up to 4MB is a status/decline (rate limit, not serving), as the gateway treats it.
async fn read_first_frame(
    upc: &mut TcpStream,
    wait: Duration,
    bootstrap: bool,
) -> std::io::Result<Vec<u8>> {
    let mut hdr = [0u8; 5];
    timeout(wait, upc.read_exact(&mut hdr)).await??;
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let mut frame = hdr.to_vec();
    if bootstrap && len > 4_000_000 {
        if len > 1_500_000_000 {
            return Err(std::io::Error::other(format!(
                "implausible abci_state len={len}"
            )));
        }
        return Ok(frame);
    }
    if bootstrap && len > 1000 {
        // a longer status / decline text (rate limited, not serving) — not state either
        return Err(std::io::Error::other(format!(
            "not serving abci_state (status frame len={len})"
        )));
    }
    if len > 1000 {
        return Err(std::io::Error::other(format!(
            "live first frame too large len={len}"
        )));
    }
    let mut payload = vec![0u8; len];
    timeout(wait, upc.read_exact(&mut payload)).await??;
    frame.extend_from_slice(&payload);
    if bootstrap && !response_is_peer_full(&frame) {
        return Err(std::io::Error::other(format!(
            "not serving abci_state (status frame len={len})"
        )));
    }
    Ok(frame)
}

// Block-push serving (push mode, heavy channel): the active peer connection is transparent for the
// node's outbound and the active peer's control / abci_state / RPC frames. Shadow peer injection is
// compiled out (ENABLE_SHADOW_PUSH = false, no runtime switch): round-only dedup cannot prove proposer/fork correctness, and a wrong shadow
// block can poison the node even when the active transparent peer is healthy.
const PUSH_MERGE_QUEUE_FRAMES: usize = 256;
const ENABLE_SHADOW_PUSH: bool = false;
// Generous: the node normally drains 4001 continuously; only a stuck/half-open node hits this.
pub(crate) const NODE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
// An upstream greeting reply is a few bytes sent right away; hl-node itself gives up ~5s after
// greeting, so a longer wait only serves a dead connection.
const PUSH_GREETING_WAIT: Duration = Duration::from_secs(4);

// Guards on the ACTIVE stream's own block rounds, independent of the network tip (which can be
// unknown, = 0):
// - a block more than MAX_ACTIVE_ROUND_JUMP above the stream's previous block is a decode artifact
//   or a hostile peer. Forwarding it would set the forward gate's last_forwarded past every real
//   round, after which all real blocks are dropped as duplicates while the session stays up.
//   20k rounds is ~23 min, well above any legitimate gap within one stream.
// - a stream that keeps sending frames (control / mempool / no-round data) but no higher block
//   round for ACTIVE_BLOCK_STALL is stalled: the per-read IDLE timeout never fires on it, and
//   hl-node's own read deadline doesn't either, so without this the node would never reconnect.
pub(crate) const MAX_ACTIVE_ROUND_JUMP: u32 = 20_000;
pub(crate) const ACTIVE_BLOCK_STALL: Duration = Duration::from_secs(30);

// Frames queued toward one node are bounded in BYTES, not count: a peer sending maximum-size
// frames would otherwise park PUSH_MERGE_QUEUE_FRAMES x MAX_LIVE_FRAME in memory per session. Each
// queued frame holds permits for its size until the writer has sent it to the node.
pub(crate) const NODE_QUEUE_BYTES: usize = 64 << 20;
pub(crate) type QueuedFrame = (Vec<u8>, OwnedSemaphorePermit);

pub(crate) fn node_queue_budget() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(NODE_QUEUE_BYTES))
}

// Wait for budget for `frame`, then queue it. Err = the session's writer is gone.
pub(crate) async fn queue_frame(
    tx: &mpsc::Sender<QueuedFrame>,
    budget: &Arc<Semaphore>,
    frame: Vec<u8>,
) -> Result<(), ()> {
    let n = frame.len().clamp(1, NODE_QUEUE_BYTES) as u32;
    let permit = budget.clone().acquire_many_owned(n).await.map_err(|_| ())?;
    tx.send((frame, permit)).await.map_err(|_| ())
}

// The node-bound queue of one push session: frames plus their byte budget, sent like an mpsc
// Sender (send(frame) waits for budget, then for a queue slot).
#[derive(Clone)]
pub(crate) struct NodeQueue {
    tx: mpsc::Sender<QueuedFrame>,
    budget: Arc<Semaphore>,
}

impl NodeQueue {
    pub(crate) fn new(frames: usize) -> (Self, mpsc::Receiver<QueuedFrame>) {
        let (tx, rx) = mpsc::channel(frames);
        (
            NodeQueue {
                tx,
                budget: node_queue_budget(),
            },
            rx,
        )
    }

    pub(crate) async fn send(&self, frame: Vec<u8>) -> Result<(), ()> {
        queue_frame(&self.tx, &self.budget, frame).await
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

pub(crate) struct BlockProgress {
    pub(crate) last_round: u32,
    at: Instant,
}

impl BlockProgress {
    pub(crate) fn new(last_round: u32, now: Instant) -> Self {
        BlockProgress {
            last_round,
            at: now,
        }
    }

    // Record a decoded, plausible block round from the active stream.
    pub(crate) fn on_round(&mut self, rnd: u32, now: Instant) -> Result<(), String> {
        if self.last_round != 0 && rnd > self.last_round.saturating_add(MAX_ACTIVE_ROUND_JUMP) {
            return Err(format!(
                "active block round jump round={rnd} prev={} max_jump={MAX_ACTIVE_ROUND_JUMP}",
                self.last_round
            ));
        }
        if rnd > self.last_round {
            self.last_round = rnd;
            self.at = now;
        }
        Ok(())
    }

    // Armed only once a block round has been seen, so a wire-format change that breaks
    // block_round (reported by no_block_rounds_alarm) doesn't tear every session down.
    pub(crate) fn stalled(&self, now: Instant) -> bool {
        self.last_round != 0 && now.duration_since(self.at) > ACTIVE_BLOCK_STALL
    }
}

pub(crate) struct PushConfig {
    pub(crate) hosts: Arc<Vec<String>>,
    pub(crate) active_idx: usize,
    pub(crate) port: u16,
    pub(crate) prefetched_greeting: Option<Vec<u8>>,
    pub(crate) prefetched_frames: Vec<Vec<u8>>,
    pub(crate) initial_last_forwarded: u32,
    pub(crate) live_floor: Arc<AtomicU32>,
    // best-known network round tip (0 = unknown); makes the round-plausibility ceiling
    // tip-relative instead of a fixed constant
    pub(crate) net_round_tip: Arc<AtomicU32>,
    // proxy only: (failover generation, value at session start). When the proxy fails over, the
    // session ends so the node reconnects to the new active with 4001 and 4002 on the same peer.
    pub(crate) superseded: Option<(Arc<AtomicU64>, u64)>,
}

// Resolves once the proxy's failover generation moves past the session's; never without one.
async fn wait_superseded(superseded: Option<(Arc<AtomicU64>, u64)>) {
    let Some((generation, at_start)) = superseded else {
        return std::future::pending().await;
    };
    while generation.load(Ordering::Relaxed) == at_start {
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

// serve_push exit reasons that are the NODE's doing (it left, or stopped reading) or a proxy
// failover; anything else is the active upstream's fault and should rotate it out.
pub(crate) fn push_exit_is_upstream_fault(reason: &str) -> bool {
    !(reason.starts_with("node outbound ended")
        // serve_failover's node reader
        || reason.starts_with("node outbound read failed")
        // internal ends (a panicked task, the session's own channel or logger): nobody's fault
        || reason.starts_with("node outbound task join error")
        || reason.starts_with("active lag logger")
        || reason.starts_with("event channel closed")
        || reason.starts_with("node write")
        || reason.starts_with("node greeting write")
        || reason.starts_with("active superseded"))
}

// Per-session accounting of what the active stream forwarded and what the round gates dropped
// (W2a). A healthy single-peer stream never repeats a block round, so in steady state every drop
// is either a cache cold-start backlog frame (floor) or a frame block_round misreads as a block —
// for example a split_client_blocks mempool piece. Those are the numbers to check on swap day
// before trusting --push with QuickNode's mempool stream.
#[derive(Default)]
pub(crate) struct PushStats {
    pub(crate) control_fwd: AtomicU64,
    pub(crate) blocks_fwd: AtomicU64,
    // type-1 frames with no decodable round: forwarded untouched; non-zero = non-block data
    // (likely split mempool) is flowing
    pub(crate) data_noround_fwd: AtomicU64,
    pub(crate) bytes_fwd: AtomicU64,
    pub(crate) drop_floor: AtomicU64,
    pub(crate) drop_gate: AtomicU64,
    drop_samples: AtomicU64,
}

const PUSH_DROP_SAMPLES: u64 = 5;

impl PushStats {
    pub(crate) fn summary(&self) -> String {
        let blocks = self.blocks_fwd.load(Ordering::Relaxed);
        let noround = self.data_noround_fwd.load(Ordering::Relaxed);
        format!(
            "ctl={} blocks={blocks} data_noround={noround} bytes={} drop_floor={} drop_dup={}{}",
            self.control_fwd.load(Ordering::Relaxed),
            self.bytes_fwd.load(Ordering::Relaxed),
            self.drop_floor.load(Ordering::Relaxed),
            self.drop_gate.load(Ordering::Relaxed),
            no_block_rounds_alarm(blocks, noround),
        )
    }

    fn forwarded(&self, frame: &[u8], kind: FrameKindSeen) {
        let c = match kind {
            FrameKindSeen::Control => &self.control_fwd,
            FrameKindSeen::Block => &self.blocks_fwd,
            FrameKindSeen::DataNoRound => &self.data_noround_fwd,
        };
        c.fetch_add(1, Ordering::Relaxed);
        self.bytes_fwd
            .fetch_add(frame.len() as u64, Ordering::Relaxed);
    }

    // Count a drop and log the first few per session with enough of the frame to classify it
    // later (W2b): length, round, last forwarded/floor, and the first payload bytes.
    fn dropped(&self, why: &str, frame: &[u8], round: u32, last: u32, floor: u32) {
        match why {
            "floor" => &self.drop_floor,
            _ => &self.drop_gate,
        }
        .fetch_add(1, Ordering::Relaxed);
        if self.drop_samples.fetch_add(1, Ordering::Relaxed) < PUSH_DROP_SAMPLES {
            let head: String = frame
                .iter()
                .skip(5)
                .take(24)
                .map(|b| format!("{b:02x}"))
                .collect();
            gwlog!(
                "[gw] push drop sample reason={why} len={} round={round} last_forwarded={last} floor={floor} payload_head={head}",
                frame.len().saturating_sub(5)
            );
        }
    }
}

// Plenty of data frames but not one decodable block round means block_round no longer matches
// the wire format (it happened once: HL moved the round between 2026-07 and 2026-10). Every
// round-based guard (vetting, lag, floor) is blind until the decoder is fixed.
fn no_block_rounds_alarm(blocks: u64, noround: u64) -> &'static str {
    if blocks == 0 && noround >= 100 {
        " ALERT=no_block_rounds_decoded(wire format change?)"
    } else {
        ""
    }
}

#[derive(Clone, Copy)]
enum FrameKindSeen {
    Control,
    Block,
    DataNoRound,
}

fn spawn_active_pump(
    act_r: OwnedReadHalf,
    tx: NodeQueue,
    forward_gate: Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: Arc<AtomicU32>,
    net_round_tip: Arc<AtomicU32>,
    active_round: Arc<AtomicU32>,
    stats: Arc<PushStats>,
) -> tokio::task::JoinHandle<std::io::Result<()>> {
    tokio::spawn(async move {
        pump_merge(
            act_r,
            tx,
            forward_gate,
            live_floor,
            net_round_tip,
            active_round,
            true,
            &stats,
        )
        .await
    })
}

pub(crate) async fn serve_push(node: TcpStream, active_conn: TcpStream, cfg: PushConfig) -> String {
    let forward_gate = Arc::new(tokio::sync::Mutex::new(
        RoundForwardGate::with_last_forwarded(16_384, cfg.initial_last_forwarded),
    ));
    // Each queued item is a complete block/control frame, bounded by count AND bytes
    // (NODE_QUEUE_BYTES): a slow node writer backpressures the upstream reader instead of letting
    // queued frames grow to GBs.
    let (tx, mut rx) = NodeQueue::new(PUSH_MERGE_QUEUE_FRAMES);
    let (node_r, mut node_w) = node.into_split();
    let (mut act_r, act_w) = active_conn.into_split();
    let active_round = Arc::new(AtomicU32::new(cfg.initial_last_forwarded));
    let stats = Arc::new(PushStats::default());
    // The node's first read on 4001 is the peer's greeting ("abci_stream recv greeting", max 1000
    // bytes). Forward the active peer's greeting frame to the node BEFORE starting the shadow
    // injectors: they all share node_w, so a shadow peer's first live block can otherwise reach the
    // node ahead of the greeting, and the node reads the block's length as the greeting length and
    // bails ("tcp read bytes over limit").
    if let Some(greet) = cfg.prefetched_greeting {
        match timeout(NODE_WRITE_TIMEOUT, node_w.write_all(&greet)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return format!("node greeting write failed: {e}"),
            Err(_) => return "node greeting write failed: timeout".to_string(),
        }
    } else {
        // Bounded: the node->peer task that would notice the node leaving only starts after this,
        // so an upstream that accepts TCP but never answers would otherwise park the session (and
        // both sockets) forever, one more per node reconnect.
        let greet = match read_live_frame_with_timeout(&mut act_r, PUSH_GREETING_WAIT, 1000).await {
            Ok(g) => g,
            Err(e) => return format!("active greeting read failed: {e}"),
        };
        if response_is_peer_full(&greet) {
            return "active peer full".to_string();
        }
        match timeout(NODE_WRITE_TIMEOUT, node_w.write_all(&greet)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return format!("node greeting write failed: {e}"),
            Err(_) => return "node greeting write failed: timeout".to_string(),
        }
    }
    // node -> active peer (transparent: RPC requests + acks). Bounded write: a half-broken peer
    // that still sends blocks but stops draining node outbound would otherwise park write_all
    // forever, silently dropping acks/RPC. A write stall means the active is unusable — exit so
    // the session tears down and the node re-elects a fresh active.
    let mut up = {
        let mut node_r = node_r;
        let mut act_w = act_w;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match node_r.read(&mut buf).await {
                    Ok(0) | Err(_) => break "node outbound ended".to_string(),
                    Ok(n) => {
                        match timeout(Duration::from_secs(5), act_w.write_all(&buf[..n])).await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => break format!("node outbound write failed: {e}"),
                            Err(_) => break "node outbound write timeout".to_string(),
                        }
                    }
                }
            }
        })
    };
    for frame in cfg.prefetched_frames {
        if let Err(e) = process_live_frame(
            frame,
            &tx,
            &forward_gate,
            &cfg.live_floor,
            &cfg.net_round_tip,
            &active_round,
            true,
            &stats,
        )
        .await
        {
            // `up` is already running; don't leave the node->active copy behind on this early exit
            up.abort();
            return format!("prefetched frame processing failed: {e}");
        }
    }
    // Spawned only once the session is past every early return above: a detached JoinHandle keeps
    // the task running, so an early exit here would leak a forever-logging task per failed session.
    let mut lag_log = {
        let active_peer = cfg
            .hosts
            .get(cfg.active_idx)
            .cloned()
            .unwrap_or_else(|| "<unknown>".to_string());
        let active_round = active_round.clone();
        let net_round_tip = cfg.net_round_tip.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let local_round = active_round.load(Ordering::Acquire);
                let peers_max_round = net_round_tip.load(Ordering::Acquire);
                let delta = i64::from(peers_max_round) - i64::from(local_round);
                let lag = delta.max(0);
                gwlog!(
                    "[gw] active peer round lag active_peer={active_peer} local_round={local_round} peers_max_round={peers_max_round} delta={delta} lag={lag} max_lag={ACTIVE_ROUND_MAX_LAG} {}",
                    stats.summary()
                );
            }
        })
    };
    // active peer -> node: the transparent backbone (forwards non-block frames + blocks). If the
    // active stream stalls or dies the session is torn down so the node reconnects and a fresh
    // active is elected — a mid-session replacement peer would start at its own tip and hand the
    // node a block whose parent it never saw (invalid parent round). Shadow sources remain
    // disabled unless validation is added.
    let mut active_task = spawn_active_pump(
        act_r,
        tx.clone(),
        forward_gate.clone(),
        cfg.live_floor.clone(),
        cfg.net_round_tip.clone(),
        active_round.clone(),
        stats.clone(),
    );
    // every other peer -> node: live blocks only, deduped. Keep this off until shadow blocks are
    // validated beyond round number.
    let mut shadows = Vec::new();
    if ENABLE_SHADOW_PUSH {
        for (i, h) in cfg.hosts.iter().enumerate() {
            if i == cfg.active_idx {
                continue;
            }
            let target = format!("{}:{}", h, cfg.port);
            let tx = tx.clone();
            let forward_gate = forward_gate.clone();
            let live_floor = cfg.live_floor.clone();
            let net_round_tip = cfg.net_round_tip.clone();
            shadows.push(tokio::spawn(async move {
                // Reconnect with backoff: public peers drop their streams, and a shadow source that
                // exited permanently would silently degrade the merge from n_live sources to fewer.
                // Stop only when the node side is gone (the merge channel is closed).
                while !tx.is_closed() {
                    if let Ok(Ok(mut s)) =
                        timeout(Duration::from_secs(5), crate::blacklist::connect(&target)).await
                    {
                        s.set_nodelay(true).ok();
                        if s.write_all(&GREET_FALSE).await.is_ok() {
                            let (r, _w) = s.into_split();
                            let _ = pump_merge(
                                r,
                                tx.clone(),
                                forward_gate.clone(),
                                live_floor.clone(),
                                net_round_tip.clone(),
                                Arc::new(AtomicU32::new(0)),
                                false,
                                &PushStats::default(),
                            )
                            .await;
                        }
                    }
                    if tx.is_closed() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }));
        }
    }
    fn active_exit_reason(joined: Result<std::io::Result<()>, tokio::task::JoinError>) -> String {
        match joined {
            Ok(Ok(())) => "active stream ended".to_string(),
            Ok(Err(e)) => format!("active stream error: {e}"),
            Err(e) => format!("active task join error: {e}"),
        }
    }
    let superseded = wait_superseded(cfg.superseded);
    tokio::pin!(superseded);
    let active_host = cfg.hosts.get(cfg.active_idx).cloned().unwrap_or_default();
    let blacklisted = crate::blacklist::wait_blacklisted(&active_host);
    tokio::pin!(blacklisted);
    let exit_reason = loop {
        tokio::select! {
            _ = &mut superseded => break "active superseded by failover".to_string(),
            _ = &mut blacklisted => break "active peer blacklisted".to_string(),
            maybe = rx.recv() => match maybe {
                // Bounded like the node -> peer direction: a half-open node (no RST) would
                // otherwise park write_all until the kernel gives up (~15 min), holding the
                // active pin and the priority session the whole time.
                // the frame's budget permit is released when this arm ends, after the write
                Some((buf, _permit)) => match timeout(NODE_WRITE_TIMEOUT, node_w.write_all(&buf)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => break format!("node write failed: {e}"),
                    Err(_) => break "node write timeout".to_string(),
                },
                // Unreachable in practice: serve_push itself keeps its own tx alive, so the
                // channel never reports closed. Kept as a safe fallback that surfaces the
                // active pump's real exit reason.
                None => break active_exit_reason((&mut active_task).await),
            },
            joined = &mut up => break match joined {
                Ok(reason) => reason,
                Err(e) => format!("node outbound task join error: {e}"),
            },
            joined = &mut active_task => break active_exit_reason(joined),
            joined = &mut lag_log => break match joined {
                Ok(()) => "active lag logger ended".to_string(),
                Err(e) => format!("active lag logger task join error: {e}"),
            },
        }
    };
    up.abort();
    active_task.abort();
    lag_log.abort();
    for s in shadows {
        s.abort();
    }
    format!("{exit_reason} stats=[{}]", stats.summary())
}

// Frame reader for serve_push: forward block frames (type=1 with a round) deduped; if forward_nonblock
// (the active peer only) also forward control / abci_state / RPC frames as-is.
#[allow(clippy::too_many_arguments)]
async fn pump_merge(
    mut r: tokio::net::tcp::OwnedReadHalf,
    tx: NodeQueue,
    forward_gate: Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: Arc<AtomicU32>,
    net_round_tip: Arc<AtomicU32>,
    active_round: Arc<AtomicU32>,
    forward_nonblock: bool,
    stats: &PushStats,
) -> std::io::Result<()> {
    // Live blocks arrive continuously (~4-15/s on mainnet). Keep this below hl-node's own abci
    // read deadline so a stalled active tears the session down early and the node reconnects
    // immediately instead of waiting out its own timeout.
    const IDLE: Duration = Duration::from_secs(10);
    let mut progress = BlockProgress::new(active_round.load(Ordering::Acquire), Instant::now());
    loop {
        if forward_nonblock && progress.stalled(Instant::now()) {
            return Err(std::io::Error::other(format!(
                "no block progress for {}s (last round {})",
                ACTIVE_BLOCK_STALL.as_secs(),
                progress.last_round
            )));
        }
        let mut hdr = [0u8; 5];
        match timeout(IDLE, r.read_exact(&mut hdr)).await {
            Ok(res) => res?,
            Err(_) => return Err(std::io::Error::other("idle timeout")),
        };
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        if len > MAX_LIVE_FRAME {
            // Legit frames on a live (send_abci:false) channel are ~200KB (greeting + blocks).
            // From the ACTIVE peer an oversized length is a broken/hostile stream — error out so
            // the session tears down (never allocate a peer-controlled multi-GB buffer). From a
            // shadow, drain to stay frame-aligned and keep the accelerator alive.
            if forward_nonblock {
                return Err(std::io::Error::other("oversized frame from active peer"));
            }
            let mut rem = len;
            let mut buf = vec![0u8; 65536];
            while rem > 0 {
                let nb = match timeout(IDLE, r.read(&mut buf[..rem.min(65536)])).await {
                    Ok(res) => res?,
                    Err(_) => return Err(std::io::Error::other("idle timeout (drain)")),
                };
                if nb == 0 {
                    return Ok(());
                }
                rem -= nb;
            }
            continue;
        }
        // Read the payload straight into a ready-to-send frame (header + payload): forwarding is
        // the common case, and rebuilding the frame from a separate payload Vec would cost an
        // extra allocation + full copy per live block.
        let mut frame = vec![0u8; 5 + len];
        frame[..5].copy_from_slice(&hdr);
        match timeout(IDLE, r.read_exact(&mut frame[5..])).await {
            Ok(res) => res?,
            Err(_) => return Err(std::io::Error::other("idle timeout (payload)")),
        };
        if typ == 1 {
            if let Some(rnd) = block_round(&frame[5..]) {
                let net_tip = net_round_tip.load(Ordering::Acquire);
                if !is_plausible_mainnet_round(rnd, net_tip) {
                    if forward_nonblock {
                        return Err(std::io::Error::other(format!(
                            "implausible active block round {rnd}"
                        )));
                    }
                    continue;
                }
                if forward_nonblock {
                    progress
                        .on_round(rnd, Instant::now())
                        .map_err(std::io::Error::other)?;
                    active_round.fetch_max(rnd, Ordering::AcqRel);
                    if let Some(lag) = round_lag_from_tip(rnd, net_tip, ACTIVE_ROUND_MAX_LAG) {
                        return Err(std::io::Error::other(format!(
                            "active block round lagged round={rnd} net_tip={net_tip} lag={lag} max_lag={ACTIVE_ROUND_MAX_LAG}"
                        )));
                    }
                }
                let floor = live_floor.load(Ordering::Acquire);
                if rnd <= floor {
                    if forward_nonblock {
                        let last = active_round.load(Ordering::Acquire);
                        stats.dropped("floor", &frame, rnd, last, floor);
                    }
                    continue;
                }
                let mut gate = forward_gate.lock().await;
                let last = gate.last_forwarded();
                if gate.should_forward(rnd, forward_nonblock) {
                    stats.forwarded(&frame, FrameKindSeen::Block);
                    if tx.send(frame).await.is_err() {
                        return Ok(());
                    }
                } else if forward_nonblock {
                    stats.dropped("dup", &frame, rnd, last, floor);
                }
                continue;
            }
            if forward_nonblock {
                stats.forwarded(&frame, FrameKindSeen::DataNoRound);
                if tx.send(frame).await.is_err() {
                    return Ok(());
                }
            }
            continue;
        }
        if forward_nonblock {
            stats.forwarded(&frame, FrameKindSeen::Control);
            if tx.send(frame).await.is_err() {
                return Ok(());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_live_frame(
    frame: Vec<u8>,
    tx: &NodeQueue,
    forward_gate: &Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: &Arc<AtomicU32>,
    net_round_tip: &Arc<AtomicU32>,
    active_round: &Arc<AtomicU32>,
    forward_nonblock: bool,
    stats: &PushStats,
) -> std::io::Result<()> {
    if frame.len() < 5 {
        return Err(std::io::Error::other("short prefetched frame"));
    }
    let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    let typ = frame[4];
    if len != frame.len().saturating_sub(5) {
        return Err(std::io::Error::other("mis-sized prefetched frame"));
    }
    let payload = &frame[5..];
    if typ == 1 {
        if let Some(rnd) = block_round(payload) {
            let net_tip = net_round_tip.load(Ordering::Acquire);
            if !is_plausible_mainnet_round(rnd, net_tip) {
                if forward_nonblock {
                    return Err(std::io::Error::other(format!(
                        "implausible active block round {rnd}"
                    )));
                }
                return Ok(());
            }
            if forward_nonblock {
                // the same jump guard pump_merge's BlockProgress applies to the live stream
                let prev = active_round.fetch_max(rnd, Ordering::AcqRel);
                if prev != 0 && rnd > prev.saturating_add(MAX_ACTIVE_ROUND_JUMP) {
                    return Err(std::io::Error::other(format!(
                        "active block round jump round={rnd} prev={prev} max_jump={MAX_ACTIVE_ROUND_JUMP}"
                    )));
                }
                if let Some(lag) = round_lag_from_tip(rnd, net_tip, ACTIVE_ROUND_MAX_LAG) {
                    return Err(std::io::Error::other(format!(
                        "active block round lagged round={rnd} net_tip={net_tip} lag={lag} max_lag={ACTIVE_ROUND_MAX_LAG}"
                    )));
                }
            }
            let floor = live_floor.load(Ordering::Acquire);
            if rnd <= floor {
                if forward_nonblock {
                    stats.dropped(
                        "floor",
                        &frame,
                        rnd,
                        active_round.load(Ordering::Acquire),
                        floor,
                    );
                }
                return Ok(());
            }
            let mut gate = forward_gate.lock().await;
            let last = gate.last_forwarded();
            if !gate.should_forward(rnd, forward_nonblock) {
                if forward_nonblock {
                    stats.dropped("dup", &frame, rnd, last, floor);
                }
                return Ok(());
            }
            stats.forwarded(&frame, FrameKindSeen::Block);
        } else if !forward_nonblock {
            return Ok(());
        } else {
            stats.forwarded(&frame, FrameKindSeen::DataNoRound);
        }
    } else if !forward_nonblock {
        return Ok(());
    } else {
        stats.forwarded(&frame, FrameKindSeen::Control);
    }
    tx.send(frame)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "node writer closed"))
}

// Transparent relay: local node <-> single upstream peer. Forwards the node's greeting
// (send_abci:true) so the upstream serves the full abci_state + live blocks; relays both ways.
// Used to let a fresh node fully sync THROUGH the gateway from a fast local upstream.
pub(crate) async fn run_relay(upstream: String) {
    // `upstream` is just the host (e.g. "172.18.0.1"); relay the whole HL port range 4000-4010,
    // each port transparently to host:<same port>. The node's bootstrap needs more than the block
    // channel: 4001 = blocks/abci heavy channel, 4002 = gossip RPC (query-height etc). Forwarding
    // only 4001 made the node's query-height RPC on 4002 time out, so it never reached bootstrap.
    let host = match upstream.rsplit_once(':') {
        Some((h, _)) => h.to_string(),
        None => upstream.clone(),
    };
    let mut handles = Vec::new();
    for p in 4000u16..=4010 {
        let host = host.clone();
        handles.push(tokio::spawn(async move {
            let l = match TcpListener::bind(("0.0.0.0", p)).await {
                Ok(l) => l,
                Err(e) => {
                    gwlog!("[relay] bind :{p}: {e}");
                    return;
                }
            };
            gwlog!("[relay] :{p} -> {host}:{p}");
            loop {
                let (down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let up = format!("{host}:{p}");
                tokio::spawn(async move {
                    let upc = match crate::blacklist::connect(&up).await {
                        Ok(s) => s,
                        Err(e) => {
                            gwlog!("[relay] :{p} {addr} upstream err {e}");
                            return;
                        }
                    };
                    down.set_nodelay(true).ok();
                    upc.set_nodelay(true).ok();
                    let (to_node, _) = splice(down, upc).await;
                    if to_node > 1_000_000 {
                        gwlog!("[relay] :{p} {addr} up->node {:.0}MB", to_node as f64 / 1e6);
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
    fn raw_proxy_blames_whichever_side_ended_first() {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_millis(1);
        // upstream closed its live stream, then the node (seeing EOF) closed: upstream's fault,
        // also when both had ended by the time anyone looked
        assert_eq!(
            raw_proxy_fault(4001, Some((true, t0)), Some((N2uEnd::Node, t1))),
            Some("upstream closed the stream")
        );
        assert_eq!(
            raw_proxy_fault(4001, Some((true, t0)), None),
            Some("upstream closed the stream")
        );
        // the node left first and the upstream closed after it: nobody to rotate out
        assert_eq!(
            raw_proxy_fault(4001, Some((true, t1)), Some((N2uEnd::Node, t0))),
            None
        );
        // the node stopped reading (write to it failed): not the upstream's fault
        assert_eq!(raw_proxy_fault(4001, Some((false, t0)), None), None);
        // side ports: an upstream closing after an RPC answer is normal, a stall is not
        assert_eq!(raw_proxy_fault(4002, Some((true, t0)), None), None);
        assert_eq!(
            raw_proxy_fault(4002, None, Some((N2uEnd::UpstreamGone, t0))),
            None
        );
        assert_eq!(
            raw_proxy_fault(4002, None, Some((N2uEnd::UpstreamStalled, t0))),
            Some("upstream stopped reading")
        );
        assert_eq!(
            raw_proxy_fault(4001, None, Some((N2uEnd::UpstreamGone, t0))),
            Some("upstream write failed")
        );
    }

    #[tokio::test]
    async fn proxy_first_frame_rejects_status_frames_and_oversize() {
        async fn upstream(reply: Vec<u8>) -> TcpStream {
            let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = l.local_addr().unwrap();
            tokio::spawn(async move {
                let (mut s, _) = l.accept().await.unwrap();
                let _ = s.write_all(&reply).await;
                tokio::time::sleep(Duration::from_secs(2)).await;
            });
            TcpStream::connect(addr).await.unwrap()
        }
        let wait = Duration::from_secs(2);
        // live: a greeting passes, a peer-full refusal is returned for the caller to rotate on
        let mut c = upstream(vec![0, 0, 0, 1, 0, 3]).await;
        assert_eq!(
            read_first_frame(&mut c, wait, false).await.unwrap(),
            [0, 0, 0, 1, 0, 3]
        );
        let mut c = upstream(vec![0, 0, 0, 1, 0, 4]).await;
        assert!(response_is_peer_full(
            &read_first_frame(&mut c, wait, false).await.unwrap()
        ));
        // live: an oversized first frame is refused without reading it
        let mut c = upstream(vec![0, 0x10, 0, 0, 1]).await;
        assert!(read_first_frame(&mut c, wait, false).await.is_err());
        // bootstrap: a small status frame (rate limited / not serving) is a refusal, not state
        let mut c = upstream(vec![0, 0, 0, 2, 0, 9, 9]).await;
        assert!(read_first_frame(&mut c, wait, true).await.is_err());
        // bootstrap: a real abci_state frame header is returned (body relayed by the splice)
        let big = 1_300_000_000u32.to_be_bytes();
        let mut c = upstream(vec![big[0], big[1], big[2], big[3], 1]).await;
        assert_eq!(read_first_frame(&mut c, wait, true).await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn cancelled_splice_stops_relaying_the_old_upstream() {
        // a node connection and an upstream connection, each as a connected TCP pair
        async fn pair() -> (TcpStream, TcpStream) {
            let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let c = TcpStream::connect(l.local_addr().unwrap()).await.unwrap();
            (c, l.accept().await.unwrap().0)
        }
        let (mut node, node_gw) = pair().await;
        let (mut up_peer, up_gw) = pair().await;
        // the old upstream keeps streaming
        tokio::spawn(async move {
            while up_peer.write_all(&[7u8; 1024]).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        // a failover cancels the relay, as proxy_conn's select! does
        tokio::select! {
            _ = splice(node_gw, up_gw) => panic!("splice ended on its own"),
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        // the node must see the relay close, not keep receiving the old peer's bytes
        let mut buf = [0u8; 4096];
        let eof = timeout(Duration::from_secs(2), async {
            loop {
                match node.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        })
        .await;
        assert!(
            eof.is_ok(),
            "upstream->node copy kept running after the splice was dropped"
        );
    }

    #[test]
    fn block_progress_detects_jumps_and_stalls() {
        let t0 = Instant::now();
        let mut p = BlockProgress::new(0, t0);
        // nothing seen yet: never stalled (a decoder break must not tear sessions down)
        assert!(!p.stalled(t0 + ACTIVE_BLOCK_STALL * 10));
        // the first round sets the baseline, whatever it is
        assert!(p.on_round(1_490_000_000, t0).is_ok());
        // normal progress, and repeats / lower rounds don't count as progress
        assert!(p
            .on_round(1_490_000_015, t0 + Duration::from_secs(1))
            .is_ok());
        assert!(p
            .on_round(1_490_000_010, t0 + Duration::from_secs(20))
            .is_ok());
        assert!(!p.stalled(t0 + Duration::from_secs(30)));
        assert!(
            p.stalled(t0 + Duration::from_secs(1) + ACTIVE_BLOCK_STALL + Duration::from_millis(1))
        );
        // a jump past MAX_ACTIVE_ROUND_JUMP is refused and does not move the baseline
        let err = p
            .on_round(
                1_490_000_015 + MAX_ACTIVE_ROUND_JUMP + 1,
                t0 + Duration::from_secs(2),
            )
            .unwrap_err();
        assert!(err.contains("round jump"), "{err}");
        assert_eq!(p.last_round, 1_490_000_015);
        assert!(p
            .on_round(
                1_490_000_015 + MAX_ACTIVE_ROUND_JUMP,
                t0 + Duration::from_secs(2)
            )
            .is_ok());
    }

    #[test]
    fn push_exit_reasons_are_classified() {
        for node_side in [
            "node outbound ended stats=[]",
            "node write failed: broken pipe",
            "node write timeout",
            "node greeting write failed: timeout",
            "active superseded by failover",
        ] {
            assert!(!push_exit_is_upstream_fault(node_side), "{node_side}");
        }
        for upstream in [
            "active stream error: idle timeout",
            "active stream error: no block progress for 30s (last round 1)",
            "active greeting read failed: deadline has elapsed",
            "active peer full",
            "node outbound write timeout",
        ] {
            assert!(push_exit_is_upstream_fault(upstream), "{upstream}");
        }
    }

    fn make_live_block_frame(round: u32) -> Vec<u8> {
        let payload = crate::protocol::test_block_payload(round, 2);
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.push(1);
        frame.extend_from_slice(&payload);
        frame
    }

    // A prefetched block far above the session's baseline is refused before it reaches the node,
    // like the live stream's round-jump guard.
    #[tokio::test]
    async fn prefetched_block_round_jump_is_refused() {
        const BASE: u32 = 1_496_000_000;
        let node_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_addr = node_l.local_addr().unwrap();
        let fake_node = tokio::spawn(async move {
            let mut c = TcpStream::connect(node_addr).await.unwrap();
            let mut buf = Vec::new();
            c.read_to_end(&mut buf).await.unwrap();
            buf
        });
        let (node_conn, _) = node_l.accept().await.unwrap();
        let peer_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_l.local_addr().unwrap();
        tokio::spawn(async move {
            let (_s, _) = peer_l.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let active_conn = TcpStream::connect(peer_addr).await.unwrap();
        let payload = crate::protocol::test_block_payload(BASE + MAX_ACTIVE_ROUND_JUMP + 1, 2);
        let mut block = (payload.len() as u32).to_be_bytes().to_vec();
        block.push(1);
        block.extend_from_slice(&payload);
        let reason = serve_push(
            node_conn,
            active_conn,
            PushConfig {
                hosts: Arc::new(vec!["127.0.0.1".into()]),
                active_idx: 0,
                port: 4001,
                prefetched_greeting: Some(vec![0, 0, 0, 1, 0, 7]),
                prefetched_frames: vec![block],
                initial_last_forwarded: BASE,
                live_floor: Arc::new(AtomicU32::new(0)),
                net_round_tip: Arc::new(AtomicU32::new(0)),
                superseded: None,
            },
        )
        .await;
        assert!(reason.contains("active block round jump"), "{reason}");
        // the node got the greeting only
        assert_eq!(fake_node.await.unwrap(), [0, 0, 0, 1, 0, 7]);
    }

    // An active peer blacklisted mid-session ends the session (the node reconnects elsewhere),
    // though the peer itself is healthy and still connected.
    #[tokio::test]
    async fn push_session_ends_when_active_is_blacklisted() {
        let _g = crate::blacklist::TEST_LOCK.lock().await;
        let node_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_addr = node_l.local_addr().unwrap();
        let fake_node = tokio::spawn(async move {
            let mut c = TcpStream::connect(node_addr).await.unwrap();
            let mut buf = Vec::new();
            c.read_to_end(&mut buf).await.unwrap();
            buf
        });
        let (node_conn, _) = node_l.accept().await.unwrap();
        let peer_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_l.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut s, _) = peer_l.accept().await.unwrap();
            s.write_all(&[0, 0, 0, 1, 0, 7]).await.unwrap();
            // stays connected and silent until the gateway hangs up
            let mut b = [0u8; 64];
            while s.read(&mut b).await.is_ok_and(|n| n > 0) {}
        });
        let active_conn = TcpStream::connect(peer_addr).await.unwrap();
        crate::blacklist::install(&["198.51.100.40"]);
        let reason = timeout(
            Duration::from_secs(5),
            serve_push(
                node_conn,
                active_conn,
                PushConfig {
                    // the session's active host as the gateway knows it
                    hosts: Arc::new(vec!["198.51.100.40".into()]),
                    active_idx: 0,
                    port: 4001,
                    prefetched_greeting: None,
                    prefetched_frames: Vec::new(),
                    initial_last_forwarded: 0,
                    live_floor: Arc::new(AtomicU32::new(0)),
                    net_round_tip: Arc::new(AtomicU32::new(0)),
                    superseded: None,
                },
            ),
        )
        .await
        .expect("session ends promptly");
        crate::blacklist::install(&[]);
        assert!(reason.starts_with("active peer blacklisted"), "{reason}");
        // the node got the greeting, then EOF
        assert_eq!(fake_node.await.unwrap(), [0, 0, 0, 1, 0, 7]);
        timeout(Duration::from_secs(2), peer)
            .await
            .expect("upstream connection closed")
            .unwrap();
    }

    // Regression for the sample-420 soak alert: an active-peer death must tear the whole 4001
    // session down (node sees EOF and reconnects) — never hot-swap to another pool peer, whose
    // stream would start at its own tip and hand the node a block with an unseen parent round.
    #[tokio::test]
    async fn push_active_death_tears_session_down() {
        let node_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_addr = node_l.local_addr().unwrap();
        let fake_node = tokio::spawn(async move {
            let mut c = TcpStream::connect(node_addr).await.unwrap();
            let mut hdr = [0u8; 5];
            c.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
            let mut payload = vec![0u8; len];
            c.read_exact(&mut payload).await.unwrap();
            // after the greeting the next read must be EOF (session torn down), not a frame
            // injected from a replacement peer
            let mut b = [0u8; 1];
            c.read(&mut b).await.unwrap()
        });
        let (node_conn, _) = node_l.accept().await.unwrap();

        // a healthy pool peer that would have been the hot-swap target; it must never be dialed
        let standby_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let standby_port = standby_l.local_addr().unwrap().port();
        let standby = tokio::spawn(async move {
            timeout(Duration::from_secs(2), standby_l.accept())
                .await
                .is_ok()
        });

        let peer_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_l.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = peer_l.accept().await.unwrap();
            // greeting frame (len=1, type=0), then die
            s.write_all(&[0, 0, 0, 1, 0, 7]).await.unwrap();
        });
        let active_conn = TcpStream::connect(peer_addr).await.unwrap();

        let reason = serve_push(
            node_conn,
            active_conn,
            PushConfig {
                hosts: Arc::new(vec!["127.0.0.1".into(), "127.0.0.1".into()]),
                active_idx: 0,
                port: standby_port,
                prefetched_greeting: None,
                prefetched_frames: Vec::new(),
                initial_last_forwarded: 0,
                live_floor: Arc::new(AtomicU32::new(0)),
                net_round_tip: Arc::new(AtomicU32::new(0)),
                superseded: None,
            },
        )
        .await;
        assert!(
            reason.starts_with("active stream error"),
            "unexpected exit reason: {reason}"
        );
        assert_eq!(
            fake_node.await.unwrap(),
            0,
            "node must see EOF, not injected frames"
        );
        assert!(
            !standby.await.unwrap(),
            "standby pool peer must never be dialed"
        );
    }

    // W2a: the session reports what it forwarded and what the round gates dropped, so a split
    // mempool frame misread as a stale block shows up as drop_dup instead of vanishing silently.
    #[test]
    fn alarm_fires_only_when_frames_flow_without_block_rounds() {
        assert!(no_block_rounds_alarm(0, 99).is_empty());
        assert!(no_block_rounds_alarm(0, 100).contains("ALERT"));
        assert!(no_block_rounds_alarm(1, 5000).is_empty());
    }

    #[tokio::test]
    async fn push_session_counts_forwarded_and_dropped_frames() {
        let node_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_addr = node_l.local_addr().unwrap();
        let fake_node = tokio::spawn(async move {
            let mut c = TcpStream::connect(node_addr).await.unwrap();
            let mut got = Vec::new();
            let _ = c.read_to_end(&mut got).await;
            got
        });
        let (node_conn, _) = node_l.accept().await.unwrap();

        let round = 1_400_000_000;
        let block = make_live_block_frame(round);
        let noround = vec![0, 0, 0, 3, 1, 0xde, 0xad, 0xbf];
        let control = vec![0, 0, 0, 2, 0, 9, 9];
        let mut sent = vec![0, 0, 0, 1, 0, 7];
        sent.extend_from_slice(&block);
        sent.extend_from_slice(&noround);
        sent.extend_from_slice(&block); // same round again: dropped by the gate
        sent.extend_from_slice(&control);
        let upstream = sent.clone();
        let peer_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_l.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = peer_l.accept().await.unwrap();
            s.write_all(&upstream).await.unwrap();
            // stay open long enough for the queue to drain; serve_push does not flush queued
            // frames once the active stream has ended (the session is over either way)
            tokio::time::sleep(Duration::from_millis(300)).await;
        });
        let active_conn = TcpStream::connect(peer_addr).await.unwrap();

        let reason = serve_push(
            node_conn,
            active_conn,
            PushConfig {
                hosts: Arc::new(vec!["127.0.0.1".into()]),
                active_idx: 0,
                port: 4001,
                prefetched_greeting: None,
                prefetched_frames: Vec::new(),
                initial_last_forwarded: 0,
                live_floor: Arc::new(AtomicU32::new(0)),
                net_round_tip: Arc::new(AtomicU32::new(0)),
                superseded: None,
            },
        )
        .await;
        assert!(
            reason.contains("ctl=1 blocks=1 data_noround=1")
                && reason.contains("drop_floor=0 drop_dup=1"),
            "unexpected stats: {reason}"
        );
        let got = fake_node.await.unwrap();
        let mut want = vec![0, 0, 0, 1, 0, 7];
        want.extend_from_slice(&block);
        want.extend_from_slice(&noround);
        want.extend_from_slice(&control);
        assert_eq!(
            got, want,
            "everything but the duplicate block must reach the node, in order"
        );
    }

    #[tokio::test]
    async fn push_lagged_active_peer_tears_session_down() {
        let node_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_addr = node_l.local_addr().unwrap();
        let fake_node = tokio::spawn(async move {
            let mut c = TcpStream::connect(node_addr).await.unwrap();
            let mut hdr = [0u8; 5];
            c.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
            let mut payload = vec![0u8; len];
            c.read_exact(&mut payload).await.unwrap();
            let mut b = [0u8; 1];
            c.read(&mut b).await.unwrap()
        });
        let (node_conn, _) = node_l.accept().await.unwrap();

        let lagged_round = 1_356_000_000;
        let net_tip = lagged_round + ACTIVE_ROUND_MAX_LAG + 1;
        let peer_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_l.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = peer_l.accept().await.unwrap();
            s.write_all(&[0, 0, 0, 1, 0, 7]).await.unwrap();
            s.write_all(&make_live_block_frame(lagged_round))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let active_conn = TcpStream::connect(peer_addr).await.unwrap();

        let reason = serve_push(
            node_conn,
            active_conn,
            PushConfig {
                hosts: Arc::new(vec!["127.0.0.1".into()]),
                active_idx: 0,
                port: 4001,
                prefetched_greeting: None,
                prefetched_frames: Vec::new(),
                initial_last_forwarded: 0,
                live_floor: Arc::new(AtomicU32::new(0)),
                net_round_tip: Arc::new(AtomicU32::new(net_tip)),
                superseded: None,
            },
        )
        .await;
        assert!(
            reason.contains("active block round lagged"),
            "unexpected exit reason: {reason}"
        );
        assert_eq!(
            fake_node.await.unwrap(),
            0,
            "node must see EOF instead of a lagged block"
        );
    }
}
