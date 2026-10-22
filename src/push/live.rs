use crate::priority::gwlog;
use crate::protocol::{
    block_round, is_plausible_mainnet_round, read_live_frame_with_timeout, response_is_peer_full,
    round_lag_from_tip, RoundForwardGate, ACTIVE_ROUND_MAX_LAG, MAX_LIVE_FRAME,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

// Bidirectionally relay a node connection and its chosen upstream until either side closes.
// Returns (bytes upstream->node, bytes node->upstream). Writes to the node are bounded by
// NODE_WRITE_TIMEOUT and either direction ending ends the relay: a node that stops reading (hung,
// SIGSTOPped, half-open) would otherwise park the task — and the upstream connection, pin and
// priority session it holds — until TCP gives up, ~15 min later. The relay also ends once the
// upstream's IP is blacklisted (hot reload), so the node reconnects to an allowed peer.
pub(crate) async fn splice(down: TcpStream, upc: TcpStream) -> (u64, u64) {
    let r = splice_with_idle(down, upc, None).await;
    (r.to_node, r.to_peer)
}

// What a relay carried, and which side ended it.
pub(crate) struct Relayed {
    pub(crate) to_node: u64,
    pub(crate) to_peer: u64,
    // the upstream ended it (closed, failed, went silent, was blacklisted) — not the node leaving
    // or no longer reading
    pub(crate) upstream_ended: bool,
}

// splice, also ending once the upstream has sent nothing for `upstream_idle` — for 4001 relays,
// whose bootstrap transfer and live blocks flow continuously, so silence means a stalled peer
// (side ports carry sporadic gossip and get no such limit).
pub(crate) async fn splice_with_idle(
    down: TcpStream,
    upc: TcpStream,
    upstream_idle: Option<Duration>,
) -> Relayed {
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
            // true when the upstream side ended the copy, false when the node stopped taking it
            loop {
                let read = ur.read(&mut buf);
                let n = match upstream_idle {
                    Some(idle) => match timeout(idle, read).await {
                        Ok(r) => r,
                        Err(_) => break true,
                    },
                    None => read.await,
                };
                let n = match n {
                    Ok(0) | Err(_) => break true,
                    Ok(n) => n,
                };
                match timeout(NODE_WRITE_TIMEOUT, dw.write_all(&buf[..n])).await {
                    Ok(Ok(())) => {}
                    _ => break false,
                }
                to_node.fetch_add(n as u64, Ordering::Relaxed);
            }
        })
    });
    let (to_peer, upstream_ended) = tokio::select! {
        r = copy_node_to_upstream(&mut dr, &mut uw, RELAY_UPSTREAM_WRITE_TIMEOUT) => r,
        r = &mut h.0 => (0, r.unwrap_or(false)),
        _ = crate::blacklist::wait_blacklisted(&up_ip) => (0, true),
    };
    drop(h);
    Relayed {
        to_node: to_node.load(Ordering::Relaxed),
        to_peer,
        upstream_ended,
    }
}

// How long a write to an upstream may stall before the session treats the upstream as gone: it
// has stopped draining its socket while the node still has requests and acks for it. The push
// session and the proxy act on it fast; the plain relays (side ports, bootstrap) exist to attribute
// a dead upstream and never hang, so they allow brief backpressure.
pub(crate) const UPSTREAM_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const RELAY_UPSTREAM_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

// The node -> upstream half of a relay: (bytes copied, whether the upstream ended it). The node
// closing or failing a read ends it as the node's doing; a failed or stalled write to the upstream
// as the upstream's — tokio::io::copy can't tell the two apart, and has no write bound.
pub(crate) async fn copy_node_to_upstream(
    dr: &mut OwnedReadHalf,
    uw: &mut tokio::net::tcp::OwnedWriteHalf,
    write_timeout: Duration,
) -> (u64, bool) {
    let mut buf = vec![0u8; 65536];
    let mut copied = 0u64;
    loop {
        let n = match dr.read(&mut buf).await {
            Ok(0) | Err(_) => return (copied, false),
            Ok(n) => n,
        };
        match timeout(write_timeout, uw.write_all(&buf[..n])).await {
            Ok(Ok(())) => copied += n as u64,
            _ => return (copied, true),
        }
    }
}

// A spawned task that is aborted when its owner is dropped (a plain JoinHandle detaches).
pub(crate) struct AbortOnDrop<T>(pub(crate) tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// Block-push serving (push mode, heavy channel): the active peer connection is transparent for the
// node's outbound and the active peer's control / abci_state / RPC frames. Only the active peer
// feeds the node: merging blocks from other peers by round alone cannot prove proposer/fork
// correctness, and a wrong block can poison the node even when the active peer is healthy.
const PUSH_MERGE_QUEUE_FRAMES: usize = 256;

// Generous: the node normally drains 4001 continuously; only a stuck/half-open node hits this.
pub(crate) const NODE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

// An upstream greeting reply is a few bytes sent right away; hl-node itself gives up ~5s after
// greeting, so a longer wait only serves a dead connection.
pub(super) const PUSH_GREETING_WAIT: Duration = Duration::from_secs(4);

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
    // the caller's own reason to end the session (e.g. returning to a priority peer): resolves
    // with the exit reason; polled with the session's other ends so it shuts down cleanly
    pub(crate) end_when: Option<Pin<Box<dyn Future<Output = String> + Send>>>,
}

// Resolves once the proxy's failover generation moves past the session's; never without one.
pub(super) async fn wait_superseded(superseded: Option<(Arc<AtomicU64>, u64)>) {
    let Some((generation, at_start)) = superseded else {
        return std::future::pending().await;
    };
    while generation.load(Ordering::Relaxed) == at_start {
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

// serve_push exit reasons that are the NODE's doing (it left, or stopped reading) or a proxy
// failover; anything else is the active upstream's fault and should rotate it out.
// The exit reason of a session ended to move its node back to a priority peer (the pool peer it
// leaves did nothing wrong).
pub(crate) const RETURNING_TO_PRIORITY: &str = "returning to a priority peer";

pub(crate) fn push_exit_is_upstream_fault(reason: &str) -> bool {
    !(reason.starts_with("node outbound ended")
        // serve_failover's node reader
        || reason.starts_with("node outbound read failed")
        // internal ends (a panicked task, the session's own channel or logger): nobody's fault
        || reason.starts_with("node outbound task join error")
        || reason.starts_with("active task join error")
        || reason.starts_with("active lag logger")
        || reason.starts_with("event channel closed")
        || reason.starts_with("node write")
        || reason.starts_with("node greeting write")
        || reason.starts_with("active superseded")
        || reason.starts_with(RETURNING_TO_PRIORITY))
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
    // bytes): forward the active peer's greeting frame before any block, or the node reads a
    // block's length as the greeting length and bails ("tcp read bytes over limit").
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
    // spawned tasks are aborted on drop too, so cancelling this future can't leave them running
    let mut up = AbortOnDrop({
        let mut node_r = node_r;
        let mut act_w = act_w;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match node_r.read(&mut buf).await {
                    Ok(0) | Err(_) => break "node outbound ended".to_string(),
                    Ok(n) => {
                        match timeout(UPSTREAM_WRITE_TIMEOUT, act_w.write_all(&buf[..n])).await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => break format!("node outbound write failed: {e}"),
                            Err(_) => break "node outbound write timeout".to_string(),
                        }
                    }
                }
            }
        })
    });
    for frame in cfg.prefetched_frames {
        if let Err(e) = process_live_frame(
            frame,
            &tx,
            &forward_gate,
            &cfg.live_floor,
            &cfg.net_round_tip,
            &active_round,
            &stats,
        )
        .await
        {
            // `up` is already running; don't leave the node->active copy behind on this early exit
            return format!("prefetched frame processing failed: {e}");
        }
    }
    // Spawned only once the session is past every early return above: a detached JoinHandle keeps
    // the task running, so an early exit here would leak a forever-logging task per failed session.
    let mut lag_log = AbortOnDrop({
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
    });
    // active peer -> node: the transparent backbone (forwards non-block frames + blocks). If the
    // active stream stalls or dies the session is torn down so the node reconnects and a fresh
    // active is elected — a mid-session replacement peer would start at its own tip and hand the
    // node a block whose parent it never saw (invalid parent round).
    let mut active_task = AbortOnDrop(spawn_active_pump(
        act_r,
        tx.clone(),
        forward_gate.clone(),
        cfg.live_floor.clone(),
        cfg.net_round_tip.clone(),
        active_round.clone(),
        stats.clone(),
    ));
    fn active_exit_reason(joined: Result<std::io::Result<()>, tokio::task::JoinError>) -> String {
        match joined {
            Ok(Ok(())) => "active stream ended".to_string(),
            Ok(Err(e)) => format!("active stream error: {e}"),
            Err(e) => format!("active task join error: {e}"),
        }
    }
    let superseded = wait_superseded(cfg.superseded);
    tokio::pin!(superseded);
    let end_signal = cfg.end_when;
    let end_when = async move {
        match end_signal {
            Some(end) => end.await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(end_when);
    let active_host = cfg.hosts.get(cfg.active_idx).cloned().unwrap_or_default();
    let blacklisted = crate::blacklist::wait_blacklisted(&active_host);
    tokio::pin!(blacklisted);
    let exit_reason = loop {
        tokio::select! {
            _ = &mut superseded => break "active superseded by failover".to_string(),
            _ = &mut blacklisted => break "active peer blacklisted".to_string(),
            reason = &mut end_when => break reason,
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
                None => break active_exit_reason((&mut active_task.0).await),
            },
            joined = &mut up.0 => break match joined {
                Ok(reason) => reason,
                Err(e) => format!("node outbound task join error: {e}"),
            },
            joined = &mut active_task.0 => break active_exit_reason(joined),
            joined = &mut lag_log.0 => break match joined {
                Ok(()) => "active lag logger ended".to_string(),
                Err(e) => format!("active lag logger task join error: {e}"),
            },
        }
    };
    drop((up, active_task, lag_log));
    format!("{exit_reason} stats=[{}]", stats.summary())
}

// The active peer's stream for serve_push: block frames (type=1 with a round) pass the round
// gates (plausibility, progress/jump, tip lag, node floor, dedup); control / abci_state / RPC
// frames are forwarded as-is.
async fn pump_merge(
    mut r: tokio::net::tcp::OwnedReadHalf,
    tx: NodeQueue,
    forward_gate: Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: Arc<AtomicU32>,
    net_round_tip: Arc<AtomicU32>,
    active_round: Arc<AtomicU32>,
    stats: &PushStats,
) -> std::io::Result<()> {
    // Live blocks arrive continuously (~4-15/s on mainnet). Keep this below hl-node's own abci
    // read deadline so a stalled active tears the session down early and the node reconnects
    // immediately instead of waiting out its own timeout.
    const IDLE: Duration = Duration::from_secs(10);
    let mut progress = BlockProgress::new(active_round.load(Ordering::Acquire), Instant::now());
    loop {
        if progress.stalled(Instant::now()) {
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
            // Legit frames on a live (send_abci:false) channel are ~200KB (greeting + blocks): an
            // oversized length is a broken/hostile stream — never allocate a peer-controlled
            // multi-GB buffer, end the session instead.
            return Err(std::io::Error::other("oversized frame from active peer"));
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
        let kind = if typ != 1 {
            FrameKindSeen::Control
        } else if let Some(rnd) = block_round(&frame[5..]) {
            let net_tip = net_round_tip.load(Ordering::Acquire);
            if !is_plausible_mainnet_round(rnd, net_tip) {
                return Err(std::io::Error::other(format!(
                    "implausible active block round {rnd}"
                )));
            }
            progress
                .on_round(rnd, Instant::now())
                .map_err(std::io::Error::other)?;
            active_round.fetch_max(rnd, Ordering::AcqRel);
            if let Some(lag) = round_lag_from_tip(rnd, net_tip, ACTIVE_ROUND_MAX_LAG) {
                return Err(std::io::Error::other(format!(
                    "active block round lagged round={rnd} net_tip={net_tip} lag={lag} max_lag={ACTIVE_ROUND_MAX_LAG}"
                )));
            }
            let floor = live_floor.load(Ordering::Acquire);
            if rnd <= floor {
                let last = active_round.load(Ordering::Acquire);
                stats.dropped("floor", &frame, rnd, last, floor);
                continue;
            }
            let mut gate = forward_gate.lock().await;
            let last = gate.last_forwarded();
            if !gate.should_forward(rnd, true) {
                stats.dropped("dup", &frame, rnd, last, floor);
                continue;
            }
            FrameKindSeen::Block
        } else {
            FrameKindSeen::DataNoRound
        };
        stats.forwarded(&frame, kind);
        if tx.send(frame).await.is_err() {
            return Ok(());
        }
    }
}

// One frame the active peer sent during selection (prefetched), through the same gates as
// pump_merge's live stream.
async fn process_live_frame(
    frame: Vec<u8>,
    tx: &NodeQueue,
    forward_gate: &Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: &Arc<AtomicU32>,
    net_round_tip: &Arc<AtomicU32>,
    active_round: &Arc<AtomicU32>,
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
    let kind = if typ != 1 {
        FrameKindSeen::Control
    } else if let Some(rnd) = block_round(&frame[5..]) {
        let net_tip = net_round_tip.load(Ordering::Acquire);
        if !is_plausible_mainnet_round(rnd, net_tip) {
            return Err(std::io::Error::other(format!(
                "implausible active block round {rnd}"
            )));
        }
        // the same jump guard pump_merge's BlockProgress applies to the live stream, checked
        // before the round is recorded so a refused jump never becomes the session's baseline
        let prev = active_round.load(Ordering::Acquire);
        if prev != 0 && rnd > prev.saturating_add(MAX_ACTIVE_ROUND_JUMP) {
            return Err(std::io::Error::other(format!(
                "active block round jump round={rnd} prev={prev} max_jump={MAX_ACTIVE_ROUND_JUMP}"
            )));
        }
        active_round.fetch_max(rnd, Ordering::AcqRel);
        if let Some(lag) = round_lag_from_tip(rnd, net_tip, ACTIVE_ROUND_MAX_LAG) {
            return Err(std::io::Error::other(format!(
                "active block round lagged round={rnd} net_tip={net_tip} lag={lag} max_lag={ACTIVE_ROUND_MAX_LAG}"
            )));
        }
        let floor = live_floor.load(Ordering::Acquire);
        if rnd <= floor {
            let last = active_round.load(Ordering::Acquire);
            stats.dropped("floor", &frame, rnd, last, floor);
            return Ok(());
        }
        let mut gate = forward_gate.lock().await;
        let last = gate.last_forwarded();
        if !gate.should_forward(rnd, true) {
            stats.dropped("dup", &frame, rnd, last, floor);
            return Ok(());
        }
        FrameKindSeen::Block
    } else {
        FrameKindSeen::DataNoRound
    };
    stats.forwarded(&frame, kind);
    tx.send(frame)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "node writer closed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    // A 4001 relay whose upstream goes silent ends (the node reconnects elsewhere), while one
    // whose upstream keeps sending is never cut by the idle limit.
    #[tokio::test]
    async fn relay_ends_when_the_upstream_goes_silent() {
        async fn relay(upstream_sends_every: Option<Duration>) -> (Instant, (u64, u64)) {
            let up_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let up_addr = up_l.local_addr().unwrap();
            tokio::spawn(async move {
                let (mut s, _) = up_l.accept().await.unwrap();
                s.write_all(b"first").await.unwrap();
                match upstream_sends_every {
                    Some(every) => loop {
                        tokio::time::sleep(every).await;
                        if s.write_all(b"x").await.is_err() {
                            break;
                        }
                    },
                    None => tokio::time::sleep(Duration::from_secs(10)).await,
                }
            });
            let node_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let node_addr = node_l.local_addr().unwrap();
            tokio::spawn(async move {
                let mut c = TcpStream::connect(node_addr).await.unwrap();
                let mut buf = [0u8; 64];
                while c.read(&mut buf).await.is_ok_and(|n| n > 0) {}
            });
            let (down, _) = node_l.accept().await.unwrap();
            let upc = TcpStream::connect(up_addr).await.unwrap();
            let t0 = Instant::now();
            let r = timeout(
                Duration::from_millis(1500),
                splice_with_idle(down, upc, Some(Duration::from_millis(300))),
            )
            .await;
            if let Ok(r) = &r {
                assert!(r.upstream_ended, "a silent upstream ended the relay");
            }
            (
                t0,
                r.map(|r| (r.to_node, r.to_peer)).unwrap_or((u64::MAX, 0)),
            )
        }
        let (t0, (to_node, _)) = relay(None).await;
        assert_eq!(to_node, 5, "relayed the first bytes, then ended on silence");
        // and says who ended it: the silent upstream, versus a node that left
        let (up_l, node_l) = (
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
        );
        let (up_addr, node_addr) = (up_l.local_addr().unwrap(), node_l.local_addr().unwrap());
        tokio::spawn(async move {
            let (_s, _) = up_l.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        tokio::spawn(async move {
            let c = TcpStream::connect(node_addr).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(c);
        });
        let (down, _) = node_l.accept().await.unwrap();
        let upc = TcpStream::connect(up_addr).await.unwrap();
        let r = splice_with_idle(down, upc, Some(Duration::from_secs(2))).await;
        assert!(!r.upstream_ended, "the node left first");
        assert!(t0.elapsed() < Duration::from_millis(1200));
        // steady traffic (every 100ms, under the 300ms limit) keeps the relay up past 1.5s
        let (_, (to_node, _)) = relay(Some(Duration::from_millis(100))).await;
        assert_eq!(to_node, u64::MAX, "still relaying when the test stopped it");
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
            "returning to a priority peer (1.2.3.4 is serving) stats=[]",
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
                prefetched_greeting: Some(vec![0, 0, 0, 1, 0, 7]),
                prefetched_frames: vec![block],
                initial_last_forwarded: BASE,
                live_floor: Arc::new(AtomicU32::new(0)),
                net_round_tip: Arc::new(AtomicU32::new(0)),
                superseded: None,
                end_when: None,
            },
        )
        .await;
        assert!(reason.contains("active block round jump"), "{reason}");
        // the node got the greeting only
        assert_eq!(fake_node.await.unwrap(), [0, 0, 0, 1, 0, 7]);
    }

    // An active peer blacklisted mid-session ends the session (the node reconnects elsewhere),
    // though the peer itself is healthy and still connected.
    // The caller's own end signal (returning to a priority peer) ends the session through the
    // normal exit path: the node sees EOF, the reason is the caller's.
    #[tokio::test]
    async fn push_session_ends_on_the_callers_signal() {
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
            let mut b = [0u8; 64];
            while s.read(&mut b).await.is_ok_and(|n| n > 0) {}
        });
        let active_conn = TcpStream::connect(peer_addr).await.unwrap();
        let reason = timeout(
            Duration::from_secs(3),
            serve_push(
                node_conn,
                active_conn,
                PushConfig {
                    hosts: Arc::new(vec!["127.0.0.1".into()]),
                    active_idx: 0,
                    prefetched_greeting: None,
                    prefetched_frames: Vec::new(),
                    initial_last_forwarded: 0,
                    live_floor: Arc::new(AtomicU32::new(0)),
                    net_round_tip: Arc::new(AtomicU32::new(0)),
                    superseded: None,
                    end_when: Some(Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        format!("{RETURNING_TO_PRIORITY} (test)")
                    })),
                },
            ),
        )
        .await
        .expect("ends on the signal");
        assert!(reason.starts_with(RETURNING_TO_PRIORITY), "{reason}");
        assert_eq!(fake_node.await.unwrap(), [0, 0, 0, 1, 0, 7]);
        timeout(Duration::from_secs(2), peer)
            .await
            .expect("upstream connection closed")
            .unwrap();
    }

    // The node -> upstream half tells who ended it: the node closing is the node's doing, an
    // upstream that stops reading (its write stalls) is the upstream's.
    #[tokio::test]
    async fn node_to_upstream_copy_says_who_ended_it() {
        async fn pair() -> (TcpStream, TcpStream) {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let a = TcpStream::connect(l.local_addr().unwrap()).await.unwrap();
            let (b, _) = l.accept().await.unwrap();
            (a, b)
        }
        let (mut node, gw_down) = pair().await;
        let (gw_up, mut upstream) = pair().await;
        node.write_all(b"abc").await.unwrap();
        drop(node);
        let (mut dr, _dw) = gw_down.into_split();
        let (_ur, mut uw) = gw_up.into_split();
        let r = copy_node_to_upstream(&mut dr, &mut uw, Duration::from_millis(200)).await;
        assert_eq!(r, (3, false));
        let mut got = [0u8; 3];
        upstream.read_exact(&mut got).await.unwrap();

        let (mut node, gw_down) = pair().await;
        let (gw_up, _upstream_never_reads) = pair().await;
        let writer = tokio::spawn(async move {
            let chunk = vec![0u8; 1 << 20];
            while node.write_all(&chunk).await.is_ok() {}
        });
        let (mut dr, _dw) = gw_down.into_split();
        let (_ur, mut uw) = gw_up.into_split();
        let (_, upstream_ended) = timeout(
            Duration::from_secs(10),
            copy_node_to_upstream(&mut dr, &mut uw, Duration::from_millis(200)),
        )
        .await
        .expect("a stalled upstream write ends the copy");
        assert!(upstream_ended);
        writer.abort();
    }

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
                    prefetched_greeting: None,
                    prefetched_frames: Vec::new(),
                    initial_last_forwarded: 0,
                    live_floor: Arc::new(AtomicU32::new(0)),
                    net_round_tip: Arc::new(AtomicU32::new(0)),
                    superseded: None,
                    end_when: None,
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
                prefetched_greeting: None,
                prefetched_frames: Vec::new(),
                initial_last_forwarded: 0,
                live_floor: Arc::new(AtomicU32::new(0)),
                net_round_tip: Arc::new(AtomicU32::new(0)),
                superseded: None,
                end_when: None,
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
                prefetched_greeting: None,
                prefetched_frames: Vec::new(),
                initial_last_forwarded: 0,
                live_floor: Arc::new(AtomicU32::new(0)),
                net_round_tip: Arc::new(AtomicU32::new(0)),
                superseded: None,
                end_when: None,
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
                prefetched_greeting: None,
                prefetched_frames: Vec::new(),
                initial_last_forwarded: 0,
                live_floor: Arc::new(AtomicU32::new(0)),
                net_round_tip: Arc::new(AtomicU32::new(net_tip)),
                superseded: None,
                end_when: None,
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
