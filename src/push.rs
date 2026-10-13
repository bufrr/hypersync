use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::priority::gwlog;
use crate::protocol::{
    block_round, is_plausible_mainnet_round, round_lag_from_tip, RoundDedup, RoundForwardGate,
    ACTIVE_ROUND_MAX_LAG, GREET_FALSE,
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
                Err(e) => eprintln!("[gw] upstream connect: {e}"),
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
    let mut s = timeout(Duration::from_secs(5), TcpStream::connect(&target)).await??;
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
                    eprintln!("[gw] upstream {ip}: first round = {r}");
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
// Returns (bytes upstream->node, bytes node->upstream); the first count is best-effort when the
// node side closes first.
pub(crate) async fn splice(down: TcpStream, upc: TcpStream) -> (u64, u64) {
    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = upc.into_split();
    let to_node = Arc::new(AtomicU64::new(0));
    let h = {
        let to_node = to_node.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 256 * 1024];
            loop {
                let n = match ur.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                if dw.write_all(&buf[..n]).await.is_err() {
                    break;
                }
                to_node.fetch_add(n as u64, Ordering::Relaxed);
            }
        })
    };
    let to_peer = tokio::io::copy(&mut dr, &mut uw).await.unwrap_or(0);
    h.abort();
    (to_node.load(Ordering::Relaxed), to_peer)
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

// Failover transparent proxy. Default (push=false): the local node dials the gateway (4000-4010,
// outbound) and the gateway transparently proxies to the ACTIVE upstream peer; a health monitor
// rotates to the next healthy peer on stall/dial-failure ("选择其他peer替代").
// With push=true: the heavy block channel (4001) runs serve_push — live blocks from the active peer
// are round-gated (dedup + live floor) before reaching the node, while the active connection stays
// transparent for the node's outbound + control/state/RPC frames. Shadow peers are compiled out
// (ENABLE_SHADOW_PUSH), so this does not merge blocks from other peers.
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
        eprintln!("[proxy] no upstreams given");
        return;
    }
    let hosts = Arc::new(hosts);
    let n = hosts.len();
    let active = Arc::new(AtomicUsize::new(0));
    let generation = Arc::new(AtomicU64::new(0));
    let base = std::time::Instant::now();
    let last_data_ms = Arc::new(AtomicU64::new(0));
    let bad_until: Arc<Vec<AtomicU64>> = Arc::new((0..n).map(|_| AtomicU64::new(0)).collect());
    eprintln!(
        "[proxy] upstreams={:?}, active={}, push={}",
        hosts, hosts[0], push
    );

    {
        let hosts = hosts.clone();
        let active = active.clone();
        let generation = generation.clone();
        let last_data_ms = last_data_ms.clone();
        let bad_until = bad_until.clone();
        tokio::spawn(async move {
            const STALL_MS: u64 = 20000;
            const COOLDOWN_MS: u64 = 30000;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = base.elapsed().as_millis() as u64;
                let ld = last_data_ms.load(Ordering::Relaxed);
                if ld != 0 && now.saturating_sub(ld) > STALL_MS {
                    let old = active.load(Ordering::Relaxed);
                    bad_until[old].store(now + COOLDOWN_MS, Ordering::Relaxed);
                    if let Some(j) = pick_next(old, &bad_until, now, n) {
                        active.store(j, Ordering::Relaxed);
                        generation.fetch_add(1, Ordering::Relaxed);
                        last_data_ms.store(now, Ordering::Relaxed);
                        eprintln!(
                            "[proxy] STALL {}ms on {} -> failover -> {}",
                            now - ld,
                            hosts[old],
                            hosts[j]
                        );
                    } else {
                        last_data_ms.store(now, Ordering::Relaxed); // no healthy alternative; keep current
                    }
                }
            }
        });
    }

    let mut handles = Vec::new();
    for p in 4000u16..=4010 {
        let hosts = hosts.clone();
        let active = active.clone();
        let generation = generation.clone();
        let last_data_ms = last_data_ms.clone();
        let bad_until = bad_until.clone();
        handles.push(tokio::spawn(async move {
            let l = match TcpListener::bind(("0.0.0.0", p)).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[proxy] bind :{p}: {e}");
                    return;
                }
            };
            loop {
                let (down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let hosts = hosts.clone();
                let active = active.clone();
                let generation = generation.clone();
                let last_data_ms = last_data_ms.clone();
                let bad_until = bad_until.clone();
                tokio::spawn(async move {
                    let cur_gen = generation.load(Ordering::Relaxed);
                    let idx = active.load(Ordering::Relaxed);
                    let up = format!("{}:{}", hosts[idx], p);
                    let upc = match timeout(Duration::from_secs(4), TcpStream::connect(&up)).await {
                        Ok(Ok(s)) => s,
                        _ => {
                            let now = base.elapsed().as_millis() as u64;
                            bad_until[idx].store(now + 30000, Ordering::Relaxed);
                            if let Some(j) = pick_next(idx, &bad_until, now, hosts.len()) {
                                active.store(j, Ordering::Relaxed);
                                generation.fetch_add(1, Ordering::Relaxed);
                                eprintln!(
                                    "[proxy] dial {} failed -> failover -> {}",
                                    hosts[idx], hosts[j]
                                );
                            } else {
                                eprintln!(
                                    "[proxy] dial {} failed, no healthy alternative",
                                    hosts[idx]
                                );
                            }
                            return;
                        }
                    };
                    down.set_nodelay(true).ok();
                    upc.set_nodelay(true).ok();
                    if push && p == 4001 {
                        // serve_push assumes the node's greeting has ALREADY been forwarded to the
                        // active peer (its first act is reading the peer's greeting reply, and the
                        // node->peer copy task only starts after that). Forward it here, like
                        // run_gateway does — otherwise peer waits for the greeting, gateway waits
                        // for the peer's reply, and the handshake deadlocks (review finding 1).
                        let mut down = down;
                        let mut upc = upc;
                        let mut greet = [0u8; 8];
                        if !matches!(
                            timeout(Duration::from_secs(20), down.read_exact(&mut greet)).await,
                            Ok(Ok(_))
                        ) {
                            return;
                        }
                        if upc.write_all(&greet).await.is_err() {
                            return;
                        }
                        if greet[5] == 1 {
                            // bootstrap (send_abci:true): transparent splice. Multi-source injection
                            // can't help mid-bootstrap (the abci_state/EVM-KVs boundaries aren't
                            // detectable), and serve_push's greeting gate would reject the >4MB
                            // state frame the peer sends first.
                            splice(down, upc).await;
                        } else {
                            // live channel: round-gated blocks from the active peer (shadows are
                            // compiled out); active stays transparent for node->peer + control/RPC.
                            let active_peer = hosts[idx].clone();
                            let exit_reason = serve_push(
                                down,
                                upc,
                                PushConfig {
                                    hosts: hosts.clone(),
                                    active_idx: idx,
                                    port: p,
                                    prefetched_greeting: None,
                                    prefetched_frames: Vec::new(),
                                    initial_last_forwarded: 0,
                                    live_floor: Arc::new(AtomicU32::new(0)),
                                    // proxy mode has no peerd tip: floor-only plausibility
                                    net_round_tip: Arc::new(AtomicU32::new(0)),
                                },
                            )
                            .await;
                            eprintln!(
                                "[proxy] :{p} {addr} push session ended active_peer={} reason={}",
                                active_peer, exit_reason
                            );
                        }
                        return;
                    }
                    let (mut dr, mut dw) = down.into_split();
                    let (mut ur, mut uw) = upc.into_split();
                    let ld = last_data_ms.clone();
                    let up2 = tokio::spawn(async move {
                        let mut buf = vec![0u8; 262144];
                        loop {
                            match ur.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(nb) => {
                                    ld.store(base.elapsed().as_millis() as u64, Ordering::Relaxed);
                                    if dw.write_all(&buf[..nb]).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    });
                    let n2u = tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut dr, &mut uw).await;
                    });
                    loop {
                        if generation.load(Ordering::Relaxed) != cur_gen {
                            break;
                        }
                        if up2.is_finished() || n2u.is_finished() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                    up2.abort();
                    n2u.abort();
                });
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

// Block-push serving (push mode, heavy channel): the active peer connection is transparent for the
// node's outbound and the active peer's control / abci_state / RPC frames. Shadow peer injection is
// compiled out (ENABLE_SHADOW_PUSH = false, no runtime switch): round-only dedup cannot prove proposer/fork correctness, and a wrong shadow
// block can poison the node even when the active transparent peer is healthy.
const PUSH_MERGE_QUEUE_FRAMES: usize = 256;
const ENABLE_SHADOW_PUSH: bool = false;
// Generous: the node normally drains 4001 continuously; only a stuck/half-open node hits this.
const NODE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

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
    tx: mpsc::Sender<Vec<u8>>,
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
    // Each queued item is a complete block/control frame. Keep this small enough that a slow node
    // writer backpressures upstream readers instead of allowing multi-GB queued Vecs.
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(PUSH_MERGE_QUEUE_FRAMES);
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
        if let Err(e) = node_w.write_all(&greet).await {
            return format!("node greeting write failed: {e}");
        }
    } else {
        let mut hdr = [0u8; 5];
        if let Err(e) = act_r.read_exact(&mut hdr).await {
            return format!("active greeting header read failed: {e}");
        }
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if len > 1000 {
            return format!("active greeting oversized len={len}");
        }
        let mut g = vec![0u8; len];
        if let Err(e) = act_r.read_exact(&mut g).await {
            return format!("active greeting payload read failed: {e}");
        }
        let mut greet = hdr.to_vec();
        greet.extend_from_slice(&g);
        if let Err(e) = node_w.write_all(&greet).await {
            return format!("node greeting write failed: {e}");
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
                        timeout(Duration::from_secs(5), TcpStream::connect(&target)).await
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
    let exit_reason = loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                // Bounded like the node -> peer direction: a half-open node (no RST) would
                // otherwise park write_all until the kernel gives up (~15 min), holding the
                // active pin and the priority session the whole time.
                Some(buf) => match timeout(NODE_WRITE_TIMEOUT, node_w.write_all(&buf)).await {
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
    tx: mpsc::Sender<Vec<u8>>,
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
    loop {
        let mut hdr = [0u8; 5];
        match timeout(IDLE, r.read_exact(&mut hdr)).await {
            Ok(res) => res?,
            Err(_) => return Err(std::io::Error::other("idle timeout")),
        };
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        if len > 60_000_000 {
            // Legit frames on a live (send_abci:false) channel are <=~1MB (greeting + blocks).
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
    tx: &mpsc::Sender<Vec<u8>>,
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
                    eprintln!("[relay] bind :{p}: {e}");
                    return;
                }
            };
            eprintln!("[relay] :{p} -> {host}:{p}");
            loop {
                let (down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let up = format!("{host}:{p}");
                tokio::spawn(async move {
                    let upc = match TcpStream::connect(&up).await {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("[relay] :{p} {addr} upstream err {e}");
                            return;
                        }
                    };
                    down.set_nodelay(true).ok();
                    upc.set_nodelay(true).ok();
                    let (mut dr, mut dw) = down.into_split();
                    let (mut ur, mut uw) = upc.into_split();
                    let h = tokio::spawn(async move {
                        let mut buf = vec![0u8; 262144];
                        let mut total = 0u64;
                        loop {
                            let n = match ur.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            if dw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                            total += n as u64;
                        }
                        if total > 1_000_000 {
                            eprintln!("[relay] :{p} {addr} up->node {:.0}MB", total as f64 / 1e6);
                        }
                    });
                    let _ = tokio::io::copy(&mut dr, &mut uw).await;
                    let _ = h.await;
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

    fn make_live_block_frame(round: u32) -> Vec<u8> {
        let payload = crate::protocol::test_block_payload(round, 2);
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.push(1);
        frame.extend_from_slice(&payload);
        frame
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
