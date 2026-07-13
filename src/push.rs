use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::protocol::{
    block_round, is_plausible_mainnet_round, RoundDedup, RoundForwardGate, GREET_FALSE,
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
pub(crate) async fn splice(down: TcpStream, upc: TcpStream) {
    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = upc.into_split();
    let h = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut ur, &mut dw).await;
    });
    let _ = tokio::io::copy(&mut dr, &mut uw).await;
    h.abort();
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
// With push=true: on the heavy block channel (4001) the gateway ALSO connects to all other peers and
// merge-pushes their live blocks (round-dedup, fastest-first) for lower block-reception latency; the
// active peer connection stays transparent for the node's outbound + control/state/RPC frames.
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
                            // live channel: merge blocks from active + all other peers (round-dedup,
                            // fastest-first); active stays transparent for node->peer + control/RPC.
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
// disabled by default: round-only dedup cannot prove proposer/fork correctness, and a wrong shadow
// block can poison the node even when the active transparent peer is healthy.
const PUSH_MERGE_QUEUE_FRAMES: usize = 256;
const ENABLE_SHADOW_PUSH: bool = false;

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

fn spawn_active_pump(
    act_r: OwnedReadHalf,
    tx: mpsc::Sender<Vec<u8>>,
    forward_gate: Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: Arc<AtomicU32>,
    net_round_tip: Arc<AtomicU32>,
) -> tokio::task::JoinHandle<std::io::Result<()>> {
    tokio::spawn(async move {
        pump_merge(act_r, tx, forward_gate, live_floor, net_round_tip, true).await
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
            true,
        )
        .await
        {
            // `up` is already running; don't leave the node->active copy behind on this early exit
            up.abort();
            return format!("prefetched frame processing failed: {e}");
        }
    }
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
                                false,
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
                Some(buf) => {
                    if let Err(e) = node_w.write_all(&buf).await {
                        break format!("node write failed: {e}");
                    }
                }
                // Unreachable in practice: serve_push itself keeps a tx clone alive, so the
                // channel never reports closed. Kept as a safe fallback that surfaces the
                // active pump's real exit reason.
                None => break active_exit_reason((&mut active_task).await),
            },
            joined = &mut up => break match joined {
                Ok(reason) => reason,
                Err(e) => format!("node outbound task join error: {e}"),
            },
            joined = &mut active_task => break active_exit_reason(joined),
        }
    };
    up.abort();
    active_task.abort();
    for s in shadows {
        s.abort();
    }
    exit_reason
}

// Frame reader for serve_push: forward block frames (type=1 with a round) deduped; if forward_nonblock
// (the active peer only) also forward control / abci_state / RPC frames as-is.
async fn pump_merge(
    mut r: tokio::net::tcp::OwnedReadHalf,
    tx: mpsc::Sender<Vec<u8>>,
    forward_gate: Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: Arc<AtomicU32>,
    net_round_tip: Arc<AtomicU32>,
    forward_nonblock: bool,
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
                if !is_plausible_mainnet_round(rnd, net_round_tip.load(Ordering::Acquire)) {
                    if forward_nonblock {
                        return Err(std::io::Error::other(format!(
                            "implausible active block round {rnd}"
                        )));
                    }
                    continue;
                }
                if rnd <= live_floor.load(Ordering::Acquire) {
                    continue;
                }
                let mut gate = forward_gate.lock().await;
                if gate.should_forward(rnd, forward_nonblock) && tx.send(frame).await.is_err() {
                    return Ok(());
                }
                continue;
            }
            if forward_nonblock && tx.send(frame).await.is_err() {
                return Ok(());
            }
            continue;
        }
        if forward_nonblock && tx.send(frame).await.is_err() {
            return Ok(());
        }
    }
}

async fn process_live_frame(
    frame: Vec<u8>,
    tx: &mpsc::Sender<Vec<u8>>,
    forward_gate: &Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: &Arc<AtomicU32>,
    net_round_tip: &Arc<AtomicU32>,
    forward_nonblock: bool,
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
            if !is_plausible_mainnet_round(rnd, net_round_tip.load(Ordering::Acquire)) {
                if forward_nonblock {
                    return Err(std::io::Error::other(format!(
                        "implausible active block round {rnd}"
                    )));
                }
                return Ok(());
            }
            if rnd <= live_floor.load(Ordering::Acquire) {
                return Ok(());
            }
            let mut gate = forward_gate.lock().await;
            if !gate.should_forward(rnd, forward_nonblock) {
                return Ok(());
            }
        } else if !forward_nonblock {
            return Ok(());
        }
    } else if !forward_nonblock {
        return Ok(());
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
}
