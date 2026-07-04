use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::protocol::{block_round, RoundDedup, RoundForwardGate, GREET_FALSE};

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
        let mut payload = vec![0u8; len];
        s.read_exact(&mut payload).await?;
        if typ != 1 {
            continue;
        }
        match block_round(&payload) {
            Some(r) => {
                if first {
                    eprintln!("[gw] upstream {ip}: first round = {r}");
                    first = false;
                }
                if dedup.is_new(r) {
                    let mut frame = hdr.to_vec();
                    frame.extend_from_slice(&payload);
                    if tx.send(frame).await.is_err() {
                        return Ok(());
                    }
                }
            }
            None => {
                let mut frame = hdr.to_vec();
                frame.extend_from_slice(&payload);
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
                let (down, _addr) = match l.accept().await {
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
                            serve_push(
                                down,
                                upc,
                                PushConfig {
                                    hosts: hosts.clone(),
                                    active_idx: idx,
                                    port: p,
                                    prefetched_greeting: None,
                                    initial_last_forwarded: 0,
                                    live_floor: Arc::new(AtomicU32::new(0)),
                                },
                            )
                            .await;
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
    pub(crate) initial_last_forwarded: u32,
    pub(crate) live_floor: Arc<AtomicU32>,
}

pub(crate) async fn serve_push(node: TcpStream, active_conn: TcpStream, cfg: PushConfig) {
    let forward_gate = Arc::new(tokio::sync::Mutex::new(
        RoundForwardGate::with_last_forwarded(16_384, cfg.initial_last_forwarded),
    ));
    // Each queued item is a complete block/control frame. Keep this small enough that a slow node
    // writer backpressures upstream readers instead of allowing multi-GB queued Vecs.
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(PUSH_MERGE_QUEUE_FRAMES);
    let (node_r, mut node_w) = node.into_split();
    let (mut act_r, mut act_w) = active_conn.into_split();
    // The node's first read on 4001 is the peer's greeting ("abci_stream recv greeting", max 1000
    // bytes). Forward the active peer's greeting frame to the node BEFORE starting the shadow
    // injectors: they all share node_w, so a shadow peer's first live block can otherwise reach the
    // node ahead of the greeting, and the node reads the block's length as the greeting length and
    // bails ("tcp read bytes over limit").
    if let Some(greet) = cfg.prefetched_greeting {
        if node_w.write_all(&greet).await.is_err() {
            return;
        }
    } else {
        let mut hdr = [0u8; 5];
        if act_r.read_exact(&mut hdr).await.is_err() {
            return;
        }
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if len > 1000 {
            return; // first frame from a live (send_abci:false) peer must be the small greeting
        }
        let mut g = vec![0u8; len];
        if act_r.read_exact(&mut g).await.is_err() {
            return;
        }
        let mut greet = hdr.to_vec();
        greet.extend_from_slice(&g);
        if node_w.write_all(&greet).await.is_err() {
            return;
        }
    }
    // node -> active peer (transparent: RPC requests + acks)
    let up = {
        let mut node_r = node_r;
        tokio::spawn(async move {
            let _ = tokio::io::copy(&mut node_r, &mut act_w).await;
        })
    };
    // active peer -> node: the transparent backbone (forwards non-block frames + blocks). When this
    // task ends the active peer's stream broke, so we tear the whole session down and let the node
    // reconnect — the gateway then pins a FRESH active (round-robin). This is the proxy-style failover;
    // shadow sources remain disabled unless validation is added.
    let mut active_task = {
        let tx = tx.clone();
        let forward_gate = forward_gate.clone();
        let live_floor = cfg.live_floor.clone();
        tokio::spawn(async move {
            let _ = pump_merge(act_r, tx, forward_gate, live_floor, true).await;
        })
    };
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
    drop(tx);
    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(buf) => {
                    if node_w.write_all(&buf).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
            // active (backbone) peer died -> tear down; the node reconnects to a fresh active
            _ = &mut active_task => break,
        }
    }
    up.abort();
    active_task.abort();
    for s in shadows {
        s.abort();
    }
}

// Frame reader for serve_push: forward block frames (type=1 with a round) deduped; if forward_nonblock
// (the active peer only) also forward control / abci_state / RPC frames as-is.
async fn pump_merge(
    mut r: tokio::net::tcp::OwnedReadHalf,
    tx: mpsc::Sender<Vec<u8>>,
    forward_gate: Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: Arc<AtomicU32>,
    forward_nonblock: bool,
) -> std::io::Result<()> {
    // Live blocks arrive continuously (~4-15/s on mainnet), so >30s of silence means a stalled
    // connection. Without this idle timeout a silently-stalled-but-open ACTIVE peer would hang
    // the backbone forever (teardown only fired on EOF/RST); erroring out here makes serve_push
    // tear the session down so the node reconnects to a fresh active, and makes a stalled shadow
    // fall into its reconnect loop.
    const IDLE: Duration = Duration::from_secs(30);
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
        let mut payload = vec![0u8; len];
        match timeout(IDLE, r.read_exact(&mut payload)).await {
            Ok(res) => res?,
            Err(_) => return Err(std::io::Error::other("idle timeout (payload)")),
        };
        if typ == 1 {
            if let Some(rnd) = block_round(&payload) {
                if rnd <= live_floor.load(Ordering::Acquire) {
                    continue;
                }
                let mut gate = forward_gate.lock().await;
                if gate.should_forward(rnd, forward_nonblock) {
                    let mut frame = hdr.to_vec();
                    frame.extend_from_slice(&payload);
                    if tx.send(frame).await.is_err() {
                        return Ok(());
                    }
                }
                continue;
            }
        }
        if forward_nonblock {
            let mut frame = hdr.to_vec();
            frame.extend_from_slice(&payload);
            if tx.send(frame).await.is_err() {
                return Ok(());
            }
        }
    }
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
