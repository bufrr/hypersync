use crate::priority::{gwlog, redact, PrioritySession, PriorityTier};
use crate::protocol::{
    is_plausible_mainnet_round, read_live_frame_with_timeout, response_is_peer_full,
    round_lag_from_tip, unix_secs, ACTIVE_ROUND_MAX_LAG, GREET_FALSE, MAX_LIVE_FRAME,
};
use crate::push::{
    node_queue_budget, queue_frame, BlockProgress, PushConfig, QueuedFrame, ACTIVE_BLOCK_STALL,
    NODE_WRITE_TIMEOUT,
};
use crate::util::lock;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

// Bytes of read-but-unprocessed frames each upstream reader may have queued to the event loop.
const EVENT_BYTES_PER_READER: usize = 2 * MAX_LIVE_FRAME;

// The active's answer to our live greeting (when live selection didn't already read it).
const GREETING_WAIT: Duration = Duration::from_secs(4);

const ACTIVE_DEAD_GRACE: Duration = Duration::from_secs(3);

// After a switch away from a peer, or a dropped standby, don't reuse it for a while.
const BAD_PEER_COOLDOWN: Duration = Duration::from_secs(120);

const TICK: Duration = Duration::from_millis(25);

// Upstream read deadline: blocks arrive ~14/s; a silent stream is closed and replaced.
const UPSTREAM_IDLE: Duration = Duration::from_secs(5);

const STANDBY_CONNECT_TIMEOUT: Duration = Duration::from_millis(1500);

const STANDBY_RETRY: Duration = Duration::from_secs(2);

// Candidates dialed at once when filling a standby slot: the first to accept wins, which favours
// the lowest-RTT (nearest) peers.
const STANDBY_RACE: usize = 4;

// A standby rotated out for being slow isn't retried for this long.
const SLOW_PEER_COOLDOWN: Duration = Duration::from_secs(30 * 60);

// The 6-byte status frame a serving peer answers our live greeting with.
const ACCEPT_GREETING: [u8; 6] = [0, 0, 0, 1, 0, 3];

// ---- I/O ----

enum Event {
    Frame {
        id: u64,
        frame: Vec<u8>,
        // (round, parent, hash) when the frame is a live block
        block: Option<(u32, u32, u64)>,
        at: Instant,
        // the reader's EVENT_BYTES_PER_READER budget for this frame, held until it's handled
        _budget: OwnedSemaphorePermit,
    },
    Closed {
        id: u64,
        reason: String,
    },
    Connected {
        ip: String,
        stream: TcpStream,
        // the priority slot this standby holds (priority peers only)
        prio: Option<PrioritySession>,
    },
    ConnectFailed,
    NodeBytes(Vec<u8>),
    NodeClosed(String),
}

// Called on every switch with the new active's ip and the priority slot it held as a standby.
pub(crate) type OnSwitch = Box<dyn Fn(&str, Option<PrioritySession>) + Send + Sync>;

pub(crate) struct FailoverCtx {
    pub(crate) node_label: String,
    pub(crate) pool: Arc<Mutex<Vec<String>>>,
    pub(crate) priority: Arc<PriorityTier>,
    // repin the node's active peer (4002 RPC routing) to this ip
    // called with the new active's ip and the priority slot it held as a standby (moved, so the
    // peer's session count never exceeds the cap or dips during a switch)
    pub(crate) on_switch: OnSwitch,
}

fn spawn_reader(id: u64, mut r: OwnedReadHalf, ev: mpsc::Sender<Event>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let budget = Arc::new(Semaphore::new(EVENT_BYTES_PER_READER));
        let reason = loop {
            let mut hdr = [0u8; 5];
            match timeout(UPSTREAM_IDLE, r.read_exact(&mut hdr)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => break format!("read error: {e}"),
                Err(_) => break format!("idle {}s", UPSTREAM_IDLE.as_secs()),
            }
            let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
            if len > MAX_LIVE_FRAME {
                break format!("oversized frame len={len}");
            }
            // wait until the event loop has handled enough of this reader's earlier frames
            let Ok(permit) = budget.clone().acquire_many_owned((5 + len) as u32).await else {
                return;
            };
            let mut frame = vec![0u8; 5 + len];
            frame[..5].copy_from_slice(&hdr);
            match timeout(UPSTREAM_IDLE, r.read_exact(&mut frame[5..])).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => break format!("read error: {e}"),
                Err(_) => break "idle (payload)".to_string(),
            }
            let at = Instant::now();
            let block = classify(&frame);
            if ev
                .send(Event::Frame {
                    id,
                    frame,
                    block,
                    at,
                    _budget: permit,
                })
                .await
                .is_err()
            {
                return;
            }
        };
        let _ = ev.send(Event::Closed { id, reason }).await;
    })
}

// A priority peer is dialed only with one of its HYPERSYNC_PRIORITY_MAX_SESSIONS slots, held for
// the standby connection's lifetime. Outer None = skip (priority peer full); inner None = pool peer.
fn standby_slot(priority: &Arc<PriorityTier>, ip: &str) -> Option<Option<PrioritySession>> {
    if priority.contains(ip) {
        priority.try_acquire(ip).map(Some)
    } else {
        Some(None)
    }
}

// Dial candidates STANDBY_RACE at a time; the first to accept a live (send_abci:false) stream
// wins and the rest are dropped (with any priority slots they took).
async fn connect_standby(
    candidates: Vec<String>,
    priority: Arc<PriorityTier>,
    ev: mpsc::Sender<Event>,
) {
    async fn attempt(
        ip: String,
        prio: Option<PrioritySession>,
    ) -> Option<(String, TcpStream, Option<PrioritySession>)> {
        let dial = async {
            let mut s = crate::blacklist::connect(&format!("{ip}:4001"))
                .await
                .ok()?;
            s.set_nodelay(true).ok();
            s.write_all(&GREET_FALSE).await.ok()?;
            let mut g = [0u8; 6];
            s.read_exact(&mut g).await.ok()?;
            (g == ACCEPT_GREETING).then_some(s)
        };
        timeout(STANDBY_CONNECT_TIMEOUT, dial)
            .await
            .ok()
            .flatten()
            .map(|s| (ip, s, prio))
    }
    for batch in candidates.chunks(STANDBY_RACE) {
        let mut set = tokio::task::JoinSet::new();
        for ip in batch {
            let Some(prio) = standby_slot(&priority, ip) else {
                continue;
            };
            set.spawn(attempt(ip.clone(), prio));
        }
        while let Some(joined) = set.join_next().await {
            if let Ok(Some((ip, stream, prio))) = joined {
                set.abort_all();
                let _ = ev.send(Event::Connected { ip, stream, prio }).await;
                return;
            }
        }
    }
    let _ = ev.send(Event::ConnectFailed).await;
}

async fn write_status(dir: &std::path::Path, node: &str, body: String) {
    let name = format!("failover-{}.json", node.replace([':', '/'], "_"));
    let tmp = dir.join(format!(".{name}.tmp"));
    if tokio::fs::write(&tmp, body).await.is_ok() {
        let _ = tokio::fs::rename(&tmp, dir.join(name)).await;
    }
}

// Writes the latest status snapshot off the event loop; ends when the session drops the sender.
fn spawn_status_writer(dir: PathBuf, node: String) -> watch::Sender<String> {
    let (stx, mut srx) = watch::channel(String::new());
    tokio::spawn(async move {
        while srx.changed().await.is_ok() {
            let body = srx.borrow_and_update().clone();
            write_status(&dir, &node, body).await;
        }
    });
    stx
}

struct Live {
    reader: JoinHandle<()>,
    writer: OwnedWriteHalf,
    // a standby's priority slot; handed to the gateway's pin (on_switch) when it becomes active
    prio: Option<PrioritySession>,
}

// Serve a node's 4001 live stream from the active peer with hot standbys (see module docs).
// Returns the session's exit reason.
pub(crate) async fn serve_failover(
    node: TcpStream,
    active_conn: TcpStream,
    cfg: PushConfig,
    fctx: FailoverCtx,
) -> String {
    let fo = FailoverConfig::get();
    let (mut node_r, mut node_w) = node.into_split();
    let (mut act_r, act_w) = active_conn.into_split();
    let active_ip = cfg.hosts[cfg.active_idx].clone();

    // greeting first: the node's first 4001 read is the peer's greeting frame
    let greeting = match cfg.prefetched_greeting {
        Some(g) => g,
        None => {
            // bounded: nothing else watches the session (or the node leaving) until this returns
            let g = match read_live_frame_with_timeout(&mut act_r, GREETING_WAIT, 1000).await {
                Ok(g) => g,
                Err(e) => return format!("active greeting read failed: {e}"),
            };
            if response_is_peer_full(&g) {
                return "active peer full".to_string();
            }
            g
        }
    };
    match timeout(NODE_WRITE_TIMEOUT, node_w.write_all(&greeting)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return format!("node greeting write failed: {e}"),
        Err(_) => return "node greeting write failed: timeout".to_string(),
    }

    // node-bound frames are bounded in bytes (push::NODE_QUEUE_BYTES); each holds its permit
    // until the writer has sent it
    let budget = node_queue_budget();
    let (tx, mut rx) = mpsc::channel::<QueuedFrame>(1024);
    let mut writer = tokio::spawn(async move {
        while let Some((buf, _permit)) = rx.recv().await {
            match timeout(NODE_WRITE_TIMEOUT, node_w.write_all(&buf)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return format!("node write failed: {e}"),
                Err(_) => return "node write timeout".to_string(),
            }
        }
        "node writer closed".to_string()
    });
    // event counts are a backstop; frame bytes are bounded per reader (EVENT_BYTES_PER_READER)
    let (ev_tx, mut ev_rx) = mpsc::channel::<Event>(1024);
    let node_reader = {
        let ev = ev_tx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            let reason = loop {
                match node_r.read(&mut buf).await {
                    Ok(0) => break "node outbound ended".to_string(),
                    Err(e) => break format!("node outbound read failed: {e}"),
                    Ok(n) => {
                        if ev.send(Event::NodeBytes(buf[..n].to_vec())).await.is_err() {
                            return;
                        }
                    }
                }
            };
            let _ = ev.send(Event::NodeClosed(reason)).await;
        })
    };

    let mut core = Core::new(
        0,
        active_ip.clone(),
        cfg.initial_last_forwarded,
        fo.stall,
        fo.lag,
        fo.min_dwell,
    );
    if fctx.priority.contains(&active_ip) {
        core.mark_preferred(0);
    }
    let mut next_id: u64 = 1;
    let mut lives: HashMap<u64, Live> = HashMap::new();
    let mut cooldown: HashMap<String, Instant> = HashMap::new();
    let mut connecting = false;
    let mut next_connect = Instant::now();
    let mut active_dead: Option<(Instant, String)> = None;
    let mut switches: u64 = 0;
    let mut last_switch_desc = String::new();
    let mut last_status = Instant::now() - Duration::from_secs(10);
    let status_tx = fo
        .status_dir
        .clone()
        .map(|dir| spawn_status_writer(dir, fctx.node_label.clone()));
    // the active stream's own round progress (jump / no-higher-block guards); re-based on a switch
    let mut progress = BlockProgress::new(cfg.initial_last_forwarded, Instant::now());
    let mut last_summary = Instant::now();
    let mut last_rotate = Instant::now();
    let rot = Arc::new(AtomicUsize::new(0));
    let net_tip = cfg.net_round_tip.clone();
    let live_floor: Arc<AtomicU32> = cfg.live_floor.clone();
    let node_label = fctx.node_label.clone();

    // a block from the active: forward it unless the node already has it
    macro_rules! forward_active_block {
        ($frame:expr, $round:expr, $hash:expr, $at:expr) => {{
            let floor = live_floor.load(Ordering::Acquire);
            if $round > floor {
                let (fwd, bad) = core.on_active_block($round, $hash, $at);
                for (id, r) in bad {
                    if let Some(ip) = core.remove(id) {
                        gwlog!("[gw] [{node_label}] failover standby {ip} dropped: {MAX_MISMATCHES} block mismatches in a row, last at round {r}");
                        cooldown.insert(ip, Instant::now() + BAD_PEER_COOLDOWN);
                    }
                    if let Some(l) = lives.remove(&id) {
                        l.reader.abort();
                    }
                }
                if fwd && queue_frame(&tx, &budget, $frame).await.is_err() {
                    break "node writer closed".to_string();
                }
            }
        }};
    }

    lives.insert(
        0,
        Live {
            reader: spawn_reader(0, act_r, ev_tx.clone()),
            writer: act_w,
            // the gateway holds the first active's slot
            prio: None,
        },
    );
    // prefetched frames (the live-selection vetting read) come first, in order
    let mut prefetched = Some(cfg.prefetched_frames);
    let mut tick = tokio::time::interval(TICK);
    // MAX: the first tick checks once (the list may have changed since the active was dialed)
    let mut blacklist_seen = u64::MAX;
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let exit_reason: String = loop {
        if let Some(frames) = prefetched.take() {
            let mut failed = None;
            for frame in frames {
                match classify(&frame) {
                    Some((r, _, h)) => {
                        if !is_plausible_mainnet_round(r, net_tip.load(Ordering::Acquire)) {
                            failed = Some(format!("implausible prefetched round {r}"));
                            break;
                        }
                        let now = Instant::now();
                        // no standby is connected yet: a jump ends the session like a dead active
                        if let Err(e) = progress.on_round(r, now) {
                            failed = Some(format!("prefetched {e}"));
                            break;
                        }
                        let floor = live_floor.load(Ordering::Acquire);
                        if r > floor
                            && core.on_active_block(r, h, now).0
                            && queue_frame(&tx, &budget, frame).await.is_err()
                        {
                            failed = Some("node writer closed".to_string());
                            break;
                        }
                    }
                    None => {
                        if queue_frame(&tx, &budget, frame).await.is_err() {
                            failed = Some("node writer closed".to_string());
                            break;
                        }
                    }
                }
            }
            if let Some(f) = failed {
                break f;
            }
        }
        tokio::select! {
            ev = ev_rx.recv() => {
                let Some(ev) = ev else { break "event channel closed".to_string() };
                match ev {
                    Event::Frame { id, frame, block, at, _budget } => {
                        if !lives.contains_key(&id) {
                            continue; // already dropped
                        }
                        let is_active = id == core.active();
                        match block {
                            Some((r, p, h)) => {
                                let tip = net_tip.load(Ordering::Acquire);
                                let lagged = round_lag_from_tip(r, tip, ACTIVE_ROUND_MAX_LAG);
                                if !is_plausible_mainnet_round(r, tip) || lagged.is_some() {
                                    let why = match lagged {
                                        Some(l) => format!("round {r} lags tip {tip} by {l}"),
                                        None => format!("implausible round {r}"),
                                    };
                                    if is_active {
                                        if active_dead.is_none() {
                                            active_dead = Some((Instant::now(), why));
                                        }
                                    } else if let Some(ip) = core.remove(id) {
                                        gwlog!("[gw] [{node_label}] failover standby {ip} dropped: {why}");
                                        cooldown.insert(ip, Instant::now() + BAD_PEER_COOLDOWN);
                                        if let Some(l) = lives.remove(&id) { l.reader.abort(); }
                                    }
                                    continue;
                                }
                                if is_active {
                                    if active_dead.is_none() {
                                        // a round jump would advance last_forwarded past every
                                        // real block: never forward it, treat the active as dead
                                        match progress.on_round(r, at) {
                                            Ok(()) => forward_active_block!(frame, r, h, at),
                                            Err(e) => active_dead = Some((Instant::now(), e)),
                                        }
                                    }
                                } else if let StandbyVerdict::Mismatch(mr) = core.on_standby_block(id, r, p, h, at, frame) {
                                    if let Some(ip) = core.remove(id) {
                                        gwlog!("[gw] [{node_label}] failover standby {ip} dropped: {MAX_MISMATCHES} block mismatches in a row, last at round {mr}");
                                        cooldown.insert(ip, Instant::now() + BAD_PEER_COOLDOWN);
                                    }
                                    if let Some(l) = lives.remove(&id) { l.reader.abort(); }
                                }
                            }
                            None => {
                                // control / non-block data: transparent from the active only
                                if is_active && active_dead.is_none() && queue_frame(&tx, &budget, frame).await.is_err() {
                                    break "node writer closed".to_string();
                                }
                            }
                        }
                    }
                    Event::Closed { id, reason } => {
                        if id == core.active() {
                            if active_dead.is_none() {
                                active_dead = Some((Instant::now(), reason));
                            }
                        } else {
                            lives.remove(&id);
                            if let Some(ip) = core.remove(id) {
                                gwlog!("[gw] [{node_label}] failover standby {ip} closed: {reason}");
                                cooldown.insert(ip, Instant::now() + STANDBY_RETRY * 5);
                            }
                        }
                    }
                    Event::Connected { ip, stream, prio } => {
                        connecting = false;
                        let id = next_id;
                        next_id += 1;
                        let (r, w) = stream.into_split();
                        lives.insert(id, Live { reader: spawn_reader(id, r, ev_tx.clone()), writer: w, prio });
                        core.add_standby(id, ip.clone());
                        if fctx.priority.contains(&ip) {
                            core.mark_preferred(id);
                        }
                        gwlog!("[gw] [{node_label}] failover standby {ip} connected ({}/{})", core.standby_count(), fo.standbys);
                    }
                    Event::ConnectFailed => {
                        connecting = false;
                        next_connect = Instant::now() + STANDBY_RETRY;
                    }
                    Event::NodeBytes(b) => {
                        // the node is silent on 4001 after its greeting; anything it does send
                        // belongs to the current active peer
                        if let Some(l) = lives.get_mut(&core.active()) {
                            if timeout(Duration::from_secs(5), l.writer.write_all(&b)).await.map_or(true, |r| r.is_err()) && active_dead.is_none() {
                                active_dead = Some((Instant::now(), "active write failed".to_string()));
                            }
                        }
                    }
                    Event::NodeClosed(reason) => break reason,
                }
            }
            joined = &mut writer => {
                break joined.unwrap_or_else(|e| format!("node writer join error: {e}"));
            }
            _ = tick.tick() => {
                let now = Instant::now();
                // an active that keeps sending (control / no-round frames) never idles out, so it
                // must also deliver a higher block every ACTIVE_BLOCK_STALL
                if active_dead.is_none() && progress.stalled(now) {
                    active_dead = Some((now, format!(
                        "no block above round {} for {}s",
                        progress.last_round,
                        ACTIVE_BLOCK_STALL.as_secs()
                    )));
                }
                // blacklisted since connecting (hot reload): the active hands over to a standby
                // like a dead one would, and blacklisted standbys go now. New standbys are dialed
                // through blacklist::connect, so only a list change needs a re-check.
                if crate::blacklist::version() != blacklist_seen {
                    blacklist_seen = crate::blacklist::version();
                    let active = core.active();
                    if core.ip_of(active).is_some_and(crate::blacklist::contains) {
                        active_dead.get_or_insert((now, "active peer blacklisted".to_string()));
                    }
                    let banned: Vec<u64> = lives
                        .keys()
                        .copied()
                        .filter(|&id| id != active && core.ip_of(id).is_some_and(crate::blacklist::contains))
                        .collect();
                    for id in banned {
                        if let Some(ip) = core.remove(id) {
                            gwlog!("[gw] [{node_label}] failover standby {ip} dropped: blacklisted");
                        }
                        if let Some(l) = lives.remove(&id) {
                            l.reader.abort();
                        }
                    }
                }
                if let Some((id, reason)) = core.pick_switch(now, active_dead.is_some()) {
                    let old = core.active();
                    let old_ip = core.ip_of(old).unwrap_or("?").to_string();
                    if let Some((frames, bad)) = core.switch_to(id, now) {
                        let new_ip = core.ip_of(id).unwrap_or("?").to_string();
                        let n = frames.len();
                        let mut send_failed = false;
                        for f in frames {
                            if queue_frame(&tx, &budget, f).await.is_err() {
                                send_failed = true;
                                break;
                            }
                        }
                        if send_failed {
                            break "node writer closed".to_string();
                        }
                        let why = active_dead.take().map(|(t, r)| format!(" ({r}, {}ms ago)", now.saturating_duration_since(t).as_millis())).unwrap_or_default();
                        switches += 1;
                        last_switch_desc = format!("{} {} -> {} {}", unix_secs(), redact(&old_ip), redact(&new_ip), reason.as_str());
                        gwlog!(
                            "[gw] [{node_label}] failover switch {old_ip} -> {new_ip} reason={}{why} replayed={n} last_forwarded={}",
                            reason.as_str(),
                            core.last_forwarded
                        );
                        progress = BlockProgress::new(core.last_forwarded, now);
                        // the standby's own priority slot moves to the gateway's pin
                        let slot = lives.get_mut(&id).and_then(|l| l.prio.take());
                        (fctx.on_switch)(&new_ip, slot);
                        core.remove(old);
                        if let Some(l) = lives.remove(&old) {
                            l.reader.abort();
                        }
                        cooldown.insert(old_ip, now + BAD_PEER_COOLDOWN);
                        for (bid, r) in bad {
                            if let Some(ip) = core.remove(bid) {
                                gwlog!("[gw] [{node_label}] failover standby {ip} dropped: {MAX_MISMATCHES} block mismatches in a row, last at round {r}");
                                cooldown.insert(ip, now + BAD_PEER_COOLDOWN);
                            }
                            if let Some(l) = lives.remove(&bid) { l.reader.abort(); }
                        }
                    }
                }
                if let Some((t, reason)) = &active_dead {
                    if now.saturating_duration_since(*t) >= ACTIVE_DEAD_GRACE {
                        break format!("active failed and no standby could continue the chain: {reason}");
                    }
                }
                // swap out the slowest standby now and then, so the set drifts toward the fastest
                // peers this host can reach (operator priority peers are kept)
                if fo.rotate_slower > Duration::ZERO
                    && !connecting
                    && active_dead.is_none()
                    && core.standby_count() >= fo.standbys
                    && now.saturating_duration_since(last_rotate) >= fo.rotate_every
                {
                    last_rotate = now;
                    let prio = fctx.priority.clone();
                    if let Some((id, m)) = core.slowest_standby(fo.rotate_slower.as_millis() as i64, |ip| prio.tier_of(ip) == "prio") {
                        if let Some(ip) = core.remove(id) {
                            gwlog!("[gw] [{node_label}] failover standby {ip} rotated out: {m}ms behind the active");
                            cooldown.insert(ip, now + SLOW_PEER_COOLDOWN);
                        }
                        if let Some(l) = lives.remove(&id) {
                            l.reader.abort();
                        }
                    }
                }
                // keep the standby set full
                if !connecting && core.standby_count() < fo.standbys && now >= next_connect {
                    cooldown.retain(|_, until| *until > now);
                    let in_use: HashSet<String> = lives.keys().filter_map(|id| core.ip_of(*id).map(str::to_string)).collect();
                    let pool = lock(&fctx.pool).clone();
                    let start = rot.fetch_add(1, Ordering::Relaxed);
                    let mut cands: Vec<String> = fctx.priority.candidates();
                    if !pool.is_empty() {
                        cands.extend((0..pool.len()).map(|i| pool[(start + i) % pool.len()].clone()));
                    }
                    let mut seen = HashSet::new();
                    cands.retain(|ip| !in_use.contains(ip) && !cooldown.contains_key(ip) && seen.insert(ip.clone()));
                    cands.truncate(STANDBY_RACE * 3);
                    if cands.is_empty() {
                        next_connect = now + STANDBY_RETRY;
                    } else {
                        connecting = true;
                        tokio::spawn(connect_standby(cands, fctx.priority.clone(), ev_tx.clone()));
                    }
                }
                if let Some(stx) = &status_tx {
                    if now.saturating_duration_since(last_status) >= Duration::from_secs(1) {
                        last_status = now;
                        stx.send_replace(core.status_json(&node_label, switches, &last_switch_desc, now));
                    }
                }
                if now.saturating_duration_since(last_summary) >= Duration::from_secs(60) {
                    last_summary = now;
                    let sb: Vec<String> = core.ups.iter().filter(|(&id, _)| id != core.active()).map(|(_, u)| {
                        format!("{}(v={} lead={})", u.ip, u.verified, u.median_lead().map(|m| format!("{m}ms")).unwrap_or_else(|| "-".into()))
                    }).collect();
                    gwlog!(
                        "[gw] [{node_label}] failover status active={} last_forwarded={} switches={switches} standbys=[{}]",
                        core.ip_of(core.active()).unwrap_or("?"),
                        core.last_forwarded,
                        sb.join(" ")
                    );
                }
            }
        }
    };
    for (_, l) in lives.drain() {
        l.reader.abort();
    }
    node_reader.abort();
    writer.abort();
    format!("{exit_reason} switches={switches}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_standbys_need_a_free_slot() {
        let tier = Arc::new(PriorityTier::new(
            vec!["1.2.3.4".into()],
            1,
            Duration::from_millis(1500),
            crate::priority::BootstrapPolicy::Fallback,
        ));
        assert!(
            matches!(standby_slot(&tier, "9.9.9.9"), Some(None)),
            "pool peer"
        );
        let held = standby_slot(&tier, "1.2.3.4").expect("free slot");
        assert!(held.is_some());
        assert_eq!(tier.sessions("1.2.3.4"), 1);
        assert!(standby_slot(&tier, "1.2.3.4").is_none(), "full: skip it");
        drop(held);
        assert_eq!(tier.sessions("1.2.3.4"), 0);
        assert!(standby_slot(&tier, "1.2.3.4").is_some());
    }

    #[tokio::test]
    async fn reader_refuses_frames_over_max_live_frame() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            s.write_all(&[0, 0, 0, 2, 0, 9, 9]).await.unwrap();
            let mut hdr = ((MAX_LIVE_FRAME + 1) as u32).to_be_bytes().to_vec();
            hdr.push(1);
            s.write_all(&hdr).await.unwrap();
            s // keep the connection open: the reader must stop on the header alone
        });
        let (r, _w) = TcpStream::connect(addr).await.unwrap().into_split();
        let (ev_tx, mut ev_rx) = mpsc::channel(8);
        let _reader = spawn_reader(7, r, ev_tx);
        match ev_rx.recv().await {
            Some(Event::Frame {
                id, frame, block, ..
            }) => {
                assert_eq!((id, frame, block), (7, vec![0, 0, 0, 2, 0, 9, 9], None));
            }
            _ => panic!("expected the small frame"),
        }
        match ev_rx.recv().await {
            Some(Event::Closed { id, reason }) => {
                assert_eq!(id, 7);
                assert!(reason.contains("oversized"), "{reason}");
            }
            _ => panic!("expected Closed"),
        }
        drop(peer.await.unwrap());
    }

    // A healthy active blacklisted mid-session is treated as dead: handed over to a standby, or
    // (none here) the session ends after ACTIVE_DEAD_GRACE.
    #[tokio::test]
    async fn blacklisted_active_is_treated_as_dead() {
        let _g = crate::blacklist::TEST_LOCK.lock().await;
        let up = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_addr = up.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = up.accept().await.unwrap();
            s.write_all(&[0, 0, 0, 1, 0, 7]).await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let nl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_client = TcpStream::connect(nl.local_addr().unwrap()).await.unwrap();
        let (node, _) = nl.accept().await.unwrap();
        let active = TcpStream::connect(up_addr).await.unwrap();
        let cfg = PushConfig {
            hosts: Arc::new(vec!["198.51.100.41".into()]),
            active_idx: 0,
            prefetched_greeting: None,
            prefetched_frames: Vec::new(),
            initial_last_forwarded: 0,
            live_floor: Arc::new(AtomicU32::new(0)),
            net_round_tip: Arc::new(AtomicU32::new(0)),
            superseded: None,
            end_when: None,
        };
        let fctx = FailoverCtx {
            node_label: "test".into(),
            pool: Arc::new(Mutex::new(Vec::new())),
            priority: Arc::new(PriorityTier::new(
                Vec::new(),
                0,
                Duration::from_millis(1500),
                crate::priority::BootstrapPolicy::Fallback,
            )),
            on_switch: Box::new(|_, _| {}),
        };
        crate::blacklist::install(&["198.51.100.41"]);
        // ends inside the 5s active-silence limit, so the blacklist is what ended it
        let reason = timeout(
            Duration::from_millis(4500),
            serve_failover(node, active, cfg, fctx),
        )
        .await
        .expect("bounded");
        crate::blacklist::install(&[]);
        assert!(reason.contains(": active peer blacklisted"), "{reason}");
        drop(node_client);
    }

    #[tokio::test]
    async fn active_peer_full_greeting_ends_the_session() {
        let up = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_addr = up.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = up.accept().await.unwrap();
            s.write_all(&[0, 0, 0, 1, 0, 4]).await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let nl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_client = TcpStream::connect(nl.local_addr().unwrap()).await.unwrap();
        let (node, _) = nl.accept().await.unwrap();
        let active = TcpStream::connect(up_addr).await.unwrap();
        let cfg = PushConfig {
            hosts: Arc::new(vec!["127.0.0.1".into()]),
            active_idx: 0,
            prefetched_greeting: None,
            prefetched_frames: Vec::new(),
            initial_last_forwarded: 0,
            live_floor: Arc::new(AtomicU32::new(0)),
            net_round_tip: Arc::new(AtomicU32::new(0)),
            superseded: None,
            end_when: None,
        };
        let fctx = FailoverCtx {
            node_label: "test".into(),
            pool: Arc::new(Mutex::new(Vec::new())),
            priority: Arc::new(PriorityTier::new(
                Vec::new(),
                0,
                Duration::from_millis(1500),
                crate::priority::BootstrapPolicy::Fallback,
            )),
            on_switch: Box::new(|_, _| {}),
        };
        let reason = timeout(
            Duration::from_secs(5),
            serve_failover(node, active, cfg, fctx),
        )
        .await
        .expect("bounded");
        assert_eq!(reason, "active peer full");
        drop(node_client);
    }
}
