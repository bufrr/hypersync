// Hot-standby failover for a node's 4001 live stream (enabled by HYPERSYNC_FAILOVER_STANDBYS > 0).
//
// The plain push session serves the node from ONE upstream and, when that peer stalls, tears the
// session down so the node reconnects — 10s of idle detection plus a node reconnect and a fresh
// peer election. Here the session also holds N standby peers on their own live streams, and
// switches the node's feed between upstreams WITHOUT dropping the node's connection.
//
// Why a mid-stream switch is safe here (and why the old shadow merge was not):
//   - every live block names its parent round, and the node rejects a block whose parent it never
//     saw. A switch therefore replays the standby's buffered blocks starting EXACTLY at the block
//     whose parent is the last block the node received, following parent links; if the standby
//     cannot continue the chain gap-free, no switch happens.
//   - live blocks are byte-identical across peers for the same round (verified on mainnet
//     2026-10-20: 429/429 rounds across 6 peers, identical wire bytes). Each standby is checked
//     block-by-block against what the active peer delivered; one mismatch drops it, and only a
//     standby with a run of matching blocks may become active.
//   - only one upstream feeds the node at a time; standby frames are never interleaved.
//
// Switch triggers:
//   - stall:   a verified standby has held the next block (parent == last forwarded) for
//              HYPERSYNC_FAILOVER_STALL_MS and the active still hasn't delivered it;
//   - lag:     a verified standby's median lead over the active across recent common blocks
//              exceeds HYPERSYNC_FAILOVER_LAG_MS (at most once per HYPERSYNC_FAILOVER_DWELL_MS);
//   - dead:    the active stream errored/closed/idled — switch at once if a standby can continue,
//              else end the session after ACTIVE_DEAD_GRACE (the node then reconnects as before).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::priority::{gwlog, redact, PriorityTier};
use crate::protocol::{
    block_round_parent, is_plausible_mainnet_round, round_lag_from_tip, unix_secs,
    ACTIVE_ROUND_MAX_LAG, GREET_FALSE,
};
use crate::push::PushConfig;

// ---- configuration ----

pub(crate) struct FailoverConfig {
    pub(crate) standbys: usize,
    stall: Duration,
    lag: Duration,
    min_dwell: Duration,
    // replace a standby whose median trails the active by more than this (0 = never)
    rotate_slower: Duration,
    rotate_every: Duration,
    status_dir: Option<PathBuf>,
}

impl FailoverConfig {
    fn from_env() -> Self {
        let num = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(d)
        };
        let cfg = Self {
            standbys: num("HYPERSYNC_FAILOVER_STANDBYS", 0).min(16) as usize,
            stall: Duration::from_millis(num("HYPERSYNC_FAILOVER_STALL_MS", 400).max(50)),
            lag: Duration::from_millis(num("HYPERSYNC_FAILOVER_LAG_MS", 150).max(20)),
            min_dwell: Duration::from_millis(num("HYPERSYNC_FAILOVER_DWELL_MS", 10_000)),
            rotate_slower: Duration::from_millis(num("HYPERSYNC_FAILOVER_ROTATE_MS", 100)),
            rotate_every: Duration::from_secs(num("HYPERSYNC_FAILOVER_ROTATE_SECS", 60).max(10)),
            status_dir: std::env::var("HYPERSYNC_FAILOVER_STATUS_DIR")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .map(PathBuf::from),
        };
        if cfg.standbys > 0 {
            gwlog!(
                "[gw] failover: standbys={} stall={}ms lag={}ms dwell={}ms rotate>{}ms/{}s status_dir={}",
                cfg.standbys,
                cfg.stall.as_millis(),
                cfg.lag.as_millis(),
                cfg.min_dwell.as_millis(),
                cfg.rotate_slower.as_millis(),
                cfg.rotate_every.as_secs(),
                cfg.status_dir
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "-".into())
            );
        }
        cfg
    }

    pub(crate) fn get() -> &'static FailoverConfig {
        static CFG: OnceLock<FailoverConfig> = OnceLock::new();
        CFG.get_or_init(FailoverConfig::from_env)
    }
}

// A standby must match the active on this many blocks before it may take over (one is enough when
// the active is already dead — waiting for more would only lengthen the outage).
const MIN_VERIFIED: u32 = 3;
// Lead samples (active arrival - standby arrival, per common round) kept per standby.
const LEAD_WINDOW: usize = 40;
const LEAD_MIN_SAMPLES: usize = 20;
// Forwarded-round hashes kept for verifying slower standbys (~70s of blocks).
const FORWARDED_KEEP: usize = 1024;
// Blocks a standby may hold ahead of the node before its oldest are discarded.
const AHEAD_KEEP: usize = 512;
const ACTIVE_DEAD_GRACE: Duration = Duration::from_secs(3);
// After a switch away from a peer, or a dropped standby, don't reuse it for a while.
const BAD_PEER_COOLDOWN: Duration = Duration::from_secs(120);
const TICK: Duration = Duration::from_millis(25);
// Upstream read deadline: blocks arrive ~14/s; a silent stream is closed and replaced.
const UPSTREAM_IDLE: Duration = Duration::from_secs(5);
const NODE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const STANDBY_CONNECT_TIMEOUT: Duration = Duration::from_millis(1500);
const STANDBY_RETRY: Duration = Duration::from_secs(2);
// Candidates dialed at once when filling a standby slot: the first to accept wins, which favours
// the lowest-RTT (nearest) peers.
const STANDBY_RACE: usize = 4;
// A standby rotated out for being slow isn't retried for this long.
const SLOW_PEER_COOLDOWN: Duration = Duration::from_secs(30 * 60);
// The 6-byte status frame a serving peer answers our live greeting with.
const ACCEPT_GREETING: [u8; 6] = [0, 0, 0, 1, 0, 3];

// ---- decision core (pure, no I/O) ----

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SwitchReason {
    Stall,
    Lag,
    ActiveDead,
}

impl SwitchReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stall => "stall",
            Self::Lag => "lag",
            Self::ActiveDead => "active-dead",
        }
    }
}

struct Ahead {
    parent: u32,
    hash: u64,
    at: Instant,
    frame: Vec<u8>,
}

struct Upstream {
    ip: String,
    verified: u32,
    // blocks this upstream delivered that the node has not received yet, by round
    ahead: BTreeMap<u32, Ahead>,
    // ms the active was later than this standby, per common round (negative = standby later)
    leads: VecDeque<i64>,
    last_rx: Option<Instant>,
}

impl Upstream {
    fn new(ip: String) -> Self {
        Self {
            ip,
            verified: 0,
            ahead: BTreeMap::new(),
            leads: VecDeque::new(),
            last_rx: None,
        }
    }

    fn push_lead(&mut self, ms: i64) {
        if self.leads.len() == LEAD_WINDOW {
            self.leads.pop_front();
        }
        self.leads.push_back(ms);
    }

    fn median_lead(&self) -> Option<i64> {
        if self.leads.len() < LEAD_MIN_SAMPLES {
            return None;
        }
        let mut v: Vec<i64> = self.leads.iter().copied().collect();
        v.sort_unstable();
        Some(v[v.len() / 2])
    }

    // Blocks this upstream can hand the node next, gap-free from `from` by parent links.
    fn chain_len(&self, from: u32) -> (usize, Option<Instant>) {
        let mut cur = from;
        let mut n = 0;
        let mut first_at = None;
        for (&r, a) in self.ahead.range(from.saturating_add(1)..) {
            if a.parent != cur {
                break;
            }
            if first_at.is_none() {
                first_at = Some(a.at);
            }
            cur = r;
            n += 1;
        }
        (n, first_at)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StandbyVerdict {
    Ok,
    // the standby delivered a different block than the active for the same round
    Mismatch(u32),
}

pub(crate) struct Core {
    pub(crate) last_forwarded: u32,
    forwarded: BTreeMap<u32, (u64, Instant)>,
    active: u64,
    ups: HashMap<u64, Upstream>,
    last_switch: Option<Instant>,
    stall: Duration,
    lag: Duration,
    min_dwell: Duration,
}

impl Core {
    pub(crate) fn new(
        active: u64,
        active_ip: String,
        last_forwarded: u32,
        stall: Duration,
        lag: Duration,
        min_dwell: Duration,
    ) -> Self {
        let mut ups = HashMap::new();
        ups.insert(active, Upstream::new(active_ip));
        Self {
            last_forwarded,
            forwarded: BTreeMap::new(),
            active,
            ups,
            last_switch: None,
            stall,
            lag,
            min_dwell,
        }
    }

    pub(crate) fn add_standby(&mut self, id: u64, ip: String) {
        self.ups.insert(id, Upstream::new(ip));
    }

    pub(crate) fn remove(&mut self, id: u64) -> Option<String> {
        self.ups.remove(&id).map(|u| u.ip)
    }

    fn record_forwarded(&mut self, round: u32, hash: u64, at: Instant) {
        self.last_forwarded = round;
        self.forwarded.insert(round, (hash, at));
        while self.forwarded.len() > FORWARDED_KEEP {
            self.forwarded.pop_first();
        }
    }

    // Check every standby's buffered copy of a just-forwarded round against it, and drop buffered
    // blocks the node already has. Returns standbys that delivered a different block.
    fn settle_standbys(&mut self, round: u32, hash: u64, at: Instant) -> Vec<(u64, u32)> {
        let mut bad = Vec::new();
        let floor = self.last_forwarded;
        for (&id, u) in self.ups.iter_mut() {
            if id == self.active {
                continue;
            }
            if let Some(a) = u.ahead.remove(&round) {
                if a.hash == hash {
                    u.verified += 1;
                    u.push_lead(at.saturating_duration_since(a.at).as_millis() as i64);
                } else {
                    bad.push((id, round));
                }
            }
            // anything at/below the node's tip that we didn't match is no longer useful
            while u.ahead.first_key_value().is_some_and(|(&r, _)| r <= floor) {
                u.ahead.pop_first();
            }
        }
        bad
    }

    // A block from the ACTIVE upstream. Returns whether to forward it, plus standbys that turned
    // out to disagree with it.
    pub(crate) fn on_active_block(
        &mut self,
        round: u32,
        hash: u64,
        at: Instant,
    ) -> (bool, Vec<(u64, u32)>) {
        if let Some(u) = self.ups.get_mut(&self.active) {
            u.last_rx = Some(at);
        }
        if self.last_forwarded != 0 && round <= self.last_forwarded {
            return (false, Vec::new());
        }
        self.record_forwarded(round, hash, at);
        let bad = self.settle_standbys(round, hash, at);
        (true, bad)
    }

    // A block from a STANDBY upstream: verify it against the forwarded chain, or buffer it.
    pub(crate) fn on_standby_block(
        &mut self,
        id: u64,
        round: u32,
        parent: u32,
        hash: u64,
        at: Instant,
        frame: Vec<u8>,
    ) -> StandbyVerdict {
        let last = self.last_forwarded;
        let fwd = self.forwarded.get(&round).copied();
        let Some(u) = self.ups.get_mut(&id) else {
            return StandbyVerdict::Ok;
        };
        u.last_rx = Some(at);
        if round <= last {
            if let Some((h, fat)) = fwd {
                if h != hash {
                    return StandbyVerdict::Mismatch(round);
                }
                u.verified += 1;
                u.push_lead(-(at.saturating_duration_since(fat).as_millis() as i64));
            }
            return StandbyVerdict::Ok;
        }
        u.ahead.insert(
            round,
            Ahead {
                parent,
                hash,
                at,
                frame,
            },
        );
        while u.ahead.len() > AHEAD_KEEP {
            u.ahead.pop_first();
        }
        StandbyVerdict::Ok
    }

    // Which standby to switch to right now, if any.
    pub(crate) fn pick_switch(&self, now: Instant, active_dead: bool) -> Option<(u64, SwitchReason)> {
        if self.last_forwarded == 0 {
            return None;
        }
        let min_verified = if active_dead { 1 } else { MIN_VERIFIED };
        // stall candidates rank by measured speed (median lead over the active), then by how much
        // of the chain they already hold — a momentarily long buffer alone can come from a peer
        // that is steadily slower than the others
        let mut stall_best: Option<(u64, i64, usize, Instant)> = None;
        let mut lag_best: Option<(u64, i64)> = None;
        let dwell_ok = self
            .last_switch
            .map_or(true, |t| now.saturating_duration_since(t) >= self.min_dwell);
        for (&id, u) in &self.ups {
            if id == self.active || u.verified < min_verified {
                continue;
            }
            let (n, first_at) = u.chain_len(self.last_forwarded);
            let Some(first_at) = first_at else { continue };
            let held = now.saturating_duration_since(first_at);
            if active_dead || held >= self.stall {
                let lead = u.median_lead().unwrap_or(i64::MIN);
                let better = match stall_best {
                    None => true,
                    Some((_, bl, bn, bat)) => {
                        (lead, n, std::cmp::Reverse(first_at)) > (bl, bn, std::cmp::Reverse(bat))
                    }
                };
                if better {
                    stall_best = Some((id, lead, n, first_at));
                }
            }
            if dwell_ok {
                if let Some(m) = u.median_lead() {
                    if m >= self.lag.as_millis() as i64 && lag_best.map_or(true, |(_, bm)| m > bm) {
                        lag_best = Some((id, m));
                    }
                }
            }
        }
        if let Some((id, _, _, _)) = stall_best {
            let reason = if active_dead {
                SwitchReason::ActiveDead
            } else {
                SwitchReason::Stall
            };
            return Some((id, reason));
        }
        lag_best.map(|(id, _)| (id, SwitchReason::Lag))
    }

    // Make `id` the active upstream. Returns the frames to send the node (the standby's buffered
    // chain continuing exactly from the node's last block) and standbys found disagreeing on the
    // way, or None if `id` cannot continue the chain gap-free (nothing is changed then).
    pub(crate) fn switch_to(
        &mut self,
        id: u64,
        now: Instant,
    ) -> Option<(Vec<Vec<u8>>, Vec<(u64, u32)>)> {
        let u = self.ups.get_mut(&id)?;
        let mut cur = self.last_forwarded;
        let mut chain = Vec::new();
        while let Some((&r, a)) = u.ahead.range(cur.saturating_add(1)..).next() {
            if a.parent != cur {
                break;
            }
            let a = u.ahead.remove(&r).expect("present");
            chain.push((r, a));
            cur = r;
        }
        if chain.is_empty() {
            return None;
        }
        self.active = id;
        self.last_switch = Some(now);
        let mut frames = Vec::with_capacity(chain.len());
        let mut bad = Vec::new();
        for (r, a) in chain {
            self.record_forwarded(r, a.hash, a.at);
            bad.extend(self.settle_standbys(r, a.hash, a.at));
            frames.push(a.frame);
        }
        // leads were measured against the old active
        for u in self.ups.values_mut() {
            u.leads.clear();
        }
        Some((frames, bad))
    }

    // The standby trailing the active the most, if its median lag exceeds `slower_ms` (standbys
    // `exempt` returns true for — the operator's priority peers — are never picked).
    pub(crate) fn slowest_standby(
        &self,
        slower_ms: i64,
        exempt: impl Fn(&str) -> bool,
    ) -> Option<(u64, i64)> {
        self.ups
            .iter()
            .filter(|(&id, u)| id != self.active && !exempt(&u.ip))
            .filter_map(|(&id, u)| u.median_lead().map(|m| (id, m)))
            .filter(|&(_, m)| m < -slower_ms)
            .min_by_key(|&(_, m)| m)
    }

    pub(crate) fn active(&self) -> u64 {
        self.active
    }

    fn ip_of(&self, id: u64) -> Option<&str> {
        self.ups.get(&id).map(|u| u.ip.as_str())
    }

    fn standby_count(&self) -> usize {
        self.ups.len().saturating_sub(1)
    }

    fn status_json(&self, node: &str, switches: u64, last: &str, now: Instant) -> String {
        let mut sb: Vec<(&u64, &Upstream)> = self.ups.iter().filter(|(&id, _)| id != self.active).collect();
        sb.sort_by(|a, b| a.1.ip.cmp(&b.1.ip));
        let ms_ago = |t: Option<Instant>| {
            t.map(|t| now.saturating_duration_since(t).as_millis().to_string())
                .unwrap_or_else(|| "null".into())
        };
        let standbys: Vec<String> = sb
            .iter()
            .map(|(_, u)| {
                format!(
                    "{{\"ip\":\"{}\",\"verified\":{},\"lead_ms_p50\":{},\"lead_samples\":{},\"ahead\":{},\"last_rx_ms\":{}}}",
                    redact(&u.ip),
                    u.verified,
                    u.median_lead().map(|m| m.to_string()).unwrap_or_else(|| "null".into()),
                    u.leads.len(),
                    u.ahead.len(),
                    ms_ago(u.last_rx),
                )
            })
            .collect();
        let active = self.ups.get(&self.active);
        format!(
            "{{\"node\":\"{node}\",\"updated\":{},\"active\":\"{}\",\"active_last_rx_ms\":{},\"last_forwarded\":{},\"switches\":{switches},\"last_switch\":\"{}\",\"standbys\":[{}]}}",
            unix_secs(),
            active.map(|u| redact(&u.ip)).unwrap_or_default(),
            ms_ago(active.and_then(|u| u.last_rx)),
            self.last_forwarded,
            last.replace('"', "'"),
            standbys.join(",")
        )
    }
}

// ---- I/O ----

enum Event {
    Frame {
        id: u64,
        frame: Vec<u8>,
        // (round, parent, hash) when the frame is a live block
        block: Option<(u32, u32, u64)>,
        at: Instant,
    },
    Closed {
        id: u64,
        reason: String,
    },
    Connected {
        ip: String,
        stream: TcpStream,
    },
    ConnectFailed,
    NodeBytes(Vec<u8>),
    NodeClosed(String),
}

pub(crate) struct FailoverCtx {
    pub(crate) node_label: String,
    pub(crate) pool: Arc<Mutex<Vec<String>>>,
    pub(crate) priority: Arc<PriorityTier>,
    // repin the node's active peer (4002 RPC routing) to this ip
    pub(crate) on_switch: Box<dyn Fn(&str) + Send + Sync>,
}

fn frame_hash(frame: &[u8]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    h.write(frame);
    h.finish()
}

fn classify(frame: &[u8]) -> Option<(u32, u32, u64)> {
    if frame.len() <= 5 || frame[4] != 1 {
        return None;
    }
    let (r, p) = block_round_parent(&frame[5..])?;
    Some((r, p, frame_hash(frame)))
}

fn spawn_reader(id: u64, mut r: OwnedReadHalf, ev: mpsc::Sender<Event>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let reason = loop {
            let mut hdr = [0u8; 5];
            match timeout(UPSTREAM_IDLE, r.read_exact(&mut hdr)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => break format!("read error: {e}"),
                Err(_) => break format!("idle {}s", UPSTREAM_IDLE.as_secs()),
            }
            let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
            if len > 60_000_000 {
                break format!("oversized frame len={len}");
            }
            let mut frame = vec![0u8; 5 + len];
            frame[..5].copy_from_slice(&hdr);
            match timeout(UPSTREAM_IDLE, r.read_exact(&mut frame[5..])).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => break format!("read error: {e}"),
                Err(_) => break "idle (payload)".to_string(),
            }
            let at = Instant::now();
            let block = classify(&frame);
            if ev.send(Event::Frame { id, frame, block, at }).await.is_err() {
                return;
            }
        };
        let _ = ev.send(Event::Closed { id, reason }).await;
    })
}

// Dial candidates in order until one accepts a live (send_abci:false) stream.
// Dial candidates STANDBY_RACE at a time; the first to accept a live (send_abci:false) stream
// wins and the rest are dropped.
async fn connect_standby(candidates: Vec<String>, ev: mpsc::Sender<Event>) {
    async fn attempt(ip: String) -> Option<(String, TcpStream)> {
        let dial = async {
            let mut s = TcpStream::connect((ip.as_str(), 4001)).await.ok()?;
            s.set_nodelay(true).ok();
            s.write_all(&GREET_FALSE).await.ok()?;
            let mut g = [0u8; 6];
            s.read_exact(&mut g).await.ok()?;
            (g == ACCEPT_GREETING).then_some(s)
        };
        timeout(STANDBY_CONNECT_TIMEOUT, dial).await.ok().flatten().map(|s| (ip, s))
    }
    for batch in candidates.chunks(STANDBY_RACE) {
        let mut set = tokio::task::JoinSet::new();
        for ip in batch {
            set.spawn(attempt(ip.clone()));
        }
        while let Some(joined) = set.join_next().await {
            if let Ok(Some((ip, stream))) = joined {
                set.abort_all();
                let _ = ev.send(Event::Connected { ip, stream }).await;
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

struct Live {
    reader: JoinHandle<()>,
    writer: OwnedWriteHalf,
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
            let mut hdr = [0u8; 5];
            if let Err(e) = act_r.read_exact(&mut hdr).await {
                return format!("active greeting header read failed: {e}");
            }
            let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
            if len > 1000 {
                return format!("active greeting oversized len={len}");
            }
            let mut g = hdr.to_vec();
            g.resize(5 + len, 0);
            if let Err(e) = act_r.read_exact(&mut g[5..]).await {
                return format!("active greeting payload read failed: {e}");
            }
            g
        }
    };
    if let Err(e) = node_w.write_all(&greeting).await {
        return format!("node greeting write failed: {e}");
    }

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(256);
    let mut writer = tokio::spawn(async move {
        while let Some(buf) = rx.recv().await {
            match timeout(NODE_WRITE_TIMEOUT, node_w.write_all(&buf)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return format!("node write failed: {e}"),
                Err(_) => return "node write timeout".to_string(),
            }
        }
        "node writer closed".to_string()
    });
    let (ev_tx, mut ev_rx) = mpsc::channel::<Event>(1024);
    let node_reader = {
        let ev = ev_tx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match node_r.read(&mut buf).await {
                    Ok(0) => break "node outbound ended".to_string(),
                    Err(e) => break format!("node outbound read failed: {e}"),
                    Ok(n) => {
                        if ev.send(Event::NodeBytes(buf[..n].to_vec())).await.is_err() {
                            break String::new();
                        }
                    }
                }
            }
        })
    };
    let node_reader_ev = ev_tx.clone();
    let node_reader = tokio::spawn(async move {
        if let Ok(reason) = node_reader.await {
            if !reason.is_empty() {
                let _ = node_reader_ev.send(Event::NodeClosed(reason)).await;
            }
        }
    });

    let mut core = Core::new(
        0,
        active_ip.clone(),
        cfg.initial_last_forwarded,
        fo.stall,
        fo.lag,
        fo.min_dwell,
    );
    let mut next_id: u64 = 1;
    let mut lives: HashMap<u64, Live> = HashMap::new();
    let mut cooldown: HashMap<String, Instant> = HashMap::new();
    let mut connecting = false;
    let mut next_connect = Instant::now();
    let mut active_dead: Option<(Instant, String)> = None;
    let mut switches: u64 = 0;
    let mut last_switch_desc = String::new();
    let mut last_status = Instant::now() - Duration::from_secs(10);
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
                        gwlog!("[gw] [{node_label}] failover standby {ip} dropped: block mismatch at round {r}");
                        cooldown.insert(ip, Instant::now() + BAD_PEER_COOLDOWN);
                    }
                    if let Some(l) = lives.remove(&id) {
                        l.reader.abort();
                    }
                }
                if fwd && tx.send($frame).await.is_err() {
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
        },
    );
    // prefetched frames (the live-selection vetting read) come first, in order
    let mut prefetched = Some(cfg.prefetched_frames);
    let mut tick = tokio::time::interval(TICK);
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
                        let floor = live_floor.load(Ordering::Acquire);
                        if r > floor && core.on_active_block(r, h, Instant::now()).0 && tx.send(frame).await.is_err() {
                            failed = Some("node writer closed".to_string());
                            break;
                        }
                    }
                    None => {
                        if tx.send(frame).await.is_err() {
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
                    Event::Frame { id, frame, block, at } => {
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
                                        forward_active_block!(frame, r, h, at);
                                    }
                                } else if let StandbyVerdict::Mismatch(mr) = core.on_standby_block(id, r, p, h, at, frame) {
                                    if let Some(ip) = core.remove(id) {
                                        gwlog!("[gw] [{node_label}] failover standby {ip} dropped: block mismatch at round {mr}");
                                        cooldown.insert(ip, Instant::now() + BAD_PEER_COOLDOWN);
                                    }
                                    if let Some(l) = lives.remove(&id) { l.reader.abort(); }
                                }
                            }
                            None => {
                                // control / non-block data: transparent from the active only
                                if is_active && active_dead.is_none() && tx.send(frame).await.is_err() {
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
                    Event::Connected { ip, stream } => {
                        connecting = false;
                        let id = next_id;
                        next_id += 1;
                        let (r, w) = stream.into_split();
                        lives.insert(id, Live { reader: spawn_reader(id, r, ev_tx.clone()), writer: w });
                        core.add_standby(id, ip.clone());
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
                if let Some((id, reason)) = core.pick_switch(now, active_dead.is_some()) {
                    let old = core.active();
                    let old_ip = core.ip_of(old).unwrap_or("?").to_string();
                    if let Some((frames, bad)) = core.switch_to(id, now) {
                        let new_ip = core.ip_of(id).unwrap_or("?").to_string();
                        let n = frames.len();
                        let mut send_failed = false;
                        for f in frames {
                            if tx.send(f).await.is_err() {
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
                        (fctx.on_switch)(&new_ip);
                        core.remove(old);
                        if let Some(l) = lives.remove(&old) {
                            l.reader.abort();
                        }
                        cooldown.insert(old_ip, now + BAD_PEER_COOLDOWN);
                        for (bid, r) in bad {
                            if let Some(ip) = core.remove(bid) {
                                gwlog!("[gw] [{node_label}] failover standby {ip} dropped: block mismatch at round {r}");
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
                    let pool = fctx.pool.lock().unwrap().clone();
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
                        tokio::spawn(connect_standby(cands, ev_tx.clone()));
                    }
                }
                if let Some(dir) = &fo.status_dir {
                    if now.saturating_duration_since(last_status) >= Duration::from_secs(1) {
                        last_status = now;
                        write_status(dir, &node_label, core.status_json(&node_label, switches, &last_switch_desc, now)).await;
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

    const MS: Duration = Duration::from_millis(1);

    fn core() -> Core {
        Core::new(0, "a".into(), 0, 400 * MS, 150 * MS, 10_000 * MS)
    }

    fn f(r: u32) -> Vec<u8> {
        r.to_le_bytes().to_vec()
    }

    // active delivers rounds [from..=to] at t, each standby 'b' copy arriving `b_off` relative
    fn feed(c: &mut Core, from: u32, to: u32, t0: Instant, b: u64, b_off_ms: i64) {
        for r in from..=to {
            let ta = t0 + (r - from) as u32 * 70 * MS;
            let tb = if b_off_ms >= 0 {
                ta + (b_off_ms as u32) * MS
            } else {
                ta - ((-b_off_ms) as u32) * MS
            };
            if b_off_ms < 0 {
                assert_eq!(c.on_standby_block(b, r, r - 1, r as u64, tb, f(r)), StandbyVerdict::Ok);
                assert!(c.on_active_block(r, r as u64, ta).0);
            } else {
                assert!(c.on_active_block(r, r as u64, ta).0);
                assert_eq!(c.on_standby_block(b, r, r - 1, r as u64, tb, f(r)), StandbyVerdict::Ok);
            }
        }
    }

    #[test]
    fn active_forwards_new_rounds_only() {
        let mut c = core();
        let t = Instant::now();
        assert!(c.on_active_block(100, 1, t).0);
        assert!(!c.on_active_block(100, 1, t).0);
        assert!(!c.on_active_block(99, 1, t).0);
        assert!(c.on_active_block(101, 1, t).0);
        assert_eq!(c.last_forwarded, 101);
    }

    #[test]
    fn stall_switch_replays_gap_free_chain() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        feed(&mut c, 100, 110, t0, 1, 30); // b slightly slower, verified 11x
        // active stalls; b delivers 111..=114
        let t1 = t0 + 2000 * MS;
        for r in 111..=114 {
            c.on_standby_block(1, r, r - 1, r as u64, t1, f(r));
        }
        assert_eq!(c.pick_switch(t1 + 100 * MS, false), None, "not held long enough");
        let (id, why) = c.pick_switch(t1 + 400 * MS, false).expect("stall switch");
        assert_eq!((id, why), (1, SwitchReason::Stall));
        let (frames, bad) = c.switch_to(1, t1 + 400 * MS).unwrap();
        assert!(bad.is_empty());
        assert_eq!(frames, (111..=114).map(f).collect::<Vec<_>>());
        assert_eq!(c.last_forwarded, 114);
        assert_eq!(c.active(), 1);
        // new active continues; old rounds are deduped
        assert!(!c.on_active_block(114, 114, t1).0);
        assert!(c.on_active_block(115, 115, t1).0);
    }

    #[test]
    fn no_switch_across_a_gap() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        feed(&mut c, 100, 110, t0, 1, 30);
        // b has 112.. but not 111: switching would hand the node a block with an unseen parent
        let t1 = t0 + 2000 * MS;
        for r in 112..=114 {
            c.on_standby_block(1, r, r - 1, r as u64, t1, f(r));
        }
        assert_eq!(c.pick_switch(t1 + 5000 * MS, true), None);
        assert!(c.switch_to(1, t1).is_none());
        assert_eq!(c.active(), 0);
    }

    #[test]
    fn parent_links_allow_skipped_rounds() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        feed(&mut c, 100, 110, t0, 1, 30);
        let t1 = t0 + 2000 * MS;
        // round 111 timed out: 112's parent is 110
        c.on_standby_block(1, 112, 110, 112, t1, f(112));
        c.on_standby_block(1, 113, 112, 113, t1, f(113));
        let (frames, _) = c.switch_to(1, t1).unwrap();
        assert_eq!(frames, vec![f(112), f(113)]);
    }

    #[test]
    fn unverified_standby_cannot_take_over_a_live_active() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        assert!(c.on_active_block(100, 100, t0).0);
        c.on_standby_block(1, 100, 99, 100, t0, f(100)); // 1 verification
        c.on_standby_block(1, 101, 100, 101, t0, f(101));
        assert_eq!(c.pick_switch(t0 + 5000 * MS, false), None);
        // ...but one verified block is enough once the active is dead
        assert_eq!(c.pick_switch(t0, true), Some((1, SwitchReason::ActiveDead)));
    }

    #[test]
    fn mismatching_standby_is_reported() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        assert!(c.on_active_block(100, 100, t0).0);
        assert_eq!(c.on_standby_block(1, 100, 99, 999, t0, f(100)), StandbyVerdict::Mismatch(100));
        // ahead-of-active copy that later disagrees
        c.on_standby_block(1, 101, 100, 555, t0, f(101));
        let (_, bad) = c.on_active_block(101, 101, t0);
        assert_eq!(bad, vec![(1, 101)]);
    }

    #[test]
    fn lag_switch_to_consistently_faster_standby_respects_dwell() {
        let mut c = core();
        c.add_standby(1, "b".into());
        c.add_standby(2, "c".into());
        let t0 = Instant::now();
        // b leads the active by 300ms on every block
        feed(&mut c, 100, 140, t0, 1, -300);
        let now = t0 + 41 * 70 * MS;
        // b holds nothing ahead right now -> cannot switch yet
        assert!(c.ups[&1].ahead.is_empty());
        assert_eq!(c.pick_switch(now, false), None);
        c.on_standby_block(1, 141, 140, 141, now, f(141));
        assert_eq!(c.pick_switch(now, false), Some((1, SwitchReason::Lag)));
        let (frames, _) = c.switch_to(1, now).unwrap();
        assert_eq!(frames, vec![f(141)]);
        // right after a switch, lag switching waits out the dwell (stall switching does not)
        c.on_standby_block(2, 142, 141, 142, now, f(142));
        assert_eq!(c.pick_switch(now + 100 * MS, false), None);
    }

    #[test]
    fn stall_prefers_the_faster_standby_over_the_longer_buffer() {
        let mut c = core();
        c.add_standby(1, "slow".into());
        c.add_standby(2, "fast".into());
        let t0 = Instant::now();
        for r in 100..=130u32 {
            let ta = t0 + (r - 100) * 70 * MS;
            assert!(c.on_active_block(r, r as u64, ta).0);
            c.on_standby_block(1, r, r - 1, r as u64, ta + 500 * MS, f(r));
            c.on_standby_block(2, r, r - 1, r as u64, ta + 20 * MS, f(r));
        }
        // active stalls; the slow peer happens to hold more of the chain right now
        let t1 = t0 + 3000 * MS;
        for r in 131..=134 {
            c.on_standby_block(1, r, r - 1, r as u64, t1, f(r));
        }
        for r in 131..=132 {
            c.on_standby_block(2, r, r - 1, r as u64, t1, f(r));
        }
        assert_eq!(c.pick_switch(t1 + 400 * MS, false), Some((2, SwitchReason::Stall)));
    }

    #[test]
    fn rotation_picks_the_slowest_non_exempt_standby_past_the_threshold() {
        let mut c = core();
        for (id, ip, off) in [(1, "near", 20i64), (2, "far", 300), (3, "prio", 500)] {
            c.add_standby(id, ip.into());
            let t0 = Instant::now();
            for r in 100..=130u32 {
                let ta = t0 + (r - 100) * 70 * MS;
                c.on_active_block(r, r as u64, ta);
                c.on_standby_block(id, r, r - 1, r as u64, ta + (off as u32) * MS, f(r));
            }
            c.last_forwarded = 0; // replay the same rounds for the next standby
            c.forwarded.clear();
        }
        assert_eq!(c.slowest_standby(100, |ip| ip == "prio"), Some((2, -300)));
        assert_eq!(c.slowest_standby(400, |ip| ip == "prio"), None);
        assert_eq!(c.slowest_standby(100, |_| false), Some((3, -500)));
    }

    #[test]
    fn slower_standby_leads_are_negative() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        feed(&mut c, 100, 130, t0, 1, 200);
        assert_eq!(c.ups[&1].median_lead(), Some(-200));
        assert_eq!(c.pick_switch(t0 + 10_000 * MS, false), None);
    }
}
