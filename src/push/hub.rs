// The shared live stream: ONE upstream 4001 connection feeding every node behind the gateway, so
// an upstream peer sees this gateway as a single node however many sit behind it.
//
// Why one stream can feed every node (captured 2026-11-15, stock mainnet hl-node): after its
// 8-byte greeting a node sends NOTHING on 4001, ever, and every node is sent the same thing — the
// peer's accept frame, then the same live blocks. So the upstream's bytes fan out to each node and
// no node -> upstream traffic needs merging. A node joining mid-stream gets blocks from that point
// and fetches the gap over 4002, exactly as on any new live session.
//
// The upstream stream is judged once, here, by the same gates a per-node push session applies
// (plausibility, round jump, tip lag, the freshest-round lag guard, stall, idle). When it fails,
// every node's session ends and the nodes reconnect onto one new stream: a replacement peer starts
// at its own tip, which a node mid-stream would see as a block without a parent.
//
// Each node has its own queue, bounded in bytes. A node that stops reading is dropped on its own
// (its fault, not the upstream's); it never holds the stream, or the other nodes, back.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

use crate::priority::gwlog;
use crate::protocol::{block_round, RoundForwardGate, ACTIVE_ROUND_MAX_LAG};
use crate::util::lock;

#[allow(unused_imports)]
use super::*;

// What one node may have queued and not yet taken: ~40 s of live blocks at 1.3-1.9 MB/s, more
// than NODE_WRITE_TIMEOUT, so a node is only dropped here once it is clearly not reading.
#[cfg(not(test))]
pub(crate) const SUB_QUEUE_BYTES: usize = 64 << 20;
#[cfg(test)]
pub(crate) const SUB_QUEUE_BYTES: usize = 256 << 10;
const SUB_QUEUE_FRAMES: usize = 4096;

// A stream with no node left lingers this long, so a node that restarts or reconnects finds it.
#[cfg(not(test))]
pub(crate) const HUB_IDLE: Duration = Duration::from_secs(30);
#[cfg(test)]
pub(crate) const HUB_IDLE: Duration = Duration::from_millis(300);

// exit reason prefixes that are not the upstream's fault (see push_exit_is_upstream_fault)
pub(crate) const HUB_IDLE_REASON: &str = "live hub idle: no node left";
pub(crate) const HUB_ENDED: &str = "live hub ended";
pub(crate) const SUB_BACKLOG: &str = "node write backlog";

#[derive(Clone)]
pub(crate) struct HubFrame {
    pub(crate) frame: Arc<Vec<u8>>,
    // the block round, for each node's own floor (None: control or no-round data)
    pub(crate) round: Option<u32>,
}

type SubItem = (HubFrame, OwnedSemaphorePermit);

struct Sub {
    tx: mpsc::Sender<SubItem>,
    budget: Arc<Semaphore>,
    overflowed: Arc<AtomicBool>,
    // wakes the node's session at once when it is dropped for not reading (it is likely blocked
    // writing to the node, and would otherwise only notice at NODE_WRITE_TIMEOUT)
    kicked: Arc<Notify>,
}

impl Sub {
    // dropped for not reading
    fn kick(&self) {
        self.overflowed.store(true, Ordering::Release);
        self.kicked.notify_one();
    }

    // the stream ended: wake the session even if it's blocked writing to its node
    fn wake(&self) {
        self.kicked.notify_one();
    }
}

// The nodes a hub stream feeds.
pub(crate) struct Fanout {
    subs: Mutex<HashMap<u64, Sub>>,
    next_id: AtomicU64,
    ended: watch::Sender<Option<String>>,
    // the highest block round delivered (0 = none yet)
    round: AtomicU32,
}

impl Default for Fanout {
    fn default() -> Self {
        Self {
            subs: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            ended: watch::channel(None).0,
            round: AtomicU32::new(0),
        }
    }
}

pub(crate) struct Subscription {
    rx: mpsc::Receiver<SubItem>,
    overflowed: Arc<AtomicBool>,
    kicked: Arc<Notify>,
    ended: watch::Receiver<Option<String>>,
    id: u64,
    fanout: Arc<Fanout>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        lock(&self.fanout.subs).remove(&self.id);
    }
}

impl Fanout {
    // None once the stream has ended: join a new one instead
    pub(crate) fn subscribe(self: &Arc<Self>) -> Option<Subscription> {
        let mut subs = lock(&self.subs);
        if self.is_ended() {
            return None;
        }
        let (tx, rx) = mpsc::channel(SUB_QUEUE_FRAMES);
        let overflowed = Arc::new(AtomicBool::new(false));
        let kicked = Arc::new(Notify::new());
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        subs.insert(
            id,
            Sub {
                tx,
                budget: Arc::new(Semaphore::new(SUB_QUEUE_BYTES)),
                overflowed: overflowed.clone(),
                kicked: kicked.clone(),
            },
        );
        Some(Subscription {
            rx,
            overflowed,
            kicked,
            ended: self.ended.subscribe(),
            id,
            fanout: self.clone(),
        })
    }

    pub(crate) fn len(&self) -> usize {
        lock(&self.subs).len()
    }

    pub(crate) fn is_ended(&self) -> bool {
        self.ended.borrow().is_some()
    }

    // Ends every node's session with `reason` (the first reason given wins).
    pub(crate) fn end(&self, reason: &str) {
        let mut subs = lock(&self.subs);
        self.ended.send_if_modified(|r| {
            if r.is_none() {
                *r = Some(reason.to_string());
                true
            } else {
                false
            }
        });
        for sub in subs.values() {
            sub.wake();
        }
        subs.clear();
    }

    // One frame to every node; never waits. A node whose queue is full is dropped.
    pub(crate) fn round(&self) -> u32 {
        self.round.load(Ordering::Acquire)
    }

    pub(crate) fn deliver(&self, f: HubFrame) {
        if let Some(r) = f.round {
            self.round.fetch_max(r, Ordering::AcqRel);
        }
        let n = f.frame.len().clamp(1, SUB_QUEUE_BYTES) as u32;
        lock(&self.subs).retain(|_, sub| {
            let Ok(permit) = sub.budget.clone().try_acquire_many_owned(n) else {
                sub.kick();
                return false;
            };
            match sub.tx.try_send((f.clone(), permit)) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    sub.kick();
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
    }
}

pub(crate) struct HubConfig {
    pub(crate) host: String,
    // the live frames selection read while vetting the peer (its greeting is the nodes')
    pub(crate) prefetched_frames: Vec<Vec<u8>>,
    pub(crate) net_round_tip: Arc<AtomicU32>,
    pub(crate) freshest: Option<Arc<FreshestRound>>,
    // the caller's reason to end the stream (e.g. returning to a priority peer)
    pub(crate) end_when: Option<Pin<Box<dyn Future<Output = String> + Send>>>,
}

// Read the shared upstream stream until it fails, fanning every frame that passes its gates out to
// the nodes. Returns the exit reason; the nodes' sessions have all been ended with it.
pub(crate) async fn run_hub(active_conn: TcpStream, cfg: HubConfig, fanout: Arc<Fanout>) -> String {
    let reason = hub_loop(active_conn, cfg, &fanout).await;
    fanout.end(&reason);
    reason
}

async fn hub_loop(active_conn: TcpStream, cfg: HubConfig, fanout: &Arc<Fanout>) -> String {
    // block-round dedup and monotonicity, once for every node (their own floors apply per node)
    let forward_gate = Arc::new(tokio::sync::Mutex::new(
        RoundForwardGate::with_last_forwarded(16_384, 0),
    ));
    let (tx, mut rx) = NodeQueue::new(PUSH_MERGE_QUEUE_FRAMES);
    // the write half stays open and unused: like a node, the stream sends nothing after its
    // greeting (dropping it would half-close the connection, which a peer may take as a goodbye)
    let (act_r, _act_w) = active_conn.into_split();
    let no_floor = Arc::new(AtomicU32::new(0));
    let active_round = Arc::new(AtomicU32::new(0));
    let stats = Arc::new(PushStats::default());
    let feed = cfg.freshest.as_ref().map(|f| f.feed(&cfg.host));
    for frame in cfg.prefetched_frames {
        let Ok(()) = process_live_frame(
            frame,
            &tx,
            &forward_gate,
            &no_floor,
            &cfg.net_round_tip,
            &active_round,
            feed.as_ref(),
            &stats,
        )
        .await
        else {
            return "prefetched frame processing failed".to_string();
        };
    }
    let own_round = feed.as_ref().map(FreshestFeed::own_round);
    let mut active_task = AbortOnDrop(spawn_active_pump(
        act_r,
        tx.clone(),
        forward_gate,
        no_floor,
        cfg.net_round_tip.clone(),
        active_round.clone(),
        feed,
        stats.clone(),
    ));
    let end_signal = cfg.end_when;
    let end_when = async move {
        match end_signal {
            Some(end) => end.await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(end_when);
    let blacklisted = crate::blacklist::wait_blacklisted(&cfg.host);
    tokio::pin!(blacklisted);
    let mut stall_check = tokio::time::interval(STALL_CHECK_EVERY);
    stall_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut stalled_behind = StalledBehind::new(STALL_LAG_ROUNDS, STALL_LAG_GRACE, STALL_LAG_WIDEN);
    let mut own_rounds = OwnRounds::default();
    let mut lag_log = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(60),
        Duration::from_secs(60),
    );
    let mut empty_since: Option<Instant> = None;
    let exit = loop {
        tokio::select! {
            _ = &mut blacklisted => break "active peer blacklisted".to_string(),
            reason = &mut end_when => break reason,
            _ = stall_check.tick() => {
                let now = Instant::now();
                if fanout.len() == 0 {
                    let since = *empty_since.get_or_insert(now);
                    if now.saturating_duration_since(since) >= HUB_IDLE {
                        break HUB_IDLE_REASON.to_string();
                    }
                } else {
                    empty_since = None;
                }
                let (Some(own), Some(freshest)) = (&own_round, &cfg.freshest) else { continue };
                // the stream is never held back by a node (fan-out never waits): no exemption
                if let Some(behind) = check_stalled_behind(
                    &mut stalled_behind,
                    &mut own_rounds,
                    own.load(Ordering::Acquire),
                    freshest,
                    &cfg.host,
                    false,
                    now,
                ) {
                    break format!("active peer stalled behind the freshest stream by {behind} rounds");
                }
            }
            _ = lag_log.tick() => {
                let local_round = active_round.load(Ordering::Acquire);
                let peers_max_round = cfg.net_round_tip.load(Ordering::Acquire);
                let delta = i64::from(peers_max_round) - i64::from(local_round);
                let freshest = cfg.freshest.as_ref().and_then(|f| f.get()).unwrap_or(0);
                gwlog!(
                    "[gw] active peer round lag active_peer={} shared=true nodes={} local_round={local_round} peers_max_round={peers_max_round} delta={delta} lag={} max_lag={ACTIVE_ROUND_MAX_LAG} freshest={freshest} behind_freshest={} node_backlog=0 {}",
                    cfg.host,
                    fanout.len(),
                    delta.max(0),
                    freshest.saturating_sub(local_round),
                    stats.summary()
                );
            }
            maybe = rx.recv() => match maybe {
                Some((frame, _permit)) => {
                    let round = (frame.len() > 5 && frame[4] == 1)
                        .then(|| block_round(&frame[5..]))
                        .flatten();
                    fanout.deliver(HubFrame { frame: Arc::new(frame), round });
                }
                None => break "active stream ended".to_string(),
            },
            joined = &mut active_task.0 => break match joined {
                Ok(Ok(())) => "active stream ended".to_string(),
                Ok(Err(e)) => format!("active stream error: {e}"),
                Err(e) => format!("active task join error: {e}"),
            },
        }
    };
    drop(active_task);
    format!("{exit} stats=[{}]", stats.summary())
}

#[derive(Default)]
pub(crate) struct SubStats {
    pub(crate) blocks: u64,
    pub(crate) bytes: u64,
    pub(crate) dropped_floor: u64,
}

// Serve one node from a hub stream: the stream's greeting, then its frames, until the node leaves
// or stops reading, the hub ends, or `end_when` resolves. Returns the exit reason.
pub(crate) async fn serve_subscriber(
    node: TcpStream,
    greeting: &[u8],
    mut sub: Subscription,
    live_floor: Arc<AtomicU32>,
    end_when: Option<Pin<Box<dyn Future<Output = String> + Send>>>,
) -> (String, SubStats) {
    let mut stats = SubStats::default();
    let (mut node_r, mut node_w) = node.into_split();
    match timeout(NODE_WRITE_TIMEOUT, node_w.write_all(greeting)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return (format!("node greeting write failed: {e}"), stats),
        Err(_) => return ("node greeting write failed: timeout".to_string(), stats),
    }
    // A node sends nothing after its greeting; anything it does send has no upstream to go to
    // (other nodes share it) and is dropped. Its connection closing ends the session.
    let mut node_out = AbortOnDrop(tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        let mut dropped = 0u64;
        loop {
            match node_r.read(&mut buf).await {
                Ok(0) | Err(_) => break dropped,
                Ok(n) => dropped += n as u64,
            }
        }
    }));
    let end_when = async move {
        match end_when {
            Some(end) => end.await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(end_when);
    let hub_reason = |sub: &Subscription| {
        let r = sub.ended.borrow().clone().unwrap_or_default();
        format!("{HUB_ENDED}: {r}")
    };
    let backlog = || {
        format!(
            "{SUB_BACKLOG} over {} MB (node not reading)",
            SUB_QUEUE_BYTES >> 20
        )
    };
    let kicked = sub.kicked.clone();
    let reason = loop {
        tokio::select! {
            biased;
            reason = &mut end_when => break reason,
            joined = &mut node_out.0 => break match joined {
                Ok(0) => "node outbound ended".to_string(),
                Ok(n) => format!("node outbound ended ({n} bytes from the node dropped)"),
                Err(e) => format!("node outbound task join error: {e}"),
            },
            _ = sub.ended.changed() => break hub_reason(&sub),
            item = sub.rx.recv() => match item {
                Some((f, _permit)) => {
                    if let Some(round) = f.round {
                        // a cache replay's backlog the node already has
                        if round <= live_floor.load(Ordering::Acquire) {
                            stats.dropped_floor += 1;
                            continue;
                        }
                        stats.blocks += 1;
                    }
                    stats.bytes += f.frame.len() as u64;
                    tokio::select! {
                        r = timeout(NODE_WRITE_TIMEOUT, node_w.write_all(&f.frame)) => match r {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => break format!("node write failed: {e}"),
                            Err(_) => break "node write timeout".to_string(),
                        },
                        _ = kicked.notified() => break if sub.overflowed.load(Ordering::Acquire) {
                            backlog()
                        } else {
                            hub_reason(&sub)
                        },
                    }
                }
                None if sub.overflowed.load(Ordering::Acquire) => break backlog(),
                None => break hub_reason(&sub),
            },
        }
    };
    (reason, stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::test_block_payload;
    use tokio::net::TcpListener;

    fn block(round: u32) -> Vec<u8> {
        let payload = test_block_payload(round, 2);
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.push(1);
        frame.extend_from_slice(&payload);
        frame
    }

    // An upstream that accepts connections (counting them), answers the greeting, and streams
    // consecutive blocks every 10 ms until `stop` is set.
    async fn upstream(first_round: u32, accepts: Arc<AtomicU64>, stop: Arc<AtomicBool>) -> String {
        upstream_sized(first_round, accepts, stop, 0).await
    }

    // ... also sending a `pad32` x 32-byte control frame after each block
    async fn upstream_sized(
        first_round: u32,
        accepts: Arc<AtomicU64>,
        stop: Arc<AtomicBool>,
        pad32: usize,
    ) -> String {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                accepts.fetch_add(1, Ordering::Relaxed);
                let stop = stop.clone();
                tokio::spawn(async move {
                    let mut greet = [0u8; 8];
                    if s.read_exact(&mut greet).await.is_err() {
                        return;
                    }
                    let _ = s.write_all(&[0, 0, 0, 1, 0, 3]).await;
                    let mut round = first_round;
                    while !stop.load(Ordering::Relaxed) {
                        if s.write_all(&block(round)).await.is_err() {
                            return;
                        }
                        // bulk: a control frame of `pad32` 32-byte words, forwarded as-is
                        if pad32 > 0 {
                            let mut ctl = ((pad32 * 32) as u32).to_be_bytes().to_vec();
                            ctl.push(0);
                            ctl.resize(5 + pad32 * 32, 0x5a);
                            if s.write_all(&ctl).await.is_err() {
                                return;
                            }
                        }
                        round += 1;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                });
            }
        });
        addr
    }

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let c = TcpStream::connect(l.local_addr().unwrap()).await.unwrap();
        (c, l.accept().await.unwrap().0)
    }

    async fn dial_hub(addr: &str) -> TcpStream {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(&crate::protocol::GREET_FALSE).await.unwrap();
        let mut accept = [0u8; 6];
        s.read_exact(&mut accept).await.unwrap();
        s
    }

    fn hub_cfg(host: &str) -> HubConfig {
        HubConfig {
            host: host.to_string(),
            prefetched_frames: Vec::new(),
            net_round_tip: Arc::new(AtomicU32::new(0)),
            freshest: None,
            end_when: None,
        }
    }

    // read frames from a node's end until `n` block rounds arrived; returns them
    async fn read_rounds(node: &mut TcpStream, n: usize) -> Vec<u32> {
        let mut out = Vec::new();
        while out.len() < n {
            let f = crate::protocol::read_live_frame_with_timeout(
                node,
                Duration::from_secs(2),
                crate::protocol::MAX_LIVE_FRAME,
            )
            .await
            .unwrap();
            if f[4] == 1 {
                out.push(block_round(&f[5..]).unwrap());
            }
        }
        out
    }

    #[tokio::test]
    async fn one_upstream_connection_feeds_every_node() {
        let accepts = Arc::new(AtomicU64::new(0));
        let addr = upstream(1_496_000_000, accepts.clone(), Arc::default()).await;
        let fanout = Arc::new(Fanout::default());
        let subs: Vec<_> = (0..3).map(|_| fanout.subscribe().unwrap()).collect();
        let up = dial_hub(&addr).await;
        let hub = tokio::spawn(run_hub(up, hub_cfg(&addr), fanout.clone()));
        let mut nodes = Vec::new();
        for sub in subs {
            let (mut node, gw_side) = tcp_pair().await;
            tokio::spawn(serve_subscriber(
                gw_side,
                &[0, 0, 0, 1, 0, 3],
                sub,
                Arc::new(AtomicU32::new(0)),
                None,
            ));
            let mut accept = [0u8; 6];
            node.read_exact(&mut accept).await.unwrap();
            assert_eq!(
                accept,
                [0, 0, 0, 1, 0, 3],
                "each node gets the hub's greeting"
            );
            nodes.push(node);
        }
        let mut seen = Vec::new();
        for node in &mut nodes {
            seen.push(read_rounds(node, 5).await);
        }
        for rounds in &seen {
            assert!(rounds.windows(2).all(|w| w[1] > w[0]), "{rounds:?}");
        }
        assert_eq!(
            accepts.load(Ordering::Relaxed),
            1,
            "one upstream connection"
        );
        assert_eq!(fanout.len(), 3);
        hub.abort();
    }

    #[tokio::test]
    async fn a_node_that_stops_reading_is_dropped_without_holding_the_others_back() {
        // ~64 KB every 10 ms: the stuck node's socket buffers fill within a second or two
        let addr = upstream_sized(1_496_000_000, Arc::default(), Arc::default(), 2_000).await;
        let fanout = Arc::new(Fanout::default());
        let stuck_sub = fanout.subscribe().unwrap();
        let ok_sub = fanout.subscribe().unwrap();
        let hub = tokio::spawn(run_hub(
            dial_hub(&addr).await,
            hub_cfg(&addr),
            fanout.clone(),
        ));
        // the stuck node never reads past its greeting
        let (_stuck, stuck_gw) = tcp_pair().await;
        let stuck = tokio::spawn(serve_subscriber(
            stuck_gw,
            &[0, 0, 0, 1, 0, 3],
            stuck_sub,
            Arc::new(AtomicU32::new(0)),
            None,
        ));
        let (mut ok, ok_gw) = tcp_pair().await;
        tokio::spawn(serve_subscriber(
            ok_gw,
            &[0, 0, 0, 1, 0, 3],
            ok_sub,
            Arc::new(AtomicU32::new(0)),
            None,
        ));
        let mut accept = [0u8; 6];
        ok.read_exact(&mut accept).await.unwrap();
        // the stuck node's socket buffers fill, then its queue: it is dropped
        // well before NODE_WRITE_TIMEOUT: the kick wakes it out of its blocked write
        let (reason, _) = timeout(Duration::from_secs(10), stuck)
            .await
            .expect("the stuck node is dropped")
            .unwrap();
        assert!(reason.starts_with(SUB_BACKLOG), "{reason}");
        assert!(!push_exit_is_upstream_fault(&reason), "{reason}");
        // the other node kept getting blocks throughout, and still does
        let rounds = read_rounds(&mut ok, 20).await;
        assert!(rounds.windows(2).all(|w| w[1] > w[0]), "{rounds:?}");
        assert!(!hub.is_finished());
        hub.abort();
    }

    #[tokio::test]
    async fn an_upstream_that_closes_ends_every_node_session() {
        let stop = Arc::new(AtomicBool::new(false));
        let addr = upstream(1_496_000_000, Arc::default(), stop.clone()).await;
        let fanout = Arc::new(Fanout::default());
        let subs: Vec<_> = (0..2).map(|_| fanout.subscribe().unwrap()).collect();
        let hub = tokio::spawn(run_hub(
            dial_hub(&addr).await,
            hub_cfg(&addr),
            fanout.clone(),
        ));
        let mut sessions = Vec::new();
        let mut nodes = Vec::new();
        for sub in subs {
            let (node, gw) = tcp_pair().await;
            sessions.push(tokio::spawn(serve_subscriber(
                gw,
                &[0, 0, 0, 1, 0, 3],
                sub,
                Arc::new(AtomicU32::new(0)),
                None,
            )));
            nodes.push(node);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.store(true, Ordering::Relaxed);
        let hub_reason = timeout(Duration::from_secs(15), hub)
            .await
            .expect("the hub ends")
            .unwrap();
        assert!(push_exit_is_upstream_fault(&hub_reason), "{hub_reason}");
        for s in sessions {
            let (reason, stats) = timeout(Duration::from_secs(2), s).await.unwrap().unwrap();
            assert!(reason.starts_with(HUB_ENDED), "{reason}");
            assert!(stats.blocks > 0);
        }
        assert!(
            fanout.subscribe().is_none(),
            "an ended hub takes no new node"
        );
    }

    // A node blocked in a write (not reading) is released the moment the stream ends, with the
    // stream's reason, not left registered until NODE_WRITE_TIMEOUT.
    #[tokio::test]
    async fn a_hub_end_releases_a_node_blocked_in_a_write() {
        let fanout = Arc::new(Fanout::default());
        let sub = fanout.subscribe().unwrap();
        // tiny socket buffers, so one queued frame blocks the session's write to the node
        let ls = tokio::net::TcpSocket::new_v4().unwrap();
        ls.set_send_buffer_size(4096).unwrap();
        ls.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let l = ls.listen(1).unwrap();
        let cs = tokio::net::TcpSocket::new_v4().unwrap();
        cs.set_recv_buffer_size(4096).unwrap();
        let _stuck = cs.connect(l.local_addr().unwrap()).await.unwrap(); // never read
        let (gw, _) = l.accept().await.unwrap();
        let session = tokio::spawn(serve_subscriber(
            gw,
            &[0, 0, 0, 1, 0, 3],
            sub,
            Arc::new(AtomicU32::new(0)),
            None,
        ));
        // one frame inside the queue budget, far bigger than both socket buffers
        let mut f = vec![0u8; SUB_QUEUE_BYTES / 2];
        f[4] = 1;
        fanout.deliver(HubFrame {
            frame: Arc::new(f),
            round: None,
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!session.is_finished(), "blocked writing");
        fanout.end("active stream error: test");
        let (reason, _) = timeout(Duration::from_secs(2), session)
            .await
            .expect("released at once, not at NODE_WRITE_TIMEOUT")
            .unwrap();
        assert!(reason.starts_with(HUB_ENDED), "{reason}");
    }

    #[tokio::test]
    async fn a_hub_with_no_node_left_ends_after_lingering() {
        let addr = upstream(1_496_000_000, Arc::default(), Arc::default()).await;
        let fanout = Arc::new(Fanout::default());
        let sub = fanout.subscribe().unwrap();
        let hub = tokio::spawn(run_hub(
            dial_hub(&addr).await,
            hub_cfg(&addr),
            fanout.clone(),
        ));
        tokio::time::sleep(HUB_IDLE).await;
        assert!(!hub.is_finished(), "a node is still on it");
        drop(sub);
        let reason = timeout(HUB_IDLE * 10, hub)
            .await
            .expect("idle hub ends")
            .unwrap();
        assert!(reason.starts_with(HUB_IDLE_REASON), "{reason}");
        assert!(!push_exit_is_upstream_fault(&reason));
    }

    #[tokio::test]
    async fn a_nodes_cache_floor_drops_only_its_own_backlog() {
        let addr = upstream(1_496_000_000, Arc::default(), Arc::default()).await;
        let fanout = Arc::new(Fanout::default());
        let sub = fanout.subscribe().unwrap();
        let hub = tokio::spawn(run_hub(
            dial_hub(&addr).await,
            hub_cfg(&addr),
            fanout.clone(),
        ));
        let (mut node, gw) = tcp_pair().await;
        tokio::spawn(serve_subscriber(
            gw,
            &[0, 0, 0, 1, 0, 3],
            sub,
            Arc::new(AtomicU32::new(1_496_000_004)),
            None,
        ));
        let mut accept = [0u8; 6];
        node.read_exact(&mut accept).await.unwrap();
        let rounds = read_rounds(&mut node, 3).await;
        assert!(rounds[0] > 1_496_000_004, "{rounds:?}");
        hub.abort();
    }
}
