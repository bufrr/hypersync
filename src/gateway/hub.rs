// The gateway's shared live stream (push::hub): one upstream 4001 stream for every node behind
// the gateway, so an upstream peer sees it as a single node.
//
// The first node to start a live session selects the peer, exactly as a per-node session would;
// every later node joins the running stream. All of them pin their 4002 client-block RPC to its
// peer: upstream sees one source IP (this gateway's), so the peer knows every node's requests as
// coming from the node it streams to (no "Peer-only request").
//
// The pace references (pace.rs) never poll the stream's peer (`streams_in_use`), so the lag guard
// always judges the stream against other peers.
//
// Not shared (served per node, as before): a node greeting with other bytes than the stream's
// (another broadcast group), hot-standby sessions (HYPERSYNC_FAILOVER_STANDBYS > 0), the live
// session after a --cache replay, and a node whose selection found no peer (the push fallback).
// HYPERSYNC_SHARED_LIVE=0 turns it off.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::failover::FailoverConfig;
use crate::priority::gwlog;
use crate::push::{run_hub, Fanout, HubConfig, Subscription};
use crate::util::lock;

#[allow(unused_imports)]
use super::*;

// A sibling must have been on the shared stream this long (its catch-up after joining shows up as
// 4002 batches well within it) ...
const SIBLING_MIN_ON_STREAM: Duration = Duration::from_secs(120);
// ... and answer a height query no further than this behind (~20 s of rounds)
const SIBLING_MAX_BEHIND: u32 = 300;
// A sibling whose 4002 port can't be connected to (a node on another host publishes no ports) is
// not asked for anything this long: bootstrap or range query.
pub(super) const SIBLING_UNREACHABLE: Duration = Duration::from_secs(10 * 60);

pub(super) struct LiveHub {
    // serializes creating a stream, so nodes reconnecting together share one selection
    create: tokio::sync::Mutex<()>,
    current: Mutex<Option<Arc<Hub>>>,
    // the stream's peer, claimed while it runs (the pace references never poll it)
    pub(super) streams_in_use: Arc<StreamClaims>,
    // holds the stream's priority-return probe rate limit (one per REPIN.retry)
    probe_state: Arc<NodeState>,
    // siblings that just failed a 4002 range query, skipped until then
    range_cool: Mutex<HashMap<String, Instant>>,
    // siblings whose 4002 refused a connection, skipped for everything until then
    unreachable: Mutex<HashMap<String, Instant>>,
}

// a node on the stream: its state, its session count, when it joined
type Member = (Arc<NodeState>, usize, Instant);

pub(super) struct Hub {
    pub(super) ip: String,
    // the peer's answer to the greeting, sent to every node that joins
    pub(super) greeting: Vec<u8>,
    // the greeting this stream was opened with; a node greeting otherwise is served on its own
    node_greet: [u8; 8],
    fanout: Arc<Fanout>,
    nodes: Mutex<HashMap<IpAddr, Member>>,
}

impl Hub {
    pub(super) fn nodes(&self) -> usize {
        self.fanout.len()
    }

    fn node_states(&self) -> Vec<Arc<NodeState>> {
        lock(&self.nodes)
            .values()
            .map(|(n, _, _)| n.clone())
            .collect()
    }

    // Counts `node` as on this stream until the returned guard drops.
    pub(super) fn member(self: &Arc<Self>, node: &Arc<NodeState>) -> HubMember {
        lock(&self.nodes)
            .entry(node.ip)
            .or_insert_with(|| (node.clone(), 0, Instant::now()))
            .1 += 1;
        HubMember {
            hub: self.clone(),
            ip: node.ip,
        }
    }
}

pub(super) struct HubMember {
    hub: Arc<Hub>,
    ip: IpAddr,
}

impl Drop for HubMember {
    fn drop(&mut self) {
        let mut nodes = lock(&self.hub.nodes);
        if let Some(e) = nodes.get_mut(&self.ip) {
            e.1 -= 1;
            if e.1 == 0 {
                nodes.remove(&self.ip);
            }
        }
    }
}

pub(super) enum Join {
    Joined(Arc<Hub>, Subscription),
    // the running stream was opened with another greeting: serve this node on its own
    OtherGreeting,
    // no peer passed live selection
    NoPeer,
}

// HYPERSYNC_SIBLING_SYNC=0: never use nodes behind the gateway as a source (bootstrap state or
// 4002 catch-up); always upstream peers.
pub(super) fn sibling_sync_enabled() -> bool {
    std::env::var("HYPERSYNC_SIBLING_SYNC").map_or(true, |v| v.trim() != "0")
}

pub(super) fn shared_live_enabled() -> bool {
    std::env::var("HYPERSYNC_SHARED_LIVE").map_or(true, |v| v.trim() != "0")
        && FailoverConfig::get().standbys == 0
}

impl LiveHub {
    pub(super) fn new(streams_in_use: Arc<StreamClaims>) -> Self {
        Self {
            create: tokio::sync::Mutex::new(()),
            current: Mutex::new(None),
            streams_in_use,
            probe_state: Arc::new(NodeState::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)),
            range_cool: Mutex::new(HashMap::new()),
            unreachable: Mutex::new(HashMap::new()),
        }
    }

    fn live(&self) -> Option<Arc<Hub>> {
        lock(&self.current).clone()
    }

    // The shared stream's peer (None without a running stream).
    pub(super) fn peer(&self) -> Option<String> {
        self.live().map(|h| h.ip.clone())
    }

    // Nodes to ask first for a sibling's 4002 range query: on the shared stream for at least
    // SIBLING_MIN_ON_STREAM, not catching up, not `except`, not cooling after a failed answer. No
    // height check here: a node slightly behind still holds the older ranges a catch-up asks for,
    // and one that doesn't answers with an error, which sends the request upstream.
    pub(super) fn range_siblings(&self, except: IpAddr) -> Vec<String> {
        let Some(hub) = self.live() else {
            return Vec::new();
        };
        let now = Instant::now();
        let mut cool = lock(&self.range_cool);
        cool.retain(|_, until| *until > now);
        let unreachable = self.unreachable_now(now);
        let nodes = lock(&hub.nodes);
        nodes
            .values()
            .filter(|(n, _, joined)| {
                n.ip != except
                    && now.saturating_duration_since(*joined) >= SIBLING_MIN_ON_STREAM
                    && n.client_blocks_run_since(now, CATCHUP_RUN_GAP).is_none()
            })
            .map(|(n, _, _)| n.ip.to_string())
            .filter(|ip| !cool.contains_key(ip) && !unreachable.contains(ip))
            .collect()
    }

    pub(super) fn cool_range_sibling(&self, ip: &str, ttl: Duration) {
        lock(&self.range_cool).insert(ip.to_string(), Instant::now() + ttl);
    }

    // Its 4002 port refused a connection: ask it for nothing for SIBLING_UNREACHABLE.
    pub(super) fn mark_unreachable(&self, ip: &str) {
        lock(&self.unreachable).insert(ip.to_string(), Instant::now() + SIBLING_UNREACHABLE);
    }

    fn unreachable_now(&self, now: Instant) -> std::collections::HashSet<String> {
        let mut u = lock(&self.unreachable);
        u.retain(|_, until| *until > now);
        u.keys().cloned().collect()
    }

    // Nodes that can serve a sibling's bootstrap, by a POSITIVE sign that each is synced: its own
    // 4002 height answer within SIBLING_MAX_BEHIND of the round the gateway knows (the shared
    // stream's, or the freshest reference's). Candidates: on the shared stream for at least
    // SIBLING_MIN_ON_STREAM, not catching up, not recently failed as a source (`slow`), and not
    // `except` (the node asking). A node that doesn't answer (a remote one publishes no ports) or
    // answers too far behind isn't asked for the state.
    pub(super) async fn bootstrap_siblings(
        &self,
        except: IpAddr,
        freshest: Option<u32>,
        slow: &SlowStateServers,
    ) -> Vec<String> {
        let Some(hub) = self.live() else {
            return Vec::new();
        };
        let now = Instant::now();
        let unreachable = self.unreachable_now(now);
        let candidates: Vec<String> = lock(&hub.nodes)
            .values()
            .filter(|(n, _, joined)| {
                n.ip != except
                    && now.saturating_duration_since(*joined) >= SIBLING_MIN_ON_STREAM
                    && n.client_blocks_run_since(now, CATCHUP_RUN_GAP).is_none()
                    && !slow.contains(&n.ip.to_string(), now)
            })
            .map(|(n, _, _)| n.ip.to_string())
            .filter(|ip| !unreachable.contains(ip))
            .collect();
        let reference = hub.fanout.round().max(freshest.unwrap_or(0));
        if candidates.is_empty() || reference == 0 {
            return Vec::new();
        }
        let mut set = tokio::task::JoinSet::new();
        for ip in candidates {
            set.spawn(async move {
                let r = query_round(&ip).await;
                (ip, r)
            });
        }
        let mut fresh = Vec::new();
        while let Some(joined) = set.join_next().await {
            let Ok((ip, r)) = joined else { continue };
            match r {
                Ok((round, _)) if round.saturating_add(SIBLING_MAX_BEHIND) >= reference => {
                    fresh.push(ip)
                }
                Ok((round, _)) => gwlog!(
                    "[gw] sibling {ip} not a bootstrap source: at round {round}, {} behind",
                    reference.saturating_sub(round)
                ),
                // nothing listens (or no route): skip it for longer than a one-off failure
                Err(e) if e.starts_with(HEIGHT_CONNECT_ERR) => {
                    self.mark_unreachable(&ip);
                    gwlog!(
                        "[gw] sibling {ip} not a bootstrap source: height query {e} (skipped {}s)",
                        SIBLING_UNREACHABLE.as_secs()
                    )
                }
                Err(e) => gwlog!("[gw] sibling {ip} not a bootstrap source: height query {e}"),
            }
        }
        fresh
    }

    // Join the running stream, or select a peer and open it.
    pub(super) async fn join(
        self: &Arc<Self>,
        greet: [u8; 8],
        peers: &[String],
        start: usize,
        ctx: &GatewayCtx,
    ) -> Join {
        let _creating = self.create.lock().await;
        if let Some(hub) = self.live() {
            if hub.node_greet != greet {
                return Join::OtherGreeting;
            }
            if let Some(sub) = hub.fanout.subscribe() {
                return Join::Joined(hub, sub);
            }
            // it just ended: open the next one
        }
        let Some(live) = select_live_peer(
            &ctx.priority,
            peers,
            start,
            greet,
            0,
            ctx.net_round_tip.load(Ordering::Acquire),
            select_guard(ctx),
        )
        .await
        else {
            return Join::NoPeer;
        };
        gwlog!(
            "[gw] live hub start peer={} tier={} first_round={} greeting={}",
            live.ip,
            ctx.priority.tier_of(&live.ip),
            live.first_round,
            hex_bytes(&live.greeting)
        );
        let fanout = Arc::new(Fanout::default());
        let Some(sub) = fanout.subscribe() else {
            return Join::NoPeer; // unreachable: a new fanout hasn't ended
        };
        let hub = Arc::new(Hub {
            ip: live.ip.clone(),
            greeting: live.greeting,
            node_greet: greet,
            fanout,
            nodes: Mutex::new(HashMap::new()),
        });
        *lock(&self.current) = Some(hub.clone());
        self.streams_in_use.claim(&hub.ip);
        tokio::spawn(self.clone().run(
            hub.clone(),
            live.stream,
            live.prefetched_frames,
            live.prio_session,
            ctx.clone(),
        ));
        Join::Joined(hub, sub)
    }

    async fn run(
        self: Arc<Self>,
        hub: Arc<Hub>,
        stream: tokio::net::TcpStream,
        prefetched_frames: Vec<Vec<u8>>,
        prio_session: Option<crate::priority::PrioritySession>,
        ctx: GatewayCtx,
    ) {
        // one priority slot for the whole stream, however many nodes are on it
        let _prio = prio_session.or_else(|| ctx.priority.acquire(&hub.ip));
        let started = Instant::now();
        // a stream on a pool peer moves back once a priority peer serves again, and only while
        // every node on it is caught up
        let end_when = (!ctx.priority.is_empty() && !ctx.priority.contains(&hub.ip)).then(|| {
            let watched = hub.clone();
            Box::pin(wait_priority_return_for(
                self.probe_state.clone(),
                Box::new(move || watched.node_states()),
                ctx.priority.clone(),
                ctx.net_round_tip.clone(),
                REPIN,
                Some(ctx.freshest.clone()),
            )) as std::pin::Pin<Box<dyn std::future::Future<Output = String> + Send>>
        });
        let reason = run_hub(
            stream,
            HubConfig {
                host: hub.ip.clone(),
                prefetched_frames,
                net_round_tip: ctx.net_round_tip.clone(),
                freshest: Some(ctx.freshest.clone()),
                end_when,
            },
            hub.fanout.clone(),
        )
        .await;
        {
            let mut current = lock(&self.current);
            if current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &hub)) {
                *current = None;
            }
            self.streams_in_use.release(&hub.ip);
        }
        cool_down_on_fault(&ctx.priority, &ctx.cooldown, &hub.ip, &reason);
        gwlog!(
            "[gw] live hub ended peer={} tier={} secs={} reason={reason}",
            hub.ip,
            ctx.priority.tier_of(&hub.ip),
            started.elapsed().as_secs()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    // a peer that answers live greetings and streams consecutive blocks; counts its connections
    async fn streaming_peer(accepts: Arc<AtomicUsize>) -> String {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                accepts.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let mut greet = [0u8; 8];
                    if s.read_exact(&mut greet).await.is_err() {
                        return;
                    }
                    let _ = s.write_all(&[0, 0, 0, 1, 0, 3]).await;
                    let mut round = 1_496_000_000u32;
                    loop {
                        let payload = crate::protocol::test_block_payload(round, 2);
                        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
                        frame.push(1);
                        frame.extend_from_slice(&payload);
                        if s.write_all(&frame).await.is_err() {
                            return;
                        }
                        round += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
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

    // Two nodes (distinct source IPs to the gateway) start live sessions: the upstream sees ONE
    // connection, both nodes get the peer's greeting and blocks, and both pin 4002 to that peer.
    #[tokio::test]
    async fn two_nodes_share_one_upstream_live_stream() {
        let accepts = Arc::new(AtomicUsize::new(0));
        let peer = streaming_peer(accepts.clone()).await;
        let mut ctx = super::super::tests::routing_ctx();
        *lock(&ctx.pool) = vec![peer.clone()];
        ctx.push = true;
        let hubs = Arc::new(LiveHub::new(Arc::new(StreamClaims::default())));
        ctx.live_hub = Some(hubs.clone());
        let mut node_ends = Vec::new();
        let mut states = Vec::new();
        for i in 0..2u8 {
            let state = Arc::new(NodeState::new(
                IpAddr::V4(Ipv4Addr::new(172, 28, 0, 200 + i)),
                0,
            ));
            states.push(state.clone());
            let (node_end, gw_end) = tcp_pair().await;
            node_ends.push(node_end);
            tokio::spawn(handle_4001_live(
                gw_end,
                state,
                vec![peer.clone()],
                crate::protocol::GREET_FALSE,
                ctx.clone(),
            ));
        }
        for node in &mut node_ends {
            let greeting = crate::protocol::read_live_frame_with_timeout(
                node,
                std::time::Duration::from_secs(5),
                1000,
            )
            .await
            .unwrap();
            assert_eq!(greeting, [0, 0, 0, 1, 0, 3]);
            let block = crate::protocol::read_live_frame_with_timeout(
                node,
                std::time::Duration::from_secs(5),
                crate::protocol::MAX_LIVE_FRAME,
            )
            .await
            .unwrap();
            assert_eq!(block[4], 1, "a live block follows");
        }
        assert_eq!(
            accepts.load(Ordering::Relaxed),
            1,
            "one upstream 4001 connection"
        );
        let hub = hubs.live().expect("a shared stream is running");
        assert_eq!(hub.nodes(), 2);
        for s in &states {
            assert_eq!(
                s.active_snapshot().map(|a| a.ip),
                Some(peer.clone()),
                "4002 pinned"
            );
        }
        assert!(
            hubs.streams_in_use.contains(&peer),
            "pace references avoid it"
        );
        // a node leaving doesn't end the stream for the other
        drop(node_ends.remove(0));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let block = crate::protocol::read_live_frame_with_timeout(
            &mut node_ends[0],
            std::time::Duration::from_secs(5),
            crate::protocol::MAX_LIVE_FRAME,
        )
        .await
        .unwrap();
        assert_eq!(block[4], 1);
        assert_eq!(hub.nodes(), 1);
        assert_eq!(accepts.load(Ordering::Relaxed), 1);
    }

    // A state server on `addr`: answers a bootstrap greeting with a 6 MB abci_state frame (or the
    // peer-full status when `refuse`), then holds the connection; counts its connections.
    async fn state_server(addr: &str, refuse: bool, accepts: Arc<AtomicUsize>) {
        let l = TcpListener::bind(addr).await.unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                accepts.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let mut greet = [0u8; 8];
                    if s.read_exact(&mut greet).await.is_err() {
                        return;
                    }
                    if refuse {
                        let _ = s.write_all(&[0, 0, 0, 1, 0, 4]).await;
                        return;
                    }
                    let len = 6_000_000u32;
                    let mut f = len.to_be_bytes().to_vec();
                    f.push(1);
                    f.resize(5 + len as usize, 7);
                    if s.write_all(&f).await.is_ok() {
                        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    }
                });
            }
        });
    }

    const HUB_ROUND: u32 = 1_528_460_000;

    // a 4002 peer answering height queries with `round` (the captured 29-byte answer's layout)
    async fn height_server(addr: &str, round: u32) {
        let l = TcpListener::bind(addr).await.unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let mut req = [0u8; 6];
                    if s.read_exact(&mut req).await.is_err() {
                        return;
                    }
                    let h = "00000018000201fc3069c348fc9c99e34871fc9b7cf85a01fc93731a5b";
                    let mut f: Vec<u8> = (0..h.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
                        .collect();
                    let n = f.len();
                    f[n - 4..].copy_from_slice(&round.to_le_bytes());
                    let _ = s.write_all(&f).await;
                });
            }
        });
    }

    // a running shared stream at HUB_ROUND with `sibling` on it since `joined_ago`
    fn hub_with_sibling(hubs: &LiveHub, sibling: &Arc<NodeState>, joined_ago: Duration) {
        let fanout = Arc::new(Fanout::default());
        fanout.deliver(crate::push::HubFrame {
            frame: Arc::new(Vec::new()),
            round: Some(HUB_ROUND),
        });
        let hub = Arc::new(Hub {
            ip: "203.0.113.50".into(),
            greeting: vec![0, 0, 0, 1, 0, 3],
            node_greet: crate::protocol::GREET_FALSE,
            fanout,
            nodes: Mutex::new(HashMap::new()),
        });
        let joined = Instant::now()
            .checked_sub(joined_ago)
            .unwrap_or_else(Instant::now);
        lock(&hub.nodes).insert(sibling.ip, (sibling.clone(), 1, joined));
        *lock(&hubs.current) = Some(hub);
    }

    struct Outcome {
        sibling_4001: usize,
        upstream_4001: usize,
        pin: String,
    }

    // One cold-starting node bootstraps (twice when `again`) with one sibling on the stream whose
    // height answer is `sibling_round` and which serves (or, `refuse`, declines) the state.
    async fn bootstrap(
        sibling_ip: [u8; 4],
        sibling_round: u32,
        refuse: bool,
        again: bool,
    ) -> Outcome {
        bootstrap_after(sibling_ip, sibling_round, refuse, again, None).await
    }

    // ... where the node last bootstrapped from that sibling `prior` ago (None: never)
    async fn bootstrap_after(
        sibling_ip: [u8; 4],
        sibling_round: u32,
        refuse: bool,
        again: bool,
        prior: Option<Duration>,
    ) -> Outcome {
        let sib_accepts = Arc::new(AtomicUsize::new(0));
        let up_accepts = Arc::new(AtomicUsize::new(0));
        let sib = Ipv4Addr::from(sibling_ip);
        state_server(&format!("{sib}:4001"), refuse, sib_accepts.clone()).await;
        height_server(&format!("{sib}:4002"), sibling_round).await;
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = l.local_addr().unwrap().to_string();
        drop(l);
        state_server(&upstream, false, up_accepts.clone()).await;
        let mut ctx = super::super::tests::routing_ctx();
        *lock(&ctx.pool) = vec![upstream.clone()];
        let hubs = Arc::new(LiveHub::new(Arc::new(StreamClaims::default())));
        ctx.live_hub = Some(hubs.clone());
        let sibling = Arc::new(NodeState::new(IpAddr::V4(sib), 0));
        hub_with_sibling(&hubs, &sibling, Duration::from_secs(600));
        let node = Arc::new(NodeState::new(
            IpAddr::V4(Ipv4Addr::new(172, 28, 0, 201)),
            0,
        ));
        if let Some(ago) = prior {
            let ended = Instant::now().checked_sub(ago).unwrap();
            *lock(&node.last_sibling_bootstrap) = Some((sib.to_string(), ended));
        }
        let mut pin = String::new();
        for _ in 0..if again { 2 } else { 1 } {
            let (mut node_end, gw_end) = tcp_pair().await;
            tokio::spawn(handle_4001_bootstrap(
                gw_end,
                node.clone(),
                vec![upstream.clone()],
                crate::protocol::GREET_TRUE,
                ctx.clone(),
            ));
            let mut frame = vec![0u8; 5 + 6_000_000];
            tokio::time::timeout(Duration::from_secs(20), node_end.read_exact(&mut frame))
                .await
                .expect("the node got its state")
                .unwrap();
            // 4002 never goes to a sibling
            pin = node.active_snapshot().map(|a| a.ip).unwrap_or_default();
        }
        Outcome {
            sibling_4001: sib_accepts.load(Ordering::Relaxed),
            upstream_4001: up_accepts.load(Ordering::Relaxed),
            pin,
        }
    }

    // A sibling that accepts the state request and then sends nothing: the node's attempt is
    // closed soon after the sibling phase gives up (too late for upstream to beat the node's
    // deadline), and its retry goes straight upstream, the sibling skipped.
    #[tokio::test]
    async fn a_hung_sibling_closes_the_attempt_and_the_retry_goes_upstream() {
        let sib = Ipv4Addr::new(127, 0, 0, 84);
        let sib_accepts = Arc::new(AtomicUsize::new(0));
        let l = TcpListener::bind(format!("{sib}:4001")).await.unwrap();
        let counted = sib_accepts.clone();
        tokio::spawn(async move {
            while let Ok((s, _)) = l.accept().await {
                counted.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let _held = s;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                });
            }
        });
        height_server(&format!("{sib}:4002"), HUB_ROUND).await;
        let up_accepts = Arc::new(AtomicUsize::new(0));
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = l.local_addr().unwrap().to_string();
        drop(l);
        state_server(&upstream, false, up_accepts.clone()).await;
        let mut ctx = super::super::tests::routing_ctx();
        *lock(&ctx.pool) = vec![upstream.clone()];
        let hubs = Arc::new(LiveHub::new(Arc::new(StreamClaims::default())));
        ctx.live_hub = Some(hubs.clone());
        let sibling = Arc::new(NodeState::new(IpAddr::V4(sib), 0));
        hub_with_sibling(&hubs, &sibling, Duration::from_secs(600));
        let node = Arc::new(NodeState::new(
            IpAddr::V4(Ipv4Addr::new(172, 28, 0, 201)),
            0,
        ));
        // first attempt: closed without any state once the sibling phase gives up
        let (mut node_end, gw_end) = tcp_pair().await;
        tokio::spawn(handle_4001_bootstrap(
            gw_end,
            node.clone(),
            vec![upstream.clone()],
            crate::protocol::GREET_TRUE,
            ctx.clone(),
        ));
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), node_end.read_to_end(&mut got))
            .await
            .expect("the attempt was closed")
            .unwrap();
        assert!(got.is_empty(), "no state on the closed attempt");
        assert_eq!(
            (
                sib_accepts.load(Ordering::Relaxed),
                up_accepts.load(Ordering::Relaxed)
            ),
            (1, 0)
        );
        // the retry: straight upstream
        let (mut node_end, gw_end) = tcp_pair().await;
        tokio::spawn(handle_4001_bootstrap(
            gw_end,
            node.clone(),
            vec![upstream],
            crate::protocol::GREET_TRUE,
            ctx,
        ));
        let mut frame = vec![0u8; 5 + 6_000_000];
        tokio::time::timeout(Duration::from_secs(10), node_end.read_exact(&mut frame))
            .await
            .expect("the retry got its state")
            .unwrap();
        assert_eq!(
            (
                sib_accepts.load(Ordering::Relaxed),
                up_accepts.load(Ordering::Relaxed)
            ),
            (1, 1)
        );
    }

    // A node back for another bootstrap soon after a sibling's didn't get a usable state from it:
    // that sibling is skipped. Back much later (say a restart hours on), the sibling serves again.
    #[tokio::test]
    async fn a_quick_second_bootstrap_skips_the_sibling_that_served_the_first() {
        let o = bootstrap_after(
            [127, 0, 0, 85],
            HUB_ROUND,
            false,
            false,
            Some(Duration::from_secs(30)),
        )
        .await;
        assert_eq!(
            (o.sibling_4001, o.upstream_4001),
            (0, 1),
            "straight upstream"
        );
        let o = bootstrap_after(
            [127, 0, 0, 86],
            HUB_ROUND,
            false,
            false,
            Some(Duration::from_secs(3600)),
        )
        .await;
        assert_eq!(
            (o.sibling_4001, o.upstream_4001),
            (1, 0),
            "the sibling serves again"
        );
    }

    // A quick retry served from --cache (not a transparent bootstrap) still counts against the
    // sibling whose bootstrap didn't take.
    #[tokio::test]
    async fn a_quick_retry_from_cache_still_skips_the_sibling() {
        let mut ctx = super::super::tests::routing_ctx();
        ctx.cache_coldstart = true;
        *lock(&ctx.boot_blob) = Some(Arc::new(vec![0u8; 64]));
        ctx.boot_blob_cached_at
            .store(crate::protocol::unix_secs(), Ordering::Release);
        let node = Arc::new(NodeState::new(
            IpAddr::V4(Ipv4Addr::new(172, 28, 0, 201)),
            0,
        ));
        let sib = "127.0.0.87";
        *lock(&node.last_sibling_bootstrap) =
            Some((sib.to_string(), Instant::now() - Duration::from_secs(30)));
        let (mut node_end, gw_end) = tcp_pair().await;
        let served = tokio::spawn(handle_4001_bootstrap(
            gw_end,
            node.clone(),
            Vec::new(),
            crate::protocol::GREET_TRUE,
            ctx.clone(),
        ));
        // the retry really was served from the cache: the node gets the cached bytes
        let mut cached = [0u8; 64];
        tokio::time::timeout(Duration::from_secs(5), node_end.read_exact(&mut cached))
            .await
            .expect("served from the cache")
            .unwrap();
        assert_ne!(node.last_cache_replay_secs.load(Ordering::Acquire), 0);
        assert!(
            ctx.slow_state_servers.contains(sib, Instant::now()),
            "the sibling is skipped"
        );
        assert!(
            lock(&node.last_sibling_bootstrap).is_none(),
            "the record is consumed"
        );
        served.abort();
    }

    #[tokio::test]
    async fn a_synced_sibling_node_serves_the_bootstrap_instead_of_upstream() {
        let o = bootstrap([127, 0, 0, 77], HUB_ROUND - 20, false, false).await;
        assert_eq!(
            (o.sibling_4001, o.upstream_4001),
            (1, 0),
            "sibling served, upstream unasked"
        );
        assert_eq!(
            o.pin, "203.0.113.50",
            "4002 pinned to the shared stream's peer"
        );
    }

    #[tokio::test]
    async fn a_sibling_that_declines_falls_back_and_is_not_asked_again() {
        let o = bootstrap([127, 0, 0, 78], HUB_ROUND, true, true).await;
        assert_eq!(
            (o.sibling_4001, o.upstream_4001),
            (1, 2),
            "tried the sibling once, then upstream both times"
        );
    }

    #[tokio::test]
    async fn a_sibling_behind_the_stream_is_never_asked_for_the_state() {
        let o = bootstrap([127, 0, 0, 79], HUB_ROUND - 5_000, false, false).await;
        assert_eq!((o.sibling_4001, o.upstream_4001), (0, 1));
    }

    #[tokio::test]
    async fn a_new_or_catching_up_node_is_no_bootstrap_source() {
        let sib = Ipv4Addr::new(127, 0, 0, 80);
        height_server(&format!("{sib}:4002"), HUB_ROUND).await;
        let asking = IpAddr::V4(Ipv4Addr::new(172, 28, 0, 201));
        let slow = SlowStateServers::default();
        let hubs = LiveHub::new(Arc::new(StreamClaims::default()));
        let a = Arc::new(NodeState::new(IpAddr::V4(sib), 0));
        hub_with_sibling(&hubs, &a, Duration::from_secs(600));
        assert_eq!(
            hubs.bootstrap_siblings(asking, None, &slow).await,
            [sib.to_string()]
        );
        assert!(
            hubs.bootstrap_siblings(a.ip, None, &slow).await.is_empty(),
            "never itself"
        );
        a.mark_client_blocks_response(Instant::now(), None, 100);
        assert!(
            hubs.bootstrap_siblings(asking, None, &slow)
                .await
                .is_empty(),
            "a node catching up isn't asked"
        );
        let b = Arc::new(NodeState::new(IpAddr::V4(sib), 0));
        hub_with_sibling(&hubs, &b, Duration::from_secs(5));
        assert!(
            hubs.bootstrap_siblings(asking, None, &slow)
                .await
                .is_empty(),
            "nor one that only just joined the stream"
        );
    }

    // a 4002 sibling answering every request with `answer`; counts requests
    async fn range_server(addr: &str, answer: Vec<u8>, asked: Arc<AtomicUsize>) {
        let l = TcpListener::bind(addr).await.unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                asked.fetch_add(1, Ordering::Relaxed);
                let answer = answer.clone();
                tokio::spawn(async move {
                    let mut hdr = [0u8; 5];
                    if s.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let mut body =
                        vec![0u8; u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize];
                    let _ = s.read_exact(&mut body).await;
                    let _ = s.write_all(&answer).await;
                });
            }
        });
    }

    async fn range_from_sibling(
        sib: Ipv4Addr,
        answer: Vec<u8>,
    ) -> (Result<Vec<u8>, String>, usize, Vec<String>) {
        let asked = Arc::new(AtomicUsize::new(0));
        range_server(&format!("{sib}:4002"), answer, asked.clone()).await;
        let ctx = super::super::tests::routing_ctx();
        let hubs = Arc::new(LiveHub::new(Arc::new(StreamClaims::default())));
        let s = Arc::new(NodeState::new(IpAddr::V4(sib), 0));
        hub_with_sibling(&hubs, &s, Duration::from_secs(600));
        let node = Arc::new(NodeState::new(
            IpAddr::V4(Ipv4Addr::new(172, 28, 0, 201)),
            0,
        ));
        let r = fetch_client_blocks(
            Some(&hubs),
            &node,
            &ctx.priority,
            &[],
            &ctx.rr,
            Arc::new(client_block_range_request(1_000, 1_099)),
        )
        .await
        .map(|f| f.to_vec());
        let left = hubs.range_siblings(node.ip);
        (r, asked.load(Ordering::Relaxed), left)
    }

    #[tokio::test]
    async fn a_synced_sibling_answers_a_catch_up_range() {
        let batch = vec![0, 0, 0, 3, 1, 9, 9, 9];
        let (r, asked, left) =
            range_from_sibling(Ipv4Addr::new(127, 0, 0, 81), batch.clone()).await;
        assert_eq!(r.unwrap(), batch);
        assert_eq!(asked, 1);
        assert_eq!(left.len(), 1, "still a source");
    }

    #[tokio::test]
    async fn a_sibling_answering_anything_but_a_batch_sends_the_range_upstream() {
        // peer full: a type-0 status no range-error detector knows
        let (r, asked, left) =
            range_from_sibling(Ipv4Addr::new(127, 0, 0, 83), vec![0, 0, 0, 1, 0, 4]).await;
        assert!(r.is_err(), "not forwarded to the node as a batch");
        assert_eq!(asked, 1);
        assert!(left.is_empty(), "the sibling cools down");
    }

    #[tokio::test]
    async fn an_unreachable_sibling_is_skipped_for_bootstrap_and_ranges() {
        // nothing listens on this sibling's 4002 (an address no other test uses) (a node on another host publishes no ports)
        let ctx = super::super::tests::routing_ctx();
        let hubs = Arc::new(LiveHub::new(Arc::new(StreamClaims::default())));
        let sib = Arc::new(NodeState::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 184)), 0));
        hub_with_sibling(&hubs, &sib, Duration::from_secs(600));
        let node = Arc::new(NodeState::new(
            IpAddr::V4(Ipv4Addr::new(172, 28, 0, 201)),
            0,
        ));
        let r = fetch_client_blocks(
            Some(&hubs),
            &node,
            &ctx.priority,
            &[],
            &ctx.rr,
            Arc::new(client_block_range_request(1_000, 1_099)),
        )
        .await;
        assert!(
            r.is_err(),
            "no upstream in this test: the range went there and failed"
        );
        // a 60 s cool would also hide it from ranges; it must be the long skip, which bootstrap
        // selection honours too
        lock(&hubs.range_cool).clear();
        assert!(
            hubs.range_siblings(node.ip).is_empty(),
            "skipped for ranges"
        );
        let until = lock(&hubs.unreachable)["127.0.0.184"];
        assert!(until > Instant::now() + SIBLING_UNREACHABLE - Duration::from_secs(5));
        let slow = SlowStateServers::default();
        assert!(hubs
            .bootstrap_siblings(node.ip, None, &slow)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn a_bootstrap_height_query_refused_marks_the_sibling_unreachable() {
        let hubs = Arc::new(LiveHub::new(Arc::new(StreamClaims::default())));
        let sib = Arc::new(NodeState::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 185)), 0));
        hub_with_sibling(&hubs, &sib, Duration::from_secs(600));
        let asking = IpAddr::V4(Ipv4Addr::new(172, 28, 0, 201));
        let slow = SlowStateServers::default();
        assert!(hubs
            .bootstrap_siblings(asking, None, &slow)
            .await
            .is_empty());
        assert!(lock(&hubs.unreachable).contains_key("127.0.0.185"));
        assert!(
            hubs.range_siblings(asking).is_empty(),
            "nor asked for ranges"
        );
    }

    #[tokio::test]
    async fn a_sibling_that_refuses_a_range_sends_it_upstream_and_is_skipped() {
        // the structured Peer-only error
        let refusal = vec![0, 0, 0, 3, 0, 0x00, 0x01, 0x00];
        let (r, asked, left) = range_from_sibling(Ipv4Addr::new(127, 0, 0, 82), refusal).await;
        // no upstream peer in this test: the request reached the upstream path and failed there
        assert!(r.is_err(), "the refusal was not forwarded to the node");
        assert_eq!(asked, 1);
        assert!(left.is_empty(), "the sibling cools down");
    }
}
