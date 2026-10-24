// Pace reference streams: extra live streams (send_abci:false, nothing forwarded to any node), each
// from a different pool peer, feeding the gateway's FreshestRound. That round only counts what at
// least two streams agree on, so two references let even a single node session notice its upstream
// falling behind the network (push::FreshestRound, BlockProgress::lagging). A reference peer that
// stalls or misbehaves is replaced and avoided for a while — in the references' own cooldown, never
// the one node selection uses (a reference hiccup says little about the peer). One that merely
// lags the others is replaced but not avoided: the round it lagged may itself be wrong.

use crate::priority::gwlog;
use crate::protocol::{
    block_round, is_plausible_mainnet_round, read_live_frame_with_timeout, GREET_FALSE,
    MAX_LIVE_FRAME,
};
use crate::push::{BlockProgress, FreshestRound};
use crate::util::lock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[allow(unused_imports)]
use super::*;

// a live stream sends several blocks a second; this much silence is a dead reference
const PACE_IDLE: Duration = Duration::from_secs(10);
const PACE_RETRY: Duration = Duration::from_secs(2);
const PACE_COOLDOWN: Duration = Duration::from_secs(60);
// A reference whose stream never yields a decodable block round (only control or undecodable
// frames, often enough to beat PACE_IDLE) paces nothing and holds a reference slot: replace it.
// BlockProgress::stalled only arms after a first round, so this needs its own deadline.
#[cfg(not(test))]
const PACE_FIRST_BLOCK: Duration = Duration::from_secs(30);
#[cfg(test)]
const PACE_FIRST_BLOCK: Duration = Duration::from_millis(400);
pub(super) const PACE_REFERENCES: usize = 2;

// One reference stream, forever. The references share `in_use` (a reference never follows a peer
// another one is on) and `cooled`, their own cooldown.
pub(super) async fn run_pace_reference(
    pool: Arc<Mutex<Vec<String>>>,
    freshest: Arc<FreshestRound>,
    net_tip: Arc<AtomicU32>,
    in_use: Arc<Mutex<HashSet<String>>>,
    cooled: Arc<Mutex<HashMap<String, Instant>>>,
    start: usize,
) {
    let mut next = start;
    loop {
        let now = Instant::now();
        // pick and claim in one step, so two references never land on the same peer
        let ip = {
            let mut busy = lock(&in_use);
            let mut cooled = lock(&cooled);
            cooled.retain(|_, until| *until > now);
            let peers: Vec<String> = lock(&pool)
                .iter()
                .filter(|ip| !busy.contains(*ip) && !cooled.contains_key(*ip))
                .cloned()
                .collect();
            if peers.is_empty() {
                None
            } else {
                let ip = peers[next % peers.len()].clone();
                busy.insert(ip.clone());
                Some(ip)
            }
        };
        let Some(ip) = ip else {
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        };
        next = next.wrapping_add(1);
        let ended = follow_pace(&ip, &freshest, &net_tip).await;
        lock(&in_use).remove(&ip);
        gwlog!("[gw] pace reference via {ip} ended: {}", ended.reason);
        if ended.cool {
            lock(&cooled).insert(ip, Instant::now() + PACE_COOLDOWN);
        }
        tokio::time::sleep(PACE_RETRY).await;
    }
}

struct PaceEnd {
    reason: String,
    // avoid the peer for a while (false: it only lagged the other streams)
    cool: bool,
}

impl From<String> for PaceEnd {
    fn from(reason: String) -> Self {
        PaceEnd { reason, cool: true }
    }
}

// Follow one peer's live stream, offering each block round that passes the same plausibility and
// jump gates as a node session's active stream. Returns why it stopped.
async fn follow_pace(ip: &str, freshest: &Arc<FreshestRound>, net_tip: &AtomicU32) -> PaceEnd {
    let mut s = match connect_and_greet_4001(ip, GREET_FALSE, Duration::from_secs(4)).await {
        Ok((s, _greeting)) => s,
        Err(e) => return format!("connect: {e}").into(),
    };
    let feed = freshest.feed(ip);
    let started = Instant::now();
    let mut progress = BlockProgress::new(0, started);
    loop {
        let now = Instant::now();
        if progress.last_round == 0 && now.duration_since(started) > PACE_FIRST_BLOCK {
            return format!("no block round within {}s", PACE_FIRST_BLOCK.as_secs()).into();
        }
        if progress.stalled(now) {
            return format!("no block above round {}", progress.last_round).into();
        }
        let frame = match read_live_frame_with_timeout(&mut s, PACE_IDLE, MAX_LIVE_FRAME).await {
            Ok(f) => f,
            Err(e) => return format!("read: {e}").into(),
        };
        if frame.len() <= 5 || frame[4] != 1 {
            continue;
        }
        let Some(round) = block_round(&frame[5..]) else {
            continue;
        };
        if !is_plausible_mainnet_round(round, net_tip.load(Ordering::Acquire)) {
            return format!("implausible round {round}").into();
        }
        if let Err(e) = progress.on_round(round, now) {
            return e.into();
        }
        feed.offer(round);
        // a reference that falls behind the gateway's other streams paces nothing
        if let Some(behind) = progress.lagging(freshest, false, now) {
            return PaceEnd {
                reason: format!("lags the freshest stream by {behind} rounds"),
                cool: false,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // a peer that greets, then streams consecutive live blocks every 20ms
    async fn streaming_peer(first_round: u32) -> String {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut greet = [0u8; 8];
            s.read_exact(&mut greet).await.unwrap();
            s.write_all(&[0, 0, 0, 1, 0, 3]).await.unwrap();
            let mut round = first_round;
            loop {
                let payload = crate::protocol::test_block_payload(round, 2);
                let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
                frame.push(1);
                frame.extend_from_slice(&payload);
                if s.write_all(&frame).await.is_err() {
                    return;
                }
                round += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        addr
    }

    // A reference peer that keeps the connection alive with control frames but never sends a block
    // round is dropped (and cooled) once PACE_FIRST_BLOCK passes.
    #[tokio::test]
    async fn a_reference_that_never_sends_a_block_is_dropped() {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut greet = [0u8; 8];
            s.read_exact(&mut greet).await.unwrap();
            s.write_all(&[0, 0, 0, 1, 0, 3]).await.unwrap();
            // a type-0 control frame every 50ms, well inside PACE_IDLE
            while s.write_all(&[0, 0, 0, 2, 0, 1, 2]).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        let freshest = Arc::new(FreshestRound::new(
            150,
            Duration::from_secs(15),
            Duration::from_secs(30),
            Arc::new(AtomicU32::new(1_496_000_000)),
        ));
        let ended = tokio::time::timeout(
            Duration::from_secs(5),
            follow_pace(&addr, &freshest, &AtomicU32::new(0)),
        )
        .await
        .expect("the reference ends");
        assert!(
            ended.reason.starts_with("no block round"),
            "{}",
            ended.reason
        );
        assert!(ended.cool);
        peer.abort();
    }

    #[tokio::test]
    async fn two_pace_references_on_different_peers_set_the_freshest_round() {
        let pool = Arc::new(Mutex::new(vec![
            streaming_peer(1_496_000_000).await,
            streaming_peer(1_496_000_000).await,
        ]));
        let freshest = Arc::new(FreshestRound::new(
            150,
            Duration::from_secs(15),
            Duration::from_secs(30),
            Arc::new(AtomicU32::new(1_496_000_000)),
        ));
        let in_use = Arc::new(Mutex::new(HashSet::new()));
        let cooled = Arc::new(Mutex::new(HashMap::new()));
        let tasks: Vec<_> = (0..PACE_REFERENCES)
            .map(|i| {
                tokio::spawn(run_pace_reference(
                    pool.clone(),
                    freshest.clone(),
                    Arc::new(AtomicU32::new(0)),
                    in_use.clone(),
                    cooled.clone(),
                    i,
                ))
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(500)).await;
        // each streaming peer accepts one connection: both following proves two distinct peers
        assert_eq!(lock(&in_use).len(), 2, "one peer per reference");
        let round = freshest.get().expect("two references agree on a round");
        assert!(round > 1_496_000_005, "{round}");
        for t in tasks {
            t.abort();
        }
    }
}
