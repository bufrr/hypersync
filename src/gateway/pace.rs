// Pace references for the gateway's lag guard (push::FreshestRound): the rounds of a few pool
// peers, polled with the 4002 height query, so even a single live stream can notice its peer
// falling behind the network (BlockProgress::lagging, the stalled-behind check). FreshestRound
// counts only what at least two different peers agree on.

use crate::priority::gwlog;
use crate::protocol::{is_plausible_mainnet_round, read_live_frame_with_timeout};
use crate::push::{BlockProgress, FreshestRound};
use crate::util::lock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

#[allow(unused_imports)]
use super::*;

// a peer replaced for failing or misbehaving isn't polled again for this long
const PACE_COOLDOWN: Duration = Duration::from_secs(60);

// Peers with a long-lived stream from this gateway (the shared live stream's), counted per claim:
// a stream replacing another on the same peer claims before the old one releases, so the old one
// ending never clears the new one's claim. The pace references never poll a claimed peer.
#[derive(Default)]
pub(super) struct StreamClaims(Mutex<HashMap<String, usize>>);

impl StreamClaims {
    pub(super) fn claim(&self, ip: &str) {
        *lock(&self.0).entry(ip.to_string()).or_insert(0) += 1;
    }

    pub(super) fn release(&self, ip: &str) {
        let mut m = lock(&self.0);
        if let Some(n) = m.get_mut(ip) {
            *n -= 1;
            if *n == 0 {
                m.remove(ip);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn contains(&self, ip: &str) -> bool {
        lock(&self.0).contains_key(ip)
    }

    pub(super) fn claimed(&self) -> HashSet<String> {
        lock(&self.0).keys().cloned().collect()
    }
}

// A peer answers `[0,0,0,1, 0, 2]` with a 29-byte
// frame whose last varint-tagged u32 is its current round (2026-11-16: it tracked the live round
// within a few rounds, at ~14 rounds/s, from peers that never streamed to this IP). Polling
// HEIGHT_REFERENCES peers every HEIGHT_POLL_EVERY costs ~15 bytes/s each; a live stream as a
// reference would download every block (~2.2 MB/s). With the shared live stream, upstream 4001
// block traffic is then that one stream however many nodes are behind the gateway.
pub(super) const HEIGHT_REFERENCES: usize = 3;
#[cfg(not(test))]
const HEIGHT_POLL_EVERY: Duration = Duration::from_secs(2);
#[cfg(test)]
const HEIGHT_POLL_EVERY: Duration = Duration::from_millis(50);
const HEIGHT_TIMEOUT: Duration = Duration::from_secs(3);
// consecutive failed polls before a peer is replaced (and cooled down)
const HEIGHT_MAX_FAILS: u32 = 3;
const HEIGHT_REQUEST: [u8; 6] = [0, 0, 0, 1, 0, 2];

// The round in a height answer frame (header included): its last 0xfc-tagged u32 (LE).
pub(super) fn height_answer_round(frame: &[u8]) -> Option<u32> {
    if frame.len() < 7 || frame[4] != 0 || frame[5] != 0x02 {
        return None;
    }
    let p = &frame[5..];
    (0..p.len().saturating_sub(4))
        .rev()
        .find(|&i| p[i] == 0xfc)
        .map(|i| u32::from_le_bytes([p[i + 1], p[i + 2], p[i + 3], p[i + 4]]))
}

fn addr_4002(ip: &str) -> String {
    if ip.contains(':') {
        ip.to_string()
    } else {
        format!("{ip}:4002")
    }
}

// One height query: the peer's round and when the peer read it (halfway through the request's
// round trip, as close as we can tell). HEIGHT_TIMEOUT bounds the whole query, not each step.
// a height answer: the round and when the peer read it
pub(super) type HeightAnswer = Result<(u32, Instant), String>;

pub(super) async fn query_round(ip: &str) -> HeightAnswer {
    tokio::time::timeout(HEIGHT_TIMEOUT, async {
        let mut s = crate::blacklist::connect(&addr_4002(ip))
            .await
            .map_err(|e| format!("connect: {e}"))?;
        let sent = Instant::now();
        s.write_all(&HEIGHT_REQUEST)
            .await
            .map_err(|e| format!("write: {e}"))?;
        let frame = read_live_frame_with_timeout(&mut s, HEIGHT_TIMEOUT, 256)
            .await
            .map_err(|e| format!("read: {e}"))?;
        let sampled = sent + sent.elapsed() / 2;
        height_answer_round(&frame)
            .map(|r| (r, sampled))
            .ok_or_else(|| format!("no round in answer {}", hex_bytes(&frame)))
    })
    .await
    .map_err(|_| format!("no answer within {}s", HEIGHT_TIMEOUT.as_secs()))?
}

struct Polled {
    ip: String,
    feed: crate::push::FreshestFeed,
    progress: BlockProgress,
    fails: u32,
}

// Keep HEIGHT_REFERENCES pool peers (none with a long-lived stream from this gateway, in `avoid`)
// polled, feeding each one's round into `freshest` through the same gates as a live stream's rounds:
// plausible, no jump, still rising; a peer that fails, stalls or jumps is replaced and cooled down,
// one that lags the others too (polling a laggard costs nothing, but re-adding it every poll would
// only churn the log; the round it lagged is one two peers agree on).
pub(super) async fn run_height_references(
    pool: Arc<Mutex<Vec<String>>>,
    freshest: Arc<FreshestRound>,
    net_tip: Arc<AtomicU32>,
    avoid: Arc<StreamClaims>,
) {
    let mut polled: Vec<Polled> = Vec::new();
    let mut cooled: HashMap<String, Instant> = HashMap::new();
    let mut next = 0usize;
    let mut short_warned = false;
    let mut poll = tokio::time::interval(HEIGHT_POLL_EVERY);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    poll.tick().await; // the first tick is immediate
    loop {
        let now = Instant::now();
        cooled.retain(|_, until| *until > now);
        if polled.len() < HEIGHT_REFERENCES {
            let busy = avoid.claimed();
            let pool = lock(&pool).clone();
            let free: Vec<&String> = pool
                .iter()
                .filter(|ip| {
                    !busy.contains(*ip)
                        && !cooled.contains_key(*ip)
                        && !polled.iter().any(|p| &p.ip == *ip)
                })
                .collect();
            for k in 0..free.len().min(HEIGHT_REFERENCES - polled.len()) {
                let ip = free[(next + k) % free.len()].clone();
                polled.push(Polled {
                    feed: freshest.feed(&ip),
                    ip,
                    progress: BlockProgress::new(0, now),
                    fails: 0,
                });
            }
            next = next.wrapping_add(1);
        }
        // a stream is judged only against two OTHER peers (FreshestRound::get_at): with fewer
        // references the lag guards pause, so say so
        let short = polled.len() < 2;
        if short != short_warned {
            short_warned = short;
            if short {
                gwlog!(
                    "[gw] WARN only {} pace reference(s) available: the lag guards pause until there are 2",
                    polled.len()
                );
            } else {
                gwlog!("[gw] pace references back to {}", polled.len());
            }
        }
        let mut set = tokio::task::JoinSet::new();
        for (i, p) in polled.iter().enumerate() {
            let ip = p.ip.clone();
            set.spawn(async move { (i, query_round(&ip).await) });
        }
        let mut results: Vec<(usize, HeightAnswer)> = Vec::new();
        while let Some(joined) = set.join_next().await {
            if let Ok(r) = joined {
                results.push(r);
            }
        }
        let now = Instant::now();
        // (index, why, cool)
        let mut drop_: Vec<(usize, String, bool)> = Vec::new();
        for (i, r) in results {
            let p = &mut polled[i];
            match r {
                Err(e) => {
                    p.fails += 1;
                    if p.fails >= HEIGHT_MAX_FAILS {
                        drop_.push((i, e, true));
                    }
                }
                Ok((round, sampled)) => {
                    p.fails = 0;
                    if !is_plausible_mainnet_round(round, net_tip.load(Ordering::Acquire)) {
                        drop_.push((i, format!("implausible round {round}"), true));
                    } else if let Err(e) = p.progress.on_round(round, sampled) {
                        drop_.push((i, e, true));
                    } else {
                        p.feed.offer_at(round, sampled);
                        if let Some(behind) = p.progress.lagging(&freshest, &p.ip, false) {
                            drop_.push((i, format!("lags the freshest round by {behind}"), true));
                        }
                    }
                }
            }
        }
        for (i, p) in polled.iter().enumerate() {
            if p.progress.stalled(now) && !drop_.iter().any(|d| d.0 == i) {
                drop_.push((i, format!("no round above {}", p.progress.last_round), true));
            }
        }
        drop_.sort_by(|a, b| b.0.cmp(&a.0));
        for (i, why, cool) in drop_ {
            let p = polled.remove(i);
            gwlog!("[gw] height reference {} replaced: {why}", p.ip);
            if cool {
                cooled.insert(p.ip, now + PACE_COOLDOWN);
            }
        }
        // a fixed cadence, whatever a slow answer took (each query is bounded by HEIGHT_TIMEOUT)
        poll.tick().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // a 4002 peer answering every height query with `round()` (as captured on mainnet)
    async fn height_peer(round: Arc<AtomicU32>) -> String {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                let r = round.load(Ordering::Relaxed);
                tokio::spawn(async move {
                    let mut req = [0u8; 6];
                    if s.read_exact(&mut req).await.is_err() || req != HEIGHT_REQUEST {
                        return;
                    }
                    let mut f = hex_answer();
                    let n = f.len();
                    f[n - 4..].copy_from_slice(&r.to_le_bytes());
                    let _ = s.write_all(&f).await;
                });
            }
        });
        addr
    }

    // the 29-byte answer captured from a mainnet peer on 2026-11-16
    fn hex_answer() -> Vec<u8> {
        let h = "00000018000201fc3069c348fc9c99e34871fc9b7cf85a01fc93731a5b";
        (0..h.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn the_round_is_the_last_tagged_value_of_a_height_answer() {
        assert_eq!(height_answer_round(&hex_answer()), Some(1_528_460_179));
        assert_eq!(height_answer_round(&[0, 0, 0, 1, 0, 4]), None, "peer full");
        assert_eq!(
            height_answer_round(&[0, 0, 0, 3, 0, 0, 1, 0]),
            None,
            "peer-only"
        );
    }

    #[tokio::test]
    async fn height_references_set_the_freshest_round_and_drop_a_laggard() {
        let tip = 1_528_460_000u32;
        let (a, b, slow) = (
            Arc::new(AtomicU32::new(tip)),
            Arc::new(AtomicU32::new(tip)),
            Arc::new(AtomicU32::new(tip - 12_000)),
        );
        let pool = Arc::new(Mutex::new(vec![
            height_peer(a.clone()).await,
            height_peer(b.clone()).await,
            height_peer(slow.clone()).await,
        ]));
        let freshest = Arc::new(FreshestRound::new(
            100,
            Duration::from_millis(200),
            Duration::from_secs(30),
            Arc::new(AtomicU32::new(tip)),
        ));
        let task = tokio::spawn(run_height_references(
            pool,
            freshest.clone(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(StreamClaims::default()),
        ));
        // the healthy peers advance, the slow one too but 12k rounds behind
        for _ in 0..20 {
            for r in [&a, &b, &slow] {
                r.fetch_add(3, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let f = freshest
            .get()
            .expect("two height references agree on a round");
        assert!(f > tip && f <= a.load(Ordering::Relaxed), "{f}");
        assert_eq!(
            freshest.peers_for_test().len(),
            2,
            "the laggard was dropped and stays out"
        );
        task.abort();
    }

    #[tokio::test]
    async fn height_references_never_poll_a_peer_with_a_stream_from_us() {
        let r = Arc::new(AtomicU32::new(1_528_460_000));
        let busy = height_peer(r.clone()).await;
        let free = height_peer(r.clone()).await;
        let freshest = Arc::new(FreshestRound::new(
            100,
            Duration::from_secs(5),
            Duration::from_secs(30),
            Arc::new(AtomicU32::new(1_528_460_000)),
        ));
        let avoid = Arc::new(StreamClaims::default());
        avoid.claim(&busy);
        let task = tokio::spawn(run_height_references(
            Arc::new(Mutex::new(vec![busy.clone(), free.clone()])),
            freshest.clone(),
            Arc::new(AtomicU32::new(0)),
            avoid,
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            freshest.peers_for_test(),
            vec![free],
            "only the free peer is polled"
        );
        task.abort();
    }
}
