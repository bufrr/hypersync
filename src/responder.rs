// Public probe responder (`hypersync responder [<bind-ip>]`): answers what peers send to the
// public 4001/4002 of an IP that peers with them, so a node behind the gateway needs no public
// ports and the gateway host alone holds the address the upstreams know.
//
// Captured on node1's public ports (docs/hl-p2p-protocol.md §8c): QuickNode and pool peers open
// 4001 and close it without sending a byte (a reachability check) and ask 4002 for query_peers;
// some pool peers also greet 4001 to pull a live stream or ask 4002 for heights.
//
// What it answers, and nothing more:
// - 4001: a connect-only probe is held until the peer closes; a greeting gets the "peer full"
//   status a busy node sends, then the connection closes. No block is ever sent.
// - 4002: query_peers gets an empty peer table; any other request gets the structured
//   Peer-only error.
// It never dials out: nothing it receives reaches a node or an upstream.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;

use crate::priority::{gwlog, is_priority};

// a probe that never closes is dropped after this; a real greeting or request arrives at once
const IDLE: Duration = Duration::from_secs(10);
// connections held at once across both ports; past it a new one is closed unanswered
const MAX_CONNS: usize = 256;
// the longest request accepted on 4002 (query_peers is 1 byte, a range query 11)
const MAX_REQUEST: usize = 64;
const STATS_EVERY: Duration = Duration::from_secs(600);

// [L=1][type=0][4]: the refusal a full node sends to a 4001 greeting (protocol::response_is_peer_full)
pub(crate) const PEER_FULL: [u8; 6] = [0, 0, 0, 1, 0, 4];
// [L=2][type=0][0x01 query_peers][count 0]
pub(crate) const EMPTY_PEER_TABLE: [u8; 7] = [0, 0, 0, 2, 0, 0x01, 0];
// [L=3][type=0][00 01 00]: error, variant 0 = Peer-only request (gateway::response_is_peer_only)
pub(crate) const PEER_ONLY: [u8; 8] = [0, 0, 0, 3, 0, 0x00, 0x01, 0x00];
const QUERY_PEERS: [u8; 2] = [0, 0x01]; // type 0, variant 1

#[derive(Default)]
pub(crate) struct Stats {
    probes: AtomicU64,
    greetings: AtomicU64,
    query_peers: AtomicU64,
    other_rpc: AtomicU64,
    bad: AtomicU64,
    busy: AtomicU64,
    from_priority: AtomicU64,
}

impl Stats {
    fn take(&self) -> [u64; 7] {
        [
            &self.probes,
            &self.greetings,
            &self.query_peers,
            &self.other_rpc,
            &self.bad,
            &self.busy,
            &self.from_priority,
        ]
        .map(|c| c.swap(0, Ordering::Relaxed))
    }
}

pub(crate) async fn run_responder(bind_ip: &str) {
    let stats = Arc::new(Stats::default());
    let slots = Arc::new(Semaphore::new(MAX_CONNS));
    let mut tasks = Vec::new();
    for port in [4001u16, 4002] {
        let l = match TcpListener::bind((bind_ip, port)).await {
            Ok(l) => l,
            Err(e) => {
                gwlog!("[responder] bind :{port}: {e}");
                std::process::exit(1);
            }
        };
        tasks.push(tokio::spawn(serve(l, port, slots.clone(), stats.clone())));
    }
    gwlog!("[responder] answering probes on :4001 and :4002");
    let st = stats.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(STATS_EVERY).await;
            let [p, g, q, o, b, busy, prio] = st.take();
            gwlog!(
                "[responder] last {}s: 4001 probes={p} greetings={g} | 4002 query_peers={q} other={o} | malformed={b} busy={busy} | from priority peers={prio}",
                STATS_EVERY.as_secs()
            );
        }
    });
    for t in tasks {
        let _ = t.await;
    }
}

pub(crate) async fn serve(l: TcpListener, port: u16, slots: Arc<Semaphore>, stats: Arc<Stats>) {
    loop {
        let (s, addr) = match l.accept().await {
            Ok(x) => x,
            Err(e) => {
                gwlog!("[responder] accept :{port}: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            stats.busy.fetch_add(1, Ordering::Relaxed);
            continue; // dropping `s` closes it
        };
        if is_priority(&addr.ip().to_string()) {
            stats.from_priority.fetch_add(1, Ordering::Relaxed);
        }
        let stats = stats.clone();
        tokio::spawn(async move {
            let _permit = permit;
            s.set_nodelay(true).ok();
            if port == 4001 {
                answer_4001(s, &stats).await;
            } else {
                answer_4002(s, &stats).await;
            }
        });
    }
}

async fn answer_4001(mut s: TcpStream, stats: &Stats) {
    let mut greeting = [0u8; 8];
    match timeout(IDLE, s.read(&mut greeting)).await {
        Ok(Ok(0)) | Err(_) => {
            stats.probes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Ok(_)) => {
            stats.greetings.fetch_add(1, Ordering::Relaxed);
            let _ = timeout(IDLE, s.write_all(&PEER_FULL)).await;
            let _ = s.shutdown().await;
        }
        Ok(Err(_)) => {
            stats.bad.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn answer_4002(mut s: TcpStream, stats: &Stats) {
    let reply = match timeout(IDLE, read_request(&mut s)).await {
        Ok(Some(req)) if req == QUERY_PEERS => {
            stats.query_peers.fetch_add(1, Ordering::Relaxed);
            &EMPTY_PEER_TABLE[..]
        }
        Ok(Some(_)) => {
            stats.other_rpc.fetch_add(1, Ordering::Relaxed);
            &PEER_ONLY[..]
        }
        Ok(None) | Err(_) => {
            stats.bad.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let _ = timeout(IDLE, s.write_all(reply)).await;
    let _ = s.shutdown().await;
}

// One request frame [u32 BE L][type][L bytes], returned as type + body; None when the peer
// closes early or declares more than MAX_REQUEST.
async fn read_request(s: &mut TcpStream) -> Option<Vec<u8>> {
    let mut hdr = [0u8; 5];
    s.read_exact(&mut hdr).await.ok()?;
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    if len > MAX_REQUEST {
        return None;
    }
    let mut req = vec![0u8; 1 + len];
    req[0] = hdr[4];
    s.read_exact(&mut req[1..]).await.ok()?;
    Some(req)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::response_is_peer_only;
    use crate::protocol::response_is_peer_full;

    async fn responder() -> (std::net::SocketAddr, std::net::SocketAddr, Arc<Stats>) {
        let stats = Arc::new(Stats::default());
        let slots = Arc::new(Semaphore::new(MAX_CONNS));
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (a1, a2) = (l1.local_addr().unwrap(), l2.local_addr().unwrap());
        tokio::spawn(serve(l1, 4001, slots.clone(), stats.clone()));
        tokio::spawn(serve(l2, 4002, slots, stats.clone()));
        (a1, a2, stats)
    }

    async fn ask(addr: std::net::SocketAddr, req: &[u8]) -> Vec<u8> {
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(req).await.unwrap();
        c.shutdown().await.unwrap(); // a peer that sent all it will send
                                     // a reset (closed with our bytes unread) reads as whatever arrived before it
        let mut out = Vec::new();
        let _ = timeout(Duration::from_secs(5), c.read_to_end(&mut out))
            .await
            .expect("responder held the connection open");
        out
    }

    #[tokio::test]
    async fn a_greeting_is_refused_as_peer_full_and_gets_no_block() {
        let (p4001, _, stats) = responder().await;
        for greeting in [crate::protocol::GREET_FALSE, crate::protocol::GREET_TRUE] {
            let out = ask(p4001, &greeting).await;
            assert_eq!(out, PEER_FULL);
            assert!(response_is_peer_full(&out));
        }
        assert_eq!(stats.greetings.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn a_connect_only_probe_is_counted_and_closed() {
        let (p4001, _, stats) = responder().await;
        drop(TcpStream::connect(p4001).await.unwrap());
        for _ in 0..50 {
            if stats.probes.load(Ordering::Relaxed) == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("probe not counted");
    }

    #[tokio::test]
    async fn query_peers_gets_an_empty_table_and_anything_else_peer_only() {
        let (_, p4002, stats) = responder().await;
        // the exact bytes QuickNode sent node1
        assert_eq!(ask(p4002, &[0, 0, 0, 1, 0, 1]).await, EMPTY_PEER_TABLE);
        // a height query and a range query
        let out = ask(p4002, &[0, 0, 0, 1, 0, 2]).await;
        assert_eq!(out, PEER_ONLY);
        assert!(response_is_peer_only(&out[5..]));
        let range = [0, 0, 0, 11, 0, 0, 0xfc, 1, 2, 3, 4, 0xfc, 5, 6, 7, 8];
        assert_eq!(ask(p4002, &range).await, PEER_ONLY);
        assert_eq!(stats.query_peers.load(Ordering::Relaxed), 1);
        assert_eq!(stats.other_rpc.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn an_oversized_or_truncated_request_gets_no_answer() {
        let (_, p4002, stats) = responder().await;
        assert!(ask(p4002, &[0, 0, 0, 65, 0, 0]).await.is_empty());
        assert!(ask(p4002, &[0, 0, 0, 5, 0]).await.is_empty());
        assert_eq!(stats.bad.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn the_empty_peer_table_parses_as_no_peers() {
        assert!(crate::peerd::parse_query_peers_response(&EMPTY_PEER_TABLE[5..]).is_empty());
        assert_eq!(EMPTY_PEER_TABLE[4..6], QUERY_PEERS);
    }
}
