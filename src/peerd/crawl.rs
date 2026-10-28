use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

// ---- query_peers gossip RPC (port 4002): self-contained peer discovery ----
//
// Reverse-engineered on live mainnet (observations/query_peers-4002-20260708.pcap). The 4002
// RPC needs NO TcpGreeting — connect and send one framed control request:
//   request  = [u32 BE L=1][type=0][0x01]                      (0x01 = query_peers variant tag)
//   response = [u32 BE L][type=0][0x01][count varint][entry..] (control frame, NOT lz4 data)
//   entry    = [0x00 IPv4 discriminant][a][b][c][d][0x01][0x01] (7 bytes; only the 4 octets matter)
// Crucially, query_peers is NOT peer-gated (unlike query_height / client_blocks, which return
// "Peer-only request"), so peerd can crawl the peer graph standalone.
const QUERY_PEERS_REQUEST: [u8; 6] = [0, 0, 0, 1, 0, 1];

const MAX_QUERY_PEERS_RESP: usize = 65_536;

// A 64KB response can list ~9k IPs and the crawl makes up to PEERD_CRAWL_MAX_QUERIES queries per
// cycle, while probing retires at most a few hundred candidates per cycle. Bound what one peer can
// inject and the persisted set, so a hostile/buggy peer cannot grow memory and peer_candidates.txt
// without limit or dilute probe coverage of real peers (mainnet today: ~1.3k candidates).
const MAX_PEERS_PER_QUERY_RESPONSE: usize = 512;

pub(super) const MAX_CANDIDATES: usize = 20_000;

// HL compact varint (docs/hl-p2p-protocol.md §3): lead <0xfb is the value; 0xfb/0xfc/0xfd introduce
// a u16/u32/u64 LE. Returns (value, bytes_consumed). 0xfe (u128) is not a plausible peer count.
fn read_hl_varint(b: &[u8]) -> Option<(u64, usize)> {
    let lead = *b.first()?;
    match lead {
        0xfb => Some((u16::from_le_bytes([*b.get(1)?, *b.get(2)?]) as u64, 3)),
        0xfc => Some((
            u32::from_le_bytes([*b.get(1)?, *b.get(2)?, *b.get(3)?, *b.get(4)?]) as u64,
            5,
        )),
        0xfd => {
            let mut arr = [0u8; 8];
            for (i, slot) in arr.iter_mut().enumerate() {
                *slot = *b.get(1 + i)?;
            }
            Some((u64::from_le_bytes(arr), 9))
        }
        0xfe => None,
        _ => Some((lead as u64, 1)),
    }
}

// Parse a query_peers response payload (the bytes AFTER the 5-byte frame header). Defensive:
// stops at the first malformed/unknown entry and returns whatever was parsed so far, never panics.
// An error frame (payload starts 0x03) or any non-Peers payload yields an empty vec.
pub(crate) fn parse_query_peers_response(payload: &[u8]) -> Vec<String> {
    if payload.first() != Some(&0x01) {
        return Vec::new();
    }
    let Some((count, adv)) = read_hl_varint(&payload[1..]) else {
        return Vec::new();
    };
    let mut off = 1 + adv;
    let mut out = Vec::new();
    for _ in 0..count {
        // entry = [0x00][a][b][c][d][0x01][0x01]
        if payload.get(off) != Some(&0x00) || off + 7 > payload.len() {
            break;
        }
        let ip = format!(
            "{}.{}.{}.{}",
            payload[off + 1],
            payload[off + 2],
            payload[off + 3],
            payload[off + 4]
        );
        off += 7;
        if is_routable(&ip) {
            out.push(ip);
        }
    }
    out
}

// Ask one peer for its peer table over 4002. All failures degrade to an empty vec.
async fn query_peers(ip: &str) -> Vec<String> {
    let fut = async {
        let mut s = timeout(
            Duration::from_secs(4),
            crate::blacklist::connect(&format!("{ip}:4002")),
        )
        .await
        .ok()?
        .ok()?;
        s.set_nodelay(true).ok();
        timeout(Duration::from_secs(3), s.write_all(&QUERY_PEERS_REQUEST))
            .await
            .ok()?
            .ok()?;
        let mut hdr = [0u8; 5];
        timeout(Duration::from_secs(5), s.read_exact(&mut hdr))
            .await
            .ok()?
            .ok()?;
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if len == 0 || len > MAX_QUERY_PEERS_RESP {
            return None;
        }
        let mut payload = vec![0u8; len];
        timeout(Duration::from_secs(5), s.read_exact(&mut payload))
            .await
            .ok()?
            .ok()?;
        Some(parse_query_peers_response(&payload))
    };
    fut.await.unwrap_or_default()
}

// Bounded multi-hop BFS over the gossip peer graph. Generic over the per-peer query function so
// the traversal bounds (depth cap, per-cycle query budget, visited-dedup) are unit-testable with a
// stub. Returns every routable IP discovered (self-IP exclusion is the caller's job on merge).
async fn crawl_peers_with<F, Fut>(
    seeds: Vec<String>,
    self_ips: &HashSet<String>,
    depth: usize,
    max_queries: usize,
    concurrency: usize,
    query: F,
) -> HashSet<String>
where
    F: Fn(String) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Vec<String>> + Send + 'static,
{
    let mut discovered: HashSet<String> = HashSet::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = seeds
        .into_iter()
        .filter(|ip| !self_ips.contains(ip))
        .collect();
    let mut queries = 0usize;

    for _hop in 0..depth {
        if frontier.is_empty() || queries >= max_queries {
            break;
        }
        // select this hop's targets: unvisited, bounded by the remaining query budget.
        let remaining = max_queries - queries;
        let mut targets = Vec::new();
        for ip in frontier.drain(..) {
            if targets.len() >= remaining {
                break;
            }
            if visited.insert(ip.clone()) {
                targets.push(ip);
            }
        }
        if targets.is_empty() {
            break;
        }

        let sem = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
        let mut set: tokio::task::JoinSet<Vec<String>> = tokio::task::JoinSet::new();
        for ip in targets {
            let sem = sem.clone();
            let query = query.clone();
            set.spawn(async move {
                let _permit = sem.acquire().await;
                query(ip).await
            });
        }

        // peers overlap heavily, so dedup the next frontier (`queued`) instead of carrying one copy
        // per advertising peer; `discovered` stops growing at MAX_CANDIDATES, which is all the
        // caller can merge anyway.
        let mut next: Vec<String> = Vec::new();
        let mut queued: HashSet<String> = HashSet::new();
        while let Some(r) = set.join_next().await {
            queries += 1;
            if let Ok(peers) = r {
                for p in peers.into_iter().take(MAX_PEERS_PER_QUERY_RESPONSE) {
                    if self_ips.contains(&p) {
                        continue;
                    }
                    if discovered.len() < MAX_CANDIDATES {
                        discovered.insert(p.clone());
                    }
                    if !visited.contains(&p) && queued.insert(p.clone()) {
                        next.push(p);
                    }
                }
            }
        }
        frontier = next;
    }
    discovered
}

pub(super) async fn crawl_peers(
    seeds: Vec<String>,
    self_ips: &HashSet<String>,
    depth: usize,
    max_queries: usize,
    concurrency: usize,
) -> HashSet<String> {
    crawl_peers_with(
        seeds,
        self_ips,
        depth,
        max_queries,
        concurrency,
        |ip: String| async move { query_peers(&ip).await },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    // hex -> bytes helper for the captured wire fixtures below.
    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn query_peers_request_bytes_match_capture() {
        // [u32 BE L=1][type=0][tag=0x01] exactly as sent in the live capture.
        assert_eq!(QUERY_PEERS_REQUEST, [0x00, 0x00, 0x00, 0x01, 0x00, 0x01]);
    }

    #[test]
    fn read_hl_varint_decodes_all_widths() {
        // single-byte (lead < 0xfb)
        assert_eq!(read_hl_varint(&[0x00]), Some((0, 1)));
        assert_eq!(read_hl_varint(&[0x0a, 0xff]), Some((10, 1)));
        assert_eq!(read_hl_varint(&[0xfa]), Some((0xfa, 1)));
        // 0xfb => u16 LE
        assert_eq!(read_hl_varint(&[0xfb, 0x34, 0x12]), Some((0x1234, 3)));
        // 0xfc => u32 LE
        assert_eq!(
            read_hl_varint(&[0xfc, 0x78, 0x56, 0x34, 0x12]),
            Some((0x1234_5678, 5))
        );
        // 0xfd => u64 LE
        assert_eq!(
            read_hl_varint(&[0xfd, 1, 0, 0, 0, 0, 0, 0, 0]),
            Some((1, 9))
        );
        // truncated multi-byte and 0xfe (u128) are rejected
        assert_eq!(read_hl_varint(&[0xfb, 0x34]), None);
        assert_eq!(read_hl_varint(&[0xfe, 0, 0]), None);
        assert_eq!(read_hl_varint(&[]), None);
    }

    // Real captured response payload (frame header stripped) from 64.31.48.111:4002,
    // observations/query_peers-4002-20260708.pcap: 0x01 tag, count=10, 7-byte entries.
    const CAPTURED_N10: &str = "010a000d718e560101000d9e2464010100344534ec01010034c6040b01010036b226f5010100401f307e0101004a3fcf6501010074c7e5e901010087b58a630101008bb4cd0b0101";

    // Same, from 135.181.138.99:4002 (count=8).
    const CAPTURED_N8: &str = "01080012b6cac901010023f34f70010100416d10e70101004529a8f70101005b861fce0101005fd9214d0101009db464d6010100b26918a50101";

    #[test]
    fn parse_query_peers_response_decodes_captured_n10() {
        assert_eq!(
            parse_query_peers_response(&unhex(CAPTURED_N10)),
            vec![
                "13.113.142.86",
                "13.158.36.100",
                "52.69.52.236",
                "52.198.4.11",
                "54.178.38.245",
                "64.31.48.126",
                "74.63.207.101",
                "116.199.229.233",
                "135.181.138.99",
                "139.180.205.11",
            ]
        );
    }

    #[test]
    fn parse_query_peers_response_decodes_captured_n8() {
        assert_eq!(
            parse_query_peers_response(&unhex(CAPTURED_N8)),
            vec![
                "18.182.202.201",
                "35.243.79.112",
                "65.109.16.231",
                "69.41.168.247",
                "91.134.31.206",
                "95.217.33.77",
                "157.180.100.214",
                "178.105.24.165",
            ]
        );
    }

    #[test]
    fn parse_query_peers_response_handles_small_counts() {
        // n=1
        assert_eq!(
            parse_query_peers_response(&unhex("010100080808080101")),
            vec!["8.8.8.8"]
        );
        // n=0
        assert!(parse_query_peers_response(&unhex("0100")).is_empty());
    }

    #[test]
    fn parse_query_peers_response_rejects_non_peers_payloads() {
        // error frame: 0x03 <len> <ascii reason>
        let mut err = vec![0x03, 0x11];
        err.extend_from_slice(b"Peer-only request");
        assert!(parse_query_peers_response(&err).is_empty());
        // empty / wrong leading tag
        assert!(parse_query_peers_response(&[]).is_empty());
        assert!(parse_query_peers_response(&[0x02, 0x01]).is_empty());
        // truncated varint
        assert!(parse_query_peers_response(&[0x01, 0xfb, 0x01]).is_empty());
    }

    #[test]
    fn parse_query_peers_response_is_partial_on_malformed_entries() {
        // count says 3 but only the first entry is complete: keep it, stop cleanly.
        let mut p = unhex("0103");
        p.extend_from_slice(&unhex("00080808080101")); // 8.8.8.8
        p.extend_from_slice(&unhex("000101")); // truncated entry
        assert_eq!(parse_query_peers_response(&p), vec!["8.8.8.8"]);

        // unknown discriminant (not 0x00) stops the parse at that entry.
        let mut p = unhex("0102");
        p.extend_from_slice(&unhex("00010101010101")); // 1.1.1.1
        p.extend_from_slice(&unhex("07080808080101")); // 0x07 = unknown
        assert_eq!(parse_query_peers_response(&p), vec!["1.1.1.1"]);

        // count larger than the payload never panics or over-reads.
        assert!(parse_query_peers_response(&[0x01, 0xfa]).is_empty());
    }

    #[test]
    fn parse_query_peers_response_filters_unroutable() {
        // 10.0.0.1 (private) is dropped, parsing continues to the next entry.
        let mut p = unhex("0102");
        p.extend_from_slice(&unhex("000a0000010101")); // 10.0.0.1
        p.extend_from_slice(&unhex("00080808080101")); // 8.8.8.8
        assert_eq!(parse_query_peers_response(&p), vec!["8.8.8.8"]);
    }

    // Stub crawl graph: each IP maps to the peers it advertises.
    fn stub_graph(
        edges: &[(&str, &[&str])],
    ) -> (
        Arc<std::collections::HashMap<String, Vec<String>>>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let map: std::collections::HashMap<String, Vec<String>> = edges
            .iter()
            .map(|(k, vs)| (k.to_string(), vs.iter().map(|s| s.to_string()).collect()))
            .collect();
        (
            Arc::new(map),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
    }

    fn stub_query(
        graph: Arc<std::collections::HashMap<String, Vec<String>>>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    ) -> impl Fn(String) -> std::future::Ready<Vec<String>> + Clone + Send + Sync + 'static {
        move |ip: String| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::future::ready(graph.get(&ip).cloned().unwrap_or_default())
        }
    }

    #[tokio::test]
    async fn crawl_discovers_multi_hop_and_dedups_queries() {
        let (graph, calls) = stub_graph(&[
            ("1.1.1.1", &["2.2.2.2", "3.3.3.3"]),
            ("2.2.2.2", &["4.4.4.4", "1.1.1.1"]), // back-edge: 1.1.1.1 must not be re-queried
            ("3.3.3.3", &["4.4.4.4"]),
            ("4.4.4.4", &["5.5.5.5"]),
        ]);
        let got = crawl_peers_with(
            vec!["1.1.1.1".to_string()],
            &HashSet::new(),
            4,
            100,
            4,
            stub_query(graph, calls.clone()),
        )
        .await;
        let want: HashSet<String> = ["2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5", "1.1.1.1"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(got, want);
        // 1.1.1.1, 2.2.2.2, 3.3.3.3, 4.4.4.4, 5.5.5.5 — each queried exactly once.
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn crawl_respects_depth_cap() {
        // chain 1 -> 2 -> 3 -> 4; depth=2 queries hops {1} and {2} only.
        let (graph, calls) = stub_graph(&[
            ("1.1.1.1", &["2.2.2.2"]),
            ("2.2.2.2", &["3.3.3.3"]),
            ("3.3.3.3", &["4.4.4.4"]),
        ]);
        let got = crawl_peers_with(
            vec!["1.1.1.1".to_string()],
            &HashSet::new(),
            2,
            100,
            4,
            stub_query(graph, calls.clone()),
        )
        .await;
        assert!(got.contains("2.2.2.2") && got.contains("3.3.3.3"));
        assert!(!got.contains("4.4.4.4"), "hop 3 must not be queried");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn crawl_respects_query_budget() {
        // star: seed fans out to 10 peers, budget allows the seed + 3 more queries.
        let peers: Vec<String> = (1..=10).map(|i| format!("9.9.9.{i}")).collect();
        let peer_refs: Vec<&str> = peers.iter().map(String::as_str).collect();
        let (graph, calls) = stub_graph(&[("1.1.1.1", peer_refs.as_slice())]);
        crawl_peers_with(
            vec!["1.1.1.1".to_string()],
            &HashSet::new(),
            6,
            4,
            4,
            stub_query(graph, calls.clone()),
        )
        .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn crawl_caps_what_one_response_can_inject() {
        let flood: Vec<String> = (0..MAX_PEERS_PER_QUERY_RESPONSE + 100)
            .map(|i| format!("8.{}.{}.1", i / 256, i % 256))
            .collect();
        let flood_refs: Vec<&str> = flood.iter().map(String::as_str).collect();
        let (graph, calls) = stub_graph(&[("1.1.1.1", flood_refs.as_slice())]);
        let got = crawl_peers_with(
            vec!["1.1.1.1".to_string()],
            &HashSet::new(),
            1,
            100,
            4,
            stub_query(graph, calls),
        )
        .await;
        assert_eq!(got.len(), MAX_PEERS_PER_QUERY_RESPONSE);
    }

    #[tokio::test]
    async fn crawl_never_queries_or_returns_self_ips() {
        let (graph, calls) = stub_graph(&[
            ("1.1.1.1", &["5.5.5.5", "2.2.2.2"]),
            ("2.2.2.2", &["6.6.6.6"]),
        ]);
        let self_ips: HashSet<String> = ["5.5.5.5".to_string(), "2.2.2.2".to_string()]
            .into_iter()
            .collect();
        let got = crawl_peers_with(
            vec!["1.1.1.1".to_string(), "5.5.5.5".to_string()],
            &self_ips,
            3,
            100,
            4,
            stub_query(graph, calls.clone()),
        )
        .await;
        assert!(!got.contains("5.5.5.5") && !got.contains("2.2.2.2"));
        assert!(
            !got.contains("6.6.6.6"),
            "peers behind a self-IP are unreachable"
        );
        // only the non-self seed is queried
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn crawl_caps_discovered_at_max_candidates() {
        // every query returns a full response of never-before-seen IPs
        let next_id = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let query = {
            let next_id = next_id.clone();
            move |_ip: String| {
                let base = next_id.fetch_add(
                    MAX_PEERS_PER_QUERY_RESPONSE,
                    std::sync::atomic::Ordering::SeqCst,
                );
                let peers = (base..base + MAX_PEERS_PER_QUERY_RESPONSE)
                    .map(|n| format!("11.{}.{}.{}", (n >> 16) & 255, (n >> 8) & 255, n & 255))
                    .collect();
                std::future::ready(peers)
            }
        };
        let got = crawl_peers_with(
            vec!["1.1.1.1".to_string()],
            &HashSet::new(),
            2,
            100,
            8,
            query,
        )
        .await;
        // 100 queries x 512 unique IPs would be ~51k without the cap
        assert!(next_id.load(std::sync::atomic::Ordering::SeqCst) > MAX_CANDIDATES);
        assert_eq!(got.len(), MAX_CANDIDATES);
    }
}
