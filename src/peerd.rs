use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::protocol::{block_round, is_plausible_mainnet_round, max_clustered_round, GREET_FALSE};

fn is_ipv4(s: &str) -> bool {
    let mut parts = 0;
    for p in s.split('.') {
        parts += 1;
        if p.is_empty() || p.len() > 3 {
            return false;
        }
        match p.parse::<u32>() {
            Ok(n) if n <= 255 => {}
            _ => return false,
        }
    }
    parts == 4
}
fn is_routable(s: &str) -> bool {
    if s.starts_with("0.")
        || s.starts_with("127.")
        || s.starts_with("10.")
        || s.starts_with("192.168.")
        || s.starts_with("169.254.")
        || s.starts_with("255.")
    {
        return false;
    }
    // 172 is private ONLY for 172.16.0.0/12 (second octet 16-31). 172.0-15 and 172.32-255 are
    // public (e.g. Cloudflare 172.64/13), so don't reject the whole /8.
    if let Some(rest) = s.strip_prefix("172.") {
        if let Some(oct) = rest.split('.').next().and_then(|o| o.parse::<u8>().ok()) {
            if (16..=31).contains(&oct) {
                return false;
            }
        }
    }
    true
}
// Extract peer IPv4s out of the local node's own peer file (e.g. hl/data/tcp_lz4_stats/<date>, which
// logs every peer the node exchanged data with). Timestamps/floats/ports are not valid 4-octet IPs so
// they are skipped. The gateway uses the node's OWN discovered peers as its upstream pool.
fn extract_ipv4(s: &str) -> Vec<String> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_digit() {
            let start = i;
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
                i += 1;
            }
            let tok = &s[start..i];
            if is_ipv4(tok) && is_routable(tok) {
                out.push(tok.to_string());
            }
        } else {
            i += 1;
        }
    }
    out
}
pub(crate) fn read_node_peers(path: &str) -> Vec<String> {
    // preserve file order (peerd ranks live-servers best-first); dedup keeping first occurrence.
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    let p = std::path::Path::new(path);
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    if p.is_dir() {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                if e.path().is_file() {
                    files.push(e.path());
                }
            }
        }
    } else {
        files.push(p.to_path_buf());
    }
    for f in files {
        if let Ok(s) = std::fs::read_to_string(&f) {
            for ip in extract_ipv4(&s) {
                if seen.insert(ip.clone()) {
                    out.push(ip);
                }
            }
        }
    }
    out
}

pub(crate) fn peer_candidates_path(node_peer_file: &str) -> PathBuf {
    Path::new(node_peer_file)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("peer_candidates.txt")
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ProbeStatus {
    #[default]
    NoLiveFrame,
    Live,
    ConnectTimeout,
    ConnectError,
    GreetWriteError,
    HeaderTimeout,
    HeaderEof,
    HeaderError,
    PayloadTimeout,
    PayloadEof,
    PayloadError,
    OversizedFrame,
    PeerFull,
    StatusFrame,
    LowRound,
    UnparseableBlock,
}

impl ProbeStatus {
    fn as_str(self) -> &'static str {
        match self {
            ProbeStatus::NoLiveFrame => "no_live_frame",
            ProbeStatus::Live => "live",
            ProbeStatus::ConnectTimeout => "connect_timeout",
            ProbeStatus::ConnectError => "connect_error",
            ProbeStatus::GreetWriteError => "greet_write_error",
            ProbeStatus::HeaderTimeout => "header_timeout",
            ProbeStatus::HeaderEof => "header_eof",
            ProbeStatus::HeaderError => "header_error",
            ProbeStatus::PayloadTimeout => "payload_timeout",
            ProbeStatus::PayloadEof => "payload_eof",
            ProbeStatus::PayloadError => "payload_error",
            ProbeStatus::OversizedFrame => "oversized_frame",
            ProbeStatus::PeerFull => "peer_full",
            ProbeStatus::StatusFrame => "status_frame",
            ProbeStatus::LowRound => "low_round",
            ProbeStatus::UnparseableBlock => "unparseable_block",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LiveProbe {
    blocks: usize,
    max_round: Option<u32>,
    status: ProbeStatus,
}

// Probe one candidate for LIVE-block serving (send_abci:false — cheap, NOT rate-limited, unlike
// abci_state). Bounded to a short wall-clock total; counts parseable type=1 live rounds (excludes
// tiny status/rejection frames). A peer-controlled frame length is capped before allocating the
// payload buffer (a live block is a few hundred KB at most).
async fn probe_live(ip: &str) -> LiveProbe {
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut s = match timeout(
        Duration::from_secs(4),
        TcpStream::connect(format!("{ip}:4001")),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(_)) => {
            return LiveProbe {
                status: ProbeStatus::ConnectError,
                ..LiveProbe::default()
            };
        }
        Err(_) => {
            return LiveProbe {
                status: ProbeStatus::ConnectTimeout,
                ..LiveProbe::default()
            };
        }
    };
    if s.write_all(&GREET_FALSE).await.is_err() {
        return LiveProbe {
            status: ProbeStatus::GreetWriteError,
            ..LiveProbe::default()
        };
    }
    let mut blocks = 0usize;
    let mut max_round = None;
    let mut status = ProbeStatus::NoLiveFrame;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let mut hdr = [0u8; 5];
        match timeout(remaining, s.read_exact(&mut hdr)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                status = ProbeStatus::HeaderEof;
                break;
            }
            Ok(Err(_)) => {
                status = ProbeStatus::HeaderError;
                break;
            }
            Err(_) => {
                status = ProbeStatus::HeaderTimeout;
                break;
            }
        };
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if len > 8_000_000 {
            status = ProbeStatus::OversizedFrame;
            break;
        }
        let mut payload = vec![0u8; len];
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, s.read_exact(&mut payload)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                status = ProbeStatus::PayloadEof;
                break;
            }
            Ok(Err(_)) => {
                status = ProbeStatus::PayloadError;
                break;
            }
            Err(_) => {
                status = ProbeStatus::PayloadTimeout;
                break;
            }
        };
        if hdr[4] == 0 && len == 1 && payload.first() == Some(&4) {
            status = ProbeStatus::PeerFull;
            break;
        }
        if hdr[4] == 0 {
            status = ProbeStatus::StatusFrame;
        }
        if hdr[4] == 1 && len > 1 {
            if let Some(round) = block_round(&payload) {
                if is_plausible_mainnet_round(round) {
                    blocks += 1;
                    max_round = Some(max_round.map_or(round, |prev: u32| prev.max(round)));
                    status = ProbeStatus::Live;
                } else {
                    status = ProbeStatus::LowRound;
                }
            } else {
                status = ProbeStatus::UnparseableBlock;
            }
        }
    }
    LiveProbe {
        blocks,
        max_round,
        status,
    }
}

// Peer discovery + probing daemon (kept as its OWN process rather than a background task inside
// the gateway): discovers candidate peers (the gossipRootIps API + reading the node's own
// tcp_lz4_stats/<date> files, which log every peer the node exchanged data with) and probes each
// concurrently for live-block serving, writing a ranked pool to <data-dir>/peers.json that
// `gateway` reads and refreshes every 30s. Deliberately a separate process: a probing storm
// across 100+ candidates never touches the gateway process that is actively serving a node. The
// stats harvest is container-friendly: it only needs the node's data volume mounted read-only
// (no docker.sock, no subprocess).
// Atomically replace `path` (write temp + rename): the gateway re-reads peers.json every 30s, and
// a plain truncate-then-write could be observed half-written — extract_ipv4 on a truncated tail
// can yield a VALID but WRONG address ("…236" cut to "…23") that then enters the dial pool.
async fn write_atomic(path: &str, contents: &str) {
    let tmp = format!("{path}.tmp");
    if tokio::fs::write(&tmp, contents).await.is_ok() {
        let _ = tokio::fs::rename(&tmp, path).await;
    }
}

const MIN_LIVE_SERVERS_TO_OVERWRITE: usize = 8;
const MIN_LIVE_BLOCKS: usize = 2;
const MAX_LIVE_ROUND_LAG: u32 = 50_000;

fn should_keep_previous_peer_pool(
    current_live: usize,
    previous_live: usize,
    previous_trusted: bool,
) -> bool {
    previous_trusted
        && previous_live > 0
        && (current_live == 0
            || (current_live < MIN_LIVE_SERVERS_TO_OVERWRITE
                && previous_live >= MIN_LIVE_SERVERS_TO_OVERWRITE))
}

fn previous_peer_pool_is_trusted(contents: &str) -> bool {
    !contents.contains("\"candidate_fallback\":true") && !extract_ipv4(contents).is_empty()
}

fn clustered_probe_tip(probes: &[(String, LiveProbe)]) -> Option<u32> {
    let mut rounds: Vec<u32> = probes
        .iter()
        .filter_map(|(_, probe)| probe.max_round)
        .collect();
    if rounds.len() == 1 {
        return rounds.first().copied();
    }
    max_clustered_round(&mut rounds, 2, MAX_LIVE_ROUND_LAG)
}

fn env_usize(name: &str, default: usize, min: usize, max: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(min, max))
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64, min: u64, max: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(min, max))
        .unwrap_or(default)
}

fn empty_pool_sleep_secs(interval: u64, empty_live_streak: u32, max_backoff: u64) -> u64 {
    if empty_live_streak == 0 {
        return interval;
    }
    let multiplier = 1u64 << empty_live_streak.min(3);
    interval
        .saturating_mul(multiplier)
        .clamp(interval, max_backoff.max(interval))
}

pub(crate) fn split_csv(v: &str) -> Vec<String> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

pub(crate) async fn run_peerd(interval: u64) {
    use std::collections::{HashMap, HashSet};
    let data_dir = std::env::var("HYPERSYNC_DATA").unwrap_or_else(|_| "./data".to_string());
    tokio::fs::create_dir_all(&data_dir).await.ok();
    // optional: comma-separated tcp_lz4_stats dirs, one per node (e.g. each node's hl-data volume
    // mounted read-only). Unset => discovery relies on the gossipRootIps API + candidates
    // persisted from previous cycles (the normal case when peerd runs on a different machine).
    let stats_dirs: Vec<String> = std::env::var("HL_STATS_DIR")
        .map(|v| split_csv(&v))
        .unwrap_or_default();
    // optional: comma-separated public IPs of ALL our own nodes (legitimate routable addresses,
    // but never peers to dial — feeding our nodes from each other would relay in a circle)
    let self_ips: HashSet<String> = std::env::var("HL_SELF_IP")
        .map(|v| split_csv(&v).into_iter().collect())
        .unwrap_or_default();
    let cand_path = format!("{data_dir}/peer_candidates.txt");
    let out_path = format!("{data_dir}/peers.json");
    let log_path = format!("{data_dir}/peerd.log");
    let probe_concurrency = env_usize("PEERD_PROBE_CONCURRENCY", 8, 1, 64);
    let empty_backoff_max = env_u64("PEERD_EMPTY_BACKOFF_MAX_SECS", 1800, interval, 7200);

    let mut candidates: HashSet<String> = tokio::fs::read_to_string(&cand_path)
        .await
        .map(|s| s.lines().map(str::to_string).collect())
        .unwrap_or_default();
    // consecutive failed-probe count per candidate; prune after PRUNE_AFTER cycles so stale
    // harvested IPs don't get re-probed forever (a pruned peer still advertised by discovery is
    // simply re-added next cycle and gets a fresh count).
    const PRUNE_AFTER: u32 = 50;
    let mut fail_counts: HashMap<String, u32> = HashMap::new();
    let mut empty_live_streak = 0u32;

    loop {
        // 1. discover: gossipRootIps API + the node's own tcp_lz4_stats files (peers the node has
        // actually exchanged data with). Shelling out to curl (rather than an HTTPS client) keeps
        // this to a thin, standard external tool instead of a heavyweight dependency for a
        // one-shot JSON POST.
        let roots = timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("curl")
                .args([
                    "-s",
                    "-X",
                    "POST",
                    "-H",
                    "Content-Type: application/json",
                    "--data",
                    r#"{"type":"gossipRootIps"}"#,
                    "https://api.hyperliquid.xyz/info",
                ])
                .output(),
        )
        .await;

        if let Ok(Ok(o)) = roots {
            for ip in extract_ipv4(&String::from_utf8_lossy(&o.stdout)) {
                if !self_ips.contains(ip.as_str()) {
                    candidates.insert(ip);
                }
            }
        }
        for dir in &stats_dirs {
            // read_node_peers handles a directory (every <date> file) and applies the same
            // routable-IPv4 extraction; the files are a few KB each, sync read is fine here.
            for ip in read_node_peers(dir) {
                if !self_ips.contains(ip.as_str()) {
                    candidates.insert(ip);
                }
            }
        }
        write_atomic(
            &cand_path,
            &candidates.iter().cloned().collect::<Vec<_>>().join("\n"),
        )
        .await;

        // 2. probe every known candidate for live-block serving. Keep concurrency deliberately low:
        // each candidate is cheap, but a collapsed pool should not keep punching all public peers
        // from the same source IP in a tight burst.
        let cand_vec: Vec<String> = candidates.iter().cloned().collect();
        let sem = Arc::new(tokio::sync::Semaphore::new(probe_concurrency));
        let mut set: tokio::task::JoinSet<(String, LiveProbe)> = tokio::task::JoinSet::new();
        for ip in cand_vec.iter().cloned() {
            let sem = sem.clone();
            set.spawn(async move {
                let _permit = sem.acquire().await;
                let probe = probe_live(&ip).await;
                (ip, probe)
            });
        }
        let mut probed_live: Vec<(String, LiveProbe)> = Vec::new();
        let mut failed: Vec<String> = Vec::new();
        let mut reason_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
        let mut failed_samples: Vec<String> = Vec::new();
        while let Some(r) = set.join_next().await {
            if let Ok((ip, probe)) = r {
                *reason_counts.entry(probe.status.as_str()).or_insert(0) += 1;
                if probe.blocks >= MIN_LIVE_BLOCKS
                    && probe.max_round.is_some_and(is_plausible_mainnet_round)
                {
                    probed_live.push((ip, probe));
                } else {
                    if failed_samples.len() < 8 {
                        failed_samples.push(format!("{}:{}", ip, probe.status.as_str()));
                    }
                    failed.push(ip);
                }
            }
        }
        let round_tip = clustered_probe_tip(&probed_live);
        let mut live = Vec::new();
        if let Some(tip) = round_tip {
            for (ip, probe) in probed_live {
                if probe
                    .max_round
                    .is_some_and(|round| round.saturating_add(MAX_LIVE_ROUND_LAG) >= tip)
                {
                    live.push((ip, probe));
                } else {
                    failed.push(ip);
                }
            }
        }
        for ip in &live {
            fail_counts.remove(&ip.0);
        }
        let mut pruned = 0usize;
        for ip in failed {
            let c = fail_counts.entry(ip.clone()).or_insert(0);
            *c += 1;
            if *c >= PRUNE_AFTER {
                candidates.remove(&ip);
                fail_counts.remove(&ip);
                pruned += 1;
            }
        }
        live.sort_by(|a, b| {
            b.1.max_round
                .cmp(&a.1.max_round)
                .then_with(|| b.1.blocks.cmp(&a.1.blocks))
        });
        let ranked: Vec<String> = live.into_iter().map(|(ip, _)| ip).collect();

        let previous_contents = tokio::fs::read_to_string(&out_path)
            .await
            .unwrap_or_default();
        let previous_live = extract_ipv4(&previous_contents).len();
        let previous_trusted = previous_peer_pool_is_trusted(&previous_contents);
        let kept_previous =
            should_keep_previous_peer_pool(ranked.len(), previous_live, previous_trusted);
        // 3. write peers.json (hand-rolled: content is plain IPv4 strings, no escaping needed)
        let round_tip_json = round_tip
            .map(|round| round.to_string())
            .unwrap_or_else(|| "null".to_string());
        let json = format!(
            "{{\"live_servers\":[{}],\"n_candidates\":{},\"round_tip\":{},\"candidate_fallback\":false}}",
            ranked
                .iter()
                .map(|ip| format!("\"{ip}\""))
                .collect::<Vec<_>>()
                .join(","),
            cand_vec.len(),
            round_tip_json
        );
        if !kept_previous {
            write_atomic(&out_path, &json).await;
        }
        if ranked.is_empty() {
            empty_live_streak = empty_live_streak.saturating_add(1);
        } else {
            empty_live_streak = 0;
        }
        let sleep_secs = empty_pool_sleep_secs(interval, empty_live_streak, empty_backoff_max);

        // UTC HH:MM:SS from the system clock (no `date` subprocess)
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let now = format!(
            "{:02}:{:02}:{:02}",
            (secs / 3600) % 24,
            (secs / 60) % 60,
            secs % 60
        );
        let line = format!(
            "{now} candidates={} live={} round_tip={:?} pruned={} kept_previous={} probe_concurrency={} empty_streak={} sleep={}s reasons={:?} samples={:?} candidate_fallback={} top={:?}\n",
            cand_vec.len(),
            ranked.len(),
            round_tip,
            pruned,
            kept_previous,
            probe_concurrency,
            empty_live_streak,
            sleep_secs,
            reason_counts,
            failed_samples,
            false,
            ranked.iter().take(6).collect::<Vec<_>>()
        );
        eprint!("[peerd] {line}");
        if let Ok(mut f) = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .await
        {
            let _ = f.write_all(line.as_bytes()).await;
        }

        tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_routable_excludes_only_real_private_ranges() {
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "172.15.0.1",
            "172.32.0.1",
            "172.64.0.1",
            "173.0.0.1",
        ] {
            assert!(is_routable(ip), "{ip} should be routable");
        }
        for ip in [
            "10.0.0.1",
            "127.0.0.1",
            "192.168.1.1",
            "169.254.1.1",
            "172.16.0.1",
            "172.31.255.1",
            "0.0.0.0",
            "255.255.255.255",
        ] {
            assert!(!is_routable(ip), "{ip} should NOT be routable");
        }
    }

    #[test]
    fn split_csv_splits_and_trims() {
        assert_eq!(
            split_csv("1.2.3.4, 5.6.7.8 ,,9.9.9.9"),
            vec!["1.2.3.4", "5.6.7.8", "9.9.9.9"]
        );
        assert!(split_csv("").is_empty());
        assert_eq!(
            split_csv("/hl/node1,/hl/node2"),
            vec!["/hl/node1", "/hl/node2"]
        );
    }

    #[test]
    fn peerd_keeps_previous_pool_on_probe_collapse() {
        assert!(should_keep_previous_peer_pool(
            MIN_LIVE_SERVERS_TO_OVERWRITE - 1,
            MIN_LIVE_SERVERS_TO_OVERWRITE,
            true
        ));
        assert!(should_keep_previous_peer_pool(
            0,
            MIN_LIVE_SERVERS_TO_OVERWRITE,
            true
        ));
        assert!(!should_keep_previous_peer_pool(
            MIN_LIVE_SERVERS_TO_OVERWRITE,
            MIN_LIVE_SERVERS_TO_OVERWRITE,
            true
        ));
        assert!(!should_keep_previous_peer_pool(
            1,
            MIN_LIVE_SERVERS_TO_OVERWRITE - 1,
            true
        ));
        assert!(should_keep_previous_peer_pool(
            0,
            MIN_LIVE_SERVERS_TO_OVERWRITE - 1,
            true
        ));
        assert!(!should_keep_previous_peer_pool(
            0,
            MIN_LIVE_SERVERS_TO_OVERWRITE,
            false
        ));
    }

    #[test]
    fn peerd_trusts_only_round_annotated_previous_pool() {
        assert!(previous_peer_pool_is_trusted(
            r#"{"live_servers":["1.1.1.1"],"round_tip":1355000000,"candidate_fallback":false}"#
        ));
        assert!(previous_peer_pool_is_trusted(
            r#"{"live_servers":["1.1.1.1"],"n_candidates":48}"#
        ));
        assert!(!previous_peer_pool_is_trusted(
            r#"{"live_servers":["1.1.1.1"],"round_tip":null,"candidate_fallback":true}"#
        ));
        assert!(!previous_peer_pool_is_trusted(
            r#"{"live_servers":[],"round_tip":null,"candidate_fallback":false}"#
        ));
    }

    #[test]
    fn peerd_empty_pool_sleep_backs_off_and_caps() {
        assert_eq!(empty_pool_sleep_secs(300, 0, 1800), 300);
        assert_eq!(empty_pool_sleep_secs(300, 1, 1800), 600);
        assert_eq!(empty_pool_sleep_secs(300, 2, 1800), 1200);
        assert_eq!(empty_pool_sleep_secs(300, 3, 1800), 1800);
        assert_eq!(empty_pool_sleep_secs(300, 9, 1800), 1800);
        assert_eq!(empty_pool_sleep_secs(300, 1, 100), 300);
    }

    #[test]
    fn peerd_clusters_probe_tip_to_ignore_single_outlier() {
        let probes = vec![
            (
                "1.1.1.1".to_string(),
                LiveProbe {
                    blocks: 2,
                    max_round: Some(1_355_730_000),
                    ..LiveProbe::default()
                },
            ),
            (
                "2.2.2.2".to_string(),
                LiveProbe {
                    blocks: 2,
                    max_round: Some(1_355_731_000),
                    ..LiveProbe::default()
                },
            ),
            (
                "3.3.3.3".to_string(),
                LiveProbe {
                    blocks: 2,
                    max_round: Some(2_818_597_137),
                    ..LiveProbe::default()
                },
            ),
        ];
        assert_eq!(clustered_probe_tip(&probes), Some(1_355_731_000));

        let one = vec![(
            "1.1.1.1".to_string(),
            LiveProbe {
                blocks: 2,
                max_round: Some(1_355_730_000),
                ..LiveProbe::default()
            },
        )];
        assert_eq!(clustered_probe_tip(&one), Some(1_355_730_000));
    }

    #[test]
    fn read_node_peers_preserves_order_and_dedups() {
        let dir = std::env::temp_dir().join(format!("hypersync_rnp_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("peers.json");
        // rank order must survive (the gateway relies on peerd's best-first ordering) and the
        // duplicate must keep its FIRST position; "3" (n_candidates) is not a 4-octet IP.
        std::fs::write(
            &f,
            r#"{"live_servers":["9.9.9.9","8.8.8.8","9.9.9.9","1.1.1.1"],"n_candidates":3}"#,
        )
        .unwrap();
        let got = read_node_peers(f.to_str().unwrap());
        assert_eq!(got, vec!["9.9.9.9", "8.8.8.8", "1.1.1.1"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn extract_ipv4_skips_noise_and_private() {
        let s =
            "Ip(172.64.1.2) at 1700000000.5 port 4001, peer 8.8.8.8, bad 999.1.1.1, lan 10.0.0.3";
        let got = extract_ipv4(s);
        assert!(got.contains(&"172.64.1.2".to_string()));
        assert!(got.contains(&"8.8.8.8".to_string()));
        assert!(
            !got.contains(&"10.0.0.3".to_string()),
            "private 10/8 excluded"
        );
        assert!(!got.iter().any(|x| x == "999.1.1.1"), "octet >255 excluded");
        assert!(
            !got.iter().any(|x| x.starts_with("1700000000")),
            "float/timestamp excluded"
        );
    }
}
