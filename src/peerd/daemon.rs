use crate::protocol::{corroborated_clustered_round, is_plausible_mainnet_round};
use crate::util::split_csv;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

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

// Probe rounds are floor-only validated, so clustering is the only thing standing between a high
// decode artifact and the published tip. The gateway rejects peers that trail the tip, so a tip
// raised by a few peers would starve every honest peer: publish the cluster's MEDIAN round, so a
// minority of the cluster (one artifact, or a couple of peers reporting up to MAX_LIVE_ROUND_LAG
// above the rest) can't move it. No tip (null) is safe by comparison: the gateway keeps its last
// reading and, if none comes for a while, drops to floor-only checks.
fn clustered_probe_tip(probes: &[(String, LiveProbe)]) -> Option<u32> {
    let mut rounds: Vec<u32> = probes
        .iter()
        .filter_map(|(_, probe)| probe.max_round)
        .collect();
    corroborated_clustered_round(&mut rounds, 2, MAX_LIVE_ROUND_LAG)
}

fn split_live_by_tip(
    probed_live: Vec<(String, LiveProbe)>,
    round_tip: Option<u32>,
) -> (Vec<(String, LiveProbe)>, Vec<String>) {
    let Some(tip) = round_tip else {
        return (probed_live, Vec::new());
    };
    let mut live = Vec::new();
    let mut lagged = Vec::new();
    for (ip, probe) in probed_live {
        if probe.max_round.is_some_and(|round| {
            round.saturating_add(MAX_LIVE_ROUND_LAG) >= tip
                && round <= tip.saturating_add(MAX_LIVE_ROUND_LAG)
        }) {
            live.push((ip, probe));
        } else {
            lagged.push(ip);
        }
    }
    (live, lagged)
}

fn env_usize(name: &str, default: usize, min: usize, max: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(min, max))
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64, min: u64, max: u64) -> u64 {
    let max = max.max(min);
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(min, max))
        .unwrap_or_else(|| default.clamp(min, max))
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

// Back off only when the probe found nothing AND there was no previous pool to keep. While
// kept_previous holds a good pool, peers.json is not rewritten, so the gateway ages its round tip
// out after 15 min; a backed-off sleep (up to 8x interval) would keep the recovery probe away for
// that long, so probe at the normal cadence instead.
fn next_empty_live_streak(streak: u32, ranked_empty: bool, kept_previous: bool) -> u32 {
    if ranked_empty && !kept_previous {
        streak.saturating_add(1)
    } else {
        0
    }
}

fn select_probe_candidates(
    all_candidates: &[String],
    previous_live: &[String],
    max_per_cycle: usize,
    cursor: &mut usize,
) -> Vec<String> {
    let max_per_cycle = max_per_cycle.max(1);
    let previous_limit = (max_per_cycle / 2).max(1);
    let candidate_set: HashSet<&str> = all_candidates.iter().map(String::as_str).collect();
    let mut selected = Vec::new();
    let mut seen = HashSet::new();

    for ip in previous_live {
        if selected.len() >= previous_limit {
            break;
        }
        if candidate_set.contains(ip.as_str()) && seen.insert(ip.clone()) {
            selected.push(ip.clone());
        }
    }

    let remaining = max_per_cycle.saturating_sub(selected.len());
    let n = all_candidates.len();
    if remaining > 0 && n > 0 {
        let start = *cursor % n;
        let mut added = 0usize;
        let mut scanned = 0usize;
        for off in 0..n {
            scanned = off + 1;
            let ip = &all_candidates[(start + off) % n];
            if seen.insert(ip.clone()) {
                selected.push(ip.clone());
                added += 1;
                if added >= remaining {
                    break;
                }
            }
        }
        *cursor = (start + scanned) % n;
    }

    selected
}

// Root IPs from the gossipRootIps curl. A failed curl (non-zero exit: HTTP error with -f, timeout,
// DNS) is treated like a spawn error/timeout and contributes nothing, even if it wrote some stdout.
// The answer must be exactly the API's JSON array of IPv4 strings (~400 bytes): an error or
// maintenance page that merely mentions addresses must not seed candidates.
const MAX_GOSSIP_ROOTS_BYTES: usize = 64 * 1024;

fn gossip_root_ips(out: &std::process::Output) -> Vec<String> {
    if !out.status.success() || out.stdout.len() > MAX_GOSSIP_ROOTS_BYTES {
        return Vec::new();
    }
    std::str::from_utf8(&out.stdout)
        .ok()
        .and_then(parse_ip_string_array)
        .unwrap_or_default()
}

// `["1.2.3.4", "5.6.7.8"]` -> routable IPv4s, or None for anything else (not an array, an entry
// that isn't a quoted IPv4).
fn parse_ip_string_array(body: &str) -> Option<Vec<String>> {
    let inner = body.trim().strip_prefix('[')?.strip_suffix(']')?.trim();
    if inner.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::new();
    for item in inner.split(',') {
        let ip = item.trim().strip_prefix('"')?.strip_suffix('"')?;
        ip.parse::<std::net::Ipv4Addr>().ok()?;
        if is_routable(ip) {
            out.push(ip.to_string());
        }
    }
    Some(out)
}

// peer_candidates.txt: one IP per line, newline-terminated so a manual `echo ip >> file` starts a
// fresh line. On load keep only lines that are a routable IPv4 on their own, so a hand-edited or
// corrupted line is dropped rather than seeded into the crawl and probe.
fn parse_peer_candidates(contents: &str) -> HashSet<String> {
    contents
        .lines()
        .map(str::trim)
        .filter(|l| is_ipv4(l) && is_routable(l))
        .map(str::to_string)
        .collect()
}

fn format_peer_candidates(candidates: &HashSet<String>) -> String {
    let mut out = String::new();
    for ip in candidates {
        out.push_str(ip);
        out.push('\n');
    }
    out
}

// Forget candidates (and their failure counts) that are now excluded — a newly blacklisted IP.
// Returns how many candidates went.
fn drop_excluded(
    candidates: &mut HashSet<String>,
    fail_counts: &mut std::collections::HashMap<String, u32>,
    excluded: &HashSet<String>,
) -> usize {
    let before = candidates.len();
    candidates.retain(|ip| !excluded.contains(ip));
    fail_counts.retain(|ip, _| !excluded.contains(ip));
    before - candidates.len()
}

// The previous peers.json's live servers minus excluded IPs, and whether any were excluded: a
// previous pool naming a now-blacklisted IP is rewritten rather than kept.
fn previous_live_pool(contents: &str, excluded: &HashSet<String>) -> (Vec<String>, bool) {
    let mut ips = extract_ipv4(contents);
    let before = ips.len();
    ips.retain(|ip| !excluded.contains(ip));
    let tainted = ips.len() < before;
    (ips, tainted)
}

// The pool peers.json gets this cycle (None = leave the file as it is): the probe's ranking, unless
// the previous pool is kept — then nothing is written, except that a kept pool naming a
// now-excluded IP is rewritten without it (never replaced by a thinner or empty probe result,
// which would leave a restarting gateway without a pool).
fn peer_pool_to_write<'a>(
    ranked: &'a [String],
    previous_pruned: &'a [String],
    kept_previous: bool,
    previous_tainted: bool,
) -> Option<&'a [String]> {
    match (kept_previous, previous_tainted) {
        (false, _) => Some(ranked),
        (true, true) => Some(previous_pruned),
        (true, false) => None,
    }
}

// When the previous pool is kept (this cycle found fewer live peers than it holds), it is still
// updated rather than frozen: the fresh live peers go first, then the previous peers this cycle's
// probes didn't see fail, then — only to keep the pool from thinning below
// MIN_LIVE_SERVERS_TO_OVERWRITE — the ones that did. Without this a kept pool could hide every
// newly verified peer and hold dead ones for as long as fewer than 8 peers probed live.
fn merge_kept_pool(
    ranked: &[String],
    previous: &[String],
    failed: &HashSet<String>,
) -> Vec<String> {
    let mut out: Vec<String> = ranked.to_vec();
    let fresh = |ip: &String, out: &Vec<String>| !out.contains(ip);
    for ip in previous.iter().filter(|ip| !failed.contains(*ip)) {
        if fresh(ip, &out) {
            out.push(ip.clone());
        }
    }
    for ip in previous.iter().filter(|ip| failed.contains(*ip)) {
        if out.len() >= MIN_LIVE_SERVERS_TO_OVERWRITE {
            break;
        }
        if fresh(ip, &out) {
            out.push(ip.clone());
        }
    }
    out
}

pub(crate) async fn run_peerd(interval: u64) {
    use std::collections::{HashMap, HashSet};
    let data_dir = std::env::var("HYPERSYNC_DATA").unwrap_or_else(|_| "./data".to_string());
    tokio::fs::create_dir_all(&data_dir).await.ok();
    // optional: comma-separated tcp_lz4_stats dirs, one per node (e.g. each node's hl-data volume
    // mounted read-only). Unset => discovery relies on the gossipRootIps API, the query_peers crawl
    // and candidates persisted from previous cycles (the normal case when peerd runs elsewhere).
    let stats_dirs: Vec<String> = std::env::var("HL_STATS_DIR")
        .map(|v| split_csv(&v))
        .unwrap_or_default();
    // optional: comma-separated public IPs of ALL our own nodes (legitimate routable addresses,
    // but never peers to dial — feeding our nodes from each other would relay in a circle)
    let mut self_ips: HashSet<String> = std::env::var("HL_SELF_IP")
        .map(|v| split_csv(&v).into_iter().collect())
        .unwrap_or_default();
    // Priority peers (HYPERSYNC_PRIORITY_PEERS) are the gateway's own business: never probed,
    // crawled, ranked or logged here, so they never reach peers.json or peerd.log.
    let priority = crate::priority::priority_peers_from_env();
    if !priority.is_empty() {
        eprintln!(
            "[peerd] excluding {} priority peer(s) from discovery and probing",
            priority.len()
        );
    }
    self_ips.extend(priority);
    let cand_path = format!("{data_dir}/peer_candidates.txt");
    let out_path = format!("{data_dir}/peers.json");
    let log_path = format!("{data_dir}/peerd.log");
    let probe_concurrency = env_usize("PEERD_PROBE_CONCURRENCY", 8, 1, 64);
    let empty_backoff_max = env_u64("PEERD_EMPTY_BACKOFF_MAX_SECS", 1800, interval, 7200);
    // query_peers crawl bounds (self-contained peer-graph discovery).
    let crawl_depth = env_usize("PEERD_CRAWL_DEPTH", 3, 1, 6);
    let crawl_max_queries = env_usize("PEERD_CRAWL_MAX_QUERIES", 200, 10, 2000);
    let crawl_concurrency = env_usize("PEERD_CRAWL_CONCURRENCY", 8, 1, 64);
    let probe_max_per_cycle = env_usize("PEERD_PROBE_MAX_PER_CYCLE", 256, 32, 5000);

    let mut candidates: HashSet<String> = tokio::fs::read_to_string(&cand_path)
        .await
        .map(|s| parse_peer_candidates(&s))
        .unwrap_or_default();
    // the persisted file predates the current exclusions (a stand-in priority peer was usually a
    // pool candidate first)
    candidates.retain(|ip| !self_ips.contains(ip));
    // consecutive failed-probe count per candidate; prune after PRUNE_AFTER cycles so stale
    // harvested IPs don't get re-probed forever (a pruned peer still advertised by discovery is
    // simply re-added next cycle and gets a fresh count).
    const PRUNE_AFTER: u32 = 50;
    let mut fail_counts: HashMap<String, u32> = HashMap::new();
    let mut empty_live_streak = 0u32;
    let mut probe_cursor = 0usize;

    loop {
        let cycle_start = Instant::now();
        // Our own nodes, the priority peers and the blacklist (re-read every cycle) are never
        // queried, probed or published. Candidates already held for a newly blacklisted IP go now.
        crate::blacklist::reload("peerd", std::path::Path::new(&data_dir));
        let mut excluded = self_ips.clone();
        excluded.extend(crate::blacklist::ips());
        let dropped = drop_excluded(&mut candidates, &mut fail_counts, &excluded);
        if dropped > 0 {
            eprintln!("[peerd] blacklist: dropped {dropped} candidate(s)");
        }
        // 1. discover. Primary source: the gossipRootIps API (seed) + a query_peers crawl of the
        // gossip peer graph (self-contained, no co-located node needed). tcp_lz4_stats stays as an
        // optional supplementary source. Shelling out to curl (rather than an HTTPS client) keeps
        // the API call to a thin, standard external tool instead of a heavyweight dependency for a
        // one-shot JSON POST. curl's own timeouts sit inside the outer 15s guard, and
        // kill_on_drop makes that guard actually reap a hung curl (tokio leaves a dropped child
        // running otherwise, one more per cycle).
        let roots = timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("curl")
                .args([
                    "-s",
                    "-f",
                    "--connect-timeout",
                    "5",
                    "--max-time",
                    "10",
                    "--max-filesize",
                    "65536",
                    "-X",
                    "POST",
                    "-H",
                    "Content-Type: application/json",
                    "--data",
                    r#"{"type":"gossipRootIps"}"#,
                    "https://api.hyperliquid.xyz/info",
                ])
                .kill_on_drop(true)
                .output(),
        )
        .await;

        if let Ok(Ok(o)) = roots {
            for ip in gossip_root_ips(&o) {
                if !excluded.contains(ip.as_str()) {
                    candidates.insert(ip);
                }
            }
        }
        // 1b. crawl the gossip peer graph via query_peers, seeded from the roots + persisted
        // candidates. This replaces tcp_lz4_stats as the backbone discovery source.
        let seeds: Vec<String> = candidates.iter().cloned().collect();
        let discovered = crawl_peers(
            seeds,
            &excluded,
            crawl_depth,
            crawl_max_queries,
            crawl_concurrency,
        )
        .await;
        let mut crawl_new = 0usize;
        let mut crawl_capped = 0usize;
        for ip in discovered {
            if excluded.contains(&ip) || candidates.contains(&ip) {
                continue;
            }
            if candidates.len() >= MAX_CANDIDATES {
                crawl_capped += 1;
                continue;
            }
            candidates.insert(ip);
            crawl_new += 1;
        }
        for dir in &stats_dirs {
            // read_node_peers handles a directory (every <date> file) and applies the same
            // routable-IPv4 extraction; the files are a few KB each, sync read is fine here.
            for ip in read_node_peers(dir) {
                if !excluded.contains(ip.as_str()) {
                    candidates.insert(ip);
                }
            }
        }
        write_atomic(&cand_path, &format_peer_candidates(&candidates)).await;

        // 2. Probe a bounded slice for live-block serving. The candidate set can grow into the
        // hundreds; probing all of it with low concurrency makes a single cycle exceed the
        // configured interval. Keep the previous top live peers hot, then rotate through the rest
        // so stale candidates are still aged out over multiple cycles.
        let previous_contents = tokio::fs::read_to_string(&out_path)
            .await
            .unwrap_or_default();
        let (previous_live_ips, previous_tainted) =
            previous_live_pool(&previous_contents, &excluded);
        let mut all_candidates: Vec<String> = candidates.iter().cloned().collect();
        all_candidates.sort();
        let total_candidates = all_candidates.len();
        let cand_vec = select_probe_candidates(
            &all_candidates,
            &previous_live_ips,
            probe_max_per_cycle,
            &mut probe_cursor,
        );
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
                    && probe
                        .max_round
                        .is_some_and(|round| is_plausible_mainnet_round(round, 0))
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
        let (mut live, lagged) = split_live_by_tip(probed_live, round_tip);
        failed.extend(lagged);
        for ip in &live {
            fail_counts.remove(&ip.0);
        }
        let failed_now: HashSet<String> = failed.iter().cloned().collect();
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

        let previous_live = previous_live_ips.len();
        let previous_trusted = previous_peer_pool_is_trusted(&previous_contents);
        let kept_previous =
            should_keep_previous_peer_pool(ranked.len(), previous_live, previous_trusted);
        let merged;
        let published = if kept_previous && !ranked.is_empty() {
            merged = merge_kept_pool(&ranked, &previous_live_ips, &failed_now);
            Some(merged.as_slice())
        } else {
            peer_pool_to_write(&ranked, &previous_live_ips, kept_previous, previous_tainted)
        };
        // 3. write peers.json (hand-rolled: content is plain IPv4 strings, no escaping needed)
        let round_tip_json = round_tip
            .map(|round| round.to_string())
            .unwrap_or_else(|| "null".to_string());
        let json = format!(
            "{{\"live_servers\":[{}],\"n_candidates\":{},\"round_tip\":{},\"candidate_fallback\":false}}",
            published
                .unwrap_or_default()
                .iter()
                .map(|ip| format!("\"{ip}\""))
                .collect::<Vec<_>>()
                .join(","),
            total_candidates,
            round_tip_json
        );
        if published.is_some() {
            write_atomic(&out_path, &json).await;
        }
        empty_live_streak =
            next_empty_live_streak(empty_live_streak, ranked.is_empty(), kept_previous);
        let cycle_secs = cycle_start.elapsed().as_secs();
        let target_sleep = empty_pool_sleep_secs(interval, empty_live_streak, empty_backoff_max);
        // Discover+probe can itself take longer than `interval` once the candidate pool grows;
        // only sleep the remainder so cadence stays close to configured instead of interval+work.
        let sleep_secs = target_sleep.saturating_sub(cycle_secs);

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
            "{now} candidates={} probed={} crawl_new={} crawl_capped={} live={} round_tip={:?} pruned={} kept_previous={} probe_concurrency={} probe_max_per_cycle={} empty_streak={} cycle={}s sleep={}s reasons={:?} samples={:?} candidate_fallback={} top={:?}\n",
            total_candidates,
            cand_vec.len(),
            crawl_new,
            crawl_capped,
            ranked.len(),
            round_tip,
            pruned,
            kept_previous,
            probe_concurrency,
            probe_max_per_cycle,
            empty_live_streak,
            cycle_secs,
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
    fn peerd_probe_selection_prioritizes_previous_live_then_rotates_candidates() {
        let all = ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let previous = ["4.4.4.4", "9.9.9.9", "2.2.2.2"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let mut cursor = 0usize;

        let first = select_probe_candidates(&all, &previous, 4, &mut cursor);
        assert_eq!(first, vec!["4.4.4.4", "2.2.2.2", "1.1.1.1", "3.3.3.3"]);
        assert_eq!(cursor, 3);

        let second = select_probe_candidates(&all, &previous, 4, &mut cursor);
        assert_eq!(second, vec!["4.4.4.4", "2.2.2.2", "5.5.5.5", "1.1.1.1"]);
        assert_eq!(cursor, 1);
    }

    #[test]
    fn peerd_probe_selection_handles_empty_candidates() {
        let mut cursor = 7usize;
        let selected = select_probe_candidates(&[], &[String::from("1.1.1.1")], 32, &mut cursor);
        assert!(selected.is_empty());
        assert_eq!(cursor, 7);
    }

    #[test]
    fn peerd_env_u64_handles_min_above_max() {
        std::env::remove_var("PEERD_TEST_BACKOFF");
        assert_eq!(env_u64("PEERD_TEST_BACKOFF", 1800, 9000, 7200), 9000);
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
        assert_eq!(clustered_probe_tip(&probes), Some(1_355_730_000));

        // a lone probe cannot publish a tip: with nothing to corroborate it, a misparse would pin
        // the gateway's monotonic tip above every honest peer and starve live serving for good
        let one = vec![(
            "1.1.1.1".to_string(),
            LiveProbe {
                blocks: 2,
                max_round: Some(2_818_597_137),
                ..LiveProbe::default()
            },
        )];
        assert_eq!(clustered_probe_tip(&one), None);
        let (live, failed) = split_live_by_tip(one, None);
        assert_eq!(live.len(), 1, "the lone peer still stays in the pool");
        assert!(failed.is_empty());
    }

    #[test]
    fn peerd_keeps_probed_live_when_tip_cannot_cluster() {
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
                    max_round: Some(1_355_900_000),
                    ..LiveProbe::default()
                },
            ),
        ];
        let tip = clustered_probe_tip(&probes);
        assert_eq!(tip, None);

        let (live, failed) = split_live_by_tip(probes, tip);
        assert_eq!(live.len(), 2);
        assert!(failed.is_empty());
    }

    #[test]
    fn peerd_tip_filter_rejects_high_misparse_peers() {
        let mk = |round: u32| LiveProbe {
            blocks: 2,
            max_round: Some(round),
            ..LiveProbe::default()
        };
        let probes = vec![
            ("1.1.1.1".to_string(), mk(1_355_900_000)),
            ("2.2.2.2".to_string(), mk(1_355_900_100)),
            // a high decode artifact must not survive the tip filter (it would rank first)
            ("3.3.3.3".to_string(), mk(2_818_597_137)),
        ];
        let tip = clustered_probe_tip(&probes);
        assert_eq!(tip, Some(1_355_900_000));
        let (live, failed) = split_live_by_tip(probes, tip);
        assert_eq!(live.len(), 2);
        assert_eq!(failed, vec!["3.3.3.3".to_string()]);
    }

    #[test]
    fn peerd_tip_resists_a_colluding_minority() {
        let mk = |round: u32| LiveProbe {
            blocks: 2,
            max_round: Some(round),
            ..LiveProbe::default()
        };
        let honest = 1_490_000_000u32;
        let probes = vec![
            ("1.1.1.1".to_string(), mk(honest)),
            ("2.2.2.2".to_string(), mk(honest + 20)),
            ("3.3.3.3".to_string(), mk(honest + 40)),
            // two colluding peers inside the 50k cluster gap
            ("4.4.4.4".to_string(), mk(honest + 49_998)),
            ("5.5.5.5".to_string(), mk(honest + 49_999)),
        ];
        assert_eq!(clustered_probe_tip(&probes), Some(honest + 40));
    }

    #[test]
    fn excluded_ips_leave_candidates_and_taint_the_previous_pool() {
        let excluded: HashSet<String> = ["5.5.5.5".to_string()].into();
        let mut candidates: HashSet<String> = ["5.5.5.5", "6.6.6.6"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut fails: std::collections::HashMap<String, u32> =
            [("5.5.5.5".to_string(), 3), ("6.6.6.6".to_string(), 1)].into();
        assert_eq!(drop_excluded(&mut candidates, &mut fails, &excluded), 1);
        assert_eq!(candidates, ["6.6.6.6".to_string()].into());
        assert!(!fails.contains_key("5.5.5.5") && fails.contains_key("6.6.6.6"));

        let prev = r#"{"live_servers":["6.6.6.6","5.5.5.5"],"round_tip":1}"#;
        assert_eq!(
            previous_live_pool(prev, &excluded),
            (vec!["6.6.6.6".to_string()], true)
        );
        assert_eq!(
            previous_live_pool(prev, &HashSet::new()),
            (vec!["6.6.6.6".to_string(), "5.5.5.5".to_string()], false)
        );
    }

    // A kept previous pool still publishes the fresh live peers (first) and drops previous peers
    // that just failed, unless they're needed to keep the pool from thinning below 8.
    #[test]
    fn a_kept_pool_takes_fresh_peers_and_sheds_failed_ones() {
        let ip = |i: u32| format!("7.7.7.{i}");
        let previous: Vec<String> = (1..=10).map(ip).collect();
        let fresh = vec!["9.9.9.9".to_string()];
        // two previous peers failed this cycle; 1 fresh + 8 surviving previous = 9 >= 8
        let failed: HashSet<String> = [ip(1), ip(2)].into();
        let out = merge_kept_pool(&fresh, &previous, &failed);
        assert_eq!(out[0], "9.9.9.9");
        assert_eq!(out.len(), 9);
        assert!(!out.contains(&ip(1)) && !out.contains(&ip(2)));
        // most previous peers failed: the failed ones only top the pool up to 8, after the rest
        let failed: HashSet<String> = (1..=9).map(ip).collect();
        let out = merge_kept_pool(&fresh, &previous, &failed);
        assert_eq!(out.len(), 8);
        assert_eq!(&out[..2], &["9.9.9.9".to_string(), ip(10)]);
        // nothing duplicated when a fresh peer was also in the previous pool
        let out = merge_kept_pool(&[ip(3)], &previous, &HashSet::new());
        assert_eq!(out.len(), 10);
        assert_eq!(out[0], ip(3));
    }

    #[test]
    fn a_tainted_kept_pool_is_rewritten_pruned_never_emptied() {
        let ranked: Vec<String> = Vec::new();
        let pruned = vec!["6.6.6.6".to_string()];
        // empty probe, previous kept: an untainted file is left alone ...
        assert_eq!(peer_pool_to_write(&ranked, &pruned, true, false), None);
        // ... a tainted one is rewritten with the pruned previous pool, not the empty ranking
        assert_eq!(
            peer_pool_to_write(&ranked, &pruned, true, true),
            Some(pruned.as_slice())
        );
        // a ranking that replaces the previous pool is written as usual
        let fresh: Vec<String> = (1..=9).map(|i| format!("7.7.7.{i}")).collect();
        assert_eq!(
            peer_pool_to_write(&fresh, &pruned, false, true),
            Some(fresh.as_slice())
        );
    }

    #[test]
    fn peerd_empty_streak_only_grows_without_a_kept_pool() {
        // nothing live and nothing kept: back off
        assert_eq!(next_empty_live_streak(0, true, false), 1);
        assert_eq!(next_empty_live_streak(3, true, false), 4);
        // nothing live but a good previous pool kept: stay at the normal cadence
        assert_eq!(next_empty_live_streak(3, true, true), 0);
        assert_eq!(
            empty_pool_sleep_secs(120, next_empty_live_streak(3, true, true), 1800),
            120
        );
        // some live peers (kept or written): reset
        assert_eq!(next_empty_live_streak(3, false, true), 0);
        assert_eq!(next_empty_live_streak(3, false, false), 0);
    }

    #[test]
    fn peer_candidates_round_trip_with_trailing_newline_and_validation() {
        let set: HashSet<String> = ["8.8.8.8", "1.1.1.1"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let text = format_peer_candidates(&set);
        assert!(text.ends_with('\n'));
        assert_eq!(parse_peer_candidates(&text), set);
        // a manual append now lands on its own line
        let appended = format!("{text}9.9.9.9\n");
        assert!(parse_peer_candidates(&appended).contains("9.9.9.9"));
        assert!(format_peer_candidates(&HashSet::new()).is_empty());
    }

    #[test]
    fn peer_candidates_load_drops_invalid_lines() {
        let got = parse_peer_candidates(
            "8.8.8.8\n 1.1.1.1 \n8.8.8.89.9.9.9\n10.0.0.1\n999.1.1.1\n\ngarbage\n172.20.0.1\n4.4.4.4",
        );
        let want: HashSet<String> = ["8.8.8.8", "1.1.1.1", "4.4.4.4"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(got, want);
    }

    #[cfg(unix)]
    #[test]
    fn gossip_root_ips_ignores_failed_curl() {
        use std::os::unix::process::ExitStatusExt;
        let out = |code: i32, body: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(code << 8),
            stdout: body.as_bytes().to_vec(),
            stderr: Vec::new(),
        };
        assert_eq!(
            gossip_root_ips(&out(0, r#"["8.8.8.8","1.1.1.1"]"#)),
            vec!["8.8.8.8".to_string(), "1.1.1.1".to_string()]
        );
        // e.g. curl -f on an HTTP 5xx (exit 22), or --max-time (exit 28) after a partial body
        assert!(gossip_root_ips(&out(22, r#"["8.8.8.8"]"#)).is_empty());
        assert!(gossip_root_ips(&out(28, r#"["8.8.8.8"]"#)).is_empty());
        // only the exact array shape counts: an error page naming an address seeds nothing
        assert!(gossip_root_ips(&out(0, "<html>blocked: 9.9.9.9</html>")).is_empty());
        assert!(gossip_root_ips(&out(0, r#"{"error":"9.9.9.9"}"#)).is_empty());
        assert!(gossip_root_ips(&out(0, r#"["8.8.8.8","x"]"#)).is_empty());
        assert_eq!(
            gossip_root_ips(&out(0, " [ \"8.8.8.8\" ,\n \"10.0.0.1\" ]\n")),
            vec!["8.8.8.8".to_string()],
            "whitespace tolerated, private addresses dropped"
        );
        assert!(gossip_root_ips(&out(0, "[]")).is_empty());
        let huge = format!("[{}]", vec![r#""8.8.8.8""#; 10_000].join(","));
        assert!(
            gossip_root_ips(&out(0, &huge)).is_empty(),
            "oversized answer"
        );
    }
}
