// Upstream tiers. Tier P is a short operator-pinned list of priority peers (QuickNode sentries, or
// public stand-ins before peering exists) that every node session tries first; Tier O is the
// peerd pool. Priority IPs never enter the pool, peers.json or the logs: peerd excludes them like
// our own nodes, the gateway strips them from the pool it reads, and every gateway log line goes
// through `redact`, which prints them as prio-1..N.

use std::borrow::Cow;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::util::{lock, split_csv};

// A priority peer that failed selection (refused, peer full, lagged, no block) is skipped for
// this long, so one dead sentry does not cost every reconnect its stage budget.
const PRIORITY_COOLDOWN: Duration = Duration::from_secs(30);
const DEFAULT_STAGE_MS: u64 = 1500;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BootstrapPolicy {
    // pool first; priority peers only after every pool candidate failed (default)
    Fallback,
    // priority peers never serve abci_state
    Never,
    // priority peers first, then the pool
    Prefer,
}

impl BootstrapPolicy {
    fn parse(v: &str) -> Option<Self> {
        match v.trim().to_ascii_lowercase().as_str() {
            "fallback" => Some(Self::Fallback),
            "never" => Some(Self::Never),
            "prefer" => Some(Self::Prefer),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fallback => "fallback",
            Self::Never => "never",
            Self::Prefer => "prefer",
        }
    }
}

pub(crate) struct PriorityTier {
    peers: Vec<String>,
    sessions: Vec<AtomicUsize>,
    cooldown_until: Mutex<Vec<Option<Instant>>>,
    // 0 = no cap
    max_sessions: usize,
    pub(crate) stage: Duration,
    pub(crate) bootstrap: BootstrapPolicy,
    // Keep nodes on the priority peers (QuickNode-style peering): the pool serves a node — live
    // blocks, 4002, the 4001 fallback, abci_state, hot standbys — only while every priority peer is
    // unusable (cooling down after a failure or lag, at its session cap, or blacklisted).
    exclusive: bool,
}

impl PriorityTier {
    pub(crate) fn new(
        peers: Vec<String>,
        max_sessions: usize,
        stage: Duration,
        bootstrap: BootstrapPolicy,
    ) -> Self {
        let n = peers.len();
        Self {
            peers,
            sessions: (0..n).map(|_| AtomicUsize::new(0)).collect(),
            cooldown_until: Mutex::new(vec![None; n]),
            max_sessions,
            stage,
            bootstrap,
            exclusive: false,
        }
    }

    pub(crate) fn with_exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    // Nodes stay on the priority tier while any priority peer is usable (see `exclusive`).
    pub(crate) fn exclusive(&self) -> bool {
        self.exclusive && !self.peers.is_empty()
    }

    // May a node be served from the pool right now?
    pub(crate) fn pool_allowed(&self) -> bool {
        !self.exclusive() || self.candidates().is_empty()
    }

    // HYPERSYNC_PRIORITY_PEERS (comma-separated IPv4), HYPERSYNC_PRIORITY_MAX_SESSIONS,
    // HYPERSYNC_PRIORITY_STAGE_MS, HYPERSYNC_PRIORITY_BOOTSTRAP, HYPERSYNC_PRIORITY_EXCLUSIVE.
    // Unset = empty tier = today's single-pool behaviour.
    pub(crate) fn from_env() -> Self {
        let peers = priority_peers_from_env();
        let exclusive = std::env::var("HYPERSYNC_PRIORITY_EXCLUSIVE").map_or(true, |v| {
            !matches!(
                v.trim()
                    .trim_matches(|c| c == '"' || c == '\'')
                    .to_ascii_lowercase()
                    .as_str(),
                "0" | "false" | "no" | "off"
            )
        });
        let max_sessions = std::env::var("HYPERSYNC_PRIORITY_MAX_SESSIONS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let stage_ms = std::env::var("HYPERSYNC_PRIORITY_STAGE_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_STAGE_MS)
            .clamp(200, 3000);
        // unset: an exclusive tier also serves abci_state first, the pool only after it
        let bootstrap = match std::env::var("HYPERSYNC_PRIORITY_BOOTSTRAP") {
            Ok(v) if !v.trim().is_empty() => BootstrapPolicy::parse(&v).unwrap_or_else(|| {
                eprintln!(
                    "[gw] HYPERSYNC_PRIORITY_BOOTSTRAP={v:?} not one of fallback|never|prefer; using fallback"
                );
                BootstrapPolicy::Fallback
            }),
            _ if exclusive => BootstrapPolicy::Prefer,
            _ => BootstrapPolicy::Fallback,
        };
        Self::new(
            peers,
            max_sessions,
            Duration::from_millis(stage_ms),
            bootstrap,
        )
        .with_exclusive(exclusive)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub(crate) fn contains(&self, ip: &str) -> bool {
        self.index(ip).is_some()
    }

    fn index(&self, ip: &str) -> Option<usize> {
        self.peers.iter().position(|p| p == ip)
    }

    pub(crate) fn describe(&self) -> String {
        format!(
            "priority peers={} max_sessions={} stage={}ms bootstrap={} exclusive={}",
            self.peers.len(),
            if self.max_sessions == 0 {
                "none".to_string()
            } else {
                self.max_sessions.to_string()
            },
            self.stage.as_millis(),
            self.bootstrap.as_str(),
            self.exclusive()
        )
    }

    // Priority peers worth trying now: not blacklisted, not in cooldown and below the session
    // cap, least-loaded first (ties keep the configured order).
    pub(crate) fn candidates(&self) -> Vec<String> {
        self.candidates_at(Instant::now())
    }

    fn candidates_at(&self, now: Instant) -> Vec<String> {
        let cooldown = lock(&self.cooldown_until);
        let mut out: Vec<(usize, usize)> = Vec::new();
        for (i, until) in cooldown.iter().enumerate() {
            if until.is_some_and(|t| now < t) || crate::blacklist::contains(&self.peers[i]) {
                continue;
            }
            let load = self.sessions[i].load(Ordering::Acquire);
            if self.max_sessions != 0 && load >= self.max_sessions {
                continue;
            }
            out.push((load, i));
        }
        out.sort();
        out.into_iter()
            .map(|(_, i)| self.peers[i].clone())
            .collect()
    }

    pub(crate) fn mark_failed(&self, ip: &str) {
        self.mark_failed_at(ip, Instant::now());
    }

    fn mark_failed_at(&self, ip: &str, now: Instant) {
        if let Some(i) = self.index(ip) {
            lock(&self.cooldown_until)[i] = Some(now + PRIORITY_COOLDOWN);
        }
    }

    // Count a node session on `ip` for as long as the returned guard lives. None for pool peers.
    pub(crate) fn acquire(self: &Arc<Self>, ip: &str) -> Option<PrioritySession> {
        let i = self.index(ip)?;
        self.sessions[i].fetch_add(1, Ordering::AcqRel);
        Some(PrioritySession {
            tier: self.clone(),
            idx: i,
        })
    }

    // Like acquire, but refuses once `ip` is at max_sessions (0 = unlimited). The check and the
    // increment are one atomic step, so two nodes selecting at the same time can't both take the
    // last slot (candidates() only filters at the time it is called).
    pub(crate) fn try_acquire(self: &Arc<Self>, ip: &str) -> Option<PrioritySession> {
        let i = self.index(ip)?;
        let max = self.max_sessions;
        // fetch_update is deprecated (renamed try_update) since Rust 1.99, but the MSRV (1.87)
        // only has fetch_update
        #[allow(deprecated)]
        self.sessions[i]
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (max == 0 || n < max).then_some(n + 1)
            })
            .ok()?;
        Some(PrioritySession {
            tier: self.clone(),
            idx: i,
        })
    }

    pub(crate) fn sessions(&self, ip: &str) -> usize {
        self.index(ip)
            .map(|i| self.sessions[i].load(Ordering::Acquire))
            .unwrap_or(0)
    }

    // Drop priority IPs from a pool list (peers.json / candidates may still carry them if peerd
    // runs without the same env).
    pub(crate) fn strip(&self, pool: Vec<String>) -> Vec<String> {
        if self.peers.is_empty() {
            return pool;
        }
        pool.into_iter().filter(|ip| !self.contains(ip)).collect()
    }

    pub(crate) fn tier_of(&self, ip: &str) -> &'static str {
        if self.contains(ip) {
            "prio"
        } else {
            "pool"
        }
    }
}

pub(crate) struct PrioritySession {
    tier: Arc<PriorityTier>,
    idx: usize,
}

impl Drop for PrioritySession {
    fn drop(&mut self) {
        self.tier.sessions[self.idx].fetch_sub(1, Ordering::AcqRel);
    }
}

// Valid, deduplicated IPv4s from HYPERSYNC_PRIORITY_PEERS, in the configured order. Invalid
// entries are counted, never echoed: a typo'd sentry IP is still close to a real one.
pub(crate) fn priority_peers_from_env() -> Vec<String> {
    let raw = std::env::var("HYPERSYNC_PRIORITY_PEERS").unwrap_or_default();
    let mut out: Vec<String> = Vec::new();
    let mut invalid = 0usize;
    for item in split_csv(&raw) {
        match item.parse::<Ipv4Addr>() {
            Ok(ip) => {
                let s = ip.to_string();
                if !out.contains(&s) {
                    out.push(s);
                }
            }
            Err(_) => invalid += 1,
        }
    }
    if invalid > 0 {
        eprintln!("[priority] ignored {invalid} invalid HYPERSYNC_PRIORITY_PEERS entr(y/ies)");
    }
    out
}

// ---- log redaction ----

static LABELS: OnceLock<Vec<String>> = OnceLock::new();

// Install the priority list used by `redact`/`peer_label`. First call wins (process-wide).
pub(crate) fn init_redaction(peers: &[String]) {
    let _ = LABELS.set(peers.to_vec());
}

fn labels() -> &'static [String] {
    LABELS.get().map(Vec::as_slice).unwrap_or(&[])
}

pub(crate) fn redact(line: &str) -> Cow<'_, str> {
    redact_with(line, labels())
}

// Replace every whole-address occurrence of a priority IP with its label. "Whole" means not
// glued to more address characters on either side: 1.2.3.4 must not match inside 11.2.3.45 or
// 1.2.3.4.5, but must match in "1.2.3.4:4001" or "via 1.2.3.4.".
fn redact_with<'a>(line: &'a str, peers: &[String]) -> Cow<'a, str> {
    if peers.is_empty() || !peers.iter().any(|p| line.contains(p.as_str())) {
        return Cow::Borrowed(line);
    }
    let mut out = line.to_string();
    for (i, ip) in peers.iter().enumerate() {
        let label = format!("prio-{}", i + 1);
        let mut from = 0usize;
        while let Some(pos) = out[from..].find(ip.as_str()).map(|p| p + from) {
            let end = pos + ip.len();
            let bytes = out.as_bytes();
            let before_ok =
                pos == 0 || !(bytes[pos - 1].is_ascii_digit() || bytes[pos - 1] == b'.');
            let after_ok = match bytes.get(end) {
                None => true,
                Some(c) if c.is_ascii_digit() => false,
                Some(b'.') => !bytes.get(end + 1).is_some_and(u8::is_ascii_digit),
                Some(_) => true,
            };
            if before_ok && after_ok {
                out.replace_range(pos..end, &label);
                from = pos + label.len();
            } else {
                from = pos + 1;
            }
        }
    }
    Cow::Owned(out)
}

// eprintln! with priority IPs redacted. Use for every log line that can name a peer.
macro_rules! gwlog {
    ($($arg:tt)*) => {
        eprintln!("{}", $crate::priority::redact(&format!($($arg)*)))
    };
}
pub(crate) use gwlog;

#[cfg(test)]
mod tests {
    use super::*;

    fn tier(peers: &[&str], max: usize) -> Arc<PriorityTier> {
        Arc::new(PriorityTier::new(
            peers.iter().map(|s| s.to_string()).collect(),
            max,
            Duration::from_millis(1500),
            BootstrapPolicy::Fallback,
        ))
    }

    // An exclusive tier lets the pool serve a node only while no priority peer is usable: each
    // one cooling down, at its session cap or blacklisted. A staged or empty tier always may.
    #[test]
    fn the_pool_serves_an_exclusive_tier_only_when_no_priority_peer_is_usable() {
        let t = Arc::new(
            PriorityTier::new(
                vec!["203.0.113.71".into(), "203.0.113.72".into()],
                1,
                Duration::from_millis(1500),
                BootstrapPolicy::Prefer,
            )
            .with_exclusive(true),
        );
        assert!(t.exclusive());
        assert!(!t.pool_allowed());
        t.mark_failed("203.0.113.71");
        assert!(!t.pool_allowed(), "one priority peer still serves");
        let held = t.try_acquire("203.0.113.72").unwrap();
        assert!(t.pool_allowed(), "one cooling down, the other full");
        drop(held);
        assert!(!t.pool_allowed());
        assert!(tier(&["203.0.113.71"], 0).pool_allowed(), "staged tier");
        let empty = PriorityTier::new(
            Vec::new(),
            0,
            Duration::from_millis(1500),
            BootstrapPolicy::Fallback,
        )
        .with_exclusive(true);
        assert!(
            !empty.exclusive() && empty.pool_allowed(),
            "no priority peers"
        );
    }

    #[test]
    fn blacklisted_priority_peers_are_not_candidates() {
        let _g = crate::blacklist::TEST_LOCK.blocking_lock();
        let t = tier(&["192.0.2.50", "192.0.2.51"], 0);
        crate::blacklist::install(&["192.0.2.50"]);
        assert_eq!(t.candidates(), ["192.0.2.51"]);
        crate::blacklist::install(&[]);
        assert_eq!(t.candidates(), ["192.0.2.50", "192.0.2.51"]);
    }

    #[test]
    fn try_acquire_never_exceeds_the_cap() {
        let t = tier(&["1.2.3.4"], 2);
        let a = t.try_acquire("1.2.3.4").expect("first slot");
        let b = t.try_acquire("1.2.3.4").expect("second slot");
        assert!(t.try_acquire("1.2.3.4").is_none(), "cap of 2 reached");
        assert_eq!(t.sessions("1.2.3.4"), 2);
        drop(a);
        assert!(
            t.try_acquire("1.2.3.4").is_some(),
            "a freed slot is reusable"
        );
        drop(b);
        assert!(
            t.try_acquire("9.9.9.9").is_none(),
            "pool peers have no slots"
        );
        let unlimited = tier(&["1.2.3.4"], 0);
        let held: Vec<_> = (0..10).map(|_| unlimited.try_acquire("1.2.3.4")).collect();
        assert!(held.iter().all(|s| s.is_some()));
    }

    #[test]
    fn candidates_least_loaded_first_then_configured_order() {
        let t = tier(&["1.1.1.1", "2.2.2.2", "3.3.3.3"], 0);
        assert_eq!(t.candidates(), vec!["1.1.1.1", "2.2.2.2", "3.3.3.3"]);
        let _a = t.acquire("1.1.1.1").unwrap();
        let _b = t.acquire("1.1.1.1").unwrap();
        let _c = t.acquire("3.3.3.3").unwrap();
        assert_eq!(t.candidates(), vec!["2.2.2.2", "3.3.3.3", "1.1.1.1"]);
    }

    #[test]
    fn session_cap_skips_full_peers_and_guard_releases() {
        let t = tier(&["1.1.1.1", "2.2.2.2"], 1);
        let a = t.acquire("1.1.1.1").unwrap();
        assert_eq!(t.candidates(), vec!["2.2.2.2"]);
        assert_eq!(t.sessions("1.1.1.1"), 1);
        drop(a);
        assert_eq!(t.sessions("1.1.1.1"), 0);
        assert_eq!(t.candidates(), vec!["1.1.1.1", "2.2.2.2"]);
        assert!(t.acquire("9.9.9.9").is_none(), "pool peers are not counted");
    }

    #[test]
    fn cooldown_skips_failed_peer_until_it_expires() {
        let t = tier(&["1.1.1.1", "2.2.2.2"], 0);
        let now = Instant::now();
        t.mark_failed_at("1.1.1.1", now);
        assert_eq!(t.candidates_at(now), vec!["2.2.2.2"]);
        assert_eq!(
            t.candidates_at(now + PRIORITY_COOLDOWN),
            vec!["1.1.1.1", "2.2.2.2"]
        );
        t.mark_failed_at("8.8.8.8", now); // not a priority peer: no-op
    }

    #[test]
    fn empty_tier_is_today_behaviour() {
        let t = tier(&[], 0);
        assert!(t.is_empty());
        assert!(t.candidates().is_empty());
        let pool = vec!["1.1.1.1".to_string(), "2.2.2.2".to_string()];
        assert_eq!(t.strip(pool.clone()), pool);
        assert_eq!(t.tier_of("1.1.1.1"), "pool");
    }

    #[test]
    fn strip_removes_priority_ips_from_pool() {
        let t = tier(&["2.2.2.2"], 0);
        let pool = vec!["1.1.1.1".into(), "2.2.2.2".into(), "3.3.3.3".into()];
        assert_eq!(t.strip(pool), vec!["1.1.1.1", "3.3.3.3"]);
        assert_eq!(t.tier_of("2.2.2.2"), "prio");
    }

    #[test]
    fn bootstrap_policy_parses() {
        assert_eq!(
            BootstrapPolicy::parse("never"),
            Some(BootstrapPolicy::Never)
        );
        assert_eq!(
            BootstrapPolicy::parse(" Prefer "),
            Some(BootstrapPolicy::Prefer)
        );
        assert_eq!(
            BootstrapPolicy::parse("fallback"),
            Some(BootstrapPolicy::Fallback)
        );
        assert_eq!(BootstrapPolicy::parse("always"), None);
    }

    #[test]
    fn redact_replaces_whole_addresses_only() {
        let peers = vec!["1.2.3.4".to_string(), "10.0.0.5".to_string()];
        let cases = [
            ("via 1.2.3.4 ok", "via prio-1 ok"),
            ("dial 1.2.3.4:4001 failed", "dial prio-1:4001 failed"),
            ("ended at 1.2.3.4.", "ended at prio-1."),
            ("[\"1.2.3.4\",\"10.0.0.5\"]", "[\"prio-1\",\"prio-2\"]"),
            (
                "11.2.3.45 and 1.2.3.45 and 1.2.3.4.5",
                "11.2.3.45 and 1.2.3.45 and 1.2.3.4.5",
            ),
            ("1.2.3.4 then 1.2.3.4", "prio-1 then prio-1"),
            ("no addresses here", "no addresses here"),
        ];
        for (input, want) in cases {
            assert_eq!(redact_with(input, &peers), want, "input: {input}");
        }
        assert!(matches!(redact_with("x", &[]), Cow::Borrowed(_)));
    }
}
