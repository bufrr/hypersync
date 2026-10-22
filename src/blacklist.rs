// Upstream IP blacklist: IPv4 addresses hypersync must never connect to, however they were
// advertised (gossipRootIps, a query_peers crawl, persisted candidates, stats files, a stale
// peers.json, the priority list, a CLI upstream). Exact IPv4 match; no CIDR or IPv6.
//
// Sources (union): env HYPERSYNC_IP_BLACKLIST (comma-separated, like HL_SELF_IP) and the file
// `ip_blacklist.txt` in the data dir (one IPv4 per line, `#` comments and blank lines allowed). The
// gateway reads the file next to its peers file (/pd in compose), peerd in $HYPERSYNC_DATA; both
// re-read it on every refresh (30s / each cycle), so edits apply without a restart.
//
// Enforcement is in two layers:
// - `connect` is the only way the gateway, peerd, proxy and relay open an upstream connection, and
//   refuses a blacklisted address — "never connect" holds for every dial path, current or future;
// - candidate lists (pool, priority peers, peerd candidates) are filtered too, so selection doesn't
//   spend its time budget or priority slots on dials that would be refused, and sessions already
//   connected to an IP that gets blacklisted are torn down.

use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError, RwLock};
use std::time::Duration;

use tokio::net::TcpStream;

const ENV_KEY: &str = "HYPERSYNC_IP_BLACKLIST";
pub(crate) const FILE_NAME: &str = "ip_blacklist.txt";

static LIST: LazyLock<RwLock<Arc<HashSet<Ipv4Addr>>>> = LazyLock::new(Default::default);
// The list's size, so the common empty case answers without touching the lock.
static LEN: AtomicUsize = AtomicUsize::new(0);
// Invalid entries last reported, so a typo is reported once when it appears, not every reload.
static INVALID: Mutex<Vec<String>> = Mutex::new(Vec::new());
// Bumped on every change, so hot loops can re-check their peers only when the list changed.
static VERSION: AtomicU64 = AtomicU64::new(0);

pub(crate) fn version() -> u64 {
    VERSION.load(Ordering::Acquire)
}

// A writer never panics while holding the lock, but a poisoned lock must not take every dial
// path down with it either.
fn current() -> Arc<HashSet<Ipv4Addr>> {
    LIST.read().unwrap_or_else(PoisonError::into_inner).clone()
}

fn install_set(list: &mut Arc<HashSet<Ipv4Addr>>, ips: HashSet<Ipv4Addr>) {
    LEN.store(ips.len(), Ordering::Release);
    *list = Arc::new(ips);
    VERSION.fetch_add(1, Ordering::AcqRel);
}

// Parse the two raw sources into (the IPv4 set, invalid tokens in encounter order).
fn parse(env_val: Option<&str>, file: Option<&str>) -> (HashSet<Ipv4Addr>, Vec<String>) {
    let mut ips = HashSet::new();
    let mut invalid = Vec::new();
    let tokens = env_val.into_iter().flat_map(|v| v.split(',')).chain(
        file.into_iter()
            .flat_map(str::lines)
            .map(|l| l.split('#').next().unwrap_or("")),
    );
    for tok in tokens.map(str::trim).filter(|t| !t.is_empty()) {
        match tok.parse::<Ipv4Addr>() {
            Ok(ip) => {
                ips.insert(ip);
            }
            Err(_) => invalid.push(tok.to_string()),
        }
    }
    (ips, invalid)
}

// Re-read env + `<dir>/ip_blacklist.txt` and install the result. Logs only when the set changes
// (with any invalid entries), so a 30s refresh loop stays quiet. Returns the number of IPs.
pub(crate) fn reload(tag: &str, dir: &Path) -> usize {
    let path = dir.join(FILE_NAME);
    let env_val = std::env::var(ENV_KEY).ok();
    let file = std::fs::read_to_string(&path).ok();
    let (ips, invalid) = parse(env_val.as_deref(), file.as_deref());
    let n = ips.len();
    {
        let mut last = INVALID.lock().unwrap_or_else(PoisonError::into_inner);
        if *last != invalid {
            for tok in invalid.iter().filter(|t| !last.contains(t)) {
                eprintln!("[{tag}] blacklist: invalid entry {tok:?} skipped");
            }
            *last = invalid;
        }
    }
    let mut list = LIST.write().unwrap_or_else(PoisonError::into_inner);
    if **list != ips {
        let mut shown: Vec<String> = ips.iter().map(|ip| ip.to_string()).collect();
        shown.sort();
        // a blacklisted priority peer still shows as prio-N
        eprintln!(
            "[{tag}] blacklist: {n} IP(s) never dialed (env {ENV_KEY} + {}): {}",
            path.display(),
            crate::priority::redact(&shown.join(","))
        );
        install_set(&mut list, ips);
    }
    n
}

// Tests share the process-wide list: every test that installs one holds this lock, and installs
// only TEST-NET addresses (192.0.2/24, 198.51.100/24, 203.0.113/24) that no other test uses.
#[cfg(test)]
pub(crate) static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
pub(crate) fn install(ips: &[&str]) {
    install_set(
        &mut LIST.write().unwrap(),
        ips.iter().map(|s| s.parse().unwrap()).collect(),
    );
}

// Is `addr` (an IPv4, "ip:port", or a hostname) blacklisted? Hostnames never are.
pub(crate) fn contains(addr: &str) -> bool {
    if LEN.load(Ordering::Acquire) == 0 {
        return false;
    }
    let host = match addr.rsplit_once(':') {
        Some((h, p)) if p.parse::<u16>().is_ok() => h,
        _ => addr,
    };
    match host.parse::<Ipv4Addr>() {
        Ok(ip) => current().contains(&ip),
        Err(_) => false,
    }
}

// The current list as strings, for callers that merge it into their own exclusion set.
pub(crate) fn ips() -> Vec<String> {
    current().iter().map(|ip| ip.to_string()).collect()
}

// `addrs` ("ip" or "ip:port") without the blacklisted ones, order kept.
pub(crate) fn filtered(mut addrs: Vec<String>) -> Vec<String> {
    if LEN.load(Ordering::Acquire) != 0 {
        addrs.retain(|a| !contains(a));
    }
    addrs
}

// The one upstream dial: refuses a blacklisted address before any packet is sent.
pub(crate) async fn connect(addr: &str) -> std::io::Result<TcpStream> {
    if contains(addr) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("{addr} is blacklisted"),
        ));
    }
    TcpStream::connect(addr).await
}

// Resolves once `ip` is blacklisted (hot reload): lets a session connected to it end itself.
pub(crate) async fn wait_blacklisted(ip: &str) {
    while !contains(ip) {
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> HashSet<Ipv4Addr> {
        items.iter().map(|s| s.parse().unwrap()).collect()
    }

    #[test]
    fn parse_env_and_file_with_comments_invalid_entries_and_dups() {
        let (ips, invalid) = parse(
            Some(" 1.2.3.4, 5.6.7.8 ,,999.1.1.1"),
            Some("# header\n\n5.6.7.8\n  7.7.7.7   # note\n# 9.9.9.9 disabled\nnot-an-ip\n1.2.3\n"),
        );
        assert_eq!(ips, set(&["1.2.3.4", "5.6.7.8", "7.7.7.7"]));
        assert_eq!(invalid, ["999.1.1.1", "not-an-ip", "1.2.3"]);
        let (ips, invalid) = parse(None, None);
        assert!(ips.is_empty() && invalid.is_empty());
    }

    #[tokio::test]
    async fn installed_list_filters_matches_and_refuses_connects() {
        let _g = TEST_LOCK.lock().await;
        let v = version();
        install(&["192.0.2.7", "198.51.100.9"]);
        assert!(version() > v);
        assert!(contains("192.0.2.7"));
        assert!(contains("192.0.2.7:4001"));
        assert!(!contains("192.0.2.70"));
        assert!(!contains("localhost:4001"));
        assert_eq!(
            filtered(vec![
                "192.0.2.7".into(),
                "203.0.113.1".into(),
                "198.51.100.9:4001".into()
            ]),
            ["203.0.113.1"]
        );
        let err = connect("198.51.100.9:4001").await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        // a not-yet-blacklisted session ends once the IP is added
        let waiting = tokio::spawn(async { wait_blacklisted("203.0.113.5").await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());
        install(&["192.0.2.7", "198.51.100.9", "203.0.113.5"]);
        tokio::time::timeout(Duration::from_secs(7), waiting)
            .await
            .expect("session sees the blacklist change")
            .unwrap();
        install(&[]);
    }

    #[test]
    fn reload_reads_the_file_in_the_given_dir() {
        let _g = TEST_LOCK.blocking_lock();
        let dir = std::env::temp_dir().join(format!("hypersync_bl_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(FILE_NAME), "# ops\n203.0.113.20\nbogus\n").unwrap();
        // env HYPERSYNC_IP_BLACKLIST is not set under `cargo test`
        assert_eq!(reload("test", &dir), 1);
        assert!(contains("203.0.113.20"));
        let v = version();
        assert_eq!(reload("test", &dir), 1);
        assert_eq!(version(), v, "an unchanged list is not re-installed");
        std::fs::remove_file(dir.join(FILE_NAME)).unwrap();
        assert_eq!(reload("test", &dir), 0);
        assert!(!contains("203.0.113.20"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
