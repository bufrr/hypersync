// Downstream node whitelist: source IPs/CIDRs that receive the gateway's node service. Every
// other source receives only the public probe answers (4001/4002) or an immediate close.
//
// Entries are IPv4 or IPv6 addresses, or CIDRs (prefix 0..=32 / 0..=128). CIDR host bits are
// masked. An IPv4-mapped IPv6 source is matched as its IPv4 address; otherwise the address
// families never match each other.
//
// Sources (union): env HYPERSYNC_NODE_WHITELIST (comma-separated) and `node_whitelist.txt` in the
// gateway's data dir (next to its peers file, /pd in compose; one entry per line, `#` comments and
// blank lines allowed). Both are re-read by the gateway's 30s pool refresher. Removing an entry
// affects new connections only; sessions already running are left alone.
//
// An entry that would serve the whole internet is rejected (logged, never installed): a /0, or a
// network containing this host's default gateway. Docker's userland proxy hands the gateway every
// connection it forwards (IPv6, and hairpin connections from the host itself) from the bridge
// gateway — 172.28.0.1 in compose — and a NAT router that rewrites forwarded connections does the
// same with its own address. So in compose an IPv6 node cannot be whitelisted; nodes must reach the
// gateway over IPv4 (or directly on gwnet).

use std::collections::HashSet;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError, RwLock};

const ENV_KEY: &str = "HYPERSYNC_NODE_WHITELIST";
pub(crate) const FILE_NAME: &str = "node_whitelist.txt";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum Net {
    V4 { network: u32, prefix: u8 },
    V6 { network: u128, prefix: u8 },
}

impl Net {
    fn prefix(&self) -> u8 {
        match self {
            Self::V4 { prefix, .. } | Self::V6 { prefix, .. } => *prefix,
        }
    }

    fn contains(&self, ip: IpAddr) -> bool {
        match (self, canonical(ip)) {
            (Self::V4 { network, prefix }, IpAddr::V4(ip)) => {
                mask_v4(u32::from(ip), *prefix) == *network
            }
            (Self::V6 { network, prefix }, IpAddr::V6(ip)) => {
                mask_v6(u128::from(ip), *prefix) == *network
            }
            _ => false,
        }
    }
}

impl fmt::Display for Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::V4 { network, prefix } => {
                write!(f, "{}/{}", Ipv4Addr::from(*network), prefix)
            }
            Self::V6 { network, prefix } => {
                write!(f, "{}/{}", Ipv6Addr::from(*network), prefix)
            }
        }
    }
}

static LIST: LazyLock<RwLock<Arc<Vec<Net>>>> = LazyLock::new(Default::default);
// The list's size, so the common empty case answers without touching the lock.
static LEN: AtomicUsize = AtomicUsize::new(0);
// Skipped entries (invalid or too broad) last reported, so each is reported once when it appears,
// not every reload.
static SKIPPED: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4),
        ip => ip,
    }
}

fn mask_v4(addr: u32, prefix: u8) -> u32 {
    addr & u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0)
}

fn mask_v6(addr: u128, prefix: u8) -> u128 {
    addr & u128::MAX.checked_shl(128 - u32::from(prefix)).unwrap_or(0)
}

pub(crate) fn parse_entry(token: &str) -> Option<Net> {
    let (addr, prefix) = match token.split_once('/') {
        Some((addr, prefix)) => {
            if prefix.is_empty() || !prefix.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            (addr, Some(prefix.parse::<u8>().ok()?))
        }
        None => (token, None),
    };
    match addr.parse::<IpAddr>().ok()? {
        IpAddr::V4(ip) => {
            let prefix = prefix.unwrap_or(32);
            if prefix > 32 {
                return None;
            }
            Some(Net::V4 {
                network: mask_v4(u32::from(ip), prefix),
                prefix,
            })
        }
        IpAddr::V6(ip) => {
            let prefix = prefix.unwrap_or(128);
            if prefix > 128 {
                return None;
            }
            Some(Net::V6 {
                network: mask_v6(u128::from(ip), prefix),
                prefix,
            })
        }
    }
}

// Parse the two raw sources into (the deduplicated networks, invalid tokens in encounter order).
pub(crate) fn parse(env_val: Option<&str>, file: Option<&str>) -> (Vec<Net>, Vec<String>) {
    let mut nets = HashSet::new();
    let mut invalid = Vec::new();
    let tokens = env_val.into_iter().flat_map(|v| v.split(',')).chain(
        file.into_iter()
            .flat_map(str::lines)
            .map(|l| l.split('#').next().unwrap_or("")),
    );
    for tok in tokens.map(str::trim).filter(|t| !t.is_empty()) {
        match parse_entry(tok) {
            Some(net) => {
                nets.insert(net);
            }
            None if !invalid.iter().any(|t| t == tok) => invalid.push(tok.to_string()),
            None => {}
        }
    }
    let mut nets: Vec<_> = nets.into_iter().collect();
    nets.sort_unstable();
    (nets, invalid)
}

// Why an entry that parses must still not be installed (see the module comment), or None.
// `proxy_sources` are the addresses proxied outside connections arrive from (this host's default
// gateways); with none given only a /0 is too broad.
pub(crate) fn too_broad(net: &Net, proxy_sources: &[IpAddr]) -> Option<String> {
    if net.prefix() == 0 {
        return Some("a /0 serves every address".into());
    }
    proxy_sources.iter().find(|ip| net.contains(**ip)).map(|ip| {
        format!("it contains {ip}, the default gateway that proxied outside connections arrive from")
    })
}

// The IPv4 default gateways in a /proc/net/route table. Its address columns are hex dumps of the
// kernel's network-order words, so their native-endian bytes are the address.
pub(crate) fn default_gateways(route_table: &str) -> Vec<IpAddr> {
    let mut out = Vec::new();
    for line in route_table.lines().skip(1) {
        let mut cols = line.split_whitespace().skip(1);
        let (Some(dest), Some(gw)) = (cols.next(), cols.next()) else {
            continue;
        };
        let Ok(raw) = u32::from_str_radix(gw, 16) else {
            continue;
        };
        let ip = IpAddr::V4(Ipv4Addr::from(raw.to_ne_bytes()));
        if dest == "00000000" && raw != 0 && !out.contains(&ip) {
            out.push(ip);
        }
    }
    out
}

// The last non-empty proxy-source set. A failed or empty read of the route table must not drop
// the guard: an over-broad entry rejected at startup would then be installed on the next reload.
static LAST_PROXY_SOURCES: Mutex<Vec<IpAddr>> = Mutex::new(Vec::new());

fn proxy_sources() -> Vec<IpAddr> {
    let read = std::fs::read_to_string("/proc/net/route")
        .map(|table| default_gateways(&table))
        .unwrap_or_default();
    remember_proxy_sources(&LAST_PROXY_SOURCES, read)
}

fn remember_proxy_sources(last: &Mutex<Vec<IpAddr>>, read: Vec<IpAddr>) -> Vec<IpAddr> {
    let mut last = last.lock().unwrap_or_else(PoisonError::into_inner);
    if read.is_empty() {
        return last.clone();
    }
    *last = read.clone();
    read
}

fn current() -> Arc<Vec<Net>> {
    LIST.read().unwrap_or_else(PoisonError::into_inner).clone()
}

fn install_set(list: &mut Arc<Vec<Net>>, nets: Vec<Net>) {
    LEN.store(nets.len(), Ordering::Release);
    *list = Arc::new(nets);
}

// Re-read env + `<dir>/node_whitelist.txt` and install the result, minus invalid and too-broad
// entries. Logs only when the set or the skipped entries change, so a 30s refresh loop stays
// quiet. Returns the number of networks installed.
pub(crate) fn reload(tag: &str, dir: &Path) -> usize {
    reload_with(tag, dir, &proxy_sources())
}

// reload, with the proxy sources given (tests pass their own instead of this host's routes)
fn reload_with(tag: &str, dir: &Path, proxy_sources: &[IpAddr]) -> usize {
    let path = dir.join(FILE_NAME);
    let env_val = std::env::var(ENV_KEY).ok();
    let file = std::fs::read_to_string(&path).ok();
    let (nets, invalid) = parse(env_val.as_deref(), file.as_deref());
    let mut skipped: Vec<String> = invalid
        .iter()
        .map(|tok| format!("invalid entry {tok:?} skipped"))
        .collect();
    let nets: Vec<Net> = nets
        .into_iter()
        .filter(|net| match too_broad(net, proxy_sources) {
            Some(why) => {
                skipped.push(format!(
                    "entry {net} REJECTED, it would serve the whole internet: {why}; list the node addresses or a narrower range"
                ));
                false
            }
            None => true,
        })
        .collect();
    let n = nets.len();
    {
        let mut last = SKIPPED.lock().unwrap_or_else(PoisonError::into_inner);
        if *last != skipped {
            for line in skipped.iter().filter(|l| !last.contains(l)) {
                crate::priority::gwlog!("[{tag}] node whitelist: {line}");
            }
            *last = skipped;
        }
    }
    let mut list = LIST.write().unwrap_or_else(PoisonError::into_inner);
    if **list != nets {
        let shown = nets
            .iter()
            .map(Net::to_string)
            .collect::<Vec<_>>()
            .join(",");
        crate::priority::gwlog!(
            "[{tag}] node whitelist: {n} network(s) served as nodes (env {ENV_KEY} + {}): {shown}",
            path.display()
        );
        install_set(&mut list, nets);
    }
    n
}

pub(crate) fn len() -> usize {
    LEN.load(Ordering::Acquire)
}

// Hot path: one atomic load in the empty case, otherwise an immutable snapshot behind a read lock.
pub(crate) fn contains(ip: IpAddr) -> bool {
    if len() == 0 {
        return false;
    }
    current().iter().any(|net| net.contains(ip))
}

// Tests share the process-wide list: every test that installs one holds this lock.
#[cfg(test)]
pub(crate) static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
pub(crate) fn install(entries: &[&str]) {
    let (nets, invalid) = parse(Some(&entries.join(",")), None);
    assert!(invalid.is_empty(), "bad test whitelist: {invalid:?}");
    install_set(&mut LIST.write().unwrap(), nets);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn matches(entries: &[&str], source: &str) -> bool {
        let (nets, invalid) = parse(Some(&entries.join(",")), None);
        assert!(invalid.is_empty(), "{invalid:?}");
        nets.iter().any(|net| net.contains(ip(source)))
    }

    #[test]
    fn exact_cidr_and_zero_prefix_matching() {
        assert!(matches(&["203.0.113.7"], "203.0.113.7"));
        assert!(!matches(&["203.0.113.7"], "203.0.113.8"));
        assert!(matches(&["192.168.1.0/24"], "192.168.1.255"));
        assert!(!matches(&["192.168.1.0/24"], "192.168.2.1"));
        assert!(matches(&["203.0.113.7/32"], "203.0.113.7"));
        assert!(!matches(&["203.0.113.7/32"], "203.0.113.8"));
        assert!(matches(&["203.0.113.7/0"], "198.51.100.9"));
        assert!(matches(&["2001:db8::7"], "2001:db8::7"));
        assert!(!matches(&["2001:db8::7"], "2001:db8::8"));
        assert!(matches(&["2001:db8:1::/64"], "2001:db8:1::ffff"));
        assert!(!matches(&["2001:db8:1::/64"], "2001:db8:2::1"));
        assert!(matches(&["2001:db8::/0"], "2001:db8:ffff::1"));
    }

    #[test]
    fn host_bits_are_masked_and_mapped_sources_use_ipv4() {
        let (nets, invalid) = parse(Some("192.168.1.5/24,2001:db8::1234/64"), None);
        assert!(invalid.is_empty());
        assert_eq!(nets[0].to_string(), "192.168.1.0/24");
        assert_eq!(nets[1].to_string(), "2001:db8::/64");
        assert!(matches(&["203.0.113.7"], "::ffff:203.0.113.7"));
        assert!(matches(&["203.0.113.0/24"], "::ffff:203.0.113.200"));
        assert!(!matches(&["2001:db8::/64"], "::ffff:203.0.113.7"));
    }

    #[test]
    fn address_families_do_not_cross_match() {
        assert!(!matches(&["203.0.113.7"], "2001:db8::7"));
        assert!(!matches(&["0.0.0.0/0"], "2001:db8::7"));
        assert!(!matches(&["2001:db8::7"], "203.0.113.7"));
        assert!(!matches(&["::/0"], "203.0.113.7"));
        assert!(!matches(&["::/0"], "::ffff:203.0.113.7"));
    }

    #[test]
    fn comments_blanks_and_duplicates_are_handled() {
        let (nets, invalid) = parse(
            Some(" 203.0.113.7, ,2001:db8::1,203.0.113.7"),
            Some("# nodes\n\n192.168.1.9/24 # masked\n203.0.113.7\n  # disabled\n"),
        );
        assert!(invalid.is_empty());
        assert_eq!(nets.len(), 3);
        assert!(nets.iter().any(|net| net.to_string() == "192.168.1.0/24"));
    }

    #[test]
    fn every_malformed_form_is_rejected() {
        let invalid = [
            "node.example",
            "203.0.113.7:4001",
            "[2001:db8::1]:4001",
            "203.0.113.7-203.0.113.9",
            "",
            " ",
            "203.0.113.7/",
            "203.0.113.7/-1",
            "203.0.113.7/+24",
            "203.0.113.7/33",
            "2001:db8::1/129",
            "203.0.113.7/24junk",
            "203.0.113.7/24/1",
            "203.0.113.7junk",
            "junk203.0.113.7",
            "999.1.1.1",
        ];
        for token in invalid {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                assert!(parse_entry(trimmed).is_none(), "accepted {token:?}");
            }
        }
        let (nets, invalid) = parse(Some(&invalid.join(",")), None);
        assert!(nets.is_empty());
        assert_eq!(invalid.len(), 14, "{invalid:?}");
        assert!(parse_entry("192.168.1.0/32").is_some());
        assert!(parse_entry("2001:db8::1/128").is_some());
    }

    #[test]
    fn reload_reads_the_file_in_the_given_dir() {
        let _g = TEST_LOCK.blocking_lock();
        let dir = std::env::temp_dir().join(format!("hypersync_nw_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(FILE_NAME),
            "# nodes\n203.0.113.20\n192.168.1.9/24\nbogus\n",
        )
        .unwrap();
        // env HYPERSYNC_NODE_WHITELIST is not set under `cargo test`; no proxy sources, so the
        // result doesn't depend on the test machine's own default gateway
        assert_eq!(reload_with("test", &dir, &[]), 2);
        assert_eq!(len(), 2);
        assert!(contains(ip("203.0.113.20")));
        assert!(contains(ip("192.168.1.200")));
        assert!(!contains(ip("192.168.2.1")));
        assert_eq!(reload_with("test", &dir, &[]), 2);
        std::fs::remove_file(dir.join(FILE_NAME)).unwrap();
        assert_eq!(reload_with("test", &dir, &[]), 0);
        assert!(!contains(ip("203.0.113.20")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn entries_that_would_serve_the_internet_are_too_broad() {
        let gw = [ip("172.28.0.1")];
        let broad = |e: &str| too_broad(&parse_entry(e).unwrap(), &gw).is_some();
        for e in [
            "0.0.0.0/0",
            "203.0.113.7/0",
            "::/0",
            "172.28.0.1",
            "172.28.0.0/24",
            "172.28.0.0/25",
            "172.16.0.0/12",
        ] {
            assert!(broad(e), "{e} accepted");
        }
        for e in [
            "172.28.0.128/25",
            "172.28.0.129",
            "172.28.0.2",
            "203.0.113.7",
            "2001:db8::/64",
        ] {
            assert!(!broad(e), "{e} rejected");
        }
        // without proxy sources only a /0 is too broad
        assert!(too_broad(&parse_entry("172.28.0.0/24").unwrap(), &[]).is_none());
        assert!(too_broad(&parse_entry("0.0.0.0/0").unwrap(), &[]).is_some());
    }

    #[test]
    #[cfg(target_endian = "little")]
    fn default_gateways_come_from_the_route_table() {
        // the gateway container's /proc/net/route in compose (gwnet 172.28.0.0/24)
        let table =
            "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
            eth0\t00000000\t01001CAC\t0003\t0\t0\t0\t00000000\t0\t0\t0\n\
            eth0\t00001CAC\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0\n";
        assert_eq!(default_gateways(table), vec![ip("172.28.0.1")]);
        assert!(default_gateways("").is_empty());
        assert!(default_gateways("Iface\tDestination\tGateway\n").is_empty());
    }

    #[test]
    fn a_failed_route_read_keeps_the_last_proxy_sources() {
        let last = Mutex::new(Vec::new());
        assert!(remember_proxy_sources(&last, Vec::new()).is_empty());
        let gw = vec![ip("172.28.0.1")];
        assert_eq!(remember_proxy_sources(&last, gw.clone()), gw);
        assert_eq!(remember_proxy_sources(&last, Vec::new()), gw);
        let other = vec![ip("10.0.0.1")];
        assert_eq!(remember_proxy_sources(&last, other.clone()), other);
    }

    #[test]
    fn reload_never_installs_a_too_broad_entry() {
        let _g = TEST_LOCK.blocking_lock();
        let dir = std::env::temp_dir().join(format!("hypersync_nw_broad_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let gw = [ip("172.28.0.1")];
        std::fs::write(
            dir.join(FILE_NAME),
            "172.28.0.0/24\n0.0.0.0/0\n172.28.0.129\n",
        )
        .unwrap();
        assert_eq!(reload_with("test", &dir, &gw), 1);
        assert!(contains(ip("172.28.0.129")));
        assert!(
            !contains(ip("172.28.0.1")),
            "the proxy source got node service"
        );
        assert!(!contains(ip("198.51.100.9")));
        // a list of only too-broad entries is empty, which the gateway refuses at startup
        std::fs::write(dir.join(FILE_NAME), "172.28.0.0/24\n").unwrap();
        assert_eq!(reload_with("test", &dir, &gw), 0);
        assert!(!contains(ip("172.28.0.1")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn install_supports_loopback_for_gateway_routing_tests() {
        let _g = TEST_LOCK.lock().await;
        install(&["127.0.0.1"]);
        assert!(contains(ip("127.0.0.1")));
        assert!(contains(ip("::ffff:127.0.0.1")));
        install(&[]);
        assert!(!contains(ip("127.0.0.1")));
    }
}
