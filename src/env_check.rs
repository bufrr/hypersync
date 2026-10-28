// Startup check of the gateway's tier and failover settings. Each reader elsewhere is lenient (a
// bad entry is dropped, a typo falls back to the default), which can quietly change behaviour: a
// mistyped HYPERSYNC_PRIORITY_PEERS leaves an empty tier and every node on the public pool with no
// error. Any invalid value stops the process here instead. Unset or blank means "use the default".
//
// Each value is parsed exactly as its reader parses it (same split, same trim, no extra unquoting),
// so a value accepted here is never read differently at runtime.

use std::collections::HashSet;
use std::net::Ipv4Addr;

use crate::priority::BootstrapPolicy;
use crate::util::split_csv;

const DAY_MS: u64 = 86_400_000;
const DAY_SECS: u64 = 86_400;

// (key, min, max): the minimum is the floor its reader applies (src/failover/mod.rs), so nothing
// accepted here is raised at runtime; the cap of a day keeps timestamp arithmetic from overflowing.
const FAILOVER_NUMBERS: [(&str, u64, u64); 5] = [
    ("HYPERSYNC_FAILOVER_STALL_MS", 50, DAY_MS),
    ("HYPERSYNC_FAILOVER_LAG_MS", 20, DAY_MS),
    ("HYPERSYNC_FAILOVER_DWELL_MS", 0, DAY_MS),
    ("HYPERSYNC_FAILOVER_ROTATE_MS", 0, DAY_MS),
    ("HYPERSYNC_FAILOVER_ROTATE_SECS", 10, DAY_SECS),
];

// Every problem in the environment, one message each; empty when all is valid. The priority peer
// list is checked in every mode (log redaction uses it everywhere); the tier and failover knobs
// only for the gateway, the one mode that reads them. Messages never contain a priority peer
// address (those are kept out of every log).
pub(crate) fn problems(get: impl Fn(&str) -> Option<String>, gateway: bool) -> Vec<String> {
    // unset and blank are the same: compose passes `${VAR:-}` through as ""
    let get = |k: &str| get(k).filter(|v| !v.trim().is_empty());
    let mut out = Vec::new();

    let mut peers = 0usize;
    if let Some(raw) = get("HYPERSYNC_PRIORITY_PEERS") {
        let mut seen = HashSet::new();
        for (i, item) in split_csv(&raw).iter().enumerate() {
            let n = i + 1;
            match item.parse::<Ipv4Addr>() {
                Err(_) => out.push(format!(
                    "HYPERSYNC_PRIORITY_PEERS entry #{n} is not an IPv4 address"
                )),
                Ok(ip) if !is_public(ip) => out.push(format!(
                    "HYPERSYNC_PRIORITY_PEERS entry #{n} is not a public IPv4 address"
                )),
                Ok(ip) if !seen.insert(ip) => out.push(format!(
                    "HYPERSYNC_PRIORITY_PEERS entry #{n} repeats an earlier one"
                )),
                Ok(_) => peers += 1,
            }
        }
    }
    if let Some(v) = get("HYPERSYNC_PRIORITY_PEERS_EXPECTED") {
        match v.trim().parse::<usize>() {
            Ok(want) if want != peers => out.push(format!(
                "HYPERSYNC_PRIORITY_PEERS has {peers} valid address(es), HYPERSYNC_PRIORITY_PEERS_EXPECTED is {want}"
            )),
            Ok(_) => {}
            Err(_) => out.push(not_a("HYPERSYNC_PRIORITY_PEERS_EXPECTED", &v, "whole number")),
        }
    }
    if !gateway {
        return out;
    }

    if let Some(v) = get("HYPERSYNC_PRIORITY_EXCLUSIVE") {
        if parse_bool(&v).is_none() {
            out.push(not_a(
                "HYPERSYNC_PRIORITY_EXCLUSIVE",
                &v,
                "1|0|true|false|yes|no|on|off",
            ));
        }
    }
    if let Some(v) = get("HYPERSYNC_PRIORITY_BOOTSTRAP") {
        if BootstrapPolicy::parse(&v).is_none() {
            out.push(not_a(
                "HYPERSYNC_PRIORITY_BOOTSTRAP",
                &v,
                "fallback|never|prefer",
            ));
        }
    }
    // read as a usize
    check_number(
        &get,
        "HYPERSYNC_PRIORITY_MAX_SESSIONS",
        0,
        usize::MAX as u64,
        &mut out,
    );
    check_number(&get, "HYPERSYNC_PRIORITY_STAGE_MS", 200, 3000, &mut out);
    check_number(&get, "HYPERSYNC_FAILOVER_STANDBYS", 0, 16, &mut out);
    for (k, min, max) in FAILOVER_NUMBERS {
        check_number(&get, k, min, max, &mut out);
    }
    out
}

// The readers parse every number as `v.trim().parse::<u64>()`.
fn check_number(
    get: &impl Fn(&str) -> Option<String>,
    key: &str,
    min: u64,
    max: u64,
    out: &mut Vec<String>,
) {
    let Some(v) = get(key) else { return };
    match v.trim().parse::<u64>() {
        Ok(n) if (min..=max).contains(&n) => {}
        _ => out.push(not_a(key, &v, &format!("whole number in {min}..={max}"))),
    }
}

fn not_a(key: &str, value: &str, want: &str) -> String {
    format!("{key}={value:?} is not a {want}")
}

// As the HYPERSYNC_PRIORITY_EXCLUSIVE reader takes it: trimmed, outer quotes dropped, any case.
fn parse_bool(v: &str) -> Option<bool> {
    match v
        .trim()
        .trim_matches(|c| c == '"' || c == '\'')
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

// A routable unicast address: not private, loopback, link-local, shared (CGNAT), documentation,
// benchmarking, 6to4 relay anycast, IETF protocol assignments, multicast, reserved, broadcast or
// "this network".
fn is_public(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_documentation()
        || ip.is_multicast()
        || ip.is_broadcast()
        || a == 0
        || a >= 240
        || (a == 100 && (64..128).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 88 && c == 99)
        || (a == 198 && (18..20).contains(&b)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn an_unset_or_empty_environment_is_valid() {
        assert!(problems(env(&[]), true).is_empty());
        // what compose passes through for unset variables
        assert!(problems(
            env(&[
                ("HYPERSYNC_PRIORITY_PEERS", ""),
                ("HYPERSYNC_PRIORITY_BOOTSTRAP", " "),
                ("HYPERSYNC_FAILOVER_STATUS_DIR", ""),
            ]),
            true
        )
        .is_empty());
    }

    #[test]
    fn the_production_shape_is_valid() {
        assert!(problems(
            env(&[
                ("HYPERSYNC_PRIORITY_PEERS", "1.2.3.4, 5.6.7.8,9.10.11.12"),
                ("HYPERSYNC_PRIORITY_PEERS_EXPECTED", "3"),
                ("HYPERSYNC_PRIORITY_EXCLUSIVE", "1"),
                ("HYPERSYNC_PRIORITY_BOOTSTRAP", "prefer"),
                ("HYPERSYNC_PRIORITY_MAX_SESSIONS", "2"),
                ("HYPERSYNC_PRIORITY_STAGE_MS", "1500"),
                ("HYPERSYNC_FAILOVER_STANDBYS", "0"),
            ]),
            true
        )
        .is_empty());
    }

    #[test]
    fn bad_priority_peers_are_reported_without_their_addresses() {
        let p = problems(
            env(&[(
                "HYPERSYNC_PRIORITY_PEERS",
                "1.2.3.4,1.2.3.400,10.0.0.1,,1.2.3.4",
            )]),
            false,
        );
        assert_eq!(p.len(), 3, "{p:?}");
        assert!(p[0].contains("#2") && p[0].contains("not an IPv4"));
        assert!(p[1].contains("#3") && p[1].contains("not a public"));
        // empty entries are skipped by the reader too, so the repeat is the 4th listed entry
        assert!(p[2].contains("#4") && p[2].contains("repeats"));
        for line in &p {
            assert!(
                !line.contains("1.2.3") && !line.contains("10.0.0.1"),
                "{line}"
            );
        }
    }

    // A list accepted here must read back, at runtime, as exactly that many peers.
    #[test]
    fn an_accepted_peer_list_reads_back_unchanged() {
        for raw in [
            "1.2.3.4",
            " 1.2.3.4 , 5.6.7.8 ",
            "1.2.3.4,,5.6.7.8,9.10.11.12",
            "\"1.2.3.4\"",
            "\"1.2.3.4,5.6.7.8,9.10.11.12\"",
            "'1.2.3.4'",
            "1.2.3.4;5.6.7.8",
        ] {
            let read = crate::priority::priority_peers_from_raw(raw);
            let n = read.len();
            let ok = problems(
                env(&[
                    ("HYPERSYNC_PRIORITY_PEERS", raw),
                    ("HYPERSYNC_PRIORITY_PEERS_EXPECTED", &n.to_string()),
                ]),
                false,
            )
            .is_empty();
            let total = split_csv(raw).len();
            assert_eq!(
                ok,
                n == total,
                "{raw:?}: accepted={ok} but reads back {n} of {total}"
            );
        }
        // quoted lists are rejected: the reader would drop the quoted entries
        assert!(!problems(
            env(&[("HYPERSYNC_PRIORITY_PEERS", "\"1.2.3.4,5.6.7.8,9.10.11.12\"")]),
            false
        )
        .is_empty());
    }

    #[test]
    fn the_expected_count_catches_a_missing_or_short_list() {
        let missing = problems(env(&[("HYPERSYNC_PRIORITY_PEERS_EXPECTED", "3")]), false);
        assert_eq!(missing.len(), 1);
        assert!(missing[0].contains("has 0 valid"), "{missing:?}");
        let short = problems(
            env(&[
                ("HYPERSYNC_PRIORITY_PEERS", "1.2.3.4,5.6.7.8"),
                ("HYPERSYNC_PRIORITY_PEERS_EXPECTED", "3"),
            ]),
            false,
        );
        assert!(short[0].contains("has 2 valid"), "{short:?}");
        assert!(!problems(
            env(&[("HYPERSYNC_PRIORITY_PEERS_EXPECTED", "three")]),
            false
        )
        .is_empty());
    }

    #[test]
    fn typos_in_flags_and_numbers_are_reported() {
        for (k, v) in [
            ("HYPERSYNC_PRIORITY_EXCLUSIVE", "flase"),
            ("HYPERSYNC_PRIORITY_BOOTSTRAP", "prefre"),
            ("HYPERSYNC_PRIORITY_BOOTSTRAP", "'never'"),
            ("HYPERSYNC_PRIORITY_MAX_SESSIONS", "two"),
            ("HYPERSYNC_PRIORITY_MAX_SESSIONS", "\"2\""),
            ("HYPERSYNC_PRIORITY_STAGE_MS", "50"),
            ("HYPERSYNC_PRIORITY_STAGE_MS", "1500ms"),
            ("HYPERSYNC_FAILOVER_STANDBYS", "17"),
            ("HYPERSYNC_FAILOVER_STANDBYS", "'2'"),
            ("HYPERSYNC_FAILOVER_LAG_MS", "-1"),
            // below the floor the reader would silently raise it to
            ("HYPERSYNC_FAILOVER_STALL_MS", "0"),
            ("HYPERSYNC_FAILOVER_LAG_MS", "19"),
            ("HYPERSYNC_FAILOVER_ROTATE_SECS", "9"),
            // past a day: would overflow timestamp arithmetic
            ("HYPERSYNC_FAILOVER_ROTATE_MS", "18446744073709551615"),
            ("HYPERSYNC_FAILOVER_DWELL_MS", "86400001"),
        ] {
            let p = problems(env(&[(k, v)]), true);
            assert_eq!(p.len(), 1, "{k}={v}: {p:?}");
            assert!(p[0].starts_with(k), "{p:?}");
        }
        // the readers' floors and the defaults are accepted
        assert!(problems(
            env(&[
                ("HYPERSYNC_FAILOVER_STALL_MS", "50"),
                ("HYPERSYNC_FAILOVER_LAG_MS", "150"),
                ("HYPERSYNC_FAILOVER_DWELL_MS", "10000"),
                ("HYPERSYNC_FAILOVER_ROTATE_MS", "0"),
                ("HYPERSYNC_FAILOVER_ROTATE_SECS", "10"),
            ]),
            true
        )
        .is_empty());
        // accepted the way the readers accept them: a quoted/cased flag, a cased bootstrap
        assert!(problems(
            env(&[
                ("HYPERSYNC_PRIORITY_EXCLUSIVE", "\"Off\""),
                ("HYPERSYNC_PRIORITY_BOOTSTRAP", " Fallback "),
            ]),
            true
        )
        .is_empty());
    }

    #[test]
    fn modes_other_than_the_gateway_check_only_the_peer_list() {
        let bad_knobs = [
            ("HYPERSYNC_PRIORITY_STAGE_MS", "50"),
            ("HYPERSYNC_FAILOVER_STANDBYS", "lots"),
        ];
        assert!(problems(env(&bad_knobs), false).is_empty());
        assert_eq!(problems(env(&bad_knobs), true).len(), 2);
        assert!(!problems(env(&[("HYPERSYNC_PRIORITY_PEERS", "10.0.0.1")]), false).is_empty());
    }

    #[test]
    fn public_means_routable_unicast() {
        for ip in [
            "1.1.1.1",
            "8.8.8.8",
            "100.63.0.1",
            "100.128.0.1",
            "198.20.0.1",
        ] {
            assert!(is_public(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "0.0.0.0",
            "10.1.2.3",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.1.1",
            "172.16.0.1",
            "192.0.0.8",
            "192.0.2.1",
            "192.88.99.2",
            "192.168.1.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(!is_public(ip.parse().unwrap()), "{ip}");
        }
    }
}
