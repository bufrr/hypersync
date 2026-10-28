// HL push gateway — multi-upstream MERGE with round-based dedup (= "most complete" live stream).
//
// Each unique consensus round is forwarded to the local node exactly once, taken from whichever
// upstream supplies it. A round missed by one peer is filled from another => gap-free / most complete.
// Live block (decompressed) carries its round as the first [0xfc R][0xfc R-1][0xfc R-2] varint
// triple in its header (protocol::block_round).
// frame = [u32 BE L][1 type byte][L payload]; type=1 data; payload = lz4_flex(prepend_size).
//
// Upstreams may be "ip" (=> :4001) or "ip:port" (for local mock peers / custom ports).
// Subcommand `mock <bind:port> <dir> <start> <end>` replays captured blocks[start..end] for testing.

mod blacklist;
mod env_check;
mod failover;
mod gateway;
mod peerd;
mod priority;
mod protocol;
mod push;
mod testing;
mod util;

use std::env;
use tokio::net::TcpListener;

use crate::gateway::run_gateway;
use crate::peerd::run_peerd;
use crate::push::{run_proxy, run_relay, serve};
use crate::testing::{run_bench, run_cache, run_fakenode, run_mock};
use crate::util::split_csv;

// glibc raises its mmap threshold (up to 32MB) each time a large mmapped chunk is freed, after
// which multi-MB buffers (4002 client-block responses, up to ~57MB) are carved from per-thread
// arenas instead and stay resident after free. That grew the gateway ~100-300MB per node catch-up
// (243MB -> 849MB over 6 node restarts, 15 near-full 64MB arenas). Pinning the threshold keeps
// every buffer >= 1MiB in its own mapping, returned to the OS on free. Live blocks (~100KB) are
// unaffected. An explicit MALLOC_MMAP_THRESHOLD_ in the environment wins.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn pin_malloc_mmap_threshold() -> bool {
    extern "C" {
        fn mallopt(param: i32, value: i32) -> i32;
    }
    const M_MMAP_THRESHOLD: i32 = -3;
    if env::var_os("MALLOC_MMAP_THRESHOLD_").is_some() {
        return true;
    }
    // SAFETY: mallopt only adjusts allocator tuning; any int is a valid argument.
    unsafe { mallopt(M_MMAP_THRESHOLD, 1 << 20) == 1 }
}

#[tokio::main(flavor = "multi_thread")]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    if !pin_malloc_mmap_threshold() {
        eprintln!("[main] WARN mallopt(M_MMAP_THRESHOLD) failed; 4002 buffers may stay resident");
    }
    let args: Vec<String> = env::args().collect();
    // Invalid settings stop the process before it starts (see env_check): the priority peer list
    // in every mode, the tier and failover knobs in the gateway that reads them.
    let gateway = args.get(1).map(String::as_str) == Some("gateway");
    let problems = env_check::problems(|k| env::var(k).ok(), gateway);
    if !problems.is_empty() {
        for p in &problems {
            eprintln!("[main] invalid environment: {p}");
        }
        std::process::exit(2);
    }
    // Priority (Tier P) IPs are redacted to prio-N in every log line of every mode (a sentry IP
    // can also be given to proxy/relay as a plain upstream).
    priority::init_redaction(&priority::priority_peers_from_env());
    // Upstream IP blacklist for the modes that dial out but never reload it (proxy/relay/cache and
    // the merge mode); gateway and peerd read it from their own data dir on every refresh.
    if !matches!(args.get(1).map(String::as_str), Some("gateway" | "peerd")) {
        blacklist::reload(
            "main",
            std::path::Path::new(&env::var("HYPERSYNC_DATA").unwrap_or_else(|_| "./data".into())),
        );
    }
    if args.get(1).map(|s| s.as_str()) == Some("bench") {
        let dir = args.get(2).cloned().unwrap_or_else(|| ".".into());
        let iters: usize = args
            .get(3)
            .and_then(|s| s.parse().ok())
            .unwrap_or(1_000_000);
        run_bench(&dir, iters);
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("mock") {
        let bind = args
            .get(2)
            .cloned()
            .unwrap_or_else(|| "127.0.0.1:6001".into());
        let dir = args.get(3).cloned().unwrap_or_else(|| ".".into());
        let start: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
        let end: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(12);
        run_mock(bind, dir, start, end).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("fakenode") {
        // testing-only: a synthetic downstream node that exercises a gateway's three node-facing
        // paths (bootstrap replay/relay, live stream, 4002 client-block RPC) and prints one
        // machine-checkable summary line. Run one container per fakenode so the gateway sees
        // distinct source IPs (sessions are keyed by source IP).
        std::process::exit(run_fakenode(&args[2..]).await);
    }
    if args.get(1).map(|s| s.as_str()) == Some("relay") {
        // `relay <upstream>` — always relays the whole 4000-4010 range (a node's bootstrap needs
        // 4002 alongside 4001). Accepts the legacy `relay <port> <upstream>` form too; the port
        // arg was always ignored, so it's no longer documented.
        let upstream = args
            .get(3)
            .cloned()
            .or_else(|| args.get(2).cloned())
            .unwrap_or_else(|| "172.18.0.2:4001".into());
        run_relay(upstream).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("proxy") {
        // default: transparent proxy + failover (NO block push).
        // optional `--push`: route live blocks through serve_push for active-stream monitoring.
        // Only the active upstream ever feeds the node (no multi-source block merging).
        let push = args.iter().any(|a| a == "--push");
        let upstreams: Vec<String> = args
            .iter()
            .skip(2)
            .find(|a| !a.starts_with("--"))
            .map(|s| split_csv(s))
            .unwrap_or_default();
        run_proxy(blacklist::filtered(upstreams), push).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("cache") {
        // testing/aux only: binds a single port, but a real node also needs 4002 (gossip RPC)
        // to bootstrap — use `gateway --cache` for a full node-facing cache.
        let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4001);
        let upstream = args
            .get(3)
            .cloned()
            .unwrap_or_else(|| "172.18.0.2:4001".into());
        run_cache(port, upstream).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("peerd") {
        // peer discovery + probing daemon. Env config:
        // HYPERSYNC_DATA (data dir, default ./data), HL_STATS_DIR (optional, comma-separated:
        // each node's tcp_lz4_stats dir — mount hl-data volumes read-only; richest candidate
        // source when co-located), HL_SELF_IP (optional, comma-separated: ALL our own nodes'
        // public IPs, excluded from candidates).
        let interval: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(300);
        run_peerd(interval).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("gateway") {
        // full P2P gateway: reads the node's OWN peer file (path arg) for its upstream pool, then
        // relays bootstrap/live/RPC through an active peer, with optional bootstrap cache.
        // --push: use serve_push's round-gated active stream; an active-stream stall/death tears
        //   the session down so the node reconnects and a fresh active is pinned.
        // --cache: additionally capture the bootstrap and replay it on a node cold-start, avoiding the
        //   per-IP abci_state rate-limit on node restart. Trade-off: a cache-cold-started node has no
        //   peer relationship, so its client-block RPC (4002) must be fetched-and-forwarded rather
        //   than spliced. Default off = robust transparent bootstrap (node keeps a real peer).
        let g = match parse_gateway_args(&args[2..]) {
            Ok(g) => g,
            Err(e) => {
                eprintln!("gateway: {e}\nusage: hypersync gateway <peers.json> [--push] [--cache]");
                std::process::exit(2);
            }
        };
        if g.live_ignored {
            eprintln!("[gw] --live is ignored: multi-source push was removed");
        }
        run_gateway(g.peer_file, g.push, g.cache).await;
        return;
    }
    // default `<port> <peers>` mode: multi-upstream live-block merge. Testing/aux only — `pump`
    // drops all type-0 (control) frames, so a real HL node would never receive the peer greeting
    // through it; it pairs with `mock` (which sends no greeting frame). Use `gateway` for nodes.
    // Anything else must be a port (or nothing): an unknown subcommand or a typo would otherwise
    // silently start this testing-only mode on :4001 against hard-coded upstreams.
    let port: u16 = match args.get(1) {
        None => {
            eprintln!(
                "usage: hypersync <gateway|peerd|proxy|relay|cache|mock|fakenode|bench> ... | hypersync <port> [peers]"
            );
            std::process::exit(2);
        }
        Some(a) => match a.parse() {
            Ok(p) => p,
            Err(_) => {
                eprintln!(
                    "unknown mode {a:?}; usage: hypersync <gateway|peerd|proxy|relay|cache|mock|fakenode|bench> ... | hypersync [port] [peers]"
                );
                std::process::exit(2);
            }
        },
    };
    let upstreams: Vec<String> = match args.get(2) {
        Some(s) => split_csv(s),
        None => vec!["74.63.207.101".into(), "64.140.170.202".into()],
    };
    let l = TcpListener::bind(("0.0.0.0", port)).await.expect("bind");
    eprintln!("[gw] round-merge :{} | {} upstreams", port, upstreams.len());
    loop {
        let (down, addr) = match l.accept().await {
            Ok(x) => x,
            Err(e) => {
                eprintln!("[gw] accept {e}");
                continue;
            }
        };
        let ups = upstreams.clone();
        tokio::spawn(async move {
            eprintln!("[gw] downstream {addr}");
            if let Err(e) = serve(down, ups).await {
                eprintln!("[gw] {addr}: {e}");
            }
            eprintln!("[gw] {addr} done");
        });
    }
}

#[derive(Debug, PartialEq)]
struct GatewayArgs {
    peer_file: String,
    push: bool,
    cache: bool,
    // `--live N` was given: accepted (old compose files pass it) but no longer does anything
    live_ignored: bool,
}

// `gateway [<peers.json>] [--push] [--cache] [--live N]`, flags in any order. The peer file
// defaults to /nodepeers. Anything else — an unknown flag, a second path, `--live` without a
// number — is an error rather than silently starting with the wrong config.
fn parse_gateway_args(args: &[String]) -> Result<GatewayArgs, String> {
    let mut g = GatewayArgs {
        peer_file: String::new(),
        push: false,
        cache: false,
        live_ignored: false,
    };
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--push" => g.push = true,
            "--cache" => g.cache = true,
            "--live" => match it.next().and_then(|v| v.parse::<u32>().ok()) {
                Some(_) => g.live_ignored = true,
                None => return Err("--live needs a number".into()),
            },
            f if f.starts_with("--") => return Err(format!("unknown flag {f}")),
            path if g.peer_file.is_empty() => g.peer_file = path.to_string(),
            extra => return Err(format!("unexpected argument {extra}")),
        }
    }
    if g.peer_file.is_empty() {
        g.peer_file = "/nodepeers".into();
    }
    Ok(g)
}

#[cfg(test)]
mod tests {
    #[test]
    fn gateway_args_are_strict() {
        let p = |s: &[&str]| {
            super::parse_gateway_args(&s.iter().map(|x| x.to_string()).collect::<Vec<_>>())
        };
        let ok = |s: &[&str]| p(s).unwrap();
        // the compose command (with the now-ignored --live) and flags in any order
        let g = ok(&["/pd/peers.json", "--push", "--live", "5"]);
        assert_eq!(
            (g.peer_file.as_str(), g.push, g.cache, g.live_ignored),
            ("/pd/peers.json", true, false, true)
        );
        assert_eq!(ok(&["--push", "--cache", "/x"]).peer_file, "/x");
        assert_eq!(ok(&["--push"]).peer_file, "/nodepeers");
        // what used to start silently with the wrong peer file / config
        assert!(p(&["--live", "/pd/peers.json"]).is_err());
        assert!(p(&["--push", "--live"]).is_err());
        assert!(p(&["/pd/peers.json", "--psuh"]).is_err());
        assert!(p(&["/pd/peers.json", "/other.json"]).is_err());
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn malloc_mmap_threshold_is_accepted() {
        assert!(super::pin_malloc_mmap_threshold());
    }
}
