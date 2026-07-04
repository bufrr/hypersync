// HL push gateway — multi-upstream MERGE with round-based dedup (= "most complete" live stream).
//
// Each unique consensus round is forwarded to the local node exactly once, taken from whichever
// upstream supplies it. A round missed by one peer is filled from another => gap-free / most complete.
// Live block (decompressed) carries the round at offset 0x5e = [0xfc][u32 LE].
// frame = [u32 BE L][1 type byte][L payload]; type=1 data; payload = lz4_flex(prepend_size).
//
// Upstreams may be "ip" (=> :4001) or "ip:port" (for local mock peers / custom ports).
// Subcommand `mock <bind:port> <dir> <start> <end>` replays captured blocks[start..end] for testing.

mod gateway;
mod peerd;
mod protocol;
mod push;
mod testing;

use std::env;
use tokio::net::TcpListener;

use crate::gateway::run_gateway;
use crate::peerd::{run_peerd, split_csv};
use crate::push::{run_proxy, run_relay, serve};
use crate::testing::{run_bench, run_cache, run_fakenode, run_mock};

#[tokio::main(flavor = "multi_thread")]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() {
    let args: Vec<String> = env::args().collect();
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
        // Shadow multi-source injection is disabled until block/proposer validation is added.
        let push = args.iter().any(|a| a == "--push");
        let upstreams: Vec<String> = args
            .iter()
            .skip(2)
            .find(|a| a.as_str() != "--push")
            .map(|s| split_csv(s))
            .unwrap_or_default();
        run_proxy(upstreams, push).await;
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
        let node_peer_file = args.get(2).cloned().unwrap_or_else(|| "/nodepeers".into());
        // --push: use serve_push's active transparent backbone and active failover. Shadow
        //   multi-source injection is currently disabled, so --live N is only a future capacity knob.
        // --cache: additionally capture the bootstrap and replay it on a node cold-start, avoiding the
        //   per-IP abci_state rate-limit on node restart. Trade-off: a cache-cold-started node has no
        //   peer relationship, so its client-block RPC (4002) must be fetched-and-forwarded rather
        //   than spliced. Default off = robust transparent bootstrap (node keeps a real peer).
        let push = args.iter().any(|a| a == "--push");
        let cache_coldstart = args.iter().any(|a| a == "--cache");
        let n_live = args
            .iter()
            .position(|a| a == "--live")
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        run_gateway(node_peer_file, push, cache_coldstart, n_live).await;
        return;
    }
    // default `<port> <peers>` mode: multi-upstream live-block merge. Testing/aux only — `pump`
    // drops all type-0 (control) frames, so a real HL node would never receive the peer greeting
    // through it; it pairs with `mock` (which sends no greeting frame). Use `gateway` for nodes.
    let port: u16 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(4001);
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
