// HL push gateway — multi-upstream MERGE with round-based dedup (= "most complete" live stream).
//
// Each unique consensus round is forwarded to the local node exactly once, taken from whichever
// upstream supplies it. A round missed by one peer is filled from another => gap-free / most complete.
// Live block (decompressed) carries the round at offset 0x5e = [0xfc][u32 LE].
// frame = [u32 BE L][1 type byte][L payload]; type=1 data; payload = lz4_flex(prepend_size).
//
// Upstreams may be "ip" (=> :4001) or "ip:port" (for local mock peers / custom ports).
// Subcommand `mock <bind:port> <dir> <start> <end>` replays captured blocks[start..end] for testing.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

const GREET_FALSE: [u8; 8] = [0, 0, 0, 3, 0, 0, 0, 0]; // send_abci:false (live blocks; no rate-limited state)
const GREET_TRUE: [u8; 8] = [0, 0, 0, 3, 0, 1, 0, 0]; // send_abci:true (full bootstrap stream)

// Reference / correctness oracle + fallback: full lz4 decompress, read round @0x5e.
// Round parsing assumes the `0xfc + u32 LE` varint form. Mainnet rounds (~1.35B, +~14.5/s) stay
// under u32::MAX for roughly 6 more years; past that the wire varint becomes `0xfd + u64`, these
// parsers return None, and dedup gracefully degrades to forward-everything (the node de-dups).
fn block_round_full(payload: &[u8]) -> Option<u32> {
    let dec = lz4_flex::block::decompress_size_prepended(payload).ok()?;
    if dec.len() >= 0x63 && dec[0x5e] == 0xfc {
        Some(u32::from_le_bytes([
            dec[0x5f], dec[0x60], dec[0x61], dec[0x62],
        ]))
    } else {
        None
    }
}

// Bounded LZ4 block decode: produce only the first `out.len()` bytes (round lives at 0x5e, so we
// never decompress the full ~200KB block). Standard LZ4 block format; returns bytes produced, or
// None if it can't safely reach the requested length (caller falls back to full decompress).
fn lz4_first_n(input: &[u8], out: &mut [u8]) -> Option<usize> {
    let n = out.len();
    let ilen = input.len();
    let mut ip = 0usize;
    let mut op = 0usize;
    while ip < ilen {
        let token = input[ip];
        ip += 1;
        let mut lit = (token >> 4) as usize;
        if lit == 15 {
            loop {
                if ip >= ilen {
                    return None;
                }
                let b = input[ip];
                ip += 1;
                lit += b as usize;
                if b != 255 {
                    break;
                }
            }
        }
        if lit > 0 {
            if ip + lit > ilen {
                return None;
            }
            let take = lit.min(n - op);
            out[op..op + take].copy_from_slice(&input[ip..ip + take]);
            op += take;
            ip += lit;
            if op >= n {
                return Some(op);
            }
        }
        if ip >= ilen {
            return Some(op);
        } // last sequence is literals-only
        if ip + 2 > ilen {
            return None;
        }
        let offset = (input[ip] as usize) | ((input[ip + 1] as usize) << 8);
        ip += 2;
        if offset == 0 || offset > op {
            return None;
        }
        let mut mlen = (token & 0x0f) as usize;
        if mlen == 15 {
            loop {
                if ip >= ilen {
                    return None;
                }
                let b = input[ip];
                ip += 1;
                mlen += b as usize;
                if b != 255 {
                    break;
                }
            }
        }
        mlen += 4;
        let take = mlen.min(n - op);
        for _ in 0..take {
            out[op] = out[op - offset];
            op += 1;
        }
        if op >= n {
            return Some(op);
        }
    }
    Some(op)
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn block_round(payload: &[u8]) -> Option<u32> {
    if payload.len() < 5 {
        return block_round_full(payload);
    }
    let lz = &payload[4..]; // skip the u32-LE uncompressed-size prefix
                            // fast path: the first 0x63 decompressed bytes of an lz4 block are always raw literals (nothing
                            // to back-reference at the start). If the first token's literal run covers them, read the round
                            // straight from the literal bytes — no decode buffer, no copy.
    let token = lz[0];
    let mut lit = (token >> 4) as usize;
    let mut p = 1usize;
    if lit == 15 {
        while p < lz.len() {
            let b = lz[p];
            p += 1;
            lit += b as usize;
            if b != 255 {
                break;
            }
        }
    }
    if lit >= 0x63 && p + 0x63 <= lz.len() {
        // SAFETY: p + 0x63 <= lz.len() is checked on the line above.
        unsafe {
            return if *lz.get_unchecked(p + 0x5e) == 0xfc {
                Some(u32::from_le_bytes([
                    *lz.get_unchecked(p + 0x5f),
                    *lz.get_unchecked(p + 0x60),
                    *lz.get_unchecked(p + 0x61),
                    *lz.get_unchecked(p + 0x62),
                ]))
            } else {
                None
            };
        }
    }
    let mut buf = [0u8; 0x63];
    match lz4_first_n(lz, &mut buf) {
        Some(n) if n >= 0x63 => {
            if buf[0x5e] == 0xfc {
                Some(u32::from_le_bytes([
                    buf[0x5f], buf[0x60], buf[0x61], buf[0x62],
                ]))
            } else {
                None
            }
        }
        _ => block_round_full(payload),
    }
}

struct RoundDedup {
    // slot[r % cap] = most recent round that mapped to that slot. Lock-free: an atomic swap is O(1)
    // with no mutex. Consensus rounds are ~sequential, so this is a hash-free sliding window of the
    // last ~cap rounds. A rare race (two threads swap the same r) only re-forwards one block, which
    // the node de-dups anyway ("received old client block"), so it's harmless.
    slots: Vec<AtomicU32>,
    mask: usize,
}
impl RoundDedup {
    fn new(cap: usize) -> Self {
        let cap = cap.next_power_of_two(); // power-of-two so `% cap` becomes a single-cycle `& mask`
        Self {
            slots: (0..cap).map(|_| AtomicU32::new(0)).collect(),
            mask: cap - 1,
        }
    }
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn is_new(&self, r: u32) -> bool {
        self.slots[(r as usize) & self.mask].swap(r, Ordering::Relaxed) != r
    }
}

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

async fn serve(mut down: TcpStream, upstreams: Vec<String>) -> std::io::Result<()> {
    down.set_nodelay(true).ok();
    let mut g = [0u8; 8];
    timeout(Duration::from_secs(20), down.read_exact(&mut g)).await??;
    eprintln!("[gw] node greeting {:02x?}", g);

    let dedup = Arc::new(RoundDedup::new(16_384));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(4096);

    for ip in &upstreams {
        let ip = ip.clone();
        let tx = tx.clone();
        let dedup = dedup.clone();
        tokio::spawn(async move {
            match connect_greet(&ip, GREET_FALSE).await {
                Ok(s) => {
                    let _ = pump(s, ip, tx, dedup).await;
                }
                Err(e) => eprintln!("[gw] upstream connect: {e}"),
            }
        });
    }
    drop(tx);

    let mut bytes = 0u64;
    let mut nframes = 0u64;
    while let Some(buf) = rx.recv().await {
        bytes += buf.len() as u64;
        nframes += 1;
        if down.write_all(&buf).await.is_err() {
            break;
        }
        if nframes.is_multiple_of(5) {
            eprintln!("[gw] frames forwarded={}", nframes);
        }
    }
    eprintln!("[gw] end: frames={} bytes={}", nframes, bytes);
    Ok(())
}

async fn connect_greet(addr: &str, g: [u8; 8]) -> std::io::Result<TcpStream> {
    let target = if addr.contains(':') {
        addr.to_string()
    } else {
        format!("{addr}:4001")
    };
    let mut s = timeout(Duration::from_secs(5), TcpStream::connect(&target)).await??;
    s.set_nodelay(true).ok();
    s.write_all(&g).await?;
    Ok(s)
}

async fn pump(
    mut s: TcpStream,
    ip: String,
    tx: mpsc::Sender<Vec<u8>>,
    dedup: Arc<RoundDedup>,
) -> std::io::Result<()> {
    let mut first = true;
    loop {
        let mut hdr = [0u8; 5];
        s.read_exact(&mut hdr).await?;
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        if len > 50_000_000 {
            let mut rem = len;
            let mut buf = vec![0u8; 65536];
            while rem > 0 {
                let n = s.read(&mut buf[..rem.min(65536)]).await?;
                if n == 0 {
                    return Ok(());
                }
                rem -= n;
            }
            continue;
        }
        let mut payload = vec![0u8; len];
        s.read_exact(&mut payload).await?;
        if typ != 1 {
            continue;
        }
        match block_round(&payload) {
            Some(r) => {
                if first {
                    eprintln!("[gw] upstream {ip}: first round = {r}");
                    first = false;
                }
                if dedup.is_new(r) {
                    let mut frame = hdr.to_vec();
                    frame.extend_from_slice(&payload);
                    if tx.send(frame).await.is_err() {
                        return Ok(());
                    }
                }
            }
            None => {
                let mut frame = hdr.to_vec();
                frame.extend_from_slice(&payload);
                if tx.send(frame).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
}

// Caching gateway (always-on store-and-forward):
//  - state feeder (send_abci:true via docker-proxy): fetches the full abci_state ONCE and caches it.
//  - live feeder (send_abci:false): permanently streams live blocks, keyed/deduped/ordered by round.
// A connecting node gets the cached state at local speed (beats its deadline), then every buffered
// live block in round order, then ongoing ones => contiguous + most-complete, so the node can
// certify the snapshot, finalize the bootstrap, and keep syncing entirely through the gateway.
type Live = Arc<Mutex<BTreeMap<u32, Arc<Vec<u8>>>>>;

fn insert_live(live: &Live, round: u32, frame: Arc<Vec<u8>>, cap: usize) -> bool {
    let mut m = live.lock().unwrap();
    if m.contains_key(&round) {
        return false;
    }
    m.insert(round, frame);
    while m.len() > cap {
        let k = *m.keys().next().unwrap();
        m.remove(&k);
    }
    true
}

async fn run_cache(port: u16, upstream: String) {
    let state: Arc<Mutex<Option<Arc<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let live: Live = Arc::new(Mutex::new(BTreeMap::new()));
    let (btx, _) = tokio::sync::broadcast::channel::<Arc<Vec<u8>>>(16384);
    let cap = 50_000usize;

    {
        let state = state.clone();
        let live = live.clone();
        let btx = btx.clone();
        let upstream = upstream.clone();
        tokio::spawn(async move {
            loop {
                if state.lock().unwrap().is_some() {
                    break; // cache the rate-limited state exactly once, then stop hammering
                }
                if let Err(e) = feed_state(&upstream, &state, &live, &btx, cap).await {
                    eprintln!("[cache] state feeder: {e} (retry)");
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            eprintln!("[cache] abci_state cached; state feeder stopped");
        });
    }
    {
        let live = live.clone();
        let btx = btx.clone();
        let upstream = upstream.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = feed_live(&upstream, &live, &btx, cap).await {
                    eprintln!("[cache] live feeder: {e} (reconnect)");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    let l = TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("cache bind");
    eprintln!(
        "[cache] :{} <- upstream {} (state cached once + continuous round-merged live)",
        port, upstream
    );
    loop {
        let (mut down, addr) = match l.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let state = state.clone();
        let live = live.clone();
        let mut brx = btx.subscribe();
        tokio::spawn(async move {
            let mut g = [0u8; 8];
            let _ = timeout(Duration::from_secs(15), down.read_exact(&mut g)).await;
            down.set_nodelay(true).ok();
            let st = loop {
                if let Some(s) = state.lock().unwrap().clone() {
                    break s;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            };
            let t0 = std::time::Instant::now();
            if down.write_all(&st).await.is_err() {
                return;
            }
            let ordered: Vec<Arc<Vec<u8>>> = live.lock().unwrap().values().cloned().collect();
            for f in &ordered {
                if down.write_all(f).await.is_err() {
                    return;
                }
            }
            eprintln!(
                "[cache] node {addr}: state ({:.0}MB) + {} round-ordered live frames in {:.1}s; streaming live",
                st.len() as f64 / 1e6, ordered.len(), t0.elapsed().as_secs_f64()
            );
            loop {
                match brx.recv().await {
                    Ok(f) => {
                        if down.write_all(&f).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => return,
                }
            }
        });
    }
}

async fn feed_state(
    upstream: &str,
    state: &Arc<Mutex<Option<Arc<Vec<u8>>>>>,
    live: &Live,
    btx: &tokio::sync::broadcast::Sender<Arc<Vec<u8>>>,
    cap: usize,
) -> std::io::Result<()> {
    let mut s = TcpStream::connect(upstream).await?;
    s.set_nodelay(true).ok();
    s.write_all(&GREET_TRUE).await?;
    loop {
        let mut hdr = [0u8; 5];
        match timeout(Duration::from_secs(30), s.read_exact(&mut hdr)).await {
            Ok(res) => res?,
            Err(_) => return Err(std::io::Error::other("idle timeout")),
        };
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        // only legitimately-large frame is the ~950MB abci_state; never trust a bigger length
        if len > 1_500_000_000 {
            return Err(std::io::Error::other("oversized frame"));
        }
        let mut payload = vec![0u8; len];
        match timeout(Duration::from_secs(300), s.read_exact(&mut payload)).await {
            Ok(res) => res?,
            Err(_) => return Err(std::io::Error::other("payload timeout")),
        };
        let mut frame = hdr.to_vec();
        frame.extend_from_slice(&payload);
        let frame = Arc::new(frame);
        if len > 4_000_000 {
            *state.lock().unwrap() = Some(frame);
            eprintln!("[cache] cached abci_state: {len} bytes");
        } else if typ == 1 {
            if let Some(r) = block_round(&payload) {
                if insert_live(live, r, frame.clone(), cap) {
                    let _ = btx.send(frame);
                }
            }
        }
    }
}

async fn feed_live(
    upstream: &str,
    live: &Live,
    btx: &tokio::sync::broadcast::Sender<Arc<Vec<u8>>>,
    cap: usize,
) -> std::io::Result<()> {
    let mut s = TcpStream::connect(upstream).await?;
    s.set_nodelay(true).ok();
    s.write_all(&GREET_FALSE).await?; // send_abci:false: continuous live blocks
    loop {
        let mut hdr = [0u8; 5];
        s.read_exact(&mut hdr).await?;
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        if len > 50_000_000 {
            let mut rem = len;
            let mut buf = vec![0u8; 65536];
            while rem > 0 {
                let n = s.read(&mut buf[..rem.min(65536)]).await?;
                if n == 0 {
                    return Ok(());
                }
                rem -= n;
            }
            continue;
        }
        let mut payload = vec![0u8; len];
        s.read_exact(&mut payload).await?;
        if typ != 1 {
            continue;
        }
        if let Some(r) = block_round(&payload) {
            let mut frame = hdr.to_vec();
            frame.extend_from_slice(&payload);
            let frame = Arc::new(frame);
            if insert_live(live, r, frame.clone(), cap) {
                let _ = btx.send(frame);
            }
        }
    }
}

// ---- full P2P gateway ----
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
fn read_node_peers(path: &str) -> Vec<String> {
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

fn peer_candidates_path(node_peer_file: &str) -> PathBuf {
    Path::new(node_peer_file)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("peer_candidates.txt")
}

fn bootstrap_capture_peers(
    node_peer_file: &str,
    live_peers: Vec<String>,
    include_candidates: bool,
) -> Vec<String> {
    if !include_candidates {
        return live_peers;
    }
    let mut seen: HashSet<String> = live_peers.iter().cloned().collect();
    let mut out = live_peers;
    let candidates = peer_candidates_path(node_peer_file);
    for ip in read_node_peers(candidates.to_string_lossy().as_ref()) {
        if seen.insert(ip.clone()) {
            out.push(ip);
        }
    }
    out
}

// Bidirectionally relay a node connection and its chosen upstream until either side closes.
async fn splice(down: TcpStream, upc: TcpStream) {
    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = upc.into_split();
    let h = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut ur, &mut dw).await;
    });
    let _ = tokio::io::copy(&mut dr, &mut uw).await;
    h.abort();
}

enum HeaderRead {
    Complete,
    TimedOut(usize),
}

async fn read_header_or_timeout<R: AsyncRead + Unpin>(
    reader: &mut R,
    hdr: &mut [u8; 5],
    dur: Duration,
) -> std::io::Result<HeaderRead> {
    let mut off = 0usize;
    while off < hdr.len() {
        match timeout(dur, reader.read(&mut hdr[off..])).await {
            Ok(Ok(0)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "header eof",
                ));
            }
            Ok(Ok(n)) => off += n,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(HeaderRead::TimedOut(off)),
        }
    }
    Ok(HeaderRead::Complete)
}

async fn finish_header_after_partial<R: AsyncRead + Unpin>(
    reader: &mut R,
    hdr: &mut [u8; 5],
    mut off: usize,
) -> std::io::Result<()> {
    while off < hdr.len() {
        let n = reader.read(&mut hdr[off..]).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "header eof",
            ));
        }
        off += n;
    }
    Ok(())
}

// The shared bootstrap cache a capture writes into: in-memory blob + timestamp + disk path.
#[derive(Clone)]
struct CacheSink {
    boot_blob: Arc<Mutex<Option<Arc<Vec<u8>>>>>,
    boot_blob_cached_at: Arc<AtomicU64>,
    disk_cache_path: Option<PathBuf>,
}

fn install_bootstrap_cache(blob: Vec<u8>, source: &str, sink: &CacheSink) {
    let blob = Arc::new(blob);
    eprintln!(
        "[gw] bootstrap captured from {source}: {} MB",
        blob.len() / 1_000_000
    );
    *sink.boot_blob.lock().unwrap() = Some(blob.clone());
    sink.boot_blob_cached_at
        .store(unix_secs(), Ordering::Release);
    if let Some(path) = sink.disk_cache_path.clone() {
        tokio::task::spawn_blocking(move || {
            match persist_bootstrap_cache(&path, blob.as_slice()) {
                Ok(()) => {
                    eprintln!("[gw] bootstrap cache persisted to {}", path.display());
                }
                Err(e) => {
                    eprintln!(
                        "[gw] bootstrap cache persist failed at {}: {e}",
                        path.display()
                    );
                }
            }
        });
    }
}

// Transparent bootstrap fallback, with a write-through cache tap. This keeps the node's real peer
// relationship intact while opportunistically filling --cache from the same successful bootstrap.
async fn splice_bootstrap_capture(
    down: TcpStream,
    upc: TcpStream,
    first_hdr: [u8; 5],
    first_payload_prefix: Vec<u8>,
    sink: CacheSink,
    capture_permit: tokio::sync::OwnedSemaphorePermit,
) {
    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = upc.into_split();
    let h = tokio::spawn(async move {
        // Single-flight permit: released as soon as capturing stops (installed, overflowed, or
        // task end/abort) — NOT held for the rest of the relay, which may stream live blocks for
        // hours and would otherwise block every future cache tap.
        let mut capture_permit = Some(capture_permit);
        let mut blob = Vec::new();
        let mut frames = 0u64;
        let mut capture = true;
        let mut first = Some((first_hdr, first_payload_prefix));
        let mut win = Instant::now();
        let mut win_bytes = 0usize;
        let mut slow = 0u32;
        let source = "transparent fallback";

        loop {
            if !capture {
                capture_permit.take();
            }
            let mut hdr = [0u8; 5];
            let mut payload_prefix = Vec::new();
            if let Some((h, prefix)) = first.take() {
                hdr = h;
                payload_prefix = prefix;
            } else if capture && complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                match read_header_or_timeout(&mut ur, &mut hdr, Duration::from_secs(20)).await {
                    Ok(HeaderRead::Complete) => {}
                    Ok(HeaderRead::TimedOut(off)) => {
                        install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                        capture = false;
                        if finish_header_after_partial(&mut ur, &mut hdr, off)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => {
                        install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                        break;
                    }
                }
            } else if ur.read_exact(&mut hdr).await.is_err() {
                if capture && complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                    install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                }
                break;
            }

            let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
            if len > 1_500_000_000 {
                break;
            }
            if dw.write_all(&hdr).await.is_err() {
                break;
            }
            if capture {
                blob.extend_from_slice(&hdr);
            }

            let mut remaining = len;
            let mut buf = vec![0u8; 1_048_576.min(len.max(1))];
            if !payload_prefix.is_empty() {
                if payload_prefix.len() > remaining {
                    break;
                }
                if dw.write_all(&payload_prefix).await.is_err() {
                    break;
                }
                if capture {
                    blob.extend_from_slice(&payload_prefix);
                }
                remaining -= payload_prefix.len();
            }
            while remaining > 0 {
                let take = remaining.min(buf.len());
                let n = match ur.read(&mut buf[..take]).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                if dw.write_all(&buf[..n]).await.is_err() {
                    return;
                }
                if capture {
                    blob.extend_from_slice(&buf[..n]);
                }
                remaining -= n;
            }
            frames += 1;

            if capture {
                if blob.len() > 10_000_000_000 {
                    eprintln!(
                        "[gw] transparent bootstrap cache tap exceeded max capture size; disabling tap"
                    );
                    blob = Vec::new();
                    capture = false;
                    continue;
                }
                win_bytes += 5 + len;
                let el = win.elapsed();
                if el >= Duration::from_millis(1500) {
                    let rate = win_bytes as f64 / el.as_secs_f64();
                    if blob.len() > 1_500_000_000 && rate < 400_000.0 {
                        slow += 1;
                        if slow >= 8 && complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                            install_bootstrap_cache(std::mem::take(&mut blob), source, &sink);
                            capture = false;
                        }
                    } else {
                        slow = 0;
                    }
                    win = Instant::now();
                    win_bytes = 0;
                }
            }
        }
    });
    let _ = tokio::io::copy(&mut dr, &mut uw).await;
    h.abort();
}

const BOOTSTRAP_PREFETCH_BYTES: usize = 64_000_000;
const BOOTSTRAP_PREFETCH_SECS: u64 = 4;
const BOOTSTRAP_SELECT_DEADLINE_SECS: u64 = 24;
const BOOTSTRAP_SELECT_MAX_INFLIGHT: usize = 4;
const BOOTSTRAP_SELECT_MAX_PEERS: usize = 32;

struct BootstrapPeerSelection {
    upc: TcpStream,
    hdr: [u8; 5],
    payload_prefix: Vec<u8>,
    ip: String,
    abci_len: usize,
}

async fn try_fast_bootstrap_peer(
    ip: String,
    greet: [u8; 8],
) -> std::io::Result<BootstrapPeerSelection> {
    let mut upc = timeout(
        Duration::from_secs(5),
        TcpStream::connect(format!("{ip}:4001")),
    )
    .await??;
    upc.set_nodelay(true).ok();
    timeout(Duration::from_secs(5), upc.write_all(&greet)).await??;

    let mut hdr = [0u8; 5];
    timeout(Duration::from_secs(12), upc.read_exact(&mut hdr)).await??;
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    if len <= 4_000_000 {
        return Err(std::io::Error::other("not serving abci_state"));
    }
    let prefetch_len = len.min(BOOTSTRAP_PREFETCH_BYTES);
    let mut payload_prefix = vec![0u8; prefetch_len];
    timeout(
        Duration::from_secs(BOOTSTRAP_PREFETCH_SECS),
        upc.read_exact(&mut payload_prefix),
    )
    .await??;

    Ok(BootstrapPeerSelection {
        upc,
        hdr,
        payload_prefix,
        ip,
        abci_len: len,
    })
}

async fn select_fast_bootstrap_peer(
    peers: &[String],
    start: usize,
    greet: [u8; 8],
) -> Option<BootstrapPeerSelection> {
    let n = peers.len();
    if n == 0 {
        return None;
    }
    let limit = n.min(BOOTSTRAP_SELECT_MAX_PEERS);
    let deadline = Instant::now() + Duration::from_secs(BOOTSTRAP_SELECT_DEADLINE_SECS);
    let mut launched = 0usize;
    let mut in_flight = 0usize;
    let mut set = tokio::task::JoinSet::new();

    loop {
        while launched < limit && in_flight < BOOTSTRAP_SELECT_MAX_INFLIGHT {
            let ip = peers[(start + launched) % n].clone();
            set.spawn(try_fast_bootstrap_peer(ip, greet));
            launched += 1;
            in_flight += 1;
        }
        if in_flight == 0 {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, set.join_next()).await {
            Ok(Some(joined)) => {
                in_flight = in_flight.saturating_sub(1);
                if let Ok(Ok(selected)) = joined {
                    set.abort_all();
                    return Some(selected);
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    set.abort_all();
    None
}

async fn read_live_greeting_with_timeout(
    s: &mut TcpStream,
    wait: Duration,
) -> std::io::Result<Vec<u8>> {
    let mut hdr = [0u8; 5];
    timeout(wait, s.read_exact(&mut hdr)).await??;
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    if len > 1000 {
        return Err(std::io::Error::other("live greeting too large"));
    }
    let mut payload = vec![0u8; len];
    timeout(wait, s.read_exact(&mut payload)).await??;
    let mut frame = hdr.to_vec();
    frame.extend_from_slice(&payload);
    Ok(frame)
}

async fn read_live_greeting(s: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    read_live_greeting_with_timeout(s, Duration::from_secs(20)).await
}

async fn open_live_peer_session(ip: &str, wait: Duration) -> std::io::Result<TcpStream> {
    let mut s = timeout(wait, TcpStream::connect(format!("{ip}:4001"))).await??;
    s.set_nodelay(true).ok();
    timeout(wait, s.write_all(&GREET_FALSE)).await??;
    read_live_greeting_with_timeout(&mut s, wait).await?;
    Ok(s)
}

// Connect to the session's active peer (the one serving the node's bootstrap) on `port`, waiting
// briefly for the bootstrap to set it; falls back to a round-robin pool peer if none is set yet.
async fn dial_active(
    node: &NodeState,
    peers: &[String],
    rr: &Arc<AtomicUsize>,
    port: u16,
) -> Option<TcpStream> {
    let n = peers.len();
    if n == 0 {
        return None;
    }
    // brief wait in case a concurrent bootstrap is about to set the active peer
    let mut target = None;
    for _ in 0..15 {
        if let Some(a) = node.active.lock().unwrap().clone() {
            target = Some(a);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // no active yet (e.g. a resuming node after a gateway restart, with no fresh bootstrap): establish
    // one now so EVERY connection of this node converges on the same upstream (client-block RPC needs
    // it). Marked lazy: this pin must not suppress a cache replay if the node is actually cold-starting
    // and its 4001 bootstrap greet simply hasn't arrived yet.
    let target = match target {
        Some(t) => t,
        None => {
            let mut a = node.active.lock().unwrap();
            match a.clone() {
                Some(t) => t,
                None => {
                    let t = peers[rr.fetch_add(1, Ordering::Relaxed) % n].clone();
                    *a = Some(t.clone());
                    node.lazy_active.store(true, Ordering::Release);
                    t
                }
            }
        }
    };
    let up = format!("{}:{}", target, port);
    match timeout(Duration::from_secs(5), TcpStream::connect(&up)).await {
        Ok(Ok(c)) => {
            c.set_nodelay(true).ok();
            Some(c)
        }
        // do NOT clear active on a transient failure (that flaps the active peer and makes the
        // client-block RPC land on a peer that doesn't know this node -> "Peer-only request").
        // The active peer is (re)set by the node's 4001 block-stream connection instead.
        _ => None,
    }
}

fn clear_active_if_current(active: &Mutex<Option<String>>, ip: &str) {
    let mut guard = active.lock().unwrap();
    if guard.as_deref() == Some(ip) {
        *guard = None;
    }
}

fn response_is_peer_only(payload: &[u8]) -> bool {
    payload
        .windows(b"Peer-only request".len())
        .any(|w| w == b"Peer-only request")
}

fn response_has_no_client_blocks(payload: &[u8]) -> bool {
    payload
        .windows(b"no client blocks to serve".len())
        .any(|w| w == b"no client blocks to serve")
}

fn response_has_client_block_round_too_large(payload: &[u8]) -> bool {
    payload
        .windows(b"client block round too large".len())
        .any(|w| w == b"client block round too large")
}

fn response_has_client_block_round_too_small(payload: &[u8]) -> bool {
    payload
        .windows(b"client block round too small".len())
        .any(|w| w == b"client block round too small")
}

async fn wait_active_peer(active: &Mutex<Option<String>>) -> Option<String> {
    for _ in 0..12 {
        if let Some(ip) = active.lock().unwrap().clone() {
            return Some(ip);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}

fn client_block_peer_candidates(
    current: Option<String>,
    peers: &[String],
    rr: &AtomicUsize,
    limit: usize,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    if let Some(ip) = current {
        if seen.insert(ip.clone()) {
            out.push(ip);
        }
    }
    if peers.is_empty() || out.len() >= limit {
        return out;
    }
    let start = rr.fetch_add(limit.max(1), Ordering::Relaxed);
    for k in 0..peers.len() {
        if out.len() >= limit {
            break;
        }
        let ip = peers[(start + k) % peers.len()].clone();
        if seen.insert(ip.clone()) {
            out.push(ip);
        }
    }
    out
}

enum ClientBlockUpstreamResponse {
    Frames(Vec<u8>),
    PeerOnly,
    TerminalError {
        frame: Vec<u8>,
        reason: &'static str,
    },
}

async fn fetch_client_blocks_from_peer(
    ip: String,
    req: Arc<Vec<u8>>,
    prewarm_live: bool,
) -> (String, std::io::Result<ClientBlockUpstreamResponse>) {
    let result = async {
        // A fallback candidate must first have a live 4001 peer relationship, otherwise many
        // public peers reject the separate 4002 RPC as "Peer-only request".
        let _live_guard = if prewarm_live {
            Some(open_live_peer_session(&ip, Duration::from_secs(4)).await?)
        } else {
            None
        };
        let mut upc = timeout(
            Duration::from_secs(4),
            TcpStream::connect(format!("{ip}:4002")),
        )
        .await??;
        upc.set_nodelay(true).ok();
        timeout(Duration::from_secs(4), upc.write_all(req.as_slice())).await??;

        let mut rh = [0u8; 5];
        timeout(Duration::from_secs(6), upc.read_exact(&mut rh)).await??;
        let rl = u32::from_be_bytes([rh[0], rh[1], rh[2], rh[3]]) as usize;
        // A client-block batch is a handful of blocks (~100 rounds/request); cap well above that
        // but far below a memory hazard.
        if rl > 64_000_000 {
            return Err(std::io::Error::other("oversized client-block response"));
        }
        let mut rp = vec![0u8; rl];
        timeout(Duration::from_secs(12), upc.read_exact(&mut rp)).await??;
        if response_is_peer_only(&rp) {
            return Ok(ClientBlockUpstreamResponse::PeerOnly);
        }
        let mut frame = rh.to_vec();
        frame.extend_from_slice(&rp);
        if response_has_no_client_blocks(&rp) {
            return Ok(ClientBlockUpstreamResponse::TerminalError {
                frame,
                reason: "no client blocks",
            });
        }
        if response_has_client_block_round_too_large(&rp) {
            return Ok(ClientBlockUpstreamResponse::TerminalError {
                frame,
                reason: "client block round too large",
            });
        }
        if response_has_client_block_round_too_small(&rp) {
            return Err(std::io::Error::other("client block round too small"));
        }
        Ok(ClientBlockUpstreamResponse::Frames(frame))
    }
    .await;
    (ip, result)
}

// Serve the node's client-block RPC (port 4002). Prefer the current 4001 active peer, but hedge
// against a slow/stalled active by briefly opening live 4001 sessions to a few pool candidates and
// racing their 4002 responses. The temporary live session is kept open until the 4002 response is
// fully read, which satisfies peers that require a live peer relationship for client-block RPC.
async fn serve_client_blocks(
    down: TcpStream,
    node: Arc<NodeState>,
    peers: Vec<String>,
    rr: Arc<AtomicUsize>,
) {
    let active = &node.active;
    let mut down = down;
    let mut hdr = [0u8; 5];
    if !matches!(
        timeout(Duration::from_secs(120), down.read_exact(&mut hdr)).await,
        Ok(Ok(_))
    ) {
        return;
    }
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    if len > 65536 {
        return;
    }
    let mut payload = vec![0u8; len];
    if !matches!(
        timeout(Duration::from_secs(30), down.read_exact(&mut payload)).await,
        Ok(Ok(_))
    ) {
        return;
    }
    let mut req = hdr.to_vec();
    req.extend_from_slice(&payload);

    let req = Arc::new(req);
    let started = Instant::now();
    let mut first_attempt = true;
    let mut last_failures = Vec::new();
    let mut last_terminal = None;
    while started.elapsed() < Duration::from_secs(45) {
        let current = if first_attempt {
            first_attempt = false;
            wait_active_peer(active).await
        } else {
            active.lock().unwrap().clone()
        };
        let candidates = client_block_peer_candidates(current.clone(), &peers, &rr, 8);
        if candidates.is_empty() {
            return;
        }

        let mut attempts = tokio::task::JoinSet::new();
        for ip in candidates {
            let prewarm_live = current.as_deref() != Some(ip.as_str());
            attempts.spawn(fetch_client_blocks_from_peer(ip, req.clone(), prewarm_live));
        }

        let mut failures = Vec::new();
        let mut terminal = None;
        while let Some(joined) = attempts.join_next().await {
            let Ok((ip, result)) = joined else {
                continue;
            };
            match result {
                Ok(ClientBlockUpstreamResponse::Frames(frame)) => {
                    attempts.abort_all();
                    let _ = down.write_all(&frame).await;
                    return;
                }
                Ok(ClientBlockUpstreamResponse::PeerOnly) => {
                    if current.as_deref() == Some(ip.as_str()) {
                        clear_active_if_current(active, &ip);
                    }
                    failures.push(format!("{ip}: peer-only"));
                }
                Ok(ClientBlockUpstreamResponse::TerminalError { frame, reason }) => {
                    if terminal.is_none() {
                        terminal = Some(frame);
                    }
                    failures.push(format!("{ip}: {reason}"));
                }
                Err(e) => {
                    failures.push(format!("{ip}: {e}"));
                }
            }
        }
        if terminal.is_some() {
            last_terminal = terminal;
        }
        last_failures = failures;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if let Some(frame) = last_terminal {
        let _ = down.write_all(&frame).await;
        return;
    }
    if !last_failures.is_empty() {
        eprintln!(
            "[gw] [{}] 4002 client-block fetch failed via {}",
            node.ip,
            last_failures.join("; ")
        );
    }
}

// Capture a full bootstrap (abci_state + EVM KVs) VERBATIM from a serving state-server, frame-aligned,
// stopping when the bulk transfer ends (byte rate collapses from the bulk's tens-of-MB/s to the
// live-block trickle). Replayed to a bootstrapping node so it never pulls the snapshot from a peer
// (avoids the per-IP abci_state rate-limit). The node handles the internal abci_state/EVM-KVs/live
// framing itself, so the gateway needn't understand the (undocumented) boundary.
fn bootstrap_capture_err(
    reason: impl std::fmt::Display,
    bytes: usize,
    frames: u64,
    started: Instant,
) -> std::io::Error {
    let elapsed = started.elapsed().as_secs_f64();
    let mb = bytes as f64 / 1_000_000.0;
    let rate = if elapsed > 0.0 { mb / elapsed } else { 0.0 };
    std::io::Error::other(format!(
        "{reason}; read={mb:.1}MB frames={frames} elapsed={elapsed:.1}s avg={rate:.1}MB/s"
    ))
}

const MIN_COMPLETE_BOOTSTRAP_BYTES: usize = 4_400_000_000;
const MIN_COMPLETE_BOOTSTRAP_FRAMES: u64 = 4_000;
const CACHE_REPLAY_COOLDOWN_SECS: u64 = 30;
const MAX_BOOTSTRAP_CACHE_AGE_SECS: u64 = 30 * 60;
const CACHE_REFRESH_SECS: u64 = 10 * 60;
const CACHE_REFRESH_RETRY_SECS: u64 = 2 * 60;
const MIN_BOOTSTRAP_RATE_BYTES_PER_SEC: f64 = 5_000_000.0;

fn complete_bootstrap_at_frame_boundary(bytes: usize, frames: u64) -> bool {
    bytes >= MIN_COMPLETE_BOOTSTRAP_BYTES && frames >= MIN_COMPLETE_BOOTSTRAP_FRAMES
}

fn complete_frame_count(buf: &[u8]) -> Option<u64> {
    let mut off = 0usize;
    let mut frames = 0u64;
    while off < buf.len() {
        if buf.len().saturating_sub(off) < 5 {
            return None;
        }
        let len = u32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]) as usize;
        if len > 1_500_000_000 {
            return None;
        }
        let next = off.checked_add(5)?.checked_add(len)?;
        if next > buf.len() {
            return None;
        }
        off = next;
        frames += 1;
    }
    Some(frames)
}

fn cache_parent_writable(path: &Path) -> bool {
    let Some(parent) = path.parent() else {
        return false;
    };
    let probe = parent.join(format!(".hypersync-cache-probe-{}", std::process::id()));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = std::fs::remove_file(probe);
            true
        }
        Err(_) => false,
    }
}

fn bootstrap_cache_path(node_peer_file: &str) -> PathBuf {
    if let Ok(path) = env::var("HYPERSYNC_BOOT_CACHE") {
        return PathBuf::from(path);
    }
    let peer_dir_cache = Path::new(node_peer_file)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("bootstrap.cache");
    if cache_parent_writable(&peer_dir_cache) {
        peer_dir_cache
    } else {
        PathBuf::from("/tmp/hypersync-bootstrap.cache")
    }
}

fn is_bootstrap_cache_fresh(now: u64, cached_at: u64) -> bool {
    cached_at != 0 && now.saturating_sub(cached_at) <= MAX_BOOTSTRAP_CACHE_AGE_SECS
}

fn load_bootstrap_cache(path: &Path, now: u64) -> std::io::Result<Option<(Vec<u8>, u64)>> {
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let cached_at = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if !is_bootstrap_cache_fresh(now, cached_at) {
        return Err(std::io::Error::other(format!(
            "disk cache too old: {}s",
            now.saturating_sub(cached_at)
        )));
    }
    if meta.len() < MIN_COMPLETE_BOOTSTRAP_BYTES as u64 {
        return Err(std::io::Error::other(format!(
            "disk cache too small: {} bytes",
            meta.len()
        )));
    }
    let blob = std::fs::read(path)?;
    let Some(frames) = complete_frame_count(&blob) else {
        return Err(std::io::Error::other(format!(
            "disk cache has incomplete frame boundary: {} bytes",
            blob.len()
        )));
    };
    if !complete_bootstrap_at_frame_boundary(blob.len(), frames) {
        return Err(std::io::Error::other(format!(
            "disk cache incomplete: {} bytes, {} frames",
            blob.len(),
            frames
        )));
    }
    Ok(Some((blob, cached_at)))
}

fn persist_bootstrap_cache(path: &Path, blob: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // unique tmp per writer: the fallback tap and the background refresher can persist
    // concurrently, and a shared tmp name would interleave their writes
    static PERSIST_SEQ: AtomicU64 = AtomicU64::new(0);
    let tmp = path.with_extension(format!(
        "tmp{}",
        PERSIST_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = std::fs::File::create(&tmp)?;
    if let Err(e) = file.write_all(blob).and_then(|_| file.sync_all()) {
        std::fs::remove_file(&tmp).ok();
        return Err(e);
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn cache_replay_allowed(now: u64, last: u64) -> bool {
    last == 0 || now.saturating_sub(last) >= CACHE_REPLAY_COOLDOWN_SECS
}

fn should_replay_cache(now: u64, last: u64, has_active_session: bool, cached_at: u64) -> bool {
    !has_active_session
        && is_bootstrap_cache_fresh(now, cached_at)
        && cache_replay_allowed(now, last)
}

fn should_fetch_forward_client_blocks(port: u16) -> bool {
    port == 4002
}

fn raise_round_floor(floor: &Arc<AtomicU32>, round: u32) -> bool {
    let mut cur = floor.load(Ordering::Acquire);
    while round > cur {
        match floor.compare_exchange(cur, round, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(next) => cur = next,
        }
    }
    false
}

fn max_block_round_in_frames(blob: &[u8]) -> Option<u32> {
    const MAX_LIVE_BLOCK_FRAME: usize = 2_000_000;
    let mut off = 0usize;
    let mut max_round = None;
    while off + 5 <= blob.len() {
        let len =
            u32::from_be_bytes([blob[off], blob[off + 1], blob[off + 2], blob[off + 3]]) as usize;
        let typ = blob[off + 4];
        off += 5;
        if len > blob.len().saturating_sub(off) {
            break;
        }
        let payload = &blob[off..off + len];
        if typ == 1 && len <= MAX_LIVE_BLOCK_FRAME {
            if let Some(round) = block_round(payload) {
                max_round = Some(max_round.map_or(round, |prev: u32| prev.max(round)));
            }
        }
        off += len;
    }
    max_round
}

fn should_forward_block_round(
    round: u32,
    from_active: bool,
    last_forwarded: &mut u32,
    dedup: &RoundDedup,
) -> bool {
    if from_active {
        let prev = *last_forwarded;
        if prev != 0 && round <= prev {
            return false;
        }
        if !dedup.is_new(round) {
            return false;
        }
        *last_forwarded = round;
        return true;
    }

    let prev = *last_forwarded;
    if prev == 0 || round != prev.saturating_add(1) {
        return false;
    }
    if !dedup.is_new(round) {
        return false;
    }
    *last_forwarded = round;
    true
}

struct RoundForwardGate {
    last_forwarded: u32,
    dedup: RoundDedup,
}

impl RoundForwardGate {
    fn with_last_forwarded(cap: usize, last_forwarded: u32) -> Self {
        Self {
            last_forwarded,
            dedup: RoundDedup::new(cap),
        }
    }

    fn should_forward(&mut self, round: u32, from_active: bool) -> bool {
        should_forward_block_round(round, from_active, &mut self.last_forwarded, &self.dedup)
    }
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn fetch_bootstrap(upstream: &str) -> std::io::Result<Vec<u8>> {
    let mut s = TcpStream::connect(upstream).await?;
    s.set_nodelay(true).ok();
    s.write_all(&GREET_TRUE).await?;
    let mut blob: Vec<u8> = Vec::new();
    let t0 = Instant::now();
    let mut first = true;
    let mut win = Instant::now();
    let mut win_bytes = 0usize;
    let mut slow = 0u32;
    let mut frames = 0u64;
    loop {
        let mut hdr = [0u8; 5];
        match timeout(Duration::from_secs(20), s.read_exact(&mut hdr)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                if complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                    return Ok(blob);
                }
                return Err(bootstrap_capture_err(
                    format!("read header failed: {e}"),
                    blob.len(),
                    frames,
                    t0,
                ));
            }
            Err(_) => {
                if complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                    return Ok(blob);
                }
                return Err(bootstrap_capture_err(
                    "read header timeout",
                    blob.len(),
                    frames,
                    t0,
                ));
            }
        }
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if first {
            if len <= 4_000_000 {
                return Err(bootstrap_capture_err(
                    format!("not serving abci_state first_len={len} type={}", hdr[4]),
                    blob.len(),
                    frames,
                    t0,
                ));
            }
            first = false;
        }
        // Cap a single frame: the only legitimately-large frame is the ~954MB abci_state snapshot;
        // 1.5GB leaves headroom for state growth while rejecting a bogus/huge length that would
        // otherwise force a multi-GB allocation (3 of these race concurrently).
        if len > 1_500_000_000 {
            return Err(bootstrap_capture_err(
                format!("frame too large len={len}"),
                blob.len(),
                frames,
                t0,
            ));
        }
        let mut payload = vec![0u8; len];
        if len > 50_000_000 {
            // The big frame is the ~954MB abci_state. Read it in chunks and abort early if the
            // sustained rate is too low to ever deliver the full bootstrap before the per-frame
            // deadline. This frees the slot so the race moves to a faster state-server in ~4s
            // instead of blocking ~60s on one slow peer. (The same peers were rejected before via
            // the 60s read_exact timeout — this just rejects them ~15x sooner.)
            let mut off = 0usize;
            let fstart = Instant::now();
            let mut ok = true;
            while off < len {
                match timeout(Duration::from_secs(15), s.read(&mut payload[off..])).await {
                    Ok(Ok(n)) if n > 0 => off += n,
                    _ => {
                        ok = false;
                        break;
                    }
                }
                let el = fstart.elapsed().as_secs_f64();
                // Abort only below the speed gate's floor; 7-9MB/s peers are slow but usable for
                // one-time cache refresh, and rejecting all of them can leave cold cache empty.
                if el > 4.0 && (off as f64 / el) < MIN_BOOTSTRAP_RATE_BYTES_PER_SEC {
                    return Err(bootstrap_capture_err(
                        format!(
                            "state-server too slow early len={len} off={off} rate={:.1}MB/s",
                            (off as f64 / el) / 1_000_000.0
                        ),
                        blob.len(),
                        frames,
                        t0,
                    ));
                }
            }
            if !ok {
                return Err(bootstrap_capture_err(
                    format!("large frame read failed len={len} off={off}"),
                    blob.len(),
                    frames,
                    t0,
                ));
            }
        } else if !matches!(
            timeout(Duration::from_secs(60), s.read_exact(&mut payload)).await,
            Ok(Ok(_))
        ) {
            return Err(bootstrap_capture_err(
                format!("small frame read failed len={len}"),
                blob.len(),
                frames,
                t0,
            ));
        }
        blob.extend_from_slice(&hdr);
        blob.extend_from_slice(&payload);
        frames += 1;
        // total cap: a real bootstrap is ~4.5GB; a peer that streams bulk forever (staying above
        // the rate-drop floor) must not inflate the capture unboundedly (x3 concurrent racers).
        if blob.len() > 10_000_000_000 {
            return Err(bootstrap_capture_err(
                "capture too large",
                blob.len(),
                frames,
                t0,
            ));
        }
        win_bytes += 5 + len;
        // speed gate: right after the abci_state snapshot, require a fast server (else the EVM-KVs
        // capture is slow and the rate-drop detector could mistake a slow tail for the end).
        if blob.len() >= 900_000_000 && blob.len() < 970_000_000 {
            let r = blob.len() as f64 / t0.elapsed().as_secs_f64();
            if r < MIN_BOOTSTRAP_RATE_BYTES_PER_SEC {
                return Err(bootstrap_capture_err(
                    format!(
                        "state-server too slow after abci_state rate={:.1}MB/s",
                        r / 1_000_000.0
                    ),
                    blob.len(),
                    frames,
                    t0,
                ));
            }
        }
        let el = win.elapsed();
        if el >= Duration::from_millis(1500) {
            let rate = win_bytes as f64 / el.as_secs_f64();
            // bulk (abci_state + EVM KVs) ends when the rate collapses to the live-block trickle.
            // Require it SUSTAINED (~12s of <400KB/s) so a mid-EVM-KVs slowdown isn't taken for the end.
            if blob.len() > 1_500_000_000 && rate < 400_000.0 {
                slow += 1;
                if slow >= 8 {
                    if complete_bootstrap_at_frame_boundary(blob.len(), frames) {
                        return Ok(blob);
                    }
                    return Err(bootstrap_capture_err(
                        "bulk ended before minimum usable size",
                        blob.len(),
                        frames,
                        t0,
                    ));
                }
            } else {
                slow = 0;
            }
            win = Instant::now();
            win_bytes = 0;
        }
    }
}

// Race several state-servers for the bootstrap capture instead of trying them one-by-one. A
// rate-limited peer fails in <1s and is replaced immediately; a slow-but-serving peer would
// otherwise block the queue while it downloads ~900MB before the speed gate rejects it, so after
// `hedge` with no winner we add a concurrent attempt to a fresh peer (up to `max_inflight`). The
// first CLEAN capture wins and the rest are aborted, freeing bandwidth and the per-IP abci_state
// quota. `hedge` is set above the typical fast-capture time, so the common case runs solo (no
// contention) and only the slow tail is hedged.
async fn capture_bootstrap_raced(
    peers: Vec<String>,
    max_inflight: usize,
    hedge: Duration,
) -> Option<(Vec<u8>, String)> {
    if peers.is_empty() {
        return None;
    }
    let mut set: tokio::task::JoinSet<(std::io::Result<Vec<u8>>, String)> =
        tokio::task::JoinSet::new();
    let mut idx = 0usize;
    let mut started = 0usize;
    macro_rules! spawn_next {
        () => {{
            let ip = peers[idx].clone();
            idx += 1;
            started += 1;
            let up = format!("{}:4001", ip);
            set.spawn(async move { (fetch_bootstrap(&up).await, ip) });
        }};
    }
    spawn_next!();
    loop {
        if set.is_empty() && idx >= peers.len() {
            eprintln!(
                "[gw] bootstrap race: no usable state-server among {started} peers tried (all too-slow / incomplete / throttled)"
            );
            return None;
        }
        tokio::select! {
            joined = set.join_next(), if !set.is_empty() => {
                match joined {
                    Some(Ok((Ok(blob), ip))) => {
                        eprintln!(
                            "[gw] bootstrap race won by {ip} ({} MB) after {started} attempt(s)",
                            blob.len() / 1_000_000
                        );
                        set.abort_all();
                        return Some((blob, ip));
                    }
                    Some(Ok((Err(e), ip))) => {
                        eprintln!("[gw] bootstrap attempt via {ip} failed: {e}");
                    }
                    Some(Err(e)) => {
                        eprintln!("[gw] bootstrap attempt task failed: {e}");
                    }
                    None => {}
                }
                // failed/short attempt: replace immediately with a fresh candidate
                if idx < peers.len() {
                    spawn_next!();
                }
            }
            _ = tokio::time::sleep(hedge), if set.len() < max_inflight && idx < peers.len() => {
                eprintln!("[gw] bootstrap race: no winner in {}s, hedging attempt #{}", hedge.as_secs(), started + 1);
                spawn_next!();
            }
        }
    }
}

// Probe one candidate for LIVE-block serving (send_abci:false — cheap, NOT rate-limited, unlike
// abci_state). Bounded to ~4s wall-clock total; counts type=1 frames with payload len>1 (excludes
// the tiny 1-byte status/rejection frames). A peer-controlled frame length is capped before
// allocating the payload buffer (a live block is a few hundred KB at most).
async fn probe_live(ip: &str) -> usize {
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut s = match timeout(
        Duration::from_secs(4),
        TcpStream::connect(format!("{ip}:4001")),
    )
    .await
    {
        Ok(Ok(s)) => s,
        _ => return 0,
    };
    if s.write_all(&GREET_FALSE).await.is_err() {
        return 0;
    }
    let mut blocks = 0usize;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let mut hdr = [0u8; 5];
        if !matches!(timeout(remaining, s.read_exact(&mut hdr)).await, Ok(Ok(_))) {
            break;
        }
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if len > 8_000_000 {
            break; // sanity cap: a live-probe frame is never this large
        }
        let mut payload = vec![0u8; len];
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !matches!(
            timeout(remaining, s.read_exact(&mut payload)).await,
            Ok(Ok(_))
        ) {
            break;
        }
        if hdr[4] == 1 && len > 1 {
            blocks += 1;
        }
    }
    blocks
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

fn should_keep_previous_peer_pool(current_live: usize, previous_live: usize) -> bool {
    current_live > 0
        && current_live < MIN_LIVE_SERVERS_TO_OVERWRITE
        && previous_live >= MIN_LIVE_SERVERS_TO_OVERWRITE
}

fn should_write_candidate_fallback(
    current_live: usize,
    previous_live: usize,
    candidates: usize,
) -> bool {
    current_live < MIN_LIVE_SERVERS_TO_OVERWRITE
        && (previous_live < MIN_LIVE_SERVERS_TO_OVERWRITE || current_live == 0)
        && candidates >= MIN_LIVE_SERVERS_TO_OVERWRITE
}

fn split_csv(v: &str) -> Vec<String> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

async fn run_peerd(interval: u64) {
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

    let mut candidates: HashSet<String> = tokio::fs::read_to_string(&cand_path)
        .await
        .map(|s| s.lines().map(str::to_string).collect())
        .unwrap_or_default();
    // consecutive failed-probe count per candidate; prune after PRUNE_AFTER cycles so stale
    // harvested IPs don't get re-probed forever (a pruned peer still advertised by discovery is
    // simply re-added next cycle and gets a fresh count).
    const PRUNE_AFTER: u32 = 50;
    let mut fail_counts: HashMap<String, u32> = HashMap::new();

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

        // 2. probe every known candidate concurrently for live-block serving (capped at 64 at a
        // time so a poisoned harvest source can't turn one cycle into thousands of sockets)
        let cand_vec: Vec<String> = candidates.iter().cloned().collect();
        let sem = Arc::new(tokio::sync::Semaphore::new(64));
        let mut set: tokio::task::JoinSet<(String, usize)> = tokio::task::JoinSet::new();
        for ip in cand_vec.iter().cloned() {
            let sem = sem.clone();
            set.spawn(async move {
                let _permit = sem.acquire().await;
                let n = probe_live(&ip).await;
                (ip, n)
            });
        }
        let mut live: Vec<(String, usize)> = Vec::new();
        let mut failed: Vec<String> = Vec::new();
        while let Some(r) = set.join_next().await {
            if let Ok((ip, n)) = r {
                if n >= 2 {
                    live.push((ip, n));
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
        live.sort_by(|a, b| b.1.cmp(&a.1));
        let ranked: Vec<String> = live.into_iter().map(|(ip, _)| ip).collect();

        let previous_live = tokio::fs::read_to_string(&out_path)
            .await
            .map(|s| extract_ipv4(&s).len())
            .unwrap_or(0);
        let kept_previous = should_keep_previous_peer_pool(ranked.len(), previous_live);
        let candidate_fallback =
            should_write_candidate_fallback(ranked.len(), previous_live, cand_vec.len());
        let output_pool = if candidate_fallback {
            &cand_vec
        } else {
            &ranked
        };
        // 3. write peers.json (hand-rolled: content is plain IPv4 strings, no escaping needed)
        let json = format!(
            "{{\"live_servers\":[{}],\"n_candidates\":{}}}",
            output_pool
                .iter()
                .map(|ip| format!("\"{ip}\""))
                .collect::<Vec<_>>()
                .join(","),
            cand_vec.len()
        );
        if !kept_previous {
            write_atomic(&out_path, &json).await;
        }

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
            "{now} candidates={} live={} pruned={} kept_previous={} candidate_fallback={} top={:?}\n",
            cand_vec.len(),
            ranked.len(),
            pruned,
            kept_previous,
            candidate_fallback,
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

        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

// Per-downstream-node session state, keyed by the node's source IP (each hl-node needs its own
// egress IP as seen by the gateway; NAT'ing two nodes through one IP conflates their sessions).
// State deliberately survives reconnects: nodes restart and come back with the same IP, and the
// replay cooldown / live floor must persist across that.
struct NodeState {
    ip: IpAddr,
    // the upstream peer serving THIS node's bootstrap; all of this node's connections reuse it
    // so client-block RPC (4002) isn't rejected with "Peer-only request".
    active: Mutex<Option<String>>,
    // Non-zero only after this gateway actually replayed boot_blob to THIS node. The short replay
    // cooldown only prevents duplicate replays from immediate reconnects while allowing normal
    // node restarts to use cache.
    last_cache_replay_secs: AtomicU64,
    // During cache cold-start, the node races ahead through 4002 client-block catch-up while the
    // 4001 live stream may still replay older backlog. Drop live block frames at/below this floor.
    // Arc so PushConfig/pump_merge keep their existing Arc<AtomicU32> shape.
    live_floor: Arc<AtomicU32>,
    last_seen_secs: AtomicU64,
    // connections currently open from this node; a long-lived live stream opens no new
    // connections for hours, so eviction must key on this, not just last_seen
    open_conns: AtomicUsize,
    // true when `active` was pinned lazily by a misc-port dial (no real 4001 session behind it).
    // A cold-starting node's gossip ports can connect before its 4001 bootstrap greet; a lazy pin
    // must not count as "has an active session" or it would suppress the node's cache replay.
    lazy_active: AtomicBool,
}

impl NodeState {
    fn new(ip: IpAddr, now: u64) -> Self {
        Self {
            ip,
            active: Mutex::new(None),
            last_cache_replay_secs: AtomicU64::new(0),
            live_floor: Arc::new(AtomicU32::new(0)),
            last_seen_secs: AtomicU64::new(now),
            open_conns: AtomicUsize::new(0),
            lazy_active: AtomicBool::new(false),
        }
    }

    // Pin this node's active peer from a real 4001 session (bootstrap or live/resume).
    fn pin_active(&self, ip: &str) {
        *self.active.lock().unwrap() = Some(ip.to_string());
        self.lazy_active.store(false, Ordering::Release);
    }

    // "This node has a live session" — a lazy misc-port pin doesn't count.
    fn has_active_session(&self) -> bool {
        self.active.lock().unwrap().is_some() && !self.lazy_active.load(Ordering::Acquire)
    }
}

// Decrements the node's open-connection count when the connection task ends (however it ends).
struct NodeConnGuard(Arc<NodeState>);

impl Drop for NodeConnGuard {
    fn drop(&mut self) {
        // stamp BEFORE decrementing: an eviction scan between the two must never see
        // open_conns==0 paired with an hours-stale last_seen
        self.0.last_seen_secs.store(unix_secs(), Ordering::Release);
        self.0.open_conns.fetch_sub(1, Ordering::AcqRel);
    }
}

// The gateway binds 0.0.0.0 with no allowlist; scanners must not grow the registry unbounded.
const NODE_STATE_CAP: usize = 256;
// Keep state across node restarts; prune entries idle this long.
const NODE_STATE_TTL_SECS: u64 = 60 * 60;

// Which entries to evict before inserting a new key. Entries with open connections are never
// evicted. Of the rest: everything past the idle TTL goes, then (if still over cap)
// oldest-last_seen first. Tie-break on ip so eviction is deterministic.
fn node_state_evict_keys(
    entries: &[(IpAddr, u64, bool)],
    now: u64,
    cap: usize,
    ttl: u64,
) -> Vec<IpAddr> {
    let mut evict: Vec<IpAddr> = entries
        .iter()
        .filter(|(_, seen, held)| !held && now.saturating_sub(*seen) > ttl)
        .map(|(ip, _, _)| *ip)
        .collect();
    let mut evictable: Vec<(IpAddr, u64)> = entries
        .iter()
        .filter(|(_, seen, held)| !held && now.saturating_sub(*seen) <= ttl)
        .map(|(ip, seen, _)| (*ip, *seen))
        .collect();
    let kept = entries.len() - evict.len();
    if kept + 1 > cap {
        evictable.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let excess = (kept + 1 - cap).min(evictable.len());
        evict.extend(evictable.iter().take(excess).map(|(ip, _)| *ip));
    }
    evict
}

#[derive(Default)]
struct NodeRegistry {
    nodes: Mutex<HashMap<IpAddr, Arc<NodeState>>>,
}

impl NodeRegistry {
    // Returns the node's state plus a guard that holds its open-connection count; keep the guard
    // alive for the life of the connection task.
    fn get_or_insert(&self, ip: IpAddr, now: u64) -> (Arc<NodeState>, NodeConnGuard) {
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(state) = nodes.get(&ip) {
            state.last_seen_secs.store(now, Ordering::Release);
            state.open_conns.fetch_add(1, Ordering::AcqRel);
            return (state.clone(), NodeConnGuard(state.clone()));
        }
        let entries: Vec<(IpAddr, u64, bool)> = nodes
            .iter()
            .map(|(ip, s)| {
                (
                    *ip,
                    s.last_seen_secs.load(Ordering::Acquire),
                    s.open_conns.load(Ordering::Acquire) > 0,
                )
            })
            .collect();
        for stale in node_state_evict_keys(&entries, now, NODE_STATE_CAP, NODE_STATE_TTL_SECS) {
            nodes.remove(&stale);
        }
        let state = Arc::new(NodeState::new(ip, now));
        state.open_conns.fetch_add(1, Ordering::AcqRel);
        nodes.insert(ip, state.clone());
        (state.clone(), NodeConnGuard(state))
    }
}

// Full P2P gateway. Every downstream node connects ONLY to the gateway; the gateway provides all of
// HL's sync P2P backed by MULTIPLE upstream peers (taken from the node's own peer file, a startup path arg):
//   - abci_state: fetched from a pool peer and CACHED (served to the node at local speed, so node
//     restarts never re-pull ~950MB and never hit the per-IP abci_state rate-limit);
//   - live blocks: round-merged from several pool peers (fastest-block-first, gap-free);
//   - gossip RPC (4002 etc.): transparently proxied to an active pool peer, failing over on dial error.
// If the active peer has a problem the gateway uses the next peer from the (continuously refreshed) pool.
async fn run_gateway(node_peer_file: String, push: bool, cache_coldstart: bool, n_live: usize) {
    let pool: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(read_node_peers(&node_peer_file)));
    // cached verbatim bootstrap (abci_state + EVM KVs) for cold-start without a peer state fetch
    let boot_blob: Arc<Mutex<Option<Arc<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let boot_blob_cached_at = Arc::new(AtomicU64::new(0));
    let disk_cache_path = cache_coldstart.then(|| bootstrap_cache_path(&node_peer_file));
    if let Some(path) = &disk_cache_path {
        match load_bootstrap_cache(path, unix_secs()) {
            Ok(Some((blob, cached_at))) => {
                eprintln!(
                    "[gw] bootstrap cache loaded from {} ({} MB, age={}s)",
                    path.display(),
                    blob.len() / 1_000_000,
                    unix_secs().saturating_sub(cached_at)
                );
                *boot_blob.lock().unwrap() = Some(Arc::new(blob));
                boot_blob_cached_at.store(cached_at, Ordering::Release);
            }
            Ok(None) => {
                eprintln!("[gw] no disk bootstrap cache at {}", path.display());
            }
            Err(e) => {
                eprintln!(
                    "[gw] disk bootstrap cache ignored at {}: {e}",
                    path.display()
                );
            }
        }
    }
    let rr = Arc::new(AtomicUsize::new(0)); // round-robin so successive bootstraps pick fresh peers
                                            // per-downstream-node session state (active peer pin, replay cooldown, live floor), keyed by
                                            // source IP. rr rotation naturally spreads different nodes across upstream peers.
    let nodes = Arc::new(NodeRegistry::default());
    // Single-flight for the transparent-fallback cache tap: concurrent fallback bootstraps must not
    // stack multiple multi-GB tap buffers or race the disk cache file; the loser relays untapped.
    let capture_tap = Arc::new(tokio::sync::Semaphore::new(1));
    eprintln!(
        "[gw] full P2P gateway: pool from {} ({} peers); bootstrap={}; live={}; live-upstream budget={}",
        node_peer_file,
        pool.lock().unwrap().len(),
        if cache_coldstart {
            "cache-coldstart(+transparent fallback)"
        } else {
            "transparent"
        },
        if push {
            "block-push(transparent active backbone + shadow-disabled + active-failover)"
        } else {
            "transparent splice"
        },
        n_live
    );

    // pool refresher: re-read the node's peer file (peerd keeps it fresh)
    {
        let pool = pool.clone();
        let path = node_peer_file.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let p = read_node_peers(&path);
                *pool.lock().unwrap() = p;
            }
        });
    }

    if cache_coldstart {
        // bootstrap capture: fetch + cache the full bootstrap (abci_state + EVM KVs) verbatim from a
        // serving state-server, so a node can cold-start from cache (no per-IP state rate-limit). Refresh.
        {
            let boot_blob = boot_blob.clone();
            let boot_blob_cached_at = boot_blob_cached_at.clone();
            let pool = pool.clone();
            let disk_cache_path = disk_cache_path.clone();
            let capture_peer_file = node_peer_file.clone();
            tokio::spawn(async move {
                // abci_state is rate-limited per (source-IP, peer) pair, so re-hitting the same
                // top-ranked peers exhausts them. Until we have a cache, start from peerd's best
                // ranked peers so cold cache can fill quickly. Once cached, refresh before the
                // freshness window expires and seed the refresh rotation from the clock so a fresh
                // gateway doesn't always begin at the same rank.
                let mut cycle = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as usize)
                    .unwrap_or(0);
                let mut fails = 0u32;
                loop {
                    // Until we have ANY cached blob the node can't cold-start, so the initial
                    // capture is urgent: search a wide window and retry fast. Once cached, the old
                    // blob stays valid, so refreshes use a small rotated window before the replay
                    // freshness window expires (gentle on peers, lets per-peer abci_state limits
                    // recover).
                    let have_cache = boot_blob.lock().unwrap().is_some();
                    let now = unix_secs();
                    let cached_at = boot_blob_cached_at.load(Ordering::Acquire);
                    let have_fresh_cache = have_cache && is_bootstrap_cache_fresh(now, cached_at);
                    if have_fresh_cache {
                        let next_refresh = cached_at.saturating_add(CACHE_REFRESH_SECS);
                        if now < next_refresh {
                            tokio::time::sleep(Duration::from_secs(
                                next_refresh
                                    .saturating_sub(now)
                                    .min(CACHE_REFRESH_RETRY_SECS),
                            ))
                            .await;
                            continue;
                        }
                    }
                    let window = if have_fresh_cache { 24 } else { 64 };
                    let peers: Vec<String> = {
                        let live = pool.lock().unwrap().clone();
                        let full =
                            bootstrap_capture_peers(&capture_peer_file, live, !have_fresh_cache);
                        let n = full.len();
                        if n == 0 {
                            Vec::new()
                        } else {
                            let take = window.min(n);
                            let rotate_window = have_fresh_cache || fails > 0;
                            let off = if rotate_window {
                                cycle.wrapping_mul(take) % n
                            } else {
                                0
                            };
                            (0..take).map(|k| full[(off + k) % n].clone()).collect()
                        }
                    };
                    cycle = cycle.wrapping_add(1);
                    // Refresh is not urgent once a usable cache exists; keep it single-flight so
                    // periodic refreshes do not stack multiple multi-GB bootstrap buffers.
                    let max_inflight = if have_fresh_cache { 1 } else { 3 };
                    let got = if let Some((blob, ip)) =
                        capture_bootstrap_raced(peers, max_inflight, Duration::from_secs(40)).await
                    {
                        eprintln!(
                            "[gw] bootstrap captured: {} MB via {} (raced)",
                            blob.len() / 1_000_000,
                            ip
                        );
                        let blob = Arc::new(blob);
                        *boot_blob.lock().unwrap() = Some(blob.clone());
                        boot_blob_cached_at.store(unix_secs(), Ordering::Release);
                        if let Some(path) = disk_cache_path.clone() {
                            tokio::task::spawn_blocking(move || {
                                match persist_bootstrap_cache(&path, blob.as_slice()) {
                                    Ok(()) => {
                                        eprintln!(
                                            "[gw] bootstrap cache persisted to {}",
                                            path.display()
                                        );
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "[gw] bootstrap cache persist failed at {}: {e}",
                                            path.display()
                                        );
                                    }
                                }
                            });
                        }
                        true
                    } else {
                        false
                    };
                    let delay = if got {
                        fails = 0;
                        CACHE_REFRESH_SECS
                    } else if have_fresh_cache {
                        CACHE_REFRESH_RETRY_SECS
                    } else {
                        // A failed initial capture may already have consumed quota/bandwidth across
                        // a full rotated window. Back off in minutes, not seconds, so --cache does
                        // not keep burning the same source-IP quotas and hundreds of GB.
                        fails = (fails + 1).min(4);
                        match fails {
                            1 => 60,
                            2 => 120,
                            3 => 300,
                            _ => 600,
                        }
                    };
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                }
            });
        }
    }

    // listeners on 4000-4010. The node connects ONLY here; the gateway transparently relays each
    // connection to a real upstream peer. For the heavy channel (4001) bootstrap (send_abci:true)
    // it picks a peer that is ACTUALLY serving the abci_state right now (peeks the first frame:
    // a serving peer sends the >4MB state, a rate-limited one sends a tiny status frame -> try next),
    // so the node streams the complete abci_state + EVM KVs + live blocks straight from a fresh peer.
    // Round-robin start means node/gateway restarts rotate to a different fresh state-server (no
    // per-peer abci_state rate-limit). The whole pool behind the gateway is the failover set.
    let mut handles = Vec::new();
    for port in 4000u16..=4010 {
        let pool = pool.clone();
        let rr = rr.clone();
        let nodes = nodes.clone();
        let boot_blob = boot_blob.clone();
        let boot_blob_cached_at = boot_blob_cached_at.clone();
        let disk_cache_path = disk_cache_path.clone();
        let capture_tap = capture_tap.clone();
        handles.push(tokio::spawn(async move {
            let l = match TcpListener::bind(("0.0.0.0", port)).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[gw] bind :{port}: {e}");
                    return;
                }
            };
            loop {
                let (down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let pool = pool.clone();
                let rr = rr.clone();
                let nodes = nodes.clone();
                let boot_blob = boot_blob.clone();
                let boot_blob_cached_at = boot_blob_cached_at.clone();
                let disk_cache_path = disk_cache_path.clone();
                let capture_tap = capture_tap.clone();
                tokio::spawn(async move {
                    let mut down = down;
                    down.set_nodelay(true).ok();
                    let (node, _conn_guard) = nodes.get_or_insert(addr.ip(), unix_secs());
                    let peers = pool.lock().unwrap().clone();
                    let n = peers.len();
                    if n == 0 {
                        return;
                    }
                    if port == 4001 {
                        let mut greet = [0u8; 8];
                        if !matches!(
                            timeout(Duration::from_secs(20), down.read_exact(&mut greet)).await,
                            Ok(Ok(_))
                        ) {
                            return;
                        }
                        if greet[5] == 1 {
                            // BOOTSTRAP. With --cache + a captured snapshot, serve it FROM CACHE (no
                            // peer state fetch, no rate-limit); then stream live from a pool peer. The
                            // node catches up via the client-block RPC (4002, fetch-forwarded). Default
                            // (no --cache) falls through to a transparent relay so the node keeps a real
                            // peer relationship (robust 4002 splice, no cache-capture dependency).
                            if cache_coldstart {
                                let now = unix_secs();
                                let last = node.last_cache_replay_secs.load(Ordering::Acquire);
                                let has_active_session = node.has_active_session();
                                let cached_at = boot_blob_cached_at.load(Ordering::Acquire);
                                let blob = boot_blob.lock().unwrap().clone();
                                let blob = if let Some(blob) = blob {
                                    if should_replay_cache(now, last, has_active_session, cached_at) {
                                        Some(blob)
                                    } else if has_active_session {
                                        eprintln!(
                                            "[gw] [{}] cache replay suppressed: this node has an active session; using transparent fallback",
                                            node.ip
                                        );
                                        None
                                    } else if !is_bootstrap_cache_fresh(now, cached_at) {
                                        eprintln!(
                                            "[gw] [{}] cache replay suppressed: cache age {}s exceeds {}s; using transparent fallback",
                                            node.ip,
                                            now.saturating_sub(cached_at),
                                            MAX_BOOTSTRAP_CACHE_AGE_SECS
                                        );
                                        None
                                    } else {
                                        eprintln!(
                                            "[gw] [{}] cache replay suppressed: replayed {}s ago; using transparent fallback",
                                            node.ip,
                                            now.saturating_sub(last)
                                        );
                                        None
                                    }
                                } else {
                                    None
                                };
                                if let Some(blob) = blob {
                                    eprintln!(
                                        "[gw] [{}] node cold-start FROM CACHE ({} MB), no peer state fetch",
                                        node.ip,
                                        blob.len() / 1_000_000
                                    );
                                    node.last_cache_replay_secs.store(now, Ordering::Release);
                                    // Chunked on purpose: one write_all over the whole >4GB blob
                                    // wedges permanently at ~2^31 bytes when the reader drains
                                    // faster than the send-buffer copy loop (a single send()
                                    // syscall then never returns to userspace before its byte
                                    // count overflows). Bounding each write keeps every syscall
                                    // small; a real node reads too slowly to trigger it, a
                                    // cache-draining fakenode reliably does.
                                    let mut replay_err = false;
                                    for chunk in blob.chunks(8 * 1024 * 1024) {
                                        if down.write_all(chunk).await.is_err() {
                                            replay_err = true;
                                            break;
                                        }
                                    }
                                    if replay_err {
                                        eprintln!(
                                            "[gw] [{}] cache replay write failed; node will re-bootstrap",
                                            node.ip
                                        );
                                        // reset the cooldown only if our own stamp is still
                                        // current — never clobber a concurrent newer replay's
                                        let _ = node.last_cache_replay_secs.compare_exchange(
                                            now,
                                            0,
                                            Ordering::AcqRel,
                                            Ordering::Acquire,
                                        );
                                        return;
                                    }
                                    node.last_cache_replay_secs
                                        .store(unix_secs(), Ordering::Release);
                                    let blob_for_scan = blob.clone();
                                    let cache_floor = tokio::task::spawn_blocking(move || {
                                        max_block_round_in_frames(&blob_for_scan)
                                    })
                                    .await
                                    .ok()
                                    .flatten()
                                    .unwrap_or(0);
                                    if cache_floor != 0
                                        && raise_round_floor(&node.live_floor, cache_floor)
                                    {
                                        eprintln!(
                                            "[gw] [{}] cache replay live floor round={cache_floor}",
                                            node.ip
                                        );
                                    }
                                    let start = rr.fetch_add(1, Ordering::Relaxed);
                                    for k in 0..n {
                                        let ip = peers[(start + k) % n].clone();
                                        if let Ok(Ok(mut upc)) = timeout(
                                            Duration::from_secs(5),
                                            TcpStream::connect(&format!("{}:4001", ip)),
                                        )
                                        .await
                                        {
                                            upc.set_nodelay(true).ok();
                                            if upc.write_all(&GREET_FALSE).await.is_ok() {
                                                let peer_greeting =
                                                    match read_live_greeting(&mut upc).await {
                                                        Ok(g) => g,
                                                        Err(_) => continue,
                                                    };
                                                node.pin_active(&ip);
                                                let mut hosts = vec![ip.clone()];
                                                if push {
                                                    for p in peers.iter() {
                                                        if hosts.len() >= n_live {
                                                            break;
                                                        }
                                                        if *p != ip {
                                                            hosts.push(p.clone());
                                                        }
                                                    }
                                                }
                                                serve_push(
                                                    down,
                                                    upc,
                                                    PushConfig {
                                                        hosts: Arc::new(hosts),
                                                        active_idx: 0,
                                                        port: 4001,
                                                        prefetched_greeting: Some(peer_greeting),
                                                        initial_last_forwarded: cache_floor,
                                                        live_floor: node.live_floor.clone(),
                                                    },
                                                )
                                                .await;
                                                clear_active_if_current(&node.active, &ip);
                                                return;
                                            }
                                        }
                                    }
                                    return;
                                }
                            }
                            // fallback: transparently relay a peer that is serving the abci_state now
                            let start = rr.fetch_add(1, Ordering::Relaxed);
                            let Some(selected) =
                                select_fast_bootstrap_peer(&peers, start, greet).await
                            else {
                                eprintln!(
                                    "[gw] [{}] no peer serving abci_state right now",
                                    node.ip
                                );
                                return;
                            };
                            let BootstrapPeerSelection {
                                upc,
                                hdr,
                                payload_prefix,
                                ip,
                                abci_len,
                            } = selected;
                            eprintln!(
                                "[gw] [{}] node bootstrap via {ip} (abci_state {} bytes, prefetched {} MB); active set",
                                node.ip,
                                abci_len,
                                payload_prefix.len() / 1_000_000
                            );
                            node.pin_active(&ip);
                            let tap_permit = if cache_coldstart {
                                capture_tap.clone().try_acquire_owned().ok()
                            } else {
                                None
                            };
                            if let Some(permit) = tap_permit {
                                splice_bootstrap_capture(
                                    down,
                                    upc,
                                    hdr,
                                    payload_prefix,
                                    CacheSink {
                                        boot_blob: boot_blob.clone(),
                                        boot_blob_cached_at: boot_blob_cached_at.clone(),
                                        disk_cache_path: disk_cache_path.clone(),
                                    },
                                    permit,
                                )
                                .await;
                            } else {
                                if cache_coldstart {
                                    eprintln!(
                                        "[gw] [{}] bootstrap capture already in flight; relaying without tap",
                                        node.ip
                                    );
                                }
                                if down.write_all(&hdr).await.is_err() {
                                    return;
                                }
                                if down.write_all(&payload_prefix).await.is_err() {
                                    return;
                                }
                                splice(down, upc).await;
                            }
                            clear_active_if_current(&node.active, &ip);
                        } else {
                            // live/resume channel: choose a reachable peer, make it THIS node's active
                            // session peer (so its client-block RPC on 4002 hits the same peer), forward
                            // the greeting, and relay. This is what (re)establishes `active`.
                            let start = rr.fetch_add(1, Ordering::Relaxed);
                            for k in 0..n {
                                let ip = peers[(start + k) % n].clone();
                                let up = format!("{}:4001", ip);
                                let mut upc = match timeout(
                                    Duration::from_secs(5),
                                    TcpStream::connect(&up),
                                )
                                .await
                                {
                                    Ok(Ok(c)) => c,
                                    _ => continue,
                                };
                                upc.set_nodelay(true).ok();
                                if upc.write_all(&greet).await.is_err() {
                                    continue;
                                }
                                if push {
                                    let peer_greeting = match read_live_greeting(&mut upc).await {
                                        Ok(g) => g,
                                        Err(_) => continue,
                                    };
                                    node.pin_active(&ip);
                                    // active stays transparent and owns the peer relationship used by
                                    // 4002. Shadow multi-source injection is disabled for correctness.
                                    let mut hosts = vec![ip.clone()];
                                    for p in peers.iter() {
                                        if hosts.len() >= n_live {
                                            break;
                                        }
                                        if *p != ip {
                                            hosts.push(p.clone());
                                        }
                                    }
                                    serve_push(
                                        down,
                                        upc,
                                        PushConfig {
                                            hosts: Arc::new(hosts),
                                            active_idx: 0,
                                            port: 4001,
                                            prefetched_greeting: Some(peer_greeting),
                                            initial_last_forwarded: 0,
                                            live_floor: node.live_floor.clone(),
                                        },
                                    )
                                    .await;
                                    clear_active_if_current(&node.active, &ip);
                                } else {
                                    node.pin_active(&ip);
                                    splice(down, upc).await;
                                    clear_active_if_current(&node.active, &ip);
                                }
                                break;
                            }
                        }
                    } else if should_fetch_forward_client_blocks(port) {
                        // 4002 client-block RPC is request/response. Always fetch-forward it through
                        // the current active peer so we can detect "Peer-only request" and wait for a
                        // fresh active instead of leaking the rejection to the node.
                        serve_client_blocks(down, node.clone(), peers, rr).await;
                    } else {
                        // Other gossip channels stay transparently spliced to the node's active peer.
                        let Some(upc) = dial_active(&node, &peers, &rr, port).await else {
                            return;
                        };
                        splice(down, upc).await;
                    }
                });
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

// Pick the next non-bad upstream after `cur` (skips peers in cooldown). None => no healthy alternative.
fn pick_next(cur: usize, bad_until: &[AtomicU64], now_ms: u64, n: usize) -> Option<usize> {
    for off in 1..n {
        let j = (cur + off) % n;
        if bad_until[j].load(Ordering::Relaxed) <= now_ms {
            return Some(j);
        }
    }
    None
}

// Failover transparent proxy. Default (push=false): the local node dials the gateway (4000-4010,
// outbound) and the gateway transparently proxies to the ACTIVE upstream peer; a health monitor
// rotates to the next healthy peer on stall/dial-failure ("选择其他peer替代").
// With push=true: on the heavy block channel (4001) the gateway ALSO connects to all other peers and
// merge-pushes their live blocks (round-dedup, fastest-first) for lower block-reception latency; the
// active peer connection stays transparent for the node's outbound + control/state/RPC frames.
// (Inbound push to the node is not implemented — verified: HL nodes only ingest from peers they dial.)
async fn run_proxy(upstreams: Vec<String>, push: bool) {
    let hosts: Vec<String> = upstreams
        .iter()
        .map(|u| match u.rsplit_once(':') {
            Some((h, _)) => h.to_string(),
            None => u.clone(),
        })
        .collect();
    if hosts.is_empty() {
        eprintln!("[proxy] no upstreams given");
        return;
    }
    let hosts = Arc::new(hosts);
    let n = hosts.len();
    let active = Arc::new(AtomicUsize::new(0));
    let generation = Arc::new(AtomicU64::new(0));
    let base = std::time::Instant::now();
    let last_data_ms = Arc::new(AtomicU64::new(0));
    let bad_until: Arc<Vec<AtomicU64>> = Arc::new((0..n).map(|_| AtomicU64::new(0)).collect());
    eprintln!(
        "[proxy] upstreams={:?}, active={}, push={}",
        hosts, hosts[0], push
    );

    {
        let hosts = hosts.clone();
        let active = active.clone();
        let generation = generation.clone();
        let last_data_ms = last_data_ms.clone();
        let bad_until = bad_until.clone();
        tokio::spawn(async move {
            const STALL_MS: u64 = 20000;
            const COOLDOWN_MS: u64 = 30000;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = base.elapsed().as_millis() as u64;
                let ld = last_data_ms.load(Ordering::Relaxed);
                if ld != 0 && now.saturating_sub(ld) > STALL_MS {
                    let old = active.load(Ordering::Relaxed);
                    bad_until[old].store(now + COOLDOWN_MS, Ordering::Relaxed);
                    if let Some(j) = pick_next(old, &bad_until, now, n) {
                        active.store(j, Ordering::Relaxed);
                        generation.fetch_add(1, Ordering::Relaxed);
                        last_data_ms.store(now, Ordering::Relaxed);
                        eprintln!(
                            "[proxy] STALL {}ms on {} -> failover -> {}",
                            now - ld,
                            hosts[old],
                            hosts[j]
                        );
                    } else {
                        last_data_ms.store(now, Ordering::Relaxed); // no healthy alternative; keep current
                    }
                }
            }
        });
    }

    let mut handles = Vec::new();
    for p in 4000u16..=4010 {
        let hosts = hosts.clone();
        let active = active.clone();
        let generation = generation.clone();
        let last_data_ms = last_data_ms.clone();
        let bad_until = bad_until.clone();
        handles.push(tokio::spawn(async move {
            let l = match TcpListener::bind(("0.0.0.0", p)).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[proxy] bind :{p}: {e}");
                    return;
                }
            };
            loop {
                let (down, _addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let hosts = hosts.clone();
                let active = active.clone();
                let generation = generation.clone();
                let last_data_ms = last_data_ms.clone();
                let bad_until = bad_until.clone();
                tokio::spawn(async move {
                    let cur_gen = generation.load(Ordering::Relaxed);
                    let idx = active.load(Ordering::Relaxed);
                    let up = format!("{}:{}", hosts[idx], p);
                    let upc = match timeout(Duration::from_secs(4), TcpStream::connect(&up)).await {
                        Ok(Ok(s)) => s,
                        _ => {
                            let now = base.elapsed().as_millis() as u64;
                            bad_until[idx].store(now + 30000, Ordering::Relaxed);
                            if let Some(j) = pick_next(idx, &bad_until, now, hosts.len()) {
                                active.store(j, Ordering::Relaxed);
                                generation.fetch_add(1, Ordering::Relaxed);
                                eprintln!(
                                    "[proxy] dial {} failed -> failover -> {}",
                                    hosts[idx], hosts[j]
                                );
                            } else {
                                eprintln!(
                                    "[proxy] dial {} failed, no healthy alternative",
                                    hosts[idx]
                                );
                            }
                            return;
                        }
                    };
                    down.set_nodelay(true).ok();
                    upc.set_nodelay(true).ok();
                    if push && p == 4001 {
                        // serve_push assumes the node's greeting has ALREADY been forwarded to the
                        // active peer (its first act is reading the peer's greeting reply, and the
                        // node->peer copy task only starts after that). Forward it here, like
                        // run_gateway does — otherwise peer waits for the greeting, gateway waits
                        // for the peer's reply, and the handshake deadlocks (review finding 1).
                        let mut down = down;
                        let mut upc = upc;
                        let mut greet = [0u8; 8];
                        if !matches!(
                            timeout(Duration::from_secs(20), down.read_exact(&mut greet)).await,
                            Ok(Ok(_))
                        ) {
                            return;
                        }
                        if upc.write_all(&greet).await.is_err() {
                            return;
                        }
                        if greet[5] == 1 {
                            // bootstrap (send_abci:true): transparent splice. Multi-source injection
                            // can't help mid-bootstrap (the abci_state/EVM-KVs boundaries aren't
                            // detectable), and serve_push's greeting gate would reject the >4MB
                            // state frame the peer sends first.
                            splice(down, upc).await;
                        } else {
                            // live channel: merge blocks from active + all other peers (round-dedup,
                            // fastest-first); active stays transparent for node->peer + control/RPC.
                            serve_push(
                                down,
                                upc,
                                PushConfig {
                                    hosts: hosts.clone(),
                                    active_idx: idx,
                                    port: p,
                                    prefetched_greeting: None,
                                    initial_last_forwarded: 0,
                                    live_floor: Arc::new(AtomicU32::new(0)),
                                },
                            )
                            .await;
                        }
                        return;
                    }
                    let (mut dr, mut dw) = down.into_split();
                    let (mut ur, mut uw) = upc.into_split();
                    let ld = last_data_ms.clone();
                    let up2 = tokio::spawn(async move {
                        let mut buf = vec![0u8; 262144];
                        loop {
                            match ur.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(nb) => {
                                    ld.store(base.elapsed().as_millis() as u64, Ordering::Relaxed);
                                    if dw.write_all(&buf[..nb]).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    });
                    let n2u = tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut dr, &mut uw).await;
                    });
                    loop {
                        if generation.load(Ordering::Relaxed) != cur_gen {
                            break;
                        }
                        if up2.is_finished() || n2u.is_finished() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                    up2.abort();
                    n2u.abort();
                });
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

// Block-push serving (push mode, heavy channel): the active peer connection is transparent for the
// node's outbound and the active peer's control / abci_state / RPC frames. Shadow peer injection is
// disabled by default: round-only dedup cannot prove proposer/fork correctness, and a wrong shadow
// block can poison the node even when the active transparent peer is healthy.
const PUSH_MERGE_QUEUE_FRAMES: usize = 256;
const ENABLE_SHADOW_PUSH: bool = false;

struct PushConfig {
    hosts: Arc<Vec<String>>,
    active_idx: usize,
    port: u16,
    prefetched_greeting: Option<Vec<u8>>,
    initial_last_forwarded: u32,
    live_floor: Arc<AtomicU32>,
}

async fn serve_push(node: TcpStream, active_conn: TcpStream, cfg: PushConfig) {
    let forward_gate = Arc::new(tokio::sync::Mutex::new(
        RoundForwardGate::with_last_forwarded(16_384, cfg.initial_last_forwarded),
    ));
    // Each queued item is a complete block/control frame. Keep this small enough that a slow node
    // writer backpressures upstream readers instead of allowing multi-GB queued Vecs.
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(PUSH_MERGE_QUEUE_FRAMES);
    let (node_r, mut node_w) = node.into_split();
    let (mut act_r, mut act_w) = active_conn.into_split();
    // The node's first read on 4001 is the peer's greeting ("abci_stream recv greeting", max 1000
    // bytes). Forward the active peer's greeting frame to the node BEFORE starting the shadow
    // injectors: they all share node_w, so a shadow peer's first live block can otherwise reach the
    // node ahead of the greeting, and the node reads the block's length as the greeting length and
    // bails ("tcp read bytes over limit").
    if let Some(greet) = cfg.prefetched_greeting {
        if node_w.write_all(&greet).await.is_err() {
            return;
        }
    } else {
        let mut hdr = [0u8; 5];
        if act_r.read_exact(&mut hdr).await.is_err() {
            return;
        }
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if len > 1000 {
            return; // first frame from a live (send_abci:false) peer must be the small greeting
        }
        let mut g = vec![0u8; len];
        if act_r.read_exact(&mut g).await.is_err() {
            return;
        }
        let mut greet = hdr.to_vec();
        greet.extend_from_slice(&g);
        if node_w.write_all(&greet).await.is_err() {
            return;
        }
    }
    // node -> active peer (transparent: RPC requests + acks)
    let up = {
        let mut node_r = node_r;
        tokio::spawn(async move {
            let _ = tokio::io::copy(&mut node_r, &mut act_w).await;
        })
    };
    // active peer -> node: the transparent backbone (forwards non-block frames + blocks). When this
    // task ends the active peer's stream broke, so we tear the whole session down and let the node
    // reconnect — the gateway then pins a FRESH active (round-robin). This is the proxy-style failover;
    // shadow sources remain disabled unless validation is added.
    let mut active_task = {
        let tx = tx.clone();
        let forward_gate = forward_gate.clone();
        let live_floor = cfg.live_floor.clone();
        tokio::spawn(async move {
            let _ = pump_merge(act_r, tx, forward_gate, live_floor, true).await;
        })
    };
    // every other peer -> node: live blocks only, deduped. Keep this off until shadow blocks are
    // validated beyond round number.
    let mut shadows = Vec::new();
    if ENABLE_SHADOW_PUSH {
        for (i, h) in cfg.hosts.iter().enumerate() {
            if i == cfg.active_idx {
                continue;
            }
            let target = format!("{}:{}", h, cfg.port);
            let tx = tx.clone();
            let forward_gate = forward_gate.clone();
            let live_floor = cfg.live_floor.clone();
            shadows.push(tokio::spawn(async move {
                // Reconnect with backoff: public peers drop their streams, and a shadow source that
                // exited permanently would silently degrade the merge from n_live sources to fewer.
                // Stop only when the node side is gone (the merge channel is closed).
                while !tx.is_closed() {
                    if let Ok(Ok(mut s)) =
                        timeout(Duration::from_secs(5), TcpStream::connect(&target)).await
                    {
                        s.set_nodelay(true).ok();
                        if s.write_all(&GREET_FALSE).await.is_ok() {
                            let (r, _w) = s.into_split();
                            let _ = pump_merge(
                                r,
                                tx.clone(),
                                forward_gate.clone(),
                                live_floor.clone(),
                                false,
                            )
                            .await;
                        }
                    }
                    if tx.is_closed() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }));
        }
    }
    drop(tx);
    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(buf) => {
                    if node_w.write_all(&buf).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
            // active (backbone) peer died -> tear down; the node reconnects to a fresh active
            _ = &mut active_task => break,
        }
    }
    up.abort();
    active_task.abort();
    for s in shadows {
        s.abort();
    }
}

// Frame reader for serve_push: forward block frames (type=1 with a round) deduped; if forward_nonblock
// (the active peer only) also forward control / abci_state / RPC frames as-is.
async fn pump_merge(
    mut r: tokio::net::tcp::OwnedReadHalf,
    tx: mpsc::Sender<Vec<u8>>,
    forward_gate: Arc<tokio::sync::Mutex<RoundForwardGate>>,
    live_floor: Arc<AtomicU32>,
    forward_nonblock: bool,
) -> std::io::Result<()> {
    // Live blocks arrive continuously (~4-15/s on mainnet), so >30s of silence means a stalled
    // connection. Without this idle timeout a silently-stalled-but-open ACTIVE peer would hang
    // the backbone forever (teardown only fired on EOF/RST); erroring out here makes serve_push
    // tear the session down so the node reconnects to a fresh active, and makes a stalled shadow
    // fall into its reconnect loop.
    const IDLE: Duration = Duration::from_secs(30);
    loop {
        let mut hdr = [0u8; 5];
        match timeout(IDLE, r.read_exact(&mut hdr)).await {
            Ok(res) => res?,
            Err(_) => return Err(std::io::Error::other("idle timeout")),
        };
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        if len > 60_000_000 {
            // Legit frames on a live (send_abci:false) channel are <=~1MB (greeting + blocks).
            // From the ACTIVE peer an oversized length is a broken/hostile stream — error out so
            // the session tears down (never allocate a peer-controlled multi-GB buffer). From a
            // shadow, drain to stay frame-aligned and keep the accelerator alive.
            if forward_nonblock {
                return Err(std::io::Error::other("oversized frame from active peer"));
            }
            let mut rem = len;
            let mut buf = vec![0u8; 65536];
            while rem > 0 {
                let nb = match timeout(IDLE, r.read(&mut buf[..rem.min(65536)])).await {
                    Ok(res) => res?,
                    Err(_) => return Err(std::io::Error::other("idle timeout (drain)")),
                };
                if nb == 0 {
                    return Ok(());
                }
                rem -= nb;
            }
            continue;
        }
        let mut payload = vec![0u8; len];
        match timeout(IDLE, r.read_exact(&mut payload)).await {
            Ok(res) => res?,
            Err(_) => return Err(std::io::Error::other("idle timeout (payload)")),
        };
        if typ == 1 {
            if let Some(rnd) = block_round(&payload) {
                if rnd <= live_floor.load(Ordering::Acquire) {
                    continue;
                }
                let mut gate = forward_gate.lock().await;
                if gate.should_forward(rnd, forward_nonblock) {
                    let mut frame = hdr.to_vec();
                    frame.extend_from_slice(&payload);
                    if tx.send(frame).await.is_err() {
                        return Ok(());
                    }
                }
                continue;
            }
        }
        if forward_nonblock {
            let mut frame = hdr.to_vec();
            frame.extend_from_slice(&payload);
            if tx.send(frame).await.is_err() {
                return Ok(());
            }
        }
    }
}

// Transparent relay: local node <-> single upstream peer. Forwards the node's greeting
// (send_abci:true) so the upstream serves the full abci_state + live blocks; relays both ways.
// Used to let a fresh node fully sync THROUGH the gateway from a fast local upstream.
async fn run_relay(upstream: String) {
    // `upstream` is just the host (e.g. "172.18.0.1"); relay the whole HL port range 4000-4010,
    // each port transparently to host:<same port>. The node's bootstrap needs more than the block
    // channel: 4001 = blocks/abci heavy channel, 4002 = gossip RPC (query-height etc). Forwarding
    // only 4001 made the node's query-height RPC on 4002 time out, so it never reached bootstrap.
    let host = match upstream.rsplit_once(':') {
        Some((h, _)) => h.to_string(),
        None => upstream.clone(),
    };
    let mut handles = Vec::new();
    for p in 4000u16..=4010 {
        let host = host.clone();
        handles.push(tokio::spawn(async move {
            let l = match TcpListener::bind(("0.0.0.0", p)).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[relay] bind :{p}: {e}");
                    return;
                }
            };
            eprintln!("[relay] :{p} -> {host}:{p}");
            loop {
                let (down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let up = format!("{host}:{p}");
                tokio::spawn(async move {
                    let upc = match TcpStream::connect(&up).await {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("[relay] :{p} {addr} upstream err {e}");
                            return;
                        }
                    };
                    down.set_nodelay(true).ok();
                    upc.set_nodelay(true).ok();
                    let (mut dr, mut dw) = down.into_split();
                    let (mut ur, mut uw) = upc.into_split();
                    let h = tokio::spawn(async move {
                        let mut buf = vec![0u8; 262144];
                        let mut total = 0u64;
                        loop {
                            let n = match ur.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            if dw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                            total += n as u64;
                        }
                        if total > 1_000_000 {
                            eprintln!("[relay] :{p} {addr} up->node {:.0}MB", total as f64 / 1e6);
                        }
                    });
                    let _ = tokio::io::copy(&mut dr, &mut uw).await;
                    let _ = h.await;
                });
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

// Benchmark the hot path: lz4 decompress + round extract (block_round) + dedup, over captured blocks.
fn run_bench(dir: &str, iters: usize) {
    let payloads: Vec<Vec<u8>> = (0..12)
        .map(|i| {
            let data = std::fs::read(format!("{dir}/block{i}.bin")).expect("read block");
            lz4_flex::block::compress_prepend_size(&data)
        })
        .collect();
    // correctness: bounded block_round must equal the full-decompress oracle for every block
    let mut mism = 0;
    for (i, p) in payloads.iter().enumerate() {
        if block_round(p) != block_round_full(p) {
            eprintln!(
                "[bench] VERIFY FAIL block{i}: fast={:?} full={:?}",
                block_round(p),
                block_round_full(p)
            );
            mism += 1;
        }
    }
    eprintln!(
        "[bench] verify: {} blocks, {} mismatch vs full-decompress oracle",
        payloads.len(),
        mism
    );

    let dedup = RoundDedup::new(16_384);
    let mut forwarded = 0u64;
    let mut bytes = 0u64;
    let t0 = std::time::Instant::now();
    for i in 0..iters {
        let p = &payloads[i % payloads.len()];
        bytes += p.len() as u64;
        if let Some(r) = block_round(p) {
            let rr = r.wrapping_add(((i / payloads.len()) as u32).wrapping_mul(12));
            if dedup.is_new(rr) {
                forwarded += 1;
            }
        }
    }
    let dt = t0.elapsed().as_secs_f64();
    eprintln!(
        "[bench] {} frames in {:.3}s = {:.1} kframes/s, {:.0} MB/s (compressed in), forwarded={}",
        iters,
        dt,
        iters as f64 / dt / 1000.0,
        bytes as f64 / 1e6 / dt,
        forwarded
    );
}

// Mock upstream: replays captured blocks[start..end], lz4_flex-compressed (gateway-compatible), 3x.
async fn run_mock(bind: String, dir: String, start: usize, end: usize) {
    let mut frames: Vec<Vec<u8>> = Vec::new();
    for i in start..end {
        let data = std::fs::read(format!("{dir}/block{i}.bin")).expect("read block");
        let payload = lz4_flex::block::compress_prepend_size(&data);
        let mut f = (payload.len() as u32).to_be_bytes().to_vec();
        f.push(1u8);
        f.extend_from_slice(&payload);
        frames.push(f);
    }
    let frames = Arc::new(frames);
    let l = TcpListener::bind(&bind).await.expect("mock bind");
    eprintln!(
        "[mock] {} ready, blocks {}..{} ({} frames)",
        bind,
        start,
        end,
        frames.len()
    );
    loop {
        let (mut c, _) = match l.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let frames = frames.clone();
        tokio::spawn(async move {
            let mut g = [0u8; 8];
            let _ = c.read_exact(&mut g).await;
            for _ in 0..3 {
                for f in frames.iter() {
                    if c.write_all(f).await.is_err() {
                        return;
                    }
                }
            }
            let mut buf = [0u8; 1024];
            loop {
                match c.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    _ => {}
                }
            }
        });
    }
}

// 4002 client-block RANGE query (docs §8a): payload = [0x00 variant tag][0xfc start u32LE]
// [0xfc end u32LE], framed as [u32 BE L=11][type=0][payload]. Closed range [start, end].
fn build_client_block_range_request(start_round: u32, end_round: u32) -> Vec<u8> {
    let mut req = Vec::with_capacity(16);
    req.extend_from_slice(&11u32.to_be_bytes());
    req.push(0u8);
    req.push(0x00);
    req.push(0xfc);
    req.extend_from_slice(&start_round.to_le_bytes());
    req.push(0xfc);
    req.extend_from_slice(&end_round.to_le_bytes());
    req
}

const FAKENODE_BOOT_TIMEOUT_SECS: u64 = 600;

async fn fk_connect(gw_ip: &str, port: u16, greet: &[u8; 8]) -> Option<(TcpStream, String)> {
    let mut c = timeout(Duration::from_secs(10), TcpStream::connect((gw_ip, port)))
        .await
        .ok()?
        .ok()?;
    c.set_nodelay(true).ok();
    let src = c
        .local_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|_| "?".to_string());
    c.write_all(greet).await.ok()?;
    Some((c, src))
}

// Read one frame header before `deadline`; None on timeout/EOF/oversized length.
async fn fk_read_hdr(c: &mut TcpStream, deadline: Instant) -> Option<(usize, u8)> {
    let mut hdr = [0u8; 5];
    let left = deadline.saturating_duration_since(Instant::now());
    match timeout(left, c.read_exact(&mut hdr)).await {
        Ok(Ok(_)) => {
            let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
            (len <= 1_500_000_000).then_some((len, hdr[4]))
        }
        _ => None,
    }
}

// Discard `remaining` payload bytes in chunks before `deadline`.
async fn fk_drain(
    c: &mut TcpStream,
    mut remaining: usize,
    deadline: Instant,
    buf: &mut [u8],
) -> bool {
    while remaining > 0 {
        let take = remaining.min(buf.len());
        let left = deadline.saturating_duration_since(Instant::now());
        match timeout(left, c.read(&mut buf[..take])).await {
            Ok(Ok(n)) if n > 0 => remaining -= n,
            _ => return false,
        }
    }
    true
}

// Receive a bootstrap and count it; PASS at the same completeness bar the gateway's own cache
// uses. The gw keeps streaming live on this socket, so close as soon as the bar is met.
async fn fakenode_bootstrap(
    gw_ip: &str,
    port: u16,
    label: &str,
) -> (bool, usize, u64, f64, String) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(FAKENODE_BOOT_TIMEOUT_SECS);
    let Some((mut c, src)) = fk_connect(gw_ip, port, &GREET_TRUE).await else {
        return (false, 0, 0, 0.0, "?".to_string());
    };
    let mut buf = vec![0u8; 1_048_576];
    let (mut bytes, mut frames) = (0usize, 0u64);
    let mut pass = false;
    while let Some((len, _typ)) = fk_read_hdr(&mut c, deadline).await {
        if !fk_drain(&mut c, len, deadline, &mut buf).await {
            break;
        }
        bytes += 5 + len;
        frames += 1;
        if bytes >= MIN_COMPLETE_BOOTSTRAP_BYTES && frames >= MIN_COMPLETE_BOOTSTRAP_FRAMES {
            pass = true;
            break;
        }
        if frames % 500 == 0 {
            eprintln!(
                "[fakenode:{label}] bootstrap {} MB / {} frames",
                bytes / 1_000_000,
                frames
            );
        }
    }
    (pass, bytes, frames, started.elapsed().as_secs_f64(), src)
}

// Read the live stream for `live_secs`, parsing block rounds. This live/resume connection is
// what pins this fakenode's active peer on the gateway.
async fn fakenode_live(gw_ip: &str, port: u16, live_secs: u64) -> (bool, u64, usize, u32, String) {
    let Some((mut c, src)) = fk_connect(gw_ip, port, &GREET_FALSE).await else {
        return (false, 0, 0, 0, "?".to_string());
    };
    let deadline = Instant::now() + Duration::from_secs(live_secs);
    let mut buf = vec![0u8; 4_194_304];
    let mut frames = 0u64;
    let mut rounds: HashSet<u32> = HashSet::new();
    let mut max_round = 0u32;
    while let Some((len, typ)) = fk_read_hdr(&mut c, deadline).await {
        if typ == 1 && len <= buf.len() {
            let left = deadline.saturating_duration_since(Instant::now());
            if !matches!(
                timeout(left, c.read_exact(&mut buf[..len])).await,
                Ok(Ok(_))
            ) {
                break;
            }
            if let Some(r) = block_round(&buf[..len]) {
                rounds.insert(r);
                max_round = max_round.max(r);
            }
        } else if !fk_drain(&mut c, len, deadline, &mut buf).await {
            break;
        }
        frames += 1;
    }
    let pass = frames >= 10 && !rounds.is_empty();
    (pass, frames, rounds.len(), max_round, src)
}

// Send a real client-block range query and classify the response. Any well-framed answer proves
// the gateway fetch-forwarded to an upstream — EXCEPT a leaked "Peer-only request" rejection,
// which means the gateway routed us through a peer that doesn't know this node.
async fn fakenode_rpc(gw_ip: &str, port: u16, max_round: u32, label: &str) -> bool {
    if max_round == 0 {
        eprintln!("[fakenode:{label}] rpc needs a round from the live phase");
        return false;
    }
    let start_round = max_round.saturating_sub(1000);
    let req = build_client_block_range_request(start_round, start_round + 99);
    let Ok(Ok(mut c)) = timeout(Duration::from_secs(10), TcpStream::connect((gw_ip, port))).await
    else {
        eprintln!("[fakenode:{label}] rpc dial {gw_ip}:{port} failed");
        return false;
    };
    c.set_nodelay(true).ok();
    if c.write_all(&req).await.is_err() {
        return false;
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    let Some((len, typ)) = fk_read_hdr(&mut c, deadline).await else {
        eprintln!("[fakenode:{label}] rpc no response");
        return false;
    };
    if typ == 0 && len <= 65536 {
        let mut payload = vec![0u8; len];
        if !matches!(
            timeout(Duration::from_secs(30), c.read_exact(&mut payload)).await,
            Ok(Ok(_))
        ) {
            return false;
        }
        if response_is_peer_only(&payload) {
            eprintln!("[fakenode:{label}] rpc got leaked Peer-only rejection");
            return false;
        }
        eprintln!(
            "[fakenode:{label}] rpc response type=0 len={len}: {}",
            String::from_utf8_lossy(&payload[..len.min(120)])
        );
        return true;
    }
    let mut buf = vec![0u8; 1_048_576];
    let ok = fk_drain(&mut c, len, deadline, &mut buf).await;
    if ok {
        eprintln!("[fakenode:{label}] rpc response type={typ} len={len}");
    }
    ok
}

// testing-only synthetic downstream node: bootstrap -> live -> optional 4002 RPC against a
// gateway. Prints one machine-checkable summary line on stdout; progress goes to stderr.
// Exit code bitmask: 0 all-pass, +1 bootstrap fail, +2 live fail, +4 rpc fail, 64 usage/dial.
async fn run_fakenode(args: &[String]) -> i32 {
    let usage = || {
        eprintln!(
            "usage: hypersync fakenode <gw-ip[:base-port]> [--live-secs N] [--rpc] [--label NAME]"
        );
        64
    };
    let Some(target) = args.first().filter(|a| !a.starts_with("--")) else {
        return usage();
    };
    let (gw_ip, base_port) = match target.rsplit_once(':') {
        Some((ip, p)) => match p.parse::<u16>() {
            Ok(p) => (ip.to_string(), p),
            Err(_) => return usage(),
        },
        None => (target.clone(), 4001),
    };
    let flag_val = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let live_secs: u64 = flag_val("--live-secs")
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let label = flag_val("--label").unwrap_or_else(|| "fakenode".to_string());
    let do_rpc = args.iter().any(|a| a == "--rpc");

    let (boot_pass, boot_bytes, boot_frames, boot_secs, src) =
        fakenode_bootstrap(&gw_ip, base_port, &label).await;
    let (live_pass, live_frames, live_rounds, max_round, live_src) =
        fakenode_live(&gw_ip, base_port, live_secs).await;
    let src = if src == "?" { live_src } else { src };
    let rpc = if do_rpc {
        Some(fakenode_rpc(&gw_ip, base_port + 1, max_round, &label).await)
    } else {
        None
    };

    let mut exit = 0i32;
    if !boot_pass {
        exit |= 1;
    }
    if !live_pass {
        exit |= 2;
    }
    if rpc == Some(false) {
        exit |= 4;
    }
    let s = |b: bool| if b { "PASS" } else { "FAIL" };
    println!(
        "FAKENODE label={label} src={src} boot={} boot_bytes={boot_bytes} \
         boot_frames={boot_frames} boot_secs={boot_secs:.1} live={} \
         live_frames={live_frames} live_rounds={live_rounds} max_round={max_round} rpc={} \
         result={}",
        s(boot_pass),
        s(live_pass),
        rpc.map_or("SKIP", s),
        s(exit == 0)
    );
    exit
}

#[cfg(test)]
mod tests {
    use super::*;

    // A decompressed block whose first bytes are unique (ramp), so lz4 emits a leading literal run
    // >= 0x63 and block_round's fast path engages; the consensus round sits at offset 0x5e
    // (varint 0xfc + u32 LE), matching the real wire format.
    fn make_block(round: u32) -> Vec<u8> {
        let mut dec: Vec<u8> = (0..0x100).map(|i| i as u8).collect();
        dec[0x5e] = 0xfc;
        dec[0x5f..0x63].copy_from_slice(&round.to_le_bytes());
        lz4_flex::block::compress_prepend_size(&dec)
    }

    fn make_frame(typ: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.push(typ);
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn block_round_matches_full_decompress() {
        for round in [0u32, 1, 1000, 1_055_000_000, u32::MAX] {
            let payload = make_block(round);
            assert_eq!(block_round_full(&payload), Some(round), "full @ {round}");
            assert_eq!(block_round(&payload), Some(round), "fast @ {round}");
        }
    }

    #[test]
    fn max_block_round_scans_only_small_complete_block_frames() {
        let mut blob = Vec::new();
        blob.extend_from_slice(&make_frame(0, b"control"));
        blob.extend_from_slice(&make_frame(1, &make_block(10)));
        blob.extend_from_slice(&make_frame(1, &vec![0u8; 2_000_001]));
        blob.extend_from_slice(&make_frame(1, &make_block(12)));
        assert_eq!(max_block_round_in_frames(&blob), Some(12));

        let truncated = vec![0, 0, 0, 10, 1, 1, 2];
        assert_eq!(max_block_round_in_frames(&truncated), None);
    }

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
    fn complete_bootstrap_boundary_accepts_only_full_sized_capture() {
        assert!(!complete_bootstrap_at_frame_boundary(
            MIN_COMPLETE_BOOTSTRAP_BYTES - 1,
            MIN_COMPLETE_BOOTSTRAP_FRAMES
        ));
        assert!(complete_bootstrap_at_frame_boundary(
            MIN_COMPLETE_BOOTSTRAP_BYTES,
            MIN_COMPLETE_BOOTSTRAP_FRAMES
        ));
        assert!(!complete_bootstrap_at_frame_boundary(
            MIN_COMPLETE_BOOTSTRAP_BYTES,
            MIN_COMPLETE_BOOTSTRAP_FRAMES - 1
        ));
        assert!(complete_bootstrap_at_frame_boundary(
            MIN_COMPLETE_BOOTSTRAP_BYTES + 1024,
            MIN_COMPLETE_BOOTSTRAP_FRAMES + 1
        ));
    }

    #[test]
    fn complete_frame_count_rejects_partial_frames() {
        let mut blob = Vec::new();
        blob.extend_from_slice(&3u32.to_be_bytes());
        blob.push(1);
        blob.extend_from_slice(&[1, 2, 3]);
        blob.extend_from_slice(&0u32.to_be_bytes());
        blob.push(2);
        assert_eq!(complete_frame_count(&blob), Some(2));

        blob.push(9);
        assert_eq!(complete_frame_count(&blob), None);
    }

    #[test]
    fn cache_replay_cooldown_blocks_immediate_duplicate_replay() {
        let first = 1_000_000;
        let cached_at = first;

        assert!(cache_replay_allowed(first, 0));
        assert!(!cache_replay_allowed(
            first + CACHE_REPLAY_COOLDOWN_SECS - 1,
            first
        ));
        assert!(cache_replay_allowed(
            first + CACHE_REPLAY_COOLDOWN_SECS,
            first
        ));
        assert!(!cache_replay_allowed(first - 1, first));

        assert!(should_replay_cache(first, 0, false, cached_at));
        assert!(!should_replay_cache(first, 0, true, cached_at));
        assert!(!should_replay_cache(first + 1, first, false, cached_at));
        assert!(!should_replay_cache(
            first + MAX_BOOTSTRAP_CACHE_AGE_SECS + 1,
            0,
            false,
            cached_at
        ));
        assert!(is_bootstrap_cache_fresh(cached_at + 15 * 60, cached_at));
    }

    #[test]
    fn client_block_fetch_forward_is_always_used_for_4002() {
        assert!(should_fetch_forward_client_blocks(4002));
        assert!(!should_fetch_forward_client_blocks(4001));
        assert!(!should_fetch_forward_client_blocks(4003));
    }

    #[test]
    fn client_block_candidates_prefer_active_then_round_robin_fallbacks() {
        let rr = AtomicUsize::new(0);
        let peers = vec![
            "1.1.1.1".to_string(),
            "2.2.2.2".to_string(),
            "3.3.3.3".to_string(),
            "4.4.4.4".to_string(),
        ];

        assert_eq!(
            client_block_peer_candidates(Some("2.2.2.2".to_string()), &peers, &rr, 4),
            vec!["2.2.2.2", "1.1.1.1", "3.3.3.3", "4.4.4.4"]
        );
        assert_eq!(
            client_block_peer_candidates(None, &peers, &rr, 3),
            vec!["1.1.1.1", "2.2.2.2", "3.3.3.3"]
        );
    }

    #[test]
    fn peer_only_response_is_detected_without_parsing_rpc() {
        assert!(response_is_peer_only(br#"{"Error":"Peer-only request"}"#));
        assert!(!response_is_peer_only(br#"{"Ok":{"blocks":[]}}"#));
    }

    #[test]
    fn no_client_blocks_response_is_not_treated_as_success() {
        assert!(response_has_no_client_blocks(
            br#"{"Error":"no client blocks to serve"}"#
        ));
        assert!(!response_has_no_client_blocks(br#"{"Ok":{"blocks":[1]}}"#));
    }

    #[test]
    fn client_block_round_too_large_response_is_not_treated_as_success() {
        assert!(response_has_client_block_round_too_large(
            br#"{"Error":"client block round too large: 1354298843 > 769127346"}"#
        ));
        assert!(!response_has_client_block_round_too_large(
            br#"{"Ok":{"blocks":[1]}}"#
        ));
    }

    #[test]
    fn client_block_round_too_small_response_is_not_treated_as_success() {
        assert!(response_has_client_block_round_too_small(
            br#"{"Error":"client block round too small: 1354354141 <= 1354362691"}"#
        ));
        assert!(!response_has_client_block_round_too_small(
            br#"{"Ok":{"blocks":[1]}}"#
        ));
    }

    #[test]
    fn raise_round_floor_is_monotonic() {
        let floor = Arc::new(AtomicU32::new(10));

        assert!(!raise_round_floor(&floor, 9));
        assert_eq!(floor.load(Ordering::Acquire), 10);
        assert!(raise_round_floor(&floor, 11));
        assert_eq!(floor.load(Ordering::Acquire), 11);
    }

    #[test]
    fn clear_active_only_clears_matching_peer() {
        let active = Arc::new(Mutex::new(Some("1.1.1.1".to_string())));

        clear_active_if_current(&active, "2.2.2.2");
        assert_eq!(active.lock().unwrap().as_deref(), Some("1.1.1.1"));

        clear_active_if_current(&active, "1.1.1.1");
        assert!(active.lock().unwrap().is_none());
    }

    #[test]
    fn node_state_evict_keys_prunes_ttl_then_oldest() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let now = 10_000u64;

        // under cap, all fresh -> nothing evicted
        let entries = vec![
            (ip("10.0.0.1"), now - 5, false),
            (ip("10.0.0.2"), now, false),
        ];
        assert!(node_state_evict_keys(&entries, now, 4, 3600).is_empty());

        // idle past ttl evicted regardless of cap
        let entries = vec![
            (ip("10.0.0.1"), now - 3601, false),
            (ip("10.0.0.2"), now, false),
        ];
        assert_eq!(
            node_state_evict_keys(&entries, now, 4, 3600),
            vec![ip("10.0.0.1")]
        );

        // at cap with all fresh -> exactly the oldest-last_seen evicted to fit the insert
        let entries = vec![
            (ip("10.0.0.1"), now - 30, false),
            (ip("10.0.0.2"), now - 10, false),
            (ip("10.0.0.3"), now - 20, false),
        ];
        assert_eq!(
            node_state_evict_keys(&entries, now, 3, 3600),
            vec![ip("10.0.0.1")]
        );

        // last_seen tie -> deterministic by ip
        let entries = vec![(ip("10.0.0.9"), now, false), (ip("10.0.0.8"), now, false)];
        assert_eq!(
            node_state_evict_keys(&entries, now, 2, 3600),
            vec![ip("10.0.0.8")]
        );

        // a node with open connections is never evicted: not by ttl (its long-lived live stream
        // opens no new connections for hours) and not by cap pressure
        let entries = vec![
            (ip("10.0.0.1"), now - 7200, true),
            (ip("10.0.0.2"), now - 30, false),
        ];
        assert_eq!(
            node_state_evict_keys(&entries, now, 2, 3600),
            vec![ip("10.0.0.2")]
        );

        // all held and over cap -> nothing evictable; the insert may exceed cap (bounded by the
        // number of concurrently open connections)
        let entries = vec![(ip("10.0.0.1"), now, true), (ip("10.0.0.2"), now, true)];
        assert!(node_state_evict_keys(&entries, now, 2, 3600).is_empty());
    }

    #[test]
    fn node_registry_reuses_state_across_reconnects() {
        let ip_a: IpAddr = "10.0.0.1".parse().unwrap();
        let ip_b: IpAddr = "10.0.0.2".parse().unwrap();
        let reg = NodeRegistry::default();
        // guard Drop stamps wall-clock last_seen, so anchor test times to the wall clock
        let t0 = unix_secs();

        let (a1, a1_guard) = reg.get_or_insert(ip_a, t0);
        *a1.active.lock().unwrap() = Some("1.1.1.1".to_string());

        // same ip reconnecting -> same state (active survives), last_seen bumped
        let (a2, a2_guard) = reg.get_or_insert(ip_a, t0 + 10);
        assert!(Arc::ptr_eq(&a1, &a2));
        assert_eq!(a2.active.lock().unwrap().as_deref(), Some("1.1.1.1"));
        assert_eq!(a2.last_seen_secs.load(Ordering::Acquire), t0 + 10);
        assert_eq!(a2.open_conns.load(Ordering::Acquire), 2);
        drop(a2_guard);
        assert_eq!(a1.open_conns.load(Ordering::Acquire), 1);

        // different ip -> fresh state
        let (b, b_guard) = reg.get_or_insert(ip_b, t0);
        assert!(!Arc::ptr_eq(&a1, &b));
        assert!(b.active.lock().unwrap().is_none());
        drop(b_guard); // stamps last_seen ~t0; ip_b now idle with no open connections

        // ip_a still has an open connection (a1_guard) -> survives TTL eviction; ip_b doesn't
        let later = t0 + NODE_STATE_TTL_SECS + 60;
        let ip_c: IpAddr = "10.0.0.3".parse().unwrap();
        let (_c, _c_guard) = reg.get_or_insert(ip_c, later);
        assert!(reg.nodes.lock().unwrap().contains_key(&ip_a));
        assert!(!reg.nodes.lock().unwrap().contains_key(&ip_b));
        assert!(reg.nodes.lock().unwrap().contains_key(&ip_c));
        drop(a1_guard);
    }

    #[test]
    fn client_block_range_request_matches_captured_frame() {
        // captured example from docs/hl-p2p-protocol.md §8a:
        // 00 00 00 0b 00 | 00 fc 8e 26 81 50 fc f1 26 81 50
        let req = build_client_block_range_request(0x5081268e, 0x508126f1);
        assert_eq!(
            req,
            vec![
                0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0xfc, 0x8e, 0x26, 0x81, 0x50, 0xfc, 0xf1, 0x26,
                0x81, 0x50
            ]
        );
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
            MIN_LIVE_SERVERS_TO_OVERWRITE
        ));
        assert!(!should_keep_previous_peer_pool(
            0,
            MIN_LIVE_SERVERS_TO_OVERWRITE
        ));
        assert!(!should_keep_previous_peer_pool(
            MIN_LIVE_SERVERS_TO_OVERWRITE,
            MIN_LIVE_SERVERS_TO_OVERWRITE
        ));
        assert!(!should_keep_previous_peer_pool(
            1,
            MIN_LIVE_SERVERS_TO_OVERWRITE - 1
        ));
    }

    #[test]
    fn peerd_uses_candidate_fallback_when_previous_pool_is_already_bad() {
        assert!(should_write_candidate_fallback(
            1,
            1,
            MIN_LIVE_SERVERS_TO_OVERWRITE
        ));
        assert!(!should_write_candidate_fallback(
            MIN_LIVE_SERVERS_TO_OVERWRITE,
            1,
            MIN_LIVE_SERVERS_TO_OVERWRITE
        ));
        assert!(!should_write_candidate_fallback(
            1,
            MIN_LIVE_SERVERS_TO_OVERWRITE,
            MIN_LIVE_SERVERS_TO_OVERWRITE
        ));
        assert!(should_write_candidate_fallback(
            0,
            MIN_LIVE_SERVERS_TO_OVERWRITE,
            MIN_LIVE_SERVERS_TO_OVERWRITE
        ));
        assert!(!should_write_candidate_fallback(
            1,
            1,
            MIN_LIVE_SERVERS_TO_OVERWRITE - 1
        ));
    }

    #[test]
    fn active_push_forwards_monotonic_rounds_after_gap() {
        let mut last = 0u32;
        let dedup = RoundDedup::new(16_384);

        assert!(!should_forward_block_round(11, false, &mut last, &dedup));
        assert!(should_forward_block_round(10, true, &mut last, &dedup));
        assert_eq!(last, 10);
        assert!(!should_forward_block_round(9, false, &mut last, &dedup));
        assert!(!should_forward_block_round(12, false, &mut last, &dedup));
        assert!(should_forward_block_round(11, false, &mut last, &dedup));
        assert_eq!(last, 11);
        assert!(!should_forward_block_round(11, true, &mut last, &dedup));
        assert!(should_forward_block_round(15, true, &mut last, &dedup));
        assert_eq!(last, 15);
        assert!(!should_forward_block_round(12, true, &mut last, &dedup));
        assert_eq!(last, 15);
    }

    #[test]
    fn block_round_bounded_decode_path() {
        // A repetitive prefix compresses to a short literal run + back-references, so the
        // literal fast path (needs >=0x63 leading literals) is skipped and the bounded decoder
        // lz4_first_n — including its overlapping match-copy loop — does the work.
        for round in [1u32, 123_456_789, u32::MAX] {
            let mut dec = vec![0xAAu8; 0x100];
            dec[0x5e] = 0xfc;
            dec[0x5f..0x63].copy_from_slice(&round.to_le_bytes());
            let payload = lz4_flex::block::compress_prepend_size(&dec);
            assert_eq!(block_round_full(&payload), Some(round), "oracle @ {round}");
            assert_eq!(block_round(&payload), Some(round), "bounded @ {round}");
        }
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
    fn round_dedup_sliding_window_semantics() {
        let d = RoundDedup::new(16_384);
        assert!(d.is_new(5), "first sighting is new");
        assert!(!d.is_new(5), "repeat is deduped");
        // same slot (5 + 16384), different round: evicts 5 from the window
        assert!(d.is_new(5 + 16_384), "colliding round is new");
        assert!(
            d.is_new(5),
            "evicted round counts as new again (window semantics)"
        );
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
