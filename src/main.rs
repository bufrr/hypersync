// HL push gateway — multi-upstream MERGE with round-based dedup (= "most complete" live stream).
//
// Each unique consensus round is forwarded to the local node exactly once, taken from whichever
// upstream supplies it. A round missed by one peer is filled from another => gap-free / most complete.
// Live block (decompressed) carries the round at offset 0x5e = [0xfc][u32 LE].
// frame = [u32 BE L][1 type byte][L payload]; type=1 data; payload = lz4_flex(prepend_size).
//
// Upstreams may be "ip" (=> :4001) or "ip:port" (for local mock peers / custom ports).
// Subcommand `mock <bind:port> <dir> <start> <end>` replays captured blocks[start..end] for testing.

use std::collections::BTreeMap;
use std::env;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

const GREET_FALSE: [u8; 8] = [0, 0, 0, 3, 0, 0, 0, 0]; // send_abci:false (live blocks; no rate-limited state)

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
        // optional `--push`: also actively merge-push live blocks from ALL peers (fastest-block, round-dedup).
        let push = args.iter().any(|a| a == "--push");
        let upstreams: Vec<String> = args
            .iter()
            .skip(2)
            .find(|a| a.as_str() != "--push")
            .map(|s| {
                s.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect()
            })
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
        // peer discovery + probing daemon (env-var config mirrors peerd.sh for a drop-in swap):
        // HYPERSYNC_DATA (data dir, default ./data), HL_NODE (default hyperliquid-node-1),
        // HL_SELF_IP (optional, exclude this node's own public IP from candidates).
        let interval: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(300);
        run_peerd(interval).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("gateway") {
        // full P2P gateway: reads the node's OWN peer file (path arg) for its upstream pool, then
        // caches abci_state + round-merges live blocks from many peers + proxies gossip RPC w/ failover.
        let node_peer_file = args.get(2).cloned().unwrap_or_else(|| "/nodepeers".into());
        // --push: merge-push live blocks from multiple peers ON TOP OF a transparent-failover
        //   backbone (default off = pure transparent failover). --live N: concurrent live upstreams
        //   (default 5).
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
        Some(s) => s
            .split(',')
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .collect(),
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
    s.write_all(&[0, 0, 0, 3, 0, 1, 0, 0]).await?; // send_abci:true
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
    use std::collections::HashSet;
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

// Connect to the session's active peer (the one serving the node's bootstrap) on `port`, waiting
// briefly for the bootstrap to set it; falls back to a round-robin pool peer if none is set yet.
async fn dial_active(
    active: &Arc<Mutex<Option<String>>>,
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
        if let Some(a) = active.lock().unwrap().clone() {
            target = Some(a);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // no active yet (e.g. a resuming node after a gateway restart, with no fresh bootstrap): establish
    // one now so EVERY connection of this node converges on the same upstream (client-block RPC needs it)
    let target = match target {
        Some(t) => t,
        None => {
            let mut a = active.lock().unwrap();
            match a.clone() {
                Some(t) => t,
                None => {
                    let t = peers[rr.fetch_add(1, Ordering::Relaxed) % n].clone();
                    *a = Some(t.clone());
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

// Serve the node's client-block RPC (port 4002). A node that cold-started from the cached bootstrap
// has NO peer relationship, so the gateway fetches the requested range from a real peer ON THE NODE'S
// BEHALF (client-block range queries are answered to any caller) and forwards the response verbatim
// (the node verifies signatures). This lets the node sync entirely through the gateway. We can't reuse
// buffered LIVE blocks here — a live/consensus block is a DIFFERENT serialization from a ClientBlock
// element (the latter needs commit-proof from later rounds), so we fetch the real thing.
async fn serve_client_blocks(
    down: TcpStream,
    active: Arc<Mutex<Option<String>>>,
    peers: Vec<String>,
    rr: Arc<AtomicUsize>,
) {
    let mut down = down;
    let n = peers.len();
    if n == 0 {
        return;
    }
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
    // fetch the same request from a real peer (active first, then round-robin pool); forward verbatim
    let mut cands: Vec<String> = Vec::new();
    if let Some(a) = active.lock().unwrap().clone() {
        cands.push(a);
    }
    let start = rr.fetch_add(1, Ordering::Relaxed);
    for k in 0..n {
        cands.push(peers[(start + k) % n].clone());
    }
    for ip in cands {
        let up = format!("{}:4002", ip);
        let Ok(Ok(mut upc)) = timeout(Duration::from_secs(5), TcpStream::connect(&up)).await else {
            continue;
        };
        upc.set_nodelay(true).ok();
        if upc.write_all(&req).await.is_err() {
            continue;
        }
        let mut rh = [0u8; 5];
        if !matches!(
            timeout(Duration::from_secs(15), upc.read_exact(&mut rh)).await,
            Ok(Ok(_))
        ) {
            continue;
        }
        let rl = u32::from_be_bytes([rh[0], rh[1], rh[2], rh[3]]) as usize;
        // a client-block batch is a handful of blocks (~100 rounds/request); cap well above that
        // but far below a memory hazard.
        if rl > 64_000_000 {
            continue;
        }
        let mut rp = vec![0u8; rl];
        if !matches!(
            timeout(Duration::from_secs(30), upc.read_exact(&mut rp)).await,
            Ok(Ok(_))
        ) {
            continue;
        }
        let _ = down.write_all(&rh).await;
        let _ = down.write_all(&rp).await;
        return;
    }
}

// Capture a full bootstrap (abci_state + EVM KVs) VERBATIM from a serving state-server, frame-aligned,
// stopping when the bulk transfer ends (byte rate collapses from the bulk's tens-of-MB/s to the
// live-block trickle). Replayed to a bootstrapping node so it never pulls the snapshot from a peer
// (avoids the per-IP abci_state rate-limit). The node handles the internal abci_state/EVM-KVs/live
// framing itself, so the gateway needn't understand the (undocumented) boundary.
async fn fetch_bootstrap(upstream: &str) -> std::io::Result<Vec<u8>> {
    let mut s = TcpStream::connect(upstream).await?;
    s.set_nodelay(true).ok();
    s.write_all(&[0, 0, 0, 3, 0, 1, 0, 0]).await?; // send_abci:true
    let mut blob: Vec<u8> = Vec::new();
    let t0 = Instant::now();
    let mut first = true;
    let mut win = Instant::now();
    let mut win_bytes = 0usize;
    let mut slow = 0u32;
    let mut clean = false; // set only when the bulk (abci_state + EVM KVs) ends via the rate-drop
    loop {
        let mut hdr = [0u8; 5];
        match timeout(Duration::from_secs(20), s.read_exact(&mut hdr)).await {
            Ok(Ok(_)) => {}
            _ => break,
        }
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if first {
            if len <= 4_000_000 {
                return Err(std::io::Error::other("not serving abci_state"));
            }
            first = false;
        }
        // Cap a single frame: the only legitimately-large frame is the ~954MB abci_state snapshot;
        // 1.5GB leaves headroom for state growth while rejecting a bogus/huge length that would
        // otherwise force a multi-GB allocation (3 of these race concurrently).
        if len > 1_500_000_000 {
            break;
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
                // Abort below the speed gate's 10MB/s floor (same peers the post-snapshot gate
                // rejects, just ~20x sooner); peers at/above it are kept so a capture still
                // completes whenever any peer serves >=10MB/s.
                if el > 4.0 && (off as f64 / el) < 10_000_000.0 {
                    return Err(std::io::Error::other("state-server too slow (early abort)"));
                }
            }
            if !ok {
                break;
            }
        } else if !matches!(
            timeout(Duration::from_secs(60), s.read_exact(&mut payload)).await,
            Ok(Ok(_))
        ) {
            break; // timeout OR mid-frame EOF/error -> incomplete (clean stays false -> Err)
        }
        blob.extend_from_slice(&hdr);
        blob.extend_from_slice(&payload);
        // total cap: a real bootstrap is ~4.5GB; a peer that streams bulk forever (staying above
        // the rate-drop floor) must not inflate the capture unboundedly (x3 concurrent racers).
        if blob.len() > 10_000_000_000 {
            return Err(std::io::Error::other("capture too large"));
        }
        win_bytes += 5 + len;
        // speed gate: right after the abci_state snapshot, require a fast server (else the EVM-KVs
        // capture is slow and the rate-drop detector could mistake a slow tail for the end).
        if blob.len() >= 900_000_000 && blob.len() < 970_000_000 {
            let r = blob.len() as f64 / t0.elapsed().as_secs_f64();
            if r < 10_000_000.0 {
                return Err(std::io::Error::other("state-server too slow"));
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
                    clean = true;
                    break;
                }
            } else {
                slow = 0;
            }
            win = Instant::now();
            win_bytes = 0;
        }
    }
    // Only a clean bulk-end is a usable capture; a mid-stream EOF/timeout leaves an incomplete
    // abci_state/EVM-KVs blob that would wedge a cold-starting node — reject it so the race never
    // prefers a fast-but-truncated capture over a slower complete one.
    if clean && blob.len() > 500_000_000 {
        Ok(blob)
    } else {
        Err(std::io::Error::other("incomplete capture"))
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
                if let Some(Ok((Ok(blob), ip))) = joined {
                    eprintln!(
                        "[gw] bootstrap race won by {ip} ({} MB) after {started} attempt(s)",
                        blob.len() / 1_000_000
                    );
                    set.abort_all();
                    return Some((blob, ip));
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

// Peer discovery + probing daemon (Rust port of peerd.sh, kept as its OWN process rather than a
// background task inside the gateway): discovers candidate peers (the gossipRootIps API +
// harvesting a running node's own logs for peers it has talked to) and probes each concurrently
// for live-block serving, writing a ranked pool to <data-dir>/peers.json that `gateway` reads and
// refreshes every 30s. Deliberately a separate process: a probing storm across 100+ candidates, or
// a hung `docker logs` call, never touches the gateway process that is actively serving a node.
// Atomically replace `path` (write temp + rename): the gateway re-reads peers.json every 30s, and
// a plain truncate-then-write could be observed half-written — extract_ipv4 on a truncated tail
// can yield a VALID but WRONG address ("…236" cut to "…23") that then enters the dial pool.
async fn write_atomic(path: &str, contents: &str) {
    let tmp = format!("{path}.tmp");
    if tokio::fs::write(&tmp, contents).await.is_ok() {
        let _ = tokio::fs::rename(&tmp, path).await;
    }
}

async fn run_peerd(interval: u64) {
    use std::collections::{HashMap, HashSet};
    let data_dir = std::env::var("HYPERSYNC_DATA").unwrap_or_else(|_| "./data".to_string());
    tokio::fs::create_dir_all(&data_dir).await.ok();
    let node = std::env::var("HL_NODE").unwrap_or_else(|_| "hyperliquid-node-1".to_string());
    // optional: exclude our own public IP (a legitimate routable address, but not a peer to dial)
    let self_ip = std::env::var("HL_SELF_IP").ok();
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
        // 1. discover: gossipRootIps API + harvest the node's own logs for peers it has talked to.
        // Shelling out to curl/docker (rather than an HTTPS client / Docker socket API) keeps this
        // to thin, standard external tools instead of adding heavyweight client dependencies for a
        // one-shot JSON POST and a log tail.
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
        let harvested = timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("sudo")
                .args(["docker", "logs", "--tail", "3000", &node])
                .output(),
        )
        .await;

        if let Ok(Ok(o)) = roots {
            for ip in extract_ipv4(&String::from_utf8_lossy(&o.stdout)) {
                if self_ip.as_deref() != Some(ip.as_str()) {
                    candidates.insert(ip);
                }
            }
        }
        if let Ok(Ok(o)) = harvested {
            let text = String::from_utf8_lossy(&o.stdout) + String::from_utf8_lossy(&o.stderr);
            for ip in extract_ipv4(&text) {
                if self_ip.as_deref() != Some(ip.as_str()) {
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

        // 3. write peers.json (hand-rolled: content is plain IPv4 strings, no escaping needed)
        let json = format!(
            "{{\"live_servers\":[{}],\"n_candidates\":{}}}",
            ranked
                .iter()
                .map(|ip| format!("\"{ip}\""))
                .collect::<Vec<_>>()
                .join(","),
            cand_vec.len()
        );
        write_atomic(&out_path, &json).await;

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
            "{now} candidates={} live={} pruned={} top={:?}\n",
            cand_vec.len(),
            ranked.len(),
            pruned,
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

// Full P2P gateway. The node connects ONLY to the gateway; the gateway provides all of HL's sync P2P
// backed by MULTIPLE upstream peers (taken from the node's own peer file, a startup path arg):
//   - abci_state: fetched from a pool peer and CACHED (served to the node at local speed, so node
//     restarts never re-pull ~950MB and never hit the per-IP abci_state rate-limit);
//   - live blocks: round-merged from several pool peers (fastest-block-first, gap-free);
//   - gossip RPC (4002 etc.): transparently proxied to an active pool peer, failing over on dial error.
// If the active peer has a problem the gateway uses the next peer from the (continuously refreshed) pool.
async fn run_gateway(node_peer_file: String, push: bool, cache_coldstart: bool, n_live: usize) {
    let pool: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(read_node_peers(&node_peer_file)));
    // cached verbatim bootstrap (abci_state + EVM KVs) for cold-start without a peer state fetch
    let boot_blob: Arc<Mutex<Option<Arc<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let rr = Arc::new(AtomicUsize::new(0)); // round-robin so successive bootstraps pick fresh peers
                                            // the peer that served the node's bootstrap; ALL the node's connections reuse it so client-block
                                            // RPC (4002) isn't rejected with "Peer-only request" for hitting a peer that doesn't know the node.
    let active: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    eprintln!(
        "[gw] full P2P gateway: pool from {} ({} peers); bootstrap={}; live={}; live-merge upstreams={}",
        node_peer_file,
        pool.lock().unwrap().len(),
        if cache_coldstart {
            "cache-coldstart(+transparent fallback)"
        } else {
            "transparent"
        },
        if push {
            "block-push(transparent backbone + multi-source + active-failover)"
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
                if !p.is_empty() {
                    *pool.lock().unwrap() = p;
                }
            }
        });
    }

    if cache_coldstart {
        // bootstrap capture: fetch + cache the full bootstrap (abci_state + EVM KVs) verbatim from a
        // serving state-server, so a node can cold-start from cache (no per-IP state rate-limit). Refresh.
        {
            let boot_blob = boot_blob.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                // abci_state is rate-limited per (source-IP, peer) pair, so re-hitting the same
                // top-ranked peers exhausts them. Take a bounded window of candidates per cycle
                // (gentle on peers), rotate the window each cycle to cover the pool and let
                // per-peer limits recover, and seed the start from the clock so a fresh gateway
                // doesn't always begin at the (often-busy) top of the rank.
                let mut cycle = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as usize)
                    .unwrap_or(0);
                let mut fails = 0u32;
                loop {
                    // Until we have ANY cached blob the node can't cold-start, so the initial
                    // capture is urgent: search a wide window and retry fast. Once cached, the old
                    // blob stays valid, so refreshes use a small rotated window on a long interval
                    // (gentle on peers, lets per-peer abci_state limits recover).
                    let have_cache = boot_blob.lock().unwrap().is_some();
                    let window = if have_cache { 24 } else { 64 };
                    let peers: Vec<String> = {
                        let full = pool.lock().unwrap();
                        let n = full.len();
                        if n == 0 {
                            Vec::new()
                        } else {
                            let take = window.min(n);
                            let off = cycle.wrapping_mul(take) % n;
                            (0..take).map(|k| full[(off + k) % n].clone()).collect()
                        }
                    };
                    cycle = cycle.wrapping_add(1);
                    let got = if let Some((blob, ip)) =
                        capture_bootstrap_raced(peers, 3, Duration::from_secs(40)).await
                    {
                        eprintln!(
                            "[gw] bootstrap captured: {} MB via {} (raced)",
                            blob.len() / 1_000_000,
                            ip
                        );
                        *boot_blob.lock().unwrap() = Some(Arc::new(blob));
                        true
                    } else {
                        false
                    };
                    let delay = if got {
                        fails = 0;
                        900
                    } else if have_cache {
                        60
                    } else {
                        // initial capture failing: back off 8->16->32->60s so a degraded /
                        // rate-limited pool isn't hammered every 8s forever (still fast for the
                        // first few tries when the pool is healthy).
                        fails = (fails + 1).min(4);
                        (8u64 << (fails - 1)).min(60)
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
        let active = active.clone();
        let boot_blob = boot_blob.clone();
        handles.push(tokio::spawn(async move {
            let l = match TcpListener::bind(("0.0.0.0", port)).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[gw] bind :{port}: {e}");
                    return;
                }
            };
            loop {
                let (down, _addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let pool = pool.clone();
                let rr = rr.clone();
                let active = active.clone();
                let boot_blob = boot_blob.clone();
                tokio::spawn(async move {
                    let mut down = down;
                    down.set_nodelay(true).ok();
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
                                let blob = boot_blob.lock().unwrap().clone();
                                if let Some(blob) = blob {
                                    eprintln!(
                                        "[gw] node cold-start FROM CACHE ({} MB), no peer state fetch",
                                        blob.len() / 1_000_000
                                    );
                                    if down.write_all(&blob).await.is_err() {
                                        return;
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
                                                *active.lock().unwrap() = Some(ip.clone());
                                                splice(down, upc).await;
                                                return;
                                            }
                                        }
                                    }
                                    return;
                                }
                            }
                            // fallback: transparently relay a peer that is serving the abci_state now
                            let start = rr.fetch_add(1, Ordering::Relaxed);
                            let mut chosen: Option<(TcpStream, [u8; 5])> = None;
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
                                let mut hdr = [0u8; 5];
                                match timeout(Duration::from_secs(12), upc.read_exact(&mut hdr)).await
                                {
                                    Ok(Ok(_)) => {
                                        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]])
                                            as usize;
                                        if len <= 4_000_000 {
                                            continue;
                                        }
                                        eprintln!(
                                            "[gw] node bootstrap via {ip} (abci_state {} bytes); active set",
                                            len
                                        );
                                        *active.lock().unwrap() = Some(ip.clone());
                                        chosen = Some((upc, hdr));
                                        break;
                                    }
                                    _ => continue,
                                }
                            }
                            let Some((upc, hdr)) = chosen else {
                                eprintln!("[gw] no peer serving abci_state right now");
                                return;
                            };
                            if down.write_all(&hdr).await.is_err() {
                                return;
                            }
                            splice(down, upc).await;
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
                                *active.lock().unwrap() = Some(ip.clone());
                                if push {
                                    // active stays transparent (keeps the peer relationship for the
                                    // 4002 client-block RPC) + inject the fastest copy of each block
                                    // from up to n_live-1 other peers (multi-source acceleration).
                                    let mut hosts = vec![ip.clone()];
                                    for p in peers.iter() {
                                        if hosts.len() >= n_live {
                                            break;
                                        }
                                        if *p != ip {
                                            hosts.push(p.clone());
                                        }
                                    }
                                    serve_push(down, upc, Arc::new(hosts), 0, 4001).await;
                                } else {
                                    splice(down, upc).await;
                                }
                                break;
                            }
                        }
                    } else if cache_coldstart && port == 4002 {
                        // a cache-cold-started node has NO peer relationship, so its client-block
                        // catch-up must be fetched from a real peer and forwarded on its behalf.
                        serve_client_blocks(down, active, peers, rr).await;
                    } else {
                        // 4002 (transparent bootstrap: the node HAS a real peer relationship) and the
                        // other gossip channels: splice to the node's active peer (robust — no
                        // fetch-forward). This is why transparent bootstrap keeps the node stable.
                        let Some(upc) = dial_active(&active, &peers, &rr, port).await else {
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
                            serve_push(down, upc, hosts.clone(), idx, p).await;
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
// node's outbound and the active peer's control / abci_state / RPC frames; block frames from the
// active peer AND every other peer are merged by consensus round (dedup) and forwarded to the node
// fastest-first => the node receives each block at the earliest arrival across all peers (lower latency).
async fn serve_push(
    node: TcpStream,
    active_conn: TcpStream,
    hosts: Arc<Vec<String>>,
    active_idx: usize,
    port: u16,
) {
    let dedup = Arc::new(RoundDedup::new(16_384));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8192);
    let (node_r, mut node_w) = node.into_split();
    let (mut act_r, mut act_w) = active_conn.into_split();
    // The node's first read on 4001 is the peer's greeting ("abci_stream recv greeting", max 1000
    // bytes). Forward the active peer's greeting frame to the node BEFORE starting the shadow
    // injectors: they all share node_w, so a shadow peer's first live block can otherwise reach the
    // node ahead of the greeting, and the node reads the block's length as the greeting length and
    // bails ("tcp read bytes over limit").
    {
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
    // reconnect — the gateway then pins a FRESH active (round-robin). This is the proxy-style failover:
    // the active peer is the guaranteed path; shadows below are best-effort accelerators only.
    let mut active_task = {
        let tx = tx.clone();
        let dedup = dedup.clone();
        tokio::spawn(async move {
            let _ = pump_merge(act_r, tx, dedup, true).await;
        })
    };
    // every other peer -> node: live blocks only, deduped (multi-source acceleration)
    let mut shadows = Vec::new();
    for (i, h) in hosts.iter().enumerate() {
        if i == active_idx {
            continue;
        }
        let target = format!("{}:{}", h, port);
        let tx = tx.clone();
        let dedup = dedup.clone();
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
                        let _ = pump_merge(r, tx.clone(), dedup.clone(), false).await;
                    }
                }
                if tx.is_closed() {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }));
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
    dedup: Arc<RoundDedup>,
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
        let forward = if typ == 1 {
            match block_round(&payload) {
                Some(rnd) => dedup.is_new(rnd), // block: forward only the first (fastest) copy
                None => forward_nonblock,       // abci_state / non-round data: only from active
            }
        } else {
            forward_nonblock // control / RPC: only from active
        };
        if forward {
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

    #[test]
    fn block_round_matches_full_decompress() {
        for round in [0u32, 1, 1000, 1_055_000_000, u32::MAX] {
            let payload = make_block(round);
            assert_eq!(block_round_full(&payload), Some(round), "full @ {round}");
            assert_eq!(block_round(&payload), Some(round), "fast @ {round}");
        }
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
