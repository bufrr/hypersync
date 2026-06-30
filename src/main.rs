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
use rustc_hash::FxHashSet;
use std::env;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

const GREET_FALSE: [u8; 8] = [0, 0, 0, 3, 0, 0, 0, 0]; // send_abci:false (live blocks; no rate-limited state)

// Reference / correctness oracle + fallback: full lz4 decompress, read round @0x5e.
fn block_round_full(payload: &[u8]) -> Option<u32> {
    let dec = lz4_flex::block::decompress_size_prepended(payload).ok()?;
    if dec.len() >= 0x63 && dec[0x5e] == 0xfc {
        Some(u32::from_le_bytes([dec[0x5f], dec[0x60], dec[0x61], dec[0x62]]))
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
                if ip >= ilen { return None; }
                let b = input[ip];
                ip += 1;
                lit += b as usize;
                if b != 255 { break; }
            }
        }
        if lit > 0 {
            if ip + lit > ilen { return None; }
            let take = lit.min(n - op);
            out[op..op + take].copy_from_slice(&input[ip..ip + take]);
            op += take;
            ip += lit;
            if op >= n { return Some(op); }
        }
        if ip >= ilen { return Some(op); } // last sequence is literals-only
        if ip + 2 > ilen { return None; }
        let offset = (input[ip] as usize) | ((input[ip + 1] as usize) << 8);
        ip += 2;
        if offset == 0 || offset > op { return None; }
        let mut mlen = (token & 0x0f) as usize;
        if mlen == 15 {
            loop {
                if ip >= ilen { return None; }
                let b = input[ip];
                ip += 1;
                mlen += b as usize;
                if b != 255 { break; }
            }
        }
        mlen += 4;
        let take = mlen.min(n - op);
        for _ in 0..take {
            out[op] = out[op - offset];
            op += 1;
        }
        if op >= n { return Some(op); }
    }
    Some(op)
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn block_round(payload: &[u8]) -> Option<u32> {
    if payload.len() < 4 {
        return block_round_full(payload);
    }
    let mut buf = [0u8; 0x63];
    match lz4_first_n(&payload[4..], &mut buf) {
        Some(p) if p >= 0x63 => {
            if buf[0x5e] == 0xfc {
                Some(u32::from_le_bytes([buf[0x5f], buf[0x60], buf[0x61], buf[0x62]]))
            } else {
                None
            }
        }
        _ => block_round_full(payload),
    }
}

struct RoundDedup {
    // (membership set, insertion-order ring buffer, ring write cursor)
    seen: parking_lot::Mutex<(FxHashSet<u32>, Vec<u32>, usize)>,
    cap: usize,
    uniq: AtomicU64,
    dups: AtomicU64,
}
impl RoundDedup {
    fn new(cap: usize) -> Self {
        let set = FxHashSet::with_capacity_and_hasher(cap, Default::default());
        Self { seen: parking_lot::Mutex::new((set, Vec::with_capacity(cap), 0usize)), cap, uniq: AtomicU64::new(0), dups: AtomicU64::new(0) }
    }
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn is_new(&self, r: u32) -> bool {
        let mut g = self.seen.lock();
        if !g.0.insert(r) {
            self.dups.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        if g.1.len() < self.cap {
            g.1.push(r);
        } else {
            let c = g.2;
            let old = g.1[c];
            g.0.remove(&old);
            g.1[c] = r;
            g.2 = (c + 1) % self.cap;
        }
        self.uniq.fetch_add(1, Ordering::Relaxed);
        true
    }
}

#[tokio::main(flavor = "multi_thread")]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("bench") {
        let dir = args.get(2).cloned().unwrap_or_else(|| ".".into());
        let iters: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
        run_bench(&dir, iters);
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("mock") {
        let bind = args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:6001".into());
        let dir = args.get(3).cloned().unwrap_or_else(|| ".".into());
        let start: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
        let end: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(12);
        run_mock(bind, dir, start, end).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("relay") {
        let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4001);
        let upstream = args.get(3).cloned().unwrap_or_else(|| "172.18.0.2:4001".into());
        run_relay(port, upstream).await;
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
            .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
            .unwrap_or_default();
        run_proxy(upstreams, push).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("cache") {
        let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4001);
        let upstream = args.get(3).cloned().unwrap_or_else(|| "172.18.0.2:4001".into());
        run_cache(port, upstream).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("gateway") {
        // full P2P gateway: reads the node's OWN peer file (path arg) for its upstream pool, then
        // caches abci_state + round-merges live blocks from many peers + proxies gossip RPC w/ failover.
        let node_peer_file = args.get(2).cloned().unwrap_or_else(|| "/nodepeers".into());
        // --push: also merge-push live blocks from multiple peers (lower latency); default off (pure
        // transparent failover). --live N: concurrent upstreams for the merge (default 10). --retain N:
        // how many recent blocks the gateway keeps (block-height window) — bounds memory (default 5000).
        let push = args.iter().any(|a| a == "--push");
        let flagval = |name: &str, def: usize| {
            args.iter()
                .position(|a| a == name)
                .and_then(|i| args.get(i + 1))
                .and_then(|v| v.parse().ok())
                .unwrap_or(def)
        };
        let n_live = flagval("--live", 5);
        let retain = flagval("--retain", 5000);
        run_gateway(node_peer_file, push, n_live, retain).await;
        return;
    }
    let port: u16 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(4001);
    let upstreams: Vec<String> = match args.get(2) {
        Some(s) => s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect(),
        None => vec!["74.63.207.101".into(), "64.140.170.202".into()],
    };
    let l = TcpListener::bind(("0.0.0.0", port)).await.expect("bind");
    eprintln!("[gw] round-merge :{} | {} upstreams", port, upstreams.len());
    loop {
        let (down, addr) = match l.accept().await {
            Ok(x) => x,
            Err(e) => { eprintln!("[gw] accept {e}"); continue; }
        };
        let ups = upstreams.clone();
        tokio::spawn(async move {
            eprintln!("[gw] downstream {addr}");
            if let Err(e) = serve(down, ups).await { eprintln!("[gw] {addr}: {e}"); }
            eprintln!("[gw] {addr} done");
        });
    }
}

async fn serve(mut down: TcpStream, upstreams: Vec<String>) -> std::io::Result<()> {
    down.set_nodelay(true).ok();
    let mut g = [0u8; 8];
    timeout(Duration::from_secs(20), down.read_exact(&mut g)).await??;
    eprintln!("[gw] node greeting {:02x?}", g);

    let dedup = Arc::new(RoundDedup::new(131_072));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(4096);

    for ip in &upstreams {
        let ip = ip.clone();
        let tx = tx.clone();
        let dedup = dedup.clone();
        tokio::spawn(async move {
            match connect_greet(&ip, GREET_FALSE).await {
                Ok(s) => { let _ = pump(s, ip, tx, dedup).await; }
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
        if nframes % 5 == 0 {
            eprintln!(
                "[gw] unique_rounds(forwarded)={} deduped={} frames={}",
                dedup.uniq.load(Ordering::Relaxed), dedup.dups.load(Ordering::Relaxed), nframes
            );
        }
    }
    eprintln!(
        "[gw] end: unique_rounds={} deduped={} frames={} bytes={}",
        dedup.uniq.load(Ordering::Relaxed), dedup.dups.load(Ordering::Relaxed), nframes, bytes
    );
    Ok(())
}

async fn connect_greet(addr: &str, g: [u8; 8]) -> std::io::Result<TcpStream> {
    let target = if addr.contains(':') { addr.to_string() } else { format!("{addr}:4001") };
    let mut s = timeout(Duration::from_secs(5), TcpStream::connect(&target)).await??;
    s.set_nodelay(true).ok();
    s.write_all(&g).await?;
    Ok(s)
}

async fn pump(mut s: TcpStream, ip: String, tx: mpsc::Sender<Vec<u8>>, dedup: Arc<RoundDedup>) -> std::io::Result<()> {
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
                if n == 0 { return Ok(()); }
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
                if first { eprintln!("[gw] upstream {ip}: first round = {r}"); first = false; }
                if dedup.is_new(r) {
                    let mut frame = hdr.to_vec();
                    frame.extend_from_slice(&payload);
                    if tx.send(frame).await.is_err() { return Ok(()); }
                }
            }
            None => {
                let mut frame = hdr.to_vec();
                frame.extend_from_slice(&payload);
                if tx.send(frame).await.is_err() { return Ok(()); }
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

    let l = TcpListener::bind(("0.0.0.0", port)).await.expect("cache bind");
    eprintln!("[cache] :{} <- upstream {} (state cached once + continuous round-merged live)", port, upstream);
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
        s.read_exact(&mut hdr).await?;
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        let mut payload = vec![0u8; len];
        s.read_exact(&mut payload).await?;
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
                if n == 0 { return Ok(()); }
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
    !(s.starts_with("0.")
        || s.starts_with("127.")
        || s.starts_with("10.")
        || s.starts_with("172.")
        || s.starts_with("192.168.")
        || s.starts_with("169.254.")
        || s.starts_with("255."))
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

type BlockBuf = Arc<Mutex<BTreeMap<u32, Arc<Vec<u8>>>>>; // reserved cache type (serve fetch-forwards)

// Serve the node's client-block RPC (port 4002). A node that cold-started from the cached bootstrap
// has NO peer relationship, so the gateway fetches the requested range from a real peer ON THE NODE'S
// BEHALF (client-block range queries are answered to any caller) and forwards the response verbatim
// (the node verifies signatures). This lets the node sync entirely through the gateway. The buffered
// LIVE blocks can't be reused here — a live/consensus block is a DIFFERENT serialization from a
// ClientBlock element (the latter needs commit-proof from later rounds), so we fetch the real thing.
async fn serve_client_blocks(
    down: TcpStream,
    _buf: BlockBuf,
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
    if timeout(Duration::from_secs(120), down.read_exact(&mut hdr))
        .await
        .is_err()
    {
        return;
    }
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    if len > 65536 {
        return;
    }
    let mut payload = vec![0u8; len];
    if timeout(Duration::from_secs(30), down.read_exact(&mut payload))
        .await
        .is_err()
    {
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
        if timeout(Duration::from_secs(15), upc.read_exact(&mut rh))
            .await
            .is_err()
        {
            continue;
        }
        let rl = u32::from_be_bytes([rh[0], rh[1], rh[2], rh[3]]) as usize;
        if rl > 200_000_000 {
            continue;
        }
        let mut rp = vec![0u8; rl];
        if timeout(Duration::from_secs(30), upc.read_exact(&mut rp))
            .await
            .is_err()
        {
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
    loop {
        let mut hdr = [0u8; 5];
        match timeout(Duration::from_secs(20), s.read_exact(&mut hdr)).await {
            Ok(Ok(_)) => {}
            _ => break,
        }
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if first {
            if len <= 4_000_000 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "not serving abci_state",
                ));
            }
            first = false;
        }
        if len > 2_000_000_000 {
            break;
        }
        let mut payload = vec![0u8; len];
        if timeout(Duration::from_secs(60), s.read_exact(&mut payload))
            .await
            .is_err()
        {
            break;
        }
        blob.extend_from_slice(&hdr);
        blob.extend_from_slice(&payload);
        win_bytes += 5 + len;
        // speed gate: right after the abci_state snapshot, require a fast server (else the EVM-KVs
        // capture is slow and the rate-drop detector could mistake a slow tail for the end).
        if blob.len() >= 900_000_000 && blob.len() < 970_000_000 {
            let r = blob.len() as f64 / t0.elapsed().as_secs_f64();
            if r < 10_000_000.0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "state-server too slow",
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
                    break;
                }
            } else {
                slow = 0;
            }
            win = Instant::now();
            win_bytes = 0;
        }
    }
    Ok(blob)
}

// Full P2P gateway. The node connects ONLY to the gateway; the gateway provides all of HL's sync P2P
// backed by MULTIPLE upstream peers (taken from the node's own peer file, a startup path arg):
//   - abci_state: fetched from a pool peer and CACHED (served to the node at local speed, so node
//     restarts never re-pull ~950MB and never hit the per-IP abci_state rate-limit);
//   - live blocks: round-merged from several pool peers (fastest-block-first, gap-free);
//   - gossip RPC (4002 etc.): transparently proxied to an active pool peer, failing over on dial error.
// If the active peer has a problem the gateway uses the next peer from the (continuously refreshed) pool.
async fn run_gateway(node_peer_file: String, push: bool, n_live: usize, retain: usize) {
    let pool: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(read_node_peers(&node_peer_file)));
    let buf: BlockBuf = Arc::new(Mutex::new(BTreeMap::new()));
    // cached verbatim bootstrap (abci_state + EVM KVs) for cold-start without a peer state fetch
    let boot_blob: Arc<Mutex<Option<Arc<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let rr = Arc::new(AtomicUsize::new(0)); // round-robin so successive bootstraps pick fresh peers
    // the peer that served the node's bootstrap; ALL the node's connections reuse it so client-block
    // RPC (4002) isn't rejected with "Peer-only request" for hitting a peer that doesn't know the node.
    let active: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    eprintln!(
        "[gw] full P2P gateway: pool from {} ({} peers); mode={}; live-merge upstreams={}, retain={} blocks",
        node_peer_file,
        pool.lock().unwrap().len(),
        if push { "transparent + block-push" } else { "transparent failover" },
        n_live,
        retain
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

    // block buffer for the gossip RPC server (--push): round-indexed recent blocks, bounded by
    // `retain`, populated by `n_live` persistent live feeders. Lets the gateway answer a node's
    // client-block catch-up RANGE requests locally (no peer load, no "Peer-only request");
    // bootstrap (abci_state + EVM KVs) still streams transparently from a real peer.
    if push {
        // bootstrap capture: fetch + cache the full bootstrap (abci_state + EVM KVs) verbatim from a
        // serving state-server, so a node can cold-start from cache (no per-IP state rate-limit). Refresh.
        {
            let boot_blob = boot_blob.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                loop {
                    let peers = pool.lock().unwrap().clone();
                    let mut got = false;
                    for ip in peers.iter().take(12) {
                        if let Ok(blob) = fetch_bootstrap(&format!("{}:4001", ip)).await {
                            if blob.len() > 500_000_000 {
                                eprintln!(
                                    "[gw] bootstrap captured: {} MB via {}",
                                    blob.len() / 1_000_000,
                                    ip
                                );
                                *boot_blob.lock().unwrap() = Some(Arc::new(blob));
                                got = true;
                                break;
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(if got { 900 } else { 60 })).await;
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
        let buf = buf.clone();
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
                let buf = buf.clone();
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
                        if timeout(Duration::from_secs(20), down.read_exact(&mut greet))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        if greet[5] == 1 {
                            // BOOTSTRAP. With --push + a captured snapshot, serve it FROM CACHE (no peer
                            // state fetch, no rate-limit); then stream live from a pool peer. The node
                            // catches up the gap via the client-block RPC server (4002, from the buffer).
                            if push {
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
                    } else if push && port == 4002 {
                        // serve the node's client-block catch-up from the local buffer; falls back
                        // to relaying to the active peer for anything not fully buffered.
                        serve_client_blocks(down, buf, active, peers, rr).await;
                    } else {
                        // other gossip channels: reuse the active peer (consistent for the node)
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
        .map(|u| match u.rsplit_once(':') { Some((h, _)) => h.to_string(), None => u.clone() })
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
    eprintln!("[proxy] upstreams={:?}, active={}, push={}", hosts, hosts[0], push);

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
                        eprintln!("[proxy] STALL {}ms on {} -> failover -> {}", now - ld, hosts[old], hosts[j]);
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
                Err(e) => { eprintln!("[proxy] bind :{p}: {e}"); return; }
            };
            loop {
                let (mut down, _addr) = match l.accept().await { Ok(x) => x, Err(_) => continue };
                let hosts = hosts.clone();
                let active = active.clone();
                let generation = generation.clone();
                let last_data_ms = last_data_ms.clone();
                let bad_until = bad_until.clone();
                tokio::spawn(async move {
                    let cur_gen = generation.load(Ordering::Relaxed);
                    let idx = active.load(Ordering::Relaxed);
                    let up = format!("{}:{}", hosts[idx], p);
                    let mut upc = match timeout(Duration::from_secs(4), TcpStream::connect(&up)).await {
                        Ok(Ok(s)) => s,
                        _ => {
                            let now = base.elapsed().as_millis() as u64;
                            bad_until[idx].store(now + 30000, Ordering::Relaxed);
                            if let Some(j) = pick_next(idx, &bad_until, now, hosts.len()) {
                                active.store(j, Ordering::Relaxed);
                                generation.fetch_add(1, Ordering::Relaxed);
                                eprintln!("[proxy] dial {} failed -> failover -> {}", hosts[idx], hosts[j]);
                            } else {
                                eprintln!("[proxy] dial {} failed, no healthy alternative", hosts[idx]);
                            }
                            return;
                        }
                    };
                    down.set_nodelay(true).ok();
                    upc.set_nodelay(true).ok();
                    if push && p == 4001 {
                        // block-push: merge live blocks from active + all other peers (round-dedup,
                        // fastest-first); active stays transparent for node->peer + control/state/RPC.
                        serve_push(down, upc, hosts.clone(), idx, p).await;
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
                                    if dw.write_all(&buf[..nb]).await.is_err() { break; }
                                }
                            }
                        }
                    });
                    let n2u = tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut dr, &mut uw).await;
                    });
                    loop {
                        if generation.load(Ordering::Relaxed) != cur_gen { break; }
                        if up2.is_finished() || n2u.is_finished() { break; }
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
async fn serve_push(node: TcpStream, active_conn: TcpStream, hosts: Arc<Vec<String>>, active_idx: usize, port: u16) {
    let dedup = Arc::new(RoundDedup::new(131_072));
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8192);
    let (node_r, mut node_w) = node.into_split();
    let (act_r, mut act_w) = active_conn.into_split();
    // node -> active peer (transparent: greeting + RPC requests + acks)
    {
        let mut node_r = node_r;
        tokio::spawn(async move { let _ = tokio::io::copy(&mut node_r, &mut act_w).await; });
    }
    // active peer -> node: control / abci_state forwarded as-is, block frames deduped by round
    {
        let tx = tx.clone();
        let dedup = dedup.clone();
        tokio::spawn(async move { let _ = pump_merge(act_r, tx, dedup, true).await; });
    }
    // every other peer -> node: live blocks only, deduped (multi-source acceleration)
    for (i, h) in hosts.iter().enumerate() {
        if i == active_idx { continue; }
        let target = format!("{}:{}", h, port);
        let tx = tx.clone();
        let dedup = dedup.clone();
        tokio::spawn(async move {
            if let Ok(Ok(mut s)) = timeout(Duration::from_secs(5), TcpStream::connect(&target)).await {
                s.set_nodelay(true).ok();
                if s.write_all(&GREET_FALSE).await.is_ok() {
                    let (r, _w) = s.into_split();
                    let _ = pump_merge(r, tx, dedup, false).await;
                }
            }
        });
    }
    drop(tx);
    while let Some(buf) = rx.recv().await {
        if node_w.write_all(&buf).await.is_err() { break; }
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
    loop {
        let mut hdr = [0u8; 5];
        r.read_exact(&mut hdr).await?;
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        if len > 60_000_000 && !forward_nonblock {
            // a non-active (shadow) peer shouldn't send huge frames; drain to stay frame-aligned
            let mut rem = len;
            let mut buf = vec![0u8; 65536];
            while rem > 0 {
                let nb = r.read(&mut buf[..rem.min(65536)]).await?;
                if nb == 0 { return Ok(()); }
                rem -= nb;
            }
            continue;
        }
        let mut payload = vec![0u8; len];
        r.read_exact(&mut payload).await?;
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
async fn run_relay(_port: u16, upstream: String) {
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
                Err(e) => { eprintln!("[relay] bind :{p}: {e}"); return; }
            };
            eprintln!("[relay] :{p} -> {host}:{p}");
            loop {
                let (mut down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let up = format!("{host}:{p}");
                tokio::spawn(async move {
                    let upc = match TcpStream::connect(&up).await {
                        Ok(s) => s,
                        Err(e) => { eprintln!("[relay] :{p} {addr} upstream err {e}"); return; }
                    };
                    down.set_nodelay(true).ok();
                    upc.set_nodelay(true).ok();
                    let (mut dr, mut dw) = down.into_split();
                    let (mut ur, mut uw) = upc.into_split();
                    let h = tokio::spawn(async move {
                        let mut buf = vec![0u8; 262144];
                        let mut total = 0u64;
                        loop {
                            let n = match ur.read(&mut buf).await { Ok(0) | Err(_) => break, Ok(n) => n };
                            if dw.write_all(&buf[..n]).await.is_err() { break; }
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
    for h in handles { let _ = h.await; }
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
            eprintln!("[bench] VERIFY FAIL block{i}: fast={:?} full={:?}", block_round(p), block_round_full(p));
            mism += 1;
        }
    }
    eprintln!("[bench] verify: {} blocks, {} mismatch vs full-decompress oracle", payloads.len(), mism);

    let dedup = RoundDedup::new(131_072);
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
        iters, dt, iters as f64 / dt / 1000.0, bytes as f64 / 1e6 / dt, forwarded
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
    eprintln!("[mock] {} ready, blocks {}..{} ({} frames)", bind, start, end, frames.len());
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
                    if c.write_all(f).await.is_err() { return; }
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
