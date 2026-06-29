// HL push gateway — multi-upstream MERGE with round-based dedup (= "most complete" live stream).
//
// Each unique consensus round is forwarded to the local node exactly once, taken from whichever
// upstream supplies it. A round missed by one peer is filled from another => gap-free / most complete.
// Live block (decompressed) carries the round at offset 0x5e = [0xfc][u32 LE].
// frame = [u32 BE L][1 type byte][L payload]; type=1 data; payload = lz4_flex(prepend_size).
//
// Upstreams may be "ip" (=> :4001) or "ip:port" (for local mock peers / custom ports).
// Subcommand `mock <bind:port> <dir> <start> <end>` replays captured blocks[start..end] for testing.

use std::collections::{BTreeMap, VecDeque};
use rustc_hash::FxHashSet;
use std::env;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
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
    seen: Mutex<(FxHashSet<u32>, VecDeque<u32>)>,
    cap: usize,
    uniq: AtomicU64,
    dups: AtomicU64,
}
impl RoundDedup {
    fn new(cap: usize) -> Self {
        Self { seen: Mutex::new((FxHashSet::default(), VecDeque::new())), cap, uniq: AtomicU64::new(0), dups: AtomicU64::new(0) }
    }
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn is_new(&self, r: u32) -> bool {
        let mut g = self.seen.lock().unwrap();
        if g.0.contains(&r) {
            self.dups.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        g.0.insert(r);
        g.1.push_back(r);
        if g.1.len() > self.cap {
            if let Some(o) = g.1.pop_front() {
                g.0.remove(&o);
            }
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
        let upstreams: Vec<String> = args
            .get(2)
            .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
            .unwrap_or_default();
        run_proxy(upstreams).await;
        return;
    }
    if args.get(1).map(|s| s.as_str()) == Some("cache") {
        let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4001);
        let upstream = args.get(3).cloned().unwrap_or_else(|| "172.18.0.2:4001".into());
        run_cache(port, upstream).await;
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

    let dedup = Arc::new(RoundDedup::new(2_000_000));
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

// Failover transparent proxy (current design): the local node dials the gateway (4000-4010, outbound)
// and the gateway transparently proxies to the ACTIVE upstream peer. A health monitor detects the
// active peer stalling (no data) or dying (dial fails) and rotates to the next peer; node connections
// are dropped on rotation so the node reconnects and is re-proxied to a healthy peer ("选择其他peer替代").
// Inbound push is deliberately not implemented (verified: HL nodes only ingest from peers they dial).
async fn run_proxy(upstreams: Vec<String>) {
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
    eprintln!("[proxy] upstreams={:?}, active={}", hosts, hosts[0]);

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

    let dedup = RoundDedup::new(2_000_000);
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
