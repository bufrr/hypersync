use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use crate::gateway::{
    response_has_client_block_round_too_large, response_has_client_block_round_too_small,
    response_has_no_client_blocks, response_is_peer_only,
};
use crate::protocol::{
    block_round, block_round_full, RoundDedup, GREET_FALSE, GREET_TRUE,
    MIN_COMPLETE_BOOTSTRAP_BYTES, MIN_COMPLETE_BOOTSTRAP_FRAMES,
};

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

pub(crate) async fn run_cache(port: u16, upstream: String) {
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
            // bounded: a connection that arrives before any state was captured must not wait
            // (and hold its socket) forever
            let wait_state = async {
                loop {
                    if let Some(s) = state.lock().unwrap().clone() {
                        break s;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            };
            let Ok(st) = timeout(Duration::from_secs(60), wait_state).await else {
                eprintln!("[cache] node {addr}: no state captured within 60s; closing");
                return;
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
    let mut s = crate::blacklist::connect(upstream).await?;
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
    let mut s = crate::blacklist::connect(upstream).await?;
    s.set_nodelay(true).ok();
    s.write_all(&GREET_FALSE).await?; // send_abci:false: continuous live blocks
                                      // a silent upstream must end the feed (so the caller reconnects) instead of freezing it
    const READ: Duration = Duration::from_secs(30);
    loop {
        let mut hdr = [0u8; 5];
        timeout(READ, s.read_exact(&mut hdr)).await??;
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let typ = hdr[4];
        if len > 50_000_000 {
            let mut rem = len;
            let mut buf = vec![0u8; 65536];
            while rem > 0 {
                let n = timeout(READ, s.read(&mut buf[..rem.min(65536)])).await??;
                if n == 0 {
                    return Ok(());
                }
                rem -= n;
            }
            continue;
        }
        let mut payload = vec![0u8; len];
        timeout(READ, s.read_exact(&mut payload)).await??;
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

// Benchmark the hot path: lz4 decompress + round extract (block_round) + dedup, over captured blocks.
pub(crate) fn run_bench(dir: &str, iters: usize) {
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
pub(crate) async fn run_mock(bind: String, dir: String, start: usize, end: usize) {
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
// what pins this fakenode's active peer on the gateway, so the still-open socket is returned to
// the caller (as the last tuple element) and kept alive across the follow-up 4002 RPC — closing
// it before the RPC would drop the pinned session and make the RPC time out.
async fn fakenode_live(
    gw_ip: &str,
    port: u16,
    live_secs: u64,
) -> (bool, u64, usize, u32, String, Option<TcpStream>) {
    let Some((mut c, src)) = fk_connect(gw_ip, port, &GREET_FALSE).await else {
        return (false, 0, 0, 0, "?".to_string(), None);
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
    (pass, frames, rounds.len(), max_round, src, Some(c))
}

// Run the 4002 client-block RPC while the live 4001 connection stays open. Real HL nodes issue
// 4002 queries with their 4001 live stream still up, and the gateway pins the per-source-IP
// session to that live connection; taking `live_conn` by value and dropping it only after the RPC
// returns holds the pinned session open for the whole query, then closes both sockets.
async fn fakenode_rpc_with_live_held(
    live_conn: Option<TcpStream>,
    gw_ip: &str,
    rpc_port: u16,
    max_round: u32,
    label: &str,
) -> bool {
    let result = fakenode_rpc(gw_ip, rpc_port, max_round, label).await;
    drop(live_conn); // close the held-open live 4001 socket now that the RPC has completed
    result
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
        // the gateway hands these terminal range errors to the node as a last resort; for a
        // check that the RPC path works they are a failure, not a pass
        if response_has_no_client_blocks(&payload)
            || response_has_client_block_round_too_large(&payload)
            || response_has_client_block_round_too_small(&payload)
        {
            eprintln!(
                "[fakenode:{label}] rpc got a terminal range error: {}",
                String::from_utf8_lossy(&payload[..len.min(120)])
            );
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
pub(crate) async fn run_fakenode(args: &[String]) -> i32 {
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
    let (live_pass, live_frames, live_rounds, max_round, live_src, live_conn) =
        fakenode_live(&gw_ip, base_port, live_secs).await;
    let src = if src == "?" { live_src } else { src };
    let rpc = if do_rpc {
        // Keep the live 4001 connection open for the duration of the 4002 RPC so the gateway's
        // per-source-IP pinned session survives the query; the socket is closed right after.
        Some(fakenode_rpc_with_live_held(live_conn, &gw_ip, base_port + 1, max_round, &label).await)
    } else {
        drop(live_conn); // no RPC: close the live connection now
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

    // Phase-overlap regression: the live 4001 socket must stay open until the 4002 RPC finishes.
    // A stand-in gateway records (a) when it serves the RPC response and (b) when the live socket
    // closes; `fakenode_rpc_with_live_held` must close the live socket only AFTER the RPC returns.
    // Under the old sequential behavior (close live, then RPC) the live socket would close first
    // and this ordering assertion would fail.
    #[tokio::test]
    async fn live_connection_held_open_until_rpc_completes() {
        use tokio::sync::oneshot;

        let live_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_port = live_l.local_addr().unwrap().port();
        let rpc_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_port = rpc_l.local_addr().unwrap().port();

        let (live_eof_tx, live_eof_rx) = oneshot::channel::<Instant>();
        let (rpc_done_tx, rpc_done_rx) = oneshot::channel::<Instant>();

        // Stand-in gateway 4001: read the greeting, then read until the client closes the socket;
        // stamp the moment the live connection is torn down.
        tokio::spawn(async move {
            let (mut s, _) = live_l.accept().await.unwrap();
            let mut greet = [0u8; 8];
            let _ = s.read_exact(&mut greet).await;
            let mut buf = [0u8; 1024];
            loop {
                match s.read(&mut buf).await {
                    Ok(0) | Err(_) => break, // EOF: fakenode closed the live socket
                    _ => {}
                }
            }
            let _ = live_eof_tx.send(Instant::now());
        });

        // Stand-in gateway 4002: read the 16-byte range request, reply with a well-framed
        // non-"Peer-only" type-0 response so `fakenode_rpc` returns true; stamp the serve time.
        tokio::spawn(async move {
            let (mut s, _) = rpc_l.accept().await.unwrap();
            let mut req = [0u8; 16];
            s.read_exact(&mut req).await.unwrap();
            let payload = b"ok";
            let mut resp = (payload.len() as u32).to_be_bytes().to_vec();
            resp.push(0u8); // type=0
            resp.extend_from_slice(payload);
            s.write_all(&resp).await.unwrap();
            s.flush().await.unwrap();
            let _ = rpc_done_tx.send(Instant::now());
            // Hold the RPC socket briefly so the client-side read completes cleanly.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        // Open the live 4001 connection exactly as the live phase does, then run the RPC while
        // holding it open.
        let (live_conn, _src) = fk_connect("127.0.0.1", live_port, &GREET_FALSE)
            .await
            .expect("live connect");
        let ok =
            fakenode_rpc_with_live_held(Some(live_conn), "127.0.0.1", rpc_port, 1, "test").await;
        assert!(ok, "rpc should succeed against the stand-in gateway");

        let rpc_done = rpc_done_rx.await.unwrap();
        let live_eof = live_eof_rx.await.unwrap();
        assert!(
            live_eof >= rpc_done,
            "live 4001 socket must close only after the 4002 RPC completes",
        );
    }
}
