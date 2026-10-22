use crate::priority::gwlog;
use crate::protocol::response_is_peer_full;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

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

// The proxy's shared failover state: which upstream is active, a generation bumped on every
// switch (sessions on an older generation end), and per-upstream cooldowns.
struct ProxyState {
    hosts: Arc<Vec<String>>,
    active: AtomicUsize,
    generation: Arc<AtomicU64>,
    bad_until: Vec<AtomicU64>,
    base: std::time::Instant,
    // last upstream->node byte on a raw splice (non-push stall monitor); restarted on a switch so
    // a new upstream isn't judged by the silence of the one it replaced
    last_data_ms: Arc<AtomicU64>,
}

impl ProxyState {
    fn now_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }

    // Cool `idx` down and, if it is still the active upstream, move to the next healthy one.
    fn fail_over(&self, idx: usize, why: &str) {
        const COOLDOWN_MS: u64 = 30_000;
        let now = self.now_ms();
        self.bad_until[idx].store(now + COOLDOWN_MS, Ordering::Relaxed);
        if self.active.load(Ordering::Relaxed) != idx {
            return; // another session already moved on
        }
        match pick_next(idx, &self.bad_until, now, self.hosts.len()) {
            Some(j) => {
                self.active.store(j, Ordering::Relaxed);
                self.generation.fetch_add(1, Ordering::Relaxed);
                self.last_data_ms.store(now, Ordering::Relaxed);
                gwlog!(
                    "[proxy] {why} on {} -> failover -> {}",
                    self.hosts[idx],
                    self.hosts[j]
                );
            }
            None => gwlog!(
                "[proxy] {why} on {}, no healthy alternative",
                self.hosts[idx]
            ),
        }
    }
}

// Failover transparent proxy. Default (push=false): the local node dials the gateway (4000-4010,
// outbound) and the gateway transparently proxies to the ACTIVE upstream peer; a health monitor
// rotates to the next healthy peer on stall/dial-failure ("选择其他peer替代").
// With push=true: the heavy block channel (4001) runs serve_push — live blocks from the active peer
// are round-gated (dedup + live floor) before reaching the node, while the active connection stays
// transparent for the node's outbound + control/state/RPC frames; no other peer's blocks are
// merged in. serve_push itself is the
// health signal then (idle, block progress, lag, implausible rounds): an upstream-side session end
// fails the active over, and a failover ends every session of the older generation so 4001 and
// 4002 always follow the same peer.
// (Inbound push to the node is not implemented — verified: HL nodes only ingest from peers they dial.)
pub(crate) async fn run_proxy(upstreams: Vec<String>, push: bool) {
    let hosts: Vec<String> = upstreams
        .iter()
        .map(|u| match u.rsplit_once(':') {
            Some((h, _)) => h.to_string(),
            None => u.clone(),
        })
        .collect();
    if hosts.is_empty() {
        gwlog!("[proxy] no upstreams given");
        return;
    }
    let n = hosts.len();
    // last upstream->node byte on a raw splice; only meaningful without --push (with --push the
    // 4001 stream goes through serve_push and never stamps it, so a stall monitor on it would be
    // blind to the main stream and fire on quiet side ports instead)
    let last_data_ms = Arc::new(AtomicU64::new(0));
    let st = Arc::new(ProxyState {
        hosts: Arc::new(hosts),
        active: AtomicUsize::new(0),
        generation: Arc::new(AtomicU64::new(0)),
        bad_until: (0..n).map(|_| AtomicU64::new(0)).collect(),
        base: std::time::Instant::now(),
        last_data_ms: last_data_ms.clone(),
    });
    gwlog!(
        "[proxy] upstreams={:?}, active={}, push={}",
        st.hosts,
        st.hosts[0],
        push
    );

    if !push {
        let st = st.clone();
        let last_data_ms = last_data_ms.clone();
        tokio::spawn(async move {
            const STALL_MS: u64 = 20000;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = st.now_ms();
                let ld = last_data_ms.load(Ordering::Relaxed);
                if ld != 0 && now.saturating_sub(ld) > STALL_MS {
                    st.fail_over(
                        st.active.load(Ordering::Relaxed),
                        &format!("STALL {}ms", now - ld),
                    );
                    last_data_ms.store(now, Ordering::Relaxed);
                }
            }
        });
    }

    let mut handles = Vec::new();
    for p in 4000u16..=4010 {
        let st = st.clone();
        let last_data_ms = last_data_ms.clone();
        handles.push(tokio::spawn(async move {
            let l = match TcpListener::bind(("0.0.0.0", p)).await {
                Ok(l) => l,
                Err(e) => {
                    gwlog!("[proxy] bind :{p}: {e}");
                    return;
                }
            };
            loop {
                let (down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let st = st.clone();
                let last_data_ms = last_data_ms.clone();
                tokio::spawn(async move {
                    proxy_conn(down, addr, p, push, st, last_data_ms).await;
                });
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

// How a raw proxy's node -> upstream copy ended.
#[derive(Debug, PartialEq)]
enum N2uEnd {
    Node,
    UpstreamGone,
    UpstreamStalled,
}

// One node connection on port `p`, proxied to the active upstream.
async fn proxy_conn(
    down: TcpStream,
    addr: std::net::SocketAddr,
    p: u16,
    push: bool,
    st: Arc<ProxyState>,
    last_data_ms: Arc<AtomicU64>,
) {
    let cur_gen = st.generation.load(Ordering::Relaxed);
    let idx = st.active.load(Ordering::Relaxed);
    let up = format!("{}:{}", st.hosts[idx], p);
    let mut upc = match timeout(Duration::from_secs(4), crate::blacklist::connect(&up)).await {
        Ok(Ok(s)) => s,
        _ => {
            st.fail_over(idx, "dial failed");
            return;
        }
    };
    let mut down = down;
    down.set_nodelay(true).ok();
    upc.set_nodelay(true).ok();
    // 4001: forward the node's greeting and vet the upstream's first frame before anything else,
    // so a refusal (peer full) or a silent upstream rotates the active out instead of being handed
    // to the node as its greeting (and, on a raw splice, counted as healthy traffic).
    let mut first = Vec::new();
    let mut bootstrap = false;
    if p == 4001 {
        let mut greet = [0u8; 8];
        if !matches!(
            timeout(Duration::from_secs(20), down.read_exact(&mut greet)).await,
            Ok(Ok(_))
        ) {
            return;
        }
        if !matches!(
            timeout(Duration::from_secs(5), upc.write_all(&greet)).await,
            Ok(Ok(()))
        ) {
            st.fail_over(idx, "greeting write failed");
            return;
        }
        bootstrap = greet[5] == 1;
        // a live greeting reply is immediate; a serving state peer may take a few seconds to
        // start the abci_state frame
        let wait = if bootstrap {
            Duration::from_secs(12)
        } else {
            PUSH_GREETING_WAIT
        };
        match read_first_frame(&mut upc, wait, bootstrap).await {
            Ok(f) if response_is_peer_full(&f) => {
                st.fail_over(idx, "peer full");
                return;
            }
            Ok(f) => first = f,
            Err(e) => {
                st.fail_over(idx, &format!("first frame: {e}"));
                return;
            }
        }
    }
    if push && p == 4001 && !bootstrap {
        // live channel: round-gated blocks from the active peer; the active stays transparent
        // for node->peer + control/RPC.
        let exit_reason = serve_push(
            down,
            upc,
            PushConfig {
                hosts: st.hosts.clone(),
                active_idx: idx,
                prefetched_greeting: Some(first),
                prefetched_frames: Vec::new(),
                initial_last_forwarded: 0,
                live_floor: Arc::new(AtomicU32::new(0)),
                // proxy mode has no peerd tip: floor-only plausibility (the session's round-jump
                // and block-progress guards still apply)
                net_round_tip: Arc::new(AtomicU32::new(0)),
                superseded: Some((st.generation.clone(), cur_gen)),
                end_when: None,
            },
        )
        .await;
        gwlog!(
            "[proxy] :{p} {addr} push session ended active_peer={} reason={exit_reason}",
            st.hosts[idx]
        );
        if push_exit_is_upstream_fault(&exit_reason) {
            st.fail_over(idx, "push session failed");
        }
        return;
    }
    if !first.is_empty() {
        // the vetted greeting / start of the abci_state frame (header only for a big frame)
        if !matches!(
            timeout(NODE_WRITE_TIMEOUT, down.write_all(&first)).await,
            Ok(Ok(()))
        ) {
            return;
        }
    }
    if push {
        // bootstrap (send_abci:true) or a side port under --push: transparent, and never stamped
        // into the (disabled) raw-splice stall monitor. Mid-bootstrap multi-source injection can't
        // help (the abci_state/EVM-KV boundaries aren't detectable). It still follows failovers,
        // like the 4001 push session, so the node's 4002 never talks to a different peer than
        // its live stream.
        tokio::select! {
            _ = splice(down, upc) => {}
            _ = wait_superseded(Some((st.generation.clone(), cur_gen))) => {}
        }
        return;
    }
    let (mut dr, mut dw) = down.into_split();
    let (mut ur, mut uw) = upc.into_split();
    let ld = last_data_ms.clone();
    let base = st.base;
    // ends true when the UPSTREAM ended the stream, false when the node stopped taking it
    let mut up2 = tokio::spawn(async move {
        let mut buf = vec![0u8; 262144];
        let closed = loop {
            match ur.read(&mut buf).await {
                Ok(0) | Err(_) => break true,
                Ok(nb) => {
                    ld.store(base.elapsed().as_millis() as u64, Ordering::Relaxed);
                    match timeout(NODE_WRITE_TIMEOUT, dw.write_all(&buf[..nb])).await {
                        Ok(Ok(())) => {}
                        _ => break false,
                    }
                }
            }
        };
        (closed, Instant::now())
    });
    // node -> upstream, bounded like serve_push's: an upstream that keeps sending but stops
    // reading would otherwise park the node's RPC requests and acks behind a full socket while
    // the stall monitor (fed by up2) sees a healthy stream. Ends with whose fault it was.
    let mut n2u = tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        let end = loop {
            match dr.read(&mut buf).await {
                Ok(0) | Err(_) => break N2uEnd::Node,
                Ok(nb) => match timeout(UPSTREAM_WRITE_TIMEOUT, uw.write_all(&buf[..nb])).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => break N2uEnd::UpstreamGone,
                    Err(_) => break N2uEnd::UpstreamStalled,
                },
            }
        };
        (end, Instant::now())
    });
    // which direction ended first (None = superseded by a failover: nobody to blame)
    let (mut up, mut down) = (None, None);
    let first_up = tokio::select! {
        _ = wait_superseded(Some((st.generation.clone(), cur_gen))) => None,
        r = &mut up2 => {
            up = r.ok();
            Some(true)
        }
        r = &mut n2u => {
            down = r.ok();
            Some(false)
        }
    };
    if let Some(first_up) = first_up {
        // one end makes the other side close right after, so give the other direction a moment
        // to end too, then let the earlier end decide (never re-poll the handle that finished)
        const SETTLE: Duration = Duration::from_millis(100);
        if first_up {
            down = timeout(SETTLE, &mut n2u).await.ok().and_then(Result::ok);
        } else {
            up = timeout(SETTLE, &mut up2).await.ok().and_then(Result::ok);
        }
        if let Some(why) = raw_proxy_fault(p, up, down) {
            st.fail_over(idx, why);
        }
    }
    up2.abort();
    n2u.abort();
}

// Why a raw proxy connection was the upstream's fault, if it was, from the direction(s) that had
// ended and when. The EARLIER end decides: an upstream that closes its stream makes the node close
// right after (it sees EOF), and a node that leaves makes the upstream close after it. A stalled
// upstream is at fault on any port; one that closed or reset only on 4001 — a side-port peer may
// close after answering an RPC.
fn raw_proxy_fault(
    p: u16,
    up: Option<(bool, Instant)>,
    down: Option<(N2uEnd, Instant)>,
) -> Option<&'static str> {
    let upstream_first = match (&up, &down) {
        (Some((_, u)), Some((_, d))) => u <= d,
        (Some(_), None) => true,
        _ => false,
    };
    if upstream_first {
        let (closed, _) = up?;
        return (p == 4001 && closed).then_some("upstream closed the stream");
    }
    match down?.0 {
        N2uEnd::UpstreamStalled => Some("upstream stopped reading"),
        N2uEnd::UpstreamGone if p == 4001 => Some("upstream write failed"),
        N2uEnd::UpstreamGone | N2uEnd::Node => None,
    }
}

// The upstream's first 4001 frame after the node's greeting. Live: the whole frame, at most 1000
// bytes (a greeting or a refusal status). Bootstrap: just the 5-byte header of the abci_state
// frame (relayed as-is by the splice that follows), which must be a real state frame — anything
// up to 4MB is a status/decline (rate limit, not serving), as the gateway treats it.
async fn read_first_frame(
    upc: &mut TcpStream,
    wait: Duration,
    bootstrap: bool,
) -> std::io::Result<Vec<u8>> {
    let mut hdr = [0u8; 5];
    timeout(wait, upc.read_exact(&mut hdr)).await??;
    let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let mut frame = hdr.to_vec();
    if bootstrap && len > 4_000_000 {
        if len > 1_500_000_000 {
            return Err(std::io::Error::other(format!(
                "implausible abci_state len={len}"
            )));
        }
        return Ok(frame);
    }
    if bootstrap && len > 1000 {
        // a longer status / decline text (rate limited, not serving) — not state either
        return Err(std::io::Error::other(format!(
            "not serving abci_state (status frame len={len})"
        )));
    }
    if len > 1000 {
        return Err(std::io::Error::other(format!(
            "live first frame too large len={len}"
        )));
    }
    let mut payload = vec![0u8; len];
    timeout(wait, upc.read_exact(&mut payload)).await??;
    frame.extend_from_slice(&payload);
    if bootstrap && !response_is_peer_full(&frame) {
        return Err(std::io::Error::other(format!(
            "not serving abci_state (status frame len={len})"
        )));
    }
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_proxy_blames_whichever_side_ended_first() {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_millis(1);
        // upstream closed its live stream, then the node (seeing EOF) closed: upstream's fault,
        // also when both had ended by the time anyone looked
        assert_eq!(
            raw_proxy_fault(4001, Some((true, t0)), Some((N2uEnd::Node, t1))),
            Some("upstream closed the stream")
        );
        assert_eq!(
            raw_proxy_fault(4001, Some((true, t0)), None),
            Some("upstream closed the stream")
        );
        // the node left first and the upstream closed after it: nobody to rotate out
        assert_eq!(
            raw_proxy_fault(4001, Some((true, t1)), Some((N2uEnd::Node, t0))),
            None
        );
        // the node stopped reading (write to it failed): not the upstream's fault
        assert_eq!(raw_proxy_fault(4001, Some((false, t0)), None), None);
        // side ports: an upstream closing after an RPC answer is normal, a stall is not
        assert_eq!(raw_proxy_fault(4002, Some((true, t0)), None), None);
        assert_eq!(
            raw_proxy_fault(4002, None, Some((N2uEnd::UpstreamGone, t0))),
            None
        );
        assert_eq!(
            raw_proxy_fault(4002, None, Some((N2uEnd::UpstreamStalled, t0))),
            Some("upstream stopped reading")
        );
        assert_eq!(
            raw_proxy_fault(4001, None, Some((N2uEnd::UpstreamGone, t0))),
            Some("upstream write failed")
        );
    }

    #[tokio::test]
    async fn proxy_first_frame_rejects_status_frames_and_oversize() {
        async fn upstream(reply: Vec<u8>) -> TcpStream {
            let l = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = l.local_addr().unwrap();
            tokio::spawn(async move {
                let (mut s, _) = l.accept().await.unwrap();
                let _ = s.write_all(&reply).await;
                tokio::time::sleep(Duration::from_secs(2)).await;
            });
            TcpStream::connect(addr).await.unwrap()
        }
        let wait = Duration::from_secs(2);
        // live: a greeting passes, a peer-full refusal is returned for the caller to rotate on
        let mut c = upstream(vec![0, 0, 0, 1, 0, 3]).await;
        assert_eq!(
            read_first_frame(&mut c, wait, false).await.unwrap(),
            [0, 0, 0, 1, 0, 3]
        );
        let mut c = upstream(vec![0, 0, 0, 1, 0, 4]).await;
        assert!(response_is_peer_full(
            &read_first_frame(&mut c, wait, false).await.unwrap()
        ));
        // live: an oversized first frame is refused without reading it
        let mut c = upstream(vec![0, 0x10, 0, 0, 1]).await;
        assert!(read_first_frame(&mut c, wait, false).await.is_err());
        // bootstrap: a small status frame (rate limited / not serving) is a refusal, not state
        let mut c = upstream(vec![0, 0, 0, 2, 0, 9, 9]).await;
        assert!(read_first_frame(&mut c, wait, true).await.is_err());
        // bootstrap: a real abci_state frame header is returned (body relayed by the splice)
        let big = 1_300_000_000u32.to_be_bytes();
        let mut c = upstream(vec![big[0], big[1], big[2], big[3], 1]).await;
        assert_eq!(read_first_frame(&mut c, wait, true).await.unwrap().len(), 5);
    }
}
