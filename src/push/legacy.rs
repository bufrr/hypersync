use crate::priority::gwlog;
use crate::protocol::{block_round, RoundDedup, GREET_FALSE};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

pub(crate) async fn serve(mut down: TcpStream, upstreams: Vec<String>) -> std::io::Result<()> {
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
                Err(e) => gwlog!("[gw] upstream {ip} connect: {e}"),
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
    let mut s = timeout(Duration::from_secs(5), crate::blacklist::connect(&target)).await??;
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
        // single allocation: read the payload directly into the send-ready frame (see pump_merge)
        let mut frame = vec![0u8; 5 + len];
        frame[..5].copy_from_slice(&hdr);
        s.read_exact(&mut frame[5..]).await?;
        if typ != 1 {
            continue;
        }
        match block_round(&frame[5..]) {
            Some(r) => {
                if first {
                    gwlog!("[gw] upstream {ip}: first round = {r}");
                    first = false;
                }
                if dedup.is_new(r) && tx.send(frame).await.is_err() {
                    return Ok(());
                }
            }
            None => {
                if tx.send(frame).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
}

// Transparent relay: local node <-> single upstream peer. Forwards the node's greeting
// (send_abci:true) so the upstream serves the full abci_state + live blocks; relays both ways.
// Used to let a fresh node fully sync THROUGH the gateway from a fast local upstream.
pub(crate) async fn run_relay(upstream: String) {
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
                    gwlog!("[relay] bind :{p}: {e}");
                    return;
                }
            };
            gwlog!("[relay] :{p} -> {host}:{p}");
            loop {
                let (down, addr) = match l.accept().await {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                let up = format!("{host}:{p}");
                tokio::spawn(async move {
                    let upc = match crate::blacklist::connect(&up).await {
                        Ok(s) => s,
                        Err(e) => {
                            gwlog!("[relay] :{p} {addr} upstream err {e}");
                            return;
                        }
                    };
                    down.set_nodelay(true).ok();
                    upc.set_nodelay(true).ok();
                    let (to_node, _) = splice(down, upc).await;
                    if to_node > 1_000_000 {
                        gwlog!("[relay] :{p} {addr} up->node {:.0}MB", to_node as f64 / 1e6);
                    }
                });
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}
