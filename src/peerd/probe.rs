use crate::protocol::{block_round, is_plausible_mainnet_round, GREET_FALSE};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

#[allow(unused_imports)]
use super::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum ProbeStatus {
    #[default]
    NoLiveFrame,
    Live,
    ConnectTimeout,
    ConnectError,
    GreetWriteError,
    HeaderTimeout,
    HeaderEof,
    HeaderError,
    PayloadTimeout,
    PayloadEof,
    PayloadError,
    OversizedFrame,
    PeerFull,
    StatusFrame,
    LowRound,
    UnparseableBlock,
}

impl ProbeStatus {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            ProbeStatus::NoLiveFrame => "no_live_frame",
            ProbeStatus::Live => "live",
            ProbeStatus::ConnectTimeout => "connect_timeout",
            ProbeStatus::ConnectError => "connect_error",
            ProbeStatus::GreetWriteError => "greet_write_error",
            ProbeStatus::HeaderTimeout => "header_timeout",
            ProbeStatus::HeaderEof => "header_eof",
            ProbeStatus::HeaderError => "header_error",
            ProbeStatus::PayloadTimeout => "payload_timeout",
            ProbeStatus::PayloadEof => "payload_eof",
            ProbeStatus::PayloadError => "payload_error",
            ProbeStatus::OversizedFrame => "oversized_frame",
            ProbeStatus::PeerFull => "peer_full",
            ProbeStatus::StatusFrame => "status_frame",
            ProbeStatus::LowRound => "low_round",
            ProbeStatus::UnparseableBlock => "unparseable_block",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct LiveProbe {
    pub(super) blocks: usize,
    pub(super) max_round: Option<u32>,
    pub(super) status: ProbeStatus,
}

// Probe one candidate for LIVE-block serving (send_abci:false — cheap, NOT rate-limited, unlike
// abci_state). Bounded to a short wall-clock total; counts parseable type=1 live rounds (excludes
// tiny status/rejection frames). A peer-controlled frame length is capped before allocating the
// payload buffer (a live block is a few hundred KB at most).
// Matches the live-block frame cap in protocol::max_block_round_in_frames (a fn-local const there).
const MAX_PROBE_FRAME: usize = 2_000_000;

pub(super) async fn probe_live(ip: &str) -> LiveProbe {
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut s = match timeout(
        Duration::from_secs(4),
        crate::blacklist::connect(&format!("{ip}:4001")),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(_)) => {
            return LiveProbe {
                status: ProbeStatus::ConnectError,
                ..LiveProbe::default()
            };
        }
        Err(_) => {
            return LiveProbe {
                status: ProbeStatus::ConnectTimeout,
                ..LiveProbe::default()
            };
        }
    };
    if s.write_all(&GREET_FALSE).await.is_err() {
        return LiveProbe {
            status: ProbeStatus::GreetWriteError,
            ..LiveProbe::default()
        };
    }
    let mut blocks = 0usize;
    let mut max_round = None;
    let mut status = ProbeStatus::NoLiveFrame;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let mut hdr = [0u8; 5];
        match timeout(remaining, s.read_exact(&mut hdr)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                status = ProbeStatus::HeaderEof;
                break;
            }
            Ok(Err(_)) => {
                status = ProbeStatus::HeaderError;
                break;
            }
            Err(_) => {
                status = ProbeStatus::HeaderTimeout;
                break;
            }
        };
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        if len > MAX_PROBE_FRAME {
            status = ProbeStatus::OversizedFrame;
            break;
        }
        let mut payload = vec![0u8; len];
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, s.read_exact(&mut payload)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                status = ProbeStatus::PayloadEof;
                break;
            }
            Ok(Err(_)) => {
                status = ProbeStatus::PayloadError;
                break;
            }
            Err(_) => {
                status = ProbeStatus::PayloadTimeout;
                break;
            }
        };
        if hdr[4] == 0 && len == 1 && payload.first() == Some(&4) {
            status = ProbeStatus::PeerFull;
            break;
        }
        if hdr[4] == 0 {
            status = ProbeStatus::StatusFrame;
        }
        if hdr[4] == 1 && len > 1 {
            if let Some(round) = block_round(&payload) {
                // floor-only during a probe; the per-cycle clustered tip filters high artifacts
                if is_plausible_mainnet_round(round, 0) {
                    blocks += 1;
                    max_round = Some(max_round.map_or(round, |prev: u32| prev.max(round)));
                    status = ProbeStatus::Live;
                    // Enough to call the peer live, and its round is its tip: stop here. Reading
                    // on to the deadline downloaded ~6 s of the full block stream from every live
                    // peer each cycle (2026-11-16: peerd 2.4-4.1 MB/s, more than the gateway's
                    // shared live stream itself).
                    if blocks >= MIN_LIVE_BLOCKS {
                        break;
                    }
                } else {
                    status = ProbeStatus::LowRound;
                }
            } else {
                status = ProbeStatus::UnparseableBlock;
            }
        }
    }
    LiveProbe {
        blocks,
        max_round,
        status: final_probe_status(blocks, status),
    }
}

// Enough live blocks reports the probe as Live even if a later frame ended it on an error/timeout,
// but a PeerFull rejection is the more useful diagnosis and wins (the status only feeds logging;
// liveness is decided from `blocks`/`max_round`).
fn final_probe_status(blocks: usize, status: ProbeStatus) -> ProbeStatus {
    if blocks >= MIN_LIVE_BLOCKS && status != ProbeStatus::PeerFull {
        ProbeStatus::Live
    } else {
        status
    }
}

pub(super) const MIN_LIVE_BLOCKS: usize = 2;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;

    // A live peer answers the greeting, then streams a block plus ~300 KB every 70 ms (~4 MB/s, a busy
    // mainnet stream). The probe stops after MIN_LIVE_BLOCKS instead of reading to its 6 s deadline.
    #[tokio::test]
    async fn a_probe_stops_once_it_has_enough_live_blocks() {
        // probe_live dials <ip>:4001: a loopback address of its own
        let ip = "127.0.0.90";
        let l = TcpListener::bind((ip, 4001)).await.unwrap();
        let sent = Arc::new(AtomicUsize::new(0));
        let counted = sent.clone();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut greet = [0u8; 8];
            s.read_exact(&mut greet).await.unwrap();
            s.write_all(&[0, 0, 0, 1, 0, 3]).await.unwrap();
            let mut round = 1_528_000_000u32;
            loop {
                // a block, then ~300 KB of other stream data (a test block parses only small)
                let payload = crate::protocol::test_block_payload(round, 2);
                let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
                frame.push(1);
                frame.extend_from_slice(&payload);
                frame.extend_from_slice(&300_000u32.to_be_bytes());
                frame.push(0);
                frame.resize(frame.len() + 300_000, 0x5a);
                if s.write_all(&frame).await.is_err() {
                    return;
                }
                counted.fetch_add(frame.len(), Ordering::Relaxed);
                round += 1;
                tokio::time::sleep(Duration::from_millis(70)).await;
            }
        });
        let t0 = Instant::now();
        let p = probe_live(ip).await;
        assert_eq!(p.status, ProbeStatus::Live);
        assert_eq!(p.blocks, MIN_LIVE_BLOCKS);
        assert_eq!(p.max_round, Some(1_528_000_001));
        assert!(t0.elapsed() < Duration::from_secs(1), "{:?}", t0.elapsed());
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mb = sent.load(Ordering::Relaxed) as f64 / 1e6;
        assert!(mb < 3.0, "{mb} MB sent; a 6 s read would be ~25 MB");
    }

    #[test]
    fn peerd_probe_status_peer_full_wins_over_block_count() {
        assert_eq!(
            final_probe_status(2, ProbeStatus::PeerFull),
            ProbeStatus::PeerFull
        );
        assert_eq!(
            final_probe_status(2, ProbeStatus::HeaderTimeout),
            ProbeStatus::Live
        );
        assert_eq!(
            final_probe_status(1, ProbeStatus::HeaderTimeout),
            ProbeStatus::HeaderTimeout
        );
    }
}
