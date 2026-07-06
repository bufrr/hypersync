use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::time::timeout;

use crate::gateway::{MIN_COMPLETE_BOOTSTRAP_BYTES, MIN_COMPLETE_BOOTSTRAP_FRAMES};

pub(crate) const GREET_FALSE: [u8; 8] = [0, 0, 0, 3, 0, 0, 0, 0]; // send_abci:false (live blocks; no rate-limited state)
pub(crate) const GREET_TRUE: [u8; 8] = [0, 0, 0, 3, 0, 1, 0, 0]; // send_abci:true (full bootstrap stream)
pub(crate) const MIN_PLAUSIBLE_MAINNET_ROUND: u32 = 1_000_000_000;
// Ceiling slack above the observed network round tip (~4h of rounds at ~14.5/s). The ceiling is
// tip-relative rather than a constant so it keeps rejecting high decode artifacts (e.g. the
// observed 2_818_597_137 misparse) without ever turning into a wall-clock time bomb as the real
// round counter grows past any fixed value.
pub(crate) const NET_TIP_SLACK_ROUNDS: u32 = 200_000;

// `net_tip` is the best-known network round tip (peerd's clustered probe tip via peers.json,
// raised by bootstrap-cache scans); 0 = unknown, which disables the ceiling and falls back to the
// floor-only check (clustering downstream still rejects isolated artifacts). A too-high tip only
// widens the ceiling — it can never block legitimate rounds.
pub(crate) fn is_plausible_mainnet_round(round: u32, net_tip: u32) -> bool {
    round >= MIN_PLAUSIBLE_MAINNET_ROUND
        && (net_tip == 0 || round <= net_tip.saturating_add(NET_TIP_SLACK_ROUNDS))
}

// Reference / correctness oracle + fallback: full lz4 decompress, read round @0x5e.
// Round parsing assumes the `0xfc + u32 LE` varint form. Mainnet rounds (~1.35B, +~14.5/s) stay
// under u32::MAX for roughly 6 more years; past that the wire varint becomes `0xfd + u64`, these
// parsers return None, and dedup gracefully degrades to forward-everything (the node de-dups).
pub(crate) fn block_round_full(payload: &[u8]) -> Option<u32> {
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
pub(crate) fn block_round(payload: &[u8]) -> Option<u32> {
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

pub(crate) struct RoundDedup {
    // slot[r % cap] = most recent round that mapped to that slot. Lock-free: an atomic swap is O(1)
    // with no mutex. Consensus rounds are ~sequential, so this is a hash-free sliding window of the
    // last ~cap rounds. A rare race (two threads swap the same r) only re-forwards one block, which
    // the node de-dups anyway ("received old client block"), so it's harmless.
    slots: Vec<AtomicU32>,
    mask: usize,
}
impl RoundDedup {
    pub(crate) fn new(cap: usize) -> Self {
        let cap = cap.next_power_of_two(); // power-of-two so `% cap` becomes a single-cycle `& mask`
        Self {
            slots: (0..cap).map(|_| AtomicU32::new(0)).collect(),
            mask: cap - 1,
        }
    }
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn is_new(&self, r: u32) -> bool {
        self.slots[(r as usize) & self.mask].swap(r, Ordering::Relaxed) != r
    }
}

pub(crate) enum HeaderRead {
    Complete,
    TimedOut(usize),
}

pub(crate) async fn read_header_or_timeout<R: AsyncRead + Unpin>(
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

pub(crate) async fn finish_header_after_partial<R: AsyncRead + Unpin>(
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

pub(crate) fn complete_bootstrap_at_frame_boundary(bytes: usize, frames: u64) -> bool {
    bytes >= MIN_COMPLETE_BOOTSTRAP_BYTES && frames >= MIN_COMPLETE_BOOTSTRAP_FRAMES
}

pub(crate) fn complete_frame_count(buf: &[u8]) -> Option<u64> {
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

pub(crate) fn max_block_round_in_frames(blob: &[u8]) -> Option<u32> {
    const MAX_LIVE_BLOCK_FRAME: usize = 2_000_000;
    const MIN_ROUND_CLUSTER: usize = 4;
    const MAX_ROUND_CLUSTER_GAP: u32 = 10_000;
    let mut off = 0usize;
    let mut rounds = Vec::new();
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
                // floor-only prefilter (no tip available here); the cluster scan below rejects
                // isolated high artifacts
                if is_plausible_mainnet_round(round, 0) {
                    rounds.push(round);
                }
            }
        }
        off += len;
    }
    max_clustered_round(&mut rounds, MIN_ROUND_CLUSTER, MAX_ROUND_CLUSTER_GAP)
}

pub(crate) fn max_clustered_round(
    rounds: &mut [u32],
    min_cluster: usize,
    max_gap: u32,
) -> Option<u32> {
    if rounds.len() < min_cluster || min_cluster == 0 {
        return None;
    }
    rounds.sort_unstable();
    let mut best_len = 1usize;
    let mut best_max = rounds[0];
    let mut cur_len = 1usize;
    let mut cur_max = rounds[0];
    for pair in rounds.windows(2) {
        if pair[1].saturating_sub(pair[0]) <= max_gap {
            cur_len += 1;
            cur_max = pair[1];
        } else {
            if cur_len > best_len || (cur_len == best_len && cur_max > best_max) {
                best_len = cur_len;
                best_max = cur_max;
            }
            cur_len = 1;
            cur_max = pair[1];
        }
    }
    if cur_len > best_len || (cur_len == best_len && cur_max > best_max) {
        best_len = cur_len;
        best_max = cur_max;
    }
    (best_len >= min_cluster).then_some(best_max)
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

pub(crate) struct RoundForwardGate {
    last_forwarded: u32,
    dedup: RoundDedup,
}

impl RoundForwardGate {
    pub(crate) fn with_last_forwarded(cap: usize, last_forwarded: u32) -> Self {
        Self {
            last_forwarded,
            dedup: RoundDedup::new(cap),
        }
    }

    pub(crate) fn should_forward(&mut self, round: u32, from_active: bool) -> bool {
        should_forward_block_round(round, from_active, &mut self.last_forwarded, &self.dedup)
    }
}

pub(crate) fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
        blob.extend_from_slice(&make_frame(1, &make_block(1_055_000_010)));
        blob.extend_from_slice(&make_frame(1, &make_block(1_055_000_011)));
        blob.extend_from_slice(&make_frame(1, &vec![0u8; 2_000_001]));
        blob.extend_from_slice(&make_frame(1, &make_block(1_055_000_012)));
        blob.extend_from_slice(&make_frame(1, &make_block(1_055_000_013)));
        assert_eq!(max_block_round_in_frames(&blob), Some(1_055_000_013));

        let truncated = vec![0, 0, 0, 10, 1, 1, 2];
        assert_eq!(max_block_round_in_frames(&truncated), None);

        let mut isolated_false_positive = Vec::new();
        isolated_false_positive.extend_from_slice(&make_frame(1, &make_block(2_818_597_137)));
        assert_eq!(max_block_round_in_frames(&isolated_false_positive), None);

        let mut outlier = Vec::new();
        for round in [
            1_055_000_020,
            1_055_000_021,
            1_055_000_022,
            1_055_000_023,
            2_818_597_137,
        ] {
            outlier.extend_from_slice(&make_frame(1, &make_block(round)));
        }
        assert_eq!(max_block_round_in_frames(&outlier), Some(1_055_000_023));
    }

    #[test]
    fn plausible_mainnet_round_rejects_low_and_high_decode_artifacts() {
        let tip = 1_356_000_000;
        // floor applies regardless of tip knowledge
        assert!(!is_plausible_mainnet_round(
            MIN_PLAUSIBLE_MAINNET_ROUND - 1,
            tip
        ));
        assert!(!is_plausible_mainnet_round(770_000_000, 0));
        assert!(is_plausible_mainnet_round(MIN_PLAUSIBLE_MAINNET_ROUND, tip));
        // ceiling is tip-relative: tip + slack passes, beyond it fails
        assert!(is_plausible_mainnet_round(tip + NET_TIP_SLACK_ROUNDS, tip));
        assert!(!is_plausible_mainnet_round(
            tip + NET_TIP_SLACK_ROUNDS + 1,
            tip
        ));
        assert!(!is_plausible_mainnet_round(2_818_597_137, tip));
        // unknown tip disables the ceiling (never blocks legitimate growth), keeps the floor
        assert!(is_plausible_mainnet_round(2_818_597_137, 0));
        assert!(is_plausible_mainnet_round(u32::MAX, 0));
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
}
