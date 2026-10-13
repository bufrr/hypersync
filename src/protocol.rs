use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::time::timeout;

use crate::gateway::{MIN_COMPLETE_BOOTSTRAP_BYTES, MIN_COMPLETE_BOOTSTRAP_FRAMES};

pub(crate) const GREET_FALSE: [u8; 8] = [0, 0, 0, 3, 0, 0, 0, 0]; // send_abci:false (live blocks; no rate-limited state)
pub(crate) const GREET_TRUE: [u8; 8] = [0, 0, 0, 3, 0, 1, 0, 0]; // send_abci:true (full bootstrap stream)
pub(crate) const MIN_PLAUSIBLE_MAINNET_ROUND: u32 = 1_000_000_000;
// Active-session freshness guard. peerd keeps a wider pool-level lag window so it does not churn the
// whole candidate set, but a peer actively serving one node should not trail the multi-peer clustered
// tip by minutes. 1500 rounds is ~100 seconds at the current mainnet rate, but the reference tip is
// itself stale — peerd rewrites peers.json every cycle (`peerd [interval]`, 120s in compose) and the gateway re-reads it every
// 30s — so the effective threshold is 1500 rounds plus however far the tip has aged, in practice
// ~100-255s at a 120s peerd cycle. Treat this as a floor on how bad a peer must be, not a deadline.
// If peerd stops publishing entirely the frozen tip falls behind every live round and the guard
// simply stops firing, which is the direction we want it to fail.
pub(crate) const ACTIVE_ROUND_MAX_LAG: u32 = 1_500;
// Ceiling slack above the observed network round tip (~4h of rounds at ~14.5/s). The ceiling is
// tip-relative rather than a constant so it keeps rejecting high decode artifacts (e.g. the
// observed 2_818_597_137 misparse) without ever turning into a wall-clock time bomb as the real
// round counter grows past any fixed value.
pub(crate) const NET_TIP_SLACK_ROUNDS: u32 = 200_000;

// `net_tip` is the best-known network round tip (peerd's clustered probe tip via peers.json,
// raised by bootstrap-cache scans); 0 = unknown, which disables the ceiling and falls back to the
// floor-only check (clustering downstream still rejects isolated artifacts). A too-high tip only
// widens *this* ceiling, but it does tighten round_lag_from_tip below, so the tip must never be
// raised from an uncorroborated sample.
pub(crate) fn is_plausible_mainnet_round(round: u32, net_tip: u32) -> bool {
    round >= MIN_PLAUSIBLE_MAINNET_ROUND
        && (net_tip == 0 || round <= net_tip.saturating_add(NET_TIP_SLACK_ROUNDS))
}

pub(crate) fn round_lag_from_tip(round: u32, net_tip: u32, max_lag: u32) -> Option<u32> {
    if net_tip == 0 || round >= net_tip {
        return None;
    }
    let lag = net_tip - round;
    (lag > max_lag).then_some(lag)
}

// Live block round (wire format as of 2026-10 mainnet). The decompressed block opens with a
// variable-length header (8 x [0xfd + u64] entries, then a run of 32-byte entries whose count
// varies per block), so the round no longer sits at a fixed offset (it was 0x5e until mid-2026;
// it is now seen anywhere in 0x71..0xd1). It is the first `0xfc R 0xfc R1 0xfc R2` varint triple
// where R1 and R2 are the preceding rounds (the parent / QC chain): R1 < R, R2 < R1, each within
// MAX_PARENT_ROUND_GAP. The parent check rejects look-alike triples inside hashes (one sits right
// before the real round in captured blocks). A frame that doesn't match yields None, which every
// caller treats as "not a block": forwarded untouched, never dropped.
//
// Round parsing assumes the `0xfc + u32 LE` varint form. Mainnet rounds (~1.48B, +~14.5/s) stay
// under u32::MAX for roughly 6 more years; past that the wire varint becomes `0xfd + u64`, these
// parsers return None, and dedup gracefully degrades to forward-everything (the node de-dups).
const ROUND_SCAN_BYTES: usize = 0x400;
// Timed-out rounds leave gaps in the parent chain; keep the tolerance well above anything seen.
const MAX_PARENT_ROUND_GAP: u32 = 256;

fn find_block_round(dec: &[u8]) -> Option<u32> {
    let rd = |o: usize| u32::from_le_bytes([dec[o + 1], dec[o + 2], dec[o + 3], dec[o + 4]]);
    let end = dec.len().min(ROUND_SCAN_BYTES);
    let mut o = 0usize;
    while o + 15 <= end {
        if dec[o] == 0xfc && dec[o + 5] == 0xfc && dec[o + 10] == 0xfc {
            let (r, r1, r2) = (rd(o), rd(o + 5), rd(o + 10));
            if r1 < r
                && r - r1 <= MAX_PARENT_ROUND_GAP
                && r2 < r1
                && r1 - r2 <= MAX_PARENT_ROUND_GAP
            {
                return Some(r);
            }
        }
        o += 1;
    }
    None
}

// Reference / correctness oracle + fallback: full lz4 decompress, then the same header scan.
pub(crate) fn block_round_full(payload: &[u8]) -> Option<u32> {
    let dec = lz4_flex::block::decompress_size_prepended(payload).ok()?;
    find_block_round(&dec)
}

// Bounded LZ4 block decode: produce only the first `out.len()` bytes (the round lives in the
// first ROUND_SCAN_BYTES, so we never decompress the full ~250KB block). Standard LZ4 block format; returns bytes produced, or
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
    // skip the u32-LE uncompressed-size prefix; decode only the header we scan (blocks run to
    // ~250KB decompressed)
    let mut buf = [0u8; ROUND_SCAN_BYTES];
    match lz4_first_n(&payload[4..], &mut buf) {
        Some(n) => find_block_round(&buf[..n]),
        None => block_round_full(payload),
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

    pub(crate) fn last_forwarded(&self) -> u32 {
        self.last_forwarded
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

// A live-block payload in the 2026-10 wire layout: 0x00, 8 x [0xfd + u64], a count byte,
// `pad32` 32-byte entries, then `0xfc R 0xfc R-1 0xfc R-2` and filler. lz4 size-prepended.
#[cfg(test)]
pub(crate) fn test_block_payload(round: u32, pad32: usize) -> Vec<u8> {
    let mut dec = vec![0x00u8];
    for i in 0..8u8 {
        dec.push(0xfd);
        dec.extend_from_slice(&[i.wrapping_mul(37).wrapping_add(1); 8]);
    }
    dec.push(0x1c);
    for i in 0..pad32 {
        dec.extend((0..32).map(|j| (i * 32 + j) as u8 ^ 0x5a));
    }
    for r in [round, round.wrapping_sub(1), round.wrapping_sub(2)] {
        dec.push(0xfc);
        dec.extend_from_slice(&r.to_le_bytes());
    }
    dec.extend((0..0x200).map(|i| (i % 251) as u8));
    lz4_flex::block::compress_prepend_size(&dec)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block(round: u32) -> Vec<u8> {
        test_block_payload(round, 2)
    }

    fn make_frame(typ: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.push(typ);
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn block_round_matches_full_decompress() {
        for round in [2u32, 1000, 1_055_000_000, 1_483_968_338, u32::MAX] {
            for pad32 in [0, 2, 3, 5, 20] {
                let payload = test_block_payload(round, pad32);
                assert_eq!(
                    block_round_full(&payload),
                    Some(round),
                    "full @ {round}/{pad32}"
                );
                assert_eq!(
                    block_round(&payload),
                    Some(round),
                    "bounded @ {round}/{pad32}"
                );
            }
        }
    }

    // A real 2026-10 mainnet live block (smallest frame of a 20s capture). Its header has a
    // look-alike varint triple at 0x6b that fails the parent check; the round is at 0x70.
    #[test]
    fn block_round_decodes_captured_mainnet_block() {
        let payload = include_bytes!("testdata/block-2026-10.lz4");
        assert_eq!(block_round_full(payload), Some(1_483_968_338));
        assert_eq!(block_round(payload), Some(1_483_968_338));
    }

    #[test]
    fn block_round_rejects_triples_without_a_parent_chain() {
        let mut dec = vec![0x11u8; 0x200];
        for (o, r) in [
            (0x80usize, 1_400_000_000u32),
            (0x85, 1_500_000_000),
            (0x8a, 1_399_999_999),
        ] {
            dec[o] = 0xfc;
            dec[o + 1..o + 5].copy_from_slice(&r.to_le_bytes());
        }
        let payload = lz4_flex::block::compress_prepend_size(&dec);
        assert_eq!(block_round_full(&payload), None);
        assert_eq!(block_round(&payload), None);
        // old-format blocks (round alone at 0x5e) are no longer recognised
        let mut old = vec![0x22u8; 0x100];
        old[0x5e] = 0xfc;
        old[0x5f..0x63].copy_from_slice(&1_400_000_000u32.to_le_bytes());
        assert_eq!(
            block_round(&lz4_flex::block::compress_prepend_size(&old)),
            None
        );
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
    fn round_lag_from_tip_ignores_unknown_or_close_tip() {
        assert_eq!(round_lag_from_tip(1_356_000_000, 0, 5_000), None);
        assert_eq!(
            round_lag_from_tip(1_356_000_000, 1_356_005_000, 5_000),
            None
        );
        assert_eq!(
            round_lag_from_tip(1_356_000_000, 1_356_005_001, 5_000),
            Some(5_001)
        );
        assert_eq!(
            round_lag_from_tip(1_356_010_000, 1_356_005_000, 5_000),
            None
        );
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
        // A repetitive header compresses to back-references, so the bounded decoder
        // lz4_first_n -- including its overlapping match-copy loop -- does the work.
        for round in [2u32, 123_456_789, u32::MAX] {
            let mut dec = vec![0xAAu8; 0x300];
            let o = 0xb1;
            for (k, r) in [round, round - 1, round - 2].into_iter().enumerate() {
                dec[o + 5 * k] = 0xfc;
                dec[o + 5 * k + 1..o + 5 * k + 5].copy_from_slice(&r.to_le_bytes());
            }
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
