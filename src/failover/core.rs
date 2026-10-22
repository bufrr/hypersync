use crate::priority::redact;
use crate::protocol::{block_round_parent, decompress_block, unix_secs};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

#[allow(unused_imports)]
use super::*;

// A standby must match the active on this many blocks (since its last mismatch) before it may take
// over, also when the active is dead: a standby connected for even a second has far more.
const MIN_VERIFIED: u32 = 3;

// Consecutive blocks a standby may disagree with the active on before it is dropped. One mismatch
// only resets its verification: the active may be the one that is wrong.
pub(super) const MAX_MISMATCHES: u32 = 3;

// Lead samples (active arrival - standby arrival, per common round) kept per standby.
const LEAD_WINDOW: usize = 40;

const LEAD_MIN_SAMPLES: usize = 20;

// Forwarded-round hashes kept for verifying slower standbys (~70s of blocks).
const FORWARDED_KEEP: usize = 1024;

// Blocks (and bytes) a standby may hold ahead of the node. Past either, its HIGHEST rounds are
// discarded: the lowest ones continue the node's chain, which is what a switch replays.
const AHEAD_KEEP: usize = 512;

const AHEAD_BYTES: usize = 32 << 20;

// ---- decision core (pure, no I/O) ----

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SwitchReason {
    Stall,
    Lag,
    ActiveDead,
    // back to an operator priority peer from a pool active
    Preferred,
}

impl SwitchReason {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Stall => "stall",
            Self::Lag => "lag",
            Self::ActiveDead => "active-dead",
            Self::Preferred => "preferred",
        }
    }
}

struct Ahead {
    parent: u32,
    hash: u64,
    at: Instant,
    frame: Vec<u8>,
}

pub(super) struct Upstream {
    pub(super) ip: String,
    // blocks matched since the last mismatch
    pub(super) verified: u32,
    // consecutive blocks that differed from the active's
    mismatches: u32,
    // blocks this upstream delivered that the node has not received yet, by round
    ahead: BTreeMap<u32, Ahead>,
    ahead_bytes: usize,
    // highest round trimmed from `ahead` for space. The reader has moved past it, so until the
    // node has that round from the active, the buffered chain has a hole after its last block and
    // this standby can't take over (a switch would hand the node a block with an unseen parent).
    dropped_max: u32,
    // ms the active was later than this standby, per common round (negative = standby later)
    leads: VecDeque<i64>,
    last_rx: Option<Instant>,
    // an operator priority peer: the session moves back to one once it is verified (see
    // pick_switch), and lag switches never leave one for a pool peer
    preferred: bool,
}

impl Upstream {
    fn new(ip: String) -> Self {
        Self {
            ip,
            verified: 0,
            mismatches: 0,
            ahead: BTreeMap::new(),
            ahead_bytes: 0,
            dropped_max: 0,
            leads: VecDeque::new(),
            last_rx: None,
            preferred: false,
        }
    }

    // Record one comparison against the active's block. Returns true once the standby has
    // disagreed MAX_MISMATCHES times in a row (drop it).
    fn compared(&mut self, matched: bool, lead_ms: i64) -> bool {
        if matched {
            self.verified += 1;
            self.mismatches = 0;
            self.push_lead(lead_ms);
            false
        } else {
            self.verified = 0;
            self.mismatches += 1;
            self.mismatches >= MAX_MISMATCHES
        }
    }

    // Buffer a block, then trim the highest rounds back under AHEAD_KEEP / AHEAD_BYTES.
    fn insert_ahead(&mut self, round: u32, a: Ahead) {
        self.ahead_bytes += a.frame.len();
        if let Some(old) = self.ahead.insert(round, a) {
            self.ahead_bytes -= old.frame.len();
        }
        while self.ahead.len() > AHEAD_KEEP || self.ahead_bytes > AHEAD_BYTES {
            let Some((r, a)) = self.ahead.pop_last() else {
                break;
            };
            self.ahead_bytes -= a.frame.len();
            self.dropped_max = self.dropped_max.max(r);
        }
    }

    fn take_ahead(&mut self, round: u32) -> Option<Ahead> {
        let a = self.ahead.remove(&round)?;
        self.ahead_bytes -= a.frame.len();
        Some(a)
    }

    // Discard buffered blocks at/below `floor` (the node already has that round).
    fn trim_ahead(&mut self, floor: u32) {
        while self
            .ahead
            .first_key_value()
            .is_some_and(|(&r, _)| r <= floor)
        {
            if let Some((_, a)) = self.ahead.pop_first() {
                self.ahead_bytes -= a.frame.len();
            }
        }
    }

    fn push_lead(&mut self, ms: i64) {
        if self.leads.len() == LEAD_WINDOW {
            self.leads.pop_front();
        }
        self.leads.push_back(ms);
    }

    pub(super) fn median_lead(&self) -> Option<i64> {
        if self.leads.len() < LEAD_MIN_SAMPLES {
            return None;
        }
        let mut v: Vec<i64> = self.leads.iter().copied().collect();
        v.sort_unstable();
        Some(v[v.len() / 2])
    }

    // Blocks this upstream can hand the node next, gap-free from `from` by parent links.
    // Nothing this standby dropped is still missing at the node (whose last block is `from`).
    fn can_continue(&self, from: u32) -> bool {
        self.dropped_max <= from
    }

    fn chain_len(&self, from: u32) -> (usize, Option<Instant>) {
        if !self.can_continue(from) {
            return (0, None);
        }
        let mut cur = from;
        let mut n = 0;
        let mut first_at = None;
        for (&r, a) in self.ahead.range(from.saturating_add(1)..) {
            if a.parent != cur {
                break;
            }
            if first_at.is_none() {
                first_at = Some(a.at);
            }
            cur = r;
            n += 1;
        }
        (n, first_at)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StandbyVerdict {
    Ok,
    // the standby delivered a different block than the active for the same round
    Mismatch(u32),
}

// switch_to result: the frames to send the node, and the standbys found disagreeing on the way.
type SwitchFrames = (Vec<Vec<u8>>, Vec<(u64, u32)>);

pub(crate) struct Core {
    pub(crate) last_forwarded: u32,
    forwarded: BTreeMap<u32, (u64, Instant)>,
    active: u64,
    pub(super) ups: HashMap<u64, Upstream>,
    last_switch: Option<Instant>,
    stall: Duration,
    lag: Duration,
    min_dwell: Duration,
}

impl Core {
    pub(crate) fn new(
        active: u64,
        active_ip: String,
        last_forwarded: u32,
        stall: Duration,
        lag: Duration,
        min_dwell: Duration,
    ) -> Self {
        let mut ups = HashMap::new();
        ups.insert(active, Upstream::new(active_ip));
        Self {
            last_forwarded,
            forwarded: BTreeMap::new(),
            active,
            ups,
            last_switch: None,
            stall,
            lag,
            min_dwell,
        }
    }

    pub(crate) fn add_standby(&mut self, id: u64, ip: String) {
        self.ups.insert(id, Upstream::new(ip));
    }

    // Mark an upstream (the active or a standby) as an operator priority peer.
    pub(crate) fn mark_preferred(&mut self, id: u64) {
        if let Some(u) = self.ups.get_mut(&id) {
            u.preferred = true;
        }
    }

    pub(crate) fn remove(&mut self, id: u64) -> Option<String> {
        self.ups.remove(&id).map(|u| u.ip)
    }

    fn record_forwarded(&mut self, round: u32, hash: u64, at: Instant) {
        self.last_forwarded = round;
        self.forwarded.insert(round, (hash, at));
        while self.forwarded.len() > FORWARDED_KEEP {
            self.forwarded.pop_first();
        }
    }

    // Check every standby's buffered copy of a just-forwarded round against it, and drop buffered
    // blocks the node already has. Returns standbys that disagreed MAX_MISMATCHES times in a row.
    fn settle_standbys(&mut self, round: u32, hash: u64, at: Instant) -> Vec<(u64, u32)> {
        let mut bad = Vec::new();
        let floor = self.last_forwarded;
        for (&id, u) in self.ups.iter_mut() {
            if id == self.active {
                continue;
            }
            if let Some(a) = u.take_ahead(round) {
                let lead = at.saturating_duration_since(a.at).as_millis() as i64;
                if u.compared(a.hash == hash, lead) {
                    bad.push((id, round));
                }
            }
            // anything at/below the node's tip that we didn't match is no longer useful
            u.trim_ahead(floor);
        }
        bad
    }

    // A block from the ACTIVE upstream. Returns whether to forward it, plus standbys that turned
    // out to disagree with it.
    pub(crate) fn on_active_block(
        &mut self,
        round: u32,
        hash: u64,
        at: Instant,
    ) -> (bool, Vec<(u64, u32)>) {
        if let Some(u) = self.ups.get_mut(&self.active) {
            u.last_rx = Some(at);
        }
        if self.last_forwarded != 0 && round <= self.last_forwarded {
            return (false, Vec::new());
        }
        self.record_forwarded(round, hash, at);
        let bad = self.settle_standbys(round, hash, at);
        (true, bad)
    }

    // A block from a STANDBY upstream: verify it against the forwarded chain, or buffer it.
    pub(crate) fn on_standby_block(
        &mut self,
        id: u64,
        round: u32,
        parent: u32,
        hash: u64,
        at: Instant,
        frame: Vec<u8>,
    ) -> StandbyVerdict {
        let last = self.last_forwarded;
        let fwd = self.forwarded.get(&round).copied();
        let Some(u) = self.ups.get_mut(&id) else {
            return StandbyVerdict::Ok;
        };
        u.last_rx = Some(at);
        if round <= last {
            if let Some((h, fat)) = fwd {
                let lead = -(at.saturating_duration_since(fat).as_millis() as i64);
                if u.compared(h == hash, lead) {
                    return StandbyVerdict::Mismatch(round);
                }
            }
            return StandbyVerdict::Ok;
        }
        u.insert_ahead(
            round,
            Ahead {
                parent,
                hash,
                at,
                frame,
            },
        );
        StandbyVerdict::Ok
    }

    // Which standby to switch to right now, if any.
    pub(crate) fn pick_switch(
        &self,
        now: Instant,
        active_dead: bool,
    ) -> Option<(u64, SwitchReason)> {
        if self.last_forwarded == 0 {
            return None;
        }
        // stall candidates rank by measured speed (median lead over the active), then by how much
        // of the chain they already hold — a momentarily long buffer alone can come from a peer
        // that is steadily slower than the others
        let mut stall_best: Option<(u64, i64, usize, Instant)> = None;
        let mut lag_best: Option<(u64, i64)> = None;
        // back to a priority peer: the fastest verified one that continues the chain
        let mut preferred_best: Option<(u64, i64)> = None;
        let dwell_ok = self
            .last_switch
            .is_none_or(|t| now.saturating_duration_since(t) >= self.min_dwell);
        let active_preferred = self.ups.get(&self.active).is_some_and(|u| u.preferred);
        for (&id, u) in &self.ups {
            if id == self.active || u.verified < MIN_VERIFIED {
                continue;
            }
            let (n, first_at) = u.chain_len(self.last_forwarded);
            let Some(first_at) = first_at else { continue };
            let held = now.saturating_duration_since(first_at);
            if active_dead || held >= self.stall {
                let lead = u.median_lead().unwrap_or(i64::MIN);
                let better = match stall_best {
                    None => true,
                    Some((_, bl, bn, bat)) => {
                        (lead, n, std::cmp::Reverse(first_at)) > (bl, bn, std::cmp::Reverse(bat))
                    }
                };
                if better {
                    stall_best = Some((id, lead, n, first_at));
                }
            }
            if dwell_ok && u.preferred && !active_preferred {
                let lead = u.median_lead().unwrap_or(i64::MIN);
                if preferred_best.is_none_or(|(_, bl)| lead > bl) {
                    preferred_best = Some((id, lead));
                }
            }
            // a priority active is only left for speed by another priority peer (otherwise a
            // slightly faster pool standby and the preferred switch would trade the node back and
            // forth every dwell); stall/dead switches above still go wherever the chain continues
            if dwell_ok && (u.preferred || !active_preferred) {
                if let Some(m) = u.median_lead() {
                    if m >= self.lag.as_millis() as i64 && lag_best.is_none_or(|(_, bm)| m > bm) {
                        lag_best = Some((id, m));
                    }
                }
            }
        }
        if let Some((id, _, _, _)) = stall_best {
            let reason = if active_dead {
                SwitchReason::ActiveDead
            } else {
                SwitchReason::Stall
            };
            return Some((id, reason));
        }
        if let Some((id, _)) = preferred_best {
            return Some((id, SwitchReason::Preferred));
        }
        lag_best.map(|(id, _)| (id, SwitchReason::Lag))
    }

    // Make `id` the active upstream. Returns the frames to send the node (the standby's buffered
    // chain continuing exactly from the node's last block) and standbys found disagreeing on the
    // way, or None if `id` cannot continue the chain gap-free (nothing is changed then).
    pub(crate) fn switch_to(&mut self, id: u64, now: Instant) -> Option<SwitchFrames> {
        let u = self.ups.get_mut(&id)?;
        let mut cur = self.last_forwarded;
        if !u.can_continue(cur) {
            return None;
        }
        let mut chain = Vec::new();
        while let Some((&r, a)) = u.ahead.range(cur.saturating_add(1)..).next() {
            if a.parent != cur {
                break;
            }
            let a = u.take_ahead(r).expect("present");
            chain.push((r, a));
            cur = r;
        }
        if chain.is_empty() {
            return None;
        }
        self.active = id;
        self.last_switch = Some(now);
        let mut frames = Vec::with_capacity(chain.len());
        let mut bad = Vec::new();
        for (r, a) in chain {
            self.record_forwarded(r, a.hash, a.at);
            bad.extend(self.settle_standbys(r, a.hash, a.at));
            frames.push(a.frame);
        }
        // leads were measured against the old active
        for u in self.ups.values_mut() {
            u.leads.clear();
        }
        Some((frames, bad))
    }

    // The standby trailing the active the most, if its median lag exceeds `slower_ms` (standbys
    // `exempt` returns true for — the operator's priority peers — are never picked).
    pub(crate) fn slowest_standby(
        &self,
        slower_ms: i64,
        exempt: impl Fn(&str) -> bool,
    ) -> Option<(u64, i64)> {
        self.ups
            .iter()
            .filter(|(&id, u)| id != self.active && !exempt(&u.ip))
            .filter_map(|(&id, u)| u.median_lead().map(|m| (id, m)))
            .filter(|&(_, m)| m < -slower_ms)
            .min_by_key(|&(_, m)| m)
    }

    pub(crate) fn active(&self) -> u64 {
        self.active
    }

    pub(super) fn ip_of(&self, id: u64) -> Option<&str> {
        self.ups.get(&id).map(|u| u.ip.as_str())
    }

    pub(super) fn standby_count(&self) -> usize {
        self.ups.len().saturating_sub(1)
    }

    pub(super) fn status_json(
        &self,
        node: &str,
        switches: u64,
        last: &str,
        now: Instant,
    ) -> String {
        let mut sb: Vec<(&u64, &Upstream)> = self
            .ups
            .iter()
            .filter(|(&id, _)| id != self.active)
            .collect();
        sb.sort_by(|a, b| a.1.ip.cmp(&b.1.ip));
        let ms_ago = |t: Option<Instant>| {
            t.map(|t| now.saturating_duration_since(t).as_millis().to_string())
                .unwrap_or_else(|| "null".into())
        };
        let standbys: Vec<String> = sb
            .iter()
            .map(|(_, u)| {
                format!(
                    "{{\"ip\":\"{}\",\"preferred\":{},\"verified\":{},\"lead_ms_p50\":{},\"lead_samples\":{},\"ahead\":{},\"last_rx_ms\":{}}}",
                    redact(&u.ip),
                    u.preferred,
                    u.verified,
                    u.median_lead().map(|m| m.to_string()).unwrap_or_else(|| "null".into()),
                    u.leads.len(),
                    u.ahead.len(),
                    ms_ago(u.last_rx),
                )
            })
            .collect();
        let active = self.ups.get(&self.active);
        format!(
            "{{\"node\":\"{node}\",\"updated\":{},\"active\":\"{}\",\"active_last_rx_ms\":{},\"last_forwarded\":{},\"switches\":{switches},\"last_switch\":\"{}\",\"standbys\":[{}]}}",
            unix_secs(),
            active.map(|u| redact(&u.ip)).unwrap_or_default(),
            ms_ago(active.and_then(|u| u.last_rx)),
            self.last_forwarded,
            last.replace('"', "'"),
            standbys.join(",")
        )
    }
}

// Hash of a live block's DECOMPRESSED content, so peers that compress the same block differently
// still match. Keyed per process: a peer can't precompute a block that collides with another.
// A payload that doesn't decompress is hashed as sent (identical bytes still match).
fn block_hash(frame: &[u8]) -> u64 {
    static KEYS: OnceLock<RandomState> = OnceLock::new();
    let mut h = KEYS.get_or_init(RandomState::new).build_hasher();
    let payload = &frame[5..];
    match decompress_block(payload) {
        Some(dec) => {
            h.write_u8(1);
            h.write(&dec);
        }
        None => {
            h.write_u8(0);
            h.write(frame);
        }
    }
    h.finish()
}

pub(super) fn classify(frame: &[u8]) -> Option<(u32, u32, u64)> {
    if frame.len() <= 5 || frame[4] != 1 {
        return None;
    }
    let (r, p) = block_round_parent(&frame[5..])?;
    Some((r, p, block_hash(frame)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn core() -> Core {
        Core::new(0, "a".into(), 0, 400 * MS, 150 * MS, 10_000 * MS)
    }

    fn f(r: u32) -> Vec<u8> {
        r.to_le_bytes().to_vec()
    }

    // active delivers rounds [from..=to] at t, each standby 'b' copy arriving `b_off` relative
    fn feed(c: &mut Core, from: u32, to: u32, t0: Instant, b: u64, b_off_ms: i64) {
        for r in from..=to {
            let ta = t0 + (r - from) * 70 * MS;
            let tb = if b_off_ms >= 0 {
                ta + (b_off_ms as u32) * MS
            } else {
                ta - ((-b_off_ms) as u32) * MS
            };
            if b_off_ms < 0 {
                assert_eq!(
                    c.on_standby_block(b, r, r - 1, r as u64, tb, f(r)),
                    StandbyVerdict::Ok
                );
                assert!(c.on_active_block(r, r as u64, ta).0);
            } else {
                assert!(c.on_active_block(r, r as u64, ta).0);
                assert_eq!(
                    c.on_standby_block(b, r, r - 1, r as u64, tb, f(r)),
                    StandbyVerdict::Ok
                );
            }
        }
    }

    #[test]
    fn active_forwards_new_rounds_only() {
        let mut c = core();
        let t = Instant::now();
        assert!(c.on_active_block(100, 1, t).0);
        assert!(!c.on_active_block(100, 1, t).0);
        assert!(!c.on_active_block(99, 1, t).0);
        assert!(c.on_active_block(101, 1, t).0);
        assert_eq!(c.last_forwarded, 101);
    }

    #[test]
    fn stall_switch_replays_gap_free_chain() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        feed(&mut c, 100, 110, t0, 1, 30); // b slightly slower, verified 11x
                                           // active stalls; b delivers 111..=114
        let t1 = t0 + 2000 * MS;
        for r in 111..=114 {
            c.on_standby_block(1, r, r - 1, r as u64, t1, f(r));
        }
        assert_eq!(
            c.pick_switch(t1 + 100 * MS, false),
            None,
            "not held long enough"
        );
        let (id, why) = c.pick_switch(t1 + 400 * MS, false).expect("stall switch");
        assert_eq!((id, why), (1, SwitchReason::Stall));
        let (frames, bad) = c.switch_to(1, t1 + 400 * MS).unwrap();
        assert!(bad.is_empty());
        assert_eq!(frames, (111..=114).map(f).collect::<Vec<_>>());
        assert_eq!(c.last_forwarded, 114);
        assert_eq!(c.active(), 1);
        // new active continues; old rounds are deduped
        assert!(!c.on_active_block(114, 114, t1).0);
        assert!(c.on_active_block(115, 115, t1).0);
    }

    #[test]
    fn no_switch_across_a_gap() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        feed(&mut c, 100, 110, t0, 1, 30);
        // b has 112.. but not 111: switching would hand the node a block with an unseen parent
        let t1 = t0 + 2000 * MS;
        for r in 112..=114 {
            c.on_standby_block(1, r, r - 1, r as u64, t1, f(r));
        }
        assert_eq!(c.pick_switch(t1 + 5000 * MS, true), None);
        assert!(c.switch_to(1, t1).is_none());
        assert_eq!(c.active(), 0);
    }

    #[test]
    fn parent_links_allow_skipped_rounds() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        feed(&mut c, 100, 110, t0, 1, 30);
        let t1 = t0 + 2000 * MS;
        // round 111 timed out: 112's parent is 110
        c.on_standby_block(1, 112, 110, 112, t1, f(112));
        c.on_standby_block(1, 113, 112, 113, t1, f(113));
        let (frames, _) = c.switch_to(1, t1).unwrap();
        assert_eq!(frames, vec![f(112), f(113)]);
    }

    #[test]
    fn unverified_standby_cannot_take_over_even_a_dead_active() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        assert!(c.on_active_block(100, 100, t0).0);
        c.on_standby_block(1, 100, 99, 100, t0, f(100)); // 1 verification
        c.on_standby_block(1, 101, 100, 101, t0, f(101));
        assert_eq!(c.pick_switch(t0 + 5000 * MS, false), None);
        assert_eq!(c.pick_switch(t0, true), None, "MIN_VERIFIED also when dead");
        // the active delivers 101 (settling the buffered copy: 2 verified) and then dies
        assert!(c.on_active_block(101, 101, t0).0);
        c.on_standby_block(1, 102, 101, 102, t0, f(102));
        assert_eq!(c.pick_switch(t0, true), None, "2 < MIN_VERIFIED");
        assert!(c.on_active_block(102, 102, t0).0);
        c.on_standby_block(1, 103, 102, 103, t0, f(103));
        assert_eq!(c.ups[&1].verified, MIN_VERIFIED);
        assert_eq!(c.pick_switch(t0, true), Some((1, SwitchReason::ActiveDead)));
    }

    #[test]
    fn standby_is_dropped_only_after_consecutive_mismatches() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        // standby behind the active: compared against the forwarded hashes
        let standby = |c: &mut Core, r: u32, h: u64| {
            assert!(c.on_active_block(r, r as u64, t0).0);
            c.on_standby_block(1, r, r - 1, h, t0, f(r))
        };
        assert_eq!(standby(&mut c, 100, 100), StandbyVerdict::Ok);
        assert_eq!(standby(&mut c, 101, 999), StandbyVerdict::Ok);
        assert_eq!(c.ups[&1].verified, 0, "a mismatch resets verification");
        assert_eq!(standby(&mut c, 102, 102), StandbyVerdict::Ok);
        assert_eq!(c.ups[&1].mismatches, 0, "a match resets the run");
        assert_eq!(standby(&mut c, 103, 999), StandbyVerdict::Ok);
        assert_eq!(standby(&mut c, 104, 999), StandbyVerdict::Ok);
        assert_eq!(standby(&mut c, 105, 999), StandbyVerdict::Mismatch(105));

        // same rule for ahead-of-active copies settled when the active delivers the round
        let mut c = core();
        c.add_standby(1, "b".into());
        for r in 100..=101 {
            c.on_standby_block(1, r, r - 1, 555, t0, f(r));
            let (_, bad) = c.on_active_block(r, r as u64, t0);
            assert!(bad.is_empty());
        }
        c.on_standby_block(1, 102, 101, 555, t0, f(102));
        let (_, bad) = c.on_active_block(102, 102, t0);
        assert_eq!(bad, vec![(1, 102)]);
    }

    #[test]
    fn a_mismatch_blocks_switching_until_reverified() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        feed(&mut c, 100, 110, t0, 1, 30);
        assert!(c.on_active_block(111, 111, t0).0);
        c.on_standby_block(1, 111, 110, 999, t0, f(111));
        c.on_standby_block(1, 112, 111, 112, t0, f(112));
        assert_eq!(c.pick_switch(t0 + 5000 * MS, true), None);
    }

    #[test]
    fn a_pool_active_returns_to_a_verified_priority_standby() {
        let t0 = Instant::now();
        let mut c = core();
        c.add_standby(1, "prio".into());
        c.mark_preferred(1);
        // a verified priority standby, a little slower than the pool active
        feed(&mut c, 100, 110, t0, 1, 20);
        let now = t0 + 11 * 70 * MS;
        assert_eq!(
            c.pick_switch(now, false),
            None,
            "nothing continues the chain yet"
        );
        c.on_standby_block(1, 111, 110, 111, now, f(111));
        // a dead active is still a dead-active switch (no dwell), the preference comes after
        assert_eq!(
            c.pick_switch(now, true),
            Some((1, SwitchReason::ActiveDead))
        );
        assert_eq!(
            c.pick_switch(now, false),
            Some((1, SwitchReason::Preferred))
        );
        let (frames, _) = c.switch_to(1, now).unwrap();
        assert_eq!(frames, vec![f(111)]);
        // on the priority peer, a faster POOL standby is no reason to leave...
        c.add_standby(2, "pool".into());
        let t1 = now + 20_000 * MS; // past the dwell
        feed(&mut c, 112, 140, t1, 2, -300);
        let t2 = t1 + 29 * 70 * MS;
        c.on_standby_block(2, 141, 140, 141, t2, f(141));
        assert_eq!(c.pick_switch(t2, false), None);
        // ...but a priority active that stalls still is
        assert_eq!(
            c.pick_switch(t2 + 500 * MS, false),
            Some((2, SwitchReason::Stall))
        );
    }

    #[test]
    fn the_return_to_priority_waits_out_the_dwell() {
        let t0 = Instant::now();
        let mut c = core();
        c.add_standby(1, "prio".into());
        c.mark_preferred(1);
        feed(&mut c, 100, 110, t0, 1, 20);
        let now = t0 + 11 * 70 * MS;
        c.on_standby_block(1, 111, 110, 111, now, f(111));
        // just switched (to this pool active): the preference waits like a lag switch does
        c.last_switch = Some(now);
        assert_eq!(c.pick_switch(now + 100 * MS, false), None);
        c.last_switch = Some(now - 10_000 * MS);
        assert_eq!(
            c.pick_switch(now, false),
            Some((1, SwitchReason::Preferred))
        );
        // the active already being a priority peer: nothing to return to
        let mut c = core();
        c.mark_preferred(0);
        c.add_standby(1, "prio2".into());
        c.mark_preferred(1);
        feed(&mut c, 100, 110, t0, 1, 20);
        c.on_standby_block(1, 111, 110, 111, now, f(111));
        assert_eq!(c.pick_switch(now, false), None);
    }

    #[test]
    fn lag_switch_to_consistently_faster_standby_respects_dwell() {
        let mut c = core();
        c.add_standby(1, "b".into());
        c.add_standby(2, "c".into());
        let t0 = Instant::now();
        // b leads the active by 300ms on every block
        feed(&mut c, 100, 140, t0, 1, -300);
        let now = t0 + 41 * 70 * MS;
        // b holds nothing ahead right now -> cannot switch yet
        assert!(c.ups[&1].ahead.is_empty());
        assert_eq!(c.pick_switch(now, false), None);
        c.on_standby_block(1, 141, 140, 141, now, f(141));
        assert_eq!(c.pick_switch(now, false), Some((1, SwitchReason::Lag)));
        let (frames, _) = c.switch_to(1, now).unwrap();
        assert_eq!(frames, vec![f(141)]);
        // right after a switch, lag switching waits out the dwell (stall switching does not)
        c.on_standby_block(2, 142, 141, 142, now, f(142));
        assert_eq!(c.pick_switch(now + 100 * MS, false), None);
    }

    #[test]
    fn stall_prefers_the_faster_standby_over_the_longer_buffer() {
        let mut c = core();
        c.add_standby(1, "slow".into());
        c.add_standby(2, "fast".into());
        let t0 = Instant::now();
        for r in 100..=130u32 {
            let ta = t0 + (r - 100) * 70 * MS;
            assert!(c.on_active_block(r, r as u64, ta).0);
            c.on_standby_block(1, r, r - 1, r as u64, ta + 500 * MS, f(r));
            c.on_standby_block(2, r, r - 1, r as u64, ta + 20 * MS, f(r));
        }
        // active stalls; the slow peer happens to hold more of the chain right now
        let t1 = t0 + 3000 * MS;
        for r in 131..=134 {
            c.on_standby_block(1, r, r - 1, r as u64, t1, f(r));
        }
        for r in 131..=132 {
            c.on_standby_block(2, r, r - 1, r as u64, t1, f(r));
        }
        assert_eq!(
            c.pick_switch(t1 + 400 * MS, false),
            Some((2, SwitchReason::Stall))
        );
    }

    #[test]
    fn rotation_picks_the_slowest_non_exempt_standby_past_the_threshold() {
        let mut c = core();
        for (id, ip, off) in [(1, "near", 20i64), (2, "far", 300), (3, "prio", 500)] {
            c.add_standby(id, ip.into());
            let t0 = Instant::now();
            for r in 100..=130u32 {
                let ta = t0 + (r - 100) * 70 * MS;
                c.on_active_block(r, r as u64, ta);
                c.on_standby_block(id, r, r - 1, r as u64, ta + (off as u32) * MS, f(r));
            }
            c.last_forwarded = 0; // replay the same rounds for the next standby
            c.forwarded.clear();
        }
        assert_eq!(c.slowest_standby(100, |ip| ip == "prio"), Some((2, -300)));
        assert_eq!(c.slowest_standby(400, |ip| ip == "prio"), None);
        assert_eq!(c.slowest_standby(100, |_| false), Some((3, -500)));
    }

    #[test]
    fn slower_standby_leads_are_negative() {
        let mut c = core();
        c.add_standby(1, "b".into());
        let t0 = Instant::now();
        feed(&mut c, 100, 130, t0, 1, 200);
        assert_eq!(c.ups[&1].median_lead(), Some(-200));
        assert_eq!(c.pick_switch(t0 + 10_000 * MS, false), None);
    }

    #[test]
    fn ahead_buffer_keeps_the_lowest_rounds_within_its_byte_budget() {
        let big = |r: u32| {
            let mut v = vec![0u8; 8 << 20];
            v[..4].copy_from_slice(&r.to_le_bytes());
            v
        };
        let t0 = Instant::now();
        // highest rounds first (e.g. a peer pushing far-ahead rounds): later, lower ones win
        let mut c = core();
        c.add_standby(1, "b".into());
        for r in (101..=105).rev() {
            c.on_standby_block(1, r, r - 1, r as u64, t0, big(r));
        }
        let u = &c.ups[&1];
        assert_eq!(
            u.ahead.keys().copied().collect::<Vec<_>>(),
            vec![101, 102, 103, 104]
        );
        assert_eq!(u.ahead_bytes, 4 * (8 << 20));
        // in order: the chain start a switch needs is kept, the newest is dropped
        let mut c = core();
        c.add_standby(1, "b".into());
        for r in 101..=105 {
            c.on_standby_block(1, r, r - 1, r as u64, t0, big(r));
        }
        assert_eq!(
            c.ups[&1].ahead.keys().copied().collect::<Vec<_>>(),
            vec![101, 102, 103, 104]
        );
        // settling / trimming / replay keep the byte count in step
        c.last_forwarded = 100;
        assert!(c.on_active_block(101, 101, t0).0);
        assert_eq!(c.ups[&1].ahead_bytes, 3 * (8 << 20));
        assert!(c.on_active_block(102, 999, t0).0);
        assert_eq!(c.ups[&1].ahead_bytes, 2 * (8 << 20));
        c.ups.get_mut(&1).unwrap().verified = MIN_VERIFIED;
        // 105 was dropped and the reader has moved on (106 is next): replaying 103-104 would
        // leave the node without 105, so the standby can't take over yet
        assert_eq!(c.pick_switch(t0 + 10_000 * MS, true), None);
        assert!(c.switch_to(1, t0).is_none());
        assert_eq!(c.active, 0);
        // once the active has delivered the dropped round, the standby's later blocks continue
        // the node's chain exactly
        for r in 103..=105 {
            c.on_active_block(r, r as u64, t0);
        }
        assert_eq!((c.ups[&1].ahead.len(), c.ups[&1].ahead_bytes), (0, 0));
        for r in 106..=107 {
            c.on_standby_block(1, r, r - 1, r as u64, t0, f(r));
        }
        assert_eq!(c.pick_switch(t0 + 10_000 * MS, true).map(|s| s.0), Some(1));
        let (frames, _) = c.switch_to(1, t0).unwrap();
        assert_eq!(frames, vec![f(106), f(107)]);
        assert_eq!(c.last_forwarded, 107);
        // and a frame-count cap for small frames
        let mut c = core();
        c.add_standby(1, "b".into());
        for r in 1..=AHEAD_KEEP as u32 + 50 {
            c.on_standby_block(1, r, r - 1, r as u64, t0, f(r));
        }
        let u = &c.ups[&1];
        assert_eq!(u.ahead.len(), AHEAD_KEEP);
        assert_eq!(
            u.ahead.last_key_value().map(|(&r, _)| r),
            Some(AHEAD_KEEP as u32)
        );
    }

    // a live block frame for `round`, as `compress` encodes the decompressed bytes
    fn block_frame(round: u32, body: u8, compress: fn(&[u8]) -> Vec<u8>) -> Vec<u8> {
        let mut dec = vec![0xaa; 16];
        for r in [round, round - 1, round - 2] {
            dec.push(0xfc);
            dec.extend_from_slice(&r.to_le_bytes());
        }
        dec.extend(std::iter::repeat_n(body, 4000));
        let payload = compress(&dec);
        let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
        frame.push(1);
        frame.extend_from_slice(&payload);
        frame
    }

    // an LZ4 block of literals only: valid, but encoded differently than lz4_flex would
    fn literals_only(dec: &[u8]) -> Vec<u8> {
        let mut out = (dec.len() as u32).to_le_bytes().to_vec();
        out.push(0xf0);
        let mut n = dec.len() - 15;
        while n >= 255 {
            out.push(255);
            n -= 255;
        }
        out.push(n as u8);
        out.extend_from_slice(dec);
        out
    }

    #[test]
    fn block_hash_ignores_how_the_block_was_compressed() {
        const R: u32 = 1_500_000_000;
        let a = block_frame(R, 7, lz4_flex::block::compress_prepend_size);
        let b = block_frame(R, 7, literals_only);
        assert_ne!(a, b, "different wire bytes");
        let (ra, pa, ha) = classify(&a).expect("block");
        let (rb, pb, hb) = classify(&b).expect("block");
        assert_eq!((ra, pa), (R, R - 1));
        assert_eq!((rb, pb, hb), (ra, pa, ha));
        let other = block_frame(R, 8, lz4_flex::block::compress_prepend_size);
        assert_ne!(classify(&other).unwrap().2, ha, "different content");
    }
}
