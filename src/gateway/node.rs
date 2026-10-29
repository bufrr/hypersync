use crate::protocol::unix_secs;
use crate::util::lock;
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[allow(unused_imports)]
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ActivePeer {
    pub(super) ip: String,
    pub(super) epoch: u64,
}

// Pairs `pin_active` with `clear_active_if_current` regardless of how the caller's scope exits
// (early return, panic) — a pin left dangling after a failed relay can wrongly suppress cache
// replay and route 4002 RPC at a stale peer.
pub(super) struct ActivePinGuard {
    pub(super) node: Arc<NodeState>,
    pub(super) peer: ActivePeer,
}

impl Drop for ActivePinGuard {
    fn drop(&mut self) {
        self.node.clear_active_if_current(&self.peer);
    }
}

// Per-downstream-node session state, keyed by the node's source IP (each hl-node needs its own
// egress IP as seen by the gateway; NAT'ing two nodes through one IP conflates their sessions).
// State deliberately survives reconnects: nodes restart and come back with the same IP, and the
// replay cooldown / live floor must persist across that.
pub(super) struct NodeState {
    pub(super) ip: IpAddr,
    // the upstream peer serving THIS node's bootstrap; all of this node's connections reuse it
    // so client-block RPC (4002) isn't rejected with "Peer-only request".
    active: Mutex<Option<ActivePeer>>,
    next_active_epoch: AtomicU64,
    // Non-zero only after this gateway actually replayed boot_blob to THIS node. The short replay
    // cooldown only prevents duplicate replays from immediate reconnects while allowing normal
    // node restarts to use cache.
    pub(super) last_cache_replay_secs: AtomicU64,
    // During cache cold-start, the node races ahead through 4002 client-block catch-up while the
    // 4001 live stream may still replay older backlog. Drop live block frames at/below this floor.
    // Arc so PushConfig/pump_merge keep their existing Arc<AtomicU32> shape.
    pub(super) live_floor: Arc<AtomicU32>,
    last_seen_secs: AtomicU64,
    // connections currently open from this node; a long-lived live stream opens no new
    // connections for hours, so eviction must key on this, not just last_seen
    open_conns: AtomicUsize,
    // true when `active` was pinned lazily by a misc-port dial (no real 4001 session behind it).
    // A cold-starting node's gossip ports can connect before its 4001 bootstrap greet; a lazy pin
    // must not count as "has an active session" or it would suppress the node's cache replay.
    lazy_active: AtomicBool,
    // The upstream that last answered this node's client-block RPC as a hedge after the active
    // stopped serving it (Peer-only, so the pin was cleared while the 4001 session stays up), or
    // the priority peer that answered while a pool active is pinned on an exclusive tier.
    // Tried first, without waiting for a pin, until the next real 4001 session pins a new active;
    // otherwise every later 4002 request pays the pin wait plus the full hedge fan-out.
    pub(super) rpc_peer: Mutex<Option<String>>,
    // a cache replay to this node is running (see ReplayInFlight)
    pub(super) replay_in_flight: AtomicBool,
    // when this node last probed a priority peer to leave a pool session (see
    // select::wait_priority_return); kept across reconnects so re-pins stay rate-limited
    last_repin_attempt: Mutex<Option<Instant>>,
    // when a session of this node was last ended for a catch-up that wasn't converging (see
    // select::wait_catchup_stuck); kept across reconnects so those ends stay rate-limited
    last_catchup_cut: Mutex<Option<Instant>>,
    // the last client-block (4002) batch forwarded to this node, and when its current run of
    // batches began (a run: no CATCHUP_RUN_GAP between batches): a catching-up node pulls a
    // batch every few seconds, one at the tip almost never
    last_client_blocks: Mutex<Option<ClientBlockRun>>,
    // the next client-block range, fetched while the node processes the current one
    pub(super) prefetch: Mutex<Option<super::client_blocks::Prefetch>>,
}

impl NodeState {
    pub(super) fn new(ip: IpAddr, now: u64) -> Self {
        Self {
            ip,
            active: Mutex::new(None),
            next_active_epoch: AtomicU64::new(0),
            last_cache_replay_secs: AtomicU64::new(0),
            live_floor: Arc::new(AtomicU32::new(0)),
            last_seen_secs: AtomicU64::new(now),
            open_conns: AtomicUsize::new(0),
            lazy_active: AtomicBool::new(false),
            rpc_peer: Mutex::new(None),
            replay_in_flight: AtomicBool::new(false),
            last_repin_attempt: Mutex::new(None),
            last_catchup_cut: Mutex::new(None),
            last_client_blocks: Mutex::new(None),
            prefetch: Mutex::new(None),
        }
    }

    // Pin this node's active peer from a real 4001 session (bootstrap or live/resume).
    // `lazy_active` is only ever written while holding the `active` lock (and read either under
    // the lock or after cloning the guard's contents), so readers can never observe a pin whose
    // lazy flag belongs to the previous pin.
    pub(super) fn pin_active(&self, ip: &str) -> ActivePeer {
        let peer = ActivePeer {
            ip: ip.to_string(),
            epoch: self.next_active_epoch.fetch_add(1, Ordering::AcqRel) + 1,
        };
        // a client-block range prefetched for the previous session is neither served into this
        // one nor left running (its fetch would keep acting on the old session's peers)
        drop(lock(&self.prefetch).take());
        let mut active = lock(&self.active);
        *active = Some(peer.clone());
        self.lazy_active.store(false, Ordering::Release);
        *lock(&self.rpc_peer) = None;
        peer
    }

    // Changes whenever a new pin (real or lazy) is made; unpinning alone leaves it.
    pub(super) fn pin_generation(&self) -> u64 {
        self.next_active_epoch.load(Ordering::Acquire)
    }

    pub(super) fn pin_lazy_active(&self, ip: &str) -> ActivePeer {
        let mut active = lock(&self.active);
        unpin_blacklisted(&mut active);
        if let Some(peer) = active.clone() {
            return peer;
        }
        let peer = ActivePeer {
            ip: ip.to_string(),
            epoch: self.next_active_epoch.fetch_add(1, Ordering::AcqRel) + 1,
        };
        // as in pin_active: a prefetch from before this pin is neither served nor left running
        drop(lock(&self.prefetch).take());
        *active = Some(peer.clone());
        self.lazy_active.store(true, Ordering::Release);
        peer
    }

    pub(super) fn active_snapshot(&self) -> Option<ActivePeer> {
        let mut active = lock(&self.active);
        unpin_blacklisted(&mut active);
        active.clone()
    }

    pub(super) fn active_session_snapshot(&self) -> Option<ActivePeer> {
        let mut active = lock(&self.active);
        unpin_blacklisted(&mut active);
        if self.lazy_active.load(Ordering::Acquire) {
            None
        } else {
            active.clone()
        }
    }

    pub(super) fn active_is_current(&self, peer: &ActivePeer) -> bool {
        lock(&self.active).as_ref() == Some(peer)
    }

    pub(super) fn clear_active_if_current(&self, peer: &ActivePeer) {
        let mut active = lock(&self.active);
        if active.as_ref() == Some(peer) {
            *active = None;
        }
    }

    // "This node has a live session" — a lazy misc-port pin doesn't count.
    pub(super) fn has_active_session(&self) -> bool {
        let mut active = lock(&self.active);
        unpin_blacklisted(&mut active);
        active.is_some() && !self.lazy_active.load(Ordering::Acquire)
    }

    // A client-block batch was forwarded; `behind` = rounds between the gateway's freshest live
    // round and the batch's requested start (None: tip unknown, which keeps the catch-up from
    // counting as stalled).
    pub(super) fn mark_client_blocks_response(&self, now: Instant, behind: Option<u32>) {
        let mut run = lock(&self.last_client_blocks);
        match run.as_mut() {
            Some(r) if now.saturating_duration_since(r.last) < CATCHUP_RUN_GAP => {
                r.last = now;
                r.samples.push_back((now, behind));
                while r
                    .samples
                    .front()
                    .is_some_and(|(t, _)| now.saturating_duration_since(*t) > CATCHUP_SAMPLE_SPAN)
                {
                    r.samples.pop_front();
                }
                // past the count cap: dropped samples may lie inside a window, so the run counts
                // as starting after them (no stalled verdict until a full window is kept again)
                while r.samples.len() > CATCHUP_MAX_SAMPLES {
                    if let Some((t, _)) = r.samples.pop_front() {
                        r.since = r.since.max(t);
                    }
                }
            }
            _ => {
                *run = Some(ClientBlockRun {
                    since: now,
                    last: now,
                    samples: VecDeque::from([(now, behind)]),
                })
            }
        }
    }

    // While the node is catching up (a batch within `quiet`): whether its catch-up sat still over
    // the last `window`, and its distance behind now. Still: every batch in the window stayed
    // within CATCHUP_PROGRESS_ROUNDS of the others — hl-node's stuck loop, which re-fetches only
    // what the network produced meanwhile and so holds a constant distance (~70 s, within a few
    // rounds). Anything that moves is not cut: a catch-up closing in, one that lost ground and is
    // closing in again (2026-10-28: cut while gaining 1,500 rounds), or one falling behind under
    // heavy blocks, where a new session would only start the same catch-up over. Not still before
    // the run is a full window old, or with an unknown tip in the window.
    pub(super) fn client_blocks_stalled(
        &self,
        now: Instant,
        quiet: Duration,
        window: Duration,
    ) -> Option<(bool, Option<u32>)> {
        let run = lock(&self.last_client_blocks);
        let r = run
            .as_ref()
            .filter(|r| now.saturating_duration_since(r.last) < quiet)?;
        let behind = r.samples.back().and_then(|(_, b)| *b);
        let Some(from) = now.checked_sub(window) else {
            return Some((false, behind));
        };
        if r.since > from {
            return Some((false, behind));
        }
        let in_window: Option<Vec<u32>> = r
            .samples
            .iter()
            .filter(|(t, _)| *t > from)
            .map(|(_, b)| *b)
            .collect();
        let Some(d) = in_window.filter(|d| !d.is_empty()) else {
            return Some((false, behind));
        };
        let (lo, hi) = (*d.iter().min().unwrap_or(&0), *d.iter().max().unwrap_or(&0));
        // ...and not closing in, however slowly: under heavy blocks a catch-up can gain less than
        // the band in a window (2026-10-29: ~90 rounds in 3 min at 2,467 behind, cut as "still"),
        // while the stuck loop's distance only jitters. Compared by the median of the window's
        // first and last third, so one stray batch doesn't decide it.
        let third = (d.len() / 3).max(1);
        let closing = median(&d[..third]).saturating_sub(median(&d[d.len() - third..]))
            >= CATCHUP_CLOSING_ROUNDS;
        let still = hi - lo < CATCHUP_PROGRESS_ROUNDS && !closing;
        Some((still, behind))
    }

    // While the node is catching up (a batch within `quiet`): when its current run of batches
    // began; None once it has been quiet that long (it has finished catching up). Gaps are judged
    // at each batch, so none is missed between two polls.
    pub(super) fn client_blocks_run_since(&self, now: Instant, quiet: Duration) -> Option<Instant> {
        lock(&self.last_client_blocks)
            .as_ref()
            .filter(|r| now.saturating_duration_since(r.last) < quiet)
            .map(|r| r.since)
    }

    // An attempt that never probed (the slot was gone): doesn't count against the node.
    pub(super) fn forget_repin_attempt(&self) {
        *lock(&self.last_repin_attempt) = None;
    }

    // One re-pin attempt per `retry` per node: true (and the attempt is recorded) if allowed now.
    pub(super) fn claim_repin_attempt(&self, now: Instant, retry: Duration) -> bool {
        claim_every(&self.last_repin_attempt, now, retry)
    }

    // One stuck-catch-up session end per `retry` per node, likewise.
    pub(super) fn claim_catchup_cut(&self, now: Instant, retry: Duration) -> bool {
        claim_every(&self.last_catchup_cut, now, retry)
    }
}

// A gap this long between client-block batches ends the node's catch-up run.
pub(super) const CATCHUP_RUN_GAP: Duration = Duration::from_secs(60);

// A catch-up whose distance behind stays within this band (~10 s of rounds) is sitting still.
const CATCHUP_PROGRESS_ROUNDS: u32 = 150;
// ...unless it closed in by this much over the window (~3 s of rounds; the loop's jitter nets ~0)
const CATCHUP_CLOSING_ROUNDS: u32 = 50;

fn median(v: &[u32]) -> u32 {
    let mut s = v.to_vec();
    s.sort_unstable();
    s.get(s.len() / 2).copied().unwrap_or(0)
}

// How long a run's batch distances are kept: the stuck-catch-up watchdog's window
// (select::REPIN.catchup_max, 3 min) plus slack; and at most this many (a batch every ~1 s or more).
const CATCHUP_SAMPLE_SPAN: Duration = Duration::from_secs(5 * 60);
const CATCHUP_MAX_SAMPLES: usize = 1024;

struct ClientBlockRun {
    since: Instant,
    last: Instant,
    // each recent batch: when, and its distance behind (None: tip unknown)
    samples: VecDeque<(Instant, Option<u32>)>,
}

fn claim_every(last: &Mutex<Option<Instant>>, now: Instant, every: Duration) -> bool {
    let mut last = lock(last);
    if last.is_some_and(|t| now.saturating_duration_since(t) < every) {
        return false;
    }
    *last = Some(now);
    true
}

// A pin to an IP blacklisted after it was set is dropped on sight, so no new connection of the
// node follows it (the session on it is being torn down, see blacklist::wait_blacklisted).
fn unpin_blacklisted(active: &mut Option<ActivePeer>) {
    if active
        .as_ref()
        .is_some_and(|p| crate::blacklist::contains(&p.ip))
    {
        *active = None;
    }
}

// Decrements the node's open-connection count when the connection task ends (however it ends).
pub(super) struct NodeConnGuard(pub(super) Arc<NodeState>);

impl Drop for NodeConnGuard {
    fn drop(&mut self) {
        // stamp BEFORE decrementing: an eviction scan between the two must never see
        // open_conns==0 paired with an hours-stale last_seen
        self.0.last_seen_secs.store(unix_secs(), Ordering::Release);
        self.0.open_conns.fetch_sub(1, Ordering::AcqRel);
    }
}

// The gateway binds 0.0.0.0 with no allowlist; scanners must not grow the registry unbounded.
const NODE_STATE_CAP: usize = 256;

// Keep state across node restarts; prune entries idle this long.
const NODE_STATE_TTL_SECS: u64 = 60 * 60;

// Which entries to evict before inserting a new key. Entries with open connections are never
// evicted. Of the rest: everything past the idle TTL goes, then (if still over cap)
// oldest-last_seen first. Tie-break on ip so eviction is deterministic.
fn node_state_evict_keys(
    entries: &[(IpAddr, u64, bool)],
    now: u64,
    cap: usize,
    ttl: u64,
) -> Vec<IpAddr> {
    let mut evict: Vec<IpAddr> = entries
        .iter()
        .filter(|(_, seen, held)| !held && now.saturating_sub(*seen) > ttl)
        .map(|(ip, _, _)| *ip)
        .collect();
    let mut evictable: Vec<(IpAddr, u64)> = entries
        .iter()
        .filter(|(_, seen, held)| !held && now.saturating_sub(*seen) <= ttl)
        .map(|(ip, seen, _)| (*ip, *seen))
        .collect();
    let kept = entries.len() - evict.len();
    if kept + 1 > cap {
        evictable.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let excess = (kept + 1 - cap).min(evictable.len());
        evict.extend(evictable.iter().take(excess).map(|(ip, _)| *ip));
    }
    evict
}

#[derive(Default)]
pub(super) struct NodeRegistry {
    nodes: Mutex<HashMap<IpAddr, Arc<NodeState>>>,
}

impl NodeRegistry {
    // Returns the node's state plus a guard that holds its open-connection count; keep the guard
    // alive for the life of the connection task.
    pub(super) fn get_or_insert(&self, ip: IpAddr, now: u64) -> (Arc<NodeState>, NodeConnGuard) {
        let mut nodes = lock(&self.nodes);
        if let Some(state) = nodes.get(&ip) {
            state.last_seen_secs.store(now, Ordering::Release);
            state.open_conns.fetch_add(1, Ordering::AcqRel);
            return (state.clone(), NodeConnGuard(state.clone()));
        }
        let entries: Vec<(IpAddr, u64, bool)> = nodes
            .iter()
            .map(|(ip, s)| {
                (
                    *ip,
                    s.last_seen_secs.load(Ordering::Acquire),
                    s.open_conns.load(Ordering::Acquire) > 0,
                )
            })
            .collect();
        for stale in node_state_evict_keys(&entries, now, NODE_STATE_CAP, NODE_STATE_TTL_SECS) {
            nodes.remove(&stale);
        }
        let state = Arc::new(NodeState::new(ip, now));
        state.open_conns.fetch_add(1, Ordering::AcqRel);
        nodes.insert(ip, state.clone());
        (state.clone(), NodeConnGuard(state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::priority::BootstrapPolicy;

    #[test]
    fn blacklisted_ips_leave_the_pool_and_the_pins() {
        let _g = crate::blacklist::TEST_LOCK.blocking_lock();
        let dir = std::env::temp_dir().join(format!("hypersync_gwbl_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("peers.json");
        std::fs::write(
            &file,
            r#"{"live_servers":["203.0.113.30","203.0.113.31","203.0.113.32"]}"#,
        )
        .unwrap();
        let path = file.to_str().unwrap();
        assert_eq!(peer_file_dir(path), dir);
        assert_eq!(peer_file_dir("peers.json"), PathBuf::from("."));
        let priority = PriorityTier::new(
            vec!["203.0.113.32".into()],
            0,
            Duration::from_millis(1500),
            BootstrapPolicy::Fallback,
        );
        let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
        node.pin_active("203.0.113.30");
        crate::blacklist::install(&["203.0.113.30"]);
        // priority peers and blacklisted IPs are both out of the pool
        assert_eq!(read_pool(&priority, path), ["203.0.113.31"]);
        // a pin made before the IP was blacklisted is dropped, so no new connection follows it
        assert!(!node.has_active_session());
        assert!(node.active_snapshot().is_none());
        assert_eq!(node.pin_lazy_active("203.0.113.31").ip, "203.0.113.31");
        // a remembered state server blacklisted since is skipped
        assert_eq!(
            ordered_candidates(vec!["203.0.113.30".into()], &["203.0.113.31".into()], 0),
            ["203.0.113.31"]
        );
        crate::blacklist::install(&[]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn clear_active_only_clears_matching_epoch() {
        let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
        let first = node.pin_active("1.1.1.1");
        let second = node.pin_active("1.1.1.1");

        node.clear_active_if_current(&first);
        assert_eq!(node.active_snapshot(), Some(second.clone()));

        node.clear_active_if_current(&second);
        assert!(node.active_snapshot().is_none());
    }

    #[test]
    fn active_pin_guard_clears_pin_on_early_drop() {
        let node = Arc::new(NodeState::new("10.0.0.1".parse().unwrap(), 1));
        let peer = node.pin_active("1.1.1.1");
        {
            let _guard = ActivePinGuard {
                node: node.clone(),
                peer,
            };
            assert!(node.has_active_session());
            // guard drops here, as it would on an early `return` out of transparent_bootstrap
        }
        assert!(!node.has_active_session());
    }

    #[test]
    fn active_pin_guard_leaves_a_newer_pin_untouched() {
        let node = Arc::new(NodeState::new("10.0.0.1".parse().unwrap(), 1));
        let stale = node.pin_active("1.1.1.1");
        let guard = ActivePinGuard {
            node: node.clone(),
            peer: stale,
        };
        // a fresh session pins over the guarded one before it drops (e.g. node reconnected)
        let fresh = node.pin_active("2.2.2.2");
        drop(guard);
        assert_eq!(node.active_snapshot(), Some(fresh));
    }

    #[test]
    fn lazy_active_is_not_a_real_4001_session() {
        let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
        let lazy = node.pin_lazy_active("1.1.1.1");

        assert_eq!(node.active_snapshot(), Some(lazy));
        assert!(node.active_session_snapshot().is_none());

        let real = node.pin_active("2.2.2.2");
        assert_eq!(node.active_session_snapshot(), Some(real));
    }

    #[test]
    fn node_state_evict_keys_prunes_ttl_then_oldest() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let now = 10_000u64;

        // under cap, all fresh -> nothing evicted
        let entries = vec![
            (ip("10.0.0.1"), now - 5, false),
            (ip("10.0.0.2"), now, false),
        ];
        assert!(node_state_evict_keys(&entries, now, 4, 3600).is_empty());

        // idle past ttl evicted regardless of cap
        let entries = vec![
            (ip("10.0.0.1"), now - 3601, false),
            (ip("10.0.0.2"), now, false),
        ];
        assert_eq!(
            node_state_evict_keys(&entries, now, 4, 3600),
            vec![ip("10.0.0.1")]
        );

        // at cap with all fresh -> exactly the oldest-last_seen evicted to fit the insert
        let entries = vec![
            (ip("10.0.0.1"), now - 30, false),
            (ip("10.0.0.2"), now - 10, false),
            (ip("10.0.0.3"), now - 20, false),
        ];
        assert_eq!(
            node_state_evict_keys(&entries, now, 3, 3600),
            vec![ip("10.0.0.1")]
        );

        // last_seen tie -> deterministic by ip
        let entries = vec![(ip("10.0.0.9"), now, false), (ip("10.0.0.8"), now, false)];
        assert_eq!(
            node_state_evict_keys(&entries, now, 2, 3600),
            vec![ip("10.0.0.8")]
        );

        // a node with open connections is never evicted: not by ttl (its long-lived live stream
        // opens no new connections for hours) and not by cap pressure
        let entries = vec![
            (ip("10.0.0.1"), now - 7200, true),
            (ip("10.0.0.2"), now - 30, false),
        ];
        assert_eq!(
            node_state_evict_keys(&entries, now, 2, 3600),
            vec![ip("10.0.0.2")]
        );

        // all held and over cap -> nothing evictable; the insert may exceed cap (bounded by the
        // number of concurrently open connections)
        let entries = vec![(ip("10.0.0.1"), now, true), (ip("10.0.0.2"), now, true)];
        assert!(node_state_evict_keys(&entries, now, 2, 3600).is_empty());
    }

    #[test]
    fn repin_attempts_are_rate_limited_per_node() {
        let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
        let t0 = Instant::now();
        let retry = Duration::from_secs(300);
        assert!(node.claim_repin_attempt(t0, retry));
        assert!(!node.claim_repin_attempt(t0 + Duration::from_secs(299), retry));
        assert!(node.claim_repin_attempt(t0 + retry, retry));
        // an attempt that never probed (lost the slot race) doesn't count
        node.forget_repin_attempt();
        assert!(node.claim_repin_attempt(t0 + retry, retry));
    }

    #[test]
    fn client_block_responses_mark_the_node_as_catching_up() {
        let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
        let t0 = Instant::now();
        let quiet = Duration::from_secs(60);
        assert!(
            node.client_blocks_run_since(t0, quiet).is_none(),
            "never fetched: quiet"
        );
        node.mark_client_blocks_response(t0, None);
        assert_eq!(
            node.client_blocks_run_since(t0 + Duration::from_secs(59), quiet),
            Some(t0)
        );
        assert!(node.client_blocks_run_since(t0 + quiet, quiet).is_none());
    }

    #[test]
    fn only_a_catch_up_sitting_at_one_distance_is_stalled() {
        let s = Duration::from_secs;
        let w = s(180);
        // a run of batches every 10 s from t0 with the given distances; stalled at batch `at`?
        let run_at = |distances: &[Option<u32>], at: usize| {
            let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
            let t0 = Instant::now();
            for (i, b) in distances.iter().take(at + 1).enumerate() {
                node.mark_client_blocks_response(t0 + s(i as u64 * 10), *b);
            }
            node.client_blocks_stalled(t0 + s(at as u64 * 10), CATCHUP_RUN_GAP, w)
                .map(|r| r.0)
        };
        let run = |d: &[Option<u32>]| run_at(d, d.len() - 1);
        let steps = |from: u32, step: i64, n: usize| -> Vec<Option<u32>> {
            (0..n)
                .map(|i| Some((from as i64 + step * i as i64) as u32))
                .collect()
        };
        // hl-node's stuck loop: a constant distance, a few rounds of jitter
        let jitter: Vec<_> = (0..40).map(|i| Some(1_060 + (i % 3) * 7)).collect();
        assert_eq!(run(&jitter), Some(true));
        // closing in ~20 rounds a batch
        assert_eq!(run(&steps(9_000, -20, 40)), Some(false));
        // the live case: lost ground (3,943 -> ~5,400) then closing in 40 a batch — at no batch of
        // the recovery is it stalled, even while still behind where the run began
        let mut lost = steps(3_943, 50, 30);
        lost.extend(steps(5_400, -40, 37));
        for at in 18..lost.len() {
            assert_eq!(run_at(&lost, at), Some(false), "batch {at}");
        }
        // ...then stuck at the tip's fixed distance: stalled once a full window sits still
        let turn = lost.len();
        lost.extend(vec![Some(3_960); 19]);
        assert_eq!(
            run_at(&lost, turn + 10),
            Some(false),
            "the window still holds the recovery"
        );
        assert_eq!(run_at(&lost, turn + 18), Some(true));
        // closing in slower than the band under heavy blocks (the live case: ~90 rounds in a
        // window at 2,467 behind) is not still; the same distance with jitter is
        assert_eq!(run(&steps(2_667, -5, 40)), Some(false));
        let slow_jitter: Vec<_> = (0..40).map(|i| Some(2_500 + (i % 4) * 20)).collect();
        assert_eq!(run(&slow_jitter), Some(true));
        // falling behind under heavy blocks, or swinging widely: not cut (a new session would
        // only start the same catch-up over)
        assert_eq!(run(&steps(5_000, 15, 40)), Some(false));
        let swing: Vec<_> = (0..40)
            .map(|i| Some(if i % 2 == 0 { 5_000 } else { 5_200 }))
            .collect();
        assert_eq!(run(&swing), Some(false));
        // not before a full window, nor with an unknown tip in it
        assert_eq!(run(&[Some(900); 18]), Some(false));
        let mut unknown = vec![Some(900); 40];
        unknown[35] = None;
        assert_eq!(run(&unknown), Some(false));
        // the band: 149 rounds of spread is still, 150 is moving; no overflow at the top
        let spread = |hi: u32| -> Vec<Option<u32>> {
            (0..40)
                .map(|i| Some(if i % 2 == 0 { 5_000 } else { hi }))
                .collect()
        };
        assert_eq!(
            run(&spread(5_000 + CATCHUP_PROGRESS_ROUNDS - 1)),
            Some(true)
        );
        assert_eq!(run(&spread(5_000 + CATCHUP_PROGRESS_ROUNDS)), Some(false));
        assert_eq!(run(&[Some(u32::MAX); 40]), Some(true));
        // past the count cap the run counts as starting after the dropped samples
        let capped = NodeState::new("10.0.0.3".parse().unwrap(), 1);
        let t0 = Instant::now();
        // 10 batches/s for 200 s: the run is a full window old, the kept 1,024 span ~102 s
        let n = 2_000;
        for i in 0..n {
            capped
                .mark_client_blocks_response(t0 + Duration::from_millis(i as u64 * 100), Some(900));
        }
        let end = t0 + Duration::from_millis((n as u64 - 1) * 100);
        assert_eq!(
            capped
                .client_blocks_stalled(end, CATCHUP_RUN_GAP, w)
                .map(|r| r.0),
            Some(false),
            "the kept samples span under a window"
        );
        // quiet past `quiet`: not catching up at all
        let node = NodeState::new("10.0.0.2".parse().unwrap(), 1);
        let t0 = Instant::now();
        node.mark_client_blocks_response(t0, Some(900));
        assert_eq!(
            node.client_blocks_stalled(t0 + CATCHUP_RUN_GAP, CATCHUP_RUN_GAP, w),
            None
        );
    }

    #[test]
    fn a_catch_up_run_ends_at_a_gap_seen_between_polls() {
        let node = NodeState::new("10.0.0.1".parse().unwrap(), 1);
        let t0 = Instant::now();
        let s = Duration::from_secs;
        assert_eq!(node.client_blocks_run_since(t0, CATCHUP_RUN_GAP), None);
        node.mark_client_blocks_response(t0, None);
        node.mark_client_blocks_response(t0 + s(30), None);
        assert_eq!(
            node.client_blocks_run_since(t0 + s(31), CATCHUP_RUN_GAP),
            Some(t0)
        );
        // a 61 s gap between two batches starts a new run, whenever anyone looks
        node.mark_client_blocks_response(t0 + s(91), None);
        node.mark_client_blocks_response(t0 + s(95), None);
        assert_eq!(
            node.client_blocks_run_since(t0 + s(96), CATCHUP_RUN_GAP),
            Some(t0 + s(91))
        );
        assert_eq!(
            node.client_blocks_run_since(t0 + s(155), CATCHUP_RUN_GAP),
            None
        );
    }

    #[test]
    fn node_registry_reuses_state_across_reconnects() {
        let ip_a: IpAddr = "10.0.0.1".parse().unwrap();
        let ip_b: IpAddr = "10.0.0.2".parse().unwrap();
        let reg = NodeRegistry::default();
        // guard Drop stamps wall-clock last_seen, so anchor test times to the wall clock
        let t0 = unix_secs();

        let (a1, a1_guard) = reg.get_or_insert(ip_a, t0);
        let peer = a1.pin_active("1.1.1.1");

        // same ip reconnecting -> same state (active survives), last_seen bumped
        let (a2, a2_guard) = reg.get_or_insert(ip_a, t0 + 10);
        assert!(Arc::ptr_eq(&a1, &a2));
        assert_eq!(a2.active_snapshot(), Some(peer));
        assert_eq!(a2.last_seen_secs.load(Ordering::Acquire), t0 + 10);
        assert_eq!(a2.open_conns.load(Ordering::Acquire), 2);
        drop(a2_guard);
        assert_eq!(a1.open_conns.load(Ordering::Acquire), 1);

        // different ip -> fresh state
        let (b, b_guard) = reg.get_or_insert(ip_b, t0);
        assert!(!Arc::ptr_eq(&a1, &b));
        assert!(lock(&b.active).is_none());
        drop(b_guard); // stamps last_seen ~t0; ip_b now idle with no open connections

        // ip_a still has an open connection (a1_guard) -> survives TTL eviction; ip_b doesn't
        let later = t0 + NODE_STATE_TTL_SECS + 60;
        let ip_c: IpAddr = "10.0.0.3".parse().unwrap();
        let (_c, _c_guard) = reg.get_or_insert(ip_c, later);
        assert!(lock(&reg.nodes).contains_key(&ip_a));
        assert!(!lock(&reg.nodes).contains_key(&ip_b));
        assert!(lock(&reg.nodes).contains_key(&ip_c));
        drop(a1_guard);
    }
}
