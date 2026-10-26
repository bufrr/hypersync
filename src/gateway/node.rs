use crate::protocol::unix_secs;
use crate::util::lock;
use std::collections::HashMap;
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
    // when a client-block (4002) response was last forwarded to this node: a catching-up node
    // pulls a batch every few seconds, one at the tip almost never
    last_client_blocks: Mutex<Option<Instant>>,
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
            last_client_blocks: Mutex::new(None),
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
        let mut active = lock(&self.active);
        *active = Some(peer.clone());
        self.lazy_active.store(false, Ordering::Release);
        *lock(&self.rpc_peer) = None;
        peer
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

    pub(super) fn mark_client_blocks_response(&self, now: Instant) {
        *lock(&self.last_client_blocks) = Some(now);
    }

    // No client-block response forwarded to this node for `quiet`: it has finished catching up.
    pub(super) fn client_blocks_quiet_for(&self, now: Instant, quiet: Duration) -> bool {
        lock(&self.last_client_blocks).is_none_or(|t| now.saturating_duration_since(t) >= quiet)
    }

    // An attempt that never probed (the slot was gone): doesn't count against the node.
    pub(super) fn forget_repin_attempt(&self) {
        *lock(&self.last_repin_attempt) = None;
    }

    // One re-pin attempt per `retry` per node: true (and the attempt is recorded) if allowed now.
    pub(super) fn claim_repin_attempt(&self, now: Instant, retry: Duration) -> bool {
        let mut last = lock(&self.last_repin_attempt);
        if last.is_some_and(|t| now.saturating_duration_since(t) < retry) {
            return false;
        }
        *last = Some(now);
        true
    }
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
            node.client_blocks_quiet_for(t0, quiet),
            "never fetched: quiet"
        );
        node.mark_client_blocks_response(t0);
        assert!(!node.client_blocks_quiet_for(t0 + Duration::from_secs(59), quiet));
        assert!(node.client_blocks_quiet_for(t0 + quiet, quiet));
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
