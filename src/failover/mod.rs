mod core;
mod session;

use crate::priority::gwlog;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

// every submodule `use super::*`s: its siblings' pub(super) items resolve through these globs
#[allow(unused_imports)]
use self::{core::*, session::*};

// Hot-standby failover for a node's 4001 live stream (enabled by HYPERSYNC_FAILOVER_STANDBYS > 0).
//
// The plain push session serves the node from ONE upstream and, when that peer stalls, tears the
// session down so the node reconnects — 10s of idle detection plus a node reconnect and a fresh
// peer election. Here the session also holds N standby peers on their own live streams, and
// switches the node's feed between upstreams WITHOUT dropping the node's connection.
//
// Why a mid-stream switch is safe here (and why the old shadow merge was not):
//   - every live block names its parent round, and the node rejects a block whose parent it never
//     saw. A switch therefore replays the standby's buffered blocks starting EXACTLY at the block
//     whose parent is the last block the node received, following parent links; if the standby
//     cannot continue the chain gap-free, no switch happens.
//   - live blocks are identical across peers for the same round (verified on mainnet 2026-10-20:
//     429/429 rounds across 6 peers, identical wire bytes). Each standby is checked block-by-block
//     against what the active peer delivered, comparing the DECOMPRESSED block (a peer that
//     re-compresses the same block still matches); a mismatch resets its verification and
//     MAX_MISMATCHES in a row drop it (one bad block from the active can't evict every standby),
//     and only a standby with MIN_VERIFIED matching blocks since may become active.
//   - the active stream has the plain push session's progress guards (push::BlockProgress): a
//     round jump or no higher block for ACTIVE_BLOCK_STALL marks it dead like a closed stream.
//   - only one upstream feeds the node at a time; standby frames are never interleaved.
//
// Switch triggers:
//   - stall:   a verified standby has held the next block (parent == last forwarded) for
//              HYPERSYNC_FAILOVER_STALL_MS and the active still hasn't delivered it;
//   - lag:     a verified standby's median lead over the active across recent common blocks
//              exceeds HYPERSYNC_FAILOVER_LAG_MS (at most once per HYPERSYNC_FAILOVER_DWELL_MS);
//   - dead:    the active stream errored/closed/idled — switch at once if a standby can continue,
//              else end the session after ACTIVE_DEAD_GRACE (the node then reconnects as before).

// ---- configuration ----

pub(crate) struct FailoverConfig {
    pub(crate) standbys: usize,
    stall: Duration,
    lag: Duration,
    min_dwell: Duration,
    // replace a standby whose median trails the active by more than this (0 = never)
    rotate_slower: Duration,
    rotate_every: Duration,
    status_dir: Option<PathBuf>,
}

impl FailoverConfig {
    fn from_env() -> Self {
        let num = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(d)
        };
        let cfg = Self {
            standbys: num("HYPERSYNC_FAILOVER_STANDBYS", 0).min(16) as usize,
            stall: Duration::from_millis(num("HYPERSYNC_FAILOVER_STALL_MS", 400).max(50)),
            lag: Duration::from_millis(num("HYPERSYNC_FAILOVER_LAG_MS", 150).max(20)),
            min_dwell: Duration::from_millis(num("HYPERSYNC_FAILOVER_DWELL_MS", 10_000)),
            rotate_slower: Duration::from_millis(num("HYPERSYNC_FAILOVER_ROTATE_MS", 100)),
            rotate_every: Duration::from_secs(num("HYPERSYNC_FAILOVER_ROTATE_SECS", 60).max(10)),
            status_dir: std::env::var("HYPERSYNC_FAILOVER_STATUS_DIR")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .map(PathBuf::from),
        };
        if cfg.standbys > 0 {
            if let Some(dir) = &cfg.status_dir {
                if let Err(e) = std::fs::create_dir_all(dir) {
                    gwlog!(
                        "[gw] failover: status dir {} not usable: {e}",
                        dir.display()
                    );
                }
            }
            gwlog!(
                "[gw] failover: standbys={} stall={}ms lag={}ms dwell={}ms rotate>{}ms/{}s status_dir={}",
                cfg.standbys,
                cfg.stall.as_millis(),
                cfg.lag.as_millis(),
                cfg.min_dwell.as_millis(),
                cfg.rotate_slower.as_millis(),
                cfg.rotate_every.as_secs(),
                cfg.status_dir
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "-".into())
            );
        }
        cfg
    }

    pub(crate) fn get() -> &'static FailoverConfig {
        static CFG: OnceLock<FailoverConfig> = OnceLock::new();
        CFG.get_or_init(FailoverConfig::from_env)
    }
}

pub(crate) use self::session::{serve_failover, FailoverCtx};
