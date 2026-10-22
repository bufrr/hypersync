// Upstream relaying for node connections: the round-gated live push session and the relay
// primitives the gateway uses (live), the fixed-list failover proxy (proxy), and the testing-only
// merge mode plus the plain relay (legacy).

mod legacy;
mod live;
mod proxy;

// every submodule `use super::*`s: its siblings' pub(super) items resolve through these globs
#[allow(unused_imports)]
use self::{legacy::*, live::*, proxy::*};

pub(crate) use self::legacy::{run_relay, serve};
pub(crate) use self::live::{
    copy_node_to_upstream, node_queue_budget, push_exit_is_upstream_fault, queue_frame, serve_push,
    splice, splice_with_idle, AbortOnDrop, BlockProgress, PushConfig, QueuedFrame, Relayed,
    ACTIVE_BLOCK_STALL, MAX_ACTIVE_ROUND_JUMP, NODE_WRITE_TIMEOUT, RELAY_UPSTREAM_WRITE_TIMEOUT,
    RETURNING_TO_PRIORITY, UPSTREAM_WRITE_TIMEOUT,
};
pub(crate) use self::proxy::run_proxy;
