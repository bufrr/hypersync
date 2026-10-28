// peerd: upstream peer discovery (gossipRootIps + query_peers crawl), live probing, and the
// peers.json the gateway reads.

mod crawl;
mod daemon;
mod peers_file;
mod probe;

// every submodule `use super::*`s: its siblings' pub(super) items resolve through these globs
#[allow(unused_imports)]
use self::{crawl::*, daemon::*, peers_file::*, probe::*};

#[cfg(test)]
pub(crate) use self::crawl::parse_query_peers_response;
pub(crate) use self::daemon::run_peerd;
pub(crate) use self::peers_file::{extract_round_tip, peer_candidates_path, read_node_peers};
