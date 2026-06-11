//! libp2p networking layer for Rucio: the swarm driver, the composite
//! behaviour, the protocol codecs and the `NodeCmd`/`NodeEvent` channel
//! interface that the rest of an application uses to talk to the node task.
//!
//! Extracted from `rucio-daemon::node` so that other binaries (e.g. the
//! `rucio-bootstrap` node + DHT indexer) can drive the same swarm without
//! pulling in the daemon. The dependency arrow is `rucio-net -> rucio-core`,
//! never the reverse: `rucio-core` stays the protocol/types vocabulary, this
//! crate is one concrete transport implementation of it.

pub mod behaviour;
pub mod classify;
pub mod codec_utils;
pub mod identity;
pub mod manifest_codec;
pub mod messages;
pub mod pinset_codec;
pub mod task;
pub mod transfer_codec;

use std::path::PathBuf;

/// Runtime configuration for the node task.
///
/// Path conventions (where the identity key lives, which addresses to listen
/// on) are a concern of the embedding application, not of the network layer —
/// the caller resolves them and passes the values in.
#[derive(Debug, Clone)]
pub struct NetConfig {
    pub identity_path: PathBuf,
    pub listen_addrs: Vec<String>,
    /// Which optional sub-behaviours to mount (identify + kademlia are always
    /// present). A full node uses [`BehaviourConfig::full`].
    pub behaviour: BehaviourConfig,
}

pub use behaviour::BehaviourConfig;
pub use codec_utils::{ByteLimiter, ReadProgress};
pub use messages::{NodeCmd, NodeEvent};
pub use task::{NodeHandle, spawn};
