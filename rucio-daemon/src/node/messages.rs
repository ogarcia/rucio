//! Internal message bus between the libp2p node task and the rest of the
//! daemon (API server, DB layer, etc.).
//!
//! The node task owns the swarm and exposes two channels:
//!
//! ```text
//!   caller  ──[NodeCmd]──►  node task
//!   caller  ◄─[NodeEvent]── node task
//! ```
//!
//! All interaction with the network goes through these types — no other
//! module imports libp2p types directly.

use libp2p::{Multiaddr, PeerId};

// ---------------------------------------------------------------------------
// Commands (caller → node)
// ---------------------------------------------------------------------------

/// Commands that external code can send to the running node.
#[derive(Debug)]
pub enum NodeCmd {
    /// Add a bootstrap peer address and dial it.
    AddBootstrapPeer(Multiaddr),
    /// Start providing (announcing) a content hash in the DHT.
    StartProviding(Vec<u8>),
    /// Stop providing a content hash.
    StopProviding(Vec<u8>),
    /// Ask the DHT for providers of a content hash.
    FindProviders(Vec<u8>),
    /// Gracefully stop the node task.
    Shutdown,
}

// ---------------------------------------------------------------------------
// Events (node → caller)
// ---------------------------------------------------------------------------

/// Events emitted by the running node.
#[derive(Debug)]
pub enum NodeEvent {
    /// The node is ready: identity and listen addresses are confirmed.
    Ready {
        peer_id: PeerId,
        listen_addrs: Vec<Multiaddr>,
    },
    /// A new peer was discovered (mDNS or Kademlia).
    PeerDiscovered {
        peer_id: PeerId,
        addrs: Vec<Multiaddr>,
    },
    /// A peer is no longer reachable.
    PeerExpired { peer_id: PeerId },
    /// DHT returned providers for a hash requested via `FindProviders`.
    ProvidersFound {
        key: Vec<u8>,
        providers: Vec<PeerId>,
    },
    /// A fatal error in the node task.
    FatalError(String),
}
