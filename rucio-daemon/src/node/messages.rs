//! Internal message bus between the libp2p node task and the rest of the
//! daemon (API server, DB layer, transfer engine, etc.).

use libp2p::{Multiaddr, PeerId, request_response::OutboundRequestId};
use rucio_core::protocol::{
    manifest::{ManifestRequest, ManifestResponse},
    node::NodeClass,
    search::{SearchQuery, SearchResult},
    transfer::{ChunkRequest, ChunkResponse},
};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Commands (caller → node)
// ---------------------------------------------------------------------------

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
    /// Publish a search query on the gossip network.
    Search(SearchQuery),
    /// Publish a search result on the gossip network.
    PublishSearchResult(SearchResult),
    /// Request a single chunk from a remote peer.
    /// The node task sends the assigned `OutboundRequestId` back through `id_tx`
    /// so the engine can correlate the eventual response.
    RequestChunk {
        peer: PeerId,
        request: ChunkRequest,
        id_tx: oneshot::Sender<OutboundRequestId>,
    },
    /// Send a chunk response back to a peer that requested it.
    RespondChunk {
        channel_id: u64,
        response: ChunkResponse,
    },
    /// Request the manifest for a file from a remote peer.
    /// The node task sends the assigned `OutboundRequestId` back through `id_tx`.
    RequestManifest {
        peer: PeerId,
        request: ManifestRequest,
        id_tx: oneshot::Sender<OutboundRequestId>,
    },
    /// Send a manifest response back to a peer that requested it.
    RespondManifest {
        channel_id: u64,
        response: ManifestResponse,
    },
    /// Gracefully stop the node task.
    Shutdown,
}

// ---------------------------------------------------------------------------
// Events (node → caller)
// ---------------------------------------------------------------------------

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
    /// A remote peer reported our observed (external) address via Identify.
    ObservedAddr {
        addr: Multiaddr,
        reported_by: PeerId,
    },
    /// Node connectivity class has been (re)determined.
    ClassChanged(NodeClass),
    /// DHT returned providers for a hash requested via `FindProviders`.
    ProvidersFound {
        key: Vec<u8>,
        providers: Vec<PeerId>,
    },
    /// A search result arrived from the gossip network.
    SearchResult(SearchResult),
    /// A search query arrived — daemon should check local shares and reply.
    SearchQueryReceived(SearchQuery),
    /// A chunk response arrived for a request we sent.
    ChunkReceived {
        request_id: OutboundRequestId,
        peer: PeerId,
        response: ChunkResponse,
    },
    /// A remote peer sent us a chunk request — we must respond.
    ChunkRequested {
        peer: PeerId,
        request: ChunkRequest,
        channel_id: u64,
    },
    /// A manifest response arrived for a request we sent.
    ManifestReceived {
        request_id: OutboundRequestId,
        peer: PeerId,
        response: ManifestResponse,
    },
    /// A remote peer sent us a manifest request — we must respond.
    ManifestRequested {
        peer: PeerId,
        request: ManifestRequest,
        channel_id: u64,
    },
    /// A fatal error in the node task.
    FatalError(String),
}
