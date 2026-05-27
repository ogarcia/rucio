//! Composite libp2p behaviour for Rucio.
//!
//! `identify` and `kademlia` are always mounted — they are what make a node a
//! DHT participant. The rest (mDNS discovery, gossipsub search, and the
//! transfer / manifest request-response protocols) are optional, wrapped in
//! [`Toggle`] and selected via [`BehaviourConfig`]. A disabled `Toggle`
//! advertises no protocol and emits no events, so a node that only needs the
//! DHT (e.g. a bootstrap node) carries no inert protocol surface.

use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::{gossipsub, identify, kad, mdns, request_response, swarm::NetworkBehaviour};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use super::manifest_codec::{ManifestCodec, ManifestProtocol};
use super::transfer_codec::{TransferCodec, TransferProtocol};
use rucio_core::protocol::manifest::{ManifestRequest, ManifestResponse};
use rucio_core::protocol::transfer::{ChunkRequest, ChunkResponse};

pub const TOPIC_SEARCH: &str = "/rucio/search/1.0.0";
pub const TOPIC_SEARCH_RESULT: &str = "/rucio/search/result/1.0.0";

pub type TransferBehaviour = request_response::Behaviour<TransferCodec>;
pub type ManifestBehaviour = request_response::Behaviour<ManifestCodec>;

/// Selects which optional sub-behaviours to mount. `identify` and `kademlia`
/// are always present.
#[derive(Debug, Clone, Copy)]
pub struct BehaviourConfig {
    /// mDNS local-network peer discovery.
    pub mdns: bool,
    /// Gossipsub search query / result propagation.
    pub gossipsub: bool,
    /// Chunk transfer request-response protocol (serving / downloading data).
    pub transfer: bool,
    /// Manifest request-response protocol.
    pub manifest: bool,
    /// Capture inbound `ADD_PROVIDER` announcements. When enabled, Kademlia
    /// runs with `StoreInserts::FilterBoth` so each received provider record is
    /// surfaced as a [`NodeEvent::ProviderRecord`](crate::NodeEvent) (and must
    /// be re-stored explicitly to keep serving it). This is the basis of the
    /// passive DHT indexer; a normal node leaves it off.
    pub capture_provider_records: bool,
}

impl BehaviourConfig {
    /// A full participating node: everything enabled (the daemon).
    pub fn full() -> Self {
        Self {
            mdns: true,
            gossipsub: true,
            transfer: true,
            manifest: true,
            capture_provider_records: false,
        }
    }

    /// A bare DHT participant: only `identify` + `kademlia`. Used by a
    /// bootstrap node that keeps the routing table alive without discovering,
    /// searching, serving or transferring files.
    pub fn dht_only() -> Self {
        Self {
            mdns: false,
            gossipsub: false,
            transfer: false,
            manifest: false,
            capture_provider_records: false,
        }
    }

    /// A DHT indexer: like [`dht_only`](Self::dht_only) but capturing provider
    /// announcements, and optionally mounting `manifest` to enrich records with
    /// the file name and size by querying the announcing peer.
    pub fn indexer(enrich: bool) -> Self {
        Self {
            manifest: enrich,
            capture_provider_records: true,
            ..Self::dht_only()
        }
    }
}

/// The combined network behaviour.
#[derive(NetworkBehaviour)]
pub struct RucioBehaviour {
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
    pub gossipsub: Toggle<gossipsub::Behaviour>,
    pub transfer: Toggle<TransferBehaviour>,
    pub manifest: Toggle<ManifestBehaviour>,
}

impl RucioBehaviour {
    pub fn new(
        keypair: &libp2p::identity::Keypair,
        peer_id: libp2p::PeerId,
        cfg: BehaviourConfig,
    ) -> anyhow::Result<Self> {
        let identify = identify::Behaviour::new(identify::Config::new(
            "/rucio/1.0.0".to_string(),
            keypair.public(),
        ));

        let mut kademlia_config = kad::Config::new(libp2p::StreamProtocol::new("/rucio/kad/1.0.0"));
        if cfg.capture_provider_records {
            // FilterBoth surfaces each received provider record as an event
            // (InboundRequest::AddProvider) instead of storing it silently.
            kademlia_config.set_record_filtering(kad::StoreInserts::FilterBoth);
        }
        let store = kad::store::MemoryStore::new(peer_id);
        let mut kademlia = kad::Behaviour::with_config(peer_id, store, kademlia_config);
        // Run as a full DHT server so provider records are stored and
        // propagated to other peers.  Without this libp2p defaults to client
        // mode and start_providing() records are never forwarded.
        kademlia.set_mode(Some(kad::Mode::Server));

        let mdns = cfg
            .mdns
            .then(|| mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id))
            .transpose()?;

        let gossipsub = cfg
            .gossipsub
            .then(|| build_gossipsub(keypair))
            .transpose()?;

        let transfer = cfg.transfer.then(|| {
            request_response::Behaviour::new(
                vec![(TransferProtocol, request_response::ProtocolSupport::Full)],
                request_response::Config::default(),
            )
        });

        let manifest = cfg.manifest.then(|| {
            request_response::Behaviour::new(
                vec![(ManifestProtocol, request_response::ProtocolSupport::Full)],
                request_response::Config::default(),
            )
        });

        Ok(Self {
            identify,
            kademlia,
            mdns: Toggle::from(mdns),
            gossipsub: Toggle::from(gossipsub),
            transfer: Toggle::from(transfer),
            manifest: Toggle::from(manifest),
        })
    }
}

fn build_gossipsub(keypair: &libp2p::identity::Keypair) -> anyhow::Result<gossipsub::Behaviour> {
    let config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10))
        .validation_mode(gossipsub::ValidationMode::Strict)
        .message_id_fn(|msg: &gossipsub::Message| {
            let mut s = DefaultHasher::new();
            msg.data.hash(&mut s);
            gossipsub::MessageId::from(s.finish().to_be_bytes())
        })
        .build()
        .map_err(|e| anyhow::anyhow!("gossipsub config: {e}"))?;

    gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(keypair.clone()),
        config,
    )
    .map_err(|e| anyhow::anyhow!("gossipsub behaviour: {e}"))
}

pub use request_response::{OutboundRequestId, ResponseChannel};
pub type TransferRequest = ChunkRequest;
pub type TransferResponse = ChunkResponse;
pub type ManifestReq = ManifestRequest;
pub type ManifestResp = ManifestResponse;
