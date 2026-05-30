//! Composite libp2p behaviour for Rucio.
//!
//! `identify` and `kademlia` are always mounted — they are what make a node a
//! DHT participant. The rest (mDNS discovery, gossipsub search, and the
//! transfer / manifest request-response protocols) are optional, wrapped in
//! [`Toggle`] and selected via [`BehaviourConfig`]. A disabled `Toggle`
//! advertises no protocol and emits no events, so a node that only needs the
//! DHT (e.g. a bootstrap node) carries no inert protocol surface.

use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::{
    connection_limits, dcutr, gossipsub, identify, kad, mdns, relay, request_response,
    swarm::NetworkBehaviour,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use super::manifest_codec::{ManifestCodec, ManifestProtocol};
use super::transfer_codec::{TransferCodec, TransferProtocol};
use rucio_core::protocol::manifest::{ManifestRequest, ManifestResponse};
use rucio_core::protocol::transfer::{ChunkRequest, ChunkResponse};

pub const TOPIC_SEARCH: &str = "/rucio/search/1.0.0";
pub const TOPIC_SEARCH_RESULT: &str = "/rucio/search/result/1.0.0";

/// Protocol ID advertised by peers that can serve as a circuit relay (hop).
pub const RELAY_HOP_PROTOCOL: &str = "/libp2p/circuit/relay/0.2.0/hop";

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
    /// Act as a circuit relay server (hop).  HighID nodes enable this so that
    /// LowID nodes behind NAT can make reservations and be reachable via
    /// `/p2p-circuit` addresses.  The built-in resource limits prevent abuse.
    pub relay_server: bool,
    /// Enable DCUtR hole punching.  When a LowID node connects to a peer
    /// through a relay, DCUtR attempts to upgrade to a direct connection by
    /// coordinating simultaneous TCP/QUIC dials (NAT hole punch).
    pub dcutr: bool,
    /// Kademlia `MemoryStore` cap on **self-provided** keys — i.e. how many of
    /// our own shared files we can announce. The libp2p default (1024) is far
    /// too low for a real library, so set this generously.
    pub kad_max_provided_keys: usize,
    /// Kademlia `MemoryStore` cap on **stored** records — provider records from
    /// *other* peers that we hold in RAM as a DHT server. A client keeps this
    /// modest (it shouldn't become a large in-memory store); a bootstrap /
    /// indexer node, which sees the whole network, sets it high.
    ///
    /// This is a RAM ceiling, not a hard data limit: a bootstrap/indexer also
    /// persists every captured record to SQLite for search, so hitting this cap
    /// loses only DHT re-serving from RAM, not the index. See the storage-model
    /// notes in `rucio-bootstrap`'s `indexer` module before raising it or
    /// reaching for a disk-backed `RecordStore`.
    pub kad_max_records: usize,
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
            relay_server: true,
            dcutr: true,
            // We may share many files; keep records (others' provider records
            // we hold as a DHT server) modest so a client isn't a big RAM store.
            kad_max_provided_keys: 1_000_000,
            kad_max_records: 100_000,
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
            relay_server: false,
            dcutr: false,
            // A bootstrap node provides no files of its own but sees the whole
            // network, so hold few provided keys and many stored records.
            kad_max_provided_keys: 1024,
            kad_max_records: 1_000_000,
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
    /// Guards against connection-count abuse (per-peer and pending-inbound
    /// caps). Emits no events. Listed first so its limit checks run before the
    /// other behaviours accept a connection.
    pub connection_limits: connection_limits::Behaviour,
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
    pub gossipsub: Toggle<gossipsub::Behaviour>,
    pub transfer: Toggle<TransferBehaviour>,
    pub manifest: Toggle<ManifestBehaviour>,
    /// Circuit relay server: lets other (LowID) peers make reservations so
    /// they become reachable via `/p2p-circuit` addresses.
    pub relay: Toggle<relay::Behaviour>,
    /// Circuit relay client: allows this node to connect through a relay and
    /// also required for DCUtR (the relay transport is wired in at the
    /// SwarmBuilder level).
    pub relay_client: relay::client::Behaviour,
    /// DCUtR hole-punching: upgrades relay-mediated connections to direct ones.
    pub dcutr: Toggle<dcutr::Behaviour>,
}

impl RucioBehaviour {
    pub fn new(
        keypair: &libp2p::identity::Keypair,
        peer_id: libp2p::PeerId,
        relay_client: relay::client::Behaviour,
        cfg: BehaviourConfig,
    ) -> anyhow::Result<Self> {
        // Cap connection-count abuse without throttling a bootstrap node's
        // legitimate fan-in: limit how many connections a single peer may hold
        // and how many inbound handshakes can be in flight at once, but leave
        // the total number of established connections unbounded.
        let connection_limits = connection_limits::Behaviour::new(
            connection_limits::ConnectionLimits::default()
                .with_max_established_per_peer(Some(8))
                .with_max_pending_incoming(Some(128)),
        );

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
        // The default MemoryStore caps both stored and self-provided keys at
        // 1024 — far too low for a real library (a node sharing >1024 files
        // would fail to announce the excess: "store cannot contain any more
        // provider records"). The caps are role-tuned via BehaviourConfig:
        // generous self-provided keys for everyone, modest stored records on a
        // client and a large pool on a bootstrap/indexer node.
        let store_config = kad::store::MemoryStoreConfig {
            max_provided_keys: cfg.kad_max_provided_keys,
            max_records: cfg.kad_max_records,
            ..Default::default()
        };
        let store = kad::store::MemoryStore::with_config(peer_id, store_config);
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

        let relay = cfg
            .relay_server
            .then(|| relay::Behaviour::new(peer_id, relay::Config::default()));

        let dcutr = cfg.dcutr.then(|| dcutr::Behaviour::new(peer_id));

        Ok(Self {
            connection_limits,
            identify,
            kademlia,
            mdns: Toggle::from(mdns),
            gossipsub: Toggle::from(gossipsub),
            transfer: Toggle::from(transfer),
            manifest: Toggle::from(manifest),
            relay: Toggle::from(relay),
            relay_client,
            dcutr: Toggle::from(dcutr),
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
