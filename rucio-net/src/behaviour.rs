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
    autonat, connection_limits, dcutr, gossipsub, identify, kad, mdns, relay, request_response,
    swarm::NetworkBehaviour,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use super::have_codec::{HaveCodec, HaveProtocol};
use super::manifest_codec::{ManifestCodec, ManifestProtocol};
use super::outboard_codec::{OutboardCodec, OutboardProtocol};
use super::pinset_codec::{PinsetCodec, PinsetProtocol};
use super::transfer_codec::{TransferCodec, TransferProtocol};
use rucio_core::protocol::manifest::{ManifestRequest, ManifestResponse};
use rucio_core::protocol::outboard::{OutboardRequest, OutboardResponse};
use rucio_core::protocol::transfer::{ChunkRequest, ChunkResponse};

pub const TOPIC_SEARCH: &str = "/rucio/search/1.0.0";
pub const TOPIC_SEARCH_RESULT: &str = "/rucio/search/result/1.0.0";

/// Protocol ID advertised by peers that can serve as a circuit relay (hop).
pub const RELAY_HOP_PROTOCOL: &str = "/libp2p/circuit/relay/0.2.0/hop";

/// AutoNAT v2 dial-request protocol, advertised by nodes running the AutoNAT
/// server. Used to recognise which connected peers can perform reachability
/// dial-backs for us (so a new external-address candidate knows whether it
/// already has a server to be verified against).
pub const AUTONAT_DIAL_REQUEST_PROTOCOL: &str = "/libp2p/autonat/2/dial-request";

pub type TransferBehaviour = request_response::Behaviour<TransferCodec>;
pub type ManifestBehaviour = request_response::Behaviour<ManifestCodec>;
pub type OutboardBehaviour = request_response::Behaviour<OutboardCodec>;
pub type PinsetBehaviour = request_response::Behaviour<PinsetCodec>;
pub type HaveBehaviour = request_response::Behaviour<HaveCodec>;

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
    /// Outboard request-response protocol (fetch the full bao outboard of a file
    /// by its root hash to rebuild a lost `.obao` sidecar).
    pub outboard: bool,
    /// Pin-set request-response protocol (cooperative pinning: serve our pin-set
    /// and fetch peers' pin-sets).
    pub pinset: bool,
    /// Availability request-response protocol (`/have`): serve our per-chunk
    /// "have" bitmap and query providers' to learn aggregate swarm coverage.
    pub have: bool,
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
    /// Cap on **stored records from other peers** that we hold in RAM as a DHT
    /// server — both Kademlia value records (`max_records`) and, crucially, the
    /// provider records re-stored on inbound `AddProvider`. A client keeps this
    /// modest (it shouldn't become a large in-memory store); a bootstrap /
    /// indexer node, which sees the whole network, sets it high.
    ///
    /// Provider records need explicit handling: libp2p's `MemoryStore` counts
    /// them under `max_provided_keys` *together with our own announced shares*,
    /// so it can't bound foreign ones without also limiting how many files we
    /// can share. The task loop therefore enforces this cap itself via a
    /// second-chance (CLOCK) sweep, so records still being refreshed survive and
    /// only idle ones are evicted once the count is exceeded.
    ///
    /// This is a RAM ceiling, not a hard data limit: a bootstrap/indexer also
    /// persists every captured record to SQLite for search, so hitting this cap
    /// loses only DHT re-serving from RAM, not the index. See the storage-model
    /// notes in `rucio-bootstrap`'s `indexer` module before raising it or
    /// reaching for a disk-backed `RecordStore`.
    pub kad_max_records: usize,
    /// Role token appended to the Identify agent string so infrastructure nodes
    /// are recognisable on the network, e.g. `Rucio/0.28.0 (Linux x86_64;
    /// bootstrap) libp2p/0.56.0`. `None` for a plain node (the daemon), which
    /// keeps the unadorned `(<os> <arch>)` form.
    pub agent_role: Option<&'static str>,
}

impl BehaviourConfig {
    /// A full participating node: everything enabled (the daemon).
    pub fn full() -> Self {
        Self {
            mdns: true,
            gossipsub: true,
            transfer: true,
            manifest: true,
            outboard: true,
            pinset: true,
            have: true,
            capture_provider_records: false,
            relay_server: true,
            dcutr: true,
            // We may share many files, so allow plenty of self-provided keys;
            // but keep foreign records (others' provider records we hold as a
            // DHT server, CLOCK-evicted in the task loop) modest so a client
            // isn't a big in-RAM store — ~50k ≈ a few hundred MB at most.
            kad_max_provided_keys: 1_000_000,
            kad_max_records: 50_000,
            // A plain node advertises no role token.
            agent_role: None,
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
            outboard: false,
            pinset: false,
            have: false,
            capture_provider_records: false,
            relay_server: false,
            dcutr: false,
            // A bootstrap node provides no files of its own but sees the whole
            // network, so hold few provided keys and many stored records.
            kad_max_provided_keys: 1024,
            kad_max_records: 1_000_000,
            agent_role: Some("bootstrap"),
        }
    }

    /// A DHT indexer: like [`dht_only`](Self::dht_only) but capturing provider
    /// announcements, and optionally mounting `manifest` to enrich records with
    /// the file name and size by querying the announcing peer.
    pub fn indexer(enrich: bool) -> Self {
        Self {
            manifest: enrich,
            capture_provider_records: true,
            // An indexer is a bootstrap that also harvests provider records;
            // mark it distinctly so it stands out from a plain bootstrap.
            agent_role: Some("indexer"),
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
    pub outboard: Toggle<OutboardBehaviour>,
    pub pinset: Toggle<PinsetBehaviour>,
    pub have: Toggle<HaveBehaviour>,
    /// Circuit relay server: lets other (LowID) peers make reservations so
    /// they become reachable via `/p2p-circuit` addresses.
    pub relay: Toggle<relay::Behaviour>,
    /// Circuit relay client: allows this node to connect through a relay and
    /// also required for DCUtR (the relay transport is wired in at the
    /// SwarmBuilder level).
    pub relay_client: relay::client::Behaviour,
    /// DCUtR hole-punching: upgrades relay-mediated connections to direct ones.
    pub dcutr: Toggle<dcutr::Behaviour>,
    /// AutoNAT v2 client: asks other nodes to dial our external-address
    /// candidates (which `identify` derives by translating the observed public
    /// IP onto our listen port). A successful probe confirms the address with
    /// the swarm, which is how we reliably reach `HighId` — instead of waiting
    /// for a peer to happen to dial us inbound.
    pub autonat_client: autonat::v2::client::Behaviour,
    /// AutoNAT v2 server: performs those dial-back probes for other nodes.
    /// Mounted on every node (reciprocity); the bootstrap especially needs it so
    /// a freshly-started public node can get confirmed from a single probe.
    pub autonat_server: autonat::v2::server::Behaviour,
}

impl RucioBehaviour {
    pub fn new(
        keypair: &libp2p::identity::Keypair,
        peer_id: libp2p::PeerId,
        relay_client: relay::client::Behaviour,
        cfg: BehaviourConfig,
        upload_limiter: Option<crate::codec_utils::ByteLimiter>,
        download_progress: Option<crate::codec_utils::ReadProgress>,
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

        // Identify carries two strings: protocol_version gates compatibility
        // (peers on a different /rucio/<v> won't talk), while agent_version is a
        // free-form, HTTP User-Agent-style label purely for diagnostics. We model
        // it on Firefox's UA — product/version, platform in parentheses, then the
        // underlying "engine" (libp2p plays Gecko's role). Unlike on the eMule
        // network we identify openly here: this is our own network, not one to
        // blend into.
        let identify = identify::Behaviour::new(
            identify::Config::new("/rucio/1.0.0".to_string(), keypair.public())
                .with_agent_version(agent_version(cfg.agent_role)),
        );

        let mut kademlia_config = kad::Config::new(libp2p::StreamProtocol::new("/rucio/kad/1.0.0"));
        // Refresh the routing table periodically so it doesn't go stale as peers
        // churn. libp2p enables this by default, but pin it explicitly so the
        // behaviour doesn't silently change if that default ever does.
        kademlia_config.set_periodic_bootstrap_interval(Some(Duration::from_secs(5 * 60)));
        // Disable libp2p's internal provider re-publication: it re-announces every
        // provided key in a single burst, which balloons the QueryPool on a large
        // share library (each query carries ~10 KB and the pool's table never
        // shrinks). We drive re-provide ourselves from the task loop, under the
        // same in-flight concurrency cap as the initial announce.
        kademlia_config.set_provider_publication_interval(None);
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
            request_response::Behaviour::with_codec(
                TransferCodec::new(upload_limiter, download_progress),
                vec![(TransferProtocol, request_response::ProtocolSupport::Full)],
                // Per-request budget for a chunk transfer (up to 4 MiB). The
                // downloader caps how many chunks it keeps in-flight to one peer
                // adaptively (see `transfer.rs` PeerState): it backs off to a
                // single in-flight chunk when a peer times out, so a slow peer's
                // chunk gets the whole link and completes in chunk/rate — the
                // shortest possible time. This timeout therefore only needs to
                // cover ONE 4 MiB chunk on a slow link (not several at once),
                // and it doubles as the signal that triggers the back-off, so it
                // shouldn't be huge or convergence drags (each back-off step
                // waits one timeout). 120 s covers a 4 MiB chunk down to
                // ~34 KB/s. A dropped connection fails its requests immediately,
                // independent of this timeout.
                request_response::Config::default()
                    .with_request_timeout(std::time::Duration::from_secs(120)),
            )
        });

        let manifest = cfg.manifest.then(|| {
            request_response::Behaviour::new(
                vec![(ManifestProtocol, request_response::ProtocolSupport::Full)],
                request_response::Config::default(),
            )
        });

        let outboard = cfg.outboard.then(|| {
            request_response::Behaviour::new(
                vec![(OutboardProtocol, request_response::ProtocolSupport::Full)],
                request_response::Config::default(),
            )
        });

        let pinset = cfg.pinset.then(|| {
            request_response::Behaviour::new(
                vec![(PinsetProtocol, request_response::ProtocolSupport::Full)],
                request_response::Config::default(),
            )
        });

        let have = cfg.have.then(|| {
            request_response::Behaviour::new(
                vec![(HaveProtocol, request_response::ProtocolSupport::Full)],
                request_response::Config::default(),
            )
        });

        let relay = cfg
            .relay_server
            .then(|| relay::Behaviour::new(peer_id, relay::Config::default()));

        let dcutr = cfg.dcutr.then(|| dcutr::Behaviour::new(peer_id));

        // AutoNAT v2: always mounted (client + server). Default config uses an
        // OS RNG and is fine for our use — no candidates are tested until
        // identify supplies one, and the server only acts when a peer asks.
        let autonat_client = autonat::v2::client::Behaviour::default();
        let autonat_server = autonat::v2::server::Behaviour::default();

        Ok(Self {
            connection_limits,
            identify,
            kademlia,
            mdns: Toggle::from(mdns),
            gossipsub: Toggle::from(gossipsub),
            transfer: Toggle::from(transfer),
            manifest: Toggle::from(manifest),
            outboard: Toggle::from(outboard),
            pinset: Toggle::from(pinset),
            have: Toggle::from(have),
            relay: Toggle::from(relay),
            relay_client,
            dcutr: Toggle::from(dcutr),
            autonat_client,
            autonat_server,
        })
    }
}

/// HTTP User-Agent-style identifier advertised over Identify, e.g.
/// `Rucio/0.28.0 (Linux x86_64) libp2p/0.56.0`, or with a role token for
/// infrastructure nodes: `Rucio/0.28.0 (Linux x86_64; bootstrap) libp2p/0.56.0`.
/// The crate version and platform come from the compiler; the libp2p version is
/// resolved from Cargo.lock by build.rs (RUCIO_LIBP2P_VERSION).
fn agent_version(role: Option<&str>) -> String {
    // std::env::consts::OS is lowercase ("linux"); spell the common ones the way
    // a UA would and leave anything unexpected as-is.
    let os = match std::env::consts::OS {
        "linux" => "Linux",
        "macos" => "macOS",
        "windows" => "Windows",
        "freebsd" => "FreeBSD",
        "android" => "Android",
        "ios" => "iOS",
        other => other,
    };
    let arch = std::env::consts::ARCH;
    // Mirror Firefox's "(platform; detail)" convention: the role is an extra
    // semicolon-separated token inside the same parenthesised group.
    let platform = match role {
        Some(role) => format!("{os} {arch}; {role}"),
        None => format!("{os} {arch}"),
    };
    format!(
        "Rucio/{} ({platform}) libp2p/{}",
        env!("CARGO_PKG_VERSION"),
        env!("RUCIO_LIBP2P_VERSION"),
    )
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
pub type OutboardReq = OutboardRequest;
pub type OutboardResp = OutboardResponse;
pub use rucio_core::protocol::have::{HaveRequest, HaveResponse};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_version_is_well_formed() {
        let ua = agent_version(None);
        // Shape: Rucio/<ver> (<os> <arch>) libp2p/<ver>
        assert!(ua.starts_with("Rucio/"), "missing product: {ua}");
        assert!(ua.contains(" libp2p/"), "missing engine: {ua}");
        // build.rs must have resolved the real lock version, not the fallback.
        assert!(
            !ua.ends_with("libp2p/unknown"),
            "libp2p version unresolved: {ua}"
        );
        // A plain node carries no role token inside the parentheses.
        assert!(
            !ua.contains(';'),
            "plain node should not carry a role: {ua}"
        );
    }

    #[test]
    fn agent_version_carries_role_token() {
        let ua = agent_version(Some("bootstrap"));
        assert!(
            ua.contains("; bootstrap) libp2p/"),
            "role token missing or malformed: {ua}"
        );
    }
}
