//! Composite libp2p behaviour for Rucio.
//!
//! Combines:
//! - **Identify**          — announces our listen addresses and agent version
//! - **Kademlia**          — DHT for content-provider records
//! - **mDNS**              — local peer discovery (LAN / development)
//! - **Gossipsub**         — search query / result propagation
//! - **RequestResponse**   — chunk transfer protocol `/rucio/transfer/1.0.0`

use libp2p::{gossipsub, identify, kad, mdns, request_response, swarm::NetworkBehaviour};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use super::transfer_codec::{TransferCodec, TransferProtocol};
use rucio_core::protocol::transfer::{ChunkRequest, ChunkResponse};

pub const TOPIC_SEARCH: &str = "/rucio/search/1.0.0";
pub const TOPIC_SEARCH_RESULT: &str = "/rucio/search/result/1.0.0";

/// Convenience alias for the transfer request-response behaviour.
pub type TransferBehaviour = request_response::Behaviour<TransferCodec>;

/// The combined network behaviour.
#[derive(NetworkBehaviour)]
pub struct RucioBehaviour {
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub transfer: TransferBehaviour,
}

impl RucioBehaviour {
    pub fn new(
        keypair: &libp2p::identity::Keypair,
        peer_id: libp2p::PeerId,
    ) -> anyhow::Result<Self> {
        let identify = identify::Behaviour::new(identify::Config::new(
            "/rucio/1.0.0".to_string(),
            keypair.public(),
        ));

        let kademlia_config = kad::Config::new(libp2p::StreamProtocol::new("/rucio/kad/1.0.0"));
        let store = kad::store::MemoryStore::new(peer_id);
        let kademlia = kad::Behaviour::with_config(peer_id, store, kademlia_config);

        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?;

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .message_id_fn(|msg: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                msg.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_be_bytes())
            })
            .build()
            .map_err(|e| anyhow::anyhow!("gossipsub config: {e}"))?;

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .map_err(|e| anyhow::anyhow!("gossipsub behaviour: {e}"))?;

        let transfer = request_response::Behaviour::new(
            vec![(TransferProtocol, request_response::ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        Ok(Self {
            identify,
            kademlia,
            mdns,
            gossipsub,
            transfer,
        })
    }
}

// Re-export types needed by task.rs
pub use request_response::{OutboundRequestId, ResponseChannel};
pub type TransferRequest = ChunkRequest;
pub type TransferResponse = ChunkResponse;
