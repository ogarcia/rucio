//! Composite libp2p behaviour for Rucio.
//!
//! Combines:
//! - **Identify**   — announces our listen addresses and agent version to peers
//! - **Kademlia**   — DHT for content-provider records
//! - **mDNS**       — local peer discovery (LAN / development)
//! - **Gossipsub**  — flood-based search query / result propagation

use libp2p::{gossipsub, identify, kad, mdns, swarm::NetworkBehaviour};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// Gossipsub topic for outbound search queries.
pub const TOPIC_SEARCH: &str = "/rucio/search/1.0.0";
/// Gossipsub topic for search results.
pub const TOPIC_SEARCH_RESULT: &str = "/rucio/search/result/1.0.0";

/// The combined network behaviour.
#[derive(NetworkBehaviour)]
pub struct RucioBehaviour {
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
}

impl RucioBehaviour {
    pub fn new(
        keypair: &libp2p::identity::Keypair,
        peer_id: libp2p::PeerId,
    ) -> anyhow::Result<Self> {
        // Identify: tell peers who we are
        let identify = identify::Behaviour::new(identify::Config::new(
            "/rucio/1.0.0".to_string(),
            keypair.public(),
        ));

        // Kademlia: content-provider DHT
        let kademlia_config = kad::Config::new(libp2p::StreamProtocol::new("/rucio/kad/1.0.0"));
        let store = kad::store::MemoryStore::new(peer_id);
        let kademlia = kad::Behaviour::with_config(peer_id, store, kademlia_config);

        // mDNS: automatic LAN discovery
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?;

        // Gossipsub: search query/result propagation
        //
        // Message authenticity: sign with our keypair so peers can verify the
        // sender.  Message deduplication is handled by gossipsub itself.
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10))
            .validation_mode(gossipsub::ValidationMode::Strict)
            // Use a fast message-id function to avoid duplicate forwarding.
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

        Ok(Self {
            identify,
            kademlia,
            mdns,
            gossipsub,
        })
    }
}
