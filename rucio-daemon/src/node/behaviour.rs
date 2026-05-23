//! Composite libp2p behaviour for Rucio.
//!
//! Combines:
//! - **Identify** — announces our listen addresses and agent version to peers
//! - **Kademlia** — DHT for content-provider records
//! - **mDNS**     — local peer discovery (LAN / development)
//!
//! Gossipsub is intentionally left out of this struct for now; it will be
//! added in a follow-up once the basic DHT plumbing is verified.

use libp2p::{identify, kad, mdns, swarm::NetworkBehaviour};

/// The combined network behaviour.
#[derive(NetworkBehaviour)]
pub struct RucioBehaviour {
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub mdns: mdns::tokio::Behaviour,
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

        Ok(Self {
            identify,
            kademlia,
            mdns,
        })
    }
}
