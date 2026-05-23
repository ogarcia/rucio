//! The node task: owns the libp2p swarm and drives it to completion.
//!
//! Spawn with [`spawn`]; it returns a [`NodeHandle`] through which callers
//! send [`NodeCmd`]s and receive [`NodeEvent`]s.

use anyhow::{Context, Result};
use libp2p::futures::StreamExt;
use libp2p::{
    Multiaddr, SwarmBuilder,
    kad::{self, QueryId},
    swarm::SwarmEvent,
};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::NodeConfig;

use super::{
    behaviour::{RucioBehaviour, RucioBehaviourEvent},
    classify::ClassificationState,
    identity,
    messages::{NodeCmd, NodeEvent},
};

// Channel capacities
const CMD_BUFFER: usize = 64;
const EVENT_BUFFER: usize = 256;

// ---------------------------------------------------------------------------
// Public handle
// ---------------------------------------------------------------------------

/// A cheaply-cloneable handle to the running node task.
pub struct NodeHandle {
    pub cmd_tx: mpsc::Sender<NodeCmd>,
    pub event_rx: mpsc::Receiver<NodeEvent>,
}

// ---------------------------------------------------------------------------
// Public spawn function
// ---------------------------------------------------------------------------

/// Spawn the node task and return a [`NodeHandle`].
pub async fn spawn(cfg: &NodeConfig) -> Result<NodeHandle> {
    let keypair = identity::load_or_create(&cfg.identity_path)?;
    let peer_id = keypair.public().to_peer_id();

    let (cmd_tx, cmd_rx) = mpsc::channel::<NodeCmd>(CMD_BUFFER);
    let (event_tx, event_rx) = mpsc::channel::<NodeEvent>(EVENT_BUFFER);

    let listen_addrs: Vec<Multiaddr> = cfg
        .listen_addrs
        .iter()
        .filter_map(|s| {
            s.parse::<Multiaddr>()
                .map_err(|e| warn!("Invalid listen addr {s}: {e}"))
                .ok()
        })
        .collect();

    let behaviour = RucioBehaviour::new(&keypair, peer_id)?;
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )
        .context("building TCP transport")?
        .with_quic()
        .with_behaviour(|_| behaviour)
        .context("attaching behaviour")?
        .build();

    for addr in &listen_addrs {
        if let Err(e) = swarm.listen_on(addr.clone()) {
            warn!("Failed to listen on {addr}: {e}");
        }
    }

    tokio::spawn(run_loop(swarm, peer_id, cmd_rx, event_tx));

    Ok(NodeHandle { cmd_tx, event_rx })
}

// ---------------------------------------------------------------------------
// Event loop state
// ---------------------------------------------------------------------------

struct LoopState {
    confirmed_addrs: HashSet<Multiaddr>,
    ready_sent: bool,
    provider_queries: HashMap<QueryId, Vec<u8>>,
    classifier: ClassificationState,
}

impl LoopState {
    fn new() -> Self {
        Self {
            confirmed_addrs: HashSet::new(),
            ready_sent: false,
            provider_queries: HashMap::new(),
            classifier: ClassificationState::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

async fn run_loop(
    mut swarm: libp2p::Swarm<RucioBehaviour>,
    peer_id: libp2p::PeerId,
    mut cmd_rx: mpsc::Receiver<NodeCmd>,
    event_tx: mpsc::Sender<NodeEvent>,
) {
    let mut state = LoopState::new();

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    None | Some(NodeCmd::Shutdown) => {
                        info!("Node task shutting down");
                        break;
                    }
                    Some(NodeCmd::AddBootstrapPeer(addr)) => {
                        info!(%addr, "Dialling bootstrap peer");
                        if let Err(e) = swarm.dial(addr) {
                            warn!("Dial failed: {e}");
                        }
                    }
                    Some(NodeCmd::StartProviding(key)) => {
                        let record_key = kad::RecordKey::new(&key);
                        if let Err(e) = swarm.behaviour_mut().kademlia.start_providing(record_key) {
                            warn!("start_providing error: {e}");
                        }
                    }
                    Some(NodeCmd::StopProviding(key)) => {
                        let record_key = kad::RecordKey::new(&key);
                        swarm.behaviour_mut().kademlia.stop_providing(&record_key);
                    }
                    Some(NodeCmd::FindProviders(key)) => {
                        let record_key = kad::RecordKey::new(&key);
                        let qid = swarm.behaviour_mut().kademlia.get_providers(record_key);
                        state.provider_queries.insert(qid, key);
                    }
                }
            }

            event = swarm.next() => {
                let Some(event) = event else { break };
                handle_swarm_event(event, &event_tx, &mut state, peer_id).await;
            }
        }
    }
}

async fn handle_swarm_event(
    event: SwarmEvent<RucioBehaviourEvent>,
    event_tx: &mpsc::Sender<NodeEvent>,
    state: &mut LoopState,
    peer_id: libp2p::PeerId,
) {
    match event {
        // ---- listener events -------------------------------------------
        SwarmEvent::NewListenAddr { address, .. } => {
            info!(%address, "Listening");
            state.confirmed_addrs.insert(address);
            if !state.ready_sent {
                state.ready_sent = true;
                let _ = event_tx
                    .send(NodeEvent::Ready {
                        peer_id,
                        listen_addrs: state.confirmed_addrs.iter().cloned().collect(),
                    })
                    .await;
            }
        }
        SwarmEvent::ListenerClosed {
            addresses, reason, ..
        } => {
            warn!(?addresses, ?reason, "Listener closed");
            for a in &addresses {
                state.confirmed_addrs.remove(a);
            }
        }

        // ---- connection events -----------------------------------------
        SwarmEvent::ConnectionEstablished { peer_id: pid, .. } => {
            debug!(%pid, "Connection established");
        }
        SwarmEvent::ConnectionClosed {
            peer_id: pid,
            cause,
            ..
        } => {
            debug!(%pid, ?cause, "Connection closed");
        }
        SwarmEvent::OutgoingConnectionError { error, .. } => {
            warn!(%error, "Outgoing connection error");
        }

        // ---- behaviour events ------------------------------------------
        SwarmEvent::Behaviour(bev) => match bev {
            RucioBehaviourEvent::Mdns(mdns_event) => {
                use libp2p::mdns::Event;
                match mdns_event {
                    Event::Discovered(peers) => {
                        let mut by_peer: HashMap<libp2p::PeerId, Vec<Multiaddr>> = HashMap::new();
                        for (pid, addr) in peers {
                            by_peer.entry(pid).or_default().push(addr);
                        }
                        for (pid, addrs) in by_peer {
                            info!(%pid, "mDNS discovered peer");
                            let _ = event_tx
                                .send(NodeEvent::PeerDiscovered {
                                    peer_id: pid,
                                    addrs,
                                })
                                .await;
                        }
                    }
                    Event::Expired(peers) => {
                        let mut seen = HashSet::new();
                        for (pid, _) in peers {
                            if seen.insert(pid) {
                                let _ =
                                    event_tx.send(NodeEvent::PeerExpired { peer_id: pid }).await;
                            }
                        }
                    }
                }
            }

            RucioBehaviourEvent::Kademlia(kad_event) => {
                use kad::Event;
                match kad_event {
                    Event::OutboundQueryProgressed { id, result, .. } => {
                        use kad::QueryResult;
                        if let QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                            providers,
                            ..
                        })) = result
                            && let Some(key) = state.provider_queries.get(&id)
                        {
                            let _ = event_tx
                                .send(NodeEvent::ProvidersFound {
                                    key: key.clone(),
                                    providers: providers.into_iter().collect(),
                                })
                                .await;
                        }
                    }
                    Event::RoutingUpdated { peer, .. } => {
                        debug!(%peer, "Kademlia routing table updated");
                    }
                    _ => {}
                }
            }

            RucioBehaviourEvent::Identify(id_event) => {
                use libp2p::identify::Event;
                match id_event {
                    Event::Received {
                        peer_id: pid, info, ..
                    } => {
                        debug!(%pid, agent = %info.agent_version, "Identify received");

                        let observed = info.observed_addr.clone();
                        let listen_vec: Vec<Multiaddr> =
                            state.confirmed_addrs.iter().cloned().collect();

                        // Emit the raw observation so consumers can log/display it
                        let _ = event_tx
                            .send(NodeEvent::ObservedAddr {
                                addr: observed.clone(),
                                reported_by: pid,
                            })
                            .await;

                        // Run the classifier; emit ClassChanged only if it changes
                        if let Some(new_class) =
                            state
                                .classifier
                                .record_observation(observed, pid, &listen_vec)
                        {
                            info!(?new_class, "Node class determined");
                            let _ = event_tx.send(NodeEvent::ClassChanged(new_class)).await;
                        }
                    }
                    Event::Sent { .. } => {}
                    _ => {}
                }
            }
        },

        _ => {}
    }
}
