//! The node task: owns the libp2p swarm and drives it to completion.

use anyhow::{Context, Result};
use libp2p::futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder,
    gossipsub::{self, IdentTopic},
    kad::{self, QueryId},
    request_response::{self, ResponseChannel},
    swarm::SwarmEvent,
};
use rucio_core::protocol::{
    manifest::{ManifestRequest, ManifestResponse},
    search::{SearchQuery, SearchResult},
    transfer::{ChunkRequest, ChunkResponse},
};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::NodeConfig;

use super::{
    behaviour::{RucioBehaviour, RucioBehaviourEvent, TOPIC_SEARCH, TOPIC_SEARCH_RESULT},
    classify::ClassificationState,
    identity,
    messages::{NodeCmd, NodeEvent},
};

const CMD_BUFFER: usize = 64;
const EVENT_BUFFER: usize = 256;

// ---------------------------------------------------------------------------
// Public handle
// ---------------------------------------------------------------------------

pub struct NodeHandle {
    pub cmd_tx: mpsc::Sender<NodeCmd>,
    pub event_rx: mpsc::Receiver<NodeEvent>,
}

// ---------------------------------------------------------------------------
// spawn
// ---------------------------------------------------------------------------

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

    let behaviour = super::behaviour::RucioBehaviour::new(&keypair, peer_id)?;
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

    let topic_query = IdentTopic::new(TOPIC_SEARCH);
    let topic_result = IdentTopic::new(TOPIC_SEARCH_RESULT);
    if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&topic_query) {
        warn!("Failed to subscribe to search topic: {e}");
    }
    if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&topic_result) {
        warn!("Failed to subscribe to search-result topic: {e}");
    }

    for addr in &listen_addrs {
        if let Err(e) = swarm.listen_on(addr.clone()) {
            warn!("Failed to listen on {addr}: {e}");
        }
    }

    tokio::spawn(run_loop(swarm, peer_id, cmd_rx, event_tx));

    Ok(NodeHandle { cmd_tx, event_rx })
}

// ---------------------------------------------------------------------------
// Loop state
// ---------------------------------------------------------------------------

struct LoopState {
    confirmed_addrs: HashSet<Multiaddr>,
    ready_sent: bool,
    provider_queries: HashMap<QueryId, Vec<u8>>,
    classifier: ClassificationState,
    /// Pending inbound chunk request channels keyed by a monotonic id.
    pending_chunk_channels: HashMap<u64, ResponseChannel<ChunkResponse>>,
    /// Pending inbound manifest request channels keyed by a monotonic id.
    pending_manifest_channels: HashMap<u64, ResponseChannel<ManifestResponse>>,
    next_channel_id: u64,
}

impl LoopState {
    fn new() -> Self {
        Self {
            confirmed_addrs: HashSet::new(),
            ready_sent: false,
            provider_queries: HashMap::new(),
            classifier: ClassificationState::default(),
            pending_chunk_channels: HashMap::new(),
            pending_manifest_channels: HashMap::new(),
            next_channel_id: 0,
        }
    }

    fn store_chunk_channel(&mut self, ch: ResponseChannel<ChunkResponse>) -> u64 {
        let id = self.next_channel_id;
        self.next_channel_id += 1;
        self.pending_chunk_channels.insert(id, ch);
        id
    }

    fn store_manifest_channel(&mut self, ch: ResponseChannel<ManifestResponse>) -> u64 {
        let id = self.next_channel_id;
        self.next_channel_id += 1;
        self.pending_manifest_channels.insert(id, ch);
        id
    }
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

async fn run_loop(
    mut swarm: libp2p::Swarm<RucioBehaviour>,
    peer_id: PeerId,
    mut cmd_rx: mpsc::Receiver<NodeCmd>,
    event_tx: mpsc::Sender<NodeEvent>,
) {
    let topic_query = IdentTopic::new(TOPIC_SEARCH);
    let topic_result = IdentTopic::new(TOPIC_SEARCH_RESULT);
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
                    Some(NodeCmd::Search(query)) => {
                        publish_json(&mut swarm, &topic_query, &query, "search query");
                    }
                    Some(NodeCmd::PublishSearchResult(result)) => {
                        publish_json(&mut swarm, &topic_result, &result, "search result");
                    }
                    Some(NodeCmd::RequestChunk { peer, request }) => {
                        swarm.behaviour_mut().transfer.send_request(&peer, request);
                    }
                    Some(NodeCmd::RespondChunk { channel_id, response }) => {
                        if let Some(ch) = state.pending_chunk_channels.remove(&channel_id) {
                            if let Err(e) = swarm.behaviour_mut().transfer.send_response(ch, response) {
                                warn!("Failed to send chunk response: {e:?}");
                            }
                        } else {
                            warn!(%channel_id, "RespondChunk: unknown channel id");
                        }
                    }
                    Some(NodeCmd::RequestManifest { peer, request }) => {
                        swarm.behaviour_mut().manifest.send_request(&peer, request);
                    }
                    Some(NodeCmd::RespondManifest { channel_id, response }) => {
                        if let Some(ch) = state.pending_manifest_channels.remove(&channel_id) {
                            if let Err(e) = swarm.behaviour_mut().manifest.send_response(ch, response) {
                                warn!("Failed to send manifest response: {e:?}");
                            }
                        } else {
                            warn!(%channel_id, "RespondManifest: unknown channel id");
                        }
                    }
                }
            }

            event = swarm.next() => {
                let Some(event) = event else { break };
                on_swarm_event(event, &event_tx, &mut state, peer_id, &mut swarm).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Publish helper
// ---------------------------------------------------------------------------

fn publish_json<T: serde::Serialize>(
    swarm: &mut libp2p::Swarm<RucioBehaviour>,
    topic: &IdentTopic,
    value: &T,
    label: &str,
) {
    match serde_json::to_vec(value) {
        Ok(bytes) => {
            if let Err(e) = swarm
                .behaviour_mut()
                .gossipsub
                .publish(topic.clone(), bytes)
            {
                debug!("Could not publish {label}: {e}");
            } else {
                debug!("Published {label}");
            }
        }
        Err(e) => warn!("Failed to serialise {label}: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Swarm event handler
// ---------------------------------------------------------------------------

async fn on_swarm_event(
    event: SwarmEvent<RucioBehaviourEvent>,
    event_tx: &mpsc::Sender<NodeEvent>,
    state: &mut LoopState,
    peer_id: PeerId,
    swarm: &mut libp2p::Swarm<RucioBehaviour>,
) {
    match event {
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
        SwarmEvent::ConnectionEstablished { peer_id: pid, .. } => {
            debug!(%pid, "Connection established");
            swarm.behaviour_mut().gossipsub.add_explicit_peer(&pid);
        }
        SwarmEvent::ConnectionClosed {
            peer_id: pid,
            cause,
            ..
        } => {
            debug!(%pid, ?cause, "Connection closed");
            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&pid);
        }
        SwarmEvent::OutgoingConnectionError { error, .. } => {
            warn!(%error, "Outgoing connection error");
        }

        SwarmEvent::Behaviour(bev) => match bev {
            RucioBehaviourEvent::Mdns(mdns_event) => {
                use libp2p::mdns::Event;
                match mdns_event {
                    Event::Discovered(peers) => {
                        let mut by_peer: HashMap<PeerId, Vec<Multiaddr>> = HashMap::new();
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

                        let _ = event_tx
                            .send(NodeEvent::ObservedAddr {
                                addr: observed.clone(),
                                reported_by: pid,
                            })
                            .await;

                        if let Some(new_class) =
                            state
                                .classifier
                                .record_observation(observed, pid, &listen_vec)
                        {
                            info!(?new_class, "Node class determined");
                            let _ = event_tx.send(NodeEvent::ClassChanged(new_class)).await;
                        }
                    }
                    Event::Sent { .. } | Event::Error { .. } => {}
                    _ => {}
                }
            }

            RucioBehaviourEvent::Gossipsub(gs_event) => {
                on_gossipsub_event(gs_event, event_tx).await;
            }

            RucioBehaviourEvent::Transfer(tr_event) => {
                on_transfer_event(tr_event, event_tx, state).await;
            }

            RucioBehaviourEvent::Manifest(mn_event) => {
                on_manifest_event(mn_event, event_tx, state).await;
            }
        },

        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Gossipsub handler
// ---------------------------------------------------------------------------

async fn on_gossipsub_event(event: gossipsub::Event, event_tx: &mpsc::Sender<NodeEvent>) {
    match event {
        gossipsub::Event::Message { message, .. } => {
            let topic_str = message.topic.as_str();

            if topic_str == TOPIC_SEARCH {
                match serde_json::from_slice::<SearchQuery>(&message.data) {
                    Ok(query) => {
                        debug!(id = %query.id, keywords = ?query.keywords, "Received search query");
                        let _ = event_tx.send(NodeEvent::SearchQueryReceived(query)).await;
                    }
                    Err(e) => warn!("Failed to decode search query: {e}"),
                }
            } else if topic_str == TOPIC_SEARCH_RESULT {
                match serde_json::from_slice::<SearchResult>(&message.data) {
                    Ok(result) => {
                        debug!(qid = %result.query_id, "Received search result from {}", result.provider);
                        let _ = event_tx.send(NodeEvent::SearchResult(result)).await;
                    }
                    Err(e) => warn!("Failed to decode search result: {e}"),
                }
            }
        }
        gossipsub::Event::Subscribed { peer_id, topic } => {
            debug!(%peer_id, %topic, "Peer subscribed");
        }
        gossipsub::Event::Unsubscribed { peer_id, topic } => {
            debug!(%peer_id, %topic, "Peer unsubscribed");
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Transfer (request-response) handler
// ---------------------------------------------------------------------------

async fn on_transfer_event(
    event: request_response::Event<ChunkRequest, ChunkResponse>,
    event_tx: &mpsc::Sender<NodeEvent>,
    state: &mut LoopState,
) {
    match event {
        // We received a response for a request we sent.
        request_response::Event::Message {
            peer,
            message:
                request_response::Message::Response {
                    request_id,
                    response,
                },
            ..
        } => {
            debug!(%peer, "Received chunk response");
            let _ = event_tx
                .send(NodeEvent::ChunkReceived {
                    request_id,
                    peer,
                    response,
                })
                .await;
        }

        // A remote peer is requesting a chunk from us.
        request_response::Event::Message {
            peer,
            message:
                request_response::Message::Request {
                    request, channel, ..
                },
            ..
        } => {
            debug!(%peer, chunk_idx = request.chunk_idx, "Received chunk request");
            let channel_id = state.store_chunk_channel(channel);
            let _ = event_tx
                .send(NodeEvent::ChunkRequested {
                    peer,
                    request,
                    channel_id,
                })
                .await;
        }

        request_response::Event::OutboundFailure { peer, error, .. } => {
            warn!(%peer, %error, "Outbound chunk request failed");
        }
        request_response::Event::InboundFailure { peer, error, .. } => {
            warn!(%peer, %error, "Inbound chunk request failed");
        }
        request_response::Event::ResponseSent { .. } => {}
    }
}

// ---------------------------------------------------------------------------
// Manifest (request-response) handler
// ---------------------------------------------------------------------------

async fn on_manifest_event(
    event: request_response::Event<ManifestRequest, ManifestResponse>,
    event_tx: &mpsc::Sender<NodeEvent>,
    state: &mut LoopState,
) {
    match event {
        request_response::Event::Message {
            peer,
            message:
                request_response::Message::Response {
                    request_id,
                    response,
                },
            ..
        } => {
            debug!(%peer, "Received manifest response");
            let _ = event_tx
                .send(NodeEvent::ManifestReceived {
                    request_id,
                    peer,
                    response,
                })
                .await;
        }

        request_response::Event::Message {
            peer,
            message:
                request_response::Message::Request {
                    request, channel, ..
                },
            ..
        } => {
            debug!(%peer, root_hash = hex::encode(request.root_hash), "Received manifest request");
            let channel_id = state.store_manifest_channel(channel);
            let _ = event_tx
                .send(NodeEvent::ManifestRequested {
                    peer,
                    request,
                    channel_id,
                })
                .await;
        }

        request_response::Event::OutboundFailure { peer, error, .. } => {
            warn!(%peer, %error, "Outbound manifest request failed");
        }
        request_response::Event::InboundFailure { peer, error, .. } => {
            warn!(%peer, %error, "Inbound manifest request failed");
        }
        request_response::Event::ResponseSent { .. } => {}
    }
}
