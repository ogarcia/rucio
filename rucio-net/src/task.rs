//! The node task: owns the libp2p swarm and drives it to completion.

use anyhow::{Context, Result};
use libp2p::futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder,
    gossipsub::{self, IdentTopic},
    kad::{self, QueryId},
    multiaddr::Protocol,
    relay,
    request_response::{self, ResponseChannel},
    swarm::{DialError, SwarmEvent},
};
use rucio_core::protocol::{
    manifest::{ManifestRequest, ManifestResponse},
    node::NodeClass,
    search::{SearchQuery, SearchResult},
    transfer::{ChunkRequest, ChunkResponse},
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, info, warn};

use crate::NetConfig;

use super::{
    behaviour::{
        RELAY_HOP_PROTOCOL, RucioBehaviour, RucioBehaviourEvent, TOPIC_SEARCH, TOPIC_SEARCH_RESULT,
    },
    classify::{ClassificationState, is_stable_external_addr},
    identity,
    messages::{NodeCmd, NodeEvent},
};

const CMD_BUFFER: usize = 64;
const EVENT_BUFFER: usize = 1024;
/// Cap on the gossipsub retry backlog held while no mesh peer is subscribed.
/// Without a bound, a node that searches before any peer joins would grow this
/// queue indefinitely; oldest entries are dropped past this many.
const MAX_PENDING_PUBLISHES: usize = 256;

/// Event sender that never blocks the swarm reactor on a slow consumer.
///
/// The node task delivers events from the same `select!` loop that drives the
/// swarm. If we awaited a bounded `send()` and the consumer fell behind, the
/// full buffer would suspend that loop — and with it all network I/O (pings,
/// keepalive, Kademlia, gossipsub), eventually dropping every connection. So we
/// deliver with `try_send` instead: if the buffer is full the event is dropped
/// (counted, and warned periodically) rather than stalling the node. This is
/// safe for our event set — provider records are re-announced, searches and
/// transfers are retried — and a stalled-but-alive node is far worse than a few
/// dropped events under sustained overload.
struct EventTx {
    tx: mpsc::Sender<NodeEvent>,
    dropped: AtomicU64,
}

impl EventTx {
    fn new(tx: mpsc::Sender<NodeEvent>) -> Self {
        Self {
            tx,
            dropped: AtomicU64::new(0),
        }
    }

    /// Deliver an event without blocking the swarm. Drops (and accounts) the
    /// event if the consumer's buffer is full. `async` only so existing
    /// `.emit(..).await` call sites read naturally; it never awaits the channel.
    ///
    /// Returns `true` if the event was queued for the consumer, `false` if it
    /// was dropped (buffer full) or the receiver is gone. Callers that stash
    /// state keyed to an event (e.g. a response channel) use this to avoid
    /// leaking that state when the event never reaches the consumer.
    async fn emit(&self, ev: NodeEvent) -> bool {
        match self.tx.try_send(ev) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                // Warn on each power of two so the log surfaces a growing
                // backlog without spamming a line per dropped event.
                if n.is_power_of_two() {
                    warn!(
                        dropped_total = n,
                        "Event consumer overloaded — dropping node events"
                    );
                }
                false
            }
            // The receiver is gone; the loop exits on its own.
            Err(TrySendError::Closed(_)) => false,
        }
    }
}

/// How long to keep a connection with no active streams before closing it.
/// libp2p defaults this to zero (immediate close); we hold connections so the
/// gossipsub mesh and Kademlia routing table stay warm and connections are
/// reused across queries instead of re-dialled.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

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

pub async fn spawn(cfg: &NetConfig) -> Result<NodeHandle> {
    let keypair = identity::load_or_create(&cfg.identity_path)?;
    let peer_id = keypair.public().to_peer_id();

    let (cmd_tx, cmd_rx) = mpsc::channel::<NodeCmd>(CMD_BUFFER);
    let (event_tx, event_rx) = mpsc::channel::<NodeEvent>(EVENT_BUFFER);

    let listen_addrs: Vec<Multiaddr> = cfg
        .listen_addrs
        .iter()
        .filter_map(|s| {
            s.parse::<Multiaddr>()
                .map_err(|e| warn!("Invalid listen addr {s:?}: {e} — expected multiaddr format, e.g. /ip4/0.0.0.0/tcp/4321"))
                .ok()
        })
        .collect();

    if listen_addrs.is_empty() {
        anyhow::bail!(
            "No valid listen addresses configured. \
             Addresses must be in multiaddr format, e.g. /ip4/0.0.0.0/tcp/4321 or /ip6/::/tcp/4321. \
             Got: {:?}",
            cfg.listen_addrs
        );
    }

    let peer_id_copy = peer_id;
    let behaviour_cfg = cfg.behaviour;
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )
        .context("building TCP transport")?
        .with_quic()
        // Resolve /dns4 and /dns6 bootstrap addresses so the network can use
        // stable domain names instead of hard-coded IPs that change over time.
        .with_dns()
        .context("building DNS transport")?
        .with_relay_client(libp2p::noise::Config::new, libp2p::yamux::Config::default)
        .context("building relay transport")?
        .with_behaviour(|keypair, relay_client| {
            super::behaviour::RucioBehaviour::new(
                keypair,
                peer_id_copy,
                relay_client,
                behaviour_cfg,
            )
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync + 'static> { e.into() })
        })
        .context("attaching behaviour")?
        // libp2p's SwarmBuilder defaults idle_connection_timeout to ZERO, which
        // tears a connection down the instant it has no active streams. For a
        // DHT/gossipsub node that means peers connect, run one query, and drop
        // immediately — constant churn, an empty routing table, and a bootstrap
        // node that always reports 0 connected peers. Hold idle connections long
        // enough for the mesh and Kademlia to keep them warm and reuse them.
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
        .build();

    let topic_query = IdentTopic::new(TOPIC_SEARCH);
    let topic_result = IdentTopic::new(TOPIC_SEARCH_RESULT);
    if let Some(gossipsub) = swarm.behaviour_mut().gossipsub.as_mut() {
        if let Err(e) = gossipsub.subscribe(&topic_query) {
            warn!("Failed to subscribe to search topic: {e}");
        }
        if let Err(e) = gossipsub.subscribe(&topic_result) {
            warn!("Failed to subscribe to search-result topic: {e}");
        }
    }

    for addr in &listen_addrs {
        if let Err(e) = swarm.listen_on(addr.clone()) {
            warn!("Failed to listen on {addr}: {e}");
        }
    }

    tokio::spawn(run_loop(swarm, peer_id, cmd_rx, EventTx::new(event_tx)));

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
    /// Gossipsub messages that failed with InsufficientPeers, queued for
    /// retry when a peer subscribes to the relevant topic.
    pending_publishes: Vec<(IdentTopic, Vec<u8>, String)>,
    /// Peers whose outgoing connection attempt failed; will be retried after
    /// a short delay to recover from simultaneous-open handshake collisions.
    retry_dials: HashMap<PeerId, (Vec<Multiaddr>, tokio::time::Instant)>,
    /// Set to true once `KadBootstrapPeersReady` is received — signals that
    /// at least one bootstrap peer was configured and we should call
    /// `Kademlia::bootstrap()` on the first successful connection.
    has_bootstrap_peers: bool,
    /// Set to true after we have fired `Kademlia::bootstrap()` at least once
    /// so we don't repeat it on every subsequent connection.
    kad_bootstrapped: bool,
    /// Relay-capable peers discovered via Identify: (peer_id, public listen addrs).
    relay_candidates: Vec<(PeerId, Vec<Multiaddr>)>,
    /// True once `listen_on` for a relay circuit reservation has been issued.
    relay_reserved: bool,
    /// Set after the first `start_providing` failure so we warn about a full
    /// provider store only once instead of for every share.
    provider_store_full_warned: bool,
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
            pending_publishes: Vec::new(),
            retry_dials: HashMap::new(),
            has_bootstrap_peers: false,
            kad_bootstrapped: false,
            relay_candidates: Vec::new(),
            relay_reserved: false,
            provider_store_full_warned: false,
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
    event_tx: EventTx,
) {
    let topic_query = IdentTopic::new(TOPIC_SEARCH);
    let topic_result = IdentTopic::new(TOPIC_SEARCH_RESULT);
    let mut state = LoopState::new();
    let mut dial_retry_tick = tokio::time::interval(tokio::time::Duration::from_millis(500));

    loop {
        tokio::select! {
            _ = dial_retry_tick.tick() => {
                // Retry dial for peers that had a failed outgoing connection
                // more than 1 s ago (simultaneous-open recovery).
                let retry_delay = tokio::time::Duration::from_secs(1);
                let now = tokio::time::Instant::now();
                let to_retry: Vec<(PeerId, Vec<Multiaddr>)> = state
                    .retry_dials
                    .iter()
                    .filter(|(_, (_, ts))| now.duration_since(*ts) >= retry_delay)
                    .map(|(pid, (addrs, _))| (*pid, addrs.clone()))
                    .collect();
                for (pid, addrs) in to_retry {
                    // Remove first so we don't loop on persistent failures.
                    state.retry_dials.remove(&pid);
                    // Skip if already connected.
                    if swarm.is_connected(&pid) {
                        continue;
                    }
                    debug!(%pid, "Retrying dial after connection failure");
                    let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(pid)
                        .addresses(addrs)
                        .build();
                    if let Err(e) = swarm.dial(dial_opts) {
                        debug!(%pid, "Retry dial failed: {e}");
                    }
                }
            }
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
                    Some(NodeCmd::KadBootstrapPeersReady) => {
                        state.has_bootstrap_peers = true;
                        info!("Bootstrap peers ready — will run Kademlia bootstrap on first connection");
                    }
                    Some(NodeCmd::StartProviding(key)) => {
                        let record_key = kad::RecordKey::new(&key);
                        if let Err(e) = swarm.behaviour_mut().kademlia.start_providing(record_key) {
                            // A full provider store would otherwise log once per
                            // share; warn a single time and stay quiet after.
                            if !state.provider_store_full_warned {
                                warn!(
                                    "start_providing error: {e} — further occurrences suppressed"
                                );
                                state.provider_store_full_warned = true;
                            } else {
                                debug!("start_providing error: {e}");
                            }
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
                        publish_json(&mut swarm, &topic_query, &query, "search query", &mut state.pending_publishes);
                    }
                    Some(NodeCmd::PublishSearchResult(result)) => {
                        publish_json(&mut swarm, &topic_result, &result, "search result", &mut state.pending_publishes);
                    }
                    Some(NodeCmd::RequestChunk { peer, request, id_tx }) => {
                        if let Some(transfer) = swarm.behaviour_mut().transfer.as_mut() {
                            let request_id = transfer.send_request(&peer, request);
                            let _ = id_tx.send(request_id);
                        } else {
                            warn!("RequestChunk ignored: transfer protocol disabled");
                        }
                    }
                    Some(NodeCmd::RespondChunk { channel_id, response }) => {
                        if let Some(ch) = state.pending_chunk_channels.remove(&channel_id) {
                            if let Some(transfer) = swarm.behaviour_mut().transfer.as_mut()
                                && let Err(e) = transfer.send_response(ch, response)
                            {
                                warn!("Failed to send chunk response: {e:?}");
                            }
                        } else {
                            warn!(%channel_id, "RespondChunk: unknown channel id");
                        }
                    }
                    Some(NodeCmd::RequestManifest { peer, request, id_tx }) => {
                        if let Some(manifest) = swarm.behaviour_mut().manifest.as_mut() {
                            let request_id = manifest.send_request(&peer, request);
                            let _ = id_tx.send(request_id);
                        } else {
                            warn!("RequestManifest ignored: manifest protocol disabled");
                        }
                    }
                    Some(NodeCmd::RespondManifest { channel_id, response }) => {
                        if let Some(ch) = state.pending_manifest_channels.remove(&channel_id) {
                            if let Some(manifest) = swarm.behaviour_mut().manifest.as_mut()
                                && let Err(e) = manifest.send_response(ch, response)
                            {
                                warn!("Failed to send manifest response: {e:?}");
                            }
                        } else {
                            warn!(%channel_id, "RespondManifest: unknown channel id");
                        }
                    }
                    // WatchDir / UnwatchDir are handled by the WatcherService,
                    // not by the node task — ignore them here.
                    Some(NodeCmd::WatchDir(_)) | Some(NodeCmd::UnwatchDir(_)) => {}
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
    pending: &mut Vec<(IdentTopic, Vec<u8>, String)>,
) {
    match serde_json::to_vec(value) {
        Ok(bytes) => {
            let Some(gossipsub) = swarm.behaviour_mut().gossipsub.as_mut() else {
                return;
            };
            match gossipsub.publish(topic.clone(), bytes.clone()) {
                Ok(_) => debug!("Published {label}"),
                Err(gossipsub::PublishError::NoPeersSubscribedToTopic) => {
                    debug!("No mesh peers yet for {label} — queued for retry");
                    // Bound the retry backlog: on a node with no mesh peers yet
                    // this would otherwise grow without limit. Drop the oldest.
                    if pending.len() >= MAX_PENDING_PUBLISHES {
                        pending.remove(0);
                        debug!("Pending-publish queue full — dropped oldest entry");
                    }
                    pending.push((topic.clone(), bytes, label.to_string()));
                }
                Err(e) => warn!("Could not publish {label}: {e}"),
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
    event_tx: &EventTx,
    state: &mut LoopState,
    peer_id: PeerId,
    swarm: &mut libp2p::Swarm<RucioBehaviour>,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!(%address, "Listening");
            state.confirmed_addrs.insert(address.clone());
            if !state.ready_sent {
                state.ready_sent = true;
                let _ = event_tx
                    .emit(NodeEvent::Ready {
                        peer_id,
                        listen_addrs: state.confirmed_addrs.iter().cloned().collect(),
                    })
                    .await;
            } else {
                let _ = event_tx.emit(NodeEvent::ListenAddrAdded(address)).await;
            }
        }
        SwarmEvent::ListenerClosed {
            addresses, reason, ..
        } => {
            warn!(?addresses, ?reason, "Listener closed");
            for a in &addresses {
                state.confirmed_addrs.remove(a);
                let _ = event_tx.emit(NodeEvent::ListenAddrRemoved(a.clone())).await;
            }
        }
        SwarmEvent::ConnectionEstablished {
            peer_id: pid,
            num_established,
            ..
        } => {
            debug!(%pid, "Connection established");
            if let Some(gossipsub) = swarm.behaviour_mut().gossipsub.as_mut() {
                gossipsub.add_explicit_peer(&pid);
            }
            // Connection succeeded — no need to retry.
            state.retry_dials.remove(&pid);
            // If bootstrap peers were configured and we haven't bootstrapped
            // yet, fire Kademlia::bootstrap() now that we have at least one
            // live connection.
            if state.has_bootstrap_peers && !state.kad_bootstrapped {
                match swarm.behaviour_mut().kademlia.bootstrap() {
                    Ok(qid) => {
                        info!(?qid, "Kademlia bootstrap started");
                        state.kad_bootstrapped = true;
                    }
                    // NoKnownPeers is expected on the first ConnectionEstablished:
                    // identify hasn't run yet so the routing table is still empty.
                    // kad_bootstrapped stays false and we retry on the next connection.
                    Err(e) => debug!("Kademlia bootstrap deferred: {e} (identify pending)"),
                }
            }
            // Count unique peers, not connections: a peer reached over both
            // IPv4 and IPv6 opens two connections but is one peer. Report the
            // connect only for the first connection to this peer.
            if num_established.get() == 1 {
                let _ = event_tx
                    .emit(NodeEvent::PeerConnected { peer_id: pid })
                    .await;
            }
        }
        SwarmEvent::ConnectionClosed {
            peer_id: pid,
            cause,
            num_established,
            ..
        } => {
            debug!(%pid, ?cause, "Connection closed");
            // Only when the peer's last connection is gone: drop it from the
            // gossipsub mesh and report the disconnect (mirrors the unique-peer
            // count, so closing a redundant duplicate connection isn't a churn).
            if num_established == 0 {
                if let Some(gossipsub) = swarm.behaviour_mut().gossipsub.as_mut() {
                    gossipsub.remove_explicit_peer(&pid);
                }
                let _ = event_tx
                    .emit(NodeEvent::PeerDisconnected { peer_id: pid })
                    .await;
            }
        }
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            match classify_dial_error(&error) {
                // Expected, non-actionable dial failures: the peer advertised
                // non-routable addresses (LAN/loopback/link-local), or it isn't
                // reachable where it advertised (connection refused, timed out,
                // host unreachable, reset) — routine churn in a P2P swarm.
                DialNoise::Expected => {
                    debug!(error = %dial_error_line(&error), "Outgoing connection error (expected: peer unreachable)")
                }
                // ENETUNREACH on a public address: the OS has no route for
                // that address family (typically IPv6 not configured). Logged
                // at INFO so a legitimate routing change stays visible without
                // producing alarm-level noise on hosts without IPv6.
                DialNoise::Unreachable => {
                    info!(error = %dial_error_line(&error), "Outgoing connection error (network unreachable — expected if this address family is not available on this host)")
                }
                DialNoise::Real => {
                    warn!(error = %dial_error_line(&error), "Outgoing connection error")
                }
            }
            // Schedule a retry for known peers so that simultaneous-open
            // handshake collisions (both nodes dial each other at the same
            // instant) are recovered from automatically.
            if let Some(Some(entry)) = peer_id.map(|pid| state.retry_dials.get_mut(&pid)) {
                // Refresh the timestamp so the retry fires ~1 s from now.
                entry.1 = tokio::time::Instant::now();
            }
        }

        // AutoNAT confirmed (a peer dialled us back successfully) or expired one
        // of our external addresses — the authoritative HighId/LowId signal.
        SwarmEvent::ExternalAddrConfirmed { address } => {
            let listen_vec: Vec<Multiaddr> = state.confirmed_addrs.iter().cloned().collect();
            match state
                .classifier
                .record_confirmed_external(address.clone(), true, &listen_vec)
            {
                Some(new_class) => {
                    info!(%address, ?new_class, "External address confirmed (AutoNAT) — node class updated");
                    let _ = event_tx.emit(NodeEvent::ClassChanged(new_class)).await;
                }
                None => debug!(%address, "External address confirmed (AutoNAT)"),
            }
        }
        SwarmEvent::ExternalAddrExpired { address } => {
            let listen_vec: Vec<Multiaddr> = state.confirmed_addrs.iter().cloned().collect();
            if let Some(new_class) =
                state
                    .classifier
                    .record_confirmed_external(address.clone(), false, &listen_vec)
            {
                info!(%address, ?new_class, "External address expired (AutoNAT) — node class updated");
                // Lost HighId: fall back to a relay reservation if one is available.
                if matches!(new_class, NodeClass::LowId)
                    && !state.relay_reserved
                    && !state.relay_candidates.is_empty()
                {
                    try_relay_reservation(
                        swarm,
                        &state.relay_candidates,
                        &mut state.relay_reserved,
                    );
                }
                let _ = event_tx.emit(NodeEvent::ClassChanged(new_class)).await;
            }
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
                            // Store addresses for potential retry after a
                            // failed handshake (simultaneous-open collision).
                            state
                                .retry_dials
                                .entry(pid)
                                .or_insert_with(|| {
                                    (
                                        addrs.clone(),
                                        tokio::time::Instant::now()
                                            - tokio::time::Duration::from_secs(60),
                                    )
                                })
                                .0 = addrs.clone();
                            // Only dial if we are not already connected or
                            // dialling this peer — avoids simultaneous-open
                            // handshake collisions when both nodes discover
                            // each other at the same instant via mDNS.
                            let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(pid)
                                .condition(libp2p::swarm::dial_opts::PeerCondition::NotDialing)
                                .addresses(addrs.clone())
                                .build();
                            if let Err(e) = swarm.dial(dial_opts) {
                                debug!(%pid, "mDNS dial skipped or failed: {e}");
                            }
                            let _ = event_tx
                                .emit(NodeEvent::PeerDiscovered {
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
                                    event_tx.emit(NodeEvent::PeerExpired { peer_id: pid }).await;
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
                                .emit(NodeEvent::ProvidersFound {
                                    key: key.clone(),
                                    providers: providers.into_iter().collect(),
                                })
                                .await;
                        }
                    }
                    Event::RoutingUpdated { peer, .. } => {
                        debug!(%peer, "Kademlia routing table updated");
                    }
                    Event::InboundRequest {
                        request:
                            kad::InboundRequest::AddProvider {
                                record: Some(record),
                            },
                    } => {
                        use kad::store::RecordStore;
                        let key = record.key.to_vec();
                        let provider = record.provider;
                        let addresses = record.addresses.clone();
                        // Teach the routing table how to reach the announcer so
                        // it can be dialed later (e.g. to fetch its manifest for
                        // enrichment).
                        for addr in &addresses {
                            swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&provider, addr.clone());
                        }
                        // FilterBoth does not auto-store the record; re-store it
                        // so we keep serving it like a normal DHT server.
                        if let Err(e) = swarm
                            .behaviour_mut()
                            .kademlia
                            .store_mut()
                            .add_provider(record)
                        {
                            debug!(?e, "Could not store captured provider record");
                        }
                        let _ = event_tx
                            .emit(NodeEvent::ProviderRecord {
                                key,
                                provider,
                                addresses,
                            })
                            .await;
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

                        // Always feed the classifier — it needs all observations
                        // (including ephemeral NAT ports) to determine HighId/LowId.
                        if let Some(new_class) =
                            state
                                .classifier
                                .record_observation(observed.clone(), pid, &listen_vec)
                        {
                            info!(?new_class, "Node class determined");
                            // LowId nodes use a relay reservation so they become reachable.
                            if matches!(new_class, NodeClass::LowId)
                                && !state.relay_reserved
                                && !state.relay_candidates.is_empty()
                            {
                                try_relay_reservation(
                                    swarm,
                                    &state.relay_candidates,
                                    &mut state.relay_reserved,
                                );
                            }
                            let _ = event_tx.emit(NodeEvent::ClassChanged(new_class)).await;
                        }

                        // Only surface addresses that are reachable from the internet
                        // on one of our listen ports. Ephemeral source ports from
                        // outgoing connections are echoed back by identify but are
                        // not stable inbound addresses.
                        if is_stable_external_addr(&observed, &listen_vec) {
                            let _ = event_tx
                                .emit(NodeEvent::ObservedAddr {
                                    addr: observed,
                                    reported_by: pid,
                                })
                                .await;
                        }

                        // Add the peer's routable listen addresses to the
                        // Kademlia routing table. Skip loopback and link-local
                        // addresses: they refer to the *remote* peer's own
                        // localhost and would hit our local daemon if dialled,
                        // producing spurious WrongPeerId errors. Private LAN
                        // addresses (192.168.x.x, 10.x.x.x) are kept because
                        // they are valid within the local network.
                        for addr in &info.listen_addrs {
                            if !addr_is_loopback_or_link_local(addr) {
                                swarm
                                    .behaviour_mut()
                                    .kademlia
                                    .add_address(&pid, addr.clone());
                            }
                        }

                        // Detect relay-capable peers before listen_addrs is consumed.
                        let relay_addrs: Vec<Multiaddr> = if info
                            .protocols
                            .iter()
                            .any(|p| p.as_ref() == RELAY_HOP_PROTOCOL)
                        {
                            info.listen_addrs
                                .iter()
                                .filter(|a| !addr_is_loopback_or_link_local(a))
                                .cloned()
                                .collect()
                        } else {
                            vec![]
                        };

                        // Persist the peer with its addresses so that
                        // `rucio peers` shows multiaddrs for all connected
                        // peers, not just those found via mDNS.
                        let _ = event_tx
                            .emit(NodeEvent::PeerDiscovered {
                                peer_id: pid,
                                addrs: info.listen_addrs,
                            })
                            .await;

                        if !relay_addrs.is_empty() {
                            debug!(%pid, "Peer supports relay hop");
                            state.relay_candidates.push((pid, relay_addrs));
                            if matches!(state.classifier.current(), NodeClass::LowId)
                                && !state.relay_reserved
                            {
                                try_relay_reservation(
                                    swarm,
                                    &state.relay_candidates,
                                    &mut state.relay_reserved,
                                );
                            }
                        }
                    }
                    Event::Sent { .. } | Event::Error { .. } => {}
                    _ => {}
                }
            }

            RucioBehaviourEvent::Gossipsub(gs_event) => {
                let subscribed = matches!(gs_event, gossipsub::Event::Subscribed { .. });
                on_gossipsub_event(gs_event, event_tx).await;
                // When a new peer joins a topic, retry any queued publishes
                // that previously failed with InsufficientPeers.
                if subscribed && !state.pending_publishes.is_empty() {
                    let pending = std::mem::take(&mut state.pending_publishes);
                    debug!(
                        "Retrying {} queued publish(es) after peer subscription",
                        pending.len()
                    );
                    for (topic, bytes, label) in pending {
                        let Some(gossipsub) = swarm.behaviour_mut().gossipsub.as_mut() else {
                            break;
                        };
                        match gossipsub.publish(topic.clone(), bytes.clone()) {
                            Ok(_) => debug!("Retry published {label}"),
                            Err(gossipsub::PublishError::NoPeersSubscribedToTopic) => {
                                state.pending_publishes.push((topic, bytes, label));
                            }
                            Err(e) => warn!("Retry publish {label} failed: {e}"),
                        }
                    }
                }
            }

            RucioBehaviourEvent::Transfer(tr_event) => {
                on_transfer_event(tr_event, event_tx, state).await;
            }

            RucioBehaviourEvent::Manifest(mn_event) => {
                on_manifest_event(mn_event, event_tx, state).await;
            }

            RucioBehaviourEvent::Relay(relay_event) => {
                use relay::Event;
                match relay_event {
                    Event::ReservationReqAccepted {
                        src_peer_id,
                        renewed,
                    } => {
                        debug!(%src_peer_id, %renewed, "Relay: accepted reservation from peer");
                    }
                    Event::CircuitReqAccepted {
                        src_peer_id,
                        dst_peer_id,
                    } => {
                        debug!(%src_peer_id, %dst_peer_id, "Relay: circuit established");
                    }
                    _ => {}
                }
            }

            RucioBehaviourEvent::RelayClient(relay_client_event) => {
                use relay::client::Event;
                match relay_client_event {
                    Event::ReservationReqAccepted { relay_peer_id, .. } => {
                        info!(%relay_peer_id, "Relay reservation established");
                        state.relay_reserved = true;
                    }
                    Event::OutboundCircuitEstablished { relay_peer_id, .. } => {
                        debug!(%relay_peer_id, "Outbound circuit via relay established");
                    }
                    Event::InboundCircuitEstablished { src_peer_id, .. } => {
                        debug!(%src_peer_id, "Inbound circuit via relay established");
                    }
                }
            }

            RucioBehaviourEvent::Dcutr(dcutr_event) => {
                if dcutr_event.result.is_ok() {
                    info!(peer = %dcutr_event.remote_peer_id, "DCUtR hole punch succeeded — direct connection established");
                } else {
                    debug!(peer = %dcutr_event.remote_peer_id, "DCUtR hole punch failed — relay connection maintained");
                }
            }
            // AutoNAT v2 client: result of probing one of our external-address
            // candidates. The address confirmation itself arrives separately as
            // SwarmEvent::ExternalAddrConfirmed; this is just observability.
            RucioBehaviourEvent::AutonatClient(ev) => {
                if let Err(e) = &ev.result {
                    debug!(addr = %ev.tested_addr, server = %ev.server, error = %e, "AutoNAT reachability probe failed");
                } else {
                    debug!(addr = %ev.tested_addr, server = %ev.server, "AutoNAT reachability probe succeeded");
                }
            }
            // AutoNAT v2 server: we dial-tested a peer's address on its behalf.
            RucioBehaviourEvent::AutonatServer(ev) => {
                debug!(client = %ev.client, addr = %ev.tested_addr, ok = ev.result.is_ok(), "Served an AutoNAT probe for a peer");
            }
            // connection_limits emits no events of its own.
            RucioBehaviourEvent::ConnectionLimits(_) => {}
        },

        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Gossipsub handler
// ---------------------------------------------------------------------------

async fn on_gossipsub_event(event: gossipsub::Event, event_tx: &EventTx) {
    match event {
        gossipsub::Event::Message { message, .. } => {
            let topic_str = message.topic.as_str();

            if topic_str == TOPIC_SEARCH {
                match serde_json::from_slice::<SearchQuery>(&message.data) {
                    Ok(query) => {
                        debug!(id = %query.id, keywords = ?query.keywords, "Received search query");
                        let _ = event_tx.emit(NodeEvent::SearchQueryReceived(query)).await;
                    }
                    Err(e) => warn!("Failed to decode search query: {e}"),
                }
            } else if topic_str == TOPIC_SEARCH_RESULT {
                match serde_json::from_slice::<SearchResult>(&message.data) {
                    Ok(result) => {
                        debug!(qid = %result.query_id, "Received search result from {}", result.provider);
                        let _ = event_tx.emit(NodeEvent::SearchResult(result)).await;
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
    event_tx: &EventTx,
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
                .emit(NodeEvent::ChunkReceived {
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
            let delivered = event_tx
                .emit(NodeEvent::ChunkRequested {
                    peer,
                    request,
                    channel_id,
                })
                .await;
            if !delivered {
                // Event dropped under overload — drop the response channel too
                // so it doesn't linger forever (the peer's request expires).
                state.pending_chunk_channels.remove(&channel_id);
            }
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
    event_tx: &EventTx,
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
                .emit(NodeEvent::ManifestReceived {
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
            let delivered = event_tx
                .emit(NodeEvent::ManifestRequested {
                    peer,
                    request,
                    channel_id,
                })
                .await;
            if !delivered {
                // Event dropped under overload — drop the response channel too
                // so it doesn't linger forever (the peer's request expires).
                state.pending_manifest_channels.remove(&channel_id);
            }
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

// ---------------------------------------------------------------------------
// Relay reservation
// ---------------------------------------------------------------------------

/// Pick the first relay candidate with a public address and issue a
/// `listen_on` for a `/p2p-circuit` address.  The relay client behaviour
/// turns that into a reservation request; on success it emits
/// `RelayClient::ReservationReqAccepted` and the swarm starts advertising
/// the circuit address.
fn try_relay_reservation(
    swarm: &mut libp2p::Swarm<RucioBehaviour>,
    candidates: &[(PeerId, Vec<Multiaddr>)],
    reserved: &mut bool,
) {
    if *reserved {
        return;
    }
    for (relay_peer, addrs) in candidates {
        if let Some(relay_addr) = addrs.iter().find(|a| !addr_is_private_or_loopback(a)) {
            let circuit_addr = relay_addr
                .clone()
                .with(Protocol::P2p(*relay_peer))
                .with(Protocol::P2pCircuit);
            match swarm.listen_on(circuit_addr.clone()) {
                Ok(_) => {
                    info!(%circuit_addr, "Relay reservation initiated (LowId node)");
                    *reserved = true;
                    return;
                }
                Err(e) => warn!(%circuit_addr, "Failed to initiate relay reservation: {e}"),
            }
        }
    }
}

enum DialNoise {
    /// Failure is expected and not actionable: the peer advertised non-routable
    /// addresses (private/link-local/loopback), or it simply isn't reachable at
    /// the addresses it advertised (connection refused, timed out, host
    /// unreachable, reset) — routine for peers behind NAT, with a closed port,
    /// or not listening yet. → DEBUG
    Expected,
    /// All failed public addresses returned ENETUNREACH: the OS has no route for
    /// that address family. Typically means IPv6 is not configured on this
    /// host. → INFO so a legitimate routing change stays visible without
    /// producing WARN noise.
    Unreachable,
    /// Any other failure — genuinely unexpected. → WARN
    Real,
}

/// Render a `DialError` as a single log line.
///
/// libp2p folds several failed dial attempts into one error whose `Display`
/// ("Multiple dial errors occurred: …") spans multiple lines separated by blank
/// lines and ` - ` bullets. That breaks one-event-per-line log parsing, so
/// collapse every run of whitespace (the embedded newlines included) into a
/// single space.
fn dial_error_line(error: &DialError) -> String {
    error
        .to_string()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Classify a `DialError` for log-level selection.
///
/// * `WrongPeerId` on a private/loopback address → `Expected` (DEBUG).
///   This happens when identify advertises the remote node's own loopback
///   addresses and Kad later tries to dial them, hitting our local daemon.
///
/// * `Transport` failures are checked per `(addr, err)` pair:
///   - private/loopback/link-local address → skipped (contributes to `Expected`)
///   - public address + `ENETUNREACH` → contributes to `Unreachable`
///   - public address + a peer-reachability error (connection refused, reset,
///     timed out, host unreachable) → contributes to `Expected`
///   - anything else → `Real` (short-circuits immediately)
///
/// The returned level is the "worst" across all pairs:
/// `Expected < Unreachable < Real`.
fn classify_dial_error(error: &DialError) -> DialNoise {
    use libp2p::core::transport::TransportError;

    match error {
        // identify propagates every listen address of the remote, including
        // its own loopback. Dialling 127.0.0.1:4321 on our host hits our
        // local daemon with a different peer ID — expected, not actionable.
        DialError::WrongPeerId { address, .. } => {
            if addr_is_private_or_loopback(address) {
                DialNoise::Expected
            } else {
                DialNoise::Real
            }
        }
        DialError::Transport(addrs) if !addrs.is_empty() => {
            let mut has_unreachable = false;
            for (addr, err) in addrs {
                if addr_is_private_or_loopback(addr) {
                    continue;
                }
                // Non-transport errors (e.g. MultiaddrNotSupported) point at an
                // addressing/config problem — surface them.
                let TransportError::Other(e) = err else {
                    return DialNoise::Real;
                };
                match classify_transport_io(e) {
                    DialNoise::Unreachable => has_unreachable = true,
                    DialNoise::Expected => {}
                    DialNoise::Real => return DialNoise::Real,
                }
            }
            if has_unreachable {
                DialNoise::Unreachable
            } else {
                DialNoise::Expected
            }
        }
        _ => DialNoise::Real,
    }
}

/// Classify the `io::Error` behind a single transport dial attempt.
///
/// libp2p's combined transport (DNS / relay / Or-transport) can fold several
/// attempts into one error that surfaces as [`io::ErrorKind::Other`] with a
/// `"Multiple dial errors occurred: …"` message, so a `kind()` check alone
/// misses the routine reachability failures nested inside (this is why dialling
/// a peer that just went away used to log at WARN). Fall back to scanning the
/// rendered error for known-benign markers in that case.
fn classify_transport_io(e: &std::io::Error) -> DialNoise {
    use std::io::ErrorKind;
    match e.kind() {
        // ENETUNREACH: this host has no route for the address family (e.g. no
        // IPv6). Worth surfacing at INFO.
        ErrorKind::NetworkUnreachable => DialNoise::Unreachable,
        // The peer advertised a public address but isn't reachable there:
        // behind NAT, port closed, or not listening yet. Routine churn.
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::TimedOut
        | ErrorKind::HostUnreachable
        | ErrorKind::BrokenPipe => DialNoise::Expected,
        // Aggregated / wrapped errors arrive as Other; peek at the text.
        _ if dial_text_is_benign(&e.to_string()) => DialNoise::Expected,
        // Genuinely unexpected (protocol/negotiation bug, config error, …).
        _ => DialNoise::Real,
    }
}

/// Heuristic for nested/aggregated dial errors (`ErrorKind::Other`): `true` when
/// the rendered chain only mentions routine, non-actionable failures — a peer
/// that is down, advertised an unroutable address, or hit a transient
/// handshake/negotiation error. These are expected churn as peers come and go.
fn dial_text_is_benign(text: &str) -> bool {
    const BENIGN: &[&str] = &[
        "Connection refused",
        "Connection reset",
        "Broken pipe",
        "timed out",
        "Timed out",
        "unreachable", // network/host unreachable, "No route to host"
        "No route to host",
        "Invalid argument", // e.g. link-local address without a scope id
        "Handshake failed", // transient or loopback handshake noise
        "Failed to negotiate transport",
    ];
    BENIGN.iter().any(|marker| text.contains(marker))
}

/// Return `true` if the IP component of `addr` is loopback or link-local —
/// addresses that only make sense on the *originating* host and must never be
/// stored in the routing table as addresses for a *remote* peer.
///
/// Private LAN ranges (192.168.x.x, 10.x.x.x, 172.16-31.x.x) are **not**
/// excluded here because they are valid targets on a local-area network.
fn addr_is_loopback_or_link_local(addr: &Multiaddr) -> bool {
    addr.iter().any(|p| match p {
        Protocol::Ip4(ip) => ip.is_loopback() || ip.is_link_local() || ip.is_unspecified(),
        Protocol::Ip6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                // fe80::/10 — link local
                || (ip.segments()[0] & 0xffc0) == 0xfe80
        }
        _ => false,
    })
}

/// Return `true` if every IP component of `addr` is private, loopback, or
/// link-local — i.e. not routable from the public internet.
fn addr_is_private_or_loopback(addr: &Multiaddr) -> bool {
    addr.iter().any(|p| match p {
        Protocol::Ip4(ip) => ip.is_private() || ip.is_loopback() || ip.is_link_local(),
        Protocol::Ip6(ip) => {
            ip.is_loopback()
                || (ip.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
                || (ip.segments()[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
        }
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    // The exact shape that used to log at WARN when a peer went away: a public
    // address whose transport error is a nested "Multiple dial errors" aggregate
    // (ErrorKind::Other) wrapping routine reachability failures.
    #[test]
    fn aggregated_reachability_failure_is_expected() {
        let e = Error::other(
            "Multiple dial errors occurred:\n - Connection refused (os error 111): \
             Connection refused (os error 111)",
        );
        assert!(matches!(classify_transport_io(&e), DialNoise::Expected));
    }

    #[test]
    fn direct_kinds_are_classified() {
        assert!(matches!(
            classify_transport_io(&Error::from(ErrorKind::ConnectionRefused)),
            DialNoise::Expected
        ));
        assert!(matches!(
            classify_transport_io(&Error::from(ErrorKind::NetworkUnreachable)),
            DialNoise::Unreachable
        ));
    }

    #[test]
    fn link_local_invalid_argument_is_benign() {
        let e = Error::other("Invalid argument (os error 22)");
        assert!(matches!(classify_transport_io(&e), DialNoise::Expected));
    }

    #[test]
    fn genuinely_unexpected_error_stays_real() {
        let e = Error::other("unsupported protocol /rucio/kad/9.9.9");
        assert!(matches!(classify_transport_io(&e), DialNoise::Real));
    }
}
