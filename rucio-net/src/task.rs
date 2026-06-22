//! The node task: owns the libp2p swarm and drives it to completion.

use anyhow::{Context, Result};
use libp2p::futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder,
    core::transport::ListenerId,
    gossipsub::{self, IdentTopic},
    kad::{self, QueryId},
    multiaddr::Protocol,
    relay,
    request_response::{self, ResponseChannel},
    swarm::{DialError, SwarmEvent},
};
use rucio_core::protocol::{
    have::{HaveRequest, HaveResponse},
    manifest::{ManifestRequest, ManifestResponse},
    node::{NodeClass, Reachability},
    pinset::{PinsetRequest, PinsetResponse},
    search::{SearchQuery, SearchResult},
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, info, warn};

use crate::NetConfig;

use super::{
    behaviour::{
        AUTONAT_DIAL_REQUEST_PROTOCOL, RELAY_HOP_PROTOCOL, RucioBehaviour, RucioBehaviourEvent,
        TOPIC_SEARCH, TOPIC_SEARCH_RESULT,
    },
    classify::{ClassificationState, is_stable_external_addr},
    identity,
    messages::{NodeCmd, NodeEvent},
    transfer_codec::{ChunkReq, ChunkResp},
};

const CMD_BUFFER: usize = 64;
const EVENT_BUFFER: usize = 1024;
/// Cap on the gossipsub retry backlog held while no mesh peer is subscribed.
/// Without a bound, a node that searches before any peer joins would grow this
/// queue indefinitely; oldest entries are dropped past this many.
const MAX_PENDING_PUBLISHES: usize = 256;

/// Provider announcements are drained from `announce_queue` under an in-flight
/// concurrency cap rather than fired all at once. Each Kademlia provide query
/// carries ~10 KB of state; announcing thousands of shares simultaneously grows
/// the QueryPool's hash table to tens of MB which — like any Rust HashMap — never
/// shrinks back, pinning RSS for the life of the process. Capping how many run at
/// once keeps that table small (256 ≈ a couple of MB; raise only for >100k-file
/// libraries). The drain refills up to the cap on each tick as queries complete
/// and free their slots (via the StartProviding result event).
const MAX_PROVIDE_INFLIGHT: usize = 256;
const ANNOUNCE_INTERVAL: Duration = Duration::from_millis(250);
/// How often we re-announce every provided key to refresh its DHT record before
/// it expires. libp2p's own re-publication is disabled (it bursts every key at
/// once); we drive it here so it flows through the same concurrency cap. Kept
/// well under the 48 h provider-record TTL.
const REPROVIDE_INTERVAL: Duration = Duration::from_secs(12 * 60 * 60);

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

pub async fn spawn(
    cfg: &NetConfig,
    upload_limiter: Option<crate::codec_utils::ByteLimiter>,
    download_progress: Option<crate::codec_utils::ReadProgress>,
) -> Result<NodeHandle> {
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
    // Keep a clone of the keypair to sign our DHT peer-address record (the
    // builder consumes the original).
    let sign_keypair = keypair.clone();
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
                upload_limiter,
                download_progress,
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

    tokio::spawn(run_loop(
        swarm,
        peer_id,
        sign_keypair,
        cmd_rx,
        EventTx::new(event_tx),
        behaviour_cfg.kad_max_records,
    ));

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
    pending_chunk_channels: HashMap<u64, (ResponseChannel<ChunkResp>, PeerId)>,
    /// Pending inbound manifest request channels keyed by a monotonic id.
    pending_manifest_channels: HashMap<u64, ResponseChannel<ManifestResponse>>,
    /// Pending inbound pin-set request channels keyed by a monotonic id.
    pending_pinset_channels: HashMap<u64, ResponseChannel<PinsetResponse>>,
    /// Pending inbound availability request channels keyed by a monotonic id.
    pending_have_channels: HashMap<u64, ResponseChannel<HaveResponse>>,
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
    /// `ListenerId` of the active relay-circuit reservation (a `/p2p-circuit`
    /// `listen_on`), or `None` when we hold none. Tracked both to avoid
    /// duplicate reservations and to cancel it once we reach HighId.
    relay_listener: Option<ListenerId>,
    /// Multiaddrs of our bootstrap peers (which run the AutoNAT v2 server).
    /// Re-dialled on demand so the AutoNAT client always has a server to ask
    /// when an external-address candidate needs verifying.
    bootstrap_addrs: Vec<Multiaddr>,
    /// Connected peers that advertise the AutoNAT v2 server protocol. While this
    /// is empty and we are not yet HighId, a new candidate triggers a dial to our
    /// bootstrap peers; once a server is connected we leave it to the client.
    autonat_servers: HashSet<PeerId>,
    /// Last AutoNAT reachability state emitted, to dedupe `ReachabilityChanged`.
    last_reachability: Option<Reachability>,
    /// Set after the first `start_providing` failure so we warn about a full
    /// provider store only once instead of for every share.
    provider_store_full_warned: bool,
    /// Keys (root hashes) the daemon wants to provide. We only actually announce
    /// them while `providing` is true (i.e. while HighId) — see
    /// [`reconcile_provider_announcements`].
    wanted_providers: HashSet<Vec<u8>>,
    /// Whether we are currently announcing `wanted_providers` to the DHT. Tracks
    /// HighId reachability so we never advertise content a peer could only pull
    /// through a relay.
    providing: bool,
    /// Keys waiting to be announced to the DHT, drained under a concurrency cap
    /// (see [`MAX_PROVIDE_INFLIGHT`]) so we never spawn thousands of provider
    /// queries at once. Fed by StartProviding, reachability changes and the
    /// periodic re-provide.
    announce_queue: VecDeque<Vec<u8>>,
    /// QueryIds of provider announcements currently in flight, bounding how many
    /// Kademlia provide queries run concurrently (keeps the QueryPool small).
    provide_inflight: HashSet<QueryId>,
    /// Provider announcements actually issued in the current drain cycle. Logged
    /// and reset once the queue and the in-flight set are both empty, so the log
    /// reports announcements that completed, not merely got queued.
    announce_emitted: usize,
    /// Foreign provider records we re-store as a DHT server, kept under a
    /// second-chance (CLOCK) eviction policy so the in-RAM store stays bounded
    /// without dropping records that are still being refreshed. libp2p caps
    /// these together with our own announced keys, so we bound them here
    /// instead; our own announcements go through `start_providing` and never
    /// enter this structure, so they are never evicted.
    ///
    /// `foreign_providers` is the CLOCK queue (insertion / second-chance order);
    /// `foreign_provider_ref` maps each held pair to its "referenced since the
    /// last sweep" bit, set on refresh so the sweep spares it.
    foreign_providers: VecDeque<(Vec<u8>, PeerId)>,
    foreign_provider_ref: HashMap<(Vec<u8>, PeerId), bool>,
    /// Cap on the number of foreign provider records (from `kad_max_records`).
    max_foreign_providers: usize,
}

impl LoopState {
    fn new(max_foreign_providers: usize) -> Self {
        Self {
            confirmed_addrs: HashSet::new(),
            ready_sent: false,
            provider_queries: HashMap::new(),
            classifier: ClassificationState::default(),
            pending_chunk_channels: HashMap::new(),
            pending_manifest_channels: HashMap::new(),
            pending_pinset_channels: HashMap::new(),
            pending_have_channels: HashMap::new(),
            next_channel_id: 0,
            pending_publishes: Vec::new(),
            retry_dials: HashMap::new(),
            has_bootstrap_peers: false,
            kad_bootstrapped: false,
            relay_candidates: Vec::new(),
            relay_listener: None,
            bootstrap_addrs: Vec::new(),
            autonat_servers: HashSet::new(),
            last_reachability: None,
            provider_store_full_warned: false,
            wanted_providers: HashSet::new(),
            providing: false,
            announce_queue: VecDeque::new(),
            provide_inflight: HashSet::new(),
            announce_emitted: 0,
            foreign_providers: VecDeque::new(),
            foreign_provider_ref: HashMap::new(),
            max_foreign_providers,
        }
    }

    /// Record a foreign provider record we just re-stored as a DHT server, and
    /// return the `(key, provider)` pairs that must be evicted from the store to
    /// stay within `max_foreign_providers`.
    ///
    /// A record we already hold (a refresh) is not re-enqueued; instead its
    /// "referenced" bit is set so the next eviction sweep spares it. Eviction is
    /// a second-chance (CLOCK) sweep: a referenced entry has its bit cleared and
    /// is moved to the back (another lap); the first unreferenced entry is
    /// evicted. This keeps actively-refreshed records resident while still
    /// bounding the total — degrading to plain FIFO only when every record is
    /// equally hot. The sweep is bounded: after at most one full rotation every
    /// bit is clear, so an eviction always terminates.
    fn note_foreign_provider(&mut self, key: Vec<u8>, provider: PeerId) -> Vec<(Vec<u8>, PeerId)> {
        let pair = (key, provider);
        if let Some(referenced) = self.foreign_provider_ref.get_mut(&pair) {
            *referenced = true;
            return Vec::new();
        }
        self.foreign_provider_ref.insert(pair.clone(), false);
        self.foreign_providers.push_back(pair);

        let mut evicted = Vec::new();
        while self.foreign_providers.len() > self.max_foreign_providers {
            let Some(candidate) = self.foreign_providers.pop_front() else {
                break;
            };
            match self.foreign_provider_ref.get_mut(&candidate) {
                Some(referenced) if *referenced => {
                    *referenced = false;
                    self.foreign_providers.push_back(candidate);
                }
                _ => {
                    self.foreign_provider_ref.remove(&candidate);
                    evicted.push(candidate);
                }
            }
        }
        evicted
    }

    fn store_chunk_channel(&mut self, ch: ResponseChannel<ChunkResp>, peer: PeerId) -> u64 {
        let id = self.next_channel_id;
        self.next_channel_id += 1;
        self.pending_chunk_channels.insert(id, (ch, peer));
        id
    }

    fn store_manifest_channel(&mut self, ch: ResponseChannel<ManifestResponse>) -> u64 {
        let id = self.next_channel_id;
        self.next_channel_id += 1;
        self.pending_manifest_channels.insert(id, ch);
        id
    }

    fn store_have_channel(&mut self, ch: ResponseChannel<HaveResponse>) -> u64 {
        let id = self.next_channel_id;
        self.next_channel_id += 1;
        self.pending_have_channels.insert(id, ch);
        id
    }

    fn store_pinset_channel(&mut self, ch: ResponseChannel<PinsetResponse>) -> u64 {
        let id = self.next_channel_id;
        self.next_channel_id += 1;
        self.pending_pinset_channels.insert(id, ch);
        id
    }
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

/// Publish our signed peer-address record into the DHT, keyed by our PeerId, so
/// a peer that only knows our PeerId can resolve our current addresses. No-op
/// until we actually have a dialable address. The record is signed with our
/// identity key, so resolvers can trust the addresses really belong to us.
fn publish_peer_record(
    swarm: &mut libp2p::Swarm<RucioBehaviour>,
    keypair: &libp2p::identity::Keypair,
    peer_id: &PeerId,
) {
    // Confirmed external addresses first (reachable across the internet), then
    // our listen addresses (useful on the LAN); drop loopback.
    let mut addrs: Vec<Multiaddr> = swarm.external_addresses().cloned().collect();
    for a in swarm.listeners() {
        if !addrs.contains(a) {
            addrs.push(a.clone());
        }
    }
    addrs.retain(|a| !is_loopback(a));
    if addrs.is_empty() {
        debug!("PublishPeerRecord: no dialable address yet — skipping");
        return;
    }
    let record = match libp2p::core::PeerRecord::new(keypair, addrs) {
        Ok(r) => r,
        Err(e) => {
            warn!("Could not sign peer-address record: {e}");
            return;
        }
    };
    let value = record.into_signed_envelope().into_protobuf_encoding();
    let key = kad::RecordKey::new(&peer_id.to_bytes());
    match swarm
        .behaviour_mut()
        .kademlia
        .put_record(kad::Record::new(key, value), kad::Quorum::One)
    {
        Ok(_) => debug!("Published peer-address record to the DHT"),
        Err(e) => debug!("put_record (peer-address) failed: {e}"),
    }
}

/// Verify a signed peer-address record (from a `get_record` result) and add its
/// addresses to the routing table so `send_request` can dial that peer.
/// `from_signed_envelope` checks the signature and binds the addresses to the
/// signer's PeerId, so a forged record under someone else's key is rejected.
fn add_resolved_peer_addresses(swarm: &mut libp2p::Swarm<RucioBehaviour>, value: &[u8]) {
    let Ok(envelope) = libp2p::core::SignedEnvelope::from_protobuf_encoding(value) else {
        return;
    };
    let Ok(record) = libp2p::core::PeerRecord::from_signed_envelope(envelope) else {
        return;
    };
    let peer = record.peer_id();
    let n = record.addresses().len();
    for addr in record.addresses() {
        swarm
            .behaviour_mut()
            .kademlia
            .add_address(&peer, addr.clone());
    }
    debug!(%peer, addrs = n, "Resolved peer addresses from a DHT record");
}

/// Whether a multiaddr's IP component is loopback (not worth publishing).
fn is_loopback(addr: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    addr.iter().any(|p| match p {
        Protocol::Ip4(ip) => ip.is_loopback(),
        Protocol::Ip6(ip) => ip.is_loopback(),
        _ => false,
    })
}

async fn run_loop(
    mut swarm: libp2p::Swarm<RucioBehaviour>,
    peer_id: PeerId,
    sign_keypair: libp2p::identity::Keypair,
    mut cmd_rx: mpsc::Receiver<NodeCmd>,
    event_tx: EventTx,
    max_foreign_providers: usize,
) {
    let topic_query = IdentTopic::new(TOPIC_SEARCH);
    let topic_result = IdentTopic::new(TOPIC_SEARCH_RESULT);
    let mut state = LoopState::new(max_foreign_providers);
    let mut dial_retry_tick = tokio::time::interval(tokio::time::Duration::from_millis(500));
    let mut announce_tick = tokio::time::interval(ANNOUNCE_INTERVAL);
    // First re-provide one full interval from now — startup already announces.
    let mut reprovide_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + REPROVIDE_INTERVAL,
        REPROVIDE_INTERVAL,
    );

    loop {
        tokio::select! {
            _ = announce_tick.tick() => {
                // Refill in-flight provider announcements up to the concurrency
                // cap. Slots free up as queries finish (StartProviding result
                // event below). Capping concurrency — not just rate — is what
                // keeps Kademlia's QueryPool table from ballooning.
                while state.provide_inflight.len() < MAX_PROVIDE_INFLIGHT {
                    let Some(key) = state.announce_queue.pop_front() else { break };
                    // Skip keys un-shared while they sat in the queue, or if we
                    // dropped out of HighId in the meantime.
                    if state.providing
                        && state.wanted_providers.contains(&key)
                        && let Some(qid) = announce_provider(&mut swarm, &mut state, &key)
                    {
                        state.provide_inflight.insert(qid);
                        state.announce_emitted += 1;
                    }
                }
                // Once a cycle has fully drained (queue empty and every query
                // finished), log how many records actually reached the DHT — so
                // the count reflects completion, not just enqueueing.
                if state.announce_emitted > 0
                    && state.announce_queue.is_empty()
                    && state.provide_inflight.is_empty()
                {
                    info!(
                        shares = state.announce_emitted,
                        "Finished announcing share(s) to the DHT"
                    );
                    state.announce_emitted = 0;
                }
            }
            _ = reprovide_tick.tick() => {
                // Refresh every provider record before its TTL expires (replaces
                // libp2p's disabled internal re-publication). Only re-queue when
                // the queue has drained, so a slow large library never stacks a
                // re-provide on top of an unfinished cycle.
                if state.providing && state.announce_queue.is_empty() {
                    state
                        .announce_queue
                        .extend(state.wanted_providers.iter().cloned());
                }
            }
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
                        // Remember bootstrap addresses so the AutoNAT check can
                        // re-dial them on demand (see `dial_autonat_servers`).
                        if !state.bootstrap_addrs.contains(&addr) {
                            state.bootstrap_addrs.push(addr.clone());
                        }
                        if let Err(e) = swarm.dial(addr) {
                            warn!("Dial failed: {e}");
                        }
                    }
                    Some(NodeCmd::KadBootstrapPeersReady) => {
                        state.has_bootstrap_peers = true;
                        info!("Bootstrap peers ready — will run Kademlia bootstrap on first connection");
                    }
                    Some(NodeCmd::StartProviding(key)) => {
                        // Remember it regardless; only announce now if we are a
                        // direct (HighId) provider. Otherwise it's announced once
                        // we reach HighId (see reconcile_provider_announcements).
                        // Announcing is deferred to the capped drain (announce_tick)
                        // so a bulk re-provide on startup doesn't spawn thousands of
                        // Kademlia queries at once. Only queue genuinely new keys.
                        if state.wanted_providers.insert(key.clone()) && state.providing {
                            state.announce_queue.push_back(key);
                        }
                    }
                    Some(NodeCmd::StopProviding(key)) => {
                        state.wanted_providers.remove(&key);
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
                    Some(NodeCmd::RequestChunk { peer, request, download_sink, id_tx }) => {
                        if let Some(transfer) = swarm.behaviour_mut().transfer.as_mut() {
                            // The codec pairs the request with the per-peer sink;
                            // only the request goes on the wire.
                            let request_id = transfer.send_request(&peer, (request, download_sink));
                            let _ = id_tx.send(request_id);
                        } else {
                            warn!("RequestChunk ignored: transfer protocol disabled");
                        }
                    }
                    Some(NodeCmd::RespondChunk { channel_id, response, upload_sink }) => {
                        if let Some((ch, peer)) = state.pending_chunk_channels.remove(&channel_id) {
                            // `send_response` returns the response back in `Err`
                            // when the channel is gone (requester cancelled or
                            // disconnected) — expected, and we must NOT log the
                            // payload: a chunk is multiple MiB of raw bytes.
                            if let Some(transfer) = swarm.behaviour_mut().transfer.as_mut()
                                && transfer.send_response(ch, (response, upload_sink)).is_err()
                            {
                                debug!(%channel_id, "Chunk response dropped: requester no longer reachable");
                                // No ResponseSent/InboundFailure will follow, so
                                // release the scheduler slot here.
                                let _ = event_tx.emit(NodeEvent::ChunkSent { peer }).await;
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
                            // Same as chunks: `Err` carries the response back
                            // and just means the requester is gone.
                            if let Some(manifest) = swarm.behaviour_mut().manifest.as_mut()
                                && manifest.send_response(ch, response).is_err()
                            {
                                debug!(%channel_id, "Manifest response dropped: requester no longer reachable");
                            }
                        } else {
                            warn!(%channel_id, "RespondManifest: unknown channel id");
                        }
                    }
                    Some(NodeCmd::RequestPinset { peer, request, id_tx }) => {
                        if let Some(pinset) = swarm.behaviour_mut().pinset.as_mut() {
                            let request_id = pinset.send_request(&peer, request);
                            let _ = id_tx.send(request_id);
                        } else {
                            warn!("RequestPinset ignored: pinset protocol disabled");
                        }
                    }
                    Some(NodeCmd::RespondPinset { channel_id, response }) => {
                        if let Some(ch) = state.pending_pinset_channels.remove(&channel_id) {
                            if let Some(pinset) = swarm.behaviour_mut().pinset.as_mut()
                                && pinset.send_response(ch, response).is_err()
                            {
                                debug!(%channel_id, "Pinset response dropped: requester no longer reachable");
                            }
                        } else {
                            warn!(%channel_id, "RespondPinset: unknown channel id");
                        }
                    }
                    Some(NodeCmd::RequestHave { peer, request }) => {
                        if let Some(have) = swarm.behaviour_mut().have.as_mut() {
                            have.send_request(&peer, request);
                        } else {
                            warn!("RequestHave ignored: have protocol disabled");
                        }
                    }
                    Some(NodeCmd::RespondHave { channel_id, response }) => {
                        if let Some(ch) = state.pending_have_channels.remove(&channel_id) {
                            if let Some(have) = swarm.behaviour_mut().have.as_mut()
                                && have.send_response(ch, response).is_err()
                            {
                                debug!(%channel_id, "Have response dropped: requester no longer reachable");
                            }
                        } else {
                            warn!(%channel_id, "RespondHave: unknown channel id");
                        }
                    }
                    Some(NodeCmd::DiscoverPeer { peer }) => {
                        // Kick a closest-peers lookup; it populates the routing
                        // table with the peer's addresses as a side effect, so a
                        // following `send_request` can dial it. We don't track the
                        // query — the address book is the only thing we need.
                        let _ = swarm.behaviour_mut().kademlia.get_closest_peers(peer);
                    }
                    Some(NodeCmd::PublishPeerRecord) => {
                        publish_peer_record(&mut swarm, &sign_keypair, &peer_id);
                    }
                    Some(NodeCmd::ResolvePeer { peer }) => {
                        // Look up the peer's signed address record (keyed by its
                        // PeerId). The result is handled in the Kademlia
                        // `GetRecord` arm, which verifies it and adds the
                        // addresses so a following `send_request` can dial.
                        let key = kad::RecordKey::new(&peer.to_bytes());
                        swarm.behaviour_mut().kademlia.get_record(key);
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
                state.autonat_servers.remove(&pid);
                emit_reachability_if_changed(state, event_tx).await;
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
                    if matches!(new_class, NodeClass::HighId) {
                        release_relay_reservation(swarm, state);
                    }
                    reconcile_provider_announcements(swarm, state);
                    let _ = event_tx.emit(NodeEvent::ClassChanged(new_class)).await;
                    emit_reachability_if_changed(state, event_tx).await;
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
                    && state.relay_listener.is_none()
                    && !state.relay_candidates.is_empty()
                {
                    try_relay_reservation(
                        swarm,
                        &state.relay_candidates,
                        &mut state.relay_listener,
                    );
                }
                reconcile_provider_announcements(swarm, state);
                let _ = event_tx.emit(NodeEvent::ClassChanged(new_class)).await;
                emit_reachability_if_changed(state, event_tx).await;
            }
        }

        // A new external-address candidate appeared (identify translated our
        // observed public IP onto our listen port). While we are not yet HighId,
        // make sure an AutoNAT server is connected to verify it — otherwise the
        // client probes every 5s but `random_autonat_server()` finds nobody.
        // Self-limiting: if a server is already connected we do nothing, so a
        // genuine LowId node (whose candidate ends up `Failed`) barely re-dials.
        SwarmEvent::NewExternalAddrCandidate { address } => {
            debug!(%address, "New external address candidate");
            if !matches!(state.classifier.current(), NodeClass::HighId)
                && state.autonat_servers.is_empty()
            {
                dial_autonat_servers(swarm, state);
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
                                    // mDNS gives no Identify info; the agent
                                    // string arrives later via the Identify path.
                                    agent_version: None,
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
                        match result {
                            QueryResult::GetProviders(Ok(
                                kad::GetProvidersOk::FoundProviders { providers, .. },
                            )) => {
                                if let Some(key) = state.provider_queries.get(&id) {
                                    let _ = event_tx
                                        .emit(NodeEvent::ProvidersFound {
                                            key: key.clone(),
                                            providers: providers.into_iter().collect(),
                                        })
                                        .await;
                                }
                            }
                            // A peer-address record resolved by `ResolvePeer`:
                            // verify the signed envelope and add the peer's
                            // current addresses so we can dial it by PeerId.
                            QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(
                                kad::PeerRecord { record, .. },
                            ))) => {
                                add_resolved_peer_addresses(swarm, &record.value);
                            }
                            // A provider announcement finished (success or error):
                            // free its in-flight slot so the drain can launch the
                            // next queued key.
                            QueryResult::StartProviding(_) => {
                                state.provide_inflight.remove(&id);
                            }
                            _ => {}
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
                        let stored = match swarm
                            .behaviour_mut()
                            .kademlia
                            .store_mut()
                            .add_provider(record)
                        {
                            Ok(()) => true,
                            Err(e) => {
                                debug!(?e, "Could not store captured provider record");
                                false
                            }
                        };
                        // Bound how many foreign provider records we hold: libp2p
                        // counts them under max_provided_keys alongside our own
                        // shares, so cap them here. note_foreign_provider returns
                        // any pairs the second-chance sweep drops once over the cap.
                        if stored {
                            for (old_key, old_provider) in
                                state.note_foreign_provider(key.clone(), provider)
                            {
                                swarm
                                    .behaviour_mut()
                                    .kademlia
                                    .store_mut()
                                    .remove_provider(&kad::RecordKey::new(&old_key), &old_provider);
                            }
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
                            // LowId nodes use a relay reservation so they become
                            // reachable; HighId nodes drop any reservation they held.
                            if matches!(new_class, NodeClass::LowId)
                                && state.relay_listener.is_none()
                                && !state.relay_candidates.is_empty()
                            {
                                try_relay_reservation(
                                    swarm,
                                    &state.relay_candidates,
                                    &mut state.relay_listener,
                                );
                            } else if matches!(new_class, NodeClass::HighId) {
                                release_relay_reservation(swarm, state);
                            }
                            reconcile_provider_announcements(swarm, state);
                            let _ = event_tx.emit(NodeEvent::ClassChanged(new_class)).await;
                            emit_reachability_if_changed(state, event_tx).await;
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
                        //
                        // Also skip any address that references our own PeerId —
                        // our own direct address, or a `/p2p-circuit` relayed
                        // through *us* (which a peer that reserved a relay on us
                        // advertises). Dialling those loops back to ourselves
                        // ("tried to dial local peer id" / circuit cancelled).
                        let me = *swarm.local_peer_id();
                        for addr in &info.listen_addrs {
                            if !addr_is_loopback_or_link_local(addr) && !addr_references(addr, &me)
                            {
                                swarm
                                    .behaviour_mut()
                                    .kademlia
                                    .add_address(&pid, addr.clone());
                            }
                        }

                        // Track peers that can act as our AutoNAT server, so a
                        // new external-address candidate knows whether it already
                        // has somewhere to be verified.
                        if info
                            .protocols
                            .iter()
                            .any(|p| p.as_ref() == AUTONAT_DIAL_REQUEST_PROTOCOL)
                        {
                            state.autonat_servers.insert(pid);
                            emit_reachability_if_changed(state, event_tx).await;
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
                                agent_version: Some(info.agent_version),
                            })
                            .await;

                        if !relay_addrs.is_empty() {
                            debug!(%pid, "Peer supports relay hop");
                            state.relay_candidates.push((pid, relay_addrs));
                            if matches!(state.classifier.current(), NodeClass::LowId)
                                && state.relay_listener.is_none()
                            {
                                try_relay_reservation(
                                    swarm,
                                    &state.relay_candidates,
                                    &mut state.relay_listener,
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

            RucioBehaviourEvent::Pinset(ps_event) => {
                on_pinset_event(ps_event, event_tx, state).await;
            }

            RucioBehaviourEvent::Have(hv_event) => {
                on_have_event(hv_event, event_tx, state).await;
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
                        // The reservation listener was already recorded in
                        // `relay_listener` when `try_relay_reservation` issued the
                        // `listen_on`; nothing more to track here.
                        info!(%relay_peer_id, "Relay reservation established");
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
    event: request_response::Event<ChunkReq, ChunkResp>,
    event_tx: &EventTx,
    state: &mut LoopState,
) {
    match event {
        // We received a response for a request we sent. The codec already
        // counted its bytes against the per-peer sink as it read them; the
        // local sink half (`.1`) is None on the read side.
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
                    response: response.0,
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
            debug!(%peer, chunk_idx = request.0.chunk_idx, "Received chunk request");
            let channel_id = state.store_chunk_channel(channel, peer);
            let delivered = event_tx
                .emit(NodeEvent::ChunkRequested {
                    peer,
                    request: request.0,
                    channel_id,
                })
                .await;
            if !delivered {
                // Event dropped under overload — drop the response channel too
                // so it doesn't linger forever (the peer's request expires).
                state.pending_chunk_channels.remove(&channel_id);
            }
        }

        request_response::Event::OutboundFailure {
            peer,
            request_id,
            error,
            ..
        } => {
            warn!(%peer, %error, "Outbound chunk request failed");
            // Propagate so the daemon frees the slot and re-queues the chunk;
            // otherwise it stays in-flight forever and the download stalls.
            let _ = event_tx
                .emit(NodeEvent::ChunkRequestFailed { request_id, peer })
                .await;
        }
        request_response::Event::InboundFailure { peer, error, .. } => {
            // Expected peer churn on a serving node: the requester closed,
            // cancelled, or timed out (e.g. got the chunk from a faster peer, or
            // finished the file). Not actionable here, so debug, not warn.
            debug!(%peer, %error, "Inbound chunk request did not complete");
            // Release the upload-scheduler slot (the serve started but the write
            // didn't complete). No-op for LowID/untracked peers.
            let _ = event_tx.emit(NodeEvent::ChunkSent { peer }).await;
        }
        request_response::Event::ResponseSent { peer, .. } => {
            // Confirms the full chunk response was written to the peer — useful
            // for telling "responder never sent it" from "transfer stalled".
            debug!(%peer, "Chunk response fully sent");
            let _ = event_tx.emit(NodeEvent::ChunkSent { peer }).await;
        }
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
            debug!(%peer, %error, "Inbound manifest request did not complete");
        }
        request_response::Event::ResponseSent { .. } => {}
    }
}

// ---------------------------------------------------------------------------
// Pin-set (request-response) handler — cooperative pinning
// ---------------------------------------------------------------------------

async fn on_pinset_event(
    event: request_response::Event<PinsetRequest, PinsetResponse>,
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
            debug!(%peer, "Received pin-set response");
            let _ = event_tx
                .emit(NodeEvent::PinsetReceived {
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
            debug!(%peer, "Received pin-set request");
            let channel_id = state.store_pinset_channel(channel);
            let delivered = event_tx
                .emit(NodeEvent::PinsetRequested {
                    peer,
                    request,
                    channel_id,
                })
                .await;
            if !delivered {
                state.pending_pinset_channels.remove(&channel_id);
            }
        }

        request_response::Event::OutboundFailure { peer, error, .. } => {
            warn!(%peer, %error, "Outbound pin-set request failed");
        }
        request_response::Event::InboundFailure { peer, error, .. } => {
            debug!(%peer, %error, "Inbound pin-set request did not complete");
        }
        request_response::Event::ResponseSent { .. } => {}
    }
}

// ---------------------------------------------------------------------------
// Availability (request-response) handler — `/have` bitmaps
// ---------------------------------------------------------------------------

async fn on_have_event(
    event: request_response::Event<HaveRequest, HaveResponse>,
    event_tx: &EventTx,
    state: &mut LoopState,
) {
    match event {
        request_response::Event::Message {
            peer,
            message: request_response::Message::Response { response, .. },
            ..
        } => {
            let _ = event_tx
                .emit(NodeEvent::HaveReceived { peer, response })
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
            let channel_id = state.store_have_channel(channel);
            let delivered = event_tx
                .emit(NodeEvent::HaveRequested {
                    peer,
                    request,
                    channel_id,
                })
                .await;
            if !delivered {
                state.pending_have_channels.remove(&channel_id);
            }
        }

        request_response::Event::OutboundFailure { peer, error, .. } => {
            debug!(%peer, %error, "Outbound availability request failed");
        }
        request_response::Event::InboundFailure { peer, error, .. } => {
            debug!(%peer, %error, "Inbound availability request did not complete");
        }
        request_response::Event::ResponseSent { .. } => {}
    }
}

// ---------------------------------------------------------------------------
// Relay reservation
// ---------------------------------------------------------------------------

/// Announce a single key to the DHT as a provider, with the warn-once handling
/// for a full provider store.
fn announce_provider(
    swarm: &mut libp2p::Swarm<RucioBehaviour>,
    state: &mut LoopState,
    key: &[u8],
) -> Option<QueryId> {
    let record_key = kad::RecordKey::new(&key);
    match swarm.behaviour_mut().kademlia.start_providing(record_key) {
        Ok(qid) => Some(qid),
        Err(e) => {
            if !state.provider_store_full_warned {
                warn!("start_providing error: {e} — further occurrences suppressed");
                state.provider_store_full_warned = true;
            } else {
                debug!("start_providing error: {e}");
            }
            None
        }
    }
}

/// Bring the DHT provider announcements in line with our reachability.
///
/// We advertise our shares **only while HighId** — i.e. while our data path is
/// direct. A relay/DCUtR-reachable node deliberately stays a non-provider: we
/// don't want a peer pulling file data through a relay (burdening it), and an
/// opportunistic hole-punch can fall back to the relay if it fails. So when we
/// reach HighId we (re)announce every wanted key, and when we drop out of HighId
/// we stop providing them.
fn reconcile_provider_announcements(
    swarm: &mut libp2p::Swarm<RucioBehaviour>,
    state: &mut LoopState,
) {
    let want = matches!(state.classifier.current(), NodeClass::HighId);
    if want == state.providing {
        return;
    }
    state.providing = want;
    let keys: Vec<Vec<u8>> = state.wanted_providers.iter().cloned().collect();
    if keys.is_empty() {
        return;
    }
    if want {
        // Queue every wanted key for the capped drain (announce_tick) rather than
        // announcing them all here — that would spawn one Kademlia query per share
        // at once, ballooning the QueryPool and flooding the DHT.
        state.announce_queue.extend(keys.iter().cloned());
    } else {
        // Dropping out of HighId: stop providing and discard any still-pending
        // announcements so we never advertise content reachable only via a relay.
        state.announce_queue.clear();
        for key in &keys {
            let record_key = kad::RecordKey::new(key);
            swarm.behaviour_mut().kademlia.stop_providing(&record_key);
        }
    }
    info!(
        providing = want,
        shares = keys.len(),
        "Provider announcements toggled by reachability (HighId only)"
    );
}

/// Pick the first relay candidate with a public address and issue a
/// `listen_on` for a `/p2p-circuit` address.  The relay client behaviour
/// turns that into a reservation request; on success it emits
/// `RelayClient::ReservationReqAccepted` and the swarm starts advertising
/// the circuit address.
fn try_relay_reservation(
    swarm: &mut libp2p::Swarm<RucioBehaviour>,
    candidates: &[(PeerId, Vec<Multiaddr>)],
    listener: &mut Option<ListenerId>,
) {
    if listener.is_some() {
        return;
    }
    for (relay_peer, addrs) in candidates {
        if let Some(relay_addr) = addrs.iter().find(|a| !addr_is_private_or_loopback(a)) {
            let circuit_addr = relay_addr
                .clone()
                .with(Protocol::P2p(*relay_peer))
                .with(Protocol::P2pCircuit);
            match swarm.listen_on(circuit_addr.clone()) {
                Ok(id) => {
                    info!(%circuit_addr, "Relay reservation initiated (LowId node)");
                    *listener = Some(id);
                    return;
                }
                Err(e) => warn!(%circuit_addr, "Failed to initiate relay reservation: {e}"),
            }
        }
    }
}

/// Cancel an active relay-circuit reservation, if any. Called when the node
/// reaches HighId: a publicly reachable node serves peers directly, so it no
/// longer needs — nor should keep advertising — a `/p2p-circuit` fallback.
fn release_relay_reservation(swarm: &mut libp2p::Swarm<RucioBehaviour>, state: &mut LoopState) {
    if let Some(id) = state.relay_listener.take()
        && swarm.remove_listener(id)
    {
        info!("Relay reservation released (node became HighId)");
    }
}

/// Derive the current AutoNAT reachability state from the node class and
/// whether any AutoNAT server is connected.
fn current_reachability(state: &LoopState) -> Reachability {
    if matches!(state.classifier.current(), NodeClass::HighId) {
        Reachability::Confirmed
    } else if state.autonat_servers.is_empty() {
        Reachability::NoServers
    } else {
        Reachability::Verifying
    }
}

/// Emit `ReachabilityChanged` when the derived reachability differs from the
/// last one emitted. Called after anything that can change it: a class change
/// or an AutoNAT server connecting/disconnecting.
async fn emit_reachability_if_changed(state: &mut LoopState, event_tx: &EventTx) {
    let r = current_reachability(state);
    if state.last_reachability.as_ref() != Some(&r) {
        state.last_reachability = Some(r.clone());
        let _ = event_tx.emit(NodeEvent::ReachabilityChanged(r)).await;
    }
}

/// Dial our known bootstrap peers (which run the AutoNAT v2 server) so the
/// AutoNAT client has a server to ask for a reachability dial-back. Skips peers
/// we are already connected to. Called on demand when an external-address
/// candidate appears and no AutoNAT server is currently connected — see the
/// `NewExternalAddrCandidate` handler.
fn dial_autonat_servers(swarm: &mut libp2p::Swarm<RucioBehaviour>, state: &LoopState) {
    // One reachable server is enough; cap the dials so a long bootstrap list
    // (built-ins plus cached peers from previous sessions) can't trigger a
    // dial storm. The real bootstrap peers are added first, so they win.
    const MAX_AUTONAT_DIALS: usize = 3;
    let mut dialled = 0;
    for addr in &state.bootstrap_addrs {
        if dialled >= MAX_AUTONAT_DIALS {
            break;
        }
        // Skip bootstrap peers we already hold a connection to.
        if let Some(Protocol::P2p(peer)) = addr.iter().last()
            && swarm.is_connected(&peer)
        {
            continue;
        }
        match swarm.dial(addr.clone()) {
            Ok(()) => {
                debug!(%addr, "AutoNAT: dialling bootstrap to verify reachability");
                dialled += 1;
            }
            Err(e) => debug!(%addr, "AutoNAT: bootstrap dial failed: {e}"),
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
        // We tried to dial an address that resolves to ourselves (our own
        // address, or a `/p2p-circuit` relayed through us that a peer
        // advertised). libp2p refuses it; harmless, not actionable.
        DialError::LocalPeerId { .. } => DialNoise::Expected,
        DialError::Transport(addrs) if !addrs.is_empty() => {
            let mut has_unreachable = false;
            for (addr, err) in addrs {
                if addr_is_private_or_loopback(addr) {
                    continue;
                }
                // A failed dial to a relay circuit (`/p2p-circuit`) is always
                // best-effort churn: the relay's reservation for the destination
                // may have lapsed, or the destination went away. Never a fault
                // of ours, so treat it as expected regardless of the inner error.
                if addr.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
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
        // Relay circuit set-up cancelled (e.g. a circuit relayed through us, or
        // the relay/behaviour dropping the request) — routine, not a fault.
        "Response from behaviour was canceled",
        "oneshot canceled",
        // Relayed dial to a peer whose reservation on the relay has expired or
        // who has gone away — stale circuit address, routine churn. (Circuit
        // dials are also short-circuited in classify_dial_error, but keep the
        // markers for the aggregated-error case.)
        "Relay has no reservation for destination",
        "Failed to connect to destination",
        // EACCES on connect: the host blocks this outbound route/address family
        // (e.g. a VPS with no outbound IPv6). Environmental, not actionable.
        "Permission denied",
        // Relay/transport timeout phrasing distinct from io TimedOut.
        "Timeout has been reached",
        // A bare `/p2p/<id>` (peer id known but no dialable transport address)
        // or any address form we can't dial. Routine — we just lack an address.
        "Unsupported resolved address",
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

/// Return `true` if `addr` references `me` in any `/p2p/<peer>` component —
/// either as the address's own terminal peer (our own address) or as a relay
/// leg (`/p2p/<me>/p2p-circuit/...`, a circuit routed through us). Dialling
/// such an address loops back to ourselves, so we never store it for a peer.
fn addr_references(addr: &Multiaddr, me: &PeerId) -> bool {
    addr.iter()
        .any(|p| matches!(p, Protocol::P2p(id) if id == *me))
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

    #[test]
    fn foreign_provider_clock_spares_refreshed_records() {
        let mut s = LoopState::new(2);
        let p = PeerId::random();
        // Filling up to the cap evicts nothing.
        assert!(s.note_foreign_provider(b"k1".to_vec(), p).is_empty());
        assert!(s.note_foreign_provider(b"k2".to_vec(), p).is_empty());
        // With no record yet referenced, overflow behaves as plain FIFO: the
        // oldest (k1) is evicted.
        assert_eq!(
            s.note_foreign_provider(b"k3".to_vec(), p),
            vec![(b"k1".to_vec(), p)]
        );
        // Refresh k2 (now the oldest survivor): no re-enqueue, no eviction, just
        // marks it referenced for the next sweep.
        assert!(s.note_foreign_provider(b"k2".to_vec(), p).is_empty());
        assert_eq!(s.foreign_providers.len(), 2);
        // A new record overflows again. The CLOCK sweep gives the referenced k2 a
        // second chance and evicts the unreferenced k3 instead — this is what
        // distinguishes second-chance from pure FIFO (FIFO would drop k2).
        let q = PeerId::random();
        assert_eq!(
            s.note_foreign_provider(b"k4".to_vec(), q),
            vec![(b"k3".to_vec(), p)]
        );
        assert!(s.foreign_provider_ref.contains_key(&(b"k2".to_vec(), p)));
        assert!(!s.foreign_provider_ref.contains_key(&(b"k3".to_vec(), p)));
        assert!(s.foreign_provider_ref.contains_key(&(b"k4".to_vec(), q)));
    }

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
