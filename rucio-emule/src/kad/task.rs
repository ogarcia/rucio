//! Permanent Kad2 background task.
//!
//! `KadTask` owns the UDP socket and routing table for the lifetime of the
//! daemon.  It runs as a dedicated `tokio::spawn` loop that:
//!
//! - Receives every incoming Kad2 UDP packet and responds correctly:
//!   - `BOOTSTRAP_REQ`  → `BOOTSTRAP_RES`  (share our routing table)
//!   - `HELLO_REQ`      → `HELLO_RES` + `HELLO_RES_ACK`
//!   - `HELLO_RES`      → `HELLO_RES_ACK`
//!   - `PING`           → `PONG`
//!   - `REQ`            → `RES`  (closest contacts)
//!   - All others are forwarded to any waiting `SearchSources` command.
//!
//! - Answers commands sent via [`KadHandle`]:
//!   - [`KadCommand::Bootstrap`]      — connect to the network from nodes.dat
//!   - [`KadCommand::SearchSources`]  — iterative Kad2 source search
//!
//! ## Why a single task owns the socket
//!
//! A `UdpSocket` cannot be shared between concurrent tasks with `recv_from`
//! — only one task can receive at a time.  If we gave the socket directly to
//! each download, packets would be consumed by the wrong reader.  The task
//! acts as a demultiplexer: it reads every packet and dispatches it either to
//! the protocol handler or to a waiting search operation.

use super::packet::{
    self, Contact, KadId, KadPacket, decode, encode_bootstrap_req, encode_bootstrap_res,
    encode_firewalled_req, encode_firewalled_res, encode_hello_req, encode_hello_res,
    encode_hello_res_ack, encode_pong, encode_req, encode_res, kad_id_from_hash,
};
use super::routing::RoutingTable;
use super::search::{ActiveSearch, ObfuscMode, OutPacket};
pub use super::search::{KadSource, KeywordHit};
use crate::ed2k::Ed2kHash;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::time::{Instant, timeout};
use tracing::{debug, info, trace, warn};

/// How often to re-run the Kad firewall check once our IP is known, to keep the
/// connectivity verdict fresh (open nodes keep receiving callbacks). Shorter
/// than the daemon's recent-inbound window so an open node never decays.
const FW_CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);

// ── External-IP consensus ───────────────────────────────────────────────────

/// Aggregates the external IP reported by Kad peers (via `TAG_SENDER_IP` in
/// HelloRes / learned from Pong) and tracks the most-voted value.
///
/// A single peer can misreport (or lie about) our IP, so instead of trusting
/// the first report we keep a tally and publish whichever IP currently leads.
/// The first report wins immediately (so the IP shows up right away), but a
/// genuinely different value will overtake it once more peers agree.
#[derive(Default)]
struct ExternalIpVotes {
    votes: HashMap<Ipv4Addr, u32>,
    leader: Option<Ipv4Addr>,
}

impl ExternalIpVotes {
    /// Record one peer's report. Returns `Some(ip)` when the leading IP changes
    /// (i.e. the published value should be updated), `None` otherwise.
    fn record(&mut self, ip: Ipv4Addr) -> Option<Ipv4Addr> {
        if ip.is_unspecified() {
            return None;
        }
        let count = {
            let e = self.votes.entry(ip).or_insert(0);
            *e += 1;
            *e
        };
        let leader_votes = self
            .leader
            .and_then(|l| self.votes.get(&l).copied())
            .unwrap_or(0);
        if self.leader != Some(ip) && count > leader_votes {
            self.leader = Some(ip);
            Some(ip)
        } else {
            None
        }
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Commands sent to the Kad2 background task.
pub enum KadCommand {
    /// Bootstrap from nodes.dat contacts.
    Bootstrap {
        seeds: Vec<Contact>,
        reply: oneshot::Sender<usize>, // number of contacts in routing table after bootstrap
    },
    /// Search for ed2k sources.
    SearchSources {
        hash: Ed2kHash,
        file_size: u64,
        reply: oneshot::Sender<Vec<KadSource>>,
    },
    /// Keyword search — returns up to N results with file name, hash, size.
    SearchKeyword {
        keyword: String,
        reply: oneshot::Sender<Vec<KeywordHit>>,
    },
    /// Publish ourselves as a source for an ed2k file (good-citizen seeding).
    /// Replies with the number of nodes that acknowledged the store.
    PublishSource {
        hash: Ed2kHash,
        file_size: u64,
        reply: oneshot::Sender<usize>,
    },
}

/// Number of nodes to store ourselves on per source publish (eMule's
/// `SEARCHSTOREFILE_TOTAL`). Once this many acknowledge, the publish stops.
const PUBLISH_STORE_TARGET: usize = 10;

/// Handle to the running `KadTask`.  Cheap to clone.
#[derive(Clone)]
pub struct KadHandle {
    tx: mpsc::Sender<KadCommand>,
    routing_table: Arc<RwLock<RoutingTable>>,
    /// Our external IPv4 as currently known by the task, encoded as a `u32`
    /// (`0` = still unknown).  Seeded from config and then learned from peer
    /// responses (Pong / bootstrap HelloRes).
    external_ip: Arc<AtomicU32>,
    /// Serialises searches: the task runs only one search at a time and drops
    /// any that arrive while one is active. Holding a permit across a search
    /// call makes concurrent callers queue instead of being dropped, and the
    /// gate's priority means user keyword searches jump ahead of queued
    /// download source lookups.
    search_gate: Arc<super::gate::PriorityGate>,
}

impl KadHandle {
    /// Bootstrap from a set of seed contacts.
    /// Returns the number of contacts in the routing table after bootstrapping.
    pub async fn bootstrap(&self, seeds: Vec<Contact>) -> usize {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(KadCommand::Bootstrap { seeds, reply: tx })
            .await;
        rx.await.unwrap_or(0)
    }

    /// Search for sources for the given ed2k hash.
    pub async fn search_sources(&self, hash: Ed2kHash, file_size: u64) -> Vec<KadSource> {
        // One search at a time — wait our turn instead of being dropped.
        // Low priority: yields to user keyword searches.
        let _permit = self.search_gate.acquire(super::gate::Priority::Low).await;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(KadCommand::SearchSources {
                hash,
                file_size,
                reply: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Keyword search — returns matching file hits from Kad index.
    ///
    /// Acquires the (high-priority) search slot and runs the search. Use
    /// [`KadHandle::acquire_keyword_slot`] + [`KadHandle::search_keyword_held`]
    /// instead when you need to observe the "waiting for a turn" phase.
    pub async fn search_keyword(&self, keyword: String) -> Vec<KeywordHit> {
        // One search at a time — wait our turn instead of being dropped.
        // High priority: user-initiated, so it jumps ahead of queued source
        // lookups to keep the search box responsive.
        let _permit = self.acquire_keyword_slot().await;
        self.search_keyword_held(keyword).await
    }

    /// Whether a Kad search currently holds the single search slot, i.e. a
    /// keyword search started now would have to wait its turn. Best-effort.
    pub fn search_in_progress(&self) -> bool {
        self.search_gate.is_busy()
    }

    /// Acquire the high-priority search slot, waiting our turn. Awaiting this is
    /// the "queued" phase; once it resolves the search may run. Hold the
    /// returned permit until the search completes, then drop it.
    pub async fn acquire_keyword_slot(&self) -> super::gate::SearchPermit {
        self.search_gate.acquire(super::gate::Priority::High).await
    }

    /// Run a keyword search assuming the caller already holds the search slot
    /// (via [`KadHandle::acquire_keyword_slot`]). Does not touch the gate.
    pub async fn search_keyword_held(&self, keyword: String) -> Vec<KeywordHit> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(KadCommand::SearchKeyword { keyword, reply: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Publish ourselves as a source for `hash` to the Kad nodes closest to it,
    /// so other clients can discover us by the canonical route. Returns how many
    /// nodes acknowledged the store. Low priority: yields to user keyword
    /// searches and download source lookups for the single search slot.
    pub async fn publish_source(&self, hash: Ed2kHash, file_size: u64) -> usize {
        let _permit = self.search_gate.acquire(super::gate::Priority::Low).await;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(KadCommand::PublishSource {
                hash,
                file_size,
                reply: tx,
            })
            .await;
        rx.await.unwrap_or(0)
    }

    /// Number of contacts currently in the routing table.
    ///
    /// Reads the shared routing table directly rather than round-tripping a
    /// command through the task loop: the loop runs long operations (notably
    /// `do_bootstrap`) inline and would not service a command for tens of
    /// seconds, hanging every caller — including the `/emule/status` handler
    /// the web UI's settings modal polls. Mirrors `dump_nodes_dat`.
    pub async fn contact_count(&self) -> usize {
        self.routing_table.read().await.len()
    }

    /// Read the routing table directly (non-blocking snapshot).
    pub fn routing_table(&self) -> Arc<RwLock<RoutingTable>> {
        Arc::clone(&self.routing_table)
    }

    /// Our external IPv4 as learned from Kad2 peer responses, or `None` if not
    /// yet known.  Useful for displaying the public IP when UPnP is disabled.
    pub fn external_ip(&self) -> Option<Ipv4Addr> {
        match self.external_ip.load(Ordering::Relaxed) {
            0 => None,
            raw => Some(Ipv4Addr::from(raw)),
        }
    }

    /// Serialize the current routing table to `nodes.dat` format (version 3).
    ///
    /// Returns the raw bytes ready to be written to disk.  Returns an empty
    /// `Vec` if the routing table is empty.
    pub async fn dump_nodes_dat(&self) -> Vec<u8> {
        let rt = self.routing_table.read().await;
        let contacts: Vec<_> = rt.all_contacts().cloned().collect();
        if contacts.is_empty() {
            return Vec::new();
        }
        super::routing::write_nodes_dat(&contacts)
    }
}

// ── Task internals ────────────────────────────────────────────────────────────

/// Configuration for the Kad2 task.
#[derive(Debug, Clone)]
pub struct KadTaskConfig {
    /// Our TCP port (advertised in HELLO packets).
    pub tcp_port: u16,
    /// Timeout for individual UDP request/response pairs.
    pub request_timeout: Duration,
    /// Maximum wall-clock time for a full source search.
    pub search_timeout: Duration,
    /// Maximum sources to collect per source search.
    pub max_sources: usize,
    /// Maximum file hits to collect per keyword search.
    pub max_keyword_results: usize,
    /// Number of parallel lookup queries (alpha).
    pub alpha: usize,
    /// Number of node-lookup iterations before switching to source search.
    pub lookup_iterations: usize,
    /// How often to re-bootstrap if the routing table is below this threshold.
    pub min_contacts: usize,
    /// Interval between keep-alive pings to a random contact.
    pub keepalive_interval: Duration,
    /// Known external IPv4 (from UPnP or config).  Used for UDP obfuscation.
    /// If unspecified (0.0.0.0), the task will try to learn it from peer responses.
    pub initial_external_ip: std::net::Ipv4Addr,
    /// Our eMule user hash (16 bytes). Published as the **source owner ID** when
    /// we announce ourselves as a Kad source — eMule downloaders read it back as
    /// the source's user hash and key their TCP-obfuscation RC4 stream with it
    /// (`DownloadQueue.cpp`: `SetUserHash(cID)`), so it must match the hash we
    /// advertise in HELLO and decrypt inbound obfuscation with. This is distinct
    /// from the node's routing `KadId`.
    pub user_hash: [u8; 16],
}

impl Default for KadTaskConfig {
    fn default() -> Self {
        Self {
            tcp_port: 4662,
            request_timeout: Duration::from_secs(5),
            search_timeout: Duration::from_secs(60),
            max_sources: 50,
            max_keyword_results: 300,
            alpha: 20,
            lookup_iterations: 5,
            min_contacts: 4,
            keepalive_interval: Duration::from_secs(60),
            initial_external_ip: std::net::Ipv4Addr::UNSPECIFIED,
            user_hash: [0u8; 16],
        }
    }
}

/// Spawn the Kad2 background task and return a handle.
pub fn spawn(socket: Arc<UdpSocket>, our_id: KadId, cfg: KadTaskConfig) -> KadHandle {
    let routing_table = Arc::new(RwLock::new(RoutingTable::new(our_id)));
    let rt_clone = Arc::clone(&routing_table);
    let external_ip = Arc::new(AtomicU32::new(0));
    let ip_clone = Arc::clone(&external_ip);
    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    // Our persistent (per-session) Kad UDP secret. Never sent on the wire; used
    // to derive per-peer verify keys (see `obfuscation::udp_verify_key`).
    let our_udp_key = super::obfuscation::random_udp_key();

    tokio::spawn(run_task(
        socket,
        our_id,
        our_udp_key,
        cfg,
        rt_clone,
        ip_clone,
        cmd_rx,
    ));

    KadHandle {
        tx: cmd_tx,
        routing_table,
        external_ip,
        search_gate: Arc::new(super::gate::PriorityGate::new()),
    }
}

// ── Task main loop ────────────────────────────────────────────────────────────

async fn run_task(
    socket: Arc<UdpSocket>,
    our_id: KadId,
    our_udp_key: u32,
    cfg: KadTaskConfig,
    routing_table: Arc<RwLock<RoutingTable>>,
    external_ip: Arc<AtomicU32>,
    mut cmd_rx: mpsc::Receiver<KadCommand>,
) {
    let mut recv_buf = [0u8; 4096];
    // Our local Kad UDP port, advertised as SOURCEUPORT when we publish sources.
    let our_udp_port = socket
        .local_addr()
        .map(|a| a.port())
        .unwrap_or(cfg.tcp_port);
    let mut active_search: Option<ActiveSearch> = None;
    let mut keepalive_tick = Instant::now() + cfg.keepalive_interval;
    // Re-run the firewall check periodically so an open node keeps receiving
    // callbacks (its connectivity verdict stays fresh) and a firewalled one
    // keeps confirming it gets none.
    let mut fw_check_tick = Instant::now() + FW_CHECK_INTERVAL;
    // Seeds from the last bootstrap call — reused for automatic re-bootstrap
    // when the routing table drops below min_contacts.
    let mut last_seeds: Vec<Contact> = Vec::new();
    // Our external IPv4 — seeded from config, then updated by majority vote
    // across the IPs peers report back to us (TAG_SENDER_IP / bootstrap).
    let mut our_external_ip = cfg.initial_external_ip;
    let mut ip_votes = ExternalIpVotes::default();
    // Latches once we first decode an obfuscated incoming packet, so we log
    // "obfuscation active" a single time instead of per packet.
    let mut obfusc_logged = false;
    // Set while a detached bootstrap task is running. Keeps a second concurrent
    // bootstrap (e.g. several downloads re-bootstrapping at once) from flooding
    // the network with duplicate requests — it returns the current count instead.
    let bootstrap_active = Arc::new(AtomicBool::new(false));
    if !our_external_ip.is_unspecified() {
        info!(%our_external_ip, "Using configured external IP for Kad2 obfuscation");
    }

    info!("Kad2 task started");

    loop {
        // Publish the current external IP so `KadHandle::external_ip` can read
        // it.  Cheap relaxed store each iteration keeps it current regardless of
        // which branch below learns or updates the address.
        external_ip.store(u32::from(our_external_ip), Ordering::Relaxed);

        // Determine how long to wait for the next packet.
        let recv_deadline = if let Some(ref s) = active_search {
            s.deadline
        } else {
            keepalive_tick
        };
        let remaining = recv_deadline.saturating_duration_since(Instant::now());

        tokio::select! {
            // ── Incoming UDP packet ────────────────────────────────────────
            result = timeout(remaining, socket.recv_from(&mut recv_buf)) => {
                match result {
                    Ok(Ok((n, src))) => {
                        // Log raw opcode of every incoming packet during active search.
                        if active_search.is_some() && n >= 2 {
                            debug!(
                                proto = format!("0x{:02x}", recv_buf[0]),
                                opcode = if recv_buf[0] == 0xe4 { format!("0x{:02x}", recv_buf[1]) } else { "obfusc?".to_string() },
                                len = n,
                                %src,
                                "Kad2 raw pkt during search"
                            );
                            if recv_buf[0] == 0xe4 && recv_buf[1] == 0x3b {
                                debug!(hex = %hex::encode(&recv_buf[..n]), %src, "Kad2 SearchRes raw hex");
                            }
                        }
                        if let Some(reported) = handle_packet(
                            &recv_buf[..n],
                            src,
                            &socket,
                            our_id,
                            our_udp_key,
                            &cfg,
                            &routing_table,
                            &mut active_search,
                            &mut obfusc_logged,
                        ).await
                            && let Some(ip) = ip_votes.record(reported)
                        {
                            our_external_ip = ip;
                            info!(external_ip = %ip, "External IP learned from Kad peers (consensus)");
                        }
                    }
                    Ok(Err(e)) => warn!("Kad2 recv error: {e}"),
                    Err(_) => {
                        // Timeout — either search deadline or keepalive tick.
                        if let Some(s) = active_search.take() {
                            if Instant::now() >= s.deadline {
                                info!(
                                    queried = s.queried_count(),
                                    target = %s.target,
                                    "Kad2 search timed out"
                                );
                                s.finish();
                            } else {
                                active_search = Some(s);
                            }
                        }
                        if Instant::now() >= keepalive_tick {
                            keepalive_tick = Instant::now() + cfg.keepalive_interval;
                            // If the table got thin, re-bootstrap instead of just
                            // pinging — detached, like the Bootstrap command, so
                            // the loop stays responsive rather than freezing for
                            // the whole bootstrap. The guard avoids overlapping it
                            // with a bootstrap that is already running.
                            let low = routing_table.read().await.len() < cfg.min_contacts;
                            if low
                                && !last_seeds.is_empty()
                                && bootstrap_active
                                    .compare_exchange(
                                        false,
                                        true,
                                        Ordering::AcqRel,
                                        Ordering::Acquire,
                                    )
                                    .is_ok()
                            {
                                info!("Kad2 routing table low — re-bootstrapping");
                                let (tx, _rx) = oneshot::channel();
                                spawn_bootstrap_task(
                                    &socket,
                                    our_id,
                                    our_udp_key,
                                    &cfg,
                                    &routing_table,
                                    last_seeds.clone(),
                                    tx,
                                    &bootstrap_active,
                                );
                            } else {
                                send_keepalive(&socket, our_id, our_udp_key, &cfg, &routing_table)
                                    .await;
                            }
                        }
                        // Probe for callbacks: aggressively until we know our IP,
                        // then periodically to keep the connectivity verdict fresh.
                        let fw_due = Instant::now() >= fw_check_tick;
                        if our_external_ip.is_unspecified() || fw_due {
                            if fw_due {
                                fw_check_tick = Instant::now() + FW_CHECK_INTERVAL;
                            }
                            send_firewall_checks(&socket, &cfg, &routing_table, our_udp_key).await;
                        }
                    }
                }
            }

            // ── Command from daemon ────────────────────────────────────────
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    KadCommand::Bootstrap { seeds, reply } => {
                        last_seeds = seeds.clone();
                        // Run the bootstrap detached so the loop keeps servicing
                        // the socket (peers, keepalives, searches, commands)
                        // while it runs. The bootstrap only *sends*; this loop
                        // ingests every response via `handle_packet` (which also
                        // learns our external IP through `ip_votes` below), so it
                        // never needs the socket's read side. The guard turns a
                        // concurrent bootstrap into a no-op that returns the
                        // current contact count.
                        if bootstrap_active
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            spawn_bootstrap_task(
                                &socket,
                                our_id,
                                our_udp_key,
                                &cfg,
                                &routing_table,
                                seeds,
                                reply,
                                &bootstrap_active,
                            );
                        } else {
                            debug!("Kad2 bootstrap already in progress — returning current count");
                            let _ = reply.send(routing_table.read().await.len());
                        }
                    }
                    KadCommand::SearchSources { hash, file_size, reply } => {
                        if active_search.is_some() {
                            warn!("Kad2 search already in progress, dropping new request");
                            let _ = reply.send(vec![]);
                            continue;
                        }
                        let target = kad_id_from_hash(hash.as_bytes());
                        let initial_candidates: Vec<_> = {
                            let rt = routing_table.read().await;
                            rt.closest_to(&target, cfg.alpha)
                        };
                        if initial_candidates.is_empty() {
                            debug!(%target, "Kad2 not bootstrapped, cannot search for sources");
                            let _ = reply.send(vec![]);
                            continue;
                        }
                        let deadline = Instant::now() + cfg.search_timeout;
                        let (search, pkts) = ActiveSearch::new_source_search(
                            target, file_size, deadline, cfg.max_sources,
                            &initial_candidates, cfg.alpha, reply,
                        );
                        debug!(sent = pkts.len(), %target, "Started Kad2 source search");
                        send_out_packets(&socket, pkts, our_udp_key).await;
                        active_search = Some(search);
                    }
                    KadCommand::SearchKeyword { keyword, reply } => {
                        if active_search.is_some() {
                            warn!("Kad2 search already in progress, dropping keyword request");
                            let _ = reply.send(vec![]);
                            continue;
                        }
                        let target = packet::keyword_target(&keyword);
                        let initial_candidates: Vec<_> = {
                            let rt = routing_table.read().await;
                            rt.closest_to(&target, cfg.alpha)
                        };
                        if initial_candidates.is_empty() {
                            debug!(keyword, "Kad2 not bootstrapped, skipping keyword search");
                            let _ = reply.send(vec![]);
                            continue;
                        }
                        let deadline = Instant::now() + cfg.search_timeout;
                        let (search, pkts) = ActiveSearch::new_keyword_search(
                            target, deadline, cfg.max_keyword_results,
                            &initial_candidates, cfg.alpha, reply,
                        );
                        info!(keyword, %target, sent = pkts.len(), "Started Kad2 keyword search");
                        send_out_packets(&socket, pkts, our_udp_key).await;
                        active_search = Some(search);
                    }
                    KadCommand::PublishSource { hash, file_size, reply } => {
                        if active_search.is_some() {
                            warn!("Kad2 search already in progress, dropping publish request");
                            let _ = reply.send(0);
                            continue;
                        }
                        let target = kad_id_from_hash(hash.as_bytes());
                        let initial_candidates: Vec<_> = {
                            let rt = routing_table.read().await;
                            rt.closest_to(&target, cfg.alpha)
                        };
                        if initial_candidates.is_empty() {
                            debug!(%target, "Kad2 not bootstrapped, cannot publish source");
                            let _ = reply.send(0);
                            continue;
                        }
                        let deadline = Instant::now() + cfg.search_timeout;
                        // The source owner ID is our user hash in eMule's CUInt128
                        // wire form (swapped) — NOT the node's routing id — so a
                        // downloader recovers our user hash and can obfuscate to us.
                        let source_owner = kad_id_from_hash(&cfg.user_hash);
                        let (search, pkts) = ActiveSearch::new_publish(
                            target, source_owner, cfg.tcp_port, our_udp_port, file_size,
                            crate::transfer::TCP_CONNECT_OPTIONS, deadline,
                            PUBLISH_STORE_TARGET, &initial_candidates, cfg.alpha, reply,
                        );
                        debug!(sent = pkts.len(), %target, "Started Kad2 source publish");
                        send_out_packets(&socket, pkts, our_udp_key).await;
                        active_search = Some(search);
                    }
                }
            }

            else => break,
        }

        // Check if the active search has collected enough results.
        if active_search.as_ref().is_some_and(|s| s.is_done()) {
            active_search.take().unwrap().finish();
        }
    }

    info!("Kad2 task stopped");
}

// ── Packet handler ────────────────────────────────────────────────────────────

/// Send `KADEMLIA_FIREWALLED_REQ` to a few contacts so they connect back and
/// report our external IP (firewall-check phase 1a — IP discovery).
///
/// Prefers contacts whose UDP key we know (so the request — and the
/// `FIREWALLED_RES` reply — can be obfuscated), but falls back to any contact:
/// eMule answers a plain firewall request unconditionally, so a key is not
/// required to learn our IP. Cheap and idempotent — safe to call periodically.
async fn send_firewall_checks(
    socket: &UdpSocket,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    our_udp_key: u32,
) {
    let contacts: Vec<Contact> = {
        let rt = routing_table.read().await;
        let mut keyed: Vec<Contact> = Vec::new();
        let mut unkeyed: Vec<Contact> = Vec::new();
        for c in rt.all_contacts() {
            if c.udp_key.is_some() {
                keyed.push(c.clone());
            } else if unkeyed.len() < 4 {
                unkeyed.push(c.clone());
            }
            if keyed.len() >= 4 {
                break;
            }
        }
        keyed.into_iter().chain(unkeyed).take(4).collect()
    };
    if contacts.is_empty() {
        return;
    }
    let req = encode_firewalled_req(cfg.tcp_port);
    for c in &contacts {
        let addr = SocketAddr::V4(c.socket_addr_udp());
        send_kad_pkt(socket, &req, addr, c.udp_key, our_udp_key).await;
    }
    debug!(probes = contacts.len(), "Sent Kad firewall-check requests");
}

/// Our SenderVerifyKey for a given peer, derived from our secret and the peer's
/// IP. Included in obfuscated packets so the peer can store our key and reply
/// with the verified RecvKey scheme. Zero for non-IPv4 peers (we only speak v4).
fn sender_verify_key(addr: SocketAddr, our_secret: u32) -> u32 {
    match addr {
        SocketAddr::V4(v4) => super::obfuscation::udp_verify_key(our_secret, *v4.ip()),
        SocketAddr::V6(_) => 0,
    }
}

/// Send a Kad2 packet, obfuscating it if `recv_key` is known.
/// Kad obfuscation keys are derived from the receiver's key/NodeID, not our own
/// IP, so we can obfuscate even before we know our external address (unlike
/// ed2k). Falls back to plain when we have no key for the peer; eMule accepts
/// plain Kad packets. Returns `true` if sent obfuscated, `false` otherwise.
async fn send_kad_pkt(
    socket: &UdpSocket,
    plain: &[u8],
    addr: SocketAddr,
    recv_key: Option<u32>,
    our_secret: u32,
) -> bool {
    if let Some(key) = recv_key {
        let rkp = super::obfuscation::random_key_part();
        let svk = sender_verify_key(addr, our_secret);
        let obfuscated = super::obfuscation::obfuscate_recv_key(plain, key, rkp, svk);
        let _ = socket.send_to(&obfuscated, addr).await;
        return true;
    }
    let _ = socket.send_to(plain, addr).await;
    false
}

/// Send a Kad2 packet obfuscated with the KadID scheme (eMule v6+).
/// Used when we have the peer's KadID but not their announced UDPKey. The KadID
/// scheme keys off the receiver's NodeID, so our own IP is not needed.
async fn send_kad_pkt_kad_id(
    socket: &UdpSocket,
    plain: &[u8],
    addr: SocketAddr,
    kad_id: &[u8; 16],
    our_secret: u32,
) -> bool {
    let rkp = super::obfuscation::random_key_part();
    let svk = sender_verify_key(addr, our_secret);
    let obfuscated = super::obfuscation::obfuscate_kad_id(plain, kad_id, rkp, svk);
    let _ = socket.send_to(&obfuscated, addr).await;
    true
}

/// Handle one incoming UDP packet.
/// Returns `Some(Ipv4Addr)` if we learned our external IP from a HelloRes.
#[allow(clippy::too_many_arguments)]
async fn handle_packet(
    data: &[u8],
    src: SocketAddr,
    socket: &UdpSocket,
    our_id: KadId,
    our_udp_key: u32,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    active_search: &mut Option<ActiveSearch>,
    obfusc_logged: &mut bool,
) -> Option<std::net::Ipv4Addr> {
    // Try plain decode first; if that fails and the packet doesn't start with
    // 0xe4/0xe5, attempt to deobfuscate using our UDPKey.
    let src_v4_ip = match src {
        SocketAddr::V4(a) => Some(*a.ip()),
        SocketAddr::V6(_) => None,
    };
    // `sender_verify_key` is the peer's UDP key, recovered from the obfuscation
    // header of an encrypted packet (eMule carries it there, not as a HELLO tag).
    // We store it on the contact so we can obfuscate replies and — crucially —
    // send firewall checks to learn our own external IP.
    let (pkt, sender_verify_key) = {
        let plain_result = decode(data);
        match plain_result {
            Ok(p) => (p, None),
            Err(_) => {
                // Try deobfuscation if we have sender's IPv4.
                if let Some(ip) = src_v4_ip {
                    if let Some(d) = super::obfuscation::deobfuscate_keyed(
                        data,
                        our_udp_key,
                        ip,
                        Some(our_id.as_bytes()),
                    ) {
                        match decode(&d.payload) {
                            Ok(p) => {
                                if !*obfusc_logged {
                                    *obfusc_logged = true;
                                    info!(
                                        "Kad2: obfuscated UDP communication active — peers are encrypting traffic to us"
                                    );
                                }
                                trace!(%src, "Kad2 deobfuscated (encrypted) packet");
                                (p, d.sender_verify_key)
                            }
                            Err(e) => {
                                trace!("Kad2 decode error (after deobfuscate) from {src}: {e}");
                                return None;
                            }
                        }
                    } else {
                        trace!("Kad2 decode error from {src}: not plain and not deobfuscatable");
                        return None;
                    }
                } else {
                    trace!("Kad2 decode error from {src}: IPv6 source, skipping deobfuscation");
                    return None;
                }
            }
        }
    };

    // Log every incoming packet when a search is active.
    if active_search.is_some() {
        trace!(
            "Kad2 packet during search from {src}: {:?}",
            std::mem::discriminant(&pkt)
        );
    }

    match pkt {
        // ── Bootstrap request: share our routing table ─────────────────────
        KadPacket::BootstrapReq => {
            let rt = routing_table.read().await;
            let contacts: Vec<_> = rt.all_contacts().take(20).cloned().collect();
            let resp = encode_bootstrap_res(&our_id, cfg.tcp_port, &contacts);
            let _ = socket.send_to(&resp, src).await;
            debug!(
                "Replied BootstrapRes to {src} ({} contacts)",
                contacts.len()
            );
        }

        // ── Bootstrap response: populate routing table ─────────────────────
        KadPacket::BootstrapRes(res) => {
            let mut rt = routing_table.write().await;
            let mut added = 0usize;
            for c in res.contacts {
                if rt.insert(c) {
                    added += 1;
                }
            }
            debug!(
                "BootstrapRes from {src}: added {added} contacts, table={}",
                rt.len()
            );
        }

        // ── Hello request: respond with HelloRes + Ack ─────────────────────
        KadPacket::HelloReq(hello) => {
            if let Some(ip) = src_v4_ip {
                let contact = Contact {
                    id: hello.id,
                    ip,
                    udp_port: src.port(),
                    tcp_port: hello.tcp_port,
                    version: hello.version,
                    udp_key: sender_verify_key.filter(|&k| k != 0).or(hello.udp_key),
                };
                routing_table.write().await.insert(contact);
            }
            // Echo the requester's IP (as we see it) so it can learn its own
            // external address from our HelloRes.
            let resp = encode_hello_res(&our_id, cfg.tcp_port, src_v4_ip);
            let _ = socket.send_to(&resp, src).await;
            let ack = encode_hello_res_ack(&our_id);
            let _ = socket.send_to(&ack, src).await;
        }

        // ── Hello response: ack + insert into routing table ────────────────
        KadPacket::HelloRes(hello) => {
            let mut learned_ip = None;
            if let Some(ip) = src_v4_ip {
                if let Some(key) = hello.udp_key {
                    trace!(%src, udp_key = key, "Got UDPKey from HelloRes");
                }
                // Report the IP so the caller can tally votes across peers.
                if let Some(our_ip) = hello.sender_ip
                    && !our_ip.is_unspecified()
                {
                    learned_ip = Some(our_ip);
                }
                let contact = Contact {
                    id: hello.id,
                    ip,
                    udp_port: src.port(),
                    tcp_port: hello.tcp_port,
                    version: hello.version,
                    udp_key: sender_verify_key.filter(|&k| k != 0).or(hello.udp_key),
                };
                routing_table.write().await.insert_or_update_key(contact);
            }
            let ack = encode_hello_res_ack(&our_id);
            let _ = socket.send_to(&ack, src).await;
            if learned_ip.is_some() {
                return learned_ip;
            }
        }

        // ── Ping: respond with Pong ────────────────────────────────────────
        KadPacket::Ping => {
            let port = match src {
                SocketAddr::V4(a) => a.port(),
                SocketAddr::V6(a) => a.port(),
            };
            let resp = encode_pong(port);
            let _ = socket.send_to(&resp, src).await;
        }

        // ── Node lookup: return closest contacts ───────────────────────────
        KadPacket::Req(req) => {
            let rt = routing_table.read().await;
            let contacts = rt.closest_to(&req.target, 20);
            let resp = encode_res(&req.target, &contacts);
            let _ = socket.send_to(&resp, src).await;
        }

        // ── Node lookup response: update routing table + active search ──────
        KadPacket::Res(res) => {
            let mut rt = routing_table.write().await;
            for c in &res.contacts {
                rt.insert(c.clone());
            }
            drop(rt);

            if let (Some(s), SocketAddr::V4(sender)) = (active_search.as_mut(), src) {
                let pkts = s.on_res(&res, sender, cfg.alpha);
                send_out_packets(socket, pkts, our_udp_key).await;
            }
        }

        // ── Search result: collect sources or keyword hits ─────────────────
        KadPacket::SearchRes { raw } => {
            debug!(%src, raw_len = raw.len(), "Got SearchRes packet");
            if let Some(s) = active_search.as_mut() {
                s.on_search_res(&raw, src);
            }
        }

        // ── Publish result: count one acknowledged source store ────────────
        KadPacket::PublishRes { load, .. } => {
            if let Some(s) = active_search.as_mut() {
                s.on_publish_res(load);
            }
        }

        // ── Pong: learn our external IP ────────────────────────────────────
        KadPacket::Pong(_port) => {
            debug!("Pong from {src}");
            // The peer that sent the Pong knows our IP — we can learn it
            // indirectly from the socket's perspective when we get the Pong.
            if let Some(ip) = src_v4_ip {
                // We can't learn our own IP from a Pong directly; the port in
                // the Pong is our *external* UDP port. The external IP comes
                // from the socket's local address if it's not 0.0.0.0.
                let _ = ip; // used below for external IP learning
            }
        }

        // ── Firewall check response: our external IP as a peer sees us ─────
        // The most reliable external-IP source: real eMule sends this in reply
        // to our FIREWALLED_REQ regardless of whether its TCP callback succeeds.
        KadPacket::FirewalledRes { ip } if !ip.is_unspecified() => {
            debug!(%src, %ip, "Got FIREWALLED_RES — our external IP");
            return Some(ip);
        }

        // ── Firewall check request from a peer ─────────────────────────────
        // Be the checker: tell the peer its external IP and open a TCP
        // connection back to its advertised port so it can confirm it is
        // reachable (not firewalled). We only ever dial the UDP packet's source
        // IP — never an address from the payload — so we can't be turned into
        // an amplifier against a third party.
        KadPacket::FirewalledReq { tcp_port } => {
            if let Some(ip) = src_v4_ip {
                // Reply with the peer's IP (obfuscated with its key if we know it).
                let recv_key = if let SocketAddr::V4(v4) = src {
                    routing_table
                        .read()
                        .await
                        .find_by_addr(&v4)
                        .and_then(|c| c.udp_key)
                } else {
                    None
                };
                let res = encode_firewalled_res(ip);
                send_kad_pkt(socket, &res, src, recv_key, our_udp_key).await;

                // The TCP callback: a short, best-effort connect is enough — the
                // peer's listener counts the inbound connection on accept.
                if tcp_port != 0 {
                    let target = SocketAddr::V4(std::net::SocketAddrV4::new(ip, tcp_port));
                    tokio::spawn(async move {
                        let _ = timeout(
                            Duration::from_secs(5),
                            tokio::net::TcpStream::connect(target),
                        )
                        .await;
                    });
                    trace!(%ip, tcp_port, "Firewall-check callback: dialled peer's TCP port");
                }
            }
        }

        KadPacket::FirewalledAck => {
            trace!(%src, "Got FIREWALLED_ACK");
        }

        KadPacket::Unknown { opcode, payload } => {
            // Log PublishSourceReq (0x35) targets — useful for finding active hashes.
            if opcode == 0x35 && payload.len() >= 16 {
                let hash = hex::encode(&payload[..16]);
                debug!("Kad2 PUBLISH_SOURCE_REQ from {src} target={hash}");
            } else if matches!(opcode, 0x43..=0x48 | 0x4b) {
                // Normal publish/firewall-check opcodes sent by peers that have
                // us in their routing table — not implemented, not worth logging.
                trace!("Kad2 publish opcode 0x{opcode:02x} from {src}");
            } else {
                debug!("Unknown Kad2 opcode 0x{opcode:02x} from {src}");
            }
        }

        _ => {}
    }

    None
}

// ── Bootstrap (send-only) ───────────────────────────────────────────────────

/// Spawn [`bootstrap_sender`] as a detached task. The caller must have just won
/// the `bootstrap_active` guard via `compare_exchange(false -> true)`; the task
/// clears it on completion. Centralises the spawn used by both the `Bootstrap`
/// command and the keepalive's re-bootstrap-when-thin path.
#[allow(clippy::too_many_arguments)]
fn spawn_bootstrap_task(
    socket: &Arc<UdpSocket>,
    our_id: KadId,
    our_udp_key: u32,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    seeds: Vec<Contact>,
    reply: oneshot::Sender<usize>,
    bootstrap_active: &Arc<AtomicBool>,
) {
    tokio::spawn(bootstrap_sender(
        Arc::clone(socket),
        our_id,
        our_udp_key,
        cfg.clone(),
        Arc::clone(routing_table),
        seeds,
        reply,
        Arc::clone(bootstrap_active),
    ));
}

/// Drive a Kad2 bootstrap by *sending only*, run as a detached task.
///
/// It fires BOOTSTRAP_REQ / HELLO_REQ at the seeds, then sleeps between rounds
/// and re-reads the shared routing table — which the main loop fills via
/// [`handle_packet`] as the responses arrive — to query any newly discovered
/// contacts. It never calls `recv_from`: the task loop owns the socket's read
/// side and keeps servicing peers, keepalives, searches and commands while this
/// runs concurrently. Replies with the final contact count, and the `active`
/// flag is cleared on completion (including early returns / panics) via the
/// drop guard so a later bootstrap can run.
#[allow(clippy::too_many_arguments)]
async fn bootstrap_sender(
    socket: Arc<UdpSocket>,
    our_id: KadId,
    our_udp_key: u32,
    cfg: KadTaskConfig,
    routing_table: Arc<RwLock<RoutingTable>>,
    seeds: Vec<Contact>,
    reply: oneshot::Sender<usize>,
    active: Arc<AtomicBool>,
) {
    struct ClearOnDrop(Arc<AtomicBool>);
    impl Drop for ClearOnDrop {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }
    let _guard = ClearOnDrop(active);

    if seeds.is_empty() {
        let _ = reply.send(routing_table.read().await.len());
        return;
    }

    info!(seeds = seeds.len(), "Kad2 bootstrapping");
    const BOOTSTRAP_ROUNDS: usize = 5;
    const TARGET_CONTACTS: usize = 200;
    // Wait between rounds to let the loop ingest responses before we query the
    // newly discovered contacts. The loop processes each packet within
    // milliseconds, so this is just RTT slack — not a hard search deadline.
    const ROUND_WAIT: Duration = Duration::from_millis(1500);

    let boot = encode_bootstrap_req();
    let hello = encode_hello_req(&our_id, cfg.tcp_port);
    let mut already_queried: std::collections::HashSet<std::net::SocketAddrV4> =
        seeds.iter().map(|c| c.socket_addr_udp()).collect();

    // Round 0: BOOTSTRAP_REQ (plain — seeds accept it unobfuscated) + HELLO_REQ
    // to every seed.
    let mut sent = 0usize;
    for seed in &seeds {
        let addr = SocketAddr::V4(seed.socket_addr_udp());
        tracing::trace!(%addr, id = %seed.id, ver = seed.version, "Sending BOOTSTRAP_REQ to seed");
        if socket.send_to(&boot, addr).await.is_ok() {
            sent += 1;
        }
        let _ = socket.send_to(&hello, addr).await;
    }
    debug!(sent, "Sent BOOTSTRAP_REQ packets (round 0)");

    // Subsequent rounds: let the loop ingest the responses, then query any newly
    // discovered contacts we have not queried yet.
    for round in 0..BOOTSTRAP_ROUNDS {
        tokio::time::sleep(ROUND_WAIT).await;

        let count = routing_table.read().await.len();
        if count >= TARGET_CONTACTS {
            break;
        }
        let new_contacts: Vec<Contact> = {
            let rt = routing_table.read().await;
            rt.all_contacts()
                .filter(|c| !already_queried.contains(&c.socket_addr_udp()))
                .cloned()
                .collect()
        };
        if new_contacts.is_empty() {
            break;
        }
        debug!(
            new = new_contacts.len(),
            round = round + 1,
            "Starting next bootstrap round"
        );
        for c in &new_contacts {
            let addr = SocketAddr::V4(c.socket_addr_udp());
            already_queried.insert(c.socket_addr_udp());
            let _ = socket.send_to(&boot, addr).await;
            // HELLO_REQ: use the peer's known UDPKey if available, otherwise plain.
            send_kad_pkt(&socket, &hello, addr, c.udp_key, our_udp_key).await;
        }
    }

    // After bootstrap we only have contacts in low-indexed XOR buckets (far nodes).
    // Run an iterative find_node(our_id) to discover near-neighbourhood contacts.
    find_node_sender(&socket, our_id, our_udp_key, &cfg, &routing_table).await;

    // Ask a few contacts to connect back and report our external IP.
    send_firewall_checks(&socket, &cfg, &routing_table, our_udp_key).await;

    let count = routing_table.read().await.len();
    info!(contacts = count, "Kad2 bootstrap complete");
    let _ = reply.send(count);
}

// ── Post-bootstrap find_node (send-only) ─────────────────────────────────────

/// Iterative find-node lookup for `our_id` to fill near-neighbourhood k-buckets.
///
/// Bootstrap only populates low XOR-distance buckets (far nodes) because
/// BOOTSTRAP_RES returns arbitrary contacts.  Querying the closest known contacts
/// for nodes near our own ID iteratively discovers high-indexed buckets (nodes
/// XOR-close to us), which are essential for search convergence near our keyspace.
///
/// Send-only, like [`bootstrap_sender`]: it queries the closest known contacts,
/// sleeps to let the loop ingest the RES contacts (via [`handle_packet`]), and
/// repeats. It never reads the socket.
async fn find_node_sender(
    socket: &UdpSocket,
    our_id: KadId,
    our_udp_key: u32,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
) {
    const ROUNDS: usize = 3;
    const ROUND_WAIT: Duration = Duration::from_millis(1500);

    let req = encode_req(0, &our_id, &our_id);
    let mut queried: std::collections::HashSet<std::net::SocketAddrV4> = Default::default();

    for round in 0..ROUNDS {
        let candidates: Vec<Contact> = {
            let rt = routing_table.read().await;
            rt.closest_to(&our_id, cfg.alpha)
                .into_iter()
                .filter(|c| !queried.contains(&c.socket_addr_udp()))
                .collect()
        };
        if candidates.is_empty() {
            break;
        }
        for c in &candidates {
            let addr = SocketAddr::V4(c.socket_addr_udp());
            queried.insert(c.socket_addr_udp());
            send_kad_pkt(socket, &req, addr, c.udp_key, our_udp_key).await;
        }
        debug!(sent = candidates.len(), round, "Kad2 find_node round");
        tokio::time::sleep(ROUND_WAIT).await;
    }

    let count = routing_table.read().await.len();
    info!(contacts = count, "Kad2 find_node complete");
}

// ── Keep-alive ────────────────────────────────────────────────────────────────

/// Re-bootstrapping when the table is thin is handled by the task loop (see the
/// keepalive branch), which spawns it detached — so this only ever pings.
async fn send_keepalive(
    socket: &UdpSocket,
    our_id: KadId,
    our_udp_key: u32,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
) {
    let count = routing_table.read().await.len();

    // Normal keepalive: send HELLO_REQ to 3 randomly-selected contacts so that
    // we cycle through the whole table over time rather than always hitting the
    // same three entries at the front of bucket 0.
    let all: Vec<_> = routing_table.read().await.all_contacts().cloned().collect();
    let n = all.len();
    let hello = encode_hello_req(&our_id, cfg.tcp_port);
    if n > 0 {
        // Derive a pseudo-random offset from wall-clock nanoseconds — no
        // external dependency needed, and sufficient for non-security use.
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
            .hash(&mut h);
        let offset = h.finish() as usize;
        for i in 0..3usize.min(n) {
            // Stride of 31 (prime) to spread the three picks across the table.
            let c = &all[(offset + i * 31) % n];
            let addr = SocketAddr::V4(c.socket_addr_udp());
            send_kad_pkt(socket, &hello, addr, c.udp_key, our_udp_key).await;
        }
    }
    debug!(routing_table = count, "Kad2 keepalive sent");
}

// ── Search packet sender ──────────────────────────────────────────────────────

/// Dispatch a batch of packets produced by [`ActiveSearch`].
async fn send_out_packets(socket: &UdpSocket, packets: Vec<OutPacket>, our_udp_key: u32) {
    for p in packets {
        match p.obfusc {
            ObfuscMode::Plain => {
                let _ = socket.send_to(&p.bytes, p.addr).await;
            }
            ObfuscMode::RecvKey(key) => {
                send_kad_pkt(socket, &p.bytes, p.addr, Some(key), our_udp_key).await;
            }
            ObfuscMode::KadId(id) => {
                send_kad_pkt_kad_id(socket, &p.bytes, p.addr, &id, our_udp_key).await;
            }
        }
    }
}

// ── Re-export for convenience ──────────────────────────────────────────────────

pub use super::routing::parse_nodes_dat;

#[cfg(test)]
mod tests {
    use super::ExternalIpVotes;
    use std::net::Ipv4Addr;

    #[test]
    fn first_report_wins_immediately() {
        let mut v = ExternalIpVotes::default();
        let ip = Ipv4Addr::new(203, 0, 113, 5);
        assert_eq!(v.record(ip), Some(ip)); // shown right away
        assert_eq!(v.record(ip), None); // same leader, no change
    }

    #[test]
    fn majority_overtakes_a_minority_report() {
        let mut v = ExternalIpVotes::default();
        let wrong = Ipv4Addr::new(10, 0, 0, 1);
        let right = Ipv4Addr::new(203, 0, 113, 5);
        assert_eq!(v.record(wrong), Some(wrong)); // 1 vote → leads
        assert_eq!(v.record(right), None); // 1 vs 1, no strict majority yet
        assert_eq!(v.record(right), Some(right)); // 2 > 1 → new leader
    }

    #[test]
    fn unspecified_is_ignored() {
        let mut v = ExternalIpVotes::default();
        assert_eq!(v.record(Ipv4Addr::UNSPECIFIED), None);
    }
}
