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
//!   - [`KadCommand::Status`]         — return current routing-table size
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
use std::sync::atomic::{AtomicU32, Ordering};
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
    /// Return current routing-table contact count.
    Status { reply: oneshot::Sender<usize> },
    /// Keyword search — returns up to N results with file name, hash, size.
    SearchKeyword {
        keyword: String,
        reply: oneshot::Sender<Vec<KeywordHit>>,
    },
}

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

    /// Number of contacts currently in the routing table.
    pub async fn contact_count(&self) -> usize {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(KadCommand::Status { reply: tx }).await;
        rx.await.unwrap_or(0)
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
    if !our_external_ip.is_unspecified() {
        info!(%our_external_ip, "Using configured external IP for Kad2 obfuscation");
    }

    info!(udp_key = our_udp_key, "Kad2 task started");

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
                            our_external_ip,
                            &cfg,
                            &routing_table,
                            &mut active_search,
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
                            send_keepalive(&socket, our_id, our_udp_key, our_external_ip, &cfg, &routing_table, &last_seeds).await;
                        }
                        // Probe for callbacks: aggressively until we know our IP,
                        // then periodically to keep the connectivity verdict fresh.
                        let fw_due = Instant::now() >= fw_check_tick;
                        if our_external_ip.is_unspecified() || fw_due {
                            if fw_due {
                                fw_check_tick = Instant::now() + FW_CHECK_INTERVAL;
                            }
                            send_firewall_checks(&socket, &cfg, &routing_table, our_external_ip).await;
                        }
                    }
                }
            }

            // ── Command from daemon ────────────────────────────────────────
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    KadCommand::Bootstrap { seeds, reply } => {
                        last_seeds = seeds.clone();
                        let (count, learned_ip) = do_bootstrap(
                            &socket, our_id, our_udp_key, our_external_ip, &cfg, &routing_table, seeds
                        ).await;
                        if let Some(ip) = ip_votes.record(learned_ip) {
                            our_external_ip = ip;
                            info!(external_ip = %ip, "External IP learned during Kad bootstrap");
                        }
                        // Ask a few contacts to connect back and report our IP.
                        send_firewall_checks(&socket, &cfg, &routing_table, our_external_ip).await;
                        let _ = reply.send(count);
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
                        send_out_packets(&socket, pkts, our_external_ip).await;
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
                        send_out_packets(&socket, pkts, our_external_ip).await;
                        active_search = Some(search);
                    }
                    KadCommand::Status { reply } => {
                        let count = routing_table.read().await.len();
                        let _ = reply.send(count);
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
/// Prefers contacts that advertised a UDP key: they have completed a handshake
/// with us, so they know our key and can obfuscate the `FIREWALLED_RES` reply
/// back to us. Cheap and idempotent — safe to call periodically.
async fn send_firewall_checks(
    socket: &UdpSocket,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    our_external_ip: std::net::Ipv4Addr,
) {
    let contacts: Vec<Contact> = {
        let rt = routing_table.read().await;
        rt.all_contacts()
            .filter(|c| c.udp_key.is_some())
            .take(4)
            .cloned()
            .collect()
    };
    if contacts.is_empty() {
        return;
    }
    let req = encode_firewalled_req(cfg.tcp_port);
    for c in &contacts {
        let addr = SocketAddr::V4(c.socket_addr_udp());
        send_kad_pkt(socket, &req, addr, c.udp_key, our_external_ip).await;
    }
    debug!(probes = contacts.len(), "Sent Kad firewall-check requests");
}

/// Send a Kad2 packet, obfuscating it if `recv_key` is known.
/// Falls back to plain if our external IP is not yet known (can't obfuscate).
/// Returns `true` if the packet was sent obfuscated, `false` if sent plain (or failed).
async fn send_kad_pkt(
    socket: &UdpSocket,
    plain: &[u8],
    addr: SocketAddr,
    recv_key: Option<u32>,
    our_ip: std::net::Ipv4Addr,
) -> bool {
    if let Some(key) = recv_key
        && !our_ip.is_unspecified()
    {
        let rkp = super::obfuscation::random_key_part();
        let obfuscated = super::obfuscation::obfuscate_recv_key(plain, key, rkp);
        let _ = socket.send_to(&obfuscated, addr).await;
        return true;
    }
    let _ = socket.send_to(plain, addr).await;
    false
}

/// Send a Kad2 packet obfuscated with the KadID scheme (eMule v6+).
/// Used when we have the peer's KadID but not their announced UDPKey.
async fn send_kad_pkt_kad_id(
    socket: &UdpSocket,
    plain: &[u8],
    addr: SocketAddr,
    kad_id: &[u8; 16],
    our_ip: std::net::Ipv4Addr,
) -> bool {
    if our_ip.is_unspecified() {
        let _ = socket.send_to(plain, addr).await;
        return false;
    }
    let rkp = super::obfuscation::random_key_part();
    let obfuscated = super::obfuscation::obfuscate_kad_id(plain, kad_id, rkp);
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
    our_external_ip: std::net::Ipv4Addr,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    active_search: &mut Option<ActiveSearch>,
) -> Option<std::net::Ipv4Addr> {
    // Try plain decode first; if that fails and the packet doesn't start with
    // 0xe4/0xe5, attempt to deobfuscate using our UDPKey.
    let src_v4_ip = match src {
        SocketAddr::V4(a) => Some(*a.ip()),
        SocketAddr::V6(_) => None,
    };
    let pkt = {
        let plain_result = decode(data);
        match plain_result {
            Ok(p) => p,
            Err(_) => {
                // Try deobfuscation if we have sender's IPv4.
                if let Some(ip) = src_v4_ip {
                    if let Some(plain) = super::obfuscation::deobfuscate(
                        data,
                        our_udp_key,
                        ip,
                        Some(our_id.as_bytes()),
                    ) {
                        match decode(&plain) {
                            Ok(p) => {
                                trace!("Kad2 deobfuscated packet from {src}");
                                p
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
                    udp_key: hello.udp_key,
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
                    udp_key: hello.udp_key,
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
                send_out_packets(socket, pkts, our_external_ip).await;
            }
        }

        // ── Search result: collect sources or keyword hits ─────────────────
        KadPacket::SearchRes { raw } => {
            debug!(%src, raw_len = raw.len(), "Got SearchRes packet");
            if let Some(s) = active_search.as_mut() {
                s.on_search_res(&raw, src);
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
                send_kad_pkt(socket, &res, src, recv_key, our_external_ip).await;

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

// ── Bootstrap helper ──────────────────────────────────────────────────────────

async fn do_bootstrap(
    socket: &UdpSocket,
    our_id: KadId,
    our_udp_key: u32,
    our_external_ip: std::net::Ipv4Addr,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    seeds: Vec<Contact>,
) -> (usize, std::net::Ipv4Addr) {
    if seeds.is_empty() {
        return (routing_table.read().await.len(), our_external_ip);
    }

    info!(seeds = seeds.len(), "Kad2 bootstrapping");
    let mut our_external_ip = our_external_ip; // make locally mutable

    // ── Round 0: send BOOTSTRAP_REQ + HELLO_REQ to all seeds ─────────────
    let mut recv_buf = [0u8; 4096];
    let pkt = encode_bootstrap_req();
    let hello0 = encode_hello_req(&our_id, cfg.tcp_port, our_udp_key);
    let mut sent = 0usize;
    for seed in &seeds {
        let addr = SocketAddr::V4(seed.socket_addr_udp());
        tracing::trace!(%addr, id = %seed.id, ver = seed.version, "Sending BOOTSTRAP_REQ to seed");
        // Send BOOTSTRAP_REQ plain — seeds accept it without obfuscation.
        if socket.send_to(&pkt, addr).await.is_ok() {
            sent += 1;
        }
        // Also send HELLO_REQ so seeds can reply with HelloRes.
        let _ = socket.send_to(&hello0, addr).await;
    }
    debug!(sent, "Sent BOOTSTRAP_REQ packets (round 0)");

    // Collect responses; also send BOOTSTRAP_REQ to newly discovered contacts
    // for up to `BOOTSTRAP_ROUNDS` additional rounds.
    const BOOTSTRAP_ROUNDS: usize = 5;
    const TARGET_CONTACTS: usize = 200;
    // Maximum wall-clock time per round (hard cap).
    const ROUND_DEADLINE_SECS: u64 = 5;
    // If no packet arrives within this window, consider the round done early.
    const ROUND_IDLE_SECS: u64 = 1;
    let mut already_queried: std::collections::HashSet<std::net::SocketAddrV4> =
        seeds.iter().map(|c| c.socket_addr_udp()).collect();

    for round in 0..BOOTSTRAP_ROUNDS {
        let round_deadline = Instant::now() + Duration::from_secs(ROUND_DEADLINE_SECS);
        let mut idle_deadline = Instant::now() + Duration::from_secs(ROUND_IDLE_SECS);
        loop {
            let remaining = round_deadline
                .min(idle_deadline)
                .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, socket.recv_from(&mut recv_buf)).await {
                Ok(Ok((n, src))) => {
                    // Reset the idle timer — we are still receiving responses.
                    idle_deadline = Instant::now() + Duration::from_secs(ROUND_IDLE_SECS);
                    let raw = &recv_buf[..n];
                    // Try plain decode; if that fails, attempt deobfuscation.
                    let decoded = {
                        let r = decode(raw);
                        if r.is_err() {
                            if let SocketAddr::V4(a) = src {
                                if let Some(plain) = super::obfuscation::deobfuscate(
                                    raw,
                                    our_udp_key,
                                    *a.ip(),
                                    Some(our_id.as_bytes()),
                                ) {
                                    trace!(%src, plain_hex = %hex::encode(&plain[..plain.len().min(32)]), "Deobfuscated bootstrap packet");
                                    decode(&plain)
                                } else {
                                    r
                                }
                            } else {
                                r
                            }
                        } else {
                            r
                        }
                    };
                    match decoded {
                        Ok(KadPacket::BootstrapRes(res)) => {
                            let mut rt = routing_table.write().await;
                            for c in res.contacts {
                                rt.insert(c);
                            }
                            debug!(
                                "BootstrapRes from {src} (round {round}), table={}",
                                rt.len()
                            );
                        }
                        Ok(KadPacket::HelloReq(hello)) | Ok(KadPacket::HelloRes(hello)) => {
                            if let SocketAddr::V4(addr) = src {
                                trace!(
                                    %src,
                                    id = %hello.id,
                                    ver = hello.version,
                                    tag_count = hello.tag_count,
                                    udp_key = ?hello.udp_key,
                                    sender_ip = ?hello.sender_ip,
                                    "Bootstrap HelloReq/Res"
                                );
                                // Learn our external IP from TAG_SENDER_IP in HelloRes.
                                if our_external_ip.is_unspecified()
                                    && let Some(our_ip) = hello.sender_ip
                                    && !our_ip.is_unspecified()
                                {
                                    our_external_ip = our_ip;
                                    debug!(%our_external_ip, "Learned external IP from bootstrap HelloRes");
                                }
                                let contact = Contact {
                                    id: hello.id,
                                    ip: *addr.ip(),
                                    udp_port: addr.port(),
                                    tcp_port: hello.tcp_port,
                                    version: hello.version,
                                    udp_key: hello.udp_key,
                                };
                                routing_table.write().await.insert_or_update_key(contact);
                            }
                            let ack = encode_hello_res_ack(&our_id);
                            let _ = socket.send_to(&ack, src).await;
                        }
                        Ok(other) => {
                            tracing::trace!("bootstrap: unexpected packet from {src}: {other:?}");
                        }
                        Err(e) => {
                            tracing::trace!(
                                "bootstrap: unrecognised packet from {src}: {e:?} bytes={}",
                                n
                            );
                        }
                    }
                }
                Ok(Err(e)) => warn!("bootstrap recv error: {e}"),
                Err(_) => break, // timeout (idle or hard deadline)
            }
        }

        let count = routing_table.read().await.len();
        if count >= TARGET_CONTACTS {
            break;
        }

        // Send BOOTSTRAP_REQ + HELLO_REQ to newly discovered contacts.
        let new_contacts: Vec<_> = {
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
        let hello = encode_hello_req(&our_id, cfg.tcp_port, our_udp_key);
        for c in &new_contacts {
            let addr = SocketAddr::V4(c.socket_addr_udp());
            already_queried.insert(c.socket_addr_udp());
            let _ = socket.send_to(&pkt, addr).await;
            // Send HELLO_REQ: use the peer's known UDPKey if available, otherwise plain.
            // (key_from_kad_id doesn't work in practice — peers don't implement it)
            send_kad_pkt(socket, &hello, addr, c.udp_key, our_external_ip).await;
        }
    }

    // After bootstrap we only have contacts in low-indexed XOR buckets (far nodes).
    // Run an iterative find_node(our_id) to discover near-neighbourhood contacts.
    do_find_node(
        socket,
        our_id,
        our_udp_key,
        our_external_ip,
        cfg,
        routing_table,
    )
    .await;

    let count = routing_table.read().await.len();
    info!(contacts = count, "Kad2 bootstrap complete");
    (count, our_external_ip)
}

// ── Post-bootstrap find_node ──────────────────────────────────────────────────

/// Iterative find-node lookup for `our_id` to fill near-neighbourhood k-buckets.
///
/// Bootstrap only populates low XOR-distance buckets (far nodes) because
/// BOOTSTRAP_RES returns arbitrary contacts.  Querying the closest known contacts
/// for nodes near our own ID iteratively discovers high-indexed buckets (nodes
/// XOR-close to us), which are essential for search convergence near our keyspace.
async fn do_find_node(
    socket: &UdpSocket,
    our_id: KadId,
    our_udp_key: u32,
    our_external_ip: std::net::Ipv4Addr,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
) {
    const ROUNDS: usize = 3;
    const ROUND_SECS: u64 = 4;
    const IDLE_SECS: u64 = 1;

    let req = encode_req(0, &our_id, &our_id);
    let mut recv_buf = [0u8; 4096];
    let mut queried: std::collections::HashSet<std::net::SocketAddrV4> = Default::default();

    for round in 0..ROUNDS {
        let candidates: Vec<_> = {
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
            send_kad_pkt(socket, &req, addr, c.udp_key, our_external_ip).await;
        }
        debug!(sent = candidates.len(), round, "Kad2 find_node round");

        let deadline = Instant::now() + Duration::from_secs(ROUND_SECS);
        let mut idle = Instant::now() + Duration::from_secs(IDLE_SECS);
        loop {
            let remaining = deadline.min(idle).saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, socket.recv_from(&mut recv_buf)).await {
                Ok(Ok((n, src))) => {
                    idle = Instant::now() + Duration::from_secs(IDLE_SECS);
                    let raw = &recv_buf[..n];
                    let decoded = match decode(raw) {
                        Ok(p) => Ok(p),
                        Err(_) => {
                            if let SocketAddr::V4(a) = src {
                                if let Some(plain) = super::obfuscation::deobfuscate(
                                    raw,
                                    our_udp_key,
                                    *a.ip(),
                                    Some(our_id.as_bytes()),
                                ) {
                                    decode(&plain)
                                } else {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                        }
                    };
                    match decoded {
                        Ok(KadPacket::Res(res)) => {
                            let mut rt = routing_table.write().await;
                            for c in res.contacts {
                                rt.insert(c);
                            }
                        }
                        Ok(KadPacket::BootstrapRes(res)) => {
                            let mut rt = routing_table.write().await;
                            for c in res.contacts {
                                rt.insert(c);
                            }
                        }
                        _ => {}
                    }
                }
                _ => break,
            }
        }

        let count = routing_table.read().await.len();
        debug!(contacts = count, round, "Kad2 find_node round complete");
    }

    let count = routing_table.read().await.len();
    info!(contacts = count, "Kad2 find_node complete");
}

// ── Keep-alive ────────────────────────────────────────────────────────────────

async fn send_keepalive(
    socket: &UdpSocket,
    our_id: KadId,
    our_udp_key: u32,
    our_external_ip: std::net::Ipv4Addr,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    last_seeds: &[Contact],
) {
    let count = routing_table.read().await.len();

    if count < cfg.min_contacts {
        // Routing table is too small — re-bootstrap instead of just pinging.
        if last_seeds.is_empty() {
            debug!("Kad2 keepalive: routing table low ({count}) but no seeds available, skipping");
            return;
        }
        info!(
            contacts = count,
            min = cfg.min_contacts,
            seeds = last_seeds.len(),
            "Kad2 routing table low — re-bootstrapping"
        );
        do_bootstrap(
            socket,
            our_id,
            our_udp_key,
            our_external_ip,
            cfg,
            routing_table,
            last_seeds.to_vec(),
        )
        .await;
        return;
    }

    // Normal keepalive: send HELLO_REQ to 3 randomly-selected contacts so that
    // we cycle through the whole table over time rather than always hitting the
    // same three entries at the front of bucket 0.
    let all: Vec<_> = routing_table.read().await.all_contacts().cloned().collect();
    let n = all.len();
    let hello = encode_hello_req(&our_id, cfg.tcp_port, our_udp_key);
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
            send_kad_pkt(socket, &hello, addr, c.udp_key, our_external_ip).await;
        }
    }
    debug!(routing_table = count, "Kad2 keepalive sent");
}

// ── Search packet sender ──────────────────────────────────────────────────────

/// Dispatch a batch of packets produced by [`ActiveSearch`].
async fn send_out_packets(
    socket: &UdpSocket,
    packets: Vec<OutPacket>,
    our_external_ip: std::net::Ipv4Addr,
) {
    for p in packets {
        match p.obfusc {
            ObfuscMode::Plain => {
                let _ = socket.send_to(&p.bytes, p.addr).await;
            }
            ObfuscMode::RecvKey(key) => {
                send_kad_pkt(socket, &p.bytes, p.addr, Some(key), our_external_ip).await;
            }
            ObfuscMode::KadId(id) => {
                send_kad_pkt_kad_id(socket, &p.bytes, p.addr, &id, our_external_ip).await;
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
