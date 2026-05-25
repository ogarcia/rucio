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
    Contact, KadId, KadPacket, decode, encode_bootstrap_req, encode_bootstrap_res,
    encode_hello_req, encode_hello_res, encode_hello_res_ack, encode_pong, encode_req, encode_res,
    encode_search_source_req,
};
use super::routing::RoutingTable;
use crate::ed2k::Ed2kHash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::time::{Instant, timeout};
use tracing::{debug, info, trace, warn};

// ── Public API ────────────────────────────────────────────────────────────────

/// A source found by Kad2 search.
#[derive(Debug, Clone)]
pub struct KadSource {
    pub ip: std::net::Ipv4Addr,
    pub tcp_port: u16,
    pub udp_port: u16,
}

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
}

/// Handle to the running `KadTask`.  Cheap to clone.
#[derive(Clone)]
pub struct KadHandle {
    tx: mpsc::Sender<KadCommand>,
    routing_table: Arc<RwLock<RoutingTable>>,
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
    /// Maximum sources to collect per search.
    pub max_sources: usize,
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
    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    let our_udp_key = super::obfuscation::random_udp_key();

    tokio::spawn(run_task(socket, our_id, our_udp_key, cfg, rt_clone, cmd_rx));

    KadHandle {
        tx: cmd_tx,
        routing_table,
    }
}

// ── Task main loop ────────────────────────────────────────────────────────────

/// A pending search waiting for responses.
struct PendingSearch {
    target: KadId,
    file_size: u64,
    reply: oneshot::Sender<Vec<KadSource>>,
    sources: Vec<KadSource>,
    deadline: Instant,
    max_sources: usize,
    /// Contacts already queried (FIND_NODE + SEARCH_SOURCE_REQ sent).
    queried: std::collections::HashSet<std::net::SocketAddrV4>,
}

async fn run_task(
    socket: Arc<UdpSocket>,
    our_id: KadId,
    our_udp_key: u32,
    cfg: KadTaskConfig,
    routing_table: Arc<RwLock<RoutingTable>>,
    mut cmd_rx: mpsc::Receiver<KadCommand>,
) {
    let mut recv_buf = [0u8; 4096];
    let mut pending_search: Option<PendingSearch> = None;
    let mut keepalive_tick = Instant::now() + cfg.keepalive_interval;
    // Seeds from the last bootstrap call — reused for automatic re-bootstrap
    // when the routing table drops below min_contacts.
    let mut last_seeds: Vec<Contact> = Vec::new();
    // Our external IPv4 — seeded from config, then updated from peer responses.
    let mut our_external_ip = cfg.initial_external_ip;
    if !our_external_ip.is_unspecified() {
        info!(%our_external_ip, "Using configured external IP for Kad2 obfuscation");
    }

    info!(udp_key = our_udp_key, "Kad2 task started");

    loop {
        // Determine how long to wait for the next packet.
        let recv_deadline = if let Some(ref ps) = pending_search {
            ps.deadline
        } else {
            keepalive_tick
        };
        let remaining = recv_deadline.saturating_duration_since(Instant::now());

        tokio::select! {
            // ── Incoming UDP packet ────────────────────────────────────────
            result = timeout(remaining, socket.recv_from(&mut recv_buf)) => {
                match result {
                    Ok(Ok((n, src))) => {
                        if let Some(ip) = handle_packet(
                            &recv_buf[..n],
                            src,
                            &socket,
                            our_id,
                            our_udp_key,
                            our_external_ip,
                            &cfg,
                            &routing_table,
                            &mut pending_search,
                        ).await {
                            // A Pong told us our external IP.
                            if our_external_ip.is_unspecified() {
                                our_external_ip = ip;
                                debug!(%our_external_ip, "Learned our external IP from Pong");
                            }
                        }
                    }
                    Ok(Err(e)) => warn!("Kad2 recv error: {e}"),
                    Err(_) => {
                        // Timeout — either search deadline or keepalive tick.
                        if let Some(ps) = pending_search.take() {
                            if Instant::now() >= ps.deadline {
                                info!(
                                    sources = ps.sources.len(),
                                    queried = ps.queried.len(),
                                    target = %ps.target,
                                    "Kad2 search timed out"
                                );
                                let _ = ps.reply.send(ps.sources);
                            } else {
                                pending_search = Some(ps);
                            }
                        }
                        if Instant::now() >= keepalive_tick {
                            keepalive_tick = Instant::now() + cfg.keepalive_interval;
                            send_keepalive(&socket, our_id, our_udp_key, our_external_ip, &cfg, &routing_table, &last_seeds).await;
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
                        if our_external_ip.is_unspecified() && !learned_ip.is_unspecified() {
                            our_external_ip = learned_ip;
                        }
                        let _ = reply.send(count);
                    }
                    KadCommand::SearchSources { hash, file_size, reply } => {
                        if pending_search.is_some() {
                            // Already searching — queue is depth-1; just return empty for now.
                            warn!("Kad2 search already in progress, dropping new request");
                            let _ = reply.send(vec![]);
                            continue;
                        }
                        let target = KadId::from_bytes(*hash.as_bytes());
                        let deadline = Instant::now() + cfg.search_timeout;

                        // Get the initial candidates before moving into the struct.
                        let initial_candidates: Vec<_> = {
                            let rt = routing_table.read().await;
                            rt.closest_to(&target, cfg.alpha)
                        };

                        let mut queried = std::collections::HashSet::new();
                        let source_pkt = encode_search_source_req(&target, file_size);
                        let mut sent_obfuscated = 0usize;
                        let mut sent_plain = 0usize;
                        for c in &initial_candidates {
                            let addr = SocketAddr::V4(c.socket_addr_udp());
                            queried.insert(c.socket_addr_udp());
                            // REQ receiver field must be the peer's own KadID (not ours).
                            let lookup_pkt = encode_req(2, &target, &c.id);
                            trace!(
                                %addr,
                                contact_id = %c.id,
                                contact_ver = c.version,
                                has_udp_key = c.udp_key.is_some(),
                                "Sending REQ+SEARCH_SOURCE_REQ to initial candidate"
                            );
                            send_kad_pkt(&socket, &lookup_pkt, addr, c.udp_key, our_external_ip).await;
                            send_kad_pkt(&socket, &source_pkt, addr, c.udp_key, our_external_ip).await;
                            if !our_external_ip.is_unspecified() { sent_obfuscated += 1; } else { sent_plain += 1; }
                        }
                        debug!(
                            sent = initial_candidates.len(),
                            sent_obfuscated,
                            sent_plain,
                            target = %target,
                            "Started Kad2 source search"
                        );

                        pending_search = Some(PendingSearch {
                            target,
                            file_size,
                            reply,
                            sources: vec![],
                            deadline,
                            max_sources: cfg.max_sources,
                            queried,
                        });
                    }
                    KadCommand::Status { reply } => {
                        let count = routing_table.read().await.len();
                        let _ = reply.send(count);
                    }
                }
            }

            else => break,
        }

        // Check if an active search has finished.
        let done = pending_search
            .as_ref()
            .map(|ps| ps.sources.len() >= ps.max_sources || Instant::now() >= ps.deadline)
            .unwrap_or(false);
        if done && let Some(ps) = pending_search.take() {
            debug!(sources = ps.sources.len(), "Kad2 search complete");
            let _ = ps.reply.send(ps.sources);
        }
    }

    info!("Kad2 task stopped");
}

// ── Packet handler ────────────────────────────────────────────────────────────

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
        let seed: [u8; 4] = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            std::time::SystemTime::now().hash(&mut h);
            (h.finish() as u32).to_le_bytes()
        };
        let obfuscated = super::obfuscation::obfuscate(plain, key, our_ip, seed);
        let _ = socket.send_to(&obfuscated, addr).await;
        return true;
    }
    let _ = socket.send_to(plain, addr).await;
    false
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
    pending_search: &mut Option<PendingSearch>,
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
                    if let Some(plain) = super::obfuscation::deobfuscate(data, our_udp_key, ip) {
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
    if pending_search.is_some() {
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
            let resp = encode_hello_res(&our_id, cfg.tcp_port);
            let _ = socket.send_to(&resp, src).await;
            let ack = encode_hello_res_ack();
            let _ = socket.send_to(&ack, src).await;
        }

        // ── Hello response: ack + insert into routing table ────────────────
        KadPacket::HelloRes(hello) => {
            if let Some(ip) = src_v4_ip {
                if let Some(key) = hello.udp_key {
                    trace!(%src, udp_key = key, "Got UDPKey from HelloRes");
                }
                if let Some(our_ip) = hello.sender_ip
                    && !our_ip.is_unspecified()
                    && our_external_ip.is_unspecified()
                {
                    debug!(%our_ip, "Learned our external IP from HelloRes TAG_SENDER_IP");
                    return Some(our_ip);
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
            let ack = encode_hello_res_ack();
            let _ = socket.send_to(&ack, src).await;
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

        // ── Node lookup response: update routing table + pending search ─────
        KadPacket::Res(res) => {
            let mut rt = routing_table.write().await;
            for c in &res.contacts {
                rt.insert(c.clone());
            }
            drop(rt);

            // If a search is pending, query newly discovered nodes that we
            // haven't contacted yet.
            if let Some(ps) = pending_search.as_mut() {
                let target = ps.target;
                let file_size = ps.file_size;
                let source_pkt = encode_search_source_req(&target, file_size);
                let new_contacts: Vec<_> = res
                    .contacts
                    .iter()
                    .filter(|c| !ps.queried.contains(&c.socket_addr_udp()))
                    .take(cfg.alpha)
                    .cloned()
                    .collect();
                debug!(
                    from = %src,
                    total_in_res = res.contacts.len(),
                    new_to_query = new_contacts.len(),
                    "Kad2 Res received during search"
                );
                for contact in new_contacts {
                    let addr = SocketAddr::V4(contact.socket_addr_udp());
                    // REQ receiver field must be the peer's own KadID (not ours).
                    let lookup_pkt = encode_req(2, &target, &contact.id);
                    trace!(%addr, has_key = contact.udp_key.is_some(), "Querying new contact from Res");
                    ps.queried.insert(contact.socket_addr_udp());
                    send_kad_pkt(socket, &lookup_pkt, addr, contact.udp_key, our_external_ip).await;
                    send_kad_pkt(socket, &source_pkt, addr, contact.udp_key, our_external_ip).await;
                }
            }
        }

        // ── Search result: collect sources for active search ───────────────
        KadPacket::SearchRes(res) => {
            if let Some(ps) = pending_search.as_mut()
                && res.target == ps.target
            {
                for s in res.sources {
                    if ps.sources.len() < ps.max_sources
                        && s.tcp_port != 0
                        && !s.ip.is_unspecified()
                    {
                        ps.sources.push(KadSource {
                            ip: s.ip,
                            tcp_port: s.tcp_port,
                            udp_port: s.udp_port,
                        });
                    }
                }
                debug!(sources = ps.sources.len(), "Accumulated Kad2 sources");
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

        KadPacket::Unknown { opcode, .. } => {
            debug!("Unknown Kad2 opcode 0x{opcode:02x} from {src}");
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
        // Also send HELLO_REQ plain so seeds that accept plain can reply
        // with HelloRes containing TAG_SENDER_IP (our external IP).
        let _ = socket.send_to(&hello0, addr).await;
        // Also send HELLO_REQ plain so seeds that accept plain can reply
        // with HelloRes containing TAG_SENDER_IP (our external IP).
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
                                if let Some(plain) =
                                    super::obfuscation::deobfuscate(raw, our_udp_key, *a.ip())
                                {
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
                            let ack = encode_hello_res_ack();
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

    let count = routing_table.read().await.len();
    info!(contacts = count, "Kad2 bootstrap complete");
    (count, our_external_ip)
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

    // Normal keepalive: send HELLO_REQ to a few random contacts.
    let contacts: Vec<_> = routing_table
        .read()
        .await
        .all_contacts()
        .take(3)
        .cloned()
        .collect();
    let hello = encode_hello_req(&our_id, cfg.tcp_port, our_udp_key);
    for c in contacts {
        let addr = SocketAddr::V4(c.socket_addr_udp());
        send_kad_pkt(socket, &hello, addr, c.udp_key, our_external_ip).await;
    }
    debug!(routing_table = count, "Kad2 keepalive sent");
}

// ── Search helper for source search phase ─────────────────────────────────────

/// Send SEARCH_SOURCE_REQ to the closest known contacts for a target.
pub async fn send_search_source_reqs(
    socket: &UdpSocket,
    target: KadId,
    file_size: u64,
    routing_table: &Arc<RwLock<RoutingTable>>,
) {
    let rt = routing_table.read().await;
    let contacts = rt.closest_to(&target, 10);
    drop(rt);

    let pkt = encode_search_source_req(&target, file_size);
    for c in contacts {
        let addr = SocketAddr::V4(c.socket_addr_udp());
        let _ = socket.send_to(&pkt, addr).await;
    }
}

// ── Re-export for convenience ──────────────────────────────────────────────────

pub use super::routing::parse_nodes_dat;
