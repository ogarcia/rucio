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
use tracing::{debug, info, warn};

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
}

impl Default for KadTaskConfig {
    fn default() -> Self {
        Self {
            tcp_port: 4662,
            request_timeout: Duration::from_secs(5),
            search_timeout: Duration::from_secs(60),
            max_sources: 50,
            alpha: 3,
            lookup_iterations: 5,
            min_contacts: 4,
            keepalive_interval: Duration::from_secs(60),
        }
    }
}

/// Spawn the Kad2 background task and return a handle.
pub fn spawn(socket: Arc<UdpSocket>, our_id: KadId, cfg: KadTaskConfig) -> KadHandle {
    let routing_table = Arc::new(RwLock::new(RoutingTable::new(our_id)));
    let rt_clone = Arc::clone(&routing_table);
    let (cmd_tx, cmd_rx) = mpsc::channel(64);

    tokio::spawn(run_task(socket, our_id, cfg, rt_clone, cmd_rx));

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
}

async fn run_task(
    socket: Arc<UdpSocket>,
    our_id: KadId,
    cfg: KadTaskConfig,
    routing_table: Arc<RwLock<RoutingTable>>,
    mut cmd_rx: mpsc::Receiver<KadCommand>,
) {
    let mut recv_buf = [0u8; 4096];
    let mut pending_search: Option<PendingSearch> = None;
    let mut keepalive_tick = Instant::now() + cfg.keepalive_interval;

    info!("Kad2 task started");

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
                        handle_packet(
                            &recv_buf[..n],
                            src,
                            &socket,
                            our_id,
                            &cfg,
                            &routing_table,
                            &mut pending_search,
                        ).await;
                    }
                    Ok(Err(e)) => warn!("Kad2 recv error: {e}"),
                    Err(_) => {
                        // Timeout — either search deadline or keepalive tick.
                        if let Some(ps) = pending_search.take() {
                            if Instant::now() >= ps.deadline {
                                debug!(sources = ps.sources.len(), "Kad2 search timed out, returning results");
                                let _ = ps.reply.send(ps.sources);
                            } else {
                                pending_search = Some(ps);
                            }
                        }
                        if Instant::now() >= keepalive_tick {
                            keepalive_tick = Instant::now() + cfg.keepalive_interval;
                            send_keepalive(&socket, our_id, &cfg, &routing_table).await;
                        }
                    }
                }
            }

            // ── Command from daemon ────────────────────────────────────────
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    KadCommand::Bootstrap { seeds, reply } => {
                        let count = do_bootstrap(
                            &socket, our_id, &cfg, &routing_table, seeds
                        ).await;
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
                        // Start iterative lookup.
                        start_lookup(
                            &socket, our_id, &cfg, &routing_table, target
                        ).await;
                        pending_search = Some(PendingSearch {
                            target,
                            file_size,
                            reply,
                            sources: vec![],
                            deadline,
                            max_sources: cfg.max_sources,
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

async fn handle_packet(
    data: &[u8],
    src: SocketAddr,
    socket: &UdpSocket,
    our_id: KadId,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    pending_search: &mut Option<PendingSearch>,
) {
    let pkt = match decode(data) {
        Ok(p) => p,
        Err(e) => {
            debug!("Kad2 decode error from {src}: {e}");
            return;
        }
    };

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
            // Insert the sender into our routing table.
            let src_v4 = match src {
                SocketAddr::V4(a) => Some(a),
                SocketAddr::V6(_) => None,
            };
            if let Some(addr) = src_v4 {
                let contact = Contact {
                    id: hello.id,
                    ip: *addr.ip(),
                    udp_port: addr.port(),
                    tcp_port: hello.tcp_port,
                    version: hello.version,
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
            let src_v4 = match src {
                SocketAddr::V4(a) => Some(a),
                SocketAddr::V6(_) => None,
            };
            if let Some(addr) = src_v4 {
                let contact = Contact {
                    id: hello.id,
                    ip: *addr.ip(),
                    udp_port: addr.port(),
                    tcp_port: hello.tcp_port,
                    version: hello.version,
                };
                routing_table.write().await.insert(contact);
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
            // If a search is pending, query the newly discovered nodes and
            // send SEARCH_SOURCE_REQ to them right away.
            if let Some(ps) = pending_search.as_ref() {
                let target = ps.target;
                let file_size = ps.file_size;
                let candidates: Vec<_> = res
                    .contacts
                    .into_iter()
                    .filter(|c| rt.closest_to(&target, 20).iter().any(|k| k.id == c.id))
                    .take(cfg.alpha)
                    .collect();
                drop(rt);
                let lookup_pkt = encode_req(2, &target, &our_id);
                let source_pkt = encode_search_source_req(&target, file_size);
                for contact in candidates {
                    let addr = SocketAddr::V4(contact.socket_addr_udp());
                    let _ = socket.send_to(&lookup_pkt, addr).await;
                    let _ = socket.send_to(&source_pkt, addr).await;
                }
            } else {
                drop(rt);
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

        // ── Pong: note that peer is alive ──────────────────────────────────
        KadPacket::Pong(_port) => {
            debug!("Pong from {src}");
        }

        KadPacket::Unknown { opcode, .. } => {
            debug!("Unknown Kad2 opcode 0x{opcode:02x} from {src}");
        }

        _ => {}
    }
}

// ── Bootstrap helper ──────────────────────────────────────────────────────────

async fn do_bootstrap(
    socket: &UdpSocket,
    our_id: KadId,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    seeds: Vec<Contact>,
) -> usize {
    if seeds.is_empty() {
        return routing_table.read().await.len();
    }

    info!(seeds = seeds.len(), "Kad2 bootstrapping");

    // ── Round 0: send BOOTSTRAP_REQ to all seeds ──────────────────────────
    let mut recv_buf = [0u8; 4096];
    let pkt = encode_bootstrap_req();
    let mut sent = 0usize;
    for seed in &seeds {
        let addr = SocketAddr::V4(seed.socket_addr_udp());
        tracing::trace!(%addr, id = %seed.id, ver = seed.version, "Sending BOOTSTRAP_REQ to seed");
        if socket.send_to(&pkt, addr).await.is_ok() {
            sent += 1;
        }
    }
    debug!(sent, "Sent BOOTSTRAP_REQ packets (round 0)");

    // Collect responses; also send BOOTSTRAP_REQ to newly discovered contacts
    // for up to `BOOTSTRAP_ROUNDS` additional rounds.
    const BOOTSTRAP_ROUNDS: usize = 3;
    const TARGET_CONTACTS: usize = 50;
    let mut already_queried: std::collections::HashSet<std::net::SocketAddrV4> =
        seeds.iter().map(|c| c.socket_addr_udp()).collect();

    for round in 0..BOOTSTRAP_ROUNDS {
        let deadline = Instant::now() + cfg.request_timeout * 2;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, socket.recv_from(&mut recv_buf)).await {
                Ok(Ok((n, src))) => match decode(&recv_buf[..n]) {
                    Ok(KadPacket::BootstrapRes(res)) => {
                        let mut rt = routing_table.write().await;
                        let before = rt.len();
                        for c in res.contacts {
                            rt.insert(c);
                        }
                        debug!(
                            "BootstrapRes from {src} (round {round}), table={}",
                            rt.len()
                        );
                        let _ = before;
                    }
                    Ok(KadPacket::HelloReq(hello)) | Ok(KadPacket::HelloRes(hello)) => {
                        if let SocketAddr::V4(addr) = src {
                            let contact = Contact {
                                id: hello.id,
                                ip: *addr.ip(),
                                udp_port: addr.port(),
                                tcp_port: hello.tcp_port,
                                version: hello.version,
                            };
                            routing_table.write().await.insert(contact);
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
                },
                Ok(Err(e)) => warn!("bootstrap recv error: {e}"),
                Err(_) => break,
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
        let hello = encode_hello_req(&our_id, cfg.tcp_port);
        for c in &new_contacts {
            let addr = SocketAddr::V4(c.socket_addr_udp());
            already_queried.insert(c.socket_addr_udp());
            let _ = socket.send_to(&pkt, addr).await;
            let _ = socket.send_to(&hello, addr).await;
        }
    }

    let count = routing_table.read().await.len();
    info!(contacts = count, "Kad2 bootstrap complete");
    count
}

// ── Iterative lookup helper ───────────────────────────────────────────────────

async fn start_lookup(
    socket: &UdpSocket,
    our_id: KadId,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
    target: KadId,
) {
    let rt = routing_table.read().await;
    let candidates = rt.closest_to(&target, cfg.alpha);
    drop(rt);

    let pkt = encode_req(2, &target, &our_id);
    for c in candidates {
        let addr = SocketAddr::V4(c.socket_addr_udp());
        let _ = socket.send_to(&pkt, addr).await;
    }
}

// ── Keep-alive ────────────────────────────────────────────────────────────────

async fn send_keepalive(
    socket: &UdpSocket,
    our_id: KadId,
    cfg: &KadTaskConfig,
    routing_table: &Arc<RwLock<RoutingTable>>,
) {
    let rt = routing_table.read().await;
    let count = rt.len();

    if count == 0 {
        debug!("Kad2 keepalive skipped: routing table empty");
        return;
    }

    // Send HELLO_REQ to a few random contacts to keep them updated.
    let contacts: Vec<_> = rt.all_contacts().take(3).cloned().collect();
    drop(rt);

    let hello = encode_hello_req(&our_id, cfg.tcp_port);
    for c in contacts {
        let addr = SocketAddr::V4(c.socket_addr_udp());
        let _ = socket.send_to(&hello, addr).await;
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
