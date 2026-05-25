//! Kad2 source search: find eMule peers that have a given ed2k file.
//!
//! ## Algorithm
//!
//! 1. Convert the ed2k hash to a `KadId` (raw 16 bytes, little-endian — same
//!    byte order that ed2k hashes are already stored in).
//! 2. Bootstrap from the user-supplied `nodes.dat` routing table.
//! 3. Iteratively send `KADEMLIA2_REQ` (node-lookup) to the K closest nodes
//!    not yet queried, collecting new contacts from `KADEMLIA2_RES`.
//! 4. Once the closest set stabilises, send `KADEMLIA2_SEARCH_SOURCE_REQ` to
//!    the K closest nodes.
//! 5. Collect `KADEMLIA2_SEARCH_RES` packets and return the source list.
//!
//! All UDP I/O is done with Tokio.

use super::packet::{
    self, Contact, KadId, KadPacket, SourceEntry, decode, encode_req, encode_search_source_req,
};
use super::routing::RoutingTable;
use crate::ed2k::Ed2kHash;
use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tracing::{debug, warn};

/// Configuration for a source search.
#[derive(Debug, Clone)]
pub struct SearchConfig {
    /// Maximum wall-clock time for the entire search.
    pub timeout: Duration,
    /// Timeout for each individual UDP request.
    pub request_timeout: Duration,
    /// Maximum sources to collect before stopping.
    pub max_sources: usize,
    /// Number of parallel node-lookup queries (alpha).
    pub alpha: usize,
    /// Number of lookup iterations before switching to source search.
    pub lookup_iterations: usize,
    /// File size (used in SEARCH_SOURCE_REQ).
    pub file_size: u64,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(3),
            max_sources: 50,
            alpha: 3,
            lookup_iterations: 5,
            file_size: 0,
        }
    }
}

/// Result of a Kad2 source search.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub sources: Vec<SourceEntry>,
}

/// Perform a Kad2 source search for the given ed2k hash.
///
/// `routing_table` must already be populated (e.g. from `nodes.dat`).
/// Binds a random local UDP port; the OS will choose the interface.
pub async fn search_sources(
    hash: &Ed2kHash,
    routing_table: &RoutingTable,
    config: SearchConfig,
) -> Result<SearchResult> {
    let target = KadId::from_bytes(*hash.as_bytes());
    let our_id = routing_table.our_id;

    // Bind a UDP socket.
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("bind UDP socket for Kad2 search")?;
    socket
        .set_broadcast(false)
        .context("set UDP broadcast off")?;

    timeout(
        config.timeout,
        run_search(socket, target, our_id, routing_table, config),
    )
    .await
    .context("Kad2 source search timed out")?
}

async fn run_search(
    socket: UdpSocket,
    target: KadId,
    our_id: KadId,
    routing_table: &RoutingTable,
    config: SearchConfig,
) -> Result<SearchResult> {
    let mut known: HashMap<KadId, Contact> = HashMap::new();
    let mut queried: HashSet<KadId> = HashSet::new();
    let mut sources: Vec<SourceEntry> = Vec::new();

    // Seed the known set from the routing table.
    for c in routing_table.closest_to(&target, 20) {
        known.insert(c.id, c);
    }

    // ── Phase 1: iterative node lookup ──────────────────────────────────────
    for _iter in 0..config.lookup_iterations {
        let to_query: Vec<Contact> = {
            let mut candidates: Vec<&Contact> = known
                .values()
                .filter(|c| !queried.contains(&c.id))
                .collect();
            candidates.sort_by(|a, b| {
                a.id.distance(&target)
                    .cmp_bytes()
                    .cmp(b.id.distance(&target).cmp_bytes())
            });
            candidates.into_iter().take(config.alpha).cloned().collect()
        };

        if to_query.is_empty() {
            break;
        }

        for contact in &to_query {
            queried.insert(contact.id);
            let pkt = encode_req(2, &target, &our_id);
            let addr = SocketAddr::V4(contact.socket_addr_udp());
            if let Err(e) = socket.send_to(&pkt, addr).await {
                warn!("Kad2 REQ send error to {addr}: {e}");
                continue;
            }
        }

        // Collect responses.
        let deadline = tokio::time::Instant::now() + config.request_timeout;
        let mut recv_buf = [0u8; 2048];
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, socket.recv_from(&mut recv_buf)).await {
                Ok(Ok((n, _src))) => match decode(&recv_buf[..n]) {
                    Ok(KadPacket::Res(res)) => {
                        for c in res.contacts {
                            known.entry(c.id).or_insert(c);
                        }
                    }
                    Ok(KadPacket::BootstrapRes(res)) => {
                        for c in res.contacts {
                            known.entry(c.id).or_insert(c);
                        }
                    }
                    Ok(_) => {}
                    Err(e) => debug!("Kad2 decode error: {e}"),
                },
                Ok(Err(e)) => warn!("Kad2 recv error: {e}"),
                Err(_) => break, // timeout
            }
        }
    }

    // ── Phase 2: source search ───────────────────────────────────────────────
    let source_nodes: Vec<Contact> = {
        let mut v: Vec<&Contact> = known.values().collect();
        v.sort_by(|a, b| {
            a.id.distance(&target)
                .cmp_bytes()
                .cmp(b.id.distance(&target).cmp_bytes())
        });
        v.into_iter().take(10).cloned().collect()
    };

    let pkt = encode_search_source_req(&target, config.file_size);

    for contact in &source_nodes {
        let addr = SocketAddr::V4(contact.socket_addr_udp());
        if let Err(e) = socket.send_to(&pkt, addr).await {
            warn!("Kad2 SEARCH_SOURCE_REQ send error to {addr}: {e}");
        }
    }

    // Collect search responses.
    let deadline = tokio::time::Instant::now() + config.request_timeout * 2;
    let mut recv_buf = [0u8; 4096];
    loop {
        if sources.len() >= config.max_sources {
            break;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, socket.recv_from(&mut recv_buf)).await {
            Ok(Ok((n, _src))) => match decode(&recv_buf[..n]) {
                Ok(KadPacket::SearchRes(res)) => {
                    for s in res.sources {
                        if sources.len() < config.max_sources {
                            sources.push(s);
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => debug!("Kad2 decode error during source search: {e}"),
            },
            Ok(Err(e)) => warn!("Kad2 recv error: {e}"),
            Err(_) => break,
        }
    }

    Ok(SearchResult { sources })
}

/// Bootstrap a routing table by sending BOOTSTRAP_REQ to all seed contacts
/// and collecting their responses.
pub async fn bootstrap(
    routing_table: &mut RoutingTable,
    seeds: &[Contact],
    request_timeout: Duration,
) -> Result<()> {
    if seeds.is_empty() {
        return Ok(());
    }
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .context("bind UDP socket for bootstrap")?;

    let pkt = packet::encode_bootstrap_req();
    for seed in seeds {
        let addr = SocketAddr::V4(seed.socket_addr_udp());
        let _ = socket.send_to(&pkt, addr).await;
    }

    let deadline = tokio::time::Instant::now() + request_timeout;
    let mut recv_buf = [0u8; 4096];
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, socket.recv_from(&mut recv_buf)).await {
            Ok(Ok((n, _src))) => {
                if let Ok(KadPacket::BootstrapRes(res)) = decode(&recv_buf[..n]) {
                    for c in res.contacts {
                        routing_table.insert(c);
                    }
                }
            }
            Ok(Err(e)) => warn!("bootstrap recv error: {e}"),
            Err(_) => break,
        }
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ed2k::Ed2kHash;

    #[test]
    fn test_target_from_hash() {
        let hash = Ed2kHash::from_hex("d41d8cd98f00b204e9800998ecf8427e").unwrap();
        let kid = KadId::from_bytes(*hash.as_bytes());
        // Just check it doesn't panic and has the right bytes.
        assert_eq!(kid.as_bytes()[0], 0xd4);
    }

    #[test]
    fn test_search_config_defaults() {
        let cfg = SearchConfig::default();
        assert_eq!(cfg.alpha, 3);
        assert!(cfg.max_sources > 0);
    }
}
