//! GET /api/v1/status
//! GET /api/v1/peers

use axum::Json;
use axum::extract::State;
use rucio_core::api::status::{PeerResponse, PeersResponse, StatusResponse};
use rucio_core::protocol::node::NodeClass;

use crate::api::AppState;

/// Daemon status
///
/// Returns the current state of the running daemon: peer identity,
/// connectivity class (HighID / LowID / Unknown), number of connected peers,
/// listen and observed addresses, uptime, and software version.
///
/// **Connectivity class**
/// - `HighId` — the node is publicly reachable and can serve files to any peer.
/// - `LowId` — the node is behind NAT; it can download but inbound connections are not possible.
/// - `Unknown` — the class has not yet been determined (normal during the first few seconds after startup).
///
/// **Observed addresses** are the external multiaddrs reported back by remote peers via the
/// libp2p Identify protocol. They are the addresses other nodes on the internet can use to
/// reach this node and are the ones to put in another node's `bootstrap_peers` config.
#[utoipa::path(
    get,
    path = "/api/v1/status",
    tag = "node",
    responses(
        (status = 200, description = "Daemon is running and returned its status.", body = StatusResponse)
    )
)]
pub async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    let ns = state.node_status.read().await;
    let uptime = state.started_at.elapsed().as_secs();
    let external_ip = state.external_ip.read().await.clone();

    // Active rucio downloads: count rows in a transferring/searching state, the
    // same set the WS DownloadProgress stream considers active.
    use rucio_core::api::downloads::DownloadState;
    let active_downloads = crate::db::downloads::list(&state.db)
        .await
        .map(|rows| {
            rows.iter()
                .filter(|r| {
                    matches!(
                        crate::api::downloads::db_status_to_state(&r.status),
                        DownloadState::FindingProviders
                            | DownloadState::Queued
                            | DownloadState::Downloading
                            | DownloadState::Stalled
                    )
                })
                .count()
        })
        .unwrap_or(0);
    // Active rucio uploads: distinct peers currently pulling from us.
    let active_uploads = state.upload_stats.snapshot().len();

    Json(StatusResponse {
        peer_id: ns.peer_id.clone(),
        class: ns.node_class.clone(),
        reachability: ns.reachability.clone(),
        connected_peers: ns.connected_peers,
        listen_addrs: ns.listen_addrs.clone(),
        observed_addrs: ns.observed_addrs.clone(),
        uptime_secs: uptime,
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit: env!("RUCIO_GIT_HASH").to_string(),
        active_downloads,
        active_uploads,
        external_ip,
    })
}

/// Known peers
///
/// Returns the list of peers this node has seen recently (up to 200),
/// as recorded in the local database by the libp2p Identify and Kademlia protocols.
///
/// Each entry includes the peer's ID, its known multiaddrs, its connectivity class,
/// and — when the peer has completed an Identify exchange — the software agent
/// string it advertised (e.g. `Rucio/0.28.0 (Linux x86_64) libp2p/0.56.0`).
/// The list is a snapshot — peers that have disconnected may still appear here until
/// the database entry expires.
#[utoipa::path(
    get,
    path = "/api/v1/peers",
    tag = "node",
    responses(
        (status = 200, description = "List of recently seen peers.", body = PeersResponse)
    )
)]
pub async fn get_peers(State(state): State<AppState>) -> Json<PeersResponse> {
    // For now, return the peers cached in the DB.
    let rows = crate::db::peers::list_recent(&state.db, 200)
        .await
        .unwrap_or_default();

    let peers = rows
        .into_iter()
        .map(|r| {
            let addrs: Vec<String> = serde_json::from_str(&r.addrs).unwrap_or_default();
            // Peers advertise all of their listen addresses, including ones that
            // are meaningless to anyone else — drop those from the listing.
            let addrs: Vec<String> = addrs
                .into_iter()
                .filter(|a| !is_unreachable_addr(a))
                .collect();
            PeerResponse {
                peer_id: r.peer_id,
                addresses: addrs,
                class: if r.high_id {
                    NodeClass::HighId
                } else {
                    NodeClass::LowId
                },
                agent_version: r.agent_version,
            }
        })
        .collect();

    Json(PeersResponse { peers })
}

/// Whether a multiaddr points at a loopback or link-local IP. Such addresses
/// only make sense on the peer's own machine (loopback) or its own link
/// (link-local — IPv6 `fe80::/10` is interface-scoped and an IPv4 `169.254/16`
/// is APIPA), so a remote peer advertising them gives us nothing dialable. We
/// hide them from the peer listing. Unparseable or non-IP addrs are kept.
fn is_unreachable_addr(addr: &str) -> bool {
    use libp2p::multiaddr::Protocol;
    let Ok(ma) = addr.parse::<libp2p::Multiaddr>() else {
        return false;
    };
    ma.iter().any(|p| match p {
        Protocol::Ip4(ip) => ip.is_loopback() || ip.is_link_local(),
        // No stable `is_unicast_link_local`; fe80::/10 = top 10 bits 1111111010.
        Protocol::Ip6(ip) => ip.is_loopback() || (ip.segments()[0] & 0xffc0) == 0xfe80,
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::is_unreachable_addr;

    #[test]
    fn hides_loopback_and_link_local() {
        // Loopback.
        assert!(is_unreachable_addr("/ip4/127.0.0.1/tcp/4321"));
        assert!(is_unreachable_addr("/ip6/::1/tcp/4321"));
        // Link-local: IPv6 fe80::/10 and IPv4 169.254/16.
        assert!(is_unreachable_addr("/ip6/fe80::1/tcp/4321"));
        assert!(is_unreachable_addr("/ip4/169.254.1.2/tcp/4321"));
    }

    #[test]
    fn keeps_routable_and_unknown() {
        // Public / private-LAN / ULA addresses stay (reachable somewhere).
        assert!(!is_unreachable_addr("/ip4/203.0.113.7/tcp/4321"));
        assert!(!is_unreachable_addr("/ip4/192.168.1.50/tcp/4321"));
        assert!(!is_unreachable_addr("/ip6/2a05:f480::10/tcp/4321"));
        // Non-IP or unparseable: keep rather than hide silently.
        assert!(!is_unreachable_addr("/dns4/example.com/tcp/4321"));
        assert!(!is_unreachable_addr("garbage"));
    }
}
