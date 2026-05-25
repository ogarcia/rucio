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
    responses(
        (status = 200, description = "Daemon is running and returned its status.", body = StatusResponse)
    )
)]
pub async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    let ns = state.node_status.read().await;
    let uptime = state.started_at.elapsed().as_secs();
    let external_ip = state.external_ip.read().await.clone();

    Json(StatusResponse {
        peer_id: ns.peer_id.clone(),
        class: ns.node_class.clone(),
        connected_peers: ns.connected_peers,
        listen_addrs: ns.listen_addrs.clone(),
        observed_addrs: ns.observed_addrs.clone(),
        uptime_secs: uptime,
        version: env!("CARGO_PKG_VERSION").to_string(),
        external_ip,
    })
}

/// Known peers
///
/// Returns the list of peers this node has seen recently (up to 200),
/// as recorded in the local database by the libp2p Identify and Kademlia protocols.
///
/// Each entry includes the peer's ID, its known multiaddrs, and its connectivity class.
/// The list is a snapshot — peers that have disconnected may still appear here until
/// the database entry expires.
#[utoipa::path(
    get,
    path = "/api/v1/peers",
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
            PeerResponse {
                peer_id: r.peer_id,
                addresses: addrs,
                class: if r.high_id {
                    NodeClass::HighId
                } else {
                    NodeClass::LowId
                },
            }
        })
        .collect();

    Json(PeersResponse { peers })
}
