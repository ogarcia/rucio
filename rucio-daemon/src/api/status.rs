//! GET /api/v1/status
//! GET /api/v1/peers

use axum::Json;
use axum::extract::State;
use rucio_core::api::status::{PeerResponse, PeersResponse, StatusResponse};
use rucio_core::protocol::node::NodeClass;

use crate::api::AppState;

/// GET /api/v1/status
#[utoipa::path(
    get,
    path = "/api/v1/status",
    responses(
        (status = 200, description = "Daemon status", body = StatusResponse)
    )
)]
pub async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    let ns = state.node_status.read().await;
    let uptime = state.started_at.elapsed().as_secs();

    Json(StatusResponse {
        peer_id: ns.peer_id.clone(),
        class: ns.node_class.clone(),
        connected_peers: ns.connected_peers,
        listen_addrs: ns.listen_addrs.clone(),
        observed_addrs: ns.observed_addrs.clone(),
        uptime_secs: uptime,
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// GET /api/v1/peers
#[utoipa::path(
    get,
    path = "/api/v1/peers",
    responses(
        (status = 200, description = "Known peers", body = PeersResponse)
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
