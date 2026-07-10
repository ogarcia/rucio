//! `GET /api/v1/uploads` — peers currently downloading files from this node.

use axum::Json;
use axum::extract::State;

use rucio_core::api::uploads::UploadsResponse;

use crate::api::AppState;

/// List active uploads.
///
/// Returns every peer currently pulling data from us, across both networks
/// (rucio/libp2p and eMule/ed2k), with the file, bytes sent this session, and a
/// smoothed per-peer rate. The list is volatile: a row exists only while the
/// transfer is in progress and is sorted fastest-first.
#[utoipa::path(
    get,
    path = "/api/v1/uploads",
    tag = "uploads",
    responses(
        (status = 200, description = "Active uploads retrieved", body = UploadsResponse),
    )
)]
pub async fn list_uploads(State(state): State<AppState>) -> Json<UploadsResponse> {
    Json(UploadsResponse {
        uploads: state.upload_stats.snapshot(),
    })
}
