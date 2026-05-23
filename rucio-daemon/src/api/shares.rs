//! GET  /api/v1/shares
//! POST /api/v1/shares
//! DELETE /api/v1/shares/:hash

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use rucio_core::api::shares::{AddShareRequest, ShareResponse, SharesResponse};

use crate::api::AppState;

/// GET /api/v1/shares
#[utoipa::path(
    get,
    path = "/api/v1/shares",
    responses(
        (status = 200, description = "List of shared files", body = SharesResponse)
    )
)]
pub async fn list_shares(State(state): State<AppState>) -> Json<SharesResponse> {
    let rows = crate::db::shares::list(&state.db).await.unwrap_or_default();

    let shares = rows
        .into_iter()
        .map(|r| ShareResponse {
            root_hash: hex::encode(&r.root_hash),
            name: r.name,
            size: r.size as u64,
            chunk_count: 0, // TODO: join with chunks table
            mime_type: r.mime_type,
        })
        .collect();

    Json(SharesResponse { shares })
}

/// POST /api/v1/shares
#[utoipa::path(
    post,
    path = "/api/v1/shares",
    request_body = AddShareRequest,
    responses(
        (status = 202, description = "Share queued for indexing"),
        (status = 400, description = "Invalid request")
    )
)]
pub async fn add_share(
    State(_state): State<AppState>,
    Json(req): Json<AddShareRequest>,
) -> StatusCode {
    // TODO: spawn a task to hash the file and insert into the DB + DHT
    tracing::info!(path = %req.path, "Share requested (not yet implemented)");
    StatusCode::ACCEPTED
}

/// DELETE /api/v1/shares/:hash
#[utoipa::path(
    delete,
    path = "/api/v1/shares/{hash}",
    params(("hash" = String, Path, description = "BLAKE3 root hash (hex)")),
    responses(
        (status = 204, description = "Share removed"),
        (status = 404, description = "Share not found"),
        (status = 400, description = "Invalid hash")
    )
)]
pub async fn remove_share(State(state): State<AppState>, Path(hash): Path<String>) -> StatusCode {
    let Ok(bytes) = hex::decode(&hash) else {
        return StatusCode::BAD_REQUEST;
    };
    let Ok(arr): Result<[u8; 32], _> = bytes.try_into() else {
        return StatusCode::BAD_REQUEST;
    };

    match crate::db::shares::delete(&state.db, &arr).await {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("DB error removing share: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
