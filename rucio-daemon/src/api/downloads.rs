//! GET    /api/v1/downloads
//! POST   /api/v1/downloads
//! DELETE /api/v1/downloads/:id

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use rucio_core::api::downloads::{
    DownloadResponse, DownloadState, DownloadsResponse, StartDownloadRequest,
};

use crate::api::AppState;

/// GET /api/v1/downloads
#[utoipa::path(
    get,
    path = "/api/v1/downloads",
    responses(
        (status = 200, description = "List of downloads", body = DownloadsResponse)
    )
)]
pub async fn list_downloads(State(state): State<AppState>) -> Json<DownloadsResponse> {
    let rows = crate::db::downloads::list(&state.db)
        .await
        .unwrap_or_default();

    let downloads = rows
        .into_iter()
        .map(|r| DownloadResponse {
            id: r.id,
            root_hash: hex::encode(&r.root_hash),
            name: Some(r.name),
            size: Some(r.total_size as u64),
            bytes_done: r.bytes_done as u64,
            state: db_status_to_state(&r.status),
            error: r.error_msg,
        })
        .collect();

    Json(DownloadsResponse { downloads })
}

/// POST /api/v1/downloads
#[utoipa::path(
    post,
    path = "/api/v1/downloads",
    request_body = StartDownloadRequest,
    responses(
        (status = 202, description = "Download queued"),
        (status = 400, description = "Invalid magnet link")
    )
)]
pub async fn start_download(
    State(_state): State<AppState>,
    Json(req): Json<StartDownloadRequest>,
) -> StatusCode {
    // TODO: parse magnet, resolve providers via DHT, enqueue in DB
    tracing::info!(magnet = %req.magnet, "Download requested (not yet implemented)");
    StatusCode::ACCEPTED
}

/// DELETE /api/v1/downloads/:id
#[utoipa::path(
    delete,
    path = "/api/v1/downloads/{id}",
    params(("id" = i64, Path, description = "Download ID")),
    responses(
        (status = 204, description = "Download cancelled"),
        (status = 404, description = "Download not found")
    )
)]
pub async fn cancel_download(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    match crate::db::downloads::set_status(&state.db, id, "cancelled", None).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("DB error cancelling download {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

fn db_status_to_state(s: &str) -> DownloadState {
    match s {
        "downloading" => DownloadState::Downloading,
        "completed" => DownloadState::Completed,
        "error" => DownloadState::Failed,
        "cancelled" => DownloadState::Cancelled,
        _ => DownloadState::Queued,
    }
}
