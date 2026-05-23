//! GET    /api/v1/downloads
//! POST   /api/v1/downloads
//! DELETE /api/v1/downloads/:id

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use rucio_core::api::downloads::{
    DownloadResponse, DownloadState, DownloadsResponse, StartDownloadRequest,
};

use crate::api::{AppState, DownloadRequest};
use crate::transfer::parse_magnet;

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
///
/// Body: `{ "magnet": "rucio:<hash>?name=<name>&size=<size>", "provider": "<peer_id>" }`
///
/// The `provider` field is the PeerId string of the peer that holds the file,
/// typically obtained from a search result.
#[utoipa::path(
    post,
    path = "/api/v1/downloads",
    request_body = StartDownloadRequest,
    responses(
        (status = 202, description = "Download queued"),
        (status = 400, description = "Invalid magnet link or missing provider")
    )
)]
pub async fn start_download(
    State(state): State<AppState>,
    Json(req): Json<StartDownloadRequest>,
) -> StatusCode {
    // Validate magnet early so we return 400 synchronously.
    if parse_magnet(&req.magnet).is_err() {
        return StatusCode::BAD_REQUEST;
    }

    let provider = match &req.provider {
        Some(p) if !p.is_empty() => p.clone(),
        _ => {
            tracing::warn!("Download requested without provider");
            return StatusCode::BAD_REQUEST;
        }
    };

    let dl_req = DownloadRequest::Start {
        magnet: req.magnet.clone(),
        providers: vec![provider],
    };

    match state.download_tx.send(dl_req).await {
        Ok(()) => {
            tracing::info!(magnet = %req.magnet, "Download queued");
            StatusCode::ACCEPTED
        }
        Err(_) => {
            tracing::error!("Download channel closed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
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
    // Fetch the root_hash before marking cancelled so the engine can clean up
    // pending manifest state (which is keyed by hash, not by id).
    let root_hash = match crate::db::downloads::get_root_hash(&state.db, id).await {
        Ok(Some(h)) => h,
        Ok(None) => return StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("DB error fetching download {id}: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    match crate::db::downloads::set_status(&state.db, id, "cancelled", None).await {
        Ok(()) => {
            let _ = state
                .download_tx
                .send(DownloadRequest::Cancel {
                    download_id: id,
                    root_hash,
                })
                .await;
            StatusCode::NO_CONTENT
        }
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
