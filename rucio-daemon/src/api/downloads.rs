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
/// Body: `{ "magnet": "rucio:<hash>?name=<name>&size=<size>", "providers": ["<peer_id>", ...] }`
///
/// `providers` is optional.  When omitted (or empty) the daemon discovers
/// peers via Kademlia DHT automatically.  Supplying providers from a gossip
/// search result allows the download to start immediately while DHT runs in
/// parallel for additional sources.
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
    State(state): State<AppState>,
    Json(req): Json<StartDownloadRequest>,
) -> StatusCode {
    // Validate magnet early so we return 400 synchronously.
    if parse_magnet(&req.magnet).is_err() {
        return StatusCode::BAD_REQUEST;
    }

    // Parse provider strings; skip any that are not valid PeerIds with a warning.
    let providers: Vec<String> = req
        .providers
        .into_iter()
        .filter(|s| {
            if s.parse::<libp2p::PeerId>().is_ok() {
                true
            } else {
                tracing::warn!(peer = %s, "Ignoring malformed PeerId in providers list");
                false
            }
        })
        .collect();

    let dl_req = DownloadRequest::Start {
        magnet: req.magnet.clone(),
        providers,
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

/// DELETE /api/v1/downloads/:id/history
///
/// Permanently removes a finished (completed / cancelled / error) download
/// from the history.  Returns 409 if the download is still active.
#[utoipa::path(
    delete,
    path = "/api/v1/downloads/{id}/history",
    params(("id" = i64, Path, description = "Download ID")),
    responses(
        (status = 204, description = "Download removed from history"),
        (status = 404, description = "Download not found"),
        (status = 409, description = "Download is still active — cancel it first")
    )
)]
pub async fn delete_download(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    // Check current status before deleting.
    let rows = crate::db::downloads::list(&state.db)
        .await
        .unwrap_or_default();
    let row = rows.iter().find(|r| r.id == id);

    match row {
        None => return StatusCode::NOT_FOUND,
        Some(r)
            if matches!(
                r.status.as_str(),
                "finding_providers" | "queued" | "downloading"
            ) =>
        {
            return StatusCode::CONFLICT;
        }
        _ => {}
    }

    match crate::db::downloads::delete(&state.db, id).await {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("DB error deleting download {id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

fn db_status_to_state(s: &str) -> DownloadState {
    match s {
        "finding_providers" => DownloadState::FindingProviders,
        "queued" => DownloadState::Queued,
        "downloading" => DownloadState::Downloading,
        "completed" => DownloadState::Completed,
        "error" => DownloadState::Failed,
        "cancelled" => DownloadState::Cancelled,
        _ => DownloadState::FindingProviders,
    }
}
