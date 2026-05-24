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

/// List downloads
///
/// Returns all downloads — active, completed, failed, and cancelled — stored in the local
/// database.
///
/// Each entry includes the download ID, BLAKE3 root hash, file name, total size, bytes
/// transferred so far, and current state.
///
/// **States**
/// - `finding_providers` — searching the Kademlia DHT for peers that have the file.
/// - `queued` — providers found, waiting for a transfer slot.
/// - `downloading` — actively transferring chunks.
/// - `completed` — all chunks received and the file has been moved to the download directory.
/// - `failed` — the transfer encountered an unrecoverable error.
/// - `cancelled` — cancelled by the user via `DELETE /api/v1/downloads/:id`.
///
/// Completed, failed, and cancelled entries remain in the list until explicitly removed with
/// `DELETE /api/v1/downloads/:id/history`.
#[utoipa::path(
    get,
    path = "/api/v1/downloads",
    responses(
        (status = 200, description = "All downloads in the local database.", body = DownloadsResponse)
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

/// Start a download
///
/// Queues a file for download identified by a magnet link.
///
/// The magnet link uses the `rucio:` scheme and encodes the BLAKE3 root hash plus optional
/// metadata (`name`, `size`) and provider hints (`provider=<PeerId>`).
///
/// The `providers` field is optional. When empty the daemon discovers peers automatically via
/// the Kademlia DHT. Supplying provider peer IDs obtained from a search result allows the
/// transfer to start immediately while DHT lookup runs in parallel for additional sources.
///
/// The endpoint returns `202 Accepted` immediately — use `GET /api/v1/downloads` to track
/// progress. Trying to start a download for a hash that is already active returns `202` again
/// (the duplicate is silently ignored by the transfer engine).
#[utoipa::path(
    post,
    path = "/api/v1/downloads",
    request_body = StartDownloadRequest,
    responses(
        (status = 202, description = "Download queued successfully."),
        (status = 400, description = "The magnet link is malformed or uses an unsupported scheme.")
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

/// Cancel a download
///
/// Signals the transfer engine to stop an in-progress download and marks it as `cancelled`
/// in the database.
///
/// Any chunks already downloaded are discarded and the `.part` file is removed from the
/// temp directory. The cancelled entry remains visible in `GET /api/v1/downloads` until
/// removed with `DELETE /api/v1/downloads/:id/history`.
#[utoipa::path(
    delete,
    path = "/api/v1/downloads/{id}",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`.")
    ),
    responses(
        (status = 204, description = "Download cancelled."),
        (status = 404, description = "No download with that ID.")
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

/// Remove a download from history
///
/// Permanently deletes a finished download record (completed, failed, or cancelled) from the
/// database.
///
/// Returns `409 Conflict` if the download is still active — cancel it first with
/// `DELETE /api/v1/downloads/:id`.
#[utoipa::path(
    delete,
    path = "/api/v1/downloads/{id}/history",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`.")
    ),
    responses(
        (status = 204, description = "Download record deleted."),
        (status = 404, description = "No download with that ID."),
        (status = 409, description = "Download is still active — cancel it first with `DELETE /api/v1/downloads/:id`.")
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

pub(crate) fn db_status_to_state(s: &str) -> DownloadState {
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
