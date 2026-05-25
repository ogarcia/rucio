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
/// database.  eMule (ed2k) downloads are included alongside libp2p downloads.
///
/// eMule rows use **negative IDs** (`-1`, `-2`, …) so they can be distinguished from libp2p
/// rows (positive IDs) without any extra fields.  All `cancel` and `history` endpoints accept
/// both positive and negative IDs.
///
/// The `root_hash` field contains the BLAKE3 hash (hex) for libp2p rows and the MD4 hash (hex)
/// for eMule rows.
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
    let mut downloads: Vec<DownloadResponse> = Vec::new();

    // libp2p downloads (positive IDs)
    if let Ok(rows) = crate::db::downloads::list(&state.db).await {
        for r in rows {
            downloads.push(DownloadResponse {
                id: r.id,
                root_hash: hex::encode(&r.root_hash),
                name: Some(r.name),
                size: Some(r.total_size as u64),
                bytes_done: r.bytes_done as u64,
                state: db_status_to_state(&r.status),
                error: r.error_msg,
            });
        }
    }

    // eMule downloads (negative IDs)
    #[cfg(feature = "emule-compat")]
    if let Ok(rows) = crate::db::emule_downloads::list(&state.db).await {
        for r in rows {
            downloads.push(DownloadResponse {
                id: -(r.id),
                root_hash: hex::encode(&r.ed2k_hash),
                name: Some(r.name),
                size: Some(r.total_size as u64),
                bytes_done: r.bytes_done as u64,
                state: db_status_to_state(&r.status),
                error: r.error_msg,
            });
        }
    }

    // Sort newest first (libp2p rows already come newest-first; eMule too; merge keeps order)
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
///
/// Use **negative IDs** (e.g. `-3`) for eMule downloads as returned by `GET /api/v1/downloads`.
#[utoipa::path(
    delete,
    path = "/api/v1/downloads/{id}",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`. Negative = eMule download.")
    ),
    responses(
        (status = 204, description = "Download cancelled."),
        (status = 404, description = "No download with that ID.")
    )
)]
pub async fn cancel_download(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    if id < 0 {
        // eMule download — cancel by setting status to 'cancelled'.
        // The run_ed2k_download loop polls and exits on next iteration.
        #[cfg(feature = "emule-compat")]
        {
            let emule_id = -id;
            match crate::db::emule_downloads::get_status(&state.db, emule_id).await {
                Ok(None) => return StatusCode::NOT_FOUND,
                Err(e) => {
                    tracing::error!("DB error fetching emule download {emule_id}: {e}");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
                Ok(Some(_)) => {}
            }
            match crate::db::emule_downloads::set_status(&state.db, emule_id, "cancelled", None)
                .await
            {
                Ok(()) => StatusCode::NO_CONTENT,
                Err(e) => {
                    tracing::error!("DB error cancelling emule download {emule_id}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        #[cfg(not(feature = "emule-compat"))]
        StatusCode::NOT_FOUND
    } else {
        // libp2p download
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
}

/// Remove a download from history
///
/// Permanently deletes a finished download record (completed, failed, or cancelled) from the
/// database.
///
/// Returns `409 Conflict` if the download is still active — cancel it first with
/// `DELETE /api/v1/downloads/:id`.
///
/// Use **negative IDs** (e.g. `-3`) for eMule downloads as returned by `GET /api/v1/downloads`.
#[utoipa::path(
    delete,
    path = "/api/v1/downloads/{id}/history",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`. Negative = eMule download.")
    ),
    responses(
        (status = 204, description = "Download record deleted."),
        (status = 404, description = "No download with that ID."),
        (status = 409, description = "Download is still active — cancel it first with `DELETE /api/v1/downloads/:id`.")
    )
)]
pub async fn delete_download(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    if id < 0 {
        #[cfg(feature = "emule-compat")]
        {
            let emule_id = -id;
            let rows = crate::db::emule_downloads::list(&state.db)
                .await
                .unwrap_or_default();
            match rows.iter().find(|r| r.id == emule_id) {
                None => return StatusCode::NOT_FOUND,
                Some(r) if matches!(r.status.as_str(), "finding_providers" | "downloading") => {
                    return StatusCode::CONFLICT;
                }
                _ => {}
            }
            match crate::db::emule_downloads::delete(&state.db, emule_id).await {
                Ok(true) => StatusCode::NO_CONTENT,
                Ok(false) => StatusCode::NOT_FOUND,
                Err(e) => {
                    tracing::error!("DB error deleting emule download {emule_id}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        #[cfg(not(feature = "emule-compat"))]
        StatusCode::NOT_FOUND
    } else {
        // libp2p download
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
}

// ── eMule compatibility ───────────────────────────────────────────────────────

/// Start an eMule download
///
/// Queues a file for download from the eMule Kad2 network using an `ed2k://` link.
///
/// The daemon will:
/// 1. Parse the `ed2k://` link to extract the file name, size, and ed2k hash.
/// 2. Bootstrap the Kad2 routing table from `storage.nodes_dat_path` (see config).
/// 3. Search the eMule Kad2 network for peers that have the file.
/// 4. Download the file from discovered eMule peers, verifying each 9.28 MB part
///    using its MD4 hash.
/// 5. After completion, compute the BLAKE3 hash and announce it on the Rucio DHT.
///
/// Requires the daemon to be compiled with `--features emule-compat` and
/// `storage.nodes_dat_path` to be set in the configuration.
///
/// Returns `501 Not Implemented` when the feature is not compiled in.
/// Returns `400 Bad Request` for malformed links or missing `nodes.dat`.
#[utoipa::path(
    post,
    path = "/api/v1/downloads/ed2k",
    request_body = rucio_core::api::downloads::StartEd2kDownloadRequest,
    responses(
        (status = 202, description = "eMule download queued.", body = rucio_core::api::downloads::StartEd2kDownloadResponse),
        (status = 400, description = "Malformed ed2k link or nodes.dat not configured."),
        (status = 501, description = "emule-compat feature not compiled in.")
    )
)]
pub async fn start_ed2k_download(
    State(state): State<AppState>,
    Json(req): Json<rucio_core::api::downloads::StartEd2kDownloadRequest>,
) -> Result<
    (
        StatusCode,
        Json<rucio_core::api::downloads::StartEd2kDownloadResponse>,
    ),
    StatusCode,
> {
    #[cfg(not(feature = "emule-compat"))]
    {
        let _ = (state, req);
        Err(StatusCode::NOT_IMPLEMENTED)
    }

    #[cfg(feature = "emule-compat")]
    {
        use crate::db::emule_downloads::CreateResult;
        use rucio_emule::Ed2kLink;

        let link = Ed2kLink::parse(&req.link).map_err(|e| {
            tracing::warn!(link = %req.link, error = %e, "Failed to parse ed2k link");
            StatusCode::BAD_REQUEST
        })?;

        if state.config.storage.nodes_dat_path.is_none() {
            tracing::debug!("nodes_dat_path not configured, will use platform default");
        }

        // Check for an existing row *before* sending to the engine so we can
        // return the correct HTTP status synchronously.
        let result = crate::db::emule_downloads::create(
            &state.db,
            link.hash.as_bytes(),
            &link.name,
            link.size,
            &req.link,
            now_secs(),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to create eMule download record");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        match result {
            CreateResult::AlreadyCompleted(id) => {
                tracing::info!(id, name = %link.name, "eMule download already completed");
                Err(StatusCode::CONFLICT)
            }
            CreateResult::AlreadyActive(id) => {
                // Already running — return 202 with the existing ID so the
                // client can track it, but do not spawn a second task.
                tracing::info!(id, name = %link.name, "eMule download already active");
                Ok((
                    StatusCode::ACCEPTED,
                    Json(rucio_core::api::downloads::StartEd2kDownloadResponse {
                        id: -(id),
                        name: link.name,
                        size: link.size,
                        ed2k_hash: link.hash.to_hex(),
                    }),
                ))
            }
            CreateResult::Inserted(id) | CreateResult::Reactivated(id) => {
                // New or reactivated row — send to the engine to spawn the task.
                let dl_req = crate::api::DownloadRequest::StartEd2k {
                    link: req.link.clone(),
                    download_id: id,
                };
                if state.download_tx.send(dl_req).await.is_err() {
                    tracing::error!("Download channel closed");
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
                tracing::info!(id, name = %link.name, size = link.size, hash = %link.hash, "eMule download queued");
                Ok((
                    StatusCode::ACCEPTED,
                    Json(rucio_core::api::downloads::StartEd2kDownloadResponse {
                        id: -(id),
                        name: link.name,
                        size: link.size,
                        ed2k_hash: link.hash.to_hex(),
                    }),
                ))
            }
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

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
