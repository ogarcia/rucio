//! GET    /api/v1/downloads
//! POST   /api/v1/downloads
//! DELETE /api/v1/downloads/:id

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use rucio_core::api::categories::SetCategoryRequest;
use rucio_core::api::downloads::{
    DownloadDetailResponse, DownloadResponse, DownloadState, DownloadsResponse,
    RenameDownloadRequest, StartDownloadRequest,
};

use rucio_core::protocol::chunk::Hash;
use rucio_core::protocol::magnet::MagnetLink;

use crate::api::{AppState, DownloadRequest};
use crate::db::Db;
use crate::transfer::parse_magnet;

/// True if `category_id` is unassigned (None) or refers to an existing category.
/// Used to reject filing a download under a category that doesn't exist.
async fn category_exists(db: &Db, category_id: Option<i64>) -> bool {
    match category_id {
        None => true,
        Some(cid) => crate::db::categories::get(db, cid)
            .await
            .ok()
            .flatten()
            .is_some(),
    }
}

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
/// - `stalled` — no sources found after several rounds; keeps retrying in the background.
/// - `paused` — suspended by the user via `POST /api/v1/downloads/:id/pause`; resume with
///   `POST /api/v1/downloads/:id/resume`.  Not resumed automatically on daemon restart.
/// - `completed` — all chunks received and the file has been moved to the download directory.
/// - `failed` — the transfer encountered an unrecoverable error.
/// - `cancelled` — cancelled by the user via `POST /api/v1/downloads/:id/cancel`.
///
/// Completed, failed, and cancelled entries remain in the list until explicitly removed with
/// `DELETE /api/v1/downloads/:id`.
#[utoipa::path(
    get,
    path = "/api/v1/downloads",
    responses(
        (status = 200, description = "All downloads in the local database.", body = DownloadsResponse)
    )
)]
pub async fn list_downloads(State(state): State<AppState>) -> Json<DownloadsResponse> {
    // Collect (added_at, DownloadResponse) from both sources so we can merge
    // them into a single chronological list regardless of origin.
    let mut with_ts: Vec<(i64, DownloadResponse)> = Vec::new();

    // Snapshot of live stats, keyed by signed id. Used to report the live byte
    // count (with in-flight partials) so the REST list matches what the WS
    // streams; without it a refresh would clash with the WS value. Falls back
    // to the persisted figure when there is no live entry.
    let live = state.live_stats.read().await.clone();

    // libp2p downloads (positive IDs)
    if let Ok(rows) = crate::db::downloads::list(&state.db).await {
        for r in rows {
            let bytes_done = live
                .get(&r.id)
                .and_then(|l| l.bytes_done)
                .unwrap_or(r.bytes_done as u64);
            with_ts.push((
                r.added_at,
                DownloadResponse {
                    id: r.id,
                    root_hash: hex::encode(&r.root_hash),
                    name: Some(r.name),
                    size: Some(r.total_size as u64),
                    bytes_done,
                    state: db_status_to_state(&r.status),
                    error: r.error_msg,
                    category_id: r.category_id,
                },
            ));
        }
    }

    // eMule downloads (negative IDs)
    #[cfg(feature = "emule-compat")]
    if let Ok(rows) = crate::db::emule_downloads::list(&state.db).await {
        for r in rows {
            let bytes_done = live
                .get(&-r.id)
                .and_then(|l| l.bytes_done)
                .unwrap_or(r.bytes_done as u64);
            with_ts.push((
                r.added_at,
                DownloadResponse {
                    id: -(r.id),
                    root_hash: hex::encode(&r.ed2k_hash),
                    name: Some(r.name),
                    size: Some(r.total_size as u64),
                    bytes_done,
                    state: db_status_to_state(&r.status),
                    error: r.error_msg,
                    category_id: r.category_id,
                },
            ));
        }
    }

    with_ts.sort_by_key(|(ts, _)| *ts);
    let downloads = with_ts.into_iter().map(|(_, d)| d).collect();
    Json(DownloadsResponse { downloads })
}

/// Download detail
///
/// Returns the full state of a single download: identity, progress (bytes and
/// completed pieces), destination path, timestamps, and the source link.
/// Pieces are libp2p chunks for rucio downloads and 9.28 MB slices for eMule.
///
/// While the download is active it also includes live stats: sources known and
/// active, pieces in flight, download speed (bytes/s) and an ETA in seconds.
/// These fields are absent for finished downloads.
///
/// Use **negative IDs** (e.g. `-3`) for eMule downloads, as returned by
/// `GET /api/v1/downloads`.
#[utoipa::path(
    get,
    path = "/api/v1/downloads/{id}",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`. Negative = eMule download.")
    ),
    responses(
        (status = 200, description = "Download detail.", body = DownloadDetailResponse),
        (status = 404, description = "No download with that ID.")
    )
)]
pub async fn get_download(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<DownloadDetailResponse>, StatusCode> {
    // Live stats are keyed by the same signed id as the public API.
    let live = state.live_stats.read().await.get(&id).cloned();
    if id >= 0 {
        // libp2p download
        let row = crate::db::downloads::get(&state.db, id)
            .await
            .map_err(|e| {
                tracing::error!("DB error fetching download {id}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .ok_or(StatusCode::NOT_FOUND)?;

        let chunks = crate::db::downloads::chunks_for(&state.db, id)
            .await
            .unwrap_or_default();
        let pieces_total = chunks.len() as u64;
        let pieces_done = chunks.iter().filter(|c| c.status == "done").count() as u64;

        let root_hash_hex = hex::encode(&row.root_hash);
        let magnet = MagnetLink {
            root_hash: Hash(row.root_hash[..].try_into().unwrap_or([0u8; 32])),
            name: Some(row.name.clone()),
            size: Some(row.total_size as u64),
            providers: Vec::new(),
        }
        .to_string();

        // Prefer the live byte count (with in-flight partials) so the detail
        // matches the WS/list; falls back to the persisted figure.
        let bytes_done = live
            .as_ref()
            .and_then(|l| l.bytes_done)
            .unwrap_or(row.bytes_done as u64);

        Ok(Json(DownloadDetailResponse {
            id,
            kind: "rucio".to_string(),
            root_hash: root_hash_hex,
            name: Some(row.name),
            size: Some(row.total_size as u64),
            bytes_done,
            state: db_status_to_state(&row.status),
            error: row.error_msg,
            dest_path: non_empty(row.dest_path),
            added_at: row.added_at,
            updated_at: row.updated_at,
            link: Some(magnet),
            pieces_done: Some(pieces_done),
            pieces_total: (pieces_total > 0).then_some(pieces_total),
            sources_total: live.as_ref().map(|l| l.sources_total),
            sources_active: live.as_ref().map(|l| l.sources_active),
            pieces_in_flight: live.as_ref().map(|l| l.pieces_in_flight),
            speed_bps: live.as_ref().map(|l| l.speed_bps),
            eta_secs: eta_secs(
                row.total_size as u64,
                bytes_done,
                live.as_ref().map(|l| l.speed_bps).unwrap_or(0),
            ),
            peers: live.as_ref().map(|l| l.peers.clone()).unwrap_or_default(),
            // libp2p has no upload-queue concept; these stay absent.
            queued_sources: live.as_ref().map(|l| l.queued_sources).filter(|&n| n > 0),
            best_queue_rank: live.as_ref().and_then(|l| l.best_queue_rank),
            category_id: row.category_id,
        }))
    } else {
        // eMule download
        #[cfg(feature = "emule-compat")]
        {
            let emule_id = -id;
            let row = crate::db::emule_downloads::get(&state.db, emule_id)
                .await
                .map_err(|e| {
                    tracing::error!("DB error fetching emule download {emule_id}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?
                .ok_or(StatusCode::NOT_FOUND)?;

            // Completed slices come from the .part.met progress bitmap.
            let chunk = rucio_emule::ed2k::CHUNK_SIZE as u64;
            let num_slices = (row.total_size as u64).div_ceil(chunk) as usize;
            let met_path = state
                .config
                .emule
                .temp_dir
                .join(format!("{}.part.met", hex::encode(&row.ed2k_hash)));
            let done = rucio_emule::progress::load_progress(&met_path, num_slices);
            let pieces_done = done.iter().filter(|&&d| d).count() as u64;

            // Prefer the live byte count (with in-flight partials) so the detail
            // matches the WS/list; falls back to the persisted figure.
            let bytes_done = live
                .as_ref()
                .and_then(|l| l.bytes_done)
                .unwrap_or(row.bytes_done as u64);

            Ok(Json(DownloadDetailResponse {
                id,
                kind: "emule".to_string(),
                root_hash: hex::encode(&row.ed2k_hash),
                name: Some(row.name),
                size: Some(row.total_size as u64),
                bytes_done,
                state: db_status_to_state(&row.status),
                error: row.error_msg,
                dest_path: non_empty(row.dest_path),
                added_at: row.added_at,
                updated_at: row.updated_at,
                link: Some(row.ed2k_link),
                pieces_done: Some(pieces_done),
                pieces_total: Some(num_slices as u64),
                sources_total: live.as_ref().map(|l| l.sources_total),
                sources_active: live.as_ref().map(|l| l.sources_active),
                pieces_in_flight: live.as_ref().map(|l| l.pieces_in_flight),
                speed_bps: live.as_ref().map(|l| l.speed_bps),
                eta_secs: eta_secs(
                    row.total_size as u64,
                    bytes_done,
                    live.as_ref().map(|l| l.speed_bps).unwrap_or(0),
                ),
                // Per-peer eMule download detail is a later pass (sources live in
                // independent worker tasks with no central per-peer registry).
                peers: Vec::new(),
                queued_sources: live.as_ref().map(|l| l.queued_sources).filter(|&n| n > 0),
                best_queue_rank: live.as_ref().and_then(|l| l.best_queue_rank),
                category_id: row.category_id,
            }))
        }
        #[cfg(not(feature = "emule-compat"))]
        {
            let _ = &state;
            Err(StatusCode::NOT_FOUND)
        }
    }
}

/// Map an empty path string (the "not yet known" sentinel in the DB) to `None`.
fn non_empty(s: String) -> Option<String> {
    (!s.is_empty()).then_some(s)
}

/// Encode completed-piece indices into a base64 LSB-first bitmap of `total` bits.
fn encode_done_bitmap(done_idx: impl Iterator<Item = usize>, total: usize) -> String {
    use base64::Engine;
    let mut bytes = vec![0u8; total.div_ceil(8)];
    for i in done_idx {
        if i < total {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Per-piece state
///
/// Returns a compact map of which pieces are done and which are being fetched
/// right now, for rendering a fine-grained block-style progress bar.
///
/// `done_bitmap` is a base64 LSB-first bitmap (1 bit per piece, set when
/// complete); `in_flight` lists the indices currently downloading (live, empty
/// when the download is not active). Everything else is pending.
///
/// Pieces are libp2p chunks for rucio downloads and 9.28 MB slices for eMule.
/// Use **negative IDs** for eMule downloads.
#[utoipa::path(
    get,
    path = "/api/v1/downloads/{id}/pieces",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`. Negative = eMule download.")
    ),
    responses(
        (status = 200, description = "Per-piece state.", body = rucio_core::api::downloads::DownloadPiecesResponse),
        (status = 404, description = "No download with that ID.")
    )
)]
pub async fn get_download_pieces(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<rucio_core::api::downloads::DownloadPiecesResponse>, StatusCode> {
    use rucio_core::api::downloads::DownloadPiecesResponse;

    // Live in-flight indices are keyed by the same signed id as the public API.
    let in_flight = state
        .live_stats
        .read()
        .await
        .get(&id)
        .map(|l| l.in_flight_pieces.clone())
        .unwrap_or_default();

    if id >= 0 {
        // libp2p download — pieces are the rows in download_chunks.
        // Fetch the row only to distinguish an unknown id (404) from a known
        // download whose manifest hasn't arrived yet (0 pieces).
        crate::db::downloads::get(&state.db, id)
            .await
            .map_err(|e| {
                tracing::error!("DB error fetching download {id}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .ok_or(StatusCode::NOT_FOUND)?;

        let chunks = crate::db::downloads::chunks_for(&state.db, id)
            .await
            .unwrap_or_default();
        let total = chunks.len();
        let done_bitmap = encode_done_bitmap(
            chunks
                .iter()
                .filter(|c| c.status == "done")
                .map(|c| c.idx as usize),
            total,
        );

        Ok(Json(DownloadPiecesResponse {
            id,
            kind: "rucio".to_string(),
            pieces_total: total as u64,
            done_bitmap,
            in_flight,
        }))
    } else {
        // eMule download — pieces are 9.28 MB slices tracked in the .part.met file.
        #[cfg(feature = "emule-compat")]
        {
            let emule_id = -id;
            let row = crate::db::emule_downloads::get(&state.db, emule_id)
                .await
                .map_err(|e| {
                    tracing::error!("DB error fetching emule download {emule_id}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?
                .ok_or(StatusCode::NOT_FOUND)?;

            let chunk = rucio_emule::ed2k::CHUNK_SIZE as u64;
            let num_slices = (row.total_size as u64).div_ceil(chunk) as usize;
            let met_path = state
                .config
                .emule
                .temp_dir
                .join(format!("{}.part.met", hex::encode(&row.ed2k_hash)));
            let done = rucio_emule::progress::load_progress(&met_path, num_slices);
            let done_bitmap = encode_done_bitmap(
                done.iter().enumerate().filter(|(_, d)| **d).map(|(i, _)| i),
                num_slices,
            );

            Ok(Json(DownloadPiecesResponse {
                id,
                kind: "emule".to_string(),
                pieces_total: num_slices as u64,
                done_bitmap,
                in_flight,
            }))
        }
        #[cfg(not(feature = "emule-compat"))]
        {
            let _ = &state;
            Err(StatusCode::NOT_FOUND)
        }
    }
}

/// Estimate seconds to completion from current speed and remaining bytes.
/// `None` when the speed is zero or the download is already complete.
fn eta_secs(size: u64, bytes_done: u64, speed_bps: u64) -> Option<u64> {
    if speed_bps == 0 || size <= bytes_done {
        return None;
    }
    Some((size - bytes_done) / speed_bps)
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
/// progress. A hash that is already downloading returns `202` again (idempotent); content that is
/// already completed, or already on disk as a share, returns `409 Conflict` (remove it first to
/// re-download).
#[utoipa::path(
    post,
    path = "/api/v1/downloads",
    request_body = StartDownloadRequest,
    responses(
        (status = 202, description = "Download queued successfully (or already in progress)."),
        (status = 400, description = "The magnet link is malformed or uses an unsupported scheme."),
        (status = 409, description = "This content is already completed or present on disk as a share.")
    )
)]
pub async fn start_download(
    State(state): State<AppState>,
    Json(req): Json<StartDownloadRequest>,
) -> StatusCode {
    // Validate magnet early so we return 400 synchronously.
    let Ok(info) = parse_magnet(&req.magnet) else {
        return StatusCode::BAD_REQUEST;
    };

    // Already have this content as a share (e.g. a previous download that was
    // removed from history but is still on disk and being provided)? Then it's
    // already local — reject instead of re-fetching it over the network.
    if matches!(
        crate::db::shares::get_by_hash(&state.db, &info.root_hash).await,
        Ok(Some(_))
    ) {
        tracing::info!("Content already shared (on disk); rejecting re-download");
        return StatusCode::CONFLICT;
    }

    // Synchronous dedup feedback. The engine dedups authoritatively (and never
    // creates a duplicate), but its result is async; surface the common cases
    // here so the client gets a real answer, like the eMule path does.
    match crate::db::downloads::status_by_root_hash(&state.db, &info.root_hash).await {
        Ok(Some(s)) if s == "completed" => {
            tracing::info!("Download already completed; rejecting re-download");
            return StatusCode::CONFLICT;
        }
        // Already in progress — accept (idempotent) without queuing a duplicate.
        Ok(Some(s)) if matches!(s.as_str(), "finding_providers" | "queued" | "downloading") => {
            return StatusCode::ACCEPTED;
        }
        // None, or a terminal/reactivable row (cancelled/error/stalled) → proceed.
        _ => {}
    }

    // A given category must exist before we file the download under it.
    if !category_exists(&state.db, req.category_id).await {
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
        category_id: req.category_id,
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
/// removed with `DELETE /api/v1/downloads/:id`.
///
/// Use **negative IDs** (e.g. `-3`) for eMule downloads as returned by `GET /api/v1/downloads`.
#[utoipa::path(
    post,
    path = "/api/v1/downloads/{id}/cancel",
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
        // eMule download — set status to 'cancelled' and stop the task.
        #[cfg(feature = "emule-compat")]
        {
            let emule_id = -id;
            let row = match crate::db::emule_downloads::get(&state.db, emule_id).await {
                Ok(Some(r)) => r,
                Ok(None) => return StatusCode::NOT_FOUND,
                Err(e) => {
                    tracing::error!("DB error fetching emule download {emule_id}: {e}");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            };
            if let Err(e) =
                crate::db::emule_downloads::set_status(&state.db, emule_id, "cancelled", None).await
            {
                tracing::error!("DB error cancelling emule download {emule_id}: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            // Stop the running task promptly. It deletes the partial files when
            // it observes the 'cancelled' status; if no task is running, clean
            // them up here.
            if !signal_emule_stop(&state, emule_id) {
                remove_emule_partials(&state.config, &row.ed2k_hash).await;
            }
            StatusCode::NO_CONTENT
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

/// Pause a download
///
/// Suspends an active download (`finding_providers`, `queued`, `downloading`, or `stalled`)
/// and marks it as `paused`.  Unlike cancelling, the partial file and per-chunk progress are
/// kept, so the transfer can be resumed later with `POST /api/v1/downloads/:id/resume`.
///
/// A paused download is **not** resumed automatically when the daemon restarts.
///
/// Use **negative IDs** (e.g. `-3`) for eMule downloads as returned by `GET /api/v1/downloads`.
#[utoipa::path(
    post,
    path = "/api/v1/downloads/{id}/pause",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`. Negative = eMule download.")
    ),
    responses(
        (status = 204, description = "Download paused."),
        (status = 404, description = "No download with that ID."),
        (status = 409, description = "Download is not in a pausable state (already finished or paused).")
    )
)]
pub async fn pause_download(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    if id < 0 {
        // eMule download — pause by setting status to 'paused'.  The
        // run_ed2k_download loop polls and exits on its next iteration.
        #[cfg(feature = "emule-compat")]
        {
            let emule_id = -id;
            match crate::db::emule_downloads::get_status(&state.db, emule_id).await {
                Ok(None) => return StatusCode::NOT_FOUND,
                Ok(Some(s)) if !is_pausable(&s) => return StatusCode::CONFLICT,
                Err(e) => {
                    tracing::error!("DB error fetching emule download {emule_id}: {e}");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
                Ok(Some(_)) => {}
            }
            match crate::db::emule_downloads::set_status(&state.db, emule_id, "paused", None).await
            {
                Ok(()) => {
                    // Stop the running task promptly (it keeps the partial file).
                    signal_emule_stop(&state, emule_id);
                    StatusCode::NO_CONTENT
                }
                Err(e) => {
                    tracing::error!("DB error pausing emule download {emule_id}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        #[cfg(not(feature = "emule-compat"))]
        StatusCode::NOT_FOUND
    } else {
        // libp2p download
        let row = match crate::db::downloads::get(&state.db, id).await {
            Ok(Some(r)) => r,
            Ok(None) => return StatusCode::NOT_FOUND,
            Err(e) => {
                tracing::error!("DB error fetching download {id}: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
        };

        if !is_pausable(&row.status) {
            return StatusCode::CONFLICT;
        }

        match crate::db::downloads::set_status(&state.db, id, "paused", None).await {
            Ok(()) => {
                let _ = state
                    .download_tx
                    .send(DownloadRequest::Pause {
                        download_id: id,
                        root_hash: row.root_hash,
                    })
                    .await;
                StatusCode::NO_CONTENT
            }
            Err(e) => {
                tracing::error!("DB error pausing download {id}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

/// Resume a download
///
/// Restarts a previously paused download from where it left off, re-using the partial file
/// and per-chunk progress kept on disk.  Only downloads in the `paused` state can be resumed.
///
/// Use **negative IDs** (e.g. `-3`) for eMule downloads as returned by `GET /api/v1/downloads`.
#[utoipa::path(
    post,
    path = "/api/v1/downloads/{id}/resume",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`. Negative = eMule download.")
    ),
    responses(
        (status = 204, description = "Download resumed."),
        (status = 404, description = "No download with that ID."),
        (status = 409, description = "Download is not paused.")
    )
)]
pub async fn resume_download(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    if id < 0 {
        // eMule download — resume by relaunching the download task.
        #[cfg(feature = "emule-compat")]
        {
            let emule_id = -id;
            let row = match crate::db::emule_downloads::get(&state.db, emule_id).await {
                Ok(Some(r)) => r,
                Ok(None) => return StatusCode::NOT_FOUND,
                Err(e) => {
                    tracing::error!("DB error fetching emule download {emule_id}: {e}");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            };
            if row.status != "paused" {
                return StatusCode::CONFLICT;
            }
            // Move out of 'paused' before relaunching so the download loop's
            // stop check does not immediately exit again.
            if let Err(e) = crate::db::emule_downloads::set_status(
                &state.db,
                emule_id,
                "finding_providers",
                None,
            )
            .await
            {
                tracing::error!("DB error resuming emule download {emule_id}: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            // Clear any pending stop flag: if a task from the pause is still
            // shutting down, this lets it continue instead of exiting. If it has
            // already exited, the StartEd2k below spawns a fresh one.
            if let Some(flag) = state.emule_cancel.lock().unwrap().get(&emule_id) {
                flag.store(false, std::sync::atomic::Ordering::Relaxed);
            }
            let dl_req = DownloadRequest::StartEd2k {
                link: row.ed2k_link,
                download_id: emule_id,
            };
            match state.download_tx.send(dl_req).await {
                Ok(()) => StatusCode::NO_CONTENT,
                Err(_) => {
                    tracing::error!("Download channel closed");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        #[cfg(not(feature = "emule-compat"))]
        StatusCode::NOT_FOUND
    } else {
        // libp2p download
        match crate::db::downloads::get_status(&state.db, id).await {
            Ok(None) => return StatusCode::NOT_FOUND,
            Ok(Some(s)) if s != "paused" => return StatusCode::CONFLICT,
            Err(e) => {
                tracing::error!("DB error fetching download {id}: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            Ok(Some(_)) => {}
        }

        match state
            .download_tx
            .send(DownloadRequest::Resume { download_id: id })
            .await
        {
            Ok(()) => StatusCode::NO_CONTENT,
            Err(_) => {
                tracing::error!("Download channel closed");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

/// Reduce a user-supplied name to a safe bare file name: keep only the final
/// path component (so `/`, `\` and `..` cannot escape the download directory)
/// and reject empty / `.` / `..` results.
fn sanitize_download_name(raw: &str) -> Option<String> {
    std::path::Path::new(raw.trim())
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && s != "." && s != "..")
}

/// Rename an in-progress download
///
/// Changes the file name an **unfinished** download will be saved as on completion. The new name
/// is sanitised to a bare file name (directory separators and `..` are stripped). For libp2p
/// downloads the in-progress `.part` is moved to match; for eMule downloads the `.part` is keyed
/// by hash, so only the final name changes. Completed downloads cannot be renamed — the file is
/// already on disk and belongs to the user.
///
/// Use **negative IDs** (e.g. `-3`) for eMule downloads as returned by `GET /api/v1/downloads`.
#[utoipa::path(
    post,
    path = "/api/v1/downloads/{id}/rename",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`. Negative = eMule download.")
    ),
    request_body = RenameDownloadRequest,
    responses(
        (status = 204, description = "Download renamed."),
        (status = 400, description = "The supplied name is empty or invalid."),
        (status = 404, description = "No download with that ID."),
        (status = 409, description = "Download is already completed and cannot be renamed.")
    )
)]
pub async fn rename_download(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(req): Json<RenameDownloadRequest>,
) -> StatusCode {
    let Some(new_name) = sanitize_download_name(&req.name) else {
        return StatusCode::BAD_REQUEST;
    };

    if id < 0 {
        // eMule download.
        #[cfg(feature = "emule-compat")]
        {
            let emule_id = -id;
            let row = match crate::db::emule_downloads::get(&state.db, emule_id).await {
                Ok(Some(r)) => r,
                Ok(None) => return StatusCode::NOT_FOUND,
                Err(e) => {
                    tracing::error!("DB error fetching emule download {emule_id}: {e}");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            };
            if row.status == "completed" {
                return StatusCode::CONFLICT;
            }
            if let Err(e) =
                crate::db::emule_downloads::set_name(&state.db, emule_id, &new_name).await
            {
                tracing::error!("DB error renaming emule download {emule_id}: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            // Reflect the new name in what we advertise while still serving the
            // partial file. The finalising task re-reads the name from the DB.
            if let Ok(hash) = <[u8; 16]>::try_from(row.ed2k_hash.as_slice())
                && let Some(info) = state.emule_active_downloads.write().await.get_mut(&hash)
            {
                info.name = new_name;
            }
            return StatusCode::NO_CONTENT;
        }
        #[cfg(not(feature = "emule-compat"))]
        return StatusCode::NOT_FOUND;
    }

    // libp2p download.
    let row = match crate::db::downloads::get(&state.db, id).await {
        Ok(Some(r)) => r,
        Ok(None) => return StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("DB error fetching download {id}: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };
    if row.status == "completed" {
        return StatusCode::CONFLICT;
    }
    if let Err(e) = crate::db::downloads::set_name(&state.db, id, &new_name).await {
        tracing::error!("DB error renaming download {id}: {e}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    // Hand the physical .part move / in-memory repoint to the transfer engine.
    let _ = state
        .download_tx
        .send(DownloadRequest::Rename {
            download_id: id,
            new_name,
        })
        .await;
    StatusCode::NO_CONTENT
}

/// Remove a download from history
///
/// Permanently deletes a finished download record (completed, failed, or cancelled) from the
/// database.
///
/// Returns `409 Conflict` if the download is still active — cancel it first with
/// `POST /api/v1/downloads/:id/cancel`.
///
/// Use **negative IDs** (e.g. `-3`) for eMule downloads as returned by `GET /api/v1/downloads`.
#[utoipa::path(
    delete,
    path = "/api/v1/downloads/{id}",
    params(
        ("id" = i64, Path, description = "Numeric download ID from `GET /api/v1/downloads`. Negative = eMule download.")
    ),
    responses(
        (status = 204, description = "Download record deleted."),
        (status = 404, description = "No download with that ID."),
        (status = 409, description = "Download is still active — cancel it first with `POST /api/v1/downloads/:id/cancel`.")
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
            let ed2k_hash = match rows.iter().find(|r| r.id == emule_id) {
                None => return StatusCode::NOT_FOUND,
                Some(r) if matches!(r.status.as_str(), "finding_providers" | "downloading") => {
                    return StatusCode::CONFLICT;
                }
                Some(r) => r.ed2k_hash.clone(),
            };
            match crate::db::emule_downloads::delete(&state.db, emule_id).await {
                Ok(true) => {
                    // Drop any leftover partial files (paused/stalled/error rows
                    // still have them; cancelled ones were already cleaned).
                    remove_emule_partials(&state.config, &ed2k_hash).await;
                    StatusCode::NO_CONTENT
                }
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

/// Clear the download history
///
/// Removes every **finished** download — completed, failed, or cancelled — from the database
/// in a single operation, across both libp2p and eMule downloads. Active and paused downloads
/// are left untouched. Files already written to disk are **not** deleted.
#[utoipa::path(
    delete,
    path = "/api/v1/downloads/history",
    responses(
        (status = 200, description = "Returns `{ \"removed\": N }` with the number of entries cleared.")
    )
)]
pub async fn clear_history(State(state): State<AppState>) -> Json<serde_json::Value> {
    let removed = crate::db::downloads::delete_terminal(&state.db)
        .await
        .unwrap_or(0);
    #[cfg(feature = "emule-compat")]
    let removed = removed
        + crate::db::emule_downloads::delete_terminal(&state.db)
            .await
            .unwrap_or(0);
    Json(serde_json::json!({ "removed": removed }))
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

        // A given category must exist before we file the download under it.
        if !category_exists(&state.db, req.category_id).await {
            return Err(StatusCode::BAD_REQUEST);
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
            req.category_id,
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

#[cfg(feature = "emule-compat")]
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn db_status_to_state(s: &str) -> DownloadState {
    match s {
        "finding_providers" => DownloadState::FindingProviders,
        "queued" => DownloadState::Queued,
        "downloading" => DownloadState::Downloading,
        "stalled" => DownloadState::Stalled,
        "paused" => DownloadState::Paused,
        "completed" => DownloadState::Completed,
        "error" => DownloadState::Failed,
        "cancelled" => DownloadState::Cancelled,
        _ => DownloadState::FindingProviders,
    }
}

/// Statuses from which a download can be paused: active, non-terminal states.
fn is_pausable(status: &str) -> bool {
    matches!(
        status,
        "finding_providers" | "queued" | "downloading" | "stalled"
    )
}

/// Signal a running eMule download task to stop. Returns `true` if a live task
/// was found and signalled, `false` if none is running for this id.
#[cfg(feature = "emule-compat")]
fn signal_emule_stop(state: &AppState, emule_id: i64) -> bool {
    if let Some(flag) = state.emule_cancel.lock().unwrap().get(&emule_id) {
        flag.store(true, std::sync::atomic::Ordering::Relaxed);
        true
    } else {
        false
    }
}

/// Delete the `.part` and `.part.met` files for an eMule download by its raw
/// ed2k hash. Best-effort: missing files are ignored.
#[cfg(feature = "emule-compat")]
async fn remove_emule_partials(config: &crate::config::Config, ed2k_hash: &[u8]) {
    let Ok(hash) = <[u8; 16]>::try_from(ed2k_hash) else {
        return;
    };
    let (part, met) = crate::emule::part_paths(config, &hash);
    let _ = tokio::fs::remove_file(&part).await;
    let _ = tokio::fs::remove_file(&met).await;
}

/// Set (or clear) a download's category
///
/// Changes where the download lands on completion: the category's pinned
/// directory, or the global download dir when cleared (`category_id: null`). A
/// completed download is only re-labelled — its file is not moved. Use a
/// **negative ID** for eMule downloads.
#[utoipa::path(
    put,
    path = "/api/v1/downloads/{id}/category",
    params(("id" = i64, Path, description = "Download ID (negative = eMule).")),
    request_body = SetCategoryRequest,
    responses(
        (status = 204, description = "Category assignment updated."),
        (status = 400, description = "No category with that id."),
        (status = 404, description = "No download with that id."),
    )
)]
pub async fn set_download_category(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(req): Json<SetCategoryRequest>,
) -> StatusCode {
    if !category_exists(&state.db, req.category_id).await {
        return StatusCode::BAD_REQUEST;
    }
    let updated = if id < 0 {
        #[cfg(feature = "emule-compat")]
        {
            crate::db::emule_downloads::set_category(&state.db, -id, req.category_id).await
        }
        #[cfg(not(feature = "emule-compat"))]
        {
            anyhow::Ok(false)
        }
    } else {
        crate::db::downloads::set_category(&state.db, id, req.category_id).await
    };
    match updated {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("set_download_category: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[cfg(test)]
mod bitmap_tests {
    use super::encode_done_bitmap;
    use base64::Engine;

    fn decode(b64: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap()
    }

    #[test]
    fn lsb_first_within_byte() {
        // Pieces 0, 2, 7 done out of 8 → bits 0,2,7 set → 0b1000_0101.
        let bytes = decode(&encode_done_bitmap([0usize, 2, 7].into_iter(), 8));
        assert_eq!(bytes, vec![0b1000_0101]);
    }

    #[test]
    fn rounds_byte_count_up() {
        // 9 pieces → 2 bytes; piece 8 lands in bit 0 of byte 1.
        let bytes = decode(&encode_done_bitmap([8usize].into_iter(), 9));
        assert_eq!(bytes.len(), 2);
        assert_eq!(bytes[1], 0b0000_0001);
    }

    #[test]
    fn ignores_out_of_range_indices() {
        let bytes = decode(&encode_done_bitmap([100usize].into_iter(), 8));
        assert_eq!(bytes, vec![0u8]);
    }

    #[test]
    fn empty_when_no_pieces() {
        assert_eq!(encode_done_bitmap(std::iter::empty(), 0), "");
    }
}
