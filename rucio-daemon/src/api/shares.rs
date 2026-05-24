//! GET    /api/v1/shares
//! GET    /api/v1/shares/indexing
//! POST   /api/v1/shares
//! DELETE /api/v1/shares/:hash
//! DELETE /api/v1/shares          (query param: path=<prefix>)

use std::sync::atomic::Ordering;

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use rucio_core::api::shares::{AddShareRequest, AddShareResponse, ShareResponse, SharesResponse};
use rucio_core::protocol::chunk::{CHUNK_SIZE, Hash};
use rucio_core::protocol::magnet::MagnetLink;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::AppState;
use crate::db;
use crate::watcher::WatcherCmd;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a magnet link string for a shared file.
///
/// `self_peer_id` is included as a `provider=` hint so recipients can
/// connect directly without waiting for DHT discovery.
fn build_magnet(root_hash: &[u8], name: &str, size: u64, self_peer_id: &str) -> String {
    let hash = root_hash
        .try_into()
        .map(Hash)
        .unwrap_or_else(|_| Hash([0u8; 32]));
    MagnetLink {
        root_hash: hash,
        name: Some(name.to_string()),
        size: Some(size),
        providers: vec![self_peer_id.to_string()],
    }
    .to_string()
}

// ---------------------------------------------------------------------------
// GET /api/v1/shares
// ---------------------------------------------------------------------------

/// GET /api/v1/shares
#[utoipa::path(
    get,
    path = "/api/v1/shares",
    responses(
        (status = 200, description = "List of shared files", body = SharesResponse)
    )
)]
pub async fn list_shares(State(state): State<AppState>) -> Json<SharesResponse> {
    let rows = db::shares::list(&state.db).await.unwrap_or_default();
    let peer_id = state.node_status.read().await.peer_id.clone();

    let shares = rows
        .into_iter()
        .map(|r| ShareResponse {
            magnet: build_magnet(&r.root_hash, &r.name, r.size as u64, &peer_id),
            root_hash: hex::encode(&r.root_hash),
            name: r.name,
            size: r.size as u64,
            chunk_count: r.chunk_count as usize,
            mime_type: r.mime_type,
            path: r.path,
        })
        .collect();

    Json(SharesResponse { shares })
}

// ---------------------------------------------------------------------------
// POST /api/v1/shares
// ---------------------------------------------------------------------------

/// POST /api/v1/shares
#[utoipa::path(
    post,
    path = "/api/v1/shares",
    request_body = AddShareRequest,
    responses(
        (status = 202, description = "Directory registered and files queued for indexing", body = AddShareResponse),
        (status = 400, description = "Path does not exist, is not a directory, or is not accessible")
    )
)]
pub async fn add_share(
    State(state): State<AppState>,
    Json(req): Json<AddShareRequest>,
) -> Result<(StatusCode, Json<AddShareResponse>), (StatusCode, Json<serde_json::Value>)> {
    let root = PathBuf::from(&req.path);

    if !root.exists() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "path does not exist" })),
        ));
    }

    if !root.is_dir() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "only directories can be shared; individual files are not accepted"
            })),
        ));
    }

    // Collect all file paths to index
    let paths = collect_files(&root).map_err(|e| {
        tracing::error!("Failed to collect files under {}: {e}", root.display());
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
    })?;

    let total = paths.len();

    // Register the directory in shared_dirs (idempotent)
    let now = now_secs();
    let path_str = root.to_string_lossy().into_owned();
    if let Err(e) = db::shared_dirs::insert(&state.db, &path_str, false, now).await {
        tracing::error!("Failed to register shared dir {}: {e}", root.display());
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "failed to register directory" })),
        ));
    }

    // Notify the watcher about the new directory
    let _ = state
        .watcher_cmd
        .send(WatcherCmd::Watch(root.clone()))
        .await;

    if total == 0 {
        return Ok((
            StatusCode::ACCEPTED,
            Json(AddShareResponse {
                queued: 0,
                errors: vec![],
            }),
        ));
    }

    // Spawn background task so the HTTP response returns immediately
    let db = state.db.clone();
    let cmd_tx = state.node_cmd.clone();
    let indexing_count = state.indexing_count.clone();
    indexing_count.fetch_add(total, Ordering::Relaxed);
    tokio::spawn(async move {
        let mut errors: Vec<String> = vec![];
        for path in paths {
            match index_file(&db, &path).await {
                Ok(root_hash) => {
                    // Announce to Kademlia that we provide this hash.
                    let _ = cmd_tx
                        .send(crate::node::messages::NodeCmd::StartProviding(
                            root_hash.to_vec(),
                        ))
                        .await;
                }
                Err(e) => {
                    tracing::warn!("Failed to index {}: {e}", path.display());
                    errors.push(path.display().to_string());
                }
            }
            indexing_count.fetch_sub(1, Ordering::Relaxed);
        }
        if !errors.is_empty() {
            tracing::warn!("{} file(s) failed to index", errors.len());
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(AddShareResponse {
            queued: total,
            errors: vec![],
        }),
    ))
}

// ---------------------------------------------------------------------------
// GET /api/v1/shares/:hash/magnet
// ---------------------------------------------------------------------------

/// GET /api/v1/shares/:hash/magnet
///
/// Returns the magnet link string for a locally shared file.
/// The hash can be a full 64-char hex string or a prefix (shortest unique match).
#[utoipa::path(
    get,
    path = "/api/v1/shares/{hash}/magnet",
    params(("hash" = String, Path, description = "BLAKE3 root hash (hex, full or prefix)")),
    responses(
        (status = 200, description = "Magnet link string", body = String),
        (status = 404, description = "No share found for that hash"),
        (status = 400, description = "Ambiguous hash prefix — provide more characters")
    )
)]
pub async fn get_magnet(
    State(state): State<AppState>,
    AxumPath(hash): AxumPath<String>,
) -> Result<String, (StatusCode, Json<serde_json::Value>)> {
    let peer_id = state.node_status.read().await.peer_id.clone();

    // Exact 64-char hex → fast path with direct DB lookup.
    if hash.len() == 64 {
        let Ok(bytes) = hex::decode(&hash) else {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid hex" })),
            ));
        };
        let arr: [u8; 32] = bytes.try_into().unwrap();
        return match db::shares::get_by_hash(&state.db, &arr).await {
            Ok(Some(r)) => Ok(build_magnet(&r.root_hash, &r.name, r.size as u64, &peer_id)),
            Ok(None) => Err((
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "share not found" })),
            )),
            Err(e) => {
                tracing::error!("DB error fetching share: {e}");
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": e.to_string() })),
                ))
            }
        };
    }

    // Prefix search — scan all shares and filter by hex prefix.
    let all = db::shares::list(&state.db).await.unwrap_or_default();
    let matches: Vec<_> = all
        .iter()
        .filter(|r| hex::encode(&r.root_hash).starts_with(&hash))
        .collect();

    match matches.len() {
        0 => Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "share not found" })),
        )),
        1 => {
            let r = matches[0];
            Ok(build_magnet(&r.root_hash, &r.name, r.size as u64, &peer_id))
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "ambiguous hash prefix — provide more characters"
            })),
        )),
    }
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/shares/:hash
// ---------------------------------------------------------------------------

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
pub async fn remove_share(
    State(state): State<AppState>,
    AxumPath(hash): AxumPath<String>,
) -> StatusCode {
    let Ok(bytes) = hex::decode(&hash) else {
        return StatusCode::BAD_REQUEST;
    };
    let Ok(arr): Result<[u8; 32], _> = bytes.try_into() else {
        return StatusCode::BAD_REQUEST;
    };

    match db::shares::delete_by_hash(&state.db, &arr).await {
        Ok(true) => {
            let _ = state
                .node_cmd
                .send(crate::node::messages::NodeCmd::StopProviding(arr.to_vec()))
                .await;
            StatusCode::NO_CONTENT
        }
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("DB error removing share: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/shares?path=<prefix>
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RemoveByPathQuery {
    pub path: String,
}

/// DELETE /api/v1/shares?path=<prefix>
///
/// Removes all indexed files under the given directory path and unregisters
/// the directory from the watch list.  Returns 403 if the directory is
/// protected (e.g. the download directory).
#[utoipa::path(
    delete,
    path = "/api/v1/shares",
    params(("path" = String, Query, description = "Path or directory prefix to remove")),
    responses(
        (status = 200, description = "Number of shares removed"),
        (status = 400, description = "Missing path parameter"),
        (status = 403, description = "Directory is protected and cannot be removed")
    )
)]
pub async fn remove_shares_by_path(
    State(state): State<AppState>,
    Query(q): Query<RemoveByPathQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Check if directory is protected before doing anything
    match db::shared_dirs::is_protected(&state.db, &q.path).await {
        Ok(true) => {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "this directory is protected and cannot be removed"
                })),
            );
        }
        Err(e) => {
            tracing::error!("DB error checking protection: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            );
        }
        Ok(false) => {}
    }

    // Remove from shared_dirs
    let dir_path = PathBuf::from(&q.path);
    let _ = state.watcher_cmd.send(WatcherCmd::Unwatch(dir_path)).await;
    if let Err(e) = db::shared_dirs::delete(&state.db, &q.path).await {
        tracing::warn!("Could not remove shared_dir entry for {}: {e}", q.path);
    }

    // Remove all indexed files under this path
    match db::shares::delete_by_path_prefix(&state.db, &q.path).await {
        Ok(hashes) => {
            let removed = hashes.len() as u64;
            // Stop providing each deleted hash in Kademlia.
            for hash in hashes {
                let _ = state
                    .node_cmd
                    .send(crate::node::messages::NodeCmd::StopProviding(hash))
                    .await;
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({ "removed": removed })),
            )
        }
        Err(e) => {
            tracing::error!("DB error removing shares by path: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Internals: file collection and hashing
// ---------------------------------------------------------------------------

pub(crate) use rucio_core::protocol::hashing::collect_files;

/// GET /api/v1/shares/indexing
///
/// Returns the number of files currently being indexed in background tasks.
/// Returns `{ "pending": N }` where N is the count.
pub async fn indexing_status(State(state): State<super::AppState>) -> Json<serde_json::Value> {
    let pending = state.indexing_count.load(Ordering::Relaxed);
    Json(serde_json::json!({ "pending": pending }))
}

/// Hash a single file, split into chunks, and insert into the DB.
/// Returns the root hash on success.
pub(crate) async fn index_file(db: &crate::db::Db, path: &Path) -> anyhow::Result<[u8; 32]> {
    let path_owned = path.to_path_buf();

    let fh =
        tokio::task::spawn_blocking(move || rucio_core::protocol::hashing::hash_file(&path_owned))
            .await??;

    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    db::shares::insert(
        db,
        db::shares::NewSharedFile {
            root_hash: &fh.root_hash,
            name: &name,
            size: fh.size,
            mime_type: fh.mime_type.as_deref(),
            path: &path.to_string_lossy(),
            chunk_size: CHUNK_SIZE,
            added_at: now,
            chunks: &fh.chunks,
        },
    )
    .await?;

    tracing::info!(path = %path.display(), size = fh.size, "Indexed file");
    Ok(fh.root_hash)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
