//! GET    /api/v1/shares
//! GET    /api/v1/shares/indexing
//! POST   /api/v1/shares
//! DELETE /api/v1/shares/:hash
//! DELETE /api/v1/shares          (query param: path=<prefix>)

use std::sync::atomic::Ordering;

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use rucio_core::api::shares::{
    AddShareRequest, AddShareResponse, ShareResponse, SharedDirResponse, SharedDirsResponse,
    SharesResponse,
};
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

/// List shared directories
///
/// Returns the watched directories — the unit you add (`POST /api/v1/shares`) and remove
/// (`DELETE /api/v1/shares?path=…`). Each entry reports the absolute path, whether it is
/// protected (the download directory, always shared and not removable), and the number and
/// total size of indexed files under it.
///
/// Use `GET /api/v1/shares/files` for the individual indexed files.
#[utoipa::path(
    get,
    path = "/api/v1/shares",
    responses(
        (status = 200, description = "All watched directories.", body = SharedDirsResponse)
    )
)]
pub async fn list_shares(State(state): State<AppState>) -> Json<SharedDirsResponse> {
    let dirs = db::shared_dirs::list(&state.db).await.unwrap_or_default();

    let mut out = Vec::with_capacity(dirs.len());
    for d in dirs {
        let (file_count, total_size) = db::shares::count_and_size_by_prefix(&state.db, &d.path)
            .await
            .unwrap_or((0, 0));
        out.push(SharedDirResponse {
            path: d.path,
            protected: d.protected,
            file_count: file_count as u64,
            total_size: total_size as u64,
        });
    }

    Json(SharedDirsResponse { dirs: out })
}

// ---------------------------------------------------------------------------
// GET /api/v1/shares/files
// ---------------------------------------------------------------------------

/// Default page size when the client doesn't specify `limit`.
const SHARES_PAGE_DEFAULT: i64 = 200;
/// Hard cap on `limit` so a client can't ask for the whole (huge) list at once.
const SHARES_PAGE_MAX: i64 = 1000;

/// Query parameters for paginating and filtering the shared-files list.
#[derive(Debug, Deserialize)]
pub struct ListSharesParams {
    /// Case-insensitive substring to match against the file name.
    #[serde(default)]
    pub q: Option<String>,
    /// Restrict to files under this directory (exact dir or anything beneath it).
    #[serde(default)]
    pub dir: Option<String>,
    /// Page size (default 200, capped at 1000).
    #[serde(default)]
    pub limit: Option<i64>,
    /// Number of rows to skip (for paging through the result set).
    #[serde(default)]
    pub offset: Option<i64>,
}

/// List shared files (paginated)
///
/// Returns one page of indexed files matching the optional `q` (name substring)
/// and `dir` (directory) filters, newest first, plus `total` — the number of
/// files matching the filter. Filtering and slicing happen in SQL, so this
/// scales to very large shares; the client pages with `limit`/`offset`.
///
/// Each entry includes the BLAKE3 root hash, file name, size, chunk count, MIME
/// type, filesystem path, and a ready-to-use magnet link carrying this node's
/// peer ID as a provider hint.
#[utoipa::path(
    get,
    path = "/api/v1/shares/files",
    params(
        ("q" = Option<String>, Query, description = "Case-insensitive name substring filter."),
        ("dir" = Option<String>, Query, description = "Restrict to files under this directory."),
        ("limit" = Option<i64>, Query, description = "Page size (default 200, max 1000)."),
        ("offset" = Option<i64>, Query, description = "Rows to skip.")
    ),
    responses(
        (status = 200, description = "One page of shared files plus the total match count.", body = SharesResponse)
    )
)]
pub async fn list_share_files(
    State(state): State<AppState>,
    Query(params): Query<ListSharesParams>,
) -> Json<SharesResponse> {
    let limit = params
        .limit
        .unwrap_or(SHARES_PAGE_DEFAULT)
        .clamp(1, SHARES_PAGE_MAX);
    let offset = params.offset.unwrap_or(0).max(0);
    // Treat blank filters as absent.
    let q = params.q.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let dir = params
        .dir
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let (rows, total) = db::shares::list_page(&state.db, q, dir, limit, offset)
        .await
        .unwrap_or_default();
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

    Json(SharesResponse {
        shares,
        total: total as u64,
    })
}

// ---------------------------------------------------------------------------
// POST /api/v1/shares
// ---------------------------------------------------------------------------

/// Share a directory
///
/// Registers a directory for sharing and queues all files it contains for background indexing.
///
/// **Only directories are accepted** — individual files cannot be shared directly.
/// The daemon recurses into subdirectories and indexes every regular file found.
///
/// Indexing runs in the background; the response returns immediately with the number of files
/// queued. Use `GET /api/v1/shares/indexing` to poll progress.
///
/// The directory is also registered with the filesystem watcher: new files added later are
/// indexed automatically, and removed files are removed from the share list.
///
/// This endpoint is idempotent — calling it again for the same directory re-indexes any files
/// that may have changed.
#[utoipa::path(
    post,
    path = "/api/v1/shares",
    request_body = AddShareRequest,
    responses(
        (status = 202, description = "Directory registered. `queued` is the number of files sent to the indexing queue.", body = AddShareResponse),
        (status = 400, description = "The path does not exist, is not a directory, or could not be read."),
        (status = 500, description = "Internal error registering the directory.")
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
    let outboard_dir = state.config.storage.outboard_dir.clone();
    indexing_count.fetch_add(total, Ordering::Relaxed);
    // Latch so the main loop fires an "indexing complete" notification once this
    // batch drains, even if it finishes between two ws ticks.
    state.indexing_seen.store(true, Ordering::Relaxed);
    tokio::spawn(async move {
        let mut errors: Vec<String> = vec![];
        for path in paths {
            match index_file(&db, &path, Some(&outboard_dir)).await {
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

/// Get magnet link for a shared file
///
/// Returns the magnet link for a file shared by this node, identified by its BLAKE3 root hash.
///
/// The `hash` parameter accepts either a full 64-character hex string or a shorter unique prefix.
/// If the prefix matches more than one file a `400` is returned — provide more characters to
/// disambiguate.
///
/// The returned magnet link includes this node's peer ID as a `provider=` hint so the recipient
/// can start downloading immediately without waiting for DHT discovery.
#[utoipa::path(
    get,
    path = "/api/v1/shares/{hash}/magnet",
    params(
        ("hash" = String, Path, description = "BLAKE3 root hash — full 64-char hex or a unique prefix.")
    ),
    responses(
        (status = 200, description = "Magnet link string (`rucio:` scheme, plain text).", body = String),
        (status = 400, description = "Hash is invalid hex, or the prefix matches more than one file."),
        (status = 404, description = "No locally shared file with that hash.")
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

/// Remove a shared file
///
/// Stops sharing a single file identified by its full BLAKE3 root hash and removes it from
/// the local index.
///
/// The file is also withdrawn from the Kademlia DHT so other peers stop receiving this node
/// as a provider for that hash. The file itself is **not deleted** from disk.
///
/// To remove all files under a directory at once use `DELETE /api/v1/shares?path=<directory>`.
#[utoipa::path(
    delete,
    path = "/api/v1/shares/{hash}",
    params(
        ("hash" = String, Path, description = "Full 64-character BLAKE3 root hash (hex).")
    ),
    responses(
        (status = 204, description = "File removed from the share list."),
        (status = 400, description = "Hash is not valid hex or is not 32 bytes."),
        (status = 404, description = "No shared file with that hash.")
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
            // Drop the regenerable outboard cache for this hash.
            crate::transfer::remove_share_outboard(&state.config.storage.outboard_dir, &arr).await;
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

/// Remove all shares under a directory
///
/// Removes all indexed files whose path is under the given directory prefix and unregisters
/// the directory from the filesystem watcher.
///
/// Each removed file is also withdrawn from the Kademlia DHT. The files themselves are
/// **not deleted** from disk.
///
/// The download directory is protected and cannot be removed with this endpoint — it returns
/// `403` if the path matches it.
#[utoipa::path(
    delete,
    path = "/api/v1/shares",
    params(
        ("path" = String, Query, description = "Filesystem path prefix. All indexed files whose path starts with this string are removed.")
    ),
    responses(
        (status = 200, description = "Returns `{ \"removed\": N }` with the number of files unshared."),
        (status = 400, description = "Missing `path` query parameter."),
        (status = 403, description = "The directory is protected (e.g. the download directory) and cannot be removed.")
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
            // Stop providing each deleted hash in Kademlia and drop its cached
            // outboard.
            for hash in hashes {
                if let Ok(arr) = <[u8; 32]>::try_from(hash.as_slice()) {
                    crate::transfer::remove_share_outboard(
                        &state.config.storage.outboard_dir,
                        &arr,
                    )
                    .await;
                }
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

/// Indexing progress
///
/// Returns the number of files currently being indexed in background tasks.
///
/// Indexing is triggered by `POST /api/v1/shares` and by the filesystem watcher when new files
/// are detected. Poll this endpoint after adding a directory to know when all files are ready
/// to be discovered by other peers.
///
/// Returns `{ "pending": 0 }` when there is nothing being indexed.
#[utoipa::path(
    get,
    path = "/api/v1/shares/indexing",
    responses(
        (status = 200, description = "Number of files pending indexing.", body = serde_json::Value,
         example = json!({ "pending": 0 })),
    )
)]
pub async fn indexing_status(State(state): State<super::AppState>) -> Json<serde_json::Value> {
    let pending = state.indexing_count.load(Ordering::Relaxed);
    Json(serde_json::json!({ "pending": pending }))
}

/// Hash a single file, split into chunks, and insert into the DB.
/// Returns the root hash on success.
/// File modification time as Unix seconds, or `0` if it can't be read. Stored
/// with each indexed file so the rescan can detect offline changes by mtime.
pub(crate) fn file_mtime_secs(path: &Path) -> i64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Index a file into the share list and return its root hash.
///
/// `outboard_dir` is the share outboard cache directory: when `Some`, a large
/// file's bao outboard — already computed as a byproduct of hashing — is
/// persisted to the cache so the first chunk request doesn't re-read the whole
/// file to rebuild it. Pass `None` to skip that (the lazy serve path regenerates
/// it on demand).
pub(crate) async fn index_file(
    db: &crate::db::Db,
    path: &Path,
    outboard_dir: Option<&Path>,
) -> anyhow::Result<[u8; 32]> {
    // Idempotent: if this path is already indexed with the same size + mtime,
    // the content is unchanged — return the existing root hash without
    // re-hashing or re-inserting (the `shared_files` row has UNIQUE path and
    // root_hash, so a blind re-insert would error). This lets two indexers race
    // harmlessly — e.g. a just-completed eMule download indexing itself while
    // the watcher fires for the same new file — without a spurious failure.
    let path_str = path.to_string_lossy();
    if let Some(row) = db::shares::get_by_path(db, &path_str).await? {
        let disk_size = std::fs::metadata(path)
            .map(|m| m.len() as i64)
            .unwrap_or(-1);
        if disk_size == row.size
            && file_mtime_secs(path) == row.mtime
            && let Ok(existing) = <[u8; 32]>::try_from(row.root_hash.as_slice())
        {
            return Ok(existing);
        }
    }

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
            mtime: file_mtime_secs(path),
        },
    )
    .await?;

    // Save the outboard for large files so the first peer to request a chunk
    // doesn't trigger a full re-read to rebuild it (no-op below the threshold).
    if let Some(outboard_dir) = outboard_dir {
        crate::transfer::persist_share_outboard_if_large(
            outboard_dir,
            &fh.root_hash,
            fh.size,
            &fh.outboard,
        )
        .await;
    }

    tracing::info!(path = %path.display(), size = fh.size, "Indexed file");
    Ok(fh.root_hash)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
