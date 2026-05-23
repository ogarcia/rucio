//! GET    /api/v1/shares
//! POST   /api/v1/shares
//! DELETE /api/v1/shares/:hash
//! DELETE /api/v1/shares          (query param: path=<prefix>)

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use rucio_core::api::shares::{AddShareRequest, AddShareResponse, ShareResponse, SharesResponse};
use rucio_core::protocol::chunk::CHUNK_SIZE;
use serde::Deserialize;

use crate::api::AppState;
use crate::db;

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

    let shares = rows
        .into_iter()
        .map(|r| ShareResponse {
            root_hash: hex::encode(&r.root_hash),
            name: r.name,
            size: r.size as u64,
            chunk_count: 0, // TODO: join with chunks table
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
        (status = 202, description = "Files queued for indexing", body = AddShareResponse),
        (status = 400, description = "Path does not exist or is not accessible")
    )
)]
pub async fn add_share(
    State(state): State<AppState>,
    Json(req): Json<AddShareRequest>,
) -> Result<(StatusCode, Json<AddShareResponse>), StatusCode> {
    let root = PathBuf::from(&req.path);

    if !root.exists() {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Collect all file paths to index
    let paths = collect_files(&root).map_err(|e| {
        tracing::error!("Failed to collect files under {}: {e}", root.display());
        StatusCode::BAD_REQUEST
    })?;

    let total = paths.len();
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
#[utoipa::path(
    delete,
    path = "/api/v1/shares",
    params(("path" = String, Query, description = "Path or directory prefix to remove")),
    responses(
        (status = 200, description = "Number of shares removed"),
        (status = 400, description = "Missing path parameter")
    )
)]
pub async fn remove_shares_by_path(
    State(state): State<AppState>,
    Query(q): Query<RemoveByPathQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
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

/// Recursively collect all regular files under `root`.
fn collect_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_recursive(root, &mut out)?;
    Ok(out)
}

fn collect_recursive(path: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_file() {
        out.push(path.to_path_buf());
    } else if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            // Skip symlinks to avoid loops
            if ft.is_symlink() {
                continue;
            }
            collect_recursive(&entry.path(), out)?;
        }
    }
    Ok(())
}

/// Hash a single file, split into chunks, and insert into the DB.
/// Returns the root hash on success.
async fn index_file(db: &crate::db::Db, path: &Path) -> anyhow::Result<[u8; 32]> {
    let path_owned = path.to_path_buf();

    // Run blocking I/O on a dedicated thread
    let (root_hash, file_size, chunks) =
        tokio::task::spawn_blocking(move || hash_file(&path_owned)).await??;

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
            root_hash: &root_hash,
            name: &name,
            size: file_size,
            mime_type: None, // TODO: mime detection
            path: &path.to_string_lossy(),
            chunk_size: CHUNK_SIZE,
            added_at: now,
            chunks: &chunks,
        },
    )
    .await?;

    tracing::info!(path = %path.display(), size = file_size, "Indexed file");
    Ok(root_hash)
}

/// (root_hash, file_size_bytes, chunks: Vec<(chunk_idx, chunk_hash, chunk_size)>)
type HashFileResult = ([u8; 32], u64, Vec<(u32, [u8; 32], u32)>);

/// Read a file, split into CHUNK_SIZE chunks, compute per-chunk BLAKE3 hashes
/// and the Merkle root hash.
fn hash_file(path: &Path) -> anyhow::Result<HashFileResult> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut chunks: Vec<(u32, [u8; 32], u32)> = Vec::new();
    let mut file_size: u64 = 0;
    let mut idx: u32 = 0;

    let chunk_sz = CHUNK_SIZE as usize;
    let mut buf = vec![0u8; chunk_sz];

    loop {
        let mut bytes_read = 0;
        // Fill the buffer fully (or until EOF)
        loop {
            let n = file.read(&mut buf[bytes_read..])?;
            if n == 0 {
                break;
            }
            bytes_read += n;
            if bytes_read == chunk_sz {
                break;
            }
        }
        if bytes_read == 0 {
            break;
        }
        let chunk_data = &buf[..bytes_read];
        let hash = *blake3::hash(chunk_data).as_bytes();
        chunks.push((idx, hash, bytes_read as u32));
        file_size += bytes_read as u64;
        idx += 1;
    }

    // Root hash: BLAKE3 over the concatenation of all chunk hashes (Merkle-flat)
    let root_hash = if chunks.is_empty() {
        // Empty file
        *blake3::hash(&[]).as_bytes()
    } else {
        let mut hasher = blake3::Hasher::new();
        for (_, chunk_hash, _) in &chunks {
            hasher.update(chunk_hash);
        }
        *hasher.finalize().as_bytes()
    };

    Ok((root_hash, file_size, chunks))
}
