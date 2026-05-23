//! Download engine.
//!
//! Lifecycle of a download:
//!
//!  1. `start()` parses the magnet, sends `NodeCmd::RequestManifest` and
//!     records the pending request in `pending_manifests`.
//!  2. `on_manifest_received()` is called when the manifest arrives:
//!     it enqueues the download in the DB with the full chunk list and
//!     dispatches the first wave of `NodeCmd::RequestChunk` commands.
//!  3. `on_chunk_received()` verifies the BLAKE3 hash, writes the data to
//!     disk at the correct offset, marks the chunk done in the DB, and
//!     dispatches more requests until the download is complete.
//!
//! The engine also handles inbound `ManifestRequested` and `ChunkRequested`
//! events — reading from the local DB/disk and sending back the response.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use libp2p::{PeerId, request_response::OutboundRequestId};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use rucio_core::protocol::{
    manifest::{ManifestRequest, ManifestResponse},
    transfer::{ChunkRequest, ChunkResponse},
};

use crate::db::{self, Db};
use crate::node::messages::NodeCmd;

// ---------------------------------------------------------------------------
// Magnet parser
// ---------------------------------------------------------------------------

pub struct MagnetInfo {
    pub root_hash: [u8; 32],
    pub name: String,
    pub size: u64,
}

pub fn parse_magnet(magnet: &str) -> Result<MagnetInfo> {
    let rest = magnet
        .strip_prefix("rucio:")
        .ok_or_else(|| anyhow!("not a rucio: magnet link"))?;

    let (hash_hex, params) = rest
        .split_once('?')
        .ok_or_else(|| anyhow!("magnet link missing query params"))?;

    let hash_bytes = hex::decode(hash_hex).context("invalid hex in magnet link")?;
    let root_hash: [u8; 32] = hash_bytes
        .try_into()
        .map_err(|_| anyhow!("root hash must be 32 bytes"))?;

    let mut name = String::new();
    let mut size: u64 = 0;

    for part in params.split('&') {
        if let Some(v) = part.strip_prefix("name=") {
            name = urlencoding::decode(v)
                .unwrap_or_else(|_| v.into())
                .into_owned();
        } else if let Some(v) = part.strip_prefix("size=") {
            size = v.parse().context("invalid size in magnet link")?;
        }
    }

    if name.is_empty() {
        bail!("magnet link missing name param");
    }

    Ok(MagnetInfo {
        root_hash,
        name,
        size,
    })
}

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------

/// A download that is waiting for the manifest to arrive.
struct PendingManifest {
    provider: PeerId,
}

/// An active download for which we have the manifest and are fetching chunks.
#[derive(Debug)]
struct ActiveDownload {
    download_id: i64,
    /// Chunks that still need to be fetched: idx → (expected_hash, size).
    pending: HashMap<u32, ([u8; 32], u32)>,
    /// In-flight requests: OutboundRequestId → chunk_idx.
    in_flight: HashMap<OutboundRequestId, u32>,
    provider: PeerId,
    dest_path: PathBuf,
    chunk_size: u32,
}

// ---------------------------------------------------------------------------
// DownloadEngine
// ---------------------------------------------------------------------------

pub struct DownloadEngine {
    db: Db,
    cmd_tx: mpsc::Sender<NodeCmd>,
    dest_dir: PathBuf,
    /// root_hash → pending manifest state.
    pending_manifests: HashMap<[u8; 32], PendingManifest>,
    /// root_hash → active download state.
    active: HashMap<[u8; 32], ActiveDownload>,
    /// OutboundRequestId → root_hash (correlates chunk responses).
    inflight_index: HashMap<OutboundRequestId, [u8; 32]>,
}

impl DownloadEngine {
    pub fn new(db: Db, cmd_tx: mpsc::Sender<NodeCmd>, dest_dir: PathBuf) -> Self {
        Self {
            db,
            cmd_tx,
            dest_dir,
            pending_manifests: HashMap::new(),
            active: HashMap::new(),
            inflight_index: HashMap::new(),
        }
    }

    // -----------------------------------------------------------------------
    // Start: request the manifest
    // -----------------------------------------------------------------------

    pub async fn start(&mut self, magnet: &str, provider: PeerId, _now: u64) -> Result<()> {
        let info = parse_magnet(magnet)?;

        if self.active.contains_key(&info.root_hash)
            || self.pending_manifests.contains_key(&info.root_hash)
        {
            bail!("download already active for this hash");
        }

        self.pending_manifests
            .insert(info.root_hash, PendingManifest { provider });

        self.cmd_tx
            .send(NodeCmd::RequestManifest {
                peer: provider,
                request: ManifestRequest {
                    root_hash: info.root_hash,
                },
            })
            .await
            .ok();

        info!(
            root_hash = hex::encode(info.root_hash),
            "Manifest requested"
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Manifest received: enqueue in DB and start fetching chunks
    // -----------------------------------------------------------------------

    pub async fn on_manifest_received(
        &mut self,
        _request_id: OutboundRequestId,
        _peer: PeerId,
        response: ManifestResponse,
        now: u64,
    ) {
        match response {
            ManifestResponse::Ok {
                root_hash,
                name,
                total_size,
                chunk_size,
                chunks,
            } => {
                let pending = match self.pending_manifests.remove(&root_hash) {
                    Some(p) => p,
                    None => {
                        warn!(
                            root_hash = hex::encode(root_hash),
                            "Manifest for unknown request"
                        );
                        return;
                    }
                };

                let dest_path = self.dest_dir.join(&name);

                let chunk_tuples: Vec<(u32, [u8; 32], u32)> =
                    chunks.iter().map(|c| (c.idx, c.hash, c.size)).collect();

                let dl_id = match db::downloads::enqueue(
                    &self.db,
                    &root_hash,
                    &name,
                    total_size,
                    dest_path.to_str().unwrap_or(&name),
                    now,
                    &chunk_tuples,
                )
                .await
                {
                    Ok(id) => id,
                    Err(e) => {
                        warn!("Failed to enqueue download: {e}");
                        return;
                    }
                };

                // Pre-allocate destination file.
                if let Some(parent) = dest_path.parent() {
                    let _ = fs::create_dir_all(parent).await;
                }
                if let Ok(file) = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&dest_path)
                    .await
                {
                    let _ = file.set_len(total_size).await;
                }

                let mut pending_chunks = HashMap::new();
                for c in &chunks {
                    pending_chunks.insert(c.idx, (c.hash, c.size));
                }

                let dl = ActiveDownload {
                    download_id: dl_id,
                    pending: pending_chunks,
                    in_flight: HashMap::new(),
                    provider: pending.provider,
                    dest_path,
                    chunk_size,
                };

                self.active.insert(root_hash, dl);

                if let Err(e) =
                    db::downloads::set_status(&self.db, dl_id, "downloading", None).await
                {
                    warn!("set_status error: {e}");
                }

                info!(
                    root_hash = hex::encode(root_hash),
                    chunks = chunk_tuples.len(),
                    "Download started"
                );
                self.dispatch_requests(root_hash).await;
            }

            ManifestResponse::NotFound => {
                warn!("Provider returned ManifestNotFound");
                // Clean up pending entry if we can find it — we don't have the
                // root_hash in this branch, so scan by provider is not ideal.
                // The pending entry will be cleaned up on the next start() call
                // for the same hash, which will return an error.
            }

            ManifestResponse::Error(msg) => {
                warn!(%msg, "Provider returned manifest error");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Dispatch chunk requests (pipeline of MAX_INFLIGHT)
    // -----------------------------------------------------------------------

    async fn dispatch_requests(&mut self, root_hash: [u8; 32]) {
        const MAX_INFLIGHT: usize = 4;

        let dl = match self.active.get_mut(&root_hash) {
            Some(d) => d,
            None => return,
        };

        let slots = MAX_INFLIGHT.saturating_sub(dl.in_flight.len());
        if slots == 0 {
            return;
        }

        let in_flight_idxs: std::collections::HashSet<u32> =
            dl.in_flight.values().copied().collect();

        let to_request: Vec<u32> = dl
            .pending
            .keys()
            .copied()
            .filter(|idx| !in_flight_idxs.contains(idx))
            .take(slots)
            .collect();

        let provider = dl.provider;

        for chunk_idx in to_request {
            if self
                .cmd_tx
                .send(NodeCmd::RequestChunk {
                    peer: provider,
                    request: ChunkRequest {
                        root_hash,
                        chunk_idx,
                    },
                })
                .await
                .is_err()
            {
                warn!("node cmd channel closed");
                return;
            }
            debug!(chunk_idx, "Dispatched chunk request");
        }
    }

    // -----------------------------------------------------------------------
    // Register an OutboundRequestId once we know which chunk it belongs to
    // -----------------------------------------------------------------------

    pub fn register_chunk_request(
        &mut self,
        root_hash: [u8; 32],
        chunk_idx: u32,
        request_id: OutboundRequestId,
    ) {
        if let Some(dl) = self.active.get_mut(&root_hash) {
            dl.in_flight.insert(request_id, chunk_idx);
            self.inflight_index.insert(request_id, root_hash);
        }
    }

    // -----------------------------------------------------------------------
    // Chunk response received
    // -----------------------------------------------------------------------

    pub async fn on_chunk_received(
        &mut self,
        request_id: OutboundRequestId,
        _peer: PeerId,
        response: ChunkResponse,
    ) {
        let root_hash = match self.inflight_index.remove(&request_id) {
            Some(h) => h,
            None => {
                debug!(?request_id, "Chunk response for unknown request");
                return;
            }
        };

        let dl = match self.active.get_mut(&root_hash) {
            Some(d) => d,
            None => return,
        };

        let chunk_idx = match dl.in_flight.remove(&request_id) {
            Some(idx) => idx,
            None => return,
        };

        match response {
            ChunkResponse::Ok { data } => {
                let (expected_hash, chunk_size) = match dl.pending.get(&chunk_idx) {
                    Some(v) => *v,
                    None => {
                        warn!(chunk_idx, "Received unsolicited chunk");
                        return;
                    }
                };

                // Verify hash.
                if blake3::hash(&data).as_bytes() != &expected_hash {
                    warn!(chunk_idx, "Chunk hash mismatch — discarding, will retry");
                    self.dispatch_requests(root_hash).await;
                    return;
                }

                // Write to disk.
                let offset = chunk_idx as u64 * dl.chunk_size as u64;
                if let Err(e) = write_chunk(&dl.dest_path, offset, &data).await {
                    warn!(chunk_idx, "Failed to write chunk to disk: {e}");
                    return;
                }

                dl.pending.remove(&chunk_idx);
                let dl_id = dl.download_id;

                if let Err(e) =
                    db::downloads::chunk_done(&self.db, dl_id, chunk_idx, chunk_size).await
                {
                    warn!("DB chunk_done error: {e}");
                }

                debug!(chunk_idx, "Chunk written");

                if dl.pending.is_empty() && dl.in_flight.is_empty() {
                    if let Err(e) =
                        db::downloads::set_status(&self.db, dl_id, "completed", None).await
                    {
                        warn!("set_status completed error: {e}");
                    }
                    info!(root_hash = hex::encode(root_hash), "Download completed");
                    self.active.remove(&root_hash);
                } else {
                    self.dispatch_requests(root_hash).await;
                }
            }

            ChunkResponse::NotFound => {
                warn!(chunk_idx, "Provider does not have chunk");
                let dl_id = dl.download_id;
                let _ = db::downloads::set_status(
                    &self.db,
                    dl_id,
                    "error",
                    Some("provider returned NotFound"),
                )
                .await;
                self.active.remove(&root_hash);
            }

            ChunkResponse::Error(msg) => {
                warn!(chunk_idx, %msg, "Provider chunk error");
                let dl_id = dl.download_id;
                let _ = db::downloads::set_status(&self.db, dl_id, "error", Some(&msg)).await;
                self.active.remove(&root_hash);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Serve an inbound manifest request
    // -----------------------------------------------------------------------

    pub async fn serve_manifest(&self, _peer: PeerId, request: ManifestRequest, channel_id: u64) {
        let db = self.db.clone();
        let cmd_tx = self.cmd_tx.clone();

        tokio::spawn(async move {
            let response = build_manifest_response(&db, &request.root_hash).await;
            let _ = cmd_tx
                .send(NodeCmd::RespondManifest {
                    channel_id,
                    response,
                })
                .await;
        });
    }

    // -----------------------------------------------------------------------
    // Serve an inbound chunk request
    // -----------------------------------------------------------------------

    pub async fn serve_chunk(&self, _peer: PeerId, request: ChunkRequest, channel_id: u64) {
        let db = self.db.clone();
        let cmd_tx = self.cmd_tx.clone();

        tokio::spawn(async move {
            let response = read_chunk_from_db(&db, &request).await;
            let _ = cmd_tx
                .send(NodeCmd::RespondChunk {
                    channel_id,
                    response,
                })
                .await;
        });
    }
}

// ---------------------------------------------------------------------------
// I/O helpers
// ---------------------------------------------------------------------------

async fn write_chunk(path: &PathBuf, offset: u64, data: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .context("opening dest file for write")?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    Ok(())
}

async fn read_chunk_from_db(db: &Db, request: &ChunkRequest) -> ChunkResponse {
    let row = sqlx::query(
        "SELECT c.idx, c.size, sf.path, sf.chunk_size
         FROM chunks c
         JOIN shared_files sf ON sf.id = c.shared_file_id
         WHERE sf.root_hash = ?1 AND c.idx = ?2",
    )
    .bind(request.root_hash.as_slice())
    .bind(request.chunk_idx as i64)
    .fetch_optional(db)
    .await;

    let row = match row {
        Ok(Some(r)) => r,
        Ok(None) => return ChunkResponse::NotFound,
        Err(e) => return ChunkResponse::Error(e.to_string()),
    };

    use sqlx::Row;
    let path: String = row.get("path");
    let chunk_size: i64 = row.get("chunk_size");
    let idx: i64 = row.get("idx");
    let size: i64 = row.get("size");

    let offset = idx as u64 * chunk_size as u64;
    match read_file_range(&path, offset, size as usize).await {
        Ok(data) => ChunkResponse::Ok { data },
        Err(e) => ChunkResponse::Error(e.to_string()),
    }
}

async fn build_manifest_response(db: &Db, root_hash: &[u8; 32]) -> ManifestResponse {
    use sqlx::Row;

    let file_row =
        sqlx::query("SELECT id, name, size, chunk_size FROM shared_files WHERE root_hash = ?1")
            .bind(root_hash.as_slice())
            .fetch_optional(db)
            .await;

    let file_row = match file_row {
        Ok(Some(r)) => r,
        Ok(None) => return ManifestResponse::NotFound,
        Err(e) => return ManifestResponse::Error(e.to_string()),
    };

    let file_id: i64 = file_row.get("id");
    let name: String = file_row.get("name");
    let total_size: i64 = file_row.get("size");
    let chunk_size: i64 = file_row.get("chunk_size");

    let chunk_rows =
        sqlx::query("SELECT idx, hash, size FROM chunks WHERE shared_file_id = ?1 ORDER BY idx")
            .bind(file_id)
            .fetch_all(db)
            .await;

    let chunk_rows = match chunk_rows {
        Ok(r) => r,
        Err(e) => return ManifestResponse::Error(e.to_string()),
    };

    let chunks = chunk_rows
        .iter()
        .map(|r| {
            let idx: i64 = r.get("idx");
            let hash_bytes: Vec<u8> = r.get("hash");
            let size: i64 = r.get("size");
            let mut hash = [0u8; 32];
            if hash_bytes.len() == 32 {
                hash.copy_from_slice(&hash_bytes);
            }
            rucio_core::protocol::manifest::ChunkInfo {
                idx: idx as u32,
                hash,
                size: size as u32,
            }
        })
        .collect();

    ManifestResponse::Ok {
        root_hash: *root_hash,
        name,
        total_size: total_size as u64,
        chunk_size: chunk_size as u32,
        chunks,
    }
}

async fn read_file_range(path: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut file = fs::File::open(path)
        .await
        .with_context(|| format!("opening shared file {path}"))?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf).await?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_magnet_valid() {
        let hash = "a".repeat(64);
        let magnet = format!("rucio:{hash}?name=test.mp3&size=1024");
        let info = parse_magnet(&magnet).unwrap();
        assert_eq!(info.name, "test.mp3");
        assert_eq!(info.size, 1024);
        assert_eq!(hex::encode(info.root_hash), hash);
    }

    #[test]
    fn parse_magnet_wrong_prefix() {
        assert!(parse_magnet("magnet:?xt=urn:btih:abc").is_err());
    }

    #[test]
    fn parse_magnet_missing_name() {
        let hash = "b".repeat(64);
        let magnet = format!("rucio:{hash}?size=100");
        assert!(parse_magnet(&magnet).is_err());
    }

    #[test]
    fn parse_magnet_bad_hex() {
        assert!(parse_magnet("rucio:ZZZZ?name=foo&size=1").is_err());
    }

    #[test]
    fn parse_magnet_wrong_hash_length() {
        assert!(parse_magnet("rucio:deadbeef?name=foo&size=1").is_err());
    }
}
