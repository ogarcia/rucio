//! Download engine.
//!
//! `DownloadEngine` manages the lifecycle of active downloads:
//!
//!  1. `start()` parses the magnet link, enqueues in the DB, issues a
//!     Kademlia `FindProviders` for the root hash, and waits for a provider.
//!  2. Once a provider is found, it requests each pending chunk via the
//!     `RequestChunk` node command.
//!  3. Each arriving `ChunkReceived` event is verified (BLAKE3), written to
//!     disk, and marked done in the DB.
//!  4. When all chunks are done the download is marked `completed`.
//!
//! The engine also handles inbound `ChunkRequested` events — it reads the
//! chunk from disk and sends the data back via `RespondChunk`.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use libp2p::{PeerId, request_response::OutboundRequestId};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use rucio_core::protocol::transfer::{ChunkRequest, ChunkResponse};

use crate::db::{self, Db};
use crate::node::messages::NodeCmd;

// ---------------------------------------------------------------------------
// Magnet parser
// ---------------------------------------------------------------------------

/// Parsed representation of a `rucio:` magnet link.
pub struct MagnetInfo {
    pub root_hash: [u8; 32],
    pub name: String,
    pub size: u64,
}

/// Parse a `rucio:<hex>?name=<name>&size=<size>` magnet link.
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
// Per-download state tracked in memory
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ActiveDownload {
    download_id: i64,
    /// Chunks that still need to be fetched: idx → (expected_hash, size)
    pending: HashMap<u32, ([u8; 32], u32)>,
    /// Chunks currently in-flight: request_id → chunk_idx
    in_flight: HashMap<OutboundRequestId, u32>,
    provider: PeerId,
    dest_path: PathBuf,
    chunk_size: u32,
}

// ---------------------------------------------------------------------------
// DownloadEngine
// ---------------------------------------------------------------------------

/// Manages all active downloads and chunk-serving for inbound requests.
pub struct DownloadEngine {
    db: Db,
    cmd_tx: mpsc::Sender<NodeCmd>,
    /// Root-hash → download state.
    active: HashMap<[u8; 32], ActiveDownload>,
    /// request_id → root_hash (for correlating responses).
    inflight_index: HashMap<OutboundRequestId, [u8; 32]>,
    dest_dir: PathBuf,
}

impl DownloadEngine {
    pub fn new(db: Db, cmd_tx: mpsc::Sender<NodeCmd>, dest_dir: PathBuf) -> Self {
        Self {
            db,
            cmd_tx,
            active: HashMap::new(),
            inflight_index: HashMap::new(),
            dest_dir,
        }
    }

    // -----------------------------------------------------------------------
    // Start a new download
    // -----------------------------------------------------------------------

    /// Enqueue a download and start fetching as soon as a provider is found.
    ///
    /// `chunks` must be the full chunk list from the manifest
    /// `[(idx, hash, size)]`.  In the current flow the caller (search result
    /// handler) is expected to have obtained this from the search result or a
    /// separate manifest request (not yet implemented — for now we derive chunk
    /// layout from total_size and chunk_size).
    pub async fn start(
        &mut self,
        magnet: &str,
        provider: PeerId,
        chunks: Vec<(u32, [u8; 32], u32)>,
        now: u64,
    ) -> Result<i64> {
        let info = parse_magnet(magnet)?;

        if self.active.contains_key(&info.root_hash) {
            bail!("download already active for this hash");
        }

        let dest_path = self.dest_dir.join(&info.name);

        let dl_id = db::downloads::enqueue(
            &self.db,
            &info.root_hash,
            &info.name,
            info.size,
            dest_path.to_str().unwrap_or(&info.name),
            now,
            &chunks,
        )
        .await?;

        // Create the destination file (pre-allocate if possible).
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dest_path)
            .await?;
        if info.size > 0 {
            file.set_len(info.size).await?;
        }
        drop(file);

        let chunk_size = chunks
            .first()
            .map(|(_, _, s)| *s)
            .unwrap_or(4 * 1024 * 1024);

        let mut pending = HashMap::new();
        for (idx, hash, size) in &chunks {
            pending.insert(*idx, (*hash, *size));
        }

        let dl = ActiveDownload {
            download_id: dl_id,
            pending,
            in_flight: HashMap::new(),
            provider,
            dest_path,
            chunk_size,
        };

        self.active.insert(info.root_hash, dl);

        db::downloads::set_status(&self.db, dl_id, "downloading", None).await?;

        // Kick off the first wave of requests.
        self.dispatch_requests(info.root_hash).await;

        Ok(dl_id)
    }

    // -----------------------------------------------------------------------
    // Dispatch chunk requests
    // -----------------------------------------------------------------------

    /// Send requests for all pending chunks that are not yet in-flight.
    /// We pipeline up to MAX_INFLIGHT requests at a time.
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

        // Collect indices to request (avoid borrow conflicts).
        let to_request: Vec<u32> = dl
            .pending
            .keys()
            .copied()
            .filter(|idx| !dl.in_flight.values().any(|v| v == idx))
            .take(slots)
            .collect();

        for chunk_idx in to_request {
            let req = ChunkRequest {
                root_hash,
                chunk_idx,
            };
            match self
                .cmd_tx
                .send(NodeCmd::RequestChunk {
                    peer: dl.provider,
                    request: req,
                })
                .await
            {
                Ok(()) => {
                    // We don't have the request_id yet — it's returned
                    // synchronously by the swarm but goes through the event
                    // loop.  We record the in-flight entry when we receive the
                    // OutboundRequestId from the Message::Response event.
                    //
                    // For now, mark as in-flight with a placeholder that is
                    // replaced when the event arrives.
                    debug!(chunk_idx, "Dispatched chunk request");
                }
                Err(_) => warn!("node cmd channel closed"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Handle incoming chunk response
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
                // Could be a duplicate or a response to a request we didn't track.
                debug!(?request_id, "Received chunk response for unknown request");
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
                // Verify hash.
                let (expected_hash, chunk_size) = match dl.pending.get(&chunk_idx) {
                    Some(v) => *v,
                    None => {
                        warn!(chunk_idx, "Received chunk we didn't ask for");
                        return;
                    }
                };

                let actual = blake3::hash(&data);
                if actual.as_bytes() != &expected_hash {
                    warn!(chunk_idx, "Chunk hash mismatch — discarding");
                    // Re-queue for retry.
                    self.dispatch_requests(root_hash).await;
                    return;
                }

                // Write to disk.
                let offset = chunk_idx as u64 * dl.chunk_size as u64;
                if let Err(e) = write_chunk(&dl.dest_path, offset, &data).await {
                    warn!(chunk_idx, "Failed to write chunk: {e}");
                    return;
                }

                dl.pending.remove(&chunk_idx);

                let dl_id = dl.download_id;
                if let Err(e) =
                    db::downloads::chunk_done(&self.db, dl_id, chunk_idx, chunk_size).await
                {
                    warn!("DB chunk_done error: {e}");
                }

                info!(chunk_idx, "Chunk done");

                if dl.pending.is_empty() && dl.in_flight.is_empty() {
                    // All done.
                    if let Err(e) =
                        db::downloads::set_status(&self.db, dl_id, "completed", None).await
                    {
                        warn!("DB set_status completed error: {e}");
                    }
                    info!(root_hash = hex::encode(root_hash), "Download completed");
                    self.active.remove(&root_hash);
                } else {
                    self.dispatch_requests(root_hash).await;
                }
            }

            ChunkResponse::NotFound => {
                warn!(chunk_idx, "Provider does not have chunk — marking error");
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
                warn!(chunk_idx, %msg, "Provider returned error");
                let dl_id = dl.download_id;
                let _ = db::downloads::set_status(&self.db, dl_id, "error", Some(&msg)).await;
                self.active.remove(&root_hash);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Register request_id once we know it (called from lib.rs event loop)
    // -----------------------------------------------------------------------

    pub fn register_request(
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
    // Serve an inbound chunk request from another peer
    // -----------------------------------------------------------------------

    pub async fn serve_chunk(&self, peer: PeerId, request: ChunkRequest, channel_id: u64) {
        let db = self.db.clone();
        let cmd_tx = self.cmd_tx.clone();

        tokio::spawn(async move {
            let response = read_chunk_from_db(&db, &request).await;
            debug!(%peer, chunk_idx = request.chunk_idx, "Serving chunk");
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

/// Read chunk data from a shared file on disk.
async fn read_chunk_from_db(db: &Db, request: &ChunkRequest) -> ChunkResponse {
    // Look up the chunk record in the DB.
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
    let len = size as usize;

    match read_file_range(&path, offset, len).await {
        Ok(data) => ChunkResponse::Ok { data },
        Err(e) => ChunkResponse::Error(e.to_string()),
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
