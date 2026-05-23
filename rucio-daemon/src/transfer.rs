//! Download engine — multi-source parallel chunk fetcher.
//!
//! ## Lifecycle
//!
//! 1. `start()` — given a magnet and an initial provider list (may be a
//!    single peer from search results, or several from Kademlia), stores
//!    a `PendingManifest` and sends `NodeCmd::RequestManifest` to the first
//!    available provider.
//!
//! 2. `add_providers()` — called whenever Kademlia returns more providers for
//!    a hash that is already pending or active.  New peers are added to the
//!    provider pool immediately.
//!
//! 3. `on_manifest_received()` — populates the chunk list, pre-allocates the
//!    destination file, and starts dispatching chunk requests across all known
//!    providers (round-robin, `SLOTS_PER_PEER` in-flight per peer).
//!
//! 4. `on_chunk_received()` — verifies BLAKE3, writes to disk, marks done in
//!    DB, dispatches more requests.  On hash mismatch the chunk is re-queued
//!    and the offending peer is deprioritised.
//!
//! 5. Completion — when all chunks are written the download is marked
//!    `completed` in the DB.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Result, anyhow, bail};
use libp2p::{PeerId, request_response::OutboundRequestId};
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use rucio_core::protocol::{
    manifest::{ManifestRequest, ManifestResponse},
    transfer::{ChunkRequest, ChunkResponse},
};

use crate::db::{self, Db};
use crate::node::messages::NodeCmd;

// ---------------------------------------------------------------------------
// Tuning
// ---------------------------------------------------------------------------

/// Maximum simultaneous chunk requests **per provider peer**.
const SLOTS_PER_PEER: usize = 4;

/// How long to wait for a manifest response before trying another peer.
const MANIFEST_TIMEOUT_SECS: u64 = 10;

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

    let hash_bytes = hex::decode(hash_hex).map_err(|_| anyhow!("invalid hex in magnet link"))?;
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
            size = v
                .parse()
                .map_err(|_| anyhow!("invalid size in magnet link"))?;
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

/// Download waiting for the manifest to arrive.
struct PendingManifest {
    providers: Vec<PeerId>,
    /// Index into `providers` of the peer we last requested from.
    attempt: usize,
    /// When the last manifest request was sent.
    requested_at: Instant,
    /// DB download id, if the row was created before the manifest arrived.
    /// Set to `None` if the download hasn't been written to the DB yet (i.e.
    /// the manifest request was started but not yet enqueued).
    download_id: Option<i64>,
}

/// Per-peer slot tracking for an active download.
#[derive(Default)]
struct PeerState {
    /// chunk_idx values currently in-flight to this peer.
    in_flight: HashSet<u32>,
}

impl PeerState {
    fn slots_free(&self) -> usize {
        SLOTS_PER_PEER.saturating_sub(self.in_flight.len())
    }
}

/// An active download for which the manifest has been received.
struct ActiveDownload {
    download_id: i64,
    dest_path: PathBuf,
    chunk_size: u32,
    /// Chunks not yet started: ordered queue for fair dispatch.
    queued: VecDeque<u32>,
    /// Chunks that are in-flight or done.
    in_flight: HashSet<u32>,
    /// Chunks whose hash verified and were written to disk.
    done: HashSet<u32>,
    /// Total chunk count (for completion detection).
    total_chunks: usize,
    /// hash and byte-size for each chunk index.
    chunk_meta: HashMap<u32, ([u8; 32], u32)>,
    /// Known providers for this download.
    providers: Vec<PeerId>,
    /// Per-provider slot tracking.
    peer_state: HashMap<PeerId, PeerState>,
    /// in-flight request_id → (peer, chunk_idx).
    inflight_map: HashMap<OutboundRequestId, (PeerId, u32)>,
}

impl ActiveDownload {
    fn is_complete(&self) -> bool {
        self.done.len() == self.total_chunks
    }
}

// ---------------------------------------------------------------------------
// DownloadEngine
// ---------------------------------------------------------------------------

pub struct DownloadEngine {
    db: Db,
    cmd_tx: mpsc::Sender<NodeCmd>,
    dest_dir: PathBuf,
    pending_manifests: HashMap<[u8; 32], PendingManifest>,
    active: HashMap<[u8; 32], ActiveDownload>,
}

impl DownloadEngine {
    pub fn new(db: Db, cmd_tx: mpsc::Sender<NodeCmd>, dest_dir: PathBuf) -> Self {
        Self {
            db,
            cmd_tx,
            dest_dir,
            pending_manifests: HashMap::new(),
            active: HashMap::new(),
        }
    }

    // -----------------------------------------------------------------------
    // Start: given a magnet + initial providers
    // -----------------------------------------------------------------------

    pub async fn start(&mut self, magnet: &str, providers: Vec<PeerId>, _now: u64) -> Result<()> {
        let info = parse_magnet(magnet)?;

        if providers.is_empty() {
            bail!("at least one provider is required to start a download");
        }
        if self.active.contains_key(&info.root_hash)
            || self.pending_manifests.contains_key(&info.root_hash)
        {
            bail!("download already active for this hash");
        }

        // Request the manifest from the first provider; others will serve as
        // chunk sources once the download is active.
        let first = providers[0];
        self.request_manifest(info.root_hash, first).await;

        // Also ask Kademlia for additional providers — they will be added
        // dynamically via add_providers() as they arrive.
        let _ = self
            .cmd_tx
            .send(NodeCmd::FindProviders(info.root_hash.to_vec()))
            .await;

        self.pending_manifests.insert(
            info.root_hash,
            PendingManifest {
                providers,
                attempt: 0,
                requested_at: Instant::now(),
                download_id: None,
            },
        );

        info!(
            root_hash = hex::encode(info.root_hash),
            "Manifest requested"
        );
        Ok(())
    }

    async fn request_manifest(&self, root_hash: [u8; 32], peer: PeerId) {
        let (id_tx, _id_rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(NodeCmd::RequestManifest {
                peer,
                request: ManifestRequest { root_hash },
                id_tx,
            })
            .await;
        // We don't need to correlate manifest responses by request_id because
        // we match on root_hash inside the response payload.
    }

    // -----------------------------------------------------------------------
    // Add providers discovered later (e.g. from Kademlia)
    // -----------------------------------------------------------------------

    pub async fn add_providers(&mut self, root_hash: [u8; 32], new_peers: Vec<PeerId>) {
        if let Some(dl) = self.active.get_mut(&root_hash) {
            let existing: HashSet<PeerId> = dl.providers.iter().copied().collect();
            for p in new_peers {
                if existing.contains(&p) {
                    continue;
                }
                info!(%p, root_hash = hex::encode(root_hash), "New provider added");
                dl.providers.push(p);
            }
            self.dispatch_requests(root_hash).await;
        } else if let Some(pm) = self.pending_manifests.get_mut(&root_hash) {
            let existing: HashSet<PeerId> = pm.providers.iter().copied().collect();
            for p in new_peers {
                if !existing.contains(&p) {
                    pm.providers.push(p);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Manifest timeout / retry
    // -----------------------------------------------------------------------

    /// Check all pending manifest requests. For any that have exceeded
    /// `MANIFEST_TIMEOUT_SECS`, try the next provider in the list.
    /// Entries that have exhausted all providers are dropped and the download
    /// is marked failed in the DB.
    pub async fn tick_manifest_timeouts(&mut self) {
        let mut failed: Vec<[u8; 32]> = Vec::new();

        for (root_hash, pm) in &mut self.pending_manifests {
            if pm.requested_at.elapsed().as_secs() < MANIFEST_TIMEOUT_SECS {
                continue;
            }
            // Try next provider.
            let next_attempt = pm.attempt + 1;
            if next_attempt < pm.providers.len() {
                let peer = pm.providers[next_attempt];
                warn!(
                    root_hash = hex::encode(root_hash),
                    attempt = next_attempt,
                    %peer,
                    "Manifest timed out — retrying with next provider"
                );
                pm.attempt = next_attempt;
                pm.requested_at = Instant::now();
                let (id_tx, _) = oneshot::channel();
                let _ = self
                    .cmd_tx
                    .send(NodeCmd::RequestManifest {
                        peer,
                        request: ManifestRequest {
                            root_hash: *root_hash,
                        },
                        id_tx,
                    })
                    .await;
            } else {
                warn!(
                    root_hash = hex::encode(root_hash),
                    "Manifest timed out — all providers exhausted, failing download"
                );
                failed.push(*root_hash);
            }
        }

        for root_hash in failed {
            self.pending_manifests.remove(&root_hash);
            // Best-effort DB update — no db_id available at this stage so we
            // match by root_hash.
            if let Err(e) = db::downloads::fail_by_hash(&self.db, &root_hash).await {
                warn!(
                    root_hash = hex::encode(root_hash),
                    "Could not mark download failed: {e}"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Cancel
    // -----------------------------------------------------------------------

    /// Stop tracking a download identified by its DB id.
    /// In-flight chunk requests will be silently ignored when they arrive.
    pub async fn cancel(&mut self, download_id: i64) {
        // Check pending manifests first.
        self.pending_manifests
            .retain(|_, pm| pm.download_id != Some(download_id));

        // Check active downloads.
        let hash = self
            .active
            .iter()
            .find(|(_, dl)| dl.download_id == download_id)
            .map(|(h, _)| *h);
        if let Some(h) = hash {
            self.active.remove(&h);
            info!(
                download_id,
                root_hash = hex::encode(h),
                "Download cancelled"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Manifest received
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
                            "Manifest for unknown/duplicate request"
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
                match fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&dest_path)
                    .await
                {
                    Ok(f) => {
                        let _ = f.set_len(total_size).await;
                    }
                    Err(e) => {
                        warn!("Could not pre-allocate {}: {e}", dest_path.display());
                    }
                }

                let mut chunk_meta = HashMap::new();
                let mut queued = VecDeque::new();
                for c in &chunks {
                    chunk_meta.insert(c.idx, (c.hash, c.size));
                    queued.push_back(c.idx);
                }
                let total_chunks = chunk_meta.len();

                let mut peer_state = HashMap::new();
                for &p in &pending.providers {
                    peer_state.insert(p, PeerState::default());
                }

                let dl = ActiveDownload {
                    download_id: dl_id,
                    dest_path,
                    chunk_size,
                    queued,
                    in_flight: HashSet::new(),
                    done: HashSet::new(),
                    total_chunks,
                    chunk_meta,
                    providers: pending.providers,
                    peer_state,
                    inflight_map: HashMap::new(),
                };

                self.active.insert(root_hash, dl);

                if let Err(e) =
                    db::downloads::set_status(&self.db, dl_id, "downloading", None).await
                {
                    warn!("set_status error: {e}");
                }

                info!(
                    root_hash = hex::encode(root_hash),
                    chunks = total_chunks,
                    "Download started"
                );

                self.dispatch_requests(root_hash).await;
            }

            ManifestResponse::NotFound => {
                warn!("Provider returned ManifestNotFound");
            }

            ManifestResponse::Error(msg) => {
                warn!(%msg, "Provider returned manifest error");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Dispatch chunk requests — round-robin across providers
    // -----------------------------------------------------------------------

    async fn dispatch_requests(&mut self, root_hash: [u8; 32]) {
        let dl = match self.active.get_mut(&root_hash) {
            Some(d) => d,
            None => return,
        };

        // Collect (peer, free_slots) for peers that have capacity.
        // We iterate providers in order to keep round-robin stable.
        let mut work: Vec<(PeerId, usize)> = dl
            .providers
            .iter()
            .map(|&p| {
                let free = dl.peer_state.entry(p).or_default().slots_free();
                (p, free)
            })
            .filter(|(_, free)| *free > 0)
            .collect();

        if work.is_empty() || dl.queued.is_empty() {
            return;
        }

        // Assign queued chunks to peers round-robin.
        let mut assigned: Vec<(PeerId, u32)> = Vec::new();
        'outer: loop {
            let mut progress = false;
            for (peer, free) in work.iter_mut() {
                if *free == 0 {
                    continue;
                }
                let Some(chunk_idx) = dl.queued.pop_front() else {
                    break 'outer;
                };
                assigned.push((*peer, chunk_idx));
                *free -= 1;
                progress = true;
                if dl.queued.is_empty() {
                    break 'outer;
                }
            }
            if !progress {
                break;
            }
        }

        // Send the requests — we need to release the mutable borrow of `dl`.
        // assigned is already owned so we can just proceed.
        {
            let dl = self.active.get_mut(&root_hash).unwrap();
            for &(_, chunk_idx) in &assigned {
                dl.in_flight.insert(chunk_idx);
            }
        }

        for (peer, chunk_idx) in assigned {
            let (id_tx, id_rx) = oneshot::channel();
            let cmd = NodeCmd::RequestChunk {
                peer,
                request: ChunkRequest {
                    root_hash,
                    chunk_idx,
                },
                id_tx,
            };
            if self.cmd_tx.send(cmd).await.is_err() {
                warn!("node cmd channel closed");
                return;
            }
            // Get back the OutboundRequestId and record it.
            if let (Ok(request_id), Some(dl)) = (id_rx.await, self.active.get_mut(&root_hash)) {
                dl.inflight_map.insert(request_id, (peer, chunk_idx));
                dl.peer_state
                    .entry(peer)
                    .or_default()
                    .in_flight
                    .insert(chunk_idx);
            }
            debug!(chunk_idx, %peer, "Dispatched chunk request");
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
        // Find which download this belongs to.
        let root_hash = match self
            .active
            .iter()
            .find(|(_, dl)| dl.inflight_map.contains_key(&request_id))
            .map(|(k, _)| *k)
        {
            Some(h) => h,
            None => {
                debug!(?request_id, "Chunk response for unknown request — ignoring");
                return;
            }
        };

        let dl = match self.active.get_mut(&root_hash) {
            Some(d) => d,
            None => return,
        };

        let (peer, chunk_idx) = match dl.inflight_map.remove(&request_id) {
            Some(v) => v,
            None => return,
        };
        dl.in_flight.remove(&chunk_idx);
        if let Some(ps) = dl.peer_state.get_mut(&peer) {
            ps.in_flight.remove(&chunk_idx);
        }

        match response {
            ChunkResponse::Ok { data } => {
                let (expected_hash, chunk_size) = match dl.chunk_meta.get(&chunk_idx) {
                    Some(v) => *v,
                    None => {
                        warn!(chunk_idx, "Received unsolicited chunk");
                        return;
                    }
                };

                // Verify hash.
                if blake3::hash(&data).as_bytes() != &expected_hash {
                    warn!(chunk_idx, %peer, "Chunk hash mismatch — re-queuing");
                    // Re-queue for another peer.
                    dl.queued.push_back(chunk_idx);
                    self.dispatch_requests(root_hash).await;
                    return;
                }

                // Write to disk.
                let offset = chunk_idx as u64 * dl.chunk_size as u64;
                let dest_path = dl.dest_path.clone();
                if let Err(e) = write_chunk(&dest_path, offset, &data).await {
                    warn!(chunk_idx, "Failed to write chunk to disk: {e}");
                    dl.queued.push_back(chunk_idx);
                    return;
                }

                dl.done.insert(chunk_idx);
                let dl_id = dl.download_id;

                if let Err(e) =
                    db::downloads::chunk_done(&self.db, dl_id, chunk_idx, chunk_size).await
                {
                    warn!("DB chunk_done error: {e}");
                }

                debug!(chunk_idx, %peer, "Chunk written");

                if self.active[&root_hash].is_complete() {
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
                warn!(chunk_idx, %peer, "Provider does not have chunk — re-queuing");
                if let Some(dl) = self.active.get_mut(&root_hash) {
                    dl.queued.push_back(chunk_idx);
                }
                self.dispatch_requests(root_hash).await;
            }

            ChunkResponse::Error(msg) => {
                warn!(chunk_idx, %peer, %msg, "Provider chunk error — re-queuing");
                if let Some(dl) = self.active.get_mut(&root_hash) {
                    dl.queued.push_back(chunk_idx);
                }
                self.dispatch_requests(root_hash).await;
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
        .map_err(|e| anyhow!("opening dest file for write: {e}"))?;
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
    use tokio::io::AsyncReadExt;

    let mut file = fs::File::open(path)
        .await
        .map_err(|e| anyhow!("opening shared file {path}: {e}"))?;
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
