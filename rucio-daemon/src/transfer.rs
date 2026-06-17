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
//!    providers (round-robin, up to an adaptive per-peer in-flight cap that
//!    starts at `SLOTS_PER_PEER` and backs off on timeouts — see [`PeerState`]).
//!
//! 4. `on_chunk_received()` — verifies BLAKE3, writes to disk, marks done in
//!    DB, dispatches more requests.  On hash mismatch the chunk is re-queued
//!    and the offending peer is deprioritised.
//!
//! 5. Completion — when all chunks are written the download is marked
//!    `completed` in the DB.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Result, anyhow, bail};
use libp2p::{PeerId, request_response::OutboundRequestId};
use tokio::fs;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tracing::{debug, info, warn};

use rucio_core::protocol::{
    chunk::CHUNK_SIZE,
    have::{HaveRequest, HaveResponse},
    manifest::{ManifestRequest, ManifestResponse},
    transfer::{ChunkRequest, ChunkResponse},
};

use crate::db::{self, Db};
use crate::metrics::Metrics;
use crate::node::messages::NodeCmd;
use crate::throttle::{Priority, TokenBucket};

// ---------------------------------------------------------------------------
// Tuning
// ---------------------------------------------------------------------------

/// Maximum simultaneous chunk requests **per provider peer** — the ceiling the
/// adaptive per-peer concurrency starts at and can recover up to.
const SLOTS_PER_PEER: usize = 4;

/// Floor for the adaptive per-peer concurrency. At one in-flight chunk the peer
/// gets the whole available rate, so a chunk completes in `chunk / rate` — the
/// shortest possible time, which is what lets a slow link finish within the
/// request timeout instead of having its concurrent chunks all miss the
/// deadline together.
const MIN_SLOTS_PER_PEER: usize = 1;

/// Verified chunks in a row before a backed-off peer recovers one slot. Slow
/// enough not to thrash a persistently-slow peer (which would keep over-probing
/// and timing out), but it lets a transiently-slow one ramp back up.
const SLOT_RECOVER_STREAK: u32 = 8;

/// How long to wait for a manifest response before trying another peer.
const MANIFEST_TIMEOUT_SECS: u64 = 10;

/// Consecutive network-level failures **at the concurrency floor** after which a
/// provider is evicted. A peer still failing when we only ask it for one chunk
/// at a time is genuinely gone/unusable; before the floor we back off its
/// concurrency instead of evicting (it's probably just slow, not dead).
const MAX_PEER_FAILURES: u32 = 3;

/// Number of fruitless DHT re-queries after which a download is reported as
/// `stalled` (no providers found).  With the back-off below this is reached
/// after roughly 14 minutes.  Re-querying continues regardless.
const STALL_AFTER_REFINDS: u32 = 3;

/// Exponential back-off for DHT re-queries when no providers are available.
/// Sequence: 2 min, 4 min, 8 min, 16 min, 22 min (cap), …
fn refind_delay_secs(attempt: u32) -> u64 {
    const BASE: u64 = 2 * 60;
    const MAX: u64 = 22 * 60;
    // Cap the shift so we never overflow u64 before the min() clamps us.
    (BASE * (1_u64 << attempt.min(10))).min(MAX)
}

// ---------------------------------------------------------------------------
// Magnet parser
// ---------------------------------------------------------------------------

pub struct MagnetInfo {
    pub root_hash: [u8; 32],
    pub name: Option<String>,
    pub size: Option<u64>,
    pub providers: Vec<String>,
}

pub fn parse_magnet(magnet: &str) -> Result<MagnetInfo> {
    let rest = magnet
        .strip_prefix("rucio:")
        .ok_or_else(|| anyhow!("not a rucio: magnet link"))?;

    let (hash_hex, params) = match rest.split_once('?') {
        Some((h, p)) => (h, Some(p)),
        None => (rest, None),
    };

    let hash_bytes = hex::decode(hash_hex).map_err(|_| anyhow!("invalid hex in magnet link"))?;
    let root_hash: [u8; 32] = hash_bytes
        .try_into()
        .map_err(|_| anyhow!("root hash must be 32 bytes"))?;

    let mut name: Option<String> = None;
    let mut size: Option<u64> = None;
    let mut providers: Vec<String> = Vec::new();

    if let Some(params) = params {
        for part in params.split('&') {
            if let Some(v) = part.strip_prefix("name=") {
                name = Some(
                    urlencoding::decode(v)
                        .unwrap_or_else(|_| v.into())
                        .into_owned(),
                );
            } else if let Some(v) = part.strip_prefix("size=") {
                size = v.parse().ok();
            } else if let Some(v) = part.strip_prefix("provider=")
                && !v.is_empty()
            {
                providers.push(v.to_string());
            }
        }
    }

    Ok(MagnetInfo {
        root_hash,
        name,
        size,
        providers,
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
    /// Row id in the `downloads` table (placeholder inserted at start()).
    db_id: i64,
    /// When we last issued a `FindProviders` DHT query for this hash.
    /// Used to avoid hammering the DHT when no providers are available.
    last_find_at: Instant,
    /// How many times the DHT re-query has been issued with no result.
    /// Drives exponential back-off via `refind_delay_secs()`.
    refind_count: u32,
}

/// Per-peer slot tracking for an active download. Records which chunks are
/// in-flight to the peer (for re-queue on failure) and per-peer stats. The
/// concurrency *limit* lives in [`PeerCap`] (global across downloads), not here.
#[derive(Default)]
struct PeerState {
    /// chunk_idx values currently in-flight to this peer.
    in_flight: HashSet<u32>,
    /// Network-level failures in a row (no successful chunk in between).
    /// Reset to 0 on any verified chunk; drives provider eviction.
    consecutive_failures: u32,
    /// Cumulative bytes received from this peer for the current download.
    /// An `Arc<AtomicU64>` so the net codec can fill it incrementally as a chunk
    /// is read off the wire (flat per-peer download rate), handed over as the
    /// download sink in `RequestChunk`.
    bytes_downloaded: Arc<std::sync::atomic::AtomicU64>,
    /// Per-peer rate sampler state, advanced by `publish_live_stats`.
    last_sample_bytes: u64,
    last_sample_at: Option<std::time::Instant>,
    /// Smoothed per-peer download rate in bytes/s.
    rate_bps: u64,
    /// Network address of the peer, resolved once from the peers DB and cached
    /// (`addr_resolved` distinguishes "not looked up yet" from "no address").
    address: Option<String>,
    addr_resolved: bool,
}

/// Adaptive concurrency cap for a provider peer, shared across all downloads
/// from that peer. Starts at [`SLOTS_PER_PEER`], halved on a chunk timeout
/// (down to [`MIN_SLOTS_PER_PEER`]) so a slow peer ends up serving one chunk at
/// a time — that chunk gets the whole link and completes in chunk/rate, the
/// shortest possible time — and slowly recovered after a run of good chunks.
struct PeerCap {
    max_slots: usize,
    /// Verified chunks since the last cap change, for slow recovery.
    ok_streak: u32,
}

impl Default for PeerCap {
    fn default() -> Self {
        Self {
            max_slots: SLOTS_PER_PEER,
            ok_streak: 0,
        }
    }
}

/// An active download for which the manifest has been received.
struct ActiveDownload {
    download_id: i64,
    dest_path: PathBuf,
    chunk_size: u32,
    /// Total file size in bytes (defines the bao tree and the last chunk size).
    total_size: u64,
    /// Chunks not yet started: ordered queue for fair dispatch.
    queued: VecDeque<u32>,
    /// Chunks that are in-flight or done.
    in_flight: HashSet<u32>,
    /// Chunks whose slice verified and were written to disk.
    done: HashSet<u32>,
    /// Total chunk count (for completion detection).
    total_chunks: usize,
    /// Pre-order bao outboard, grown as each chunk's slice is verified. Shared
    /// behind a mutex so a `spawn_blocking` decode can update it; serialises the
    /// decodes of one download (different downloads use different mutexes).
    /// Persisted to `<dest_path>.obao` for resumption and to serve partial chunks.
    partial_outboard: Arc<std::sync::Mutex<Vec<u8>>>,
    /// Known providers for this download.
    providers: Vec<PeerId>,
    /// Per-provider slot tracking.
    peer_state: HashMap<PeerId, PeerState>,
    /// in-flight request_id → (peer, chunk_idx).
    inflight_map: HashMap<OutboundRequestId, (PeerId, u32)>,
    /// Whether we've announced ourselves as a provider yet (partial sharing).
    /// Flipped on the first completed chunk so we only StartProviding once.
    announced: bool,
    /// Aggregate availability: the OR of every known provider's `/have` bitmap,
    /// LSB-first, one bit per chunk. Tells whether the swarm can supply each
    /// chunk; a bit that stays clear means no current provider holds it.
    /// Refreshed periodically as providers come and go (and fill their own
    /// partial copies).
    available: Vec<u8>,
}

impl ActiveDownload {
    fn is_complete(&self) -> bool {
        self.done.len() == self.total_chunks
    }

    /// OR another provider's have bitmap into our aggregate availability.
    /// Ignores any bits past our own chunk count (defensive against a peer
    /// reporting a longer bitmap).
    fn fold_available(&mut self, other: &[u8]) {
        for (i, byte) in other.iter().enumerate() {
            if let Some(slot) = self.available.get_mut(i) {
                *slot |= *byte;
            }
        }
        // Never let a peer's trailing bits imply chunks we don't have.
        mask_trailing_bits(&mut self.available, self.total_chunks);
    }
}

/// Number of bytes a `bits`-long LSB-first bitmap needs.
fn bitmap_bytes(bits: usize) -> usize {
    bits.div_ceil(8)
}

/// Clear any bits at or beyond `bits` in the final byte so a popcount can't
/// exceed the real chunk count.
fn mask_trailing_bits(map: &mut [u8], bits: usize) {
    let used = bits % 8;
    if used != 0
        && let Some(last) = map.get_mut(bits / 8)
    {
        *last &= (1u8 << used) - 1;
    }
}

// ---------------------------------------------------------------------------
// DownloadEngine
// ---------------------------------------------------------------------------

pub struct DownloadEngine {
    db: Db,
    cmd_tx: mpsc::Sender<NodeCmd>,
    /// Final destination for completed downloads (always shared).
    dest_dir: PathBuf,
    /// Destination for completed downloads that are pinned: kept separate from
    /// the user's download_dir. A download whose root hash is in the `pins`
    /// table lands here instead of dest_dir / its category dir.
    pin_dir: PathBuf,
    /// Temporary directory for in-progress downloads (.part files).
    temp_dir: PathBuf,
    /// Cache of regenerable bao outboards for *completed* shares, keyed by root
    /// hash (`<outboard_dir>/<root_hex>.obao`). Built on demand the first time a
    /// chunk of a complete share is served; deletable at any time (regenerated).
    outboard_dir: PathBuf,
    pending_manifests: HashMap<[u8; 32], PendingManifest>,
    active: HashMap<[u8; 32], ActiveDownload>,
    /// All peers known to have a given file, discovered via DHT or PEX.
    /// Updated by add_providers() regardless of whether a download is active.
    /// Used by serve_chunk() to populate PEX data in chunk responses.
    known_providers: HashMap<[u8; 32], Vec<PeerId>>,
    /// Adaptive concurrency cap per provider peer, **global across all active
    /// downloads** — so N simultaneous downloads from the same peer don't flood
    /// it with N × cap concurrent chunk requests. Backs off on a timeout.
    peer_caps: HashMap<PeerId, PeerCap>,
    /// Shared session metrics — updated on every chunk event.
    metrics: Arc<Metrics>,
    /// Cap on concurrent chunk-upload tasks (semaphore with configurable permits).
    upload_semaphore: Arc<Semaphore>,
    /// VIP upload scheduler: HighID requesters are served before LowID.
    upload_scheduler: Arc<crate::upload_scheduler::UploadScheduler>,
    /// Global download bandwidth throttle (chunks received from remote peers).
    /// Upload rate limiting now lives in the net transfer codec, which paces the
    /// chunk *write* (smooth stream) instead of gating the whole-chunk handoff.
    download_throttle: Arc<TokenBucket>,
    /// Per-download live statistics, shared with the API handlers.
    live_stats: crate::live_stats::LiveStatsMap,
    /// Per-peer active-upload statistics, shared with the API handlers.
    upload_stats: Arc<crate::upload_stats::UploadRegistry>,
    /// Notification service — records a notification when a download completes.
    notifier: crate::notifier::Notifier,
}

impl DownloadEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Db,
        cmd_tx: mpsc::Sender<NodeCmd>,
        dest_dir: PathBuf,
        pin_dir: PathBuf,
        temp_dir: PathBuf,
        metrics: Arc<Metrics>,
        upload_semaphore: Arc<Semaphore>,
        upload_scheduler: Arc<crate::upload_scheduler::UploadScheduler>,
        download_throttle: Arc<TokenBucket>,
        live_stats: crate::live_stats::LiveStatsMap,
        upload_stats: Arc<crate::upload_stats::UploadRegistry>,
        notifier: crate::notifier::Notifier,
    ) -> Self {
        let outboard_dir = outboards_dir(&temp_dir);
        Self {
            db,
            cmd_tx,
            dest_dir,
            pin_dir,
            temp_dir,
            outboard_dir,
            pending_manifests: HashMap::new(),
            active: HashMap::new(),
            known_providers: HashMap::new(),
            peer_caps: HashMap::new(),
            metrics,
            upload_semaphore,
            upload_scheduler,
            download_throttle,
            live_stats,
            upload_stats,
            notifier,
        }
    }

    /// Recompute and publish live stats for every active download.  Called
    /// periodically (~2s) from the main loop. Cheap: in-memory reads plus, on
    /// the first sight of each peer, one DB lookup to resolve its address.
    pub async fn publish_live_stats(&mut self) {
        use rucio_core::api::downloads::DownloadPeerDetail;

        /// One download's recomputed snapshot, built before taking the lock so
        /// the (async) address lookups don't run while the map is write-locked.
        struct Snap {
            id: i64,
            sources_total: u32,
            sources_active: u32,
            pieces_in_flight: u32,
            in_flight_pieces: Vec<u32>,
            peers: Vec<DownloadPeerDetail>,
            /// Bytes received so far across all peers, including partial
            /// in-flight chunks. Updated incrementally by the net read hook, so
            /// it advances smoothly between whole-chunk completions instead of
            /// jumping 4 MiB at a time. Clamped to verified/total at the API.
            bytes_received: u64,
            /// Aggregate availability bitmap (OR of providers' `/have`).
            available: Vec<u8>,
        }

        let db = self.db.clone();
        let now = std::time::Instant::now();
        let mut snaps: Vec<Snap> = Vec::with_capacity(self.active.len());

        for dl in self.active.values_mut() {
            let mut active_peers = 0u32;
            let mut bytes_received = 0u64;
            let mut peers: Vec<DownloadPeerDetail> = Vec::new();
            for (peer_id, ps) in dl.peer_state.iter_mut() {
                if !ps.in_flight.is_empty() {
                    active_peers += 1;
                }
                // Per-peer rate over the interval since the last publish. The
                // byte counter is filled incrementally by the net codec, so the
                // sampled rate is flat for a steady transfer.
                let total = ps
                    .bytes_downloaded
                    .load(std::sync::atomic::Ordering::Relaxed);
                bytes_received += total;
                match ps.last_sample_at {
                    Some(last) => {
                        let elapsed = now.duration_since(last).as_secs_f64();
                        if elapsed >= 0.5 {
                            let delta = total.saturating_sub(ps.last_sample_bytes);
                            ps.rate_bps = (delta as f64 / elapsed) as u64;
                            ps.last_sample_bytes = total;
                            ps.last_sample_at = Some(now);
                        }
                    }
                    None => {
                        ps.last_sample_at = Some(now);
                        ps.last_sample_bytes = total;
                    }
                }
                // Resolve the peer's address once, then cache it.
                if !ps.addr_resolved {
                    ps.address = db::peers::first_addr(&db, &peer_id.to_base58())
                        .await
                        .ok()
                        .flatten();
                    ps.addr_resolved = true;
                }
                // Only surface peers actually contributing or being asked.
                if total > 0 || !ps.in_flight.is_empty() {
                    peers.push(DownloadPeerDetail {
                        peer_id: peer_id.to_base58(),
                        address: ps.address.clone(),
                        bytes_downloaded: total,
                        chunks_in_flight: ps.in_flight.len() as u32,
                        rate_bps: ps.rate_bps,
                    });
                }
            }
            peers.sort_by(|a, b| {
                b.rate_bps
                    .cmp(&a.rate_bps)
                    .then_with(|| b.bytes_downloaded.cmp(&a.bytes_downloaded))
            });
            snaps.push(Snap {
                id: dl.download_id,
                sources_total: dl.providers.len() as u32,
                sources_active: active_peers,
                pieces_in_flight: dl.in_flight.len() as u32,
                in_flight_pieces: dl.in_flight.iter().copied().collect(),
                peers,
                bytes_received,
                available: dl.available.clone(),
            });
        }
        // Pending manifests (no transfer yet) so `show` reports how many
        // providers are lined up before the manifest arrives.
        let pending: Vec<(i64, u32)> = self
            .pending_manifests
            .values()
            .filter(|pm| pm.db_id > 0)
            .map(|pm| (pm.db_id, pm.providers.len() as u32))
            .collect();

        // Take the lock only now, with no awaits held inside.
        let mut map = self.live_stats.write().await;
        for s in snaps {
            let e = map.entry(s.id).or_default();
            e.sources_total = s.sources_total;
            e.sources_active = s.sources_active;
            e.pieces_in_flight = s.pieces_in_flight;
            e.in_flight_pieces = s.in_flight_pieces;
            e.peers = s.peers;
            e.bytes_done = Some(s.bytes_received);
            e.available = s.available;
        }
        for (id, total) in pending {
            let e = map.entry(id).or_default();
            e.sources_total = total;
            e.sources_active = 0;
            e.pieces_in_flight = 0;
            e.in_flight_pieces = Vec::new();
            e.peers = Vec::new();
        }
    }

    // -----------------------------------------------------------------------
    // Resume: rehidrate downloads interrupted by a previous crash/restart
    // -----------------------------------------------------------------------

    /// Called once at startup.  Finds all downloads in `queued` or
    /// `downloading` state in the DB, reconstructs their `ActiveDownload`
    /// in-memory state from the saved chunk rows, and kicks off DHT provider
    /// discovery so transfers resume automatically.
    pub async fn resume_interrupted(&mut self) {
        let rows = match db::downloads::list_resumable(&self.db).await {
            Ok(r) => r,
            Err(e) => {
                warn!("Could not load resumable downloads: {e}");
                return;
            }
        };

        if rows.is_empty() {
            return;
        }

        info!(count = rows.len(), "Resuming interrupted downloads");

        for row in rows {
            self.rehydrate_row(row).await;
        }
    }

    /// Reconstruct a single download's in-memory state from its DB row and saved
    /// chunks, then kick off provider discovery so the transfer continues.
    ///
    /// Shared by [`resume_interrupted`](Self::resume_interrupted) (called once at
    /// startup for every interrupted download) and [`resume`](Self::resume)
    /// (called on demand when the user un-pauses a single download).
    async fn rehydrate_row(&mut self, row: db::downloads::DownloadRow) {
        if row.root_hash.len() != 32 {
            warn!(id = row.id, "Skipping download with malformed root_hash");
            return;
        }
        let mut root_hash = [0u8; 32];
        root_hash.copy_from_slice(&row.root_hash);

        // Skip if already active (shouldn't happen but be safe).
        if self.active.contains_key(&root_hash) || self.pending_manifests.contains_key(&root_hash) {
            return;
        }

        let chunk_rows = match db::downloads::chunks_for(&self.db, row.id).await {
            Ok(c) => c,
            Err(e) => {
                warn!(id = row.id, "Could not load chunks for download: {e}");
                return;
            }
        };

        if chunk_rows.is_empty() {
            // No chunks saved yet — treat as if just queued: request manifest.
            info!(
                id = row.id,
                name = %row.name,
                "No chunks saved; re-requesting manifest"
            );
            if let Err(e) =
                db::downloads::set_status(&self.db, row.id, "finding_providers", None).await
            {
                warn!(id = row.id, "set_status error: {e}");
            }
            let _ = self
                .cmd_tx
                .send(NodeCmd::FindProviders(root_hash.to_vec()))
                .await;
            self.pending_manifests.insert(
                root_hash,
                PendingManifest {
                    providers: vec![],
                    attempt: 0,
                    requested_at: Instant::now(),
                    last_find_at: Instant::now(),
                    db_id: row.id,
                    refind_count: 0,
                },
            );
            return;
        }

        // Derive chunk_size from the first non-last chunk (largest size).
        // Falling back to the canonical CHUNK_SIZE only matters in the
        // degenerate case of a download with no chunk rows — a real one always
        // carries its manifest chunk sizes.
        let chunk_size = chunk_rows
            .iter()
            .map(|c| c.size)
            .max()
            .unwrap_or(CHUNK_SIZE);

        let dest_path = PathBuf::from(&row.dest_path);
        let total_size = row.total_size as u64;

        let mut queued: VecDeque<u32> = VecDeque::new();
        let mut done: HashSet<u32> = HashSet::new();
        for c in &chunk_rows {
            if c.status == "done" {
                done.insert(c.idx);
            } else {
                queued.push_back(c.idx);
            }
        }

        let total_chunks = chunk_rows.len();

        // Reset any 'downloading' chunks back to 'pending' in the DB so
        // their state is consistent (they were interrupted mid-flight).
        if let Err(e) = db::downloads::reset_in_flight_chunks(&self.db, row.id).await {
            warn!(id = row.id, "Could not reset in-flight chunks: {e}");
        }

        // Reload the partial bao outboard (grown as chunks were verified) so we
        // can resume verification and serve the chunks we already hold; start
        // empty if the sidecar is missing or the wrong size.
        let want_len = rucio_core::protocol::bao::outboard_len(total_size);
        let partial = match std::fs::read(partial_outboard_path(&dest_path)) {
            Ok(b) if b.len() == want_len => b,
            _ => vec![0u8; want_len],
        };

        let done_count = done.len();
        let dl = ActiveDownload {
            download_id: row.id,
            dest_path,
            chunk_size,
            total_size,
            queued,
            in_flight: HashSet::new(),
            done,
            total_chunks,
            partial_outboard: Arc::new(std::sync::Mutex::new(partial)),
            providers: vec![],
            peer_state: HashMap::new(),
            inflight_map: HashMap::new(),
            announced: false,
            available: vec![0u8; bitmap_bytes(total_chunks)],
        };

        self.active.insert(root_hash, dl);

        // Update status to 'downloading' and kick off DHT discovery.
        if let Err(e) = db::downloads::set_status(&self.db, row.id, "downloading", None).await {
            warn!(id = row.id, "set_status error: {e}");
        }

        let _ = self
            .cmd_tx
            .send(NodeCmd::FindProviders(root_hash.to_vec()))
            .await;

        info!(
            id = row.id,
            name = %row.name,
            done = done_count,
            total = total_chunks,
            "Download resumed"
        );
    }

    // -----------------------------------------------------------------------
    // Start: given a magnet + initial providers
    // -----------------------------------------------------------------------

    pub async fn start(
        &mut self,
        magnet: &str,
        extra_providers: Vec<PeerId>,
        now: u64,
        category_id: Option<i64>,
    ) -> Result<()> {
        let info = parse_magnet(magnet)?;

        if self.active.contains_key(&info.root_hash)
            || self.pending_manifests.contains_key(&info.root_hash)
        {
            bail!("download already active for this hash");
        }

        // Merge providers from the magnet link itself with any supplied by the
        // caller (e.g. from a gossip search result).  Magnet-embedded providers
        // are tried first since the sender already verified they're live.
        let mut providers: Vec<PeerId> = info
            .providers
            .iter()
            .filter_map(|s| s.parse::<PeerId>().ok())
            .collect();
        for p in extra_providers {
            if !providers.contains(&p) {
                providers.push(p);
            }
        }

        // Insert a placeholder row so `rucio downloads` can show the state
        // immediately, before the manifest arrives.
        let db_id = match db::downloads::create_pending(
            &self.db,
            &info.root_hash,
            info.name.as_deref(),
            now,
            !providers.is_empty(),
            category_id,
        )
        .await
        {
            Ok(db::downloads::CreatePendingResult::AlreadyCompleted(id)) => {
                bail!(
                    "download already completed (id={id}); remove it from history first if you want to re-download"
                );
            }
            Ok(db::downloads::CreatePendingResult::AlreadyActive(id)) => {
                bail!("download already active (id={id})");
            }
            Ok(r) => r.id(),
            Err(e) => {
                warn!("create_pending failed: {e}");
                0
            }
        };

        // Always ask Kademlia for providers — they will be added dynamically
        // via add_providers() as they arrive, even if we already have some.
        let _ = self
            .cmd_tx
            .send(NodeCmd::FindProviders(info.root_hash.to_vec()))
            .await;

        // If we already have at least one provider, request the manifest
        // immediately for a fast start.  Otherwise we wait for DHT to return
        // providers via add_providers().
        if let Some(&first) = providers.first() {
            self.request_manifest(info.root_hash, first).await;
        }

        self.pending_manifests.insert(
            info.root_hash,
            PendingManifest {
                providers,
                attempt: 0,
                requested_at: Instant::now(),
                last_find_at: Instant::now(),
                db_id,
                refind_count: 0,
            },
        );

        info!(
            root_hash = hex::encode(info.root_hash),
            "Download queued — waiting for manifest"
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
        // Always update the global known_providers map — used for PEX even
        // when we are not downloading this file ourselves.
        const MAX_KNOWN: usize = 32;
        {
            let known = self.known_providers.entry(root_hash).or_default();
            let existing: HashSet<PeerId> = known.iter().copied().collect();
            for &p in &new_peers {
                if !existing.contains(&p) && known.len() < MAX_KNOWN {
                    known.push(p);
                }
            }
        }

        if let Some(dl) = self.active.get_mut(&root_hash) {
            let existing: HashSet<PeerId> = dl.providers.iter().copied().collect();
            for p in new_peers {
                if existing.contains(&p) {
                    continue;
                }
                info!(%p, root_hash = hex::encode(root_hash), "New provider added to active download");
                dl.providers.push(p);
            }
            self.dispatch_requests(root_hash).await;
        } else if let Some(pm) = self.pending_manifests.get_mut(&root_hash) {
            let had_providers = !pm.providers.is_empty();
            let existing: HashSet<PeerId> = pm.providers.iter().copied().collect();
            for p in new_peers {
                if !existing.contains(&p) {
                    info!(%p, root_hash = hex::encode(root_hash), "New provider added to pending manifest");
                    pm.providers.push(p);
                }
            }
            // If we had no providers before (pure DHT-only start), kick off the
            // manifest request now that we have our first peer, and update the
            // DB status from 'finding_providers' to 'queued'.
            if !had_providers && let Some(&first) = pm.providers.first() {
                pm.requested_at = Instant::now();
                let db_id = pm.db_id;
                if db_id > 0 {
                    let _ = db::downloads::set_status(&self.db, db_id, "queued", None).await;
                }
                self.request_manifest(root_hash, first).await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Manifest timeout / retry
    // -----------------------------------------------------------------------

    /// Check all pending manifest requests. For any that have exceeded
    /// `MANIFEST_TIMEOUT_SECS`, try the next provider in the list.
    /// When all known providers are exhausted the download goes back to
    /// `finding_providers` and re-issues a `FindProviders` DHT query —
    /// it never fails permanently, matching the behaviour of the eMule client.
    pub async fn tick_manifest_timeouts(&mut self) {
        let mut retry_find: Vec<[u8; 32]> = Vec::new();

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
            } else if pm.last_find_at.elapsed().as_secs() >= refind_delay_secs(pm.refind_count) {
                // All known providers exhausted.  Re-query the DHT instead of
                // giving up — the file may become available later.
                let delay = refind_delay_secs(pm.refind_count);
                warn!(
                    root_hash = hex::encode(root_hash),
                    retry_in_secs = delay,
                    "Manifest timed out — all providers exhausted, re-querying DHT"
                );
                pm.providers.clear();
                pm.attempt = 0;
                pm.refind_count += 1;
                pm.last_find_at = Instant::now();
                retry_find.push(*root_hash);
            }
            // If elapsed < refind_delay we just wait — the tick will fire
            // again in MANIFEST_TIMEOUT_SECS and re-evaluate.
        }

        for root_hash in retry_find {
            if let Some(pm) = self.pending_manifests.get(&root_hash) {
                let db_id = pm.db_id;
                if db_id > 0 {
                    // After several fruitless re-queries, surface as `stalled`.
                    let status = if pm.refind_count >= STALL_AFTER_REFINDS {
                        "stalled"
                    } else {
                        "finding_providers"
                    };
                    let _ = db::downloads::set_status(&self.db, db_id, status, None).await;
                }
            }
            let _ = self
                .cmd_tx
                .send(NodeCmd::FindProviders(root_hash.to_vec()))
                .await;
        }
    }

    // -----------------------------------------------------------------------
    // Periodic provider refresh — only when a download is stalled
    // -----------------------------------------------------------------------

    /// Re-issue `FindProviders` for downloads that are stalled: chunks are
    /// queued but nothing is in-flight, meaning we have no reachable peers.
    /// Also re-queries for pending manifests that have no providers yet
    /// (pure DHT-only start still waiting for the first peer).
    ///
    /// This is intentionally conservative — we do *not* re-query on a fixed
    /// timer for healthy downloads.  The seeder side handles reproviding so
    /// that new peers become discoverable; we only pay the DHT query cost
    /// when we actually need new peers.
    pub async fn tick_provider_refresh(&mut self) {
        // Pending manifests with no providers — still waiting for first DHT result.
        let stalled_pending: Vec<[u8; 32]> = self
            .pending_manifests
            .iter()
            .filter(|(_, pm)| pm.providers.is_empty())
            .map(|(h, _)| *h)
            .collect();

        // Active downloads where chunks are queued but nothing is in-flight.
        let stalled_active: Vec<[u8; 32]> = self
            .active
            .iter()
            .filter(|(_, dl)| !dl.queued.is_empty() && dl.in_flight.is_empty())
            .map(|(h, _)| *h)
            .collect();

        for hash in stalled_pending.into_iter().chain(stalled_active) {
            debug!(
                root_hash = hex::encode(hash),
                "Download stalled — re-querying DHT for providers"
            );
            let _ = self
                .cmd_tx
                .send(NodeCmd::FindProviders(hash.to_vec()))
                .await;
        }
    }

    // -----------------------------------------------------------------------
    // Cancel
    // -----------------------------------------------------------------------

    /// Stop tracking a download identified by its DB id and root hash.
    /// Covers both active downloads and pending manifest requests.
    /// In-flight chunk/manifest responses that arrive afterwards are silently
    /// discarded by the existing "unknown request" guards.
    pub async fn cancel(&mut self, download_id: i64, root_hash: Vec<u8>, delete_row: bool) {
        self.live_stats.write().await.remove(&download_id);

        // Drop in-memory state and remember the .part path if we had it live.
        let hash_arr: Option<[u8; 32]> = <[u8; 32]>::try_from(root_hash.as_slice()).ok();
        let mut part_path: Option<PathBuf> = None;
        // The hash to stop providing (partial sharing) — set once we know it.
        let mut stop_hash: Option<Vec<u8>> = None;
        if let Some(h) = hash_arr {
            stop_hash = Some(h.to_vec());
            // Remove a pending manifest (no .part exists yet at this stage).
            if self.pending_manifests.remove(&h).is_some() {
                info!(
                    download_id,
                    root_hash = hex::encode(h),
                    "Cancelled pending manifest"
                );
            }
            // Remove from active downloads (manifest already arrived).
            if let Some(dl) = self.active.remove(&h) {
                part_path = Some(dl.dest_path);
                info!(
                    download_id,
                    root_hash = hex::encode(h),
                    "Download cancelled"
                );
            }
        } else if let Some(h) = self
            .active
            .iter()
            .find(|(_, dl)| dl.download_id == download_id)
            .map(|(h, _)| *h)
            && let Some(dl) = self.active.remove(&h)
        {
            stop_hash = Some(h.to_vec());
            part_path = Some(dl.dest_path);
            info!(
                download_id,
                root_hash = hex::encode(h),
                "Download cancelled"
            );
        }

        // If the download wasn't tracked in memory (stalled, paused, or not yet
        // rehydrated after a restart), fall back to the DB row's dest_path —
        // otherwise its .part would leak. While downloading, dest_path is the
        // .part; it only becomes the final file once completed.
        if part_path.is_none()
            && let Ok(Some(row)) = db::downloads::get(&self.db, download_id).await
            && !row.dest_path.is_empty()
        {
            part_path = Some(PathBuf::from(row.dest_path));
        }

        // Only ever delete a `.part`: never a completed file the user owns.
        if let Some(path) = part_path
            && path.extension().is_some_and(|e| e == "part")
        {
            if let Err(e) = tokio::fs::remove_file(&path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                warn!(path = %path.display(), "Could not remove .part file on cancel: {e}");
            }
            // Drop its outboard sidecar too, so a cancelled download leaves
            // nothing behind in temp_dir.
            let _ = tokio::fs::remove_file(partial_outboard_path(&path)).await;
        }

        // Stop announcing it: the .part (and its verified chunks) are gone, so we
        // can no longer serve any part of this file (partial sharing). Harmless
        // if we never announced it.
        if let Some(h) = stop_hash {
            let _ = self.cmd_tx.send(NodeCmd::StopProviding(h)).await;
        }

        // Mirror evictions remove the row outright (after the .part cleanup
        // above, so its dest_path was still available); a user cancel keeps the
        // `cancelled` entry in the list.
        if delete_row {
            let _ = db::downloads::delete(&self.db, download_id).await;
        }
    }

    /// Suspend a download: drop its in-memory state but keep the partial file
    /// and the per-chunk progress in the DB so it can be resumed later.
    ///
    /// Unlike [`cancel`](Self::cancel) this does **not** delete the `.part`
    /// file.  The caller is responsible for setting the DB status to `paused`.
    pub async fn pause(&mut self, download_id: i64, root_hash: Vec<u8>) {
        self.live_stats.write().await.remove(&download_id);

        let hash_arr: Option<[u8; 32]> = root_hash.try_into().ok();
        let removed = if let Some(h) = hash_arr {
            let pending = self.pending_manifests.remove(&h).is_some();
            let active = self.active.remove(&h).is_some();
            pending || active
        } else {
            // Fallback: search active downloads by download_id.
            let found = self
                .active
                .iter()
                .find(|(_, dl)| dl.download_id == download_id)
                .map(|(h, _)| *h);
            match found {
                Some(h) => self.active.remove(&h).is_some(),
                None => false,
            }
        };

        if removed {
            info!(download_id, "Download paused");
        } else {
            // Not tracked in memory (e.g. already idle / stalled).  The DB
            // status change alone is enough to keep it paused.
            info!(download_id, "Download paused (was not active in engine)");
        }
    }

    /// Resume a previously paused download by re-hydrating it from the DB.
    /// Reuses the same path as startup recovery.
    pub async fn resume(&mut self, download_id: i64) {
        let row = match db::downloads::get(&self.db, download_id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                warn!(download_id, "Resume requested for unknown download");
                return;
            }
            Err(e) => {
                warn!(download_id, "DB error on resume: {e}");
                return;
            }
        };
        self.rehydrate_row(row).await;
    }

    /// Rename an in-progress download: move its `.part` to `<new_name>.part`
    /// and repoint the in-memory state plus the DB `dest_path` so it completes
    /// under the new name. The `name` column itself is updated by the API
    /// handler; completed downloads are never renamed (the handler rejects
    /// them — the file already belongs to the user).
    ///
    /// `new_name` is a bare, already-sanitised file name.
    pub async fn rename(&mut self, download_id: i64, new_name: String) {
        let new_part = self.temp_dir.join(format!("{new_name}.part"));

        // Active in memory (manifest already arrived): repoint live state too.
        let active_hash = self
            .active
            .iter()
            .find(|(_, dl)| dl.download_id == download_id)
            .map(|(h, _)| *h);

        if let Some(h) = active_hash {
            let old_part = self.active[&h].dest_path.clone();
            if old_part != new_part {
                if let Err(e) = tokio::fs::rename(&old_part, &new_part).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(download_id, old = %old_part.display(), new = %new_part.display(),
                        "Could not move .part on rename: {e}");
                    return;
                }
                // Keep the outboard sidecar alongside its `.part`.
                let _ = tokio::fs::rename(
                    partial_outboard_path(&old_part),
                    partial_outboard_path(&new_part),
                )
                .await;
                self.active.get_mut(&h).unwrap().dest_path = new_part.clone();
                if let Err(e) = db::downloads::set_dest_path(
                    &self.db,
                    download_id,
                    new_part.to_string_lossy().as_ref(),
                )
                .await
                {
                    warn!(download_id, "Could not update dest_path on rename: {e}");
                }
            }
            info!(download_id, name = %new_name, "Download renamed");
            return;
        }

        // Not active in memory (finding providers / queued / paused-not-rehydrated):
        // rename the .part on disk if the DB already points at one. If there is
        // no .part yet, only the `name` column matters — `on_manifest` reads it
        // to create the .part under the new name.
        if let Ok(Some(row)) = db::downloads::get(&self.db, download_id).await
            && !row.dest_path.is_empty()
        {
            let old_part = PathBuf::from(&row.dest_path);
            if old_part != new_part {
                if let Err(e) = tokio::fs::rename(&old_part, &new_part).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(download_id, "Could not move .part on rename: {e}");
                    return;
                }
                // Keep the outboard sidecar alongside its `.part`.
                let _ = tokio::fs::rename(
                    partial_outboard_path(&old_part),
                    partial_outboard_path(&new_part),
                )
                .await;
                if let Err(e) = db::downloads::set_dest_path(
                    &self.db,
                    download_id,
                    new_part.to_string_lossy().as_ref(),
                )
                .await
                {
                    warn!(download_id, "Could not update dest_path on rename: {e}");
                }
            }
        }
        info!(download_id, name = %new_name, "Download renamed (not active in engine)");
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
                chunk_count,
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

                // Prefer the name already chosen by the user — the magnet's
                // name, or a rename applied while finding providers — over
                // whatever the remote peer declares. The hash is the identity;
                // the name is a local choice. Fall back to the peer's name only
                // if the row somehow has none.
                let dl_id = pending.db_id;
                let chosen_name = match db::downloads::get(&self.db, dl_id).await {
                    Ok(Some(r)) if !r.name.trim().is_empty() => r.name,
                    _ => name.clone(),
                };

                // In-progress downloads go to temp_dir, named with the hash frag
                // so two same-named downloads don't share (and corrupt) one
                // `.part`. The final, user-facing name is resolved at completion.
                let dest_path = self.temp_dir.join(format!(
                    "{}.part",
                    name_with_frag(&chosen_name, &name_frag(&root_hash))
                ));

                let total_chunks = chunk_count as usize;
                // Per-chunk byte sizes derive from the geometry (every chunk is
                // `chunk_size` except a possibly-shorter last one) — there are no
                // per-chunk hashes any more; verification is against the root.
                let chunk_tuples: Vec<(u32, u32)> = (0..chunk_count)
                    .map(|idx| {
                        let start = idx as u64 * chunk_size as u64;
                        let sz = (total_size - start).min(chunk_size as u64) as u32;
                        (idx, sz)
                    })
                    .collect();

                // Use the placeholder row created at start(), updating it with
                // the real manifest data and inserting chunk rows.
                if let Err(e) = db::downloads::finalize_pending(
                    &self.db,
                    dl_id,
                    &chosen_name,
                    total_size,
                    dest_path.to_str().unwrap_or(&chosen_name),
                    now,
                    &chunk_tuples,
                )
                .await
                {
                    warn!("Failed to finalize download in DB: {e}");
                    return;
                }

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

                let queued: VecDeque<u32> = (0..chunk_count).collect();

                let mut peer_state = HashMap::new();
                for &p in &pending.providers {
                    peer_state.insert(p, PeerState::default());
                }

                let dl = ActiveDownload {
                    download_id: dl_id,
                    dest_path,
                    chunk_size,
                    total_size,
                    queued,
                    in_flight: HashSet::new(),
                    done: HashSet::new(),
                    total_chunks,
                    partial_outboard: Arc::new(std::sync::Mutex::new(vec![
                        0u8;
                        rucio_core::protocol::bao::outboard_len(total_size)
                    ])),
                    providers: pending.providers,
                    peer_state,
                    inflight_map: HashMap::new(),
                    announced: false,
                    available: vec![0u8; bitmap_bytes(total_chunks)],
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
        // Snapshot this download's providers (don't hold a borrow — we need
        // `&self.active` below to sum the *global* per-peer in-flight count).
        let providers: Vec<PeerId> = match self.active.get(&root_hash) {
            Some(d) if !d.queued.is_empty() => d.providers.clone(),
            _ => return,
        };
        if providers.is_empty() {
            return;
        }

        // Free slots per provider, bounded by the GLOBAL per-peer cap: the cap
        // counts chunks in-flight to that peer across *all* active downloads, so
        // many simultaneous downloads from one peer (e.g. a batch of mirror
        // fetches) can't flood it with cap × N concurrent requests — which would
        // share its bandwidth so thinly that every request times out.
        let mut work: Vec<(PeerId, usize)> = Vec::new();
        for p in providers {
            let used: usize = self
                .active
                .values()
                .filter_map(|d| d.peer_state.get(&p))
                .map(|ps| ps.in_flight.len())
                .sum();
            let cap = self.peer_caps.entry(p).or_default().max_slots;
            let free = cap.saturating_sub(used);
            if free > 0 {
                work.push((p, free));
            }
        }
        if work.is_empty() {
            return;
        }

        let dl = self.active.get_mut(&root_hash).unwrap();

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
            // This peer's cumulative-bytes counter, for net to fill as it reads
            // the chunk — a flat per-peer download rate in the detail view.
            let download_sink = self
                .active
                .get_mut(&root_hash)
                .map(|dl| Arc::clone(&dl.peer_state.entry(peer).or_default().bytes_downloaded));
            let cmd = NodeCmd::RequestChunk {
                peer,
                request: ChunkRequest {
                    root_hash,
                    chunk_idx,
                },
                download_sink,
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

    /// Re-dispatch every active download that still has queued chunks. Called
    /// periodically: with the global per-peer cap, a download can hold none of a
    /// peer's slots while another download uses them, and then has no event of
    /// its own to resume it when those slots free up. This safety tick picks it
    /// back up (within the tick interval) so downloads don't stall behind each
    /// other on a shared, capacity-limited provider.
    pub async fn dispatch_idle(&mut self) {
        let hashes: Vec<[u8; 32]> = self
            .active
            .iter()
            .filter(|(_, dl)| !dl.queued.is_empty())
            .map(|(h, _)| *h)
            .collect();
        for h in hashes {
            self.dispatch_requests(h).await;
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
            ChunkResponse::Ok {
                data,
                peers: pex_peers,
            } => {
                // Process PEX peers — parse before mutably borrowing self further.
                let pex: Vec<PeerId> = pex_peers.iter().filter_map(|s| s.parse().ok()).collect();

                if chunk_idx as usize >= dl.total_chunks {
                    warn!(chunk_idx, "Received unsolicited chunk");
                    return;
                }
                let nominal = dl.chunk_size;
                let total_size = dl.total_size;
                // Actual byte length of this chunk (last one may be shorter).
                let chunk_size =
                    (total_size - chunk_idx as u64 * nominal as u64).min(nominal as u64) as u32;
                let dest_path = dl.dest_path.clone();
                let ob = Arc::clone(&dl.partial_outboard);

                // Throttle download bandwidth before the (CPU + disk) verify.
                // Rucio transfers take priority over eMule on the shared cap.
                self.download_throttle
                    .acquire(data.len() as u64, Priority::High)
                    .await;

                // Verify the bao slice against the file root, write the verified
                // bytes at their offset, and fold the slice's proof nodes into the
                // partial outboard — all on a blocking thread, under the download's
                // lock (which serialises this download's decodes; other downloads
                // use other locks). The outboard is cloned so a failed verify
                // leaves the accumulated tree intact.
                let verify = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    let ranges =
                        rucio_core::protocol::bao::chunk_ranges(chunk_idx, nominal, total_size);
                    let mut dest = std::fs::OpenOptions::new().write(true).open(&dest_path)?;
                    let mut guard = ob.lock().unwrap();
                    let updated = rucio_core::protocol::bao::decode_slice_into(
                        &data,
                        &ranges,
                        &mut dest,
                        blake3::Hash::from_bytes(root_hash),
                        total_size,
                        guard.clone(),
                    )
                    .map_err(|e| anyhow::anyhow!("slice verify failed: {e}"))?;
                    // Persist for resumption / partial sharing before releasing.
                    let _ = std::fs::write(partial_outboard_path(&dest_path), &updated);
                    *guard = updated;
                    Ok(())
                })
                .await;

                if !matches!(verify, Ok(Ok(()))) {
                    warn!(chunk_idx, %peer, "Chunk slice failed verification — re-queuing");
                    self.metrics.record_rejected();
                    dl.queued.push_back(chunk_idx);
                    self.dispatch_requests(root_hash).await;
                    return;
                }

                // Bytes (global + this peer's tally) were accounted incrementally
                // as the chunk was read off the wire (the net codec's read hooks →
                // flat download speed); here we only count the completed chunk.
                self.metrics.record_download_chunk();

                // A good chunk clears this peer's failure streak.
                if let Some(ps) = dl.peer_state.get_mut(&peer) {
                    ps.consecutive_failures = 0;
                }
                // After a sustained good run, recover one global concurrency
                // slot for this peer, so one that was only transiently slow
                // (or backed off by an unrelated download) ramps back up.
                let cap = self.peer_caps.entry(peer).or_default();
                if cap.max_slots < SLOTS_PER_PEER {
                    cap.ok_streak += 1;
                    if cap.ok_streak >= SLOT_RECOVER_STREAK {
                        cap.max_slots += 1;
                        cap.ok_streak = 0;
                    }
                } else {
                    cap.ok_streak = 0;
                }

                dl.done.insert(chunk_idx);
                let dl_id = dl.download_id;

                if let Err(e) =
                    db::downloads::chunk_done(&self.db, dl_id, chunk_idx, chunk_size).await
                {
                    warn!("DB chunk_done error: {e}");
                }

                debug!(chunk_idx, %peer, "Chunk written");

                // Partial sharing: now that we hold a verified chunk, announce
                // ourselves as a provider so other peers can pull the parts we
                // already have (read straight from the .part). Announce once.
                let announce = self
                    .active
                    .get_mut(&root_hash)
                    .map(|dl| !std::mem::replace(&mut dl.announced, true))
                    .unwrap_or(false);
                if announce {
                    let _ = self
                        .cmd_tx
                        .send(NodeCmd::StartProviding(root_hash.to_vec()))
                        .await;
                    debug!(
                        root_hash = hex::encode(root_hash),
                        "Partial sharing: announced as provider while downloading"
                    );
                }

                // Incorporate PEX peers from this response.
                if !pex.is_empty() {
                    debug!(count = pex.len(), "PEX peers received");
                    self.add_providers(root_hash, pex).await;
                }

                if self.active[&root_hash].is_complete() {
                    let part_path = self.active[&root_hash].dest_path.clone();
                    // The clean, user-facing name (the DB `name` column — the
                    // `.part` is now named with a hash frag, so its stem isn't it).
                    // Used for the final file, the notification, and disambiguation.
                    let name = db::downloads::get(&self.db, dl_id)
                        .await
                        .ok()
                        .flatten()
                        .map(|r| r.name)
                        .filter(|n| !n.trim().is_empty())
                        .unwrap_or_else(|| {
                            part_path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .map(|n| n.strip_suffix(".part").unwrap_or(n).to_string())
                                .unwrap_or_default()
                        });
                    let hash_hex = hex::encode(root_hash);

                    // Resolve where this download lands, in precedence order:
                    //   1. an explicit category the user assigned (its dir, or the
                    //      global dir if the category pins none) — user intent wins,
                    //      even when the download is also pinned;
                    //   2. otherwise, if pinned manually or mirrored for a
                    //      subscription, the dedicated pin_dir;
                    //   3. otherwise the global download_dir.
                    // Pinning keeps content shared/re-provided wherever it lives, so
                    // honouring the category doesn't weaken the pin. Resolved now,
                    // not at start, so a category/pin edited mid-download is honoured.
                    let cat_id = db::downloads::get_category_id(&self.db, dl_id)
                        .await
                        .ok()
                        .flatten();
                    let dest_dir = if cat_id.is_some() {
                        db::categories::resolve_dir(&self.db, &self.dest_dir, cat_id).await
                    } else if db::pins::exists(&self.db, &root_hash)
                        .await
                        .unwrap_or(false)
                        || db::mirror_pins::is_wanted(&self.db, &root_hash)
                            .await
                            .unwrap_or(false)
                    {
                        self.pin_dir.clone()
                    } else {
                        self.dest_dir.clone()
                    };

                    // Persist the fully-verified `.part` into the download dir.
                    // `persist_completed` (re)creates that dir first — the user
                    // may have deleted it while we ran. Any failure (dir gone and
                    // unrecreatable, no write permission, full disk) is recoverable:
                    // the `.part` is left untouched, so we mark the download failed
                    // — never a phantom "completed" — and notify so the user can
                    // fix the folder and retry.
                    match persist_completed(&dest_dir, &part_path, &name, &name_frag(&root_hash))
                        .await
                    {
                        Ok(final_path) => {
                            info!(
                                from = %part_path.display(),
                                to   = %final_path.display(),
                                "Download moved to download_dir"
                            );
                            // The accumulated `.part.obao` is the full outboard
                            // (every chunk was verified). Promote it to the share
                            // outboard cache so serving doesn't recompute it; if
                            // this fails the producer regenerates from the file.
                            let part_obao = partial_outboard_path(&part_path);
                            let share_obao = share_outboard_path(&self.outboard_dir, &root_hash);
                            if let Some(parent) = share_obao.parent() {
                                let _ = tokio::fs::create_dir_all(parent).await;
                            }
                            if let Err(e) = crate::fsutil::move_file(&part_obao, &share_obao).await
                            {
                                debug!("Could not promote outboard sidecar (will regenerate): {e}");
                                let _ = tokio::fs::remove_file(&part_obao).await;
                            }
                            if let Err(e) = db::downloads::set_dest_path(
                                &self.db,
                                dl_id,
                                final_path.to_str().unwrap_or(""),
                            )
                            .await
                            {
                                warn!("Could not update dest_path in DB: {e}");
                            }
                            if let Err(e) =
                                db::downloads::set_status(&self.db, dl_id, "completed", None).await
                            {
                                warn!("set_status completed error: {e}");
                            }
                            info!(root_hash = %hash_hex, "Download completed");
                            self.notifier
                                .notify(
                                    rucio_core::api::notifications::NotificationKind::Download,
                                    "Download complete",
                                    name,
                                    Some(hash_hex),
                                )
                                .await;
                        }
                        Err(e) => {
                            warn!(
                                part = %part_path.display(),
                                dir  = %dest_dir.display(),
                                "Download finished but could not be saved (keeping .part): {e}"
                            );
                            let reason = format!("Couldn't save to {}: {e}", dest_dir.display());
                            if let Err(e2) =
                                db::downloads::set_status(&self.db, dl_id, "failed", Some(&reason))
                                    .await
                            {
                                warn!("set_status failed error: {e2}");
                            }
                            self.notifier
                                .notify(
                                    rucio_core::api::notifications::NotificationKind::Download,
                                    "Couldn't save download",
                                    format!(
                                        "{name}: the download folder is missing or not writable — fix it and retry"
                                    ),
                                    Some(hash_hex),
                                )
                                .await;
                        }
                    }
                    self.live_stats.write().await.remove(&dl_id);
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
    // Chunk request failed at the network level (timeout, EOF, conn closed)
    // -----------------------------------------------------------------------

    /// A chunk request never got a response. Free the slot, re-queue the chunk
    /// for another provider, and evict the peer if it keeps failing. Without
    /// this the chunk stays in-flight forever and the download dead-stalls.
    pub async fn on_chunk_request_failed(&mut self, request_id: OutboundRequestId, peer: PeerId) {
        let root_hash = match self
            .active
            .iter()
            .find(|(_, dl)| dl.inflight_map.contains_key(&request_id))
            .map(|(k, _)| *k)
        {
            Some(h) => h,
            None => {
                // Already resolved (a late response beat the failure) or the
                // download was cancelled — nothing to do.
                debug!(?request_id, "Chunk failure for unknown request — ignoring");
                return;
            }
        };

        let dl = match self.active.get_mut(&root_hash) {
            Some(d) => d,
            None => return,
        };

        let chunk_idx = match dl.inflight_map.remove(&request_id) {
            Some((_, idx)) => idx,
            None => return,
        };
        dl.in_flight.remove(&chunk_idx);

        if let Some(ps) = dl.peer_state.get_mut(&peer) {
            ps.in_flight.remove(&chunk_idx);
        }

        // Back off the GLOBAL per-peer concurrency cap (shared across all
        // downloads from this peer), or — if already at the floor — grow this
        // download's failure streak toward eviction.
        let backed_off_to = {
            let cap = self.peer_caps.entry(peer).or_default();
            cap.ok_streak = 0;
            if cap.max_slots > MIN_SLOTS_PER_PEER {
                // Likely over-pipelined for this peer's speed (its concurrent
                // chunks share the link and all miss the deadline together).
                // Back off and give it a clean slate instead of evicting — it's
                // probably just slow, not gone.
                cap.max_slots = (cap.max_slots / 2).max(MIN_SLOTS_PER_PEER);
                Some(cap.max_slots)
            } else {
                None
            }
        };

        let mut evict = false;
        if backed_off_to.is_some() {
            if let Some(ps) = dl.peer_state.get_mut(&peer) {
                ps.consecutive_failures = 0;
            }
        } else if let Some(ps) = dl.peer_state.get_mut(&peer) {
            // Already down to one in-flight chunk and still failing → the peer
            // is genuinely unusable.
            ps.consecutive_failures += 1;
            evict = ps.consecutive_failures >= MAX_PEER_FAILURES;
        }

        if evict {
            dl.providers.retain(|&p| p != peer);
            dl.peer_state.remove(&peer);
            warn!(
                %peer,
                chunk_idx,
                remaining_providers = dl.providers.len(),
                "Provider evicted after repeated chunk failures at the concurrency floor — re-queuing chunk"
            );
        } else if let Some(slots) = backed_off_to {
            warn!(%peer, chunk_idx, slots, "Chunk request timed out — backing off this peer's concurrency and retrying");
        } else {
            warn!(%peer, chunk_idx, "Chunk request failed — re-queuing for another provider");
        }

        // Re-queue at the front so the missing chunk is retried promptly.
        dl.queued.push_front(chunk_idx);

        // Try to reassign right away; if no providers are left with capacity
        // this is a no-op and tick_provider_refresh will re-query the DHT once
        // in_flight drains.
        self.dispatch_requests(root_hash).await;
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
    // Availability (`/have`)
    // -----------------------------------------------------------------------

    /// Answer an inbound `/have` request: report which chunks of the file we
    /// can serve (all of them for a complete share, the verified ones for an
    /// in-progress download).
    pub async fn serve_have(&self, _peer: PeerId, request: HaveRequest, channel_id: u64) {
        let db = self.db.clone();
        let cmd_tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            let response = build_have_response(&db, &request.root_hash).await;
            let _ = cmd_tx
                .send(NodeCmd::RespondHave {
                    channel_id,
                    response,
                })
                .await;
        });
    }

    /// Fold a provider's `/have` answer into the matching download's aggregate
    /// availability bitmap, so the UI can show swarm-wide coverage.
    pub fn on_have_received(&mut self, _peer: PeerId, response: HaveResponse) {
        if let HaveResponse::Ok {
            root_hash, bitmap, ..
        } = response
            && let Some(dl) = self.active.get_mut(&root_hash)
        {
            dl.fold_available(&bitmap);
        }
    }

    /// Ask every known provider of every active download for its `/have`
    /// bitmap. Cheap (a few KB per provider) and best-effort: replies arrive
    /// asynchronously and are OR-ed in by [`on_have_received`]. Called right
    /// after a manifest arrives and then periodically, so coverage tracks
    /// providers (and their partial copies) coming and going.
    pub async fn refresh_availability(&self) {
        for (root_hash, dl) in self.active.iter() {
            // Skip downloads that are already fully covered locally — once we
            // hold every chunk there's nothing left to learn about sources.
            if dl.is_complete() {
                continue;
            }
            for &peer in &dl.providers {
                let _ = self
                    .cmd_tx
                    .send(NodeCmd::RequestHave {
                        peer,
                        request: HaveRequest {
                            root_hash: *root_hash,
                        },
                    })
                    .await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Serve an inbound chunk request
    // -----------------------------------------------------------------------

    pub async fn serve_chunk(
        &self,
        peer: PeerId,
        request: ChunkRequest,
        channel_id: u64,
        is_high_id: bool,
    ) {
        const MAX_PEX_PEERS: usize = 8;

        // Collect PEX peers before spawning — known_providers is not Send.
        let pex_peers: Vec<String> = self
            .known_providers
            .get(&request.root_hash)
            .map(|peers| {
                peers
                    .iter()
                    .take(MAX_PEX_PEERS)
                    .map(|p| p.to_base58())
                    .collect()
            })
            .unwrap_or_default();

        let db = self.db.clone();
        let cmd_tx = self.cmd_tx.clone();
        let metrics = Arc::clone(&self.metrics);
        let semaphore = Arc::clone(&self.upload_semaphore);
        let scheduler = Arc::clone(&self.upload_scheduler);
        let upload_stats = Arc::clone(&self.upload_stats);
        let root_hash = request.root_hash;
        let outboard_dir = self.outboard_dir.clone();

        tokio::spawn(async move {
            let started = std::time::Instant::now();
            // Hold the permit for the entire pipeline (DB read → throttle → send)
            // to bound the number of tasks competing for disk I/O.
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("upload semaphore closed");

            let response = read_chunk_from_db(&db, &request, &outboard_dir, pex_peers).await;
            let (kind, bytes) = match &response {
                ChunkResponse::Ok { data, .. } => ("ok", data.len()),
                ChunkResponse::NotFound => ("not_found", 0),
                ChunkResponse::Error(_) => ("error", 0),
            };
            debug!(
                chunk_idx = request.chunk_idx,
                kind,
                bytes,
                produced_ms = started.elapsed().as_millis() as u64,
                "serve_chunk: response produced"
            );
            // The upload rate limit and the byte-by-byte speed accounting (both
            // global and per-peer) are applied where the bytes are written: the
            // net transfer codec, paced by the shared throttle. So the stream is
            // smooth and the rates read flat. Here we count the chunk and hand
            // the per-`(peer, file)` byte counter to net as the upload sink.
            let upload_sink = if let ChunkResponse::Ok { .. } = response {
                metrics.record_upload_chunk();
                // Get the row's byte counter; resolve the name (one DB hit) only
                // when creating the row on first contact.
                let sink = match upload_stats.rucio_sink_existing(peer, &root_hash) {
                    Some(s) => s,
                    None => {
                        let name = db::shares::get_by_hash(&db, &root_hash)
                            .await
                            .ok()
                            .flatten()
                            .map(|r| r.name);
                        upload_stats.rucio_sink_create(peer, &root_hash, name)
                    }
                };
                Some(sink)
            } else {
                None
            };

            // VIP scheduling for the actual data transfer: a HighID requester
            // is served right away (and counted as in-flight until the node task
            // reports the write done via ChunkSent → `note_chunk_sent`); a LowID
            // one defers while any HighID write is in flight, up to the floor.
            // Empty responses (NotFound/Error) don't contend, so skip them.
            if upload_sink.is_some() {
                if is_high_id {
                    scheduler.highid_started(peer);
                } else {
                    scheduler.wait_for_lowid_turn().await;
                }
            }

            debug!(
                chunk_idx = request.chunk_idx,
                total_ms = started.elapsed().as_millis() as u64,
                "serve_chunk: handing response to node task"
            );
            let _ = cmd_tx
                .send(NodeCmd::RespondChunk {
                    channel_id,
                    response,
                    upload_sink,
                })
                .await;
        });
    }

    /// A chunk response we were serving reached a terminal state (the node task
    /// finished or abandoned the write). Releases the HighID upload-scheduler
    /// slot taken in `serve_chunk` (no-op for LowID/untracked peers).
    pub fn note_chunk_sent(&self, peer: PeerId) {
        self.upload_scheduler.chunk_finished(peer);
    }
}

// ---------------------------------------------------------------------------
// I/O helpers
// ---------------------------------------------------------------------------

/// The sidecar holding the (partial or full) bao outboard for a file at
/// `dest_path`: `<dest_path>.obao`. While downloading, this is the `.part`'s
/// companion that `decode_ranges` fills in chunk by chunk.
fn partial_outboard_path(dest_path: &std::path::Path) -> PathBuf {
    let mut s = dest_path.as_os_str().to_owned();
    s.push(".obao");
    PathBuf::from(s)
}

/// The share outboard cache directory, derived from the daemon's `temp_dir`.
/// Holds the regenerable bao outboards of *completed* shares (in-progress
/// downloads keep theirs next to the `.part`, not here).
pub fn outboards_dir(temp_dir: &std::path::Path) -> PathBuf {
    temp_dir.join("outboards")
}

/// The cached full outboard for a *completed* share, keyed by root hash:
/// `<outboard_dir>/<aa>/<root_hex>.obao`, where `aa` is the first hash byte in
/// hex. Sharding by the first byte (256 buckets) keeps any single directory
/// small even with millions of shares, instead of piling every `.obao` into one
/// directory (which degrades many filesystems). Regenerable from the file at
/// any time.
fn share_outboard_path(outboard_dir: &std::path::Path, root_hash: &[u8; 32]) -> PathBuf {
    let hex = hex::encode(root_hash);
    outboard_dir.join(&hex[..2]).join(format!("{hex}.obao"))
}

/// Files at or above this size get their outboard persisted at index time, so
/// the first chunk request doesn't pay a second full read of the file to rebuild
/// it. Below it, re-outboarding lazily on demand is cheap, so we don't write a
/// sidecar until the file is actually served — keeping the cache to in-demand
/// files instead of one `.obao` per shared file.
pub const EAGER_OUTBOARD_THRESHOLD: u64 = 1024 * 1024 * 1024; // 1 GiB

/// Persist a freshly-computed outboard into the share cache, but only for files
/// large enough that a lazy re-read would hurt (>= [`EAGER_OUTBOARD_THRESHOLD`]).
/// The outboard is a byproduct of the hashing pass `index_file` already runs, so
/// this just saves it rather than recomputing later. Best-effort: on any error
/// the lazy path regenerates it on first serve.
pub async fn persist_share_outboard_if_large(
    temp_dir: &std::path::Path,
    root_hash: &[u8; 32],
    size: u64,
    outboard: &[u8],
) {
    if size < EAGER_OUTBOARD_THRESHOLD {
        return;
    }
    let path = share_outboard_path(&outboards_dir(temp_dir), root_hash);
    if let Some(parent) = path.parent()
        && tokio::fs::create_dir_all(parent).await.is_err()
    {
        return;
    }
    let _ = tokio::fs::write(&path, outboard).await;
}

/// Best-effort removal of a completed share's cached outboard, given the
/// daemon's `temp_dir`. Called when a share is un-indexed so its `.obao`
/// doesn't linger. A missing file is fine (lazily generated, may never have
/// existed). The shard subdirectory is left in place — it's reused by sibling
/// hashes and pruning it would race concurrent writers.
pub async fn remove_share_outboard(temp_dir: &std::path::Path, root_hash: &[u8; 32]) {
    let path = share_outboard_path(&outboards_dir(temp_dir), root_hash);
    let _ = tokio::fs::remove_file(path).await;
}

/// Garbage-collect orphaned share outboards: walk `<temp_dir>/outboards` and
/// delete every `.obao` whose root hash is no longer a completed share. The
/// cache holds only completed-share outboards (keyed by root hash), so the
/// `shared_files` table is the authority. Cheap on a stable library (a sharded
/// directory walk + one indexed lookup per file) and a backstop for removals
/// that didn't clean up inline (watcher de-index, startup reconcile, a crash
/// between the DB delete and the file delete). Returns the number removed.
pub async fn gc_orphan_outboards(db: &Db, temp_dir: &std::path::Path) -> usize {
    let dir = outboards_dir(temp_dir);
    let mut removed = 0usize;
    let mut buckets = match tokio::fs::read_dir(&dir).await {
        Ok(rd) => rd,
        Err(_) => return 0, // cache dir doesn't exist yet — nothing to GC
    };
    while let Ok(Some(bucket)) = buckets.next_entry().await {
        let mut files = match tokio::fs::read_dir(bucket.path()).await {
            Ok(rd) => rd,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = files.next_entry().await {
            let path = entry.path();
            // Parse the root hash from `<root_hex>.obao`; skip anything else.
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            if path.extension().and_then(|e| e.to_str()) != Some("obao") {
                continue;
            }
            let Ok(bytes) = hex::decode(stem) else {
                continue;
            };
            let Ok(root_hash): Result<[u8; 32], _> = bytes.try_into() else {
                continue;
            };
            match db::shares::get_by_hash(db, &root_hash).await {
                Ok(Some(_)) => {} // still a share — keep
                Ok(None) => {
                    if tokio::fs::remove_file(&path).await.is_ok() {
                        removed += 1;
                    }
                }
                Err(_) => {} // DB hiccup — leave it for the next sweep
            }
        }
    }
    if removed > 0 {
        debug!(removed, "GC: pruned orphaned share outboards");
    }
    removed
}

async fn read_chunk_from_db(
    db: &Db,
    request: &ChunkRequest,
    outboard_dir: &std::path::Path,
    pex_peers: Vec<String>,
) -> ChunkResponse {
    use sqlx::Row;
    let row = sqlx::query("SELECT path, size, chunk_size FROM shared_files WHERE root_hash = ?1")
        .bind(request.root_hash.as_slice())
        .fetch_optional(db)
        .await;

    let row = match row {
        Ok(Some(r)) => r,
        // Not a completed share — try an in-progress download (partial sharing).
        Ok(None) => return read_chunk_from_partial(db, request, pex_peers).await,
        Err(e) => return ChunkResponse::Error(e.to_string()),
    };

    let path: String = row.get("path");
    let total_size: i64 = row.get("size");
    let chunk_size: i64 = row.get("chunk_size");
    let total_size = total_size as u64;
    let chunk_size = chunk_size as u32;
    let chunk_idx = request.chunk_idx;
    let root = blake3::Hash::from_bytes(request.root_hash);
    let outboard_path = share_outboard_path(outboard_dir, &request.root_hash);

    // Build the self-verifying slice off the async runtime: bao encode reads the
    // data range and (if absent) recomputes the whole outboard — both blocking.
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
        let data_path = std::path::PathBuf::from(&path);
        // Load the cached outboard, regenerating it from the file if it is
        // missing or stale (wrong length for the file's tree).
        let want_len = rucio_core::protocol::bao::outboard_len(total_size);
        let outboard = match std::fs::read(&outboard_path) {
            Ok(bytes) if bytes.len() == want_len => bytes,
            _ => {
                let (_root, bytes, _size) =
                    rucio_core::protocol::bao::compute_outboard(&data_path)?;
                if let Some(parent) = outboard_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::fs::write(&outboard_path, &bytes);
                bytes
            }
        };
        let ranges = rucio_core::protocol::bao::chunk_ranges(chunk_idx, chunk_size, total_size);
        rucio_core::protocol::bao::encode_slice(&data_path, outboard, root, total_size, &ranges)
    })
    .await;

    match result {
        Ok(Ok(data)) => ChunkResponse::Ok {
            data,
            peers: pex_peers,
        },
        Ok(Err(e)) => ChunkResponse::Error(e.to_string()),
        Err(e) => ChunkResponse::Error(e.to_string()),
    }
}

/// Partial sharing: serve a chunk we hold for a file we are *still downloading*.
///
/// Only chunks already marked `done` are served — those passed verified
/// streaming when received, so their subtree is present in the `.part`'s
/// outboard sidecar and we can emit a valid slice. The bytes come from the
/// download's `.part` (its `dest_path` while in progress) and its `.part.obao`.
/// Once the download completes the file is indexed as a normal share and served
/// by the path above instead.
async fn read_chunk_from_partial(
    db: &Db,
    request: &ChunkRequest,
    pex_peers: Vec<String>,
) -> ChunkResponse {
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT d.dest_path AS dest_path, d.total_size AS total_size
         FROM download_chunks dc
         JOIN downloads d ON d.id = dc.download_id
         WHERE d.root_hash = ?1 AND dc.idx = ?2 AND dc.status = 'done'",
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

    let path: String = row.get("dest_path");
    let total_size: i64 = row.get("total_size");
    let total_size = total_size as u64;
    let chunk_idx = request.chunk_idx;
    let root = blake3::Hash::from_bytes(request.root_hash);

    // Downloads use the fixed rucio chunk size.
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
        let data_path = std::path::PathBuf::from(&path);
        let outboard = std::fs::read(partial_outboard_path(&data_path))?;
        let ranges = rucio_core::protocol::bao::chunk_ranges(chunk_idx, CHUNK_SIZE, total_size);
        rucio_core::protocol::bao::encode_slice(&data_path, outboard, root, total_size, &ranges)
    })
    .await;

    match result {
        // A done chunk whose proof nodes aren't yet in the partial outboard
        // (sidecar lost) can't be sliced; report NotFound so the peer retries
        // elsewhere rather than treating it as a hard error.
        Ok(Ok(data)) => ChunkResponse::Ok {
            data,
            peers: pex_peers,
        },
        Ok(Err(_)) => ChunkResponse::NotFound,
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

    let name: String = file_row.get("name");
    let total_size: i64 = file_row.get("size");
    let chunk_size: i64 = file_row.get("chunk_size");
    let total_size = total_size as u64;
    let chunk_size = chunk_size as u32;

    ManifestResponse::Ok {
        root_hash: *root_hash,
        name,
        total_size,
        chunk_size,
        chunk_count: derive_chunk_count(total_size, chunk_size),
    }
}

/// Number of `chunk_size`-byte chunks covering `total_size` (ceiling). A
/// zero-byte file has zero chunks.
fn derive_chunk_count(total_size: u64, chunk_size: u32) -> u32 {
    if chunk_size == 0 {
        return 0;
    }
    total_size.div_ceil(chunk_size as u64) as u32
}

/// Build our `/have` answer for a file: which of its chunks we can serve right
/// now. A complete share answers all-ones; an in-progress download answers its
/// verified chunks (partial sharing); an unknown file answers `NotFound`.
async fn build_have_response(db: &Db, root_hash: &[u8; 32]) -> HaveResponse {
    use sqlx::Row;

    // Complete share: every chunk is on disk.
    let shared = sqlx::query("SELECT size, chunk_size FROM shared_files WHERE root_hash = ?1")
        .bind(root_hash.as_slice())
        .fetch_optional(db)
        .await;
    if let Ok(Some(row)) = shared {
        let size: i64 = row.get("size");
        let chunk_size: i64 = row.get("chunk_size");
        let count = derive_chunk_count(size as u64, chunk_size as u32) as usize;
        let mut bitmap = vec![0xFFu8; bitmap_bytes(count)];
        mask_trailing_bits(&mut bitmap, count);
        return HaveResponse::Ok {
            root_hash: *root_hash,
            pieces_total: count as u64,
            bitmap,
        };
    }

    // In-progress download: report the chunks we've verified so far.
    if let Ok(Some(dl)) = db::downloads::get_by_root_hash(db, root_hash).await
        && let Ok(chunks) = db::downloads::chunks_for(db, dl.id).await
        && !chunks.is_empty()
    {
        let total = chunks.len();
        let mut bitmap = vec![0u8; bitmap_bytes(total)];
        for c in &chunks {
            if c.status == "done" {
                let i = c.idx as usize;
                if let Some(b) = bitmap.get_mut(i / 8) {
                    *b |= 1 << (i % 8);
                }
            }
        }
        return HaveResponse::Ok {
            root_hash: *root_hash,
            pieces_total: total as u64,
            bitmap,
        };
    }

    HaveResponse::NotFound
}

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

/// Short hash fragment used to keep same-named files apart on disk. 8 hex chars
/// (32 bits) — a collision between two *identically named* files is negligible.
fn name_frag(root_hash: &[u8; 32]) -> String {
    hex::encode(&root_hash[..4])
}

/// Insert `frag` before the file extension so the name stays readable and
/// unique: `tvshow.nfo` → `tvshow.<frag>.nfo`; `readme` → `readme.<frag>`.
fn name_with_frag(name: &str, frag: &str) -> String {
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => format!("{stem}.{frag}.{ext}"),
        _ => format!("{name}.{frag}"),
    }
}

/// Move a finished `.part` into `dest_dir/<name>`, (re)creating `dest_dir` first
/// — the user may have deleted it (or revoked write access) while the download
/// was in flight. If `dest_dir/<name>` is already taken (a different file with
/// the same name — common when mirroring, e.g. many `tvshow.nfo`), the hash
/// `frag` is inserted before the extension so neither clobbers the other.
/// Returns the final path. On any failure the `.part` is left untouched, so a
/// completed-but-unsaved download loses nothing and can be retried.
async fn persist_completed(
    dest_dir: &std::path::Path,
    part_path: &std::path::Path,
    name: &str,
    frag: &str,
) -> std::io::Result<std::path::PathBuf> {
    tokio::fs::create_dir_all(dest_dir).await?;
    let mut target = dest_dir.join(name);
    if tokio::fs::try_exists(&target).await.unwrap_or(false) {
        target = dest_dir.join(name_with_frag(name, frag));
    }
    crate::fsutil::move_file(part_path, &target).await?;
    Ok(target)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// Build a single-chunk bao fixture for `data`: returns its real `root_hash`
    /// (= `blake3::hash(data)`) and a self-verifying slice for chunk 0 of a file
    /// of length `data.len()` (chunk_size = the whole file). Used by tests that
    /// must drive a real verified-streaming decode.
    fn bao_one_chunk(data: &[u8]) -> ([u8; 32], Vec<u8>) {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(data).unwrap();
        f.flush().unwrap();
        let (root, ob, size) = rucio_core::protocol::bao::compute_outboard(f.path()).unwrap();
        let chunk_size = (data.len() as u32).max(1);
        let ranges = rucio_core::protocol::bao::chunk_ranges(0, chunk_size, size);
        let slice =
            rucio_core::protocol::bao::encode_slice(f.path(), ob, root, size, &ranges).unwrap();
        (*root.as_bytes(), slice)
    }

    #[test]
    fn mask_trailing_bits_clears_padding_in_last_byte() {
        // 10 bits → 2 bytes; bits 10..16 are padding and must read clear.
        let mut map = vec![0xFFu8, 0xFF];
        mask_trailing_bits(&mut map, 10);
        assert_eq!(map, vec![0xFF, 0b0000_0011]);
        // Exact byte boundary leaves the last byte untouched.
        let mut exact = vec![0xFFu8, 0xFF];
        mask_trailing_bits(&mut exact, 16);
        assert_eq!(exact, vec![0xFF, 0xFF]);
    }

    #[test]
    fn fold_available_ors_and_masks() {
        let mut dl = test_download(10); // 10 chunks → 2-byte bitmap
        // Provider A holds chunks 0,1; provider B holds chunk 9 (+ stray pad bit).
        dl.fold_available(&[0b0000_0011, 0x00]);
        dl.fold_available(&[0x00, 0b1111_1110]); // bit 9 set, plus padding bits
        // Union: bits 0,1,9 — padding (bits 10..16) masked away.
        assert_eq!(dl.available, vec![0b0000_0011, 0b0000_0010]);
        // A longer-than-expected peer bitmap can't set chunks we don't have.
        dl.fold_available(&[0xFF, 0xFF, 0xFF]);
        assert_eq!(dl.available, vec![0xFF, 0b0000_0011]);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// A minimal ActiveDownload with `total` chunks and a zeroed availability
    /// bitmap, for exercising the bitmap helpers without a full engine.
    fn test_download(total: usize) -> ActiveDownload {
        let total_size = total as u64 * CHUNK_SIZE as u64;
        ActiveDownload {
            download_id: 1,
            dest_path: PathBuf::from("/tmp/x.part"),
            chunk_size: CHUNK_SIZE,
            total_size,
            queued: VecDeque::new(),
            in_flight: HashSet::new(),
            done: HashSet::new(),
            total_chunks: total,
            partial_outboard: Arc::new(std::sync::Mutex::new(vec![
                0u8;
                rucio_core::protocol::bao::outboard_len(
                    total_size
                )
            ])),
            providers: vec![],
            peer_state: HashMap::new(),
            inflight_map: HashMap::new(),
            announced: false,
            available: vec![0u8; bitmap_bytes(total)],
        }
    }

    /// Open a temporary SQLite DB and run migrations.
    async fn make_db() -> (Db, tempfile::TempDir) {
        use sqlx::sqlite::SqlitePoolOptions;
        // Use a temp-file DB rather than :memory: to avoid SQLite in-memory
        // connection pool deadlocks when multiple queries run concurrently.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .unwrap();
        // Apply the schema exactly as the daemon does, rather than re-parsing it
        // here: a hand-rolled split on ';' breaks on a ';' inside a comment.
        crate::db::apply_schema(&pool).await.unwrap();
        (pool, dir)
    }

    /// Build a DownloadEngine with a fresh file DB and return the
    /// NodeCmd receiver so tests can inspect what the engine sends.
    async fn make_engine(
        tmp: &tempfile::TempDir,
    ) -> (DownloadEngine, mpsc::Receiver<NodeCmd>, tempfile::TempDir) {
        let (db, db_dir) = make_db().await;
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let metrics = Arc::new(crate::metrics::Metrics::default());
        let (ws_tx, _) = tokio::sync::broadcast::channel(16);
        let notif_state = crate::notifier::NotificationState::from_config(
            &crate::config::NotificationConfig::default(),
        );
        let notifier = crate::notifier::Notifier::new(db.clone(), ws_tx, notif_state);
        let engine = DownloadEngine::new(
            db,
            cmd_tx,
            tmp.path().to_path_buf(),
            tmp.path().join("pins"),
            tmp.path().to_path_buf(),
            metrics,
            Arc::new(tokio::sync::Semaphore::new(64)),
            Arc::new(crate::upload_scheduler::UploadScheduler::new()),
            Arc::new(crate::throttle::TokenBucket::new(0)),
            Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            Arc::new(crate::upload_stats::UploadRegistry::new()),
            notifier,
        );
        (engine, cmd_rx, db_dir)
    }

    /// Construct a fake PeerId deterministically from a byte seed.
    fn peer(seed: u8) -> PeerId {
        use libp2p::identity::Keypair;
        // Build a 32-byte Ed25519 seed
        let secret = libp2p::identity::ed25519::SecretKey::try_from_bytes(&mut [seed; 32]).unwrap();
        let kp = Keypair::from(libp2p::identity::ed25519::Keypair::from(secret));
        kp.public().to_peer_id()
    }

    /// Build a fake OutboundRequestId from a u64 using transmute.
    /// Safe because OutboundRequestId is a repr(transparent) newtype over u64.
    fn fake_request_id(n: u64) -> OutboundRequestId {
        // SAFETY: OutboundRequestId is `pub struct OutboundRequestId(u64)` — a
        // transparent newtype. We only use this in tests to simulate responses.
        unsafe { std::mem::transmute::<u64, OutboundRequestId>(n) }
    }

    fn fake_magnet(hash: &[u8; 32], name: &str, size: u64) -> String {
        format!("rucio:{}?name={}&size={}", hex::encode(hash), name, size)
    }

    /// Spawn a background task that drains NodeCmd messages from `rx` and
    /// automatically acks every `id_tx` oneshot with a fake OutboundRequestId.
    /// This prevents `dispatch_requests` from deadlocking while the engine
    /// awaits `id_rx` inside a single-threaded test runtime.
    ///
    /// Returns a handle + the `Arc<Mutex<Vec<NodeCmd>>>` accumulator so tests
    /// can inspect dispatched commands after `stop_acker` is called.
    fn spawn_acker(
        mut rx: mpsc::Receiver<NodeCmd>,
    ) -> (
        tokio::task::JoinHandle<Vec<NodeCmd>>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let mut cmds = Vec::new();
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    cmd = rx.recv() => {
                        match cmd {
                            None => break,
                            Some(NodeCmd::RequestChunk { peer, request, download_sink, id_tx }) => {
                                let fake_id = fake_request_id(42 + cmds.len() as u64);
                                let _ = id_tx.send(fake_id);
                                // Record just peer/request info via a dummy sentinel
                                let (tx2, _) = tokio::sync::oneshot::channel();
                                cmds.push(NodeCmd::RequestChunk { peer, request, download_sink, id_tx: tx2 });
                            }
                            Some(NodeCmd::RequestManifest { peer, request, id_tx }) => {
                                let fake_id = fake_request_id(100 + cmds.len() as u64);
                                let _ = id_tx.send(fake_id);
                                let (tx2, _) = tokio::sync::oneshot::channel();
                                cmds.push(NodeCmd::RequestManifest { peer, request, id_tx: tx2 });
                            }
                            Some(other) => cmds.push(other),
                        }
                    }
                }
            }
            cmds
        });
        (handle, stop_tx)
    }

    // -----------------------------------------------------------------------
    // Existing magnet parser tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_magnet_valid() {
        let hash = "a".repeat(64);
        let magnet = format!("rucio:{hash}?name=test.mp3&size=1024");
        let info = parse_magnet(&magnet).unwrap();
        assert_eq!(info.name, Some("test.mp3".to_string()));
        assert_eq!(info.size, Some(1024));
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
        // name is now optional — this must succeed
        assert!(parse_magnet(&magnet).is_ok());
    }

    #[test]
    fn parse_magnet_hash_only() {
        let hash = "c".repeat(64);
        let magnet = format!("rucio:{hash}");
        let info = parse_magnet(&magnet).unwrap();
        assert_eq!(info.name, None);
        assert_eq!(info.size, None);
        assert!(info.providers.is_empty());
        assert_eq!(hex::encode(info.root_hash), hash);
    }

    #[test]
    fn parse_magnet_with_providers() {
        let hash = "d".repeat(64);
        let pid1 = "12D3KooWGFiWpMFMZPmBBDrZkegLeAfi3jXnNmLoEAfFExwEHEU3";
        let pid2 = "12D3KooWHFmNNBCBCKcBkC6RkCBMKiHbBgxGFiWpMFMZPmBBDrZk";
        let magnet = format!("rucio:{hash}?name=foo&provider={pid1}&provider={pid2}");
        let info = parse_magnet(&magnet).unwrap();
        assert_eq!(info.providers, vec![pid1.to_string(), pid2.to_string()]);
    }

    #[test]
    fn parse_magnet_bad_hex() {
        assert!(parse_magnet("rucio:ZZZZ?name=foo&size=1").is_err());
    }

    #[test]
    fn parse_magnet_wrong_hash_length() {
        assert!(parse_magnet("rucio:deadbeef?name=foo&size=1").is_err());
    }

    // -----------------------------------------------------------------------
    // Partial sharing — serve completed chunks of an in-progress download
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn partial_sharing_serves_done_chunk_not_pending_one() {
        let (db, dir) = make_db().await;
        let part = dir.path().join("file.bin.part");
        let data = b"contents of chunk zero".to_vec();
        tokio::fs::write(&part, &data).await.unwrap();
        // Write the outboard sidecar the producer needs to slice the chunk; the
        // download's root hash is the canonical blake3 of the file content.
        let (root, outboard, size) = rucio_core::protocol::bao::compute_outboard(&part).unwrap();
        tokio::fs::write(partial_outboard_path(&part), &outboard)
            .await
            .unwrap();
        let hash = *root.as_bytes();

        // An in-progress download whose .part holds one verified ('done') chunk.
        sqlx::query(
            "INSERT INTO downloads (root_hash, name, total_size, dest_path, status, bytes_done, added_at, updated_at)
             VALUES (?1, 'file.bin', ?2, ?3, 'downloading', ?2, 0, 0)",
        )
        .bind(hash.as_slice())
        .bind(size as i64)
        .bind(part.to_str().unwrap())
        .execute(&db)
        .await
        .unwrap();
        let dl_id: i64 = sqlx::query_scalar("SELECT id FROM downloads WHERE root_hash = ?1")
            .bind(hash.as_slice())
            .fetch_one(&db)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO download_chunks (download_id, idx, size, status)
             VALUES (?1, 0, ?2, 'done')",
        )
        .bind(dl_id)
        .bind(size as i64)
        .execute(&db)
        .await
        .unwrap();

        // Chunk 0 is done → served from the .part as a verifying slice that
        // decodes back to the original bytes.
        let req = ChunkRequest {
            root_hash: hash,
            chunk_idx: 0,
        };
        match read_chunk_from_partial(&db, &req, vec![]).await {
            ChunkResponse::Ok { data: slice, .. } => {
                let ranges = rucio_core::protocol::bao::chunk_ranges(0, CHUNK_SIZE, size);
                let mut out = tempfile::NamedTempFile::new().unwrap();
                out.as_file().set_len(size).unwrap();
                rucio_core::protocol::bao::decode_slice_into(
                    &slice,
                    &ranges,
                    out.as_file_mut(),
                    root,
                    size,
                    vec![0u8; rucio_core::protocol::bao::outboard_len(size)],
                )
                .expect("served slice must verify against the root");
                let got = std::fs::read(out.path()).unwrap();
                assert_eq!(got, data);
            }
            _ => panic!("expected the done chunk to be served"),
        }

        // Chunk 1 isn't done → not served (never hand out what we don't have).
        let req2 = ChunkRequest {
            root_hash: hash,
            chunk_idx: 1,
        };
        assert!(matches!(
            read_chunk_from_partial(&db, &req2, vec![]).await,
            ChunkResponse::NotFound
        ));
    }

    #[tokio::test]
    async fn gc_prunes_orphan_outboards_keeps_live_shares() {
        let (db, dir) = make_db().await;
        let temp = dir.path();

        // A live share (row in shared_files) and an orphan (no row), each with a
        // cached outboard at its sharded path.
        let live = [0x11u8; 32];
        let orphan = [0x22u8; 32];
        crate::db::shares::insert(
            &db,
            crate::db::shares::NewSharedFile {
                root_hash: &live,
                name: "live.bin",
                size: 4096,
                mime_type: None,
                path: "/tmp/live.bin",
                chunk_size: 4096,
                added_at: 1,
                mtime: 0,
            },
        )
        .await
        .unwrap();

        for h in [&live, &orphan] {
            let p = share_outboard_path(&outboards_dir(temp), h);
            tokio::fs::create_dir_all(p.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(&p, b"outboard bytes").await.unwrap();
        }

        let removed = gc_orphan_outboards(&db, temp).await;
        assert_eq!(removed, 1, "exactly the orphan should be pruned");
        assert!(
            share_outboard_path(&outboards_dir(temp), &live).exists(),
            "the live share's outboard must survive"
        );
        assert!(
            !share_outboard_path(&outboards_dir(temp), &orphan).exists(),
            "the orphan outboard must be gone"
        );
    }

    #[tokio::test]
    async fn remove_share_outboard_deletes_the_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let temp = dir.path();
        let h = [0x33u8; 32];
        let p = share_outboard_path(&outboards_dir(temp), &h);
        tokio::fs::create_dir_all(p.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&p, b"x").await.unwrap();

        remove_share_outboard(temp, &h).await;
        assert!(!p.exists(), "outboard sidecar should be deleted");
        // Idempotent: a second call on a missing file is a no-op (no panic).
        remove_share_outboard(temp, &h).await;
    }

    // -----------------------------------------------------------------------
    // start() — stores PendingManifest and sends RequestManifest + FindProviders
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn start_sends_request_manifest_and_find_providers() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, mut rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0xaau8; 32];
        let magnet = fake_magnet(&hash, "file.bin", 1024);
        let p = peer(1);

        engine.start(&magnet, vec![p], 0, None).await.unwrap();

        // Should have sent RequestManifest and FindProviders
        let cmd1 = rx.try_recv().unwrap();
        let cmd2 = rx.try_recv().unwrap();
        let cmds = [cmd1, cmd2];
        assert!(
            cmds.iter()
                .any(|c| matches!(c, NodeCmd::RequestManifest { peer, .. } if *peer == p))
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c, NodeCmd::FindProviders(k) if k == hash.as_slice()))
        );
    }

    #[tokio::test]
    async fn start_duplicate_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0xbbu8; 32];
        let magnet = fake_magnet(&hash, "dup.bin", 512);
        let p = peer(2);

        engine.start(&magnet, vec![p], 0, None).await.unwrap();
        let err = engine.start(&magnet, vec![p], 0, None).await.unwrap_err();
        assert!(err.to_string().contains("already active"));
    }

    #[tokio::test]
    async fn start_no_providers_succeeds_and_queues_find_providers() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, mut rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0xccu8; 32];
        let magnet = fake_magnet(&hash, "nop.bin", 256);

        // Providers-less start should succeed — discovery via DHT.
        engine.start(&magnet, vec![], 0, None).await.unwrap();

        // Should have enqueued a FindProviders command.
        let cmd = rx.try_recv().unwrap();
        assert!(matches!(cmd, NodeCmd::FindProviders(ref k) if k == hash.as_slice()));

        // Entry should be in pending_manifests (no manifest request yet).
        let pm = engine.pending_manifests.get(&hash).unwrap();
        assert!(pm.providers.is_empty(), "no providers before DHT responds");
    }

    // -----------------------------------------------------------------------
    // add_providers() — updates pending or active state
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_providers_to_pending_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x01u8; 32];
        let magnet = fake_magnet(&hash, "f.bin", 100);
        let p1 = peer(1);
        let p2 = peer(2);

        engine.start(&magnet, vec![p1], 0, None).await.unwrap();
        engine.add_providers(hash, vec![p2]).await;

        let pm = engine.pending_manifests.get(&hash).unwrap();
        assert!(pm.providers.contains(&p2));
    }

    #[tokio::test]
    async fn add_providers_deduplicates() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x02u8; 32];
        let magnet = fake_magnet(&hash, "g.bin", 100);
        let p1 = peer(1);

        engine.start(&magnet, vec![p1], 0, None).await.unwrap();
        engine.add_providers(hash, vec![p1]).await; // same peer
        engine.add_providers(hash, vec![p1]).await;

        let pm = engine.pending_manifests.get(&hash).unwrap();
        assert_eq!(pm.providers.iter().filter(|&&p| p == p1).count(), 1);
    }

    // -----------------------------------------------------------------------
    // cancel() — clears pending manifest and active download by hash
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cancel_pending_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x03u8; 32];
        let magnet = fake_magnet(&hash, "c.bin", 200);

        engine.start(&magnet, vec![peer(1)], 0, None).await.unwrap();
        assert!(engine.pending_manifests.contains_key(&hash));

        engine.cancel(99, hash.to_vec(), false).await;
        assert!(!engine.pending_manifests.contains_key(&hash));
    }

    #[tokio::test]
    async fn cancel_nonexistent_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;
        // Should not panic
        engine.cancel(999, vec![0u8; 32], false).await;
    }

    #[tokio::test]
    async fn cancel_removes_part_of_download_not_in_memory() {
        // A stalled / not-yet-rehydrated download lives only in the DB. Cancel
        // must still delete its .part instead of leaking it on disk.
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x07u8; 32];
        let id =
            db::downloads::create_pending(&engine.db, &hash, Some("orphan.bin"), 500, false, None)
                .await
                .unwrap()
                .id();
        let part = tmp.path().join("orphan.bin.part");
        tokio::fs::write(&part, b"partial data").await.unwrap();
        db::downloads::set_dest_path(&engine.db, id, part.to_str().unwrap())
            .await
            .unwrap();
        assert!(!engine.active.contains_key(&hash), "not tracked in memory");

        engine.cancel(id, hash.to_vec(), false).await;

        assert!(
            !part.exists(),
            ".part must be removed even when not in memory"
        );
    }

    #[tokio::test]
    async fn cancel_never_deletes_a_completed_file() {
        // If dest_path points at a finished file (not a .part), cancel must not
        // touch it — the file already belongs to the user.
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x08u8; 32];
        let id =
            db::downloads::create_pending(&engine.db, &hash, Some("done.bin"), 500, false, None)
                .await
                .unwrap()
                .id();
        let final_file = tmp.path().join("done.bin");
        tokio::fs::write(&final_file, b"complete").await.unwrap();
        db::downloads::set_dest_path(&engine.db, id, final_file.to_str().unwrap())
            .await
            .unwrap();

        engine.cancel(id, hash.to_vec(), false).await;

        assert!(
            final_file.exists(),
            "a completed file must never be deleted"
        );
    }

    #[tokio::test]
    async fn cancel_with_delete_row_removes_the_record() {
        // Mirror evictions cancel with delete_row = true: the .part goes and the
        // row is removed, so no stale `cancelled` entry lingers in the list.
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x0au8; 32];
        let id = db::downloads::create_pending(&engine.db, &hash, Some("m.bin"), 500, false, None)
            .await
            .unwrap()
            .id();
        let part = tmp.path().join("m.bin.part");
        tokio::fs::write(&part, b"partial").await.unwrap();
        db::downloads::set_dest_path(&engine.db, id, part.to_str().unwrap())
            .await
            .unwrap();

        engine.cancel(id, hash.to_vec(), true).await;

        assert!(!part.exists(), ".part removed");
        assert!(
            db::downloads::get(&engine.db, id).await.unwrap().is_none(),
            "the row is deleted with delete_row = true"
        );
    }

    // -----------------------------------------------------------------------
    // tick_provider_refresh() — only fires for stalled downloads
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tick_provider_refresh_skips_healthy_download() {
        // A pending manifest that already has a provider is not stalled —
        // it is waiting for the manifest reply. No FindProviders should fire.
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, mut rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0xaau8; 32];
        let p = peer(1);

        engine
            .start(&fake_magnet(&hash, "a.bin", 100), vec![p], 0, None)
            .await
            .unwrap();
        // Drain start() commands (FindProviders + RequestManifest)
        while rx.try_recv().is_ok() {}

        engine.tick_provider_refresh().await;

        // Nothing emitted — download has a provider and is not stalled.
        assert!(
            rx.try_recv().is_err(),
            "no FindProviders for healthy download"
        );
    }

    #[tokio::test]
    async fn tick_provider_refresh_emits_for_pending_without_providers() {
        // A pending manifest with no providers is the pure DHT-only start case.
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, mut rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0xbbu8; 32];

        engine.pending_manifests.insert(
            hash,
            PendingManifest {
                providers: vec![],
                attempt: 0,
                requested_at: std::time::Instant::now(),
                last_find_at: std::time::Instant::now(),
                db_id: 0,
                refind_count: 0,
            },
        );

        engine.tick_provider_refresh().await;

        let cmd = rx.try_recv().unwrap();
        assert!(matches!(cmd, NodeCmd::FindProviders(ref k) if k == hash.as_slice()));
    }

    // -----------------------------------------------------------------------
    // tick_manifest_timeouts() — retries and exhaustion
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tick_retries_manifest_after_timeout() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, mut rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x10u8; 32];
        let magnet = fake_magnet(&hash, "t.bin", 100);
        let p1 = peer(1);
        let p2 = peer(2);

        engine.start(&magnet, vec![p1, p2], 0, None).await.unwrap();
        // Drain start() commands
        while rx.try_recv().is_ok() {}

        // Force timeout by backdating requested_at
        {
            let pm = engine.pending_manifests.get_mut(&hash).unwrap();
            pm.requested_at = Instant::now() - Duration::from_secs(15);
        }

        engine.tick_manifest_timeouts().await;

        // Should have sent a RequestManifest to p2 (attempt 1)
        let cmd = rx.try_recv().unwrap();
        assert!(matches!(cmd, NodeCmd::RequestManifest { peer, .. } if peer == p2));
        // Entry should still be in pending_manifests (not yet exhausted)
        assert!(engine.pending_manifests.contains_key(&hash));
    }

    #[tokio::test]
    async fn tick_requeues_find_providers_when_all_providers_exhausted() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, mut rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x11u8; 32];
        let magnet = fake_magnet(&hash, "u.bin", 100);

        engine.start(&magnet, vec![peer(1)], 0, None).await.unwrap();
        while rx.try_recv().is_ok() {}

        // Force timeout with only one provider (already attempted) and
        // backdate last_find_at so the REFIND_SECS guard does not block us.
        {
            let pm = engine.pending_manifests.get_mut(&hash).unwrap();
            pm.requested_at = Instant::now() - Duration::from_secs(15);
            pm.last_find_at = Instant::now() - Duration::from_secs(23 * 60);
        }

        engine.tick_manifest_timeouts().await;

        // Entry should still be in pending_manifests — we never remove it.
        assert!(engine.pending_manifests.contains_key(&hash));
        // Providers list should be cleared (reset for fresh DHT results).
        assert!(engine.pending_manifests[&hash].providers.is_empty());
        // A FindProviders command should have been issued.
        let cmd = rx.try_recv().unwrap();
        assert!(matches!(cmd, NodeCmd::FindProviders(_)));
    }

    // -----------------------------------------------------------------------
    // on_manifest_received() — happy path and orphan
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn on_manifest_received_orphan_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;

        // No pending manifest for this hash — should be silently ignored.
        let response = ManifestResponse::Ok {
            root_hash: [0x20u8; 32],
            name: "ghost.bin".to_string(),
            total_size: 100,
            chunk_size: 100,
            chunk_count: 1,
        };
        engine
            .on_manifest_received(fake_request_id(1), peer(1), response, 0)
            .await;
        // Should not panic and active should be empty
        assert!(engine.active.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_manifest_received_happy_path_starts_active_download() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x21u8; 32];
        let magnet = fake_magnet(&hash, "happy.bin", 100);
        let p = peer(1);

        // Use spawn_acker so dispatch_requests doesn't deadlock waiting for id_rx
        let (acker_handle, stop_tx) = spawn_acker(rx);

        engine.start(&magnet, vec![p], 0, None).await.unwrap();

        let response = ManifestResponse::Ok {
            root_hash: hash,
            name: "happy.bin".to_string(),
            total_size: 5,
            chunk_size: 5,
            chunk_count: 1,
        };
        engine
            .on_manifest_received(fake_request_id(2), p, response, 0)
            .await;

        // Stop the acker and collect commands
        let _ = stop_tx.send(());
        let cmds = acker_handle.await.unwrap();

        // Should have moved from pending to active
        assert!(!engine.pending_manifests.contains_key(&hash));
        assert!(engine.active.contains_key(&hash));
        // Should have dispatched at least one RequestChunk
        let has_chunk_req = cmds
            .iter()
            .any(|c| matches!(c, NodeCmd::RequestChunk { peer, .. } if *peer == p));
        assert!(has_chunk_req, "expected a RequestChunk for peer {p}");
    }

    // -----------------------------------------------------------------------
    // on_chunk_received() — hash ok, hash mismatch, completion
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn on_chunk_received_unknown_request_id_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;

        // No active download — should not panic.
        engine
            .on_chunk_received(
                fake_request_id(99),
                peer(1),
                ChunkResponse::Ok {
                    data: vec![1, 2, 3],
                    peers: vec![],
                },
            )
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_chunk_received_hash_mismatch_requeues() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x30u8; 32];
        let magnet = fake_magnet(&hash, "mis.bin", 12);
        let p = peer(1);

        // Keep acker alive for the whole test — dispatch_requests inside both
        // on_manifest_received and on_chunk_received (after requeue) need it.
        let (acker_handle, stop_tx) = spawn_acker(rx);

        engine.start(&magnet, vec![p], 0, None).await.unwrap();
        engine
            .on_manifest_received(
                fake_request_id(1),
                p,
                ManifestResponse::Ok {
                    root_hash: hash,
                    name: "mis.bin".to_string(),
                    total_size: 12,
                    chunk_size: 12,
                    chunk_count: 1,
                },
                0,
            )
            .await;

        // After on_manifest_received, chunk 0 is in inflight_map with the
        // acker's fake id (42+n). Inject a known id so we can call on_chunk_received.
        let req_id = fake_request_id(10);
        {
            let dl = engine.active.get_mut(&hash).unwrap();
            dl.inflight_map.clear();
            dl.in_flight.clear();
            for ps in dl.peer_state.values_mut() {
                ps.in_flight.clear();
            }
            dl.queued.clear(); // will be re-populated by requeue inside on_chunk_received
            dl.inflight_map.insert(req_id, (p, 0));
            dl.in_flight.insert(0);
            dl.peer_state.entry(p).or_default().in_flight.insert(0);
        }

        // Send wrong data — on_chunk_received will re-queue chunk 0 and call
        // dispatch_requests again (which the acker will service).
        engine
            .on_chunk_received(
                req_id,
                p,
                ChunkResponse::Ok {
                    data: b"wrong data!!".to_vec(),
                    peers: vec![],
                },
            )
            .await;

        // Stop acker after on_chunk_received returns
        let _ = stop_tx.send(());
        let _ = acker_handle.await.unwrap();

        // Chunk should be back in in_flight (dispatch_requests was called again)
        // OR back in queued if no slots were free — either way it must not be in done.
        let dl = engine.active.get(&hash).unwrap();
        assert!(
            dl.queued.contains(&0) || dl.in_flight.contains(&0),
            "chunk 0 should be re-queued or re-dispatched after hash mismatch"
        );
        assert!(
            !dl.done.contains(&0),
            "chunk 0 must not be done after mismatch"
        );
    }

    #[tokio::test]
    async fn on_chunk_request_failed_unknown_request_id_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, _rx, _db_dir) = make_engine(&tmp).await;
        // No active download — must not panic.
        engine
            .on_chunk_request_failed(fake_request_id(99), peer(1))
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chunk_request_failure_requeues_and_evicts_dead_peer() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x32u8; 32];
        let magnet = fake_magnet(&hash, "dead.bin", 12);
        let p = peer(1);

        let (acker_handle, stop_tx) = spawn_acker(rx);

        engine.start(&magnet, vec![p], 0, None).await.unwrap();
        engine
            .on_manifest_received(
                fake_request_id(1),
                p,
                ManifestResponse::Ok {
                    root_hash: hash,
                    name: "dead.bin".to_string(),
                    total_size: 12,
                    chunk_size: 12,
                    chunk_count: 1,
                },
                0,
            )
            .await;

        // chunk 0 is now in-flight to the only provider. Keep failing it: each
        // failure first backs the peer's concurrency down (4→2→1) and, once at
        // the floor, grows its failure streak — re-dispatching to the same peer
        // each time — until it is finally evicted. Loop until the chunk is no
        // longer in-flight (evicted, with no provider left to re-dispatch to).
        loop {
            let req_id = {
                let dl = engine.active.get(&hash).unwrap();
                match dl.inflight_map.keys().next() {
                    Some(&id) => id,
                    None => break,
                }
            };
            engine.on_chunk_request_failed(req_id, p).await;
        }

        let _ = stop_tx.send(());
        let _ = acker_handle.await.unwrap();

        let dl = engine.active.get(&hash).unwrap();
        assert!(
            dl.providers.is_empty(),
            "the only provider should be evicted after repeated failures"
        );
        assert!(
            dl.queued.contains(&0),
            "chunk must be back in the queue with no provider left to serve it"
        );
        assert!(dl.in_flight.is_empty(), "slot must be freed");
        assert!(!dl.done.contains(&0), "chunk must not be marked done");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeouts_back_off_per_peer_concurrency_before_evicting() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0x3au8; 32];
        let magnet = fake_magnet(&hash, "slow.bin", 12);
        let p = peer(1);
        let (acker_handle, stop_tx) = spawn_acker(rx);

        engine.start(&magnet, vec![p], 0, None).await.unwrap();
        engine
            .on_manifest_received(
                fake_request_id(1),
                p,
                ManifestResponse::Ok {
                    root_hash: hash,
                    name: "slow.bin".to_string(),
                    total_size: 12,
                    chunk_size: 12,
                    chunk_count: 1,
                },
                0,
            )
            .await;

        // Snapshot (global max_slots, this download's consecutive_failures).
        let state = |e: &DownloadEngine| {
            let max_slots = e
                .peer_caps
                .get(&p)
                .map(|c| c.max_slots)
                .unwrap_or(SLOTS_PER_PEER);
            let cf = e
                .active
                .get(&hash)
                .and_then(|dl| dl.peer_state.get(&p))
                .map(|ps| ps.consecutive_failures)
                .unwrap_or(0);
            (max_slots, cf)
        };
        // Fail whatever chunk is in-flight to `p` right now.
        async fn fail_once(e: &mut DownloadEngine, hash: &[u8; 32], p: PeerId) {
            let req_id = {
                let dl = e.active.get(hash).unwrap();
                *dl.inflight_map
                    .keys()
                    .next()
                    .expect("a chunk should be in-flight before each failure")
            };
            e.on_chunk_request_failed(req_id, p).await;
        }

        assert_eq!(state(&engine), (SLOTS_PER_PEER, 0));
        // Each timeout halves concurrency (4→2→1) without counting toward
        // eviction — the peer is slow, not gone.
        fail_once(&mut engine, &hash, p).await;
        assert_eq!(state(&engine), (2, 0));
        fail_once(&mut engine, &hash, p).await;
        assert_eq!(state(&engine), (1, 0));
        // At the floor, further timeouts grow the failure streak instead.
        fail_once(&mut engine, &hash, p).await;
        assert_eq!(state(&engine), (1, 1));
        // Still a provider (not yet at MAX_PEER_FAILURES).
        assert!(engine.active.get(&hash).unwrap().providers.contains(&p));

        let _ = stop_tx.send(());
        let _ = acker_handle.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_chunk_received_valid_marks_done_and_completes() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, rx, _db_dir) = make_engine(&tmp).await;
        let data = b"hello";
        // The root hash IS blake3 of the content; the chunk arrives as a bao
        // slice that verifies against it.
        let (hash, slice) = bao_one_chunk(data);
        let magnet = fake_magnet(&hash, "ok.bin", data.len() as u64);
        let p = peer(1);

        let (acker_handle, stop_tx) = spawn_acker(rx);

        engine.start(&magnet, vec![p], 0, None).await.unwrap();
        engine
            .on_manifest_received(
                fake_request_id(1),
                p,
                ManifestResponse::Ok {
                    root_hash: hash,
                    name: "ok.bin".to_string(),
                    total_size: data.len() as u64,
                    chunk_size: data.len() as u32,
                    chunk_count: 1,
                },
                0,
            )
            .await;

        let _ = stop_tx.send(());
        let _ = acker_handle.await.unwrap();

        // Inject a known request_id for chunk 0
        let req_id = fake_request_id(20);
        {
            let dl = engine.active.get_mut(&hash).unwrap();
            dl.inflight_map.clear();
            dl.in_flight.clear();
            for ps in dl.peer_state.values_mut() {
                ps.in_flight.clear();
            }
            dl.inflight_map.insert(req_id, (p, 0));
            dl.in_flight.insert(0);
            dl.peer_state.entry(p).or_default().in_flight.insert(0);
            // Also clear queued so on_chunk_received sees total == done
            dl.queued.clear();
        }

        engine
            .on_chunk_received(
                req_id,
                p,
                ChunkResponse::Ok {
                    data: slice,
                    peers: vec![],
                },
            )
            .await;

        // Download should be complete and removed from active
        assert!(
            !engine.active.contains_key(&hash),
            "completed download should be removed from active"
        );
        // File should exist on disk
        assert!(tmp.path().join("ok.bin").exists());
    }

    // PEX: add_providers always updates known_providers regardless of download
    // state, and also merges into active[hash].providers when a download is live.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pex_peers_added_to_known_providers_and_active() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (mut engine, rx, _db_dir) = make_engine(&tmp).await;
        let (acker, stop) = spawn_acker(rx);

        let magnet = format!("rucio:{}?name=pex.bin&size=1024", hex::encode([0xAAu8; 32]));
        let hash: [u8; 32] = [0xAAu8; 32];

        engine.start(&magnet, vec![peer(1)], 0, None).await.unwrap();

        let pex_peer = peer(42);

        // Add via add_providers while in pending_manifests state.
        engine.add_providers(hash, vec![pex_peer]).await;

        // Should be in known_providers unconditionally.
        assert!(
            engine
                .known_providers
                .get(&hash)
                .is_some_and(|v| v.contains(&pex_peer)),
            "PEX peer must be in known_providers"
        );
        // Should also be in pending_manifests providers.
        assert!(
            engine
                .pending_manifests
                .get(&hash)
                .is_some_and(|pm| pm.providers.contains(&pex_peer)),
            "PEX peer must be in pending_manifest providers"
        );

        let _ = stop.send(());
        let _ = acker.await;
    }

    // -----------------------------------------------------------------------
    // resume_interrupted()
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn resume_interrupted_finding_providers_re_issues_find_providers() {
        // A download that was in 'finding_providers' state when the daemon
        // shut down should be re-hydrated into pending_manifests and trigger
        // a new FindProviders command.
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, mut rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0xf1u8; 32];

        // Insert a placeholder row with status 'finding_providers' (no chunks).
        let id =
            db::downloads::create_pending(&engine.db, &hash, Some("ghost.bin"), 1_000, false, None)
                .await
                .unwrap()
                .id();
        assert!(id > 0);

        engine.resume_interrupted().await;

        // Should be tracked as a pending manifest.
        assert!(
            engine.pending_manifests.contains_key(&hash),
            "expected pending_manifests entry after resume"
        );

        // Should have issued FindProviders.
        let cmd = rx.try_recv().expect("expected a FindProviders command");
        assert!(
            matches!(cmd, NodeCmd::FindProviders(ref k) if k == hash.as_slice()),
            "expected FindProviders({hash:?}), got {cmd:?}"
        );
    }

    #[tokio::test]
    async fn resume_interrupted_queued_with_chunks_re_issues_find_providers() {
        // A download that had received its manifest (chunks saved) but was
        // still in 'downloading' state should be re-hydrated into active.
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, mut rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0xf2u8; 32];

        let id =
            db::downloads::create_pending(&engine.db, &hash, Some("resume.bin"), 1_000, true, None)
                .await
                .unwrap()
                .id();
        db::downloads::finalize_pending(
            &engine.db,
            id,
            "resume.bin",
            4096,
            tmp.path().join("resume.bin.part").to_str().unwrap(),
            1_000,
            &[(0, 4096)],
        )
        .await
        .unwrap();

        engine.resume_interrupted().await;

        // Should be tracked as an active download (manifest already known).
        assert!(
            engine.active.contains_key(&hash),
            "expected active entry after resume with chunks"
        );

        // Should have issued FindProviders to re-discover peers.
        let cmd = rx.try_recv().expect("expected a FindProviders command");
        assert!(
            matches!(cmd, NodeCmd::FindProviders(ref k) if k == hash.as_slice()),
            "expected FindProviders({hash:?}), got {cmd:?}"
        );
    }

    // -----------------------------------------------------------------------
    // pause() / resume()
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn pause_then_resume_rehydrates_active_download() {
        // Pausing drops the in-memory state but keeps the DB row and chunk
        // progress; resuming re-hydrates it and re-issues provider discovery.
        let tmp = tempfile::tempdir().unwrap();
        let (mut engine, mut rx, _db_dir) = make_engine(&tmp).await;
        let hash = [0xf3u8; 32];

        let id =
            db::downloads::create_pending(&engine.db, &hash, Some("pause.bin"), 1_000, true, None)
                .await
                .unwrap()
                .id();
        db::downloads::finalize_pending(
            &engine.db,
            id,
            "pause.bin",
            4096,
            tmp.path().join("pause.bin.part").to_str().unwrap(),
            1_000,
            &[(0, 4096)],
        )
        .await
        .unwrap();

        engine.resume_interrupted().await;
        assert!(engine.active.contains_key(&hash));
        // Drain the FindProviders issued by resume_interrupted().
        let _ = rx.try_recv();

        // Pause: the API handler sets the status, the engine drops in-memory state.
        db::downloads::set_status(&engine.db, id, "paused", None)
            .await
            .unwrap();
        engine.pause(id, hash.to_vec()).await;
        assert!(
            !engine.active.contains_key(&hash),
            "paused download must leave the active set"
        );
        // The DB row and its status must survive.
        assert_eq!(
            db::downloads::get_status(&engine.db, id)
                .await
                .unwrap()
                .as_deref(),
            Some("paused")
        );

        // Resume: re-hydrate from the DB.
        engine.resume(id).await;
        assert!(
            engine.active.contains_key(&hash),
            "resumed download must be active again"
        );
        let cmd = rx.try_recv().expect("expected FindProviders after resume");
        assert!(
            matches!(cmd, NodeCmd::FindProviders(ref k) if k == hash.as_slice()),
            "expected FindProviders({hash:?}), got {cmd:?}"
        );
        // Status should be back to a running state.
        assert_eq!(
            db::downloads::get_status(&engine.db, id)
                .await
                .unwrap()
                .as_deref(),
            Some("downloading")
        );
    }

    // -----------------------------------------------------------------------
    // persist_completed: deleted dir is recreated; an unusable dest keeps the .part
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn persist_completed_recreates_a_deleted_download_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("downloads");
        std::fs::create_dir_all(&dest).unwrap();
        // A finished .part, living in a separate temp dir.
        let part = tmp.path().join("movie.mkv.part");
        tokio::fs::write(&part, b"payload").await.unwrap();
        // The user deletes the download dir while the download was running.
        std::fs::remove_dir_all(&dest).unwrap();

        let final_path = persist_completed(&dest, &part, "movie.mkv", "a1b2c3d4")
            .await
            .expect("should recreate the dir and move the file");

        assert_eq!(final_path, dest.join("movie.mkv"));
        assert!(final_path.exists(), "file landed in the recreated dir");
        assert!(!part.exists(), ".part was moved, not copied");
    }

    #[tokio::test]
    async fn persist_completed_disambiguates_a_name_collision() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("pins");
        std::fs::create_dir_all(&dest).unwrap();
        // A file with the target name already exists (a different one).
        tokio::fs::write(dest.join("tvshow.nfo"), b"other")
            .await
            .unwrap();

        let part = tmp.path().join("incoming.part");
        tokio::fs::write(&part, b"mine").await.unwrap();
        let final_path = persist_completed(&dest, &part, "tvshow.nfo", "a1b2c3d4")
            .await
            .unwrap();

        // The frag is inserted before the extension; the existing file survives.
        assert_eq!(final_path, dest.join("tvshow.a1b2c3d4.nfo"));
        assert_eq!(
            tokio::fs::read(dest.join("tvshow.nfo")).await.unwrap(),
            b"other"
        );
        assert_eq!(tokio::fs::read(&final_path).await.unwrap(), b"mine");
    }

    #[tokio::test]
    async fn persist_completed_keeps_part_when_dest_is_unusable() {
        let tmp = tempfile::tempdir().unwrap();
        // A regular file where a parent directory is expected: create_dir_all
        // then fails with ENOTDIR — a portable stand-in for "dir unwritable".
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let dest = blocker.join("downloads");
        let part = tmp.path().join("movie.mkv.part");
        tokio::fs::write(&part, b"payload").await.unwrap();

        let res = persist_completed(&dest, &part, "movie.mkv", "a1b2c3d4").await;

        assert!(res.is_err(), "unusable dest must surface an error");
        assert!(part.exists(), ".part is preserved — nothing is lost");
    }
}
