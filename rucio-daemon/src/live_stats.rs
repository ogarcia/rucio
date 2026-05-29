//! Per-download live statistics shared between the transfer engines and the
//! API handlers.
//!
//! The libp2p [`crate::transfer::DownloadEngine`] and each eMule
//! `run_ed2k_download` task publish their volatile state here (source counts,
//! pieces in flight); a sampler in the main loop fills in the per-download
//! download speed.  The `GET /api/v1/downloads/{id}` handler reads a snapshot
//! to enrich the download-detail response.
//!
//! The map is keyed by the **signed** download id, matching the public API
//! convention: positive ids are libp2p downloads, negative ids are eMule.
//! Entries exist only while a download is active and are removed when it
//! completes, fails, or is cancelled.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

/// Volatile, non-persisted statistics for a single in-progress download.
#[derive(Debug, Clone, Default)]
pub struct DownloadLiveStats {
    /// Sources/providers currently known for this download.
    pub sources_total: u32,
    /// Sources/providers we are actively transferring from right now.
    pub sources_active: u32,
    /// Chunks (libp2p) or slices (eMule) being fetched at this moment.
    pub pieces_in_flight: u32,
    /// Indices of the pieces being fetched right now. Consumed by
    /// `GET /api/v1/downloads/{id}/pieces` to render a per-piece block bar.
    pub in_flight_pieces: Vec<u32>,
    /// Smoothed download speed in bytes per second (filled by the sampler).
    pub speed_bps: u64,
}

/// Shared map of live stats keyed by signed download id.
pub type LiveStatsMap = Arc<RwLock<HashMap<i64, DownloadLiveStats>>>;
