//! Types for `GET /api/v1/metrics` and `GET /health`.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Upload/download share ratio (uploaded ÷ downloaded).
///
/// `None` when nothing has been downloaded yet, so the ratio is undefined —
/// callers render that as "∞" when anything was uploaded, else zero.
pub fn share_ratio(uploaded_bytes: u64, downloaded_bytes: u64) -> Option<f64> {
    (downloaded_bytes > 0).then(|| uploaded_bytes as f64 / downloaded_bytes as f64)
}

/// Per-session transfer counters (since last daemon start, in memory only).
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct SessionMetrics {
    /// Total bytes uploaded (chunks served) this session.
    pub uploaded_bytes: u64,
    /// Total bytes downloaded (chunks received and verified) this session.
    pub downloaded_bytes: u64,
    /// Current upload speed in bytes/s (5-second moving average).
    pub upload_speed: u64,
    /// Current download speed in bytes/s (5-second moving average).
    pub download_speed: u64,
    /// Number of chunk requests successfully served to remote peers.
    pub chunks_served: u64,
    /// Number of chunk responses received and hash-verified OK.
    pub chunks_received: u64,
    /// Number of chunk responses rejected due to hash mismatch.
    pub chunks_rejected: u64,
    /// Upload/download share ratio this session (`null` if nothing downloaded).
    pub ratio: Option<f64>,
    /// Unix timestamp (seconds) of daemon start.
    pub started_at: u64,
}

/// Cumulative totals persisted in the database (survives restarts).
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TotalMetrics {
    /// Total bytes uploaded across all sessions.
    pub uploaded_bytes: u64,
    /// Total bytes downloaded across all sessions.
    pub downloaded_bytes: u64,
    /// Total chunks served across all sessions.
    pub chunks_served: u64,
    /// Total chunks received across all sessions.
    pub chunks_received: u64,
    /// Total chunks rejected across all sessions.
    pub chunks_rejected: u64,
    /// Total seconds the daemon has been running across all sessions.
    pub uptime_seconds: u64,
    /// Cumulative upload/download share ratio (`null` if nothing downloaded).
    ///
    /// Derived from the absolute totals at the response boundary; it is left
    /// `None` on the delta instances used by [`TotalMetrics::add`] / the flush,
    /// where a ratio is meaningless.
    pub ratio: Option<f64>,
}

impl TotalMetrics {
    /// Add another set of totals into this one (saturating).
    ///
    /// Used to overlay the not-yet-persisted session delta onto the stored
    /// totals so the API can present a live cumulative figure.
    pub fn add(&mut self, other: &TotalMetrics) {
        self.uploaded_bytes = self.uploaded_bytes.saturating_add(other.uploaded_bytes);
        self.downloaded_bytes = self.downloaded_bytes.saturating_add(other.downloaded_bytes);
        self.chunks_served = self.chunks_served.saturating_add(other.chunks_served);
        self.chunks_received = self.chunks_received.saturating_add(other.chunks_received);
        self.chunks_rejected = self.chunks_rejected.saturating_add(other.chunks_rejected);
        self.uptime_seconds = self.uptime_seconds.saturating_add(other.uptime_seconds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn share_ratio_edge_cases() {
        // Undefined while nothing is downloaded, regardless of uploads.
        assert_eq!(share_ratio(0, 0), None);
        assert_eq!(share_ratio(500, 0), None);
        // Normal ratio.
        assert_eq!(share_ratio(3000, 1000), Some(3.0));
        assert_eq!(share_ratio(0, 1000), Some(0.0));
    }
}

/// Response body for `GET /api/v1/metrics`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetricsResponse {
    /// In-memory counters since the last daemon start; reset to zero on restart.
    pub session: SessionMetrics,
    /// Cumulative counters persisted in SQLite, carried across restarts.
    pub total: TotalMetrics,
    /// Active download connections right now: the number of (file, peer)
    /// transfer pairs across all downloads (each download's active-source
    /// count, summed). A file pulled from three peers counts as three.
    #[serde(default)]
    pub download_conns: usize,
    /// Active upload connections right now: the number of (peer, file) transfer
    /// pairs being served. Counted the same way as `download_conns`, so the two
    /// figures are directly comparable.
    #[serde(default)]
    pub upload_conns: usize,
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

/// Response body for `GET /health`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    /// `"ok"` when the daemon is running normally.
    pub status: String,
    /// Daemon version string (from `CARGO_PKG_VERSION`).
    pub version: String,
    /// Short git commit hash the daemon was built from, or empty when git was
    /// unavailable at build time.
    #[serde(default)]
    pub commit: String,
}
