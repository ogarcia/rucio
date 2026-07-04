//! Types for `GET /api/v1/metrics` and `GET /health`.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

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
    }
}

/// Response body for `GET /api/v1/metrics`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetricsResponse {
    pub session: SessionMetrics,
    pub total: TotalMetrics,
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
}
