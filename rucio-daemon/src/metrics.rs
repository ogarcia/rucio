//! In-memory session metrics with atomic counters and a 5-second sliding
//! window for upload/download speed estimation.
//!
//! The `Metrics` struct is stored as `Arc<Metrics>` in `AppState` and shared
//! across the transfer engine and API handlers.  All counters use
//! `AtomicU64` with `Relaxed` ordering — exact precision is not required for
//! display purposes, and we avoid any mutex overhead on the hot path.
//!
//! ## Speed estimation
//!
//! A ring buffer of 5 one-second buckets accumulates bytes transferred.  The
//! `tick()` method (called every second from the main loop) rotates the
//! bucket and recomputes the rolling average.  The computed speeds are stored
//! as a separate pair of `AtomicU64` so handlers can read them without
//! holding any lock.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rucio_core::api::metrics::SessionMetrics;

// ---------------------------------------------------------------------------
// Speed window
// ---------------------------------------------------------------------------

const WINDOW: usize = 5;

/// Rolling 5-second byte counter used for speed estimation.
pub(crate) struct SpeedWindow {
    buckets: [u64; WINDOW],
    head: usize,
    /// Bytes accumulated in the current (not-yet-sealed) bucket.
    current: u64,
}

impl SpeedWindow {
    pub(crate) const fn new() -> Self {
        Self {
            buckets: [0; WINDOW],
            head: 0,
            current: 0,
        }
    }

    /// Record `bytes` transferred now (call on every chunk event).
    pub(crate) fn add(&mut self, bytes: u64) {
        self.current += bytes;
    }

    /// Seal the current bucket, advance the ring, return bytes/s average.
    pub(crate) fn tick(&mut self) -> u64 {
        // Seal current second into the ring.
        self.buckets[self.head % WINDOW] = self.current;
        self.head += 1;
        self.current = 0;

        // Average over the filled portion of the window.
        let filled = WINDOW.min(self.head);
        let sum: u64 = self.buckets.iter().sum();
        if filled == 0 { 0 } else { sum / filled as u64 }
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

pub struct Metrics {
    // --- raw counters (updated on every chunk event) ---
    pub uploaded_bytes: AtomicU64,
    pub downloaded_bytes: AtomicU64,
    pub chunks_served: AtomicU64,
    pub chunks_received: AtomicU64,
    pub chunks_rejected: AtomicU64,

    // --- derived speeds (updated every second by tick()) ---
    pub upload_speed: AtomicU64,
    pub download_speed: AtomicU64,

    // --- speed accumulators (protected by a mutex — updated infrequently) ---
    up_window: Mutex<SpeedWindow>,
    down_window: Mutex<SpeedWindow>,

    /// Unix timestamp of daemon start (set once at construction).
    pub started_at: u64,

    // --- last-persisted snapshot (so we only flush deltas to DB) ---
    last_up: AtomicU64,
    last_down: AtomicU64,
    last_served: AtomicU64,
    last_received: AtomicU64,
    last_rejected: AtomicU64,
}

impl Metrics {
    pub fn new(started_at: u64) -> Self {
        Self {
            uploaded_bytes: AtomicU64::new(0),
            downloaded_bytes: AtomicU64::new(0),
            chunks_served: AtomicU64::new(0),
            chunks_received: AtomicU64::new(0),
            chunks_rejected: AtomicU64::new(0),
            upload_speed: AtomicU64::new(0),
            download_speed: AtomicU64::new(0),
            up_window: Mutex::new(SpeedWindow::new()),
            down_window: Mutex::new(SpeedWindow::new()),
            started_at,
            last_up: AtomicU64::new(0),
            last_down: AtomicU64::new(0),
            last_served: AtomicU64::new(0),
            last_received: AtomicU64::new(0),
            last_rejected: AtomicU64::new(0),
        }
    }

    // -----------------------------------------------------------------------
    // Record events (called from the transfer engine)
    // -----------------------------------------------------------------------

    /// Record a chunk successfully served to a remote peer.
    pub fn record_upload(&self, bytes: u64) {
        self.uploaded_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.chunks_served.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut w) = self.up_window.lock() {
            w.add(bytes);
        }
    }

    /// Record a chunk received and hash-verified OK.
    pub fn record_download(&self, bytes: u64) {
        self.downloaded_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.chunks_received.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut w) = self.down_window.lock() {
            w.add(bytes);
        }
    }

    /// Record a chunk that failed hash verification.
    pub fn record_rejected(&self) {
        self.chunks_rejected.fetch_add(1, Ordering::Relaxed);
    }

    // -----------------------------------------------------------------------
    // Periodic tick — call every second from the main loop
    // -----------------------------------------------------------------------

    /// Advance the speed windows and update the cached speed atomics.
    /// Returns the delta since the last call to `flush_delta()`.
    pub fn tick(&self) {
        if let Ok(mut w) = self.up_window.lock() {
            let spd = w.tick();
            self.upload_speed.store(spd, Ordering::Relaxed);
        }
        if let Ok(mut w) = self.down_window.lock() {
            let spd = w.tick();
            self.download_speed.store(spd, Ordering::Relaxed);
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot for the API handler
    // -----------------------------------------------------------------------

    pub fn session_snapshot(&self) -> SessionMetrics {
        SessionMetrics {
            uploaded_bytes: self.uploaded_bytes.load(Ordering::Relaxed),
            downloaded_bytes: self.downloaded_bytes.load(Ordering::Relaxed),
            upload_speed: self.upload_speed.load(Ordering::Relaxed),
            download_speed: self.download_speed.load(Ordering::Relaxed),
            chunks_served: self.chunks_served.load(Ordering::Relaxed),
            chunks_received: self.chunks_received.load(Ordering::Relaxed),
            chunks_rejected: self.chunks_rejected.load(Ordering::Relaxed),
            started_at: self.started_at,
        }
    }

    // -----------------------------------------------------------------------
    // Delta flush — called periodically to persist increments to SQLite
    // -----------------------------------------------------------------------

    /// Compute and return the delta since the last `flush_delta()` call.
    /// Advances the "last persisted" snapshot atomically.
    pub fn take_delta(&self) -> rucio_core::api::metrics::TotalMetrics {
        let up = self.uploaded_bytes.load(Ordering::Relaxed);
        let down = self.downloaded_bytes.load(Ordering::Relaxed);
        let served = self.chunks_served.load(Ordering::Relaxed);
        let received = self.chunks_received.load(Ordering::Relaxed);
        let rejected = self.chunks_rejected.load(Ordering::Relaxed);

        let d_up = up.saturating_sub(self.last_up.swap(up, Ordering::Relaxed));
        let d_down = down.saturating_sub(self.last_down.swap(down, Ordering::Relaxed));
        let d_served = served.saturating_sub(self.last_served.swap(served, Ordering::Relaxed));
        let d_recv = received.saturating_sub(self.last_received.swap(received, Ordering::Relaxed));
        let d_rej = rejected.saturating_sub(self.last_rejected.swap(rejected, Ordering::Relaxed));

        rucio_core::api::metrics::TotalMetrics {
            uploaded_bytes: d_up,
            downloaded_bytes: d_down,
            chunks_served: d_served,
            chunks_received: d_recv,
            chunks_rejected: d_rej,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers used in tests
// ---------------------------------------------------------------------------

#[cfg(test)]
impl Default for Metrics {
    fn default() -> Self {
        Self::new(0)
    }
}

// ---------------------------------------------------------------------------
// Instant-based started_at helper
// ---------------------------------------------------------------------------

/// Convert an `Instant` to Unix seconds by anchoring it against `SystemTime`.
///
/// This is only approximate (±1 s), which is fine for display.
pub fn instant_to_unix(instant: &Instant) -> u64 {
    let elapsed = instant.elapsed();
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(elapsed.as_secs())
}
