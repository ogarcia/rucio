//! Bandwidth throttling via a token-bucket rate limiter.
//!
//! ## Design
//!
//! A [`TokenBucket`] is a standard leaky-bucket / token-bucket hybrid:
//!
//! - Tokens accumulate at `rate` bytes per second up to a `burst` cap.
//! - Callers call [`TokenBucket::acquire`] with the number of bytes they want
//!   to send/receive.  The method returns immediately when there are enough
//!   tokens; otherwise it sleeps for the exact duration needed to refill.
//! - A limit of `0` means **unlimited** — `acquire` always returns immediately.
//!
//! ## Thread safety
//!
//! [`TokenBucket`] wraps an inner [`Mutex`] so it can be shared as
//! `Arc<TokenBucket>` across many concurrent tasks without external locking.
//!
//! ## Hot reconfiguration
//!
//! [`TokenBucket::set_rate`] replaces the rate at runtime.  The next call to
//! `acquire` will use the new rate.  Existing sleeps in flight are not
//! interrupted — they will overshoot by at most one chunk duration, which is
//! acceptable for a display-level bandwidth limit.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Burst multiplier: allow up to 2 seconds of accumulated tokens.
const BURST_SECS: f64 = 2.0;

struct Inner {
    /// Bytes per second.  0 = unlimited.
    rate: u64,
    /// Maximum token accumulation (burst cap), in bytes.
    burst: u64,
    /// Currently available tokens (bytes).
    tokens: f64,
    /// Last time tokens were refilled.
    last_refill: Instant,
}

impl Inner {
    fn new(rate_kbps: u64) -> Self {
        let rate = rate_kbps * 1024;
        let burst = ((rate as f64 * BURST_SECS) as u64).max(rate);
        Self {
            rate,
            burst,
            tokens: burst as f64,
            last_refill: Instant::now(),
        }
    }

    /// Refill tokens based on elapsed time since last call.
    fn refill(&mut self) {
        if self.rate == 0 {
            return;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.rate as f64).min(self.burst as f64);
    }

    /// Try to consume `bytes` tokens.
    ///
    /// Returns `Ok(())` if tokens were available, or `Err(wait)` with the
    /// duration to sleep before retrying.
    fn try_consume(&mut self, bytes: u64) -> Result<(), Duration> {
        if self.rate == 0 {
            return Ok(());
        }
        self.refill();
        if self.tokens >= bytes as f64 {
            self.tokens -= bytes as f64;
            Ok(())
        } else {
            let deficit = bytes as f64 - self.tokens;
            let wait_secs = deficit / self.rate as f64;
            Err(Duration::from_secs_f64(wait_secs))
        }
    }
}

pub struct TokenBucket {
    inner: Mutex<Inner>,
}

impl TokenBucket {
    /// Create a new token bucket.  `rate_kbps = 0` means unlimited.
    pub fn new(rate_kbps: u64) -> Self {
        Self {
            inner: Mutex::new(Inner::new(rate_kbps)),
        }
    }

    /// Block the current async task until `bytes` tokens are available.
    ///
    /// When the limit is 0 this returns immediately without any sleep.
    pub async fn acquire(&self, bytes: u64) {
        loop {
            let wait = {
                let mut g = self.inner.lock().unwrap();
                g.try_consume(bytes)
            };
            match wait {
                Ok(()) => return,
                Err(d) => tokio::time::sleep(d).await,
            }
        }
    }

    /// Change the rate at runtime.  `rate_kbps = 0` disables throttling.
    ///
    /// The burst cap is recalculated.  Accumulated tokens are clamped to the
    /// new burst cap so a sudden rate *decrease* doesn't grant a free burst.
    pub fn set_rate(&self, rate_kbps: u64) {
        let mut g = self.inner.lock().unwrap();
        g.rate = rate_kbps * 1024;
        g.burst = ((g.rate as f64 * BURST_SECS) as u64).max(g.rate);
        // Clamp existing tokens to new burst so we don't overshoot.
        g.tokens = g.tokens.min(g.burst as f64);
    }

    /// Return the current rate in KB/s (0 = unlimited).
    pub fn rate_kbps(&self) -> u64 {
        self.inner.lock().unwrap().rate / 1024
    }
}

// ---------------------------------------------------------------------------
// Bandwidth state (base + temporary limit toggle)
// ---------------------------------------------------------------------------

/// Combine a base (normal) limit with a temporary one, returning the more
/// restrictive of the two. `0` means unlimited, so it never wins over a real
/// cap — engaging a temporary limit can only tighten the rate, never relax it.
pub fn restrictive(base_kbps: u64, temp_kbps: u64) -> u64 {
    match (base_kbps, temp_kbps) {
        (0, t) => t,
        (b, 0) => b,
        (b, t) => b.min(t),
    }
}

/// Source of truth for the bandwidth limits and the temporary-limit toggle.
///
/// Holds the user's normal ("base") caps, the temporary caps, and whether the
/// temporary limit is engaged. On any change it recomputes the effective rate
/// — the more [`restrictive`] of base/temp while engaged, otherwise the base —
/// and pushes it to the two token buckets the transfer paths consume. Keeping
/// the base here, rather than reading it back from the buckets, lets the
/// buckets carry the temporary override without losing the value to restore
/// when the toggle is switched off.
pub struct BandwidthState {
    upload: Arc<TokenBucket>,
    download: Arc<TokenBucket>,
    temp_active: AtomicBool,
    base_upload_kbps: AtomicU64,
    base_download_kbps: AtomicU64,
    temp_upload_kbps: AtomicU64,
    temp_download_kbps: AtomicU64,
}

impl BandwidthState {
    pub fn new(
        upload: Arc<TokenBucket>,
        download: Arc<TokenBucket>,
        base_upload_kbps: u64,
        base_download_kbps: u64,
        temp_upload_kbps: u64,
        temp_download_kbps: u64,
    ) -> Self {
        let s = Self {
            upload,
            download,
            temp_active: AtomicBool::new(false),
            base_upload_kbps: AtomicU64::new(base_upload_kbps),
            base_download_kbps: AtomicU64::new(base_download_kbps),
            temp_upload_kbps: AtomicU64::new(temp_upload_kbps),
            temp_download_kbps: AtomicU64::new(temp_download_kbps),
        };
        s.apply();
        s
    }

    /// Recompute the effective rate and push it to both buckets.
    fn apply(&self) {
        let (u, d) = if self.temp_active.load(Ordering::SeqCst) {
            (
                restrictive(
                    self.base_upload_kbps.load(Ordering::SeqCst),
                    self.temp_upload_kbps.load(Ordering::SeqCst),
                ),
                restrictive(
                    self.base_download_kbps.load(Ordering::SeqCst),
                    self.temp_download_kbps.load(Ordering::SeqCst),
                ),
            )
        } else {
            (
                self.base_upload_kbps.load(Ordering::SeqCst),
                self.base_download_kbps.load(Ordering::SeqCst),
            )
        };
        self.upload.set_rate(u);
        self.download.set_rate(d);
    }

    pub fn temp_active(&self) -> bool {
        self.temp_active.load(Ordering::SeqCst)
    }
    pub fn base_upload_kbps(&self) -> u64 {
        self.base_upload_kbps.load(Ordering::SeqCst)
    }
    pub fn base_download_kbps(&self) -> u64 {
        self.base_download_kbps.load(Ordering::SeqCst)
    }
    pub fn temp_upload_kbps(&self) -> u64 {
        self.temp_upload_kbps.load(Ordering::SeqCst)
    }
    pub fn temp_download_kbps(&self) -> u64 {
        self.temp_download_kbps.load(Ordering::SeqCst)
    }
    /// Rate currently applied to the upload bucket (KB/s, 0 = unlimited).
    pub fn effective_upload_kbps(&self) -> u64 {
        self.upload.rate_kbps()
    }
    /// Rate currently applied to the download bucket (KB/s, 0 = unlimited).
    pub fn effective_download_kbps(&self) -> u64 {
        self.download.rate_kbps()
    }

    /// Engage or release the temporary limit.
    pub fn set_temp_active(&self, on: bool) {
        self.temp_active.store(on, Ordering::SeqCst);
        self.apply();
    }

    /// Update the user's normal caps (e.g. from `PUT /config`) and re-apply.
    pub fn set_base(&self, upload_kbps: u64, download_kbps: u64) {
        self.base_upload_kbps.store(upload_kbps, Ordering::SeqCst);
        self.base_download_kbps
            .store(download_kbps, Ordering::SeqCst);
        self.apply();
    }

    /// Update the temporary caps (e.g. from `PUT /config`) and re-apply.
    pub fn set_temp(&self, upload_kbps: u64, download_kbps: u64) {
        self.temp_upload_kbps.store(upload_kbps, Ordering::SeqCst);
        self.temp_download_kbps
            .store(download_kbps, Ordering::SeqCst);
        self.apply();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_never_blocks() {
        let tb = TokenBucket::new(0);
        // try_consume should always succeed
        let result = tb.inner.lock().unwrap().try_consume(1024 * 1024);
        assert!(result.is_ok());
    }

    #[test]
    fn limited_returns_wait_when_exhausted() {
        // 1 KB/s → burst = 2048 bytes
        let tb = TokenBucket::new(1);
        let mut g = tb.inner.lock().unwrap();
        // Consume entire burst.
        assert!(g.try_consume(2048).is_ok());
        // Next consume should fail with a wait.
        assert!(g.try_consume(1024).is_err());
    }

    #[test]
    fn set_rate_to_zero_disables_limit() {
        let tb = TokenBucket::new(100);
        tb.set_rate(0);
        let result = tb.inner.lock().unwrap().try_consume(u64::MAX);
        assert!(result.is_ok());
    }

    #[test]
    fn set_rate_clamps_tokens_to_new_burst() {
        // Start at 1000 KB/s → burst 2 000 KB
        let tb = TokenBucket::new(1000);
        // Drop to 1 KB/s → burst 2 KB
        tb.set_rate(1);
        let g = tb.inner.lock().unwrap();
        assert!(g.tokens <= g.burst as f64);
    }

    #[tokio::test]
    async fn acquire_unlimited_is_instant() {
        let tb = TokenBucket::new(0);
        // Should complete without sleeping.
        tb.acquire(1024 * 1024).await;
    }
}
