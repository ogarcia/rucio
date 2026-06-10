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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Scheduling priority for a [`TokenBucket::acquire`] call. When the cap is
/// saturated, `High` transfers get the larger share but `Low` keeps a reserved
/// floor (see [`HIGH_QUANTA_PER_LOW`]) rather than being starved — and either
/// gets the whole cap when the other is idle (work-conserving). Rucio (libp2p)
/// transfers use `High`; eMule transfers use `Low`, so the lure protocol yields
/// to the real network without us becoming a bad eMule citizen.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Priority {
    High,
    Low,
}

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
        } else if bytes as f64 > self.burst as f64 && self.tokens >= self.burst as f64 {
            // A single request larger than the burst cap can never be covered
            // by the bucket: `refill` clamps `tokens` to `burst`, so waiting
            // would loop forever (`tokens` plateaus below `bytes`). Once the
            // bucket is full, grant it anyway and let the balance go negative.
            // The deficit is repaid by `refill` before the next acquire
            // succeeds, so the long-run rate still holds. Without this, a chunk
            // larger than `rate * BURST_SECS` — e.g. a 4 MiB chunk under any
            // sub-2 MB/s upload limit — would hang `acquire` forever, stalling
            // the upload and (via a leaked `highid_active`) starving every
            // LowID download until the inbound request times out.
            self.tokens -= bytes as f64;
            Ok(())
        } else {
            let deficit = bytes as f64 - self.tokens;
            let wait_secs = deficit / self.rate as f64;
            Err(Duration::from_secs_f64(wait_secs))
        }
    }
}

/// Fairness quantum: the most one acquirer consumes before releasing the FIFO
/// turn. Splitting every `acquire` into quanta and re-queuing between them makes
/// the bucket round-robin its tokens by *bytes* across all waiting transfers, so
/// a large-chunk transfer (e.g. a 4 MiB libp2p chunk) can't starve a
/// small-chunk one (e.g. a ~180 KiB eMule block) when the cap is saturated.
const FAIR_QUANTUM: u64 = 64 * 1024;

/// How many [`Priority::High`] quanta are served before a waiting
/// [`Priority::Low`] one is let through, when both saturate the cap. This
/// reserves Low (eMule) a floor of ~`1/(N+1)` of the bandwidth — good-citizen
/// seeding to the eMule network is never fully starved by Rucio traffic — while
/// still giving Rucio the lion's share. A user who wants 100% for Rucio just
/// disables eMule.
const HIGH_QUANTA_PER_LOW: usize = 4;

pub struct TokenBucket {
    inner: Mutex<Inner>,
    /// FIFO turnstile ordering concurrent acquirers. tokio's `Mutex` grants in
    /// request order, so holding it for one [`FAIR_QUANTUM`] at a time shares
    /// the cap fairly instead of letting whoever wins the lock race monopolise
    /// it. Only contended (and only meaningfully held) when a limit is set.
    turn: tokio::sync::Mutex<()>,
    /// Number of [`Priority::High`] transfers currently inside `acquire`.
    /// [`Priority::Low`] callers defer to these (see [`HIGH_QUANTA_PER_LOW`]).
    high_active: AtomicUsize,
    /// High quanta served since the last Low quantum. Once it reaches
    /// [`HIGH_QUANTA_PER_LOW`], a waiting Low caller is let through and resets
    /// it — this is what reserves Low its bandwidth floor.
    high_quanta: AtomicUsize,
    /// Woken when High yields a turn (count drained to zero, or the weighted
    /// allowance reached), so parked `Low` callers re-check and proceed.
    low_gate: tokio::sync::Notify,
}

impl TokenBucket {
    /// Create a new token bucket.  `rate_kbps = 0` means unlimited.
    pub fn new(rate_kbps: u64) -> Self {
        Self {
            inner: Mutex::new(Inner::new(rate_kbps)),
            turn: tokio::sync::Mutex::new(()),
            high_active: AtomicUsize::new(0),
            high_quanta: AtomicUsize::new(0),
            low_gate: tokio::sync::Notify::new(),
        }
    }

    /// Block the current async task until `bytes` tokens are available.
    ///
    /// When the limit is 0 this returns immediately without any sleep or
    /// queueing. Under a limit, the request is consumed in [`FAIR_QUANTUM`]-sized
    /// steps through a FIFO turnstile, so concurrent transfers of the same
    /// priority round-robin the available tokens by bytes. Across priorities,
    /// [`Priority::Low`] callers yield each quantum to any active
    /// [`Priority::High`] caller (see [`Priority`]).
    pub async fn acquire(&self, bytes: u64, prio: Priority) {
        // Unlimited: no throttling, and no serialisation (taking the turnstile
        // would needlessly cap concurrency when there is no rate to share).
        if self.rate_kbps() == 0 {
            return;
        }
        // A High caller stays counted for its whole acquire, so Low callers
        // keep yielding until it has fully drained its request.
        let _hi = (prio == Priority::High).then(|| HighActive::new(self));

        let mut remaining = bytes;
        while remaining > 0 {
            let take = remaining.min(FAIR_QUANTUM);
            // Low priority defers to in-flight High transfers — but only until
            // High has spent its weighted allowance, then Low takes a guaranteed
            // turn (its reserved share). The `notified()` future is armed before
            // the check so a High caller yielding between the two isn't missed.
            if prio == Priority::Low {
                loop {
                    let notified = self.low_gate.notified();
                    if self.high_active.load(Ordering::Acquire) == 0
                        || self.high_quanta.load(Ordering::Acquire) >= HIGH_QUANTA_PER_LOW
                    {
                        break;
                    }
                    notified.await;
                }
            }
            // Hold the turn only while consuming this quantum; dropping it
            // between quanta lets the next waiter take its turn (round-robin).
            {
                let _turn = self.turn.lock().await;
                loop {
                    let wait = {
                        let mut g = self.inner.lock().unwrap();
                        g.try_consume(take)
                    };
                    match wait {
                        Ok(()) => break,
                        Err(d) => tokio::time::sleep(d).await,
                    }
                }
            }
            // Weighted bookkeeping: count High quanta and, on reaching the
            // allowance, release a Low turn; a Low quantum resets the allowance.
            match prio {
                Priority::High => {
                    if self.high_quanta.fetch_add(1, Ordering::AcqRel) + 1 == HIGH_QUANTA_PER_LOW {
                        self.low_gate.notify_waiters();
                    }
                }
                Priority::Low => self.high_quanta.store(0, Ordering::Release),
            }
            remaining -= take;
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

/// Marks a [`Priority::High`] transfer as active for the lifetime of an
/// `acquire`. On drop it decrements the count and, when it was the last one,
/// wakes parked [`Priority::Low`] callers. A guard makes the decrement
/// exception-safe — a leaked count would block every Low transfer forever.
struct HighActive<'a> {
    bucket: &'a TokenBucket,
}

impl<'a> HighActive<'a> {
    fn new(bucket: &'a TokenBucket) -> Self {
        bucket.high_active.fetch_add(1, Ordering::Release);
        Self { bucket }
    }
}

impl Drop for HighActive<'_> {
    fn drop(&mut self) {
        if self.bucket.high_active.fetch_sub(1, Ordering::Release) == 1 {
            self.bucket.low_gate.notify_waiters();
        }
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
        tb.acquire(1024 * 1024, Priority::High).await;
    }

    #[test]
    fn request_larger_than_burst_is_granted_when_full() {
        // 1 KB/s → burst = 2048 bytes, far smaller than a 4 MiB chunk.
        let tb = TokenBucket::new(1);
        let mut g = tb.inner.lock().unwrap();
        // A fresh bucket is full; the oversized request must be granted rather
        // than looping forever (the bug that hung uploads under a low limit).
        assert!(g.try_consume(4 * 1024 * 1024).is_ok());
        // The balance is now negative, so the next request must wait.
        assert!(g.tokens < 0.0);
        assert!(g.try_consume(1).is_err());
    }

    #[tokio::test]
    async fn acquire_quantum_over_burst_completes_on_full_bucket() {
        // A single quantum larger than the burst cap (1 KB/s → burst 2 KiB)
        // would otherwise loop forever; on a full bucket the oversized special
        // case grants it immediately, so acquire returns instead of hanging.
        let tb = TokenBucket::new(1);
        tb.acquire(3 * 1024, Priority::High).await;
    }

    #[tokio::test]
    async fn acquire_still_rate_limits_after_burst() {
        // Fairness splits a request into quanta but must not relax the rate:
        // after draining the burst, a further 500 KiB at 1000 KB/s cannot be
        // delivered faster than ~0.5 s. Lower-bound only, so it's not flaky.
        let tb = TokenBucket::new(1000);
        tb.acquire(2000 * 1024, Priority::High).await; // drain the full burst
        let t0 = Instant::now();
        tb.acquire(500 * 1024, Priority::High).await;
        assert!(t0.elapsed() >= Duration::from_millis(350));
    }

    #[tokio::test]
    async fn high_priority_gets_the_larger_share_but_low_is_not_starved() {
        use std::sync::atomic::AtomicU64;

        let tb = Arc::new(TokenBucket::new(1000)); // 1000 KB/s
        tb.acquire(2000 * 1024, Priority::High).await; // drain the burst

        let (hi, lo) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));

        // Two transfers contend flat-out for the saturated cap.
        let (tbh, h) = (tb.clone(), hi.clone());
        let high = tokio::spawn(async move {
            loop {
                tbh.acquire(64 * 1024, Priority::High).await;
                h.fetch_add(64 * 1024, Ordering::Relaxed);
            }
        });
        let (tbl, l) = (tb.clone(), lo.clone());
        let low = tokio::spawn(async move {
            loop {
                tbl.acquire(64 * 1024, Priority::Low).await;
                l.fetch_add(64 * 1024, Ordering::Relaxed);
            }
        });

        tokio::time::sleep(Duration::from_millis(800)).await;
        high.abort();
        low.abort();

        let (h, l) = (hi.load(Ordering::Relaxed), lo.load(Ordering::Relaxed));
        // High wins the larger share, but Low keeps its reserved floor.
        assert!(l > 0, "Low (eMule) was starved");
        assert!(h > l, "High (Rucio) did not get the larger share");
    }
}
