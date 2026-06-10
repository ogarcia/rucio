//! Work-conserving upload priority scheduler.
//!
//! HighID peers (globally reachable) are served before LowID peers (behind
//! NAT).  LowID requests wait in `wait_for_lowid_turn()` until no HighID
//! upload is currently competing for the bandwidth throttle.  When the node
//! is idle the wait returns immediately, so LowID still gets full throughput.
//!
//! To stop a steady stream of HighID uploads from starving LowID peers, the
//! wait is capped at [`MAX_LOWID_WAIT`]: past that a LowID takes a turn anyway
//! (then shares the bandwidth bucket fairly with any active HighID transfer).
//! So HighID keeps strict precedence, but LowID has a time-bounded floor —
//! never blocked longer than the cap.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

/// The longest a LowID upload waits behind HighID before it is let through
/// regardless. Bounds LowID starvation while HighID still gets the bulk of the
/// bandwidth under sustained load.
const MAX_LOWID_WAIT: Duration = Duration::from_secs(10);

pub struct UploadScheduler {
    /// Count of HighID uploads currently in the throttle-acquire phase.
    highid_active: AtomicUsize,
    /// Notified when `highid_active` drops to zero so waiting LowID tasks
    /// can re-check and proceed.
    lowid_wake: Notify,
}

impl Default for UploadScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl UploadScheduler {
    pub fn new() -> Self {
        Self {
            highid_active: AtomicUsize::new(0),
            lowid_wake: Notify::new(),
        }
    }

    /// Mark a HighID upload as active for the lifetime of the returned guard.
    ///
    /// The count is decremented automatically when the guard is dropped — on
    /// normal completion, early return, or panic alike. This RAII shape is
    /// deliberate: a missing decrement leaks `highid_active` above zero, which
    /// would block `wait_for_lowid_turn` forever and deadlock every LowID
    /// download. A guard makes that failure mode unrepresentable.
    pub fn highid_guard(self: &Arc<Self>) -> HighIdGuard {
        self.highid_active.fetch_add(1, Ordering::Relaxed);
        HighIdGuard {
            scheduler: Arc::clone(self),
        }
    }

    /// Block until no HighID upload is competing for the throttle, or until
    /// [`MAX_LOWID_WAIT`] elapses — whichever comes first.
    ///
    /// Returns immediately if `highid_active == 0` (idle → LowID gets full
    /// throughput). Under sustained HighID load it returns after at most
    /// `MAX_LOWID_WAIT`, so LowID is never starved indefinitely; it then
    /// competes for the bucket alongside the active HighID transfers.
    pub async fn wait_for_lowid_turn(&self) {
        let deadline = tokio::time::Instant::now() + MAX_LOWID_WAIT;
        loop {
            // Subscribe BEFORE reading the counter so we never miss a
            // notification that fires between the load and the await.
            let notified = self.lowid_wake.notified();
            if self.highid_active.load(Ordering::Acquire) == 0 {
                return;
            }
            // Wait for HighID to drain, but no longer than the floor deadline.
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return;
            }
        }
    }
}

/// Drop guard that keeps a HighID upload counted as active. Decrements the
/// scheduler's `highid_active` on drop and wakes waiting LowID tasks when it
/// was the last one. Obtained from [`UploadScheduler::highid_guard`].
pub struct HighIdGuard {
    scheduler: Arc<UploadScheduler>,
}

impl Drop for HighIdGuard {
    fn drop(&mut self) {
        let prev = self.scheduler.highid_active.fetch_sub(1, Ordering::Release);
        if prev == 1 {
            // We were the last active HighID upload — let LowIDs proceed.
            self.scheduler.lowid_wake.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lowid_returns_immediately_when_idle() {
        let sched = Arc::new(UploadScheduler::new());
        // No HighID active → LowID proceeds at once (work-conserving).
        sched.wait_for_lowid_turn().await;
    }

    #[tokio::test]
    async fn lowid_proceeds_as_soon_as_highid_drains() {
        let sched = Arc::new(UploadScheduler::new());
        let guard = sched.highid_guard();
        let s2 = Arc::clone(&sched);
        let waiter = tokio::spawn(async move { s2.wait_for_lowid_turn().await });
        // Releasing the only HighID upload wakes the waiter well before the cap.
        drop(guard);
        waiter.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn lowid_admitted_by_floor_despite_active_highid() {
        let sched = Arc::new(UploadScheduler::new());
        // HighID stays active and never drops: strict precedence would block
        // forever, but the floor must let LowID through after MAX_LOWID_WAIT
        // (the paused clock auto-advances to the deadline).
        let _guard = sched.highid_guard();
        sched.wait_for_lowid_turn().await;
    }
}
