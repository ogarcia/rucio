//! Work-conserving upload priority scheduler.
//!
//! HighID peers (globally reachable) are served before LowID peers (behind
//! NAT).  LowID requests wait in `wait_for_lowid_turn()` until no HighID
//! upload is currently competing for the bandwidth throttle.  When the node
//! is idle the wait returns immediately, so LowID still gets full throughput.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

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

    /// Block until no HighID upload is competing for the throttle.
    ///
    /// Returns immediately if `highid_active == 0`.
    pub async fn wait_for_lowid_turn(&self) {
        loop {
            // Subscribe BEFORE reading the counter so we never miss a
            // notification that fires between the load and the await.
            let notified = self.lowid_wake.notified();
            if self.highid_active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
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
