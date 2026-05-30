//! A single-slot priority gate that serialises Kad searches while letting
//! user-initiated keyword searches jump ahead of queued source lookups.
//!
//! Only one Kad search runs at a time (the task owns a single UDP socket and
//! one `ActiveSearch`). When the running search finishes, a waiting
//! high-priority (user) search is always served before any low-priority
//! (download source) search. A search that is already running is never
//! preempted.
//!
//! Rationale: source lookups for in-progress downloads can queue several deep.
//! If a user types a query into the search box and it lands behind them, the
//! UI looks frozen and the user re-submits, piling on more work. Prioritising
//! user searches keeps the box responsive. Low-priority lookups may be delayed
//! while the user is actively searching, but they retry on their own, so this
//! is an acceptable trade-off (and the intended behaviour).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Search priority. `High` = user keyword searches typed into the search box;
/// `Low` = automatic source lookups for in-progress downloads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Priority {
    High,
    Low,
}

#[derive(Default)]
struct GateInner {
    /// A search currently holds the slot.
    busy: bool,
    /// Waiters, split so highs are always dequeued before lows.
    high: VecDeque<oneshot::Sender<()>>,
    low: VecDeque<oneshot::Sender<()>>,
}

/// Serialises searches with priority. Wrap in `Arc` to share between callers.
pub struct PriorityGate {
    inner: Mutex<GateInner>,
}

impl Default for PriorityGate {
    fn default() -> Self {
        Self::new()
    }
}

impl PriorityGate {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(GateInner::default()),
        }
    }

    /// Acquire the single search slot, waiting our turn. The returned permit
    /// must be held for the whole search; dropping it releases the slot and
    /// wakes the next waiter (highs first).
    pub async fn acquire(self: &Arc<Self>, prio: Priority) -> SearchPermit {
        loop {
            let rx = {
                let mut g = self.inner.lock().unwrap();
                // Take the slot only if free AND we wouldn't jump a waiting
                // higher-priority search: a Low yields to any waiting High.
                let blocked = prio == Priority::Low && !g.high.is_empty();
                if !g.busy && !blocked {
                    g.busy = true;
                    return SearchPermit {
                        gate: Arc::clone(self),
                    };
                }
                let (tx, rx) = oneshot::channel();
                match prio {
                    Priority::High => g.high.push_back(tx),
                    Priority::Low => g.low.push_back(tx),
                }
                rx
            };
            // Woken on release; loop back and retry acquiring the slot. If the
            // sender was dropped the await returns Err — we still retry, which
            // re-queues us cleanly.
            let _ = rx.await;
        }
    }

    /// Whether the single search slot is currently held. A `true` result means
    /// a caller acquiring now would have to wait its turn. Best-effort: the
    /// state can change the instant after this returns.
    pub fn is_busy(&self) -> bool {
        self.inner.lock().unwrap().busy
    }

    /// Release the slot and wake the next waiter (highs first). `busy` is only
    /// ever set true by an acquirer that returns a permit, so a cancelled
    /// waiter can never leave the gate stuck.
    fn release(&self) {
        let mut g = self.inner.lock().unwrap();
        g.busy = false;
        while let Some(tx) = g.high.pop_front().or_else(|| g.low.pop_front()) {
            // A dead receiver (waiter cancelled before acquiring) sends Err;
            // skip it and wake the next one instead.
            if tx.send(()).is_ok() {
                break;
            }
        }
    }
}

/// RAII permit for the single search slot. Releasing happens on drop.
pub struct SearchPermit {
    gate: Arc<PriorityGate>,
}

impl Drop for SearchPermit {
    fn drop(&mut self) {
        self.gate.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn free_slot_is_acquired_immediately() {
        let gate = Arc::new(PriorityGate::new());
        let permit = gate.acquire(Priority::Low).await;
        drop(permit);
        // Re-acquire works after release.
        let _permit = gate.acquire(Priority::High).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn high_priority_jumps_a_queued_low() {
        let gate = Arc::new(PriorityGate::new());
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        // Occupy the slot so the next two callers must queue.
        let held = gate.acquire(Priority::Low).await;

        // Enqueue a low FIRST, then a high. Despite the low arriving first,
        // the high must be served first on release.
        let (g_low, o_low) = (Arc::clone(&gate), Arc::clone(&order));
        let low = tokio::spawn(async move {
            let _p = g_low.acquire(Priority::Low).await;
            o_low.lock().unwrap().push("low");
        });
        let (g_high, o_high) = (Arc::clone(&gate), Arc::clone(&order));
        let high = tokio::spawn(async move {
            let _p = g_high.acquire(Priority::High).await;
            o_high.lock().unwrap().push("high");
        });

        // Let both tasks reach their await and enqueue.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        // Release the slot; the high waiter should win.
        drop(held);
        high.await.unwrap();
        low.await.unwrap();

        assert_eq!(*order.lock().unwrap(), vec!["high", "low"]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_new_low_yields_to_a_waiting_high() {
        let gate = Arc::new(PriorityGate::new());
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        let held = gate.acquire(Priority::Low).await;

        // A high queues up while the slot is held.
        let (g_high, o_high) = (Arc::clone(&gate), Arc::clone(&order));
        let high = tokio::spawn(async move {
            let _p = g_high.acquire(Priority::High).await;
            o_high.lock().unwrap().push("high");
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        // Release: now the slot is free but a high is waiting. A low arriving
        // at this instant must not grab the free slot ahead of the high.
        drop(held);
        let (g_low, o_low) = (Arc::clone(&gate), Arc::clone(&order));
        let low = tokio::spawn(async move {
            let _p = g_low.acquire(Priority::Low).await;
            o_low.lock().unwrap().push("low");
        });

        high.await.unwrap();
        low.await.unwrap();

        assert_eq!(*order.lock().unwrap(), vec!["high", "low"]);
    }
}
