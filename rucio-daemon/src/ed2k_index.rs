//! Queue handle for eMule ed2k hashing with a live "pending" gauge.
//!
//! Files indexed for the Rucio network are forwarded here to also be hashed
//! (MD4) and published for eMule. That hashing runs in its own background task
//! ([`crate::emule::spawn_ed2k_indexer`]), separate from the Rucio indexing
//! counter, so this type carries a dedicated counter the UI shows on its own.
//!
//! The type is deliberately not behind the `emule-compat` feature: the share
//! watcher threads it through as an `Option` regardless of the feature, and it
//! is simply `None` when eMule is disabled.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc;

/// Sender side of the ed2k hashing queue plus its shared pending counter.
/// Cheap to clone (an `mpsc::Sender` and an `Arc`).
#[derive(Clone)]
pub struct Ed2kIndex {
    tx: mpsc::Sender<PathBuf>,
    pending: Arc<AtomicUsize>,
}

impl Ed2kIndex {
    pub fn new(tx: mpsc::Sender<PathBuf>) -> Self {
        Self {
            tx,
            pending: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Forward a path to the ed2k indexer without blocking, counting it as
    /// pending on success. A full channel drops the path (best-effort seeding:
    /// the reconcile sweep re-queues anything still missing its ed2k hash), and
    /// then nothing is counted, keeping the gauge balanced with the consumer.
    pub fn enqueue(&self, path: PathBuf) {
        if self.tx.try_send(path).is_ok() {
            self.pending.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Shared pending counter, for the consumer to decrement as it finishes
    /// each file.
    pub fn pending(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.pending)
    }

    /// Current number of files queued (or in flight) for ed2k hashing.
    pub fn pending_count(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }
}
