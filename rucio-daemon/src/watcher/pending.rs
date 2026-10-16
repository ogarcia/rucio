//! The watcher's per-path table of pending work, and its hand-off to the
//! indexing worker.
//!
//! The service loop reduces every filesystem event to one of three intents —
//! upsert, remove, rename — and records it here. Each intent's method owns the
//! rules for combining it with whatever is already pending for the same path,
//! so the loop never has to reason about how operations interact. `flush` then
//! hands due work to the worker **without ever waiting on it**: when the
//! worker's queue is full the rest stay pending for the next tick, so a slow or
//! stuck worker delays indexing but never stops the loop from draining events.
//!
//! Design adopted from #25 (@p-banuelos).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::index_guard::IndexGuard;

/// Depth of the queue feeding the indexing worker. When it is full, due
/// operations simply stay in the pending table — they are never dropped.
pub(super) const WORK_QUEUE: usize = 256;

/// What the indexing worker must do for a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Op {
    /// (Re)index the file and announce it.
    Upsert,
    /// Drop the path (and anything under it) from the index.
    Remove,
    /// The file was renamed here from this old path: repoint its row when the
    /// content is unchanged, else drop the old path and index this one.
    RenamedFrom(PathBuf),
}

/// One operation handed to the indexing worker.
pub(super) struct Work {
    pub(super) path: PathBuf,
    pub(super) op: Op,
    /// Keeps an upsert counted on the indexing-status endpoint until the worker
    /// is done with it; dropped with the item if it never reaches the worker.
    pub(super) ticket: Option<IndexingTicket>,
}

/// One unit of watcher-driven indexing, counted on the indexing-status endpoint
/// (and WS) for as long as the ticket lives — queued or running. Decrements on
/// drop, so the count is self-balancing however an arm ends.
pub(super) struct IndexingTicket(Arc<AtomicUsize>);

impl IndexingTicket {
    pub(super) fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(count))
    }
}

impl Drop for IndexingTicket {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Pending {
    op: Op,
    due: Instant,
}

/// Per-path table of the next operation to hand to the indexing worker.
pub(super) struct PendingOps {
    ops: HashMap<PathBuf, Pending>,
    debounce: Duration,
}

impl PendingOps {
    /// `debounce`: how long upserts and renames wait for further events on the
    /// same path before they become due.
    pub(super) fn new(debounce: Duration) -> Self {
        Self {
            ops: HashMap::new(),
            debounce,
        }
    }

    /// The file was created or written. Restarts the debounce window, but never
    /// overrides a pending rename: notify delivers the standalone `To` half
    /// after the paired `Both`, and the rename re-indexes on its own when the
    /// content changed.
    pub(super) fn upsert(&mut self, path: PathBuf, now: Instant) {
        if !matches!(self.op(&path), Some(Op::RenamedFrom(_))) {
            self.set(path, Op::Upsert, now + self.debounce);
        }
    }

    /// The file is gone; due on the next tick (nothing to read, no point
    /// waiting). If it was renamed here and the rename is still pending
    /// (`mv a b; rm b`), the indexed row still sits under the source, so that
    /// goes too.
    pub(super) fn remove(&mut self, path: PathBuf, now: Instant) {
        if let Some(Op::RenamedFrom(src)) = self.op(&path).cloned() {
            self.set(src, Op::Remove, now);
        }
        self.set(path, Op::Remove, now);
    }

    /// `old` was renamed to `new`. `shareable` is whether `new` should be
    /// indexed at all (hidden/excluded/not a regular file → e.g. moved to a temp
    /// dir): if not, the old share is just dropped. A rename of a path whose own
    /// rename is still pending (`mv a b; mv b c`) keeps the original source,
    /// where the indexed row still sits.
    ///
    /// Renames wait out the debounce window like upserts: the worker's fallback
    /// may index `new`, which could still be being written, or which a
    /// completion path is about to mark in the `IndexGuard`.
    pub(super) fn rename(&mut self, old: &Path, new: PathBuf, shareable: bool, now: Instant) {
        let src = match self.ops.remove(old).map(|p| p.op) {
            Some(Op::RenamedFrom(src)) => src,
            _ => old.to_path_buf(),
        };
        if shareable {
            self.set(new, Op::RenamedFrom(src), now + self.debounce);
        } else {
            self.set(src, Op::Remove, now);
        }
    }

    /// The operation pending for `path`, if any.
    pub(super) fn op(&self, path: &Path) -> Option<&Op> {
        self.ops.get(path).map(|p| &p.op)
    }

    /// Hand every due operation to the worker without waiting on it. When the
    /// worker's queue is full the rest stay pending for the next tick, so a slow
    /// worker never stops the loop from reading events. Errors only when the
    /// worker is gone (its receiver dropped) — a lost task the service can't
    /// recover from.
    pub(super) fn flush(
        &mut self,
        work_tx: &mpsc::Sender<Work>,
        indexing_count: &Arc<AtomicUsize>,
        index_guard: &IndexGuard,
        now: Instant,
    ) -> Result<()> {
        let ready: Vec<PathBuf> = self
            .ops
            .iter()
            .filter(|(_, p)| p.due <= now)
            // Skip a file a completion path is already indexing (e.g. a just-
            // finished eMule download): it hashes the file itself, so indexing it
            // here too would be a redundant second full read and would race on
            // UNIQUE(path). Renames count too — one renamed out of a temp dir
            // nested in a share falls back to indexing the new path. A removal is
            // never guarded: the file is gone, there is nothing to re-read.
            .filter(|(path, p)| p.op == Op::Remove || !index_guard.is_marked(path))
            .map(|(path, _)| path.clone())
            .collect();

        for path in ready {
            let Some(Pending { op, due }) = self.ops.remove(&path) else {
                continue;
            };
            // Only existing files get indexed (the file may have vanished between
            // the event and now).
            if op == Op::Upsert && !path.is_file() {
                continue;
            }
            // We deliberately do NOT latch `indexing_seen` here: incremental
            // watcher-driven indexing shouldn't fire the "indexing complete"
            // notification, which is reserved for `share add` and the
            // startup/periodic rescan. The live count still reflects the work.
            let ticket = (op == Op::Upsert).then(|| IndexingTicket::new(indexing_count));
            match work_tx.try_send(Work { path, op, ticket }) {
                Ok(()) => {}
                // Queue full: put it back (dropping the returned ticket uncounts
                // it) and stop — the rest wait for the next tick.
                Err(mpsc::error::TrySendError::Full(Work { path, op, .. })) => {
                    self.set(path, op, due);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    anyhow::bail!("indexing worker stopped");
                }
            }
        }
        Ok(())
    }

    fn set(&mut self, path: PathBuf, op: Op, due: Instant) {
        self.ops.insert(path, Pending { op, due });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEBOUNCE: Duration = Duration::from_millis(500);

    /// Create `names` as real files under a fresh temp dir (so `is_file` holds
    /// in `flush`) and return their paths.
    fn files(names: &[&str]) -> (tempfile::TempDir, Vec<PathBuf>) {
        let dir = tempfile::tempdir().unwrap();
        let paths = names
            .iter()
            .map(|n| {
                let p = dir.path().join(n);
                std::fs::write(&p, b"x").unwrap();
                p
            })
            .collect();
        (dir, paths)
    }

    fn received(rx: &mut mpsc::Receiver<Work>) -> Option<(PathBuf, Op)> {
        rx.try_recv().ok().map(|w| (w.path, w.op))
    }

    #[test]
    fn latest_op_wins() {
        let now = Instant::now();
        let mut ops = PendingOps::new(DEBOUNCE);
        let p = PathBuf::from("/data/a.mkv");

        ops.upsert(p.clone(), now);
        ops.remove(p.clone(), now);
        assert_eq!(
            ops.op(&p),
            Some(&Op::Remove),
            "a removal replaces a pending upsert"
        );

        ops.upsert(p.clone(), now);
        assert_eq!(
            ops.op(&p),
            Some(&Op::Upsert),
            "a re-created file is indexed again"
        );
    }

    #[test]
    fn upsert_never_overrides_a_pending_rename() {
        let now = Instant::now();
        let mut ops = PendingOps::new(DEBOUNCE);
        let (old, new) = (Path::new("/data/old.mkv"), PathBuf::from("/data/new.mkv"));

        ops.rename(old, new.clone(), true, now);
        ops.upsert(new.clone(), now);
        assert_eq!(ops.op(&new), Some(&Op::RenamedFrom(old.to_path_buf())));
        assert_eq!(ops.ops.len(), 1);
    }

    #[test]
    fn rename_to_an_unshareable_path_removes_the_source() {
        let now = Instant::now();
        let mut ops = PendingOps::new(DEBOUNCE);
        let old = Path::new("/data/film.mkv");

        ops.rename(old, PathBuf::from("/data/.hidden.mkv"), false, now);
        assert_eq!(ops.op(old), Some(&Op::Remove));
        assert_eq!(ops.ops.len(), 1);
    }

    #[test]
    fn chained_renames_keep_the_original_source() {
        let now = Instant::now();
        let mut ops = PendingOps::new(DEBOUNCE);
        let (a, b, c) = (
            PathBuf::from("/data/a.mkv"),
            PathBuf::from("/data/b.mkv"),
            PathBuf::from("/data/c.mkv"),
        );

        // mv a b; mv b c — both before the next tick.
        ops.rename(&a, b.clone(), true, now);
        ops.rename(&b, c.clone(), true, now);
        assert_eq!(ops.op(&c), Some(&Op::RenamedFrom(a)));
        assert_eq!(ops.ops.len(), 1);
    }

    #[test]
    fn removing_a_renamed_file_also_drops_its_source() {
        let now = Instant::now();
        let mut ops = PendingOps::new(DEBOUNCE);
        let (a, b) = (PathBuf::from("/data/a.mkv"), PathBuf::from("/data/b.mkv"));

        // mv a b; rm b — the indexed row is still under `a`.
        ops.rename(&a, b.clone(), true, now);
        ops.remove(b.clone(), now);
        assert_eq!(ops.op(&a), Some(&Op::Remove));
        assert_eq!(ops.op(&b), Some(&Op::Remove));
    }

    #[tokio::test]
    async fn flush_waits_for_the_debounce_window() {
        let (_d, paths) = files(&["a.mkv"]);
        let now = Instant::now();
        let mut ops = PendingOps::new(DEBOUNCE);
        ops.upsert(paths[0].clone(), now);
        let (tx, mut rx) = mpsc::channel(8);
        let count = Arc::new(AtomicUsize::new(0));
        let guard = IndexGuard::default();

        ops.flush(&tx, &count, &guard, now + Duration::from_millis(100))
            .unwrap();
        assert!(
            received(&mut rx).is_none(),
            "not due inside the debounce window"
        );

        ops.flush(&tx, &count, &guard, now + DEBOUNCE).unwrap();
        assert_eq!(
            count.load(Ordering::Relaxed),
            1,
            "a queued upsert is reported as indexing"
        );
        assert_eq!(received(&mut rx), Some((paths[0].clone(), Op::Upsert)));
        assert_eq!(
            count.load(Ordering::Relaxed),
            0,
            "and uncounted once the worker drops the ticket"
        );
        assert!(ops.ops.is_empty());
    }

    #[tokio::test]
    async fn flush_never_waits_on_a_full_worker_queue() {
        let (_d, paths) = files(&["a.mkv", "b.mkv", "c.mkv"]);
        let now = Instant::now();
        let mut ops = PendingOps::new(DEBOUNCE);
        for p in &paths {
            ops.upsert(p.clone(), now);
        }
        // A worker that never drains, with room for exactly one item.
        let (tx, _rx) = mpsc::channel(1);
        let count = Arc::new(AtomicUsize::new(0));
        let guard = IndexGuard::default();

        ops.flush(&tx, &count, &guard, now + DEBOUNCE).unwrap();
        assert_eq!(
            ops.ops.len(),
            2,
            "what didn't fit stays pending for the next tick"
        );
        assert_eq!(
            count.load(Ordering::Relaxed),
            1,
            "only the handed-over upsert is counted"
        );

        ops.flush(&tx, &count, &guard, now + DEBOUNCE).unwrap();
        assert_eq!(
            ops.ops.len(),
            2,
            "retrying against a still-full queue returns at once"
        );
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn flush_fails_when_the_worker_is_gone() {
        let now = Instant::now();
        let mut ops = PendingOps::new(DEBOUNCE);
        ops.remove(PathBuf::from("/data/gone.mkv"), now);
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let count = Arc::new(AtomicUsize::new(0));
        assert!(ops.flush(&tx, &count, &IndexGuard::default(), now).is_err());
    }

    #[tokio::test]
    async fn flush_leaves_marked_files_to_their_completion_path() {
        let (_d, paths) = files(&["done.mkv"]);
        let now = Instant::now();
        let mut ops = PendingOps::new(DEBOUNCE);
        ops.upsert(paths[0].clone(), now);
        let (tx, mut rx) = mpsc::channel(8);
        let count = Arc::new(AtomicUsize::new(0));
        let guard = IndexGuard::default();

        let marking = guard.marking(&paths[0]);
        ops.flush(&tx, &count, &guard, now + DEBOUNCE).unwrap();
        assert!(
            received(&mut rx).is_none(),
            "a marked file is left to its completion path"
        );

        drop(marking);
        ops.flush(&tx, &count, &guard, now + DEBOUNCE).unwrap();
        assert_eq!(received(&mut rx).map(|(p, _)| p), Some(paths[0].clone()));
    }
}
