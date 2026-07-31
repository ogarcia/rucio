//! Coordinates the explicit indexers (the download/eMule completion paths) with
//! the filesystem watcher so a freshly-finished file is BLAKE3-hashed exactly
//! once.
//!
//! A completing download moves its file into a watched directory, so inotify and
//! the completion path would otherwise both index it — two full reads of an
//! often multi-GB file, racing on `UNIQUE(shared_files.path)`. While a completion
//! path indexes a file it holds a [`Marking`] for that path; the watcher checks
//! [`IndexGuard::is_marked`] and skips it. Once the completion's row lands, the
//! watcher's own "already indexed?" check keeps skipping it, so the mark only has
//! to cover the hashing window.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Safety net: a mark older than this is swept, so a leaked [`Marking`] (e.g. a
/// panicking index task) can never make the watcher skip a path forever. Well
/// above the time to hash any realistic file.
const MARK_TTL: Duration = Duration::from_secs(15 * 60);

/// Shared set of paths a completion path is currently indexing. Cheap to clone
/// (an `Arc`); pass it to the watcher and to every explicit indexer.
#[derive(Clone, Default)]
pub struct IndexGuard(Arc<Mutex<HashMap<PathBuf, Instant>>>);

impl IndexGuard {
    /// Mark `path` as being indexed here. The returned guard clears the mark when
    /// dropped, so callers just hold it across their `index_file` call.
    pub fn marking(&self, path: &Path) -> Marking {
        self.0
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), Instant::now());
        Marking {
            guard: self.clone(),
            path: path.to_path_buf(),
        }
    }

    /// Whether `path` is currently being indexed by a completion path (so the
    /// watcher should skip it). Sweeps expired marks as a leak safety net.
    pub fn is_marked(&self, path: &Path) -> bool {
        let mut g = self.0.lock().unwrap();
        g.retain(|_, t| t.elapsed() < MARK_TTL);
        g.contains_key(path)
    }
}

/// RAII mark: clears its path from the [`IndexGuard`] on drop.
pub struct Marking {
    guard: IndexGuard,
    path: PathBuf,
}

impl Drop for Marking {
    fn drop(&mut self) {
        self.guard.0.lock().unwrap().remove(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_is_visible_then_cleared_on_drop() {
        let g = IndexGuard::default();
        let p = Path::new("/rucio/complete/file.mkv");
        assert!(!g.is_marked(p));
        {
            let _m = g.marking(p);
            assert!(g.is_marked(p));
        }
        // Dropped → cleared.
        assert!(!g.is_marked(p));
    }

    #[test]
    fn marks_are_per_path() {
        let g = IndexGuard::default();
        let a = Path::new("/a");
        let b = Path::new("/b");
        let _m = g.marking(a);
        assert!(g.is_marked(a));
        assert!(!g.is_marked(b));
    }
}
