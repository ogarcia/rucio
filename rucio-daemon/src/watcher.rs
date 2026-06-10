//! Filesystem watcher service.
//!
//! Wraps the `notify` crate to watch registered shared directories for
//! changes and keep the index in sync:
//!
//! - `Create` / `Modify`  → index (or re-index) the file, announce to DHT
//! - `Remove`             → remove from DB, stop providing in DHT
//! - `Rename` (both)      → repoint the row in place (no re-hash, no DHT churn)
//!   when the content is unchanged; fall back to remove + re-index otherwise
//!
//! Indexing is idempotent: a path already recorded with the same size + mtime is
//! skipped, so redundant `Modify` events (and the `To` half of a rename that the
//! paired `Both` already repointed) never trigger a re-hash.
//!
//! The watcher runs `notify`'s blocking watcher on a dedicated OS thread and
//! bridges events into async-land via a channel.
//!
//! # Debounce
//!
//! Many editors and copy tools emit multiple events (Create + Modify, or
//! several Modify in quick succession) for a single logical write.  To avoid
//! re-indexing the same file repeatedly we debounce upsert events: when a
//! Create/Modify arrives for a path we record the time and only index it after
//! DEBOUNCE_MS have passed with no further events for that path.  Remove
//! events are processed immediately since there is nothing to read.
//!
//! # Watcher lifecycle
//!
//! 1. `WatcherService::spawn()` starts the service task and returns a
//!    `WatcherHandle` with a command sender.
//! 2. The daemon sends `WatchDir` / `UnwatchDir` commands as directories are
//!    added or removed.
//! 3. On shutdown the command channel is dropped; the service task exits.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio::time::{Instant, interval};
use tracing::{debug, info, warn};

use crate::api::shares::{file_mtime_secs, index_file};
use crate::db::{self, Db};
use crate::node::messages::NodeCmd;
use rucio_core::protocol::hashing::collect_files;

/// How long to wait after the last event for a path before indexing it.
const DEBOUNCE_MS: u64 = 500;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Commands the daemon sends to the watcher service.
pub enum WatcherCmd {
    Watch(PathBuf),
    Unwatch(PathBuf),
}

/// Handle to the running watcher service.
pub struct WatcherHandle {
    pub cmd_tx: mpsc::Sender<WatcherCmd>,
}

impl WatcherHandle {
    pub async fn watch(&self, path: PathBuf) {
        let _ = self.cmd_tx.send(WatcherCmd::Watch(path)).await;
    }

    pub async fn unwatch(&self, path: PathBuf) {
        let _ = self.cmd_tx.send(WatcherCmd::Unwatch(path)).await;
    }
}

/// Spawn the watcher service task.
///
/// # Parameters
/// - `db` — shared DB pool
/// - `node_tx` — channel to send `NodeCmd` to the libp2p node task
/// - `excluded` — directory prefixes whose files must never be indexed (the
///   temp dirs); guards against a `temp_dir` nested inside the `download_dir`.
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    db: Db,
    node_tx: mpsc::Sender<NodeCmd>,
    indexing_count: Arc<AtomicUsize>,
    excluded: Arc<Vec<PathBuf>>,
    ed2k_tx: Option<mpsc::Sender<PathBuf>>,
) -> (WatcherHandle, tokio::task::JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::channel::<WatcherCmd>(64);

    let task = tokio::spawn(async move {
        if let Err(e) = run(db, node_tx, cmd_rx, indexing_count, excluded, ed2k_tx).await {
            warn!("WatcherService exited with error: {e}");
        }
    });

    (WatcherHandle { cmd_tx }, task)
}

// ---------------------------------------------------------------------------
// Internal service loop
// ---------------------------------------------------------------------------

async fn run(
    db: Db,
    node_tx: mpsc::Sender<NodeCmd>,
    mut cmd_rx: mpsc::Receiver<WatcherCmd>,
    indexing_count: Arc<AtomicUsize>,
    excluded: Arc<Vec<PathBuf>>,
    ed2k_tx: Option<mpsc::Sender<PathBuf>>,
) -> Result<()> {
    // Bridge: notify (sync) → tokio (async)
    let (ev_tx, mut ev_rx) = mpsc::channel::<notify::Result<Event>>(256);

    // Shared set of currently-watched paths (used for unwatch)
    let watched: Arc<Mutex<HashSet<PathBuf>>> = Arc::new(Mutex::new(HashSet::new()));

    // Build the notify watcher on a blocking thread so it can call
    // the OS-level inotify/FSEvents API from a sync context.
    let ev_tx_clone = ev_tx.clone();
    let watcher: Arc<Mutex<RecommendedWatcher>> = {
        let w = notify::recommended_watcher(move |res| {
            let _ = ev_tx_clone.blocking_send(res);
        })?;
        Arc::new(Mutex::new(w))
    };

    // Debounce table: path → time of last upsert event.
    // We flush entries whose timestamp is older than DEBOUNCE_MS.
    let mut pending_upserts: HashMap<PathBuf, Instant> = HashMap::new();
    let debounce_dur = Duration::from_millis(DEBOUNCE_MS);
    let mut debounce_tick = interval(Duration::from_millis(DEBOUNCE_MS / 2));
    debounce_tick.tick().await; // consume immediate first tick

    info!("WatcherService started");

    loop {
        tokio::select! {
            // --- Commands from the daemon ------------------------------------
            cmd = cmd_rx.recv() => {
                match cmd {
                    None => {
                        info!("WatcherService: command channel closed, stopping");
                        break;
                    }
                    Some(WatcherCmd::Watch(path)) => {
                        watch_dir(&watcher, &watched, &path);
                    }
                    Some(WatcherCmd::Unwatch(path)) => {
                        unwatch_dir(&watcher, &watched, &path);
                    }
                }
            }

            // --- Filesystem events -------------------------------------------
            ev = ev_rx.recv() => {
                match ev {
                    None => break,
                    Some(Err(e)) => warn!("Watcher error: {e}"),
                    Some(Ok(event)) => {
                        handle_event(
                            event,
                            &db,
                            &node_tx,
                            &mut pending_upserts,
                            &excluded,
                        ).await;
                    }
                }
            }

            // --- Debounce flush tick -----------------------------------------
            _ = debounce_tick.tick() => {
                flush_pending(
                    &mut pending_upserts,
                    debounce_dur,
                    &db,
                    &node_tx,
                    &indexing_count,
                    ed2k_tx.as_ref(),
                ).await;
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Watch / Unwatch helpers
// ---------------------------------------------------------------------------

fn watch_dir(
    watcher: &Arc<Mutex<RecommendedWatcher>>,
    watched: &Arc<Mutex<HashSet<PathBuf>>>,
    path: &Path,
) {
    let mut set = watched.lock().unwrap();
    // Already covered: an equal or ancestor watch is recursive, so it already
    // sees everything under `path`. Watching `path` too would double-deliver
    // every event below it (and double-index on rescan). Skip.
    if set.iter().any(|w| path == w || path.starts_with(w)) {
        debug!(path = %path.display(), "Already covered by an existing watch, skipping");
        return;
    }
    // `path` is an ancestor of one or more existing watches: a recursive watch
    // here covers them, so they're now redundant — drop them first.
    let nested: Vec<PathBuf> = set
        .iter()
        .filter(|w| w.as_path() != path && w.starts_with(path))
        .cloned()
        .collect();
    for w in &nested {
        if watcher.lock().unwrap().unwatch(w).is_ok() {
            set.remove(w);
            info!(path = %w.display(), parent = %path.display(), "Dropping nested watch covered by parent");
        }
    }
    match watcher
        .lock()
        .unwrap()
        .watch(path, RecursiveMode::Recursive)
    {
        Ok(()) => {
            set.insert(path.to_path_buf());
            info!(path = %path.display(), "Watching directory");
        }
        Err(e) => warn!(path = %path.display(), "Failed to watch: {e}"),
    }
}

fn unwatch_dir(
    watcher: &Arc<Mutex<RecommendedWatcher>>,
    watched: &Arc<Mutex<HashSet<PathBuf>>>,
    path: &Path,
) {
    let mut set = watched.lock().unwrap();
    if !set.contains(path) {
        return;
    }
    if let Err(e) = watcher.lock().unwrap().unwatch(path) {
        warn!(path = %path.display(), "Failed to unwatch: {e}");
    } else {
        set.remove(path);
        info!(path = %path.display(), "Unwatched directory");
    }
}

// ---------------------------------------------------------------------------
// Event handler
// ---------------------------------------------------------------------------

async fn handle_event(
    event: Event,
    db: &Db,
    node_tx: &mpsc::Sender<NodeCmd>,
    pending_upserts: &mut HashMap<PathBuf, Instant>,
    excluded: &[PathBuf],
) {
    match event.kind {
        // File created — queue for debounced indexing
        EventKind::Create(_) => {
            for path in &event.paths {
                if !path.is_file() || is_hidden(path) || is_excluded(path, excluded) {
                    continue;
                }
                debug!(path = %path.display(), "Watcher: Create — queuing upsert");
                pending_upserts.insert(path.clone(), Instant::now());
            }
        }

        // Rename (Both): repoint in place when the content is unchanged (no
        // re-hash), else fall back to remove-old + index-new.
        EventKind::Modify(notify::event::ModifyKind::Name(notify::event::RenameMode::Both))
            if event.paths.len() == 2 =>
        {
            let (old, new) = (&event.paths[0], &event.paths[1]);
            // New path not shareable (hidden/excluded/not a file → e.g. moved to
            // a temp dir or to a non-regular target): just drop the old share.
            if !new.is_file() || is_hidden(new) || is_excluded(new, excluded) {
                on_file_remove(old, db, node_tx).await;
                return;
            }
            on_file_rename(old, new, db, node_tx, &mut *pending_upserts).await;
        }

        // The "from" half of a rename: on Linux notify also emits the paired
        // `Both` (handled above) which does the repoint, and the standalone
        // `To` (caught below) which is a no-op once repointed. Ignore the bare
        // `From` so a moved-away file isn't queued as an upsert of a vanished
        // path; if it was a move *out* of the tree with no `Both`, the rescan
        // de-indexes it. (Without this it would fall into the catch-all below.)
        EventKind::Modify(notify::event::ModifyKind::Name(notify::event::RenameMode::From)) => {}

        // Other modifications (data written, or the `To`/`Any` rename half) —
        // queue for debounced indexing. on_file_upsert is idempotent, so the
        // `To` of an already-repointed rename costs nothing.
        EventKind::Modify(_) => {
            for path in &event.paths {
                if !path.is_file() || is_hidden(path) || is_excluded(path, excluded) {
                    continue;
                }
                debug!(path = %path.display(), "Watcher: Modify — queuing upsert");
                // Update timestamp; this resets the debounce window.
                pending_upserts.insert(path.clone(), Instant::now());
            }
        }

        // File removed — deindex immediately (no file to read, no point waiting)
        EventKind::Remove(_) => {
            for path in &event.paths {
                // Cancel any pending upsert for this path.
                pending_upserts.remove(path);
                on_file_remove(path, db, node_tx).await;
            }
        }

        _ => {}
    }
}

/// Process all pending upserts whose debounce window has expired.
async fn flush_pending(
    pending: &mut HashMap<PathBuf, Instant>,
    debounce_dur: Duration,
    db: &Db,
    node_tx: &mpsc::Sender<NodeCmd>,
    indexing_count: &AtomicUsize,
    ed2k_tx: Option<&mpsc::Sender<PathBuf>>,
) {
    let now = Instant::now();
    let ready: Vec<PathBuf> = pending
        .iter()
        .filter(|(_, ts)| now.duration_since(**ts) >= debounce_dur)
        .map(|(p, _)| p.clone())
        .collect();

    for path in &ready {
        pending.remove(path);
    }
    // Only existing files get indexed; surface that count to the indexing
    // status endpoint (and WS), decrementing as each one completes — so the
    // CLI/web report watcher-driven indexing, not just manual `share add`.
    //
    // We deliberately do NOT latch `indexing_seen` here: incremental
    // watcher-driven indexing (a completed download landing in download_dir, a
    // file dropped into a shared folder) shouldn't fire the "indexing complete"
    // notification. That notification is reserved for indexing the user
    // actually asked for — `share add` and the startup/periodic rescan — so a
    // finished download doesn't surprise them with a second notification right
    // after "download complete". The live count above still reflects the work.
    let to_index: Vec<&PathBuf> = ready.iter().filter(|p| p.is_file()).collect();
    indexing_count.fetch_add(to_index.len(), Ordering::Relaxed);
    for path in to_index {
        on_file_upsert(path, db, node_tx, ed2k_tx).await;
        indexing_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Index (or re-index) a file and announce it in the DHT.
///
/// `ed2k_tx`, when present, receives the path of every successfully-indexed file
/// so the eMule layer can hash it and seed it as a Kad source too. It is a
/// non-blocking `try_send`: seeding eMule must never throttle the Rucio share
/// pipeline, and a dropped path is recovered by the next startup backfill.
async fn on_file_upsert(
    path: &Path,
    db: &Db,
    node_tx: &mpsc::Sender<NodeCmd>,
    ed2k_tx: Option<&mpsc::Sender<PathBuf>>,
) {
    let path_str = path.to_string_lossy().into_owned();

    // Idempotent: if this exact path is already indexed with the same size +
    // mtime, the content hasn't changed — skip the re-hash. This makes a
    // redundant Modify event (and the `To` half of an already-repointed rename)
    // a no-op, and is the same change signal the rescan uses.
    if let Ok(Some(row)) = db::shares::get_by_path(db, &path_str).await {
        let disk_size = std::fs::metadata(path)
            .map(|m| m.len() as i64)
            .unwrap_or(-1);
        if disk_size == row.size && file_mtime_secs(path) == row.mtime {
            debug!(path = %path.display(), "Watcher: unchanged, skipping re-index");
            return;
        }
        // Content changed: drop the stale row (exact match → uses the path
        // index, not the O(files²) prefix scan) before re-hashing.
        if let Ok(Some(hash)) = db::shares::delete_by_path(db, &path_str).await {
            let _ = node_tx.send(NodeCmd::StopProviding(hash)).await;
        }
    }

    match index_file(db, path).await {
        Ok(root_hash) => {
            debug!(path = %path.display(), "Watcher: indexed");
            let _ = node_tx
                .send(NodeCmd::StartProviding(root_hash.to_vec()))
                .await;
            if let Some(tx) = ed2k_tx {
                let _ = tx.try_send(path.to_path_buf());
            }
        }
        Err(e) => warn!(path = %path.display(), "Watcher: failed to index: {e}"),
    }
}

/// Handle a rename of a shared file (`old` → `new`, both inside watched dirs).
///
/// A pure rename leaves the content — and therefore the root hash and chunks —
/// untouched, so when the new path's size + mtime still match what we recorded
/// for the old path we just repoint the DB row: no re-hash, and no DHT churn
/// (we keep providing the same hash). Only if they differ (a write happened
/// around the rename) do we fall back to remove-old + index-new.
async fn on_file_rename(
    old: &Path,
    new: &Path,
    db: &Db,
    node_tx: &mpsc::Sender<NodeCmd>,
    pending: &mut HashMap<PathBuf, Instant>,
) {
    // A pending debounced upsert for the old path is now stale.
    pending.remove(old);
    let old_str = old.to_string_lossy().into_owned();
    let new_str = new.to_string_lossy().into_owned();
    let new_name = new
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| new_str.clone());

    match db::shares::get_by_path(db, &old_str).await {
        Ok(Some(row)) => {
            let disk_size = std::fs::metadata(new).map(|m| m.len() as i64).unwrap_or(-1);
            let unchanged = disk_size == row.size && file_mtime_secs(new) == row.mtime;
            if unchanged
                && db::shares::rename_path(db, &old_str, &new_str, &new_name)
                    .await
                    .unwrap_or(false)
            {
                // Cancel the upsert the standalone `To` event queued for the new
                // path — the repoint already covered it.
                pending.remove(new);
                debug!(from = %old.display(), to = %new.display(), "Watcher: renamed (repointed, no re-hash)");
                return;
            }
            // Content changed alongside the rename (or the repoint raced) →
            // re-index the new path and drop the old row.
            on_file_remove(old, db, node_tx).await;
            pending.insert(new.to_path_buf(), Instant::now());
        }
        // Old path wasn't a tracked share (hidden/excluded before, or a DB
        // miss): treat the new path as a fresh file.
        _ => {
            pending.insert(new.to_path_buf(), Instant::now());
        }
    }
}

/// Remove a file from the index and stop providing it.
async fn on_file_remove(path: &Path, db: &Db, node_tx: &mpsc::Sender<NodeCmd>) {
    let path_str = path.to_string_lossy().into_owned();
    match db::shares::delete_by_path_prefix(db, &path_str).await {
        Ok(hashes) if !hashes.is_empty() => {
            debug!(path = %path.display(), count = hashes.len(), "Watcher: deindexed");
            for hash in hashes {
                let _ = node_tx.send(NodeCmd::StopProviding(hash)).await;
            }
        }
        Ok(_) => {}
        Err(e) => warn!(path = %path.display(), "Watcher: DB error on remove: {e}"),
    }
}

/// Reconcile every shared directory against the index, catching changes made
/// while the daemon (and thus inotify) was not running.
///
/// For each watched directory the on-disk file set is compared with the index:
/// files missing from the index are hashed and announced, files gone from disk
/// are de-indexed, and files whose `size` or `mtime` differ are re-hashed.
/// Unchanged files are skipped, so on a stable library this is just a directory
/// walk + `stat` (no hashing). Run at startup and then on a long interval.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_shares(
    db: &Db,
    node_tx: &mpsc::Sender<NodeCmd>,
    indexing_count: &AtomicUsize,
    excluded: &[PathBuf],
    ed2k_tx: Option<&mpsc::Sender<PathBuf>>,
    indexing_seen: &AtomicBool,
) {
    let rows = match db::shared_dirs::list(db).await {
        Ok(d) => d,
        Err(e) => {
            warn!("Share rescan: cannot list shared dirs: {e}");
            return;
        }
    };
    // Scan only top-level dirs: a nested share (e.g. a category dir under the
    // global download_dir) is already covered by its ancestor's recursive walk,
    // so scanning it again would double-count and re-stat every file under it.
    let dirs = top_level_dirs(
        &rows
            .iter()
            .map(|d| PathBuf::from(&d.path))
            .collect::<Vec<_>>(),
    );
    // Index snapshot: path -> (size, mtime).
    let db_by_path: HashMap<String, (i64, i64)> = db::shares::list(db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|f| (f.path, (f.size, f.mtime)))
        .collect();

    let (mut added, mut changed, mut removed) = (0u64, 0u64, 0u64);
    // Files that need (re)hashing, collected first so we can surface an accurate
    // pending count and decrement it as each completes — the same progress the
    // indexing-status endpoint shows for `share add` and live watcher events.
    // Without this, a restart's reconcile re-indexes silently (pending stays 0).
    let mut to_upsert: Vec<PathBuf> = Vec::new();

    for dir in &dirs {
        // Walk + stat off the async worker threads.
        let mut disk: HashMap<String, (i64, i64)> = {
            let dir = dir.clone();
            tokio::task::spawn_blocking(move || {
                let mut m = HashMap::new();
                if let Ok(files) = collect_files(&dir) {
                    for p in files {
                        let size = std::fs::metadata(&p)
                            .map(|md| md.len() as i64)
                            .unwrap_or(-1);
                        m.insert(
                            p.to_string_lossy().into_owned(),
                            (size, file_mtime_secs(&p)),
                        );
                    }
                }
                m
            })
            .await
            .unwrap_or_default()
        };

        // Drop excluded files (.part / under a temp dir). Removing them from the
        // disk set means they're never indexed, and any that slipped into the
        // index before are de-indexed below (in DB, absent from disk → removed).
        disk.retain(|p, _| !is_excluded(Path::new(p), excluded));

        // New or changed files → (re)index (queued, indexed below).
        for (path, &(disk_size, disk_mtime)) in &disk {
            match db_by_path.get(path) {
                None => {
                    to_upsert.push(PathBuf::from(path));
                    added += 1;
                }
                Some(&(db_size, db_mtime)) if db_size != disk_size || db_mtime != disk_mtime => {
                    to_upsert.push(PathBuf::from(path));
                    changed += 1;
                }
                Some(_) => {} // unchanged
            }
        }

        // Indexed files under this dir no longer on disk → de-index.
        for path in db_by_path.keys() {
            if Path::new(path).starts_with(dir) && !disk.contains_key(path) {
                on_file_remove(Path::new(path), db, node_tx).await;
                removed += 1;
            }
        }
    }

    // (Re)index the collected files, tracking progress so the work is visible
    // after a restart — not only for runtime `share add` / live watcher events.
    if !to_upsert.is_empty() {
        indexing_seen.store(true, Ordering::Relaxed);
    }
    indexing_count.fetch_add(to_upsert.len(), Ordering::Relaxed);
    for path in &to_upsert {
        on_file_upsert(path, db, node_tx, ed2k_tx).await;
        indexing_count.fetch_sub(1, Ordering::Relaxed);
    }

    if added + changed + removed > 0 {
        info!(
            added,
            changed, removed, "Share rescan reconciled offline changes"
        );
    } else {
        info!("Share rescan complete: index already in sync with disk");
    }
}

/// Reduce a set of directories to the *top-level* ones: drop any directory that
/// is nested inside another in the set. A recursive watch (or walk) of an
/// ancestor already covers all its descendants, so watching/scanning a nested
/// share too would double-index every file under it.
///
/// This is the general guard for a user sharing both a directory and a
/// subdirectory of it — by hand or via a download category whose dir sits under
/// the global download_dir or another category's dir. `starts_with` is
/// component-wise, so `/a/bc` is **not** treated as nested in `/a/b`.
fn top_level_dirs(dirs: &[PathBuf]) -> Vec<PathBuf> {
    dirs.iter()
        .filter(|d| !dirs.iter().any(|other| *d != other && d.starts_with(other)))
        .cloned()
        .collect()
}

/// Returns `true` for files that must never be indexed or shared:
///
/// * **Partial downloads** (`*.part`) — incomplete content; sharing one would
///   serve a hash of a half-written file.
/// * **Anything under an excluded directory** (the temp dirs). This is the
///   guard for the footgun of putting `temp_dir` inside `download_dir`: the
///   recursive watcher would otherwise see, index and re-hash every `.part`.
fn is_excluded(path: &Path, excluded: &[PathBuf]) -> bool {
    if path.extension().and_then(|e| e.to_str()) == Some("part") {
        return true;
    }
    excluded.iter().any(|dir| path.starts_with(dir))
}

/// Returns `true` for hidden files (name starts with `.`).
fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.starts_with('.'))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_level_dirs_drops_nested_shares() {
        let dirs = vec![
            PathBuf::from("/data"),
            PathBuf::from("/data/movies"),    // nested in /data
            PathBuf::from("/data/movies/hd"), // nested deeper
            PathBuf::from("/music"),          // independent top
        ];
        let mut tops = top_level_dirs(&dirs);
        tops.sort();
        assert_eq!(tops, vec![PathBuf::from("/data"), PathBuf::from("/music")]);
    }

    #[test]
    fn top_level_dirs_keeps_siblings_with_shared_prefix() {
        // /data/bc is NOT nested in /data/b — comparison is component-wise.
        let dirs = vec![PathBuf::from("/data/b"), PathBuf::from("/data/bc")];
        let mut tops = top_level_dirs(&dirs);
        tops.sort();
        assert_eq!(
            tops,
            vec![PathBuf::from("/data/b"), PathBuf::from("/data/bc")]
        );
    }

    #[test]
    fn top_level_dirs_independent_dirs_all_kept() {
        let dirs = vec![
            PathBuf::from("/a"),
            PathBuf::from("/b"),
            PathBuf::from("/c"),
        ];
        assert_eq!(top_level_dirs(&dirs).len(), 3);
    }

    #[test]
    fn is_excluded_covers_part_files_and_temp_dirs() {
        let excluded = vec![PathBuf::from("/downloads/temp")];

        // Partial downloads are excluded wherever they are.
        assert!(is_excluded(Path::new("/media/movie.mkv.part"), &excluded));
        assert!(is_excluded(Path::new("/anywhere/x.part"), &[]));

        // Anything under a temp dir is excluded — the temp-inside-downloads case.
        assert!(is_excluded(
            Path::new("/downloads/temp/movie.mkv"),
            &excluded
        ));
        assert!(is_excluded(
            Path::new("/downloads/temp/sub/clip.iso"),
            &excluded
        ));

        // Ordinary shared files are not excluded.
        assert!(!is_excluded(Path::new("/downloads/movie.mkv"), &excluded));
        assert!(!is_excluded(Path::new("/media/song.mp3"), &excluded));
        // A sibling dir whose name merely starts the same is not under temp.
        assert!(!is_excluded(
            Path::new("/downloads/temp-extra/x.mkv"),
            &excluded
        ));
    }
}
