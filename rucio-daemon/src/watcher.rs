//! Filesystem watcher service.
//!
//! Wraps the `notify` crate to watch registered shared directories for
//! changes and keep the index in sync:
//!
//! - `Create` / `Modify`  → index (or re-index) the file, announce to DHT
//! - `Remove`             → remove from DB, stop providing in DHT
//! - `Rename` (from/to)   → treat as Remove + Create
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
use std::sync::atomic::{AtomicUsize, Ordering};
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
pub fn spawn(
    db: Db,
    node_tx: mpsc::Sender<NodeCmd>,
    indexing_count: Arc<AtomicUsize>,
) -> WatcherHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel::<WatcherCmd>(64);

    tokio::spawn(async move {
        if let Err(e) = run(db, node_tx, cmd_rx, indexing_count).await {
            warn!("WatcherService exited with error: {e}");
        }
    });

    WatcherHandle { cmd_tx }
}

// ---------------------------------------------------------------------------
// Internal service loop
// ---------------------------------------------------------------------------

async fn run(
    db: Db,
    node_tx: mpsc::Sender<NodeCmd>,
    mut cmd_rx: mpsc::Receiver<WatcherCmd>,
    indexing_count: Arc<AtomicUsize>,
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
    if set.contains(path) {
        debug!(path = %path.display(), "Already watching, skipping");
        return;
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
) {
    match event.kind {
        // File created — queue for debounced indexing
        EventKind::Create(_) => {
            for path in &event.paths {
                if !path.is_file() || is_hidden(path) {
                    continue;
                }
                debug!(path = %path.display(), "Watcher: Create — queuing upsert");
                pending_upserts.insert(path.clone(), Instant::now());
            }
        }

        // Rename (Both): process old removal immediately; queue new path
        EventKind::Modify(notify::event::ModifyKind::Name(notify::event::RenameMode::Both))
            if event.paths.len() == 2 =>
        {
            on_file_remove(&event.paths[0], db, node_tx).await;
            if event.paths[1].is_file() && !is_hidden(&event.paths[1]) {
                debug!(path = %event.paths[1].display(), "Watcher: Rename — queuing upsert");
                pending_upserts.insert(event.paths[1].clone(), Instant::now());
            }
        }

        // Other modifications (data written) — queue for debounced indexing
        EventKind::Modify(_) => {
            for path in &event.paths {
                if !path.is_file() || is_hidden(path) {
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
    let to_index: Vec<&PathBuf> = ready.iter().filter(|p| p.is_file()).collect();
    indexing_count.fetch_add(to_index.len(), Ordering::Relaxed);
    for path in to_index {
        on_file_upsert(path, db, node_tx).await;
        indexing_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Index (or re-index) a file and announce it in the DHT.
async fn on_file_upsert(path: &Path, db: &Db, node_tx: &mpsc::Sender<NodeCmd>) {
    // If the file already exists in the DB with the same path, remove the old
    // record first so we get a fresh hash (content may have changed).
    let path_str = path.to_string_lossy().into_owned();
    if let Ok(hashes) = db::shares::delete_by_path_prefix(db, &path_str).await {
        for hash in hashes {
            let _ = node_tx.send(NodeCmd::StopProviding(hash)).await;
        }
    }

    match index_file(db, path).await {
        Ok(root_hash) => {
            debug!(path = %path.display(), "Watcher: indexed");
            let _ = node_tx
                .send(NodeCmd::StartProviding(root_hash.to_vec()))
                .await;
        }
        Err(e) => warn!(path = %path.display(), "Watcher: failed to index: {e}"),
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
pub async fn reconcile_shares(
    db: &Db,
    node_tx: &mpsc::Sender<NodeCmd>,
    indexing_count: &AtomicUsize,
) {
    let dirs = match db::shared_dirs::list(db).await {
        Ok(d) => d,
        Err(e) => {
            warn!("Share rescan: cannot list shared dirs: {e}");
            return;
        }
    };
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

    for d in &dirs {
        let dir = PathBuf::from(&d.path);
        // Walk + stat off the async worker threads.
        let disk: HashMap<String, (i64, i64)> = {
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
            if Path::new(path).starts_with(&dir) && !disk.contains_key(path) {
                on_file_remove(Path::new(path), db, node_tx).await;
                removed += 1;
            }
        }
    }

    // (Re)index the collected files, tracking progress so the work is visible
    // after a restart — not only for runtime `share add` / live watcher events.
    indexing_count.fetch_add(to_upsert.len(), Ordering::Relaxed);
    for path in &to_upsert {
        on_file_upsert(path, db, node_tx).await;
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

/// Returns `true` for hidden files (name starts with `.`).
fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.starts_with('.'))
        .unwrap_or(false)
}
