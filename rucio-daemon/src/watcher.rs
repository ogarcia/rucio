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
//! # Watcher lifecycle
//!
//! 1. `WatcherService::spawn()` starts the service task and returns a
//!    `WatcherHandle` with a command sender.
//! 2. The daemon sends `WatchDir` / `UnwatchDir` commands as directories are
//!    added or removed.
//! 3. On shutdown the command channel is dropped; the service task exits.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::api::shares::index_file;
use crate::db::{self, Db};
use crate::node::messages::NodeCmd;

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
pub fn spawn(db: Db, node_tx: mpsc::Sender<NodeCmd>) -> WatcherHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel::<WatcherCmd>(64);

    tokio::spawn(async move {
        if let Err(e) = run(db, node_tx, cmd_rx).await {
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
                        handle_event(event, &db, &node_tx).await;
                    }
                }
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

async fn handle_event(event: Event, db: &Db, node_tx: &mpsc::Sender<NodeCmd>) {
    match event.kind {
        // File created — index it
        EventKind::Create(_) => {
            for path in &event.paths {
                if !path.is_file() || is_hidden(path) {
                    continue;
                }
                on_file_upsert(path, db, node_tx).await;
            }
        }

        // Rename (Both): treat as remove old + create new
        EventKind::Modify(notify::event::ModifyKind::Name(notify::event::RenameMode::Both))
            if event.paths.len() == 2 =>
        {
            on_file_remove(&event.paths[0], db, node_tx).await;
            if event.paths[1].is_file() && !is_hidden(&event.paths[1]) {
                on_file_upsert(&event.paths[1], db, node_tx).await;
            }
        }

        // Other modifications (data changed) — re-index
        EventKind::Modify(_) => {
            for path in &event.paths {
                if !path.is_file() || is_hidden(path) {
                    continue;
                }
                on_file_upsert(path, db, node_tx).await;
            }
        }

        // File removed — deindex it
        EventKind::Remove(_) => {
            for path in &event.paths {
                on_file_remove(path, db, node_tx).await;
            }
        }

        _ => {}
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

/// Returns `true` for hidden files (name starts with `.`).
fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.starts_with('.'))
        .unwrap_or(false)
}
