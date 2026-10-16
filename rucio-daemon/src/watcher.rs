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
//! bridges events into async-land via an **unbounded** channel (see
//! [`event_bridge`]): the reader thread must never block handing an event off,
//! or the inotify fd stops draining and the kernel queues events in
//! unreclaimable memory.
//!
//! The service loop that drains the bridge does **no I/O of its own**. It
//! reduces each event to a per-path intent in [`PendingOps`] and hands due work
//! to a single [`index_worker`] task over a bounded queue; the worker owns every
//! DB, hashing and DHT operation. So a slow or stuck worker (a backed-up node
//! command channel, a busy disk) only delays indexing — it can never stall the
//! loop and back up into the inotify fd.
//!
//! # Debounce
//!
//! Many editors and copy tools emit multiple events (Create + Modify, or
//! several Modify in quick succession) for a single logical write.  To avoid
//! re-indexing the same file repeatedly we debounce upsert events: when a
//! Create/Modify arrives for a path we record the time and only hand it to the
//! worker after DEBOUNCE_MS have passed with no further events for that path.
//! Removes carry no debounce (there is nothing to read) and go on the next tick.
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
use pending::{IndexingTicket, Op, PendingOps, WORK_QUEUE, Work};
use rucio_core::protocol::hashing::collect_files;

mod pending;

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
/// - `excluded` — paths whose files must never be indexed: directory prefixes
///   (the temp/outboard dirs; guards a `temp_dir` nested inside `download_dir`)
///   and exact files (the daemon's own identity keys, database and config), so
///   sharing the data directory never serves the node's own secrets/state.
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    db: Db,
    node_tx: mpsc::Sender<NodeCmd>,
    indexing_count: Arc<AtomicUsize>,
    excluded: Arc<Vec<PathBuf>>,
    outboard_dir: PathBuf,
    ed2k_tx: Option<crate::ed2k_index::Ed2kIndex>,
    index_guard: crate::index_guard::IndexGuard,
) -> (WatcherHandle, tokio::task::JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::channel::<WatcherCmd>(64);

    let task = tokio::spawn(async move {
        if let Err(e) = run(
            db,
            node_tx,
            cmd_rx,
            indexing_count,
            excluded,
            outboard_dir,
            ed2k_tx,
            index_guard,
        )
        .await
        {
            warn!("WatcherService exited with error: {e}");
        }
    });

    (WatcherHandle { cmd_tx }, task)
}

// ---------------------------------------------------------------------------
// Internal service loop
// ---------------------------------------------------------------------------

/// Build the `notify` → async event bridge, shared by this watcher and the
/// eMule share watcher.
///
/// **Unbounded on purpose.** `notify` runs its inotify/FSEvents reader on a
/// dedicated OS thread and hands each event to this channel from a *sync*
/// context. If that hand-off could block — a bounded channel's `blocking_send`
/// once the async consumer falls behind — the reader thread parks and stops
/// reading the fd. The kernel then queues every new event in *unreclaimable*
/// memory and, once its own `max_queued_events` is hit, drops them with
/// `Q_OVERFLOW`, losing changes silently. Backpressure against an inotify fd is
/// therefore counter-productive: it trades a bounded, reclaimable userspace
/// queue for an unbounded, unreclaimable kernel one that also loses events.
///
/// With an unbounded channel the reader thread never blocks, so the fd always
/// drains; a slow or stalled consumer merely delays indexing and shows up as
/// (reclaimable) RSS, never as kernel slab growth. The realistic working set
/// stays small regardless: `pending_upserts` coalesces repeated events per path.
pub(crate) fn event_bridge() -> (
    mpsc::UnboundedSender<notify::Result<Event>>,
    mpsc::UnboundedReceiver<notify::Result<Event>>,
) {
    mpsc::unbounded_channel()
}

#[allow(clippy::too_many_arguments)]
async fn run(
    db: Db,
    node_tx: mpsc::Sender<NodeCmd>,
    mut cmd_rx: mpsc::Receiver<WatcherCmd>,
    indexing_count: Arc<AtomicUsize>,
    excluded: Arc<Vec<PathBuf>>,
    outboard_dir: PathBuf,
    ed2k_tx: Option<crate::ed2k_index::Ed2kIndex>,
    index_guard: crate::index_guard::IndexGuard,
) -> Result<()> {
    // Bridge: notify (sync OS thread) → tokio (async). Unbounded on purpose —
    // see `event_bridge`: the reader thread must hand events off without ever
    // blocking so the inotify fd keeps draining no matter how slow the async
    // side is.
    let (ev_tx, mut ev_rx) = event_bridge();

    // Shared set of currently-watched paths (used for unwatch)
    let watched: Arc<Mutex<HashSet<PathBuf>>> = Arc::new(Mutex::new(HashSet::new()));

    // Build the notify watcher on a blocking thread so it can call
    // the OS-level inotify/FSEvents API from a sync context.
    let ev_tx_clone = ev_tx.clone();
    let watcher: Arc<Mutex<RecommendedWatcher>> = {
        let w = notify::recommended_watcher(move |res: notify::Result<Event>| {
            // Access events (a shared file opened to serve an upload) are
            // frequent and handle_event ignores them anyway — drop them here so
            // they never even occupy the bridge.
            if matches!(&res, Ok(ev) if ev.kind.is_access()) {
                return;
            }
            // Non-blocking hand-off. Only errs once the service task is gone
            // (receiver dropped), when there is nothing left to index anyway.
            let _ = ev_tx_clone.send(res);
        })?;
        Arc::new(Mutex::new(w))
    };

    // All DB / hashing / DHT work runs in a dedicated worker task, fed in order
    // over a bounded queue. The service loop itself does no I/O, so it always
    // gets straight back to draining events: a slow or stuck worker delays
    // indexing without ever backing up into the inotify fd. The handle is
    // dropped (the task is detached); it exits when `work_tx` drops on shutdown.
    let (work_tx, work_rx) = mpsc::channel::<Work>(WORK_QUEUE);
    tokio::spawn(index_worker(
        work_rx,
        db,
        node_tx,
        Arc::clone(&indexing_count),
        outboard_dir,
        ed2k_tx,
    ));

    // Per-path table of the next operation to hand to the worker.
    let mut pending = PendingOps::new(Duration::from_millis(DEBOUNCE_MS));
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
                        handle_event(event, &mut pending, &excluded, Instant::now());
                    }
                }
            }

            // --- Debounce flush tick -----------------------------------------
            _ = debounce_tick.tick() => {
                // Errors only if the worker task is gone — unrecoverable, so stop.
                pending.flush(&work_tx, &indexing_count, &index_guard, Instant::now())?;
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

/// Translate a notify event into the pending table's intents. No I/O: the loop
/// must get straight back to draining events, so every DB/hash/DHT operation is
/// deferred to the worker via [`PendingOps`].
fn handle_event(event: Event, pending: &mut PendingOps, excluded: &[PathBuf], now: Instant) {
    match event.kind {
        // Rename (Both): the worker repoints the row in place when the content
        // is unchanged (no re-hash), else falls back to remove-old + index-new.
        EventKind::Modify(notify::event::ModifyKind::Name(notify::event::RenameMode::Both))
            if event.paths.len() == 2 =>
        {
            let (old, new) = (&event.paths[0], &event.paths[1]);
            // `shareable` is false when the new path is hidden/excluded/not a
            // file (e.g. moved to a temp dir): the pending table then just drops
            // the old share.
            pending.rename(old, new.clone(), is_shareable(new, excluded), now);
        }

        // The "from" half of a rename: on Linux notify also emits the paired
        // `Both` (handled above) which does the repoint, and the standalone
        // `To` (caught below) which is a no-op once repointed. Ignore the bare
        // `From` so a moved-away file isn't queued as an upsert of a vanished
        // path; if it was a move *out* of the tree with no `Both`, the rescan
        // de-indexes it. (Without this it would fall into the catch-all below.)
        EventKind::Modify(notify::event::ModifyKind::Name(notify::event::RenameMode::From)) => {}

        // File created, or other modifications (data written, or the `To`/`Any`
        // rename half) — queue a debounced upsert. on_file_upsert is idempotent,
        // so the `To` of an already-repointed rename costs nothing, and
        // `PendingOps::upsert` never clobbers a pending rename.
        EventKind::Create(_) | EventKind::Modify(_) => {
            for path in event.paths {
                if is_shareable(&path, excluded) {
                    debug!(path = %path.display(), "Watcher: Create/Modify — queuing upsert");
                    pending.upsert(path, now);
                }
            }
        }

        // File removed — queue a de-index (due on the next tick; nothing to read).
        EventKind::Remove(_) => {
            for path in event.paths {
                pending.remove(path, now);
            }
        }

        _ => {}
    }
}

/// Run queued operations one at a time, in the order the service loop handed
/// them over. This is the only place the watcher does DB / hashing / DHT work,
/// so none of it can ever stall the loop that drains inotify events.
async fn index_worker(
    mut work_rx: mpsc::Receiver<Work>,
    db: Db,
    node_tx: mpsc::Sender<NodeCmd>,
    indexing_count: Arc<AtomicUsize>,
    outboard_dir: PathBuf,
    ed2k_tx: Option<crate::ed2k_index::Ed2kIndex>,
) {
    // `_ticket` keeps an upsert counted on the status endpoint until its arm is
    // done; the rename fallbacks mint their own ticket for the hashing they do.
    while let Some(Work {
        path,
        op,
        ticket: _ticket,
    }) = work_rx.recv().await
    {
        match op {
            Op::Upsert => {
                on_file_upsert(&path, &db, &node_tx, &outboard_dir, ed2k_tx.as_ref()).await;
            }
            Op::Remove => on_file_remove(&path, &db, &node_tx).await,
            Op::RenamedFrom(old) => match repoint_rename(&old, &path, &db).await {
                Rename::Repointed => {}
                // Content changed alongside the rename (or the repoint raced) →
                // drop the old row and re-index the new path.
                Rename::Changed => {
                    on_file_remove(&old, &db, &node_tx).await;
                    let _ticket = IndexingTicket::new(&indexing_count);
                    on_file_upsert(&path, &db, &node_tx, &outboard_dir, ed2k_tx.as_ref()).await;
                }
                // Old path wasn't a tracked share (hidden/excluded before, or a
                // DB miss): treat the new path as a fresh file.
                Rename::Untracked => {
                    let _ticket = IndexingTicket::new(&indexing_count);
                    on_file_upsert(&path, &db, &node_tx, &outboard_dir, ed2k_tx.as_ref()).await;
                }
            },
        }
    }
}

/// Whether `path` still falls under a currently-shared directory. Re-read from
/// the DB on each call so that un-sharing a directory mid-scan stops queued
/// hashing instead of draining the whole queue. On a DB error we keep the file
/// (don't silently drop indexing work).
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
    outboard_dir: &Path,
    ed2k_tx: Option<&crate::ed2k_index::Ed2kIndex>,
) {
    // Don't (re)index a file the share no longer wants: its directory may have
    // been unshared while the file was queued (e.g. the user removed the share
    // mid-scan), or the directory's filter (recursive flag / extensions) may
    // exclude it. `should_share` applies the most-specific directory's filter.
    if !db::shared_dirs::should_share(db, path).await {
        debug!(path = %path.display(), "Watcher: not shared (unshared or filtered out), skipping index");
        return;
    }

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
            // The rucio index is up to date, but the eMule hash may still be
            // missing — e.g. its earlier hashing was dropped when the ed2k
            // channel was momentarily full. Re-queue *only* the ed2k hashing
            // (no re-hash of the rucio side) when there is no ed2k row for this
            // path yet, so a reconcile sweep recovers it without a restart.
            if let Some(tx) = ed2k_tx
                && db::emule_shared_files::get_by_path(db, &path_str)
                    .await
                    .ok()
                    .flatten()
                    .is_none()
            {
                tx.enqueue(path.to_path_buf());
            }
            return;
        }
        // Content changed: drop the stale row (exact match → uses the path
        // index, not the O(files²) prefix scan) before re-hashing.
        if let Ok(Some(hash)) = db::shares::delete_by_path(db, &path_str).await {
            let _ = node_tx.send(NodeCmd::StopProviding(hash)).await;
        }
    }

    // Persist the bao outboard eagerly for large files (>= the threshold in
    // `index_file`), exactly like an API-added or eMule-completed share — the
    // watcher does the *initial* indexing of `shared_dirs`, not just rare
    // re-indexes, so a large file dropped into a watched dir must get its
    // outboard too instead of paying a full re-read on the first chunk request.
    match index_file(db, path, Some(outboard_dir)).await {
        Ok(root_hash) => {
            debug!(path = %path.display(), "Watcher: indexed");
            let _ = node_tx
                .send(NodeCmd::StartProviding(root_hash.to_vec()))
                .await;
            if let Some(tx) = ed2k_tx {
                tx.enqueue(path.to_path_buf());
            }
        }
        Err(e) => warn!(path = %path.display(), "Watcher: failed to index: {e}"),
    }
}

/// Outcome of trying to repoint a renamed share in place.
enum Rename {
    /// Same content: the row now points at the new path.
    Repointed,
    /// The old path was indexed but the content changed around the rename.
    Changed,
    /// The old path was not a tracked share.
    Untracked,
}

/// Try to repoint a renamed share (`old` → `new`, both inside watched dirs).
///
/// A pure rename leaves the content — and therefore the root hash and chunks —
/// untouched, so when the new path's size + mtime still match what we recorded
/// for the old path we just repoint the DB row: no re-hash, and no DHT churn
/// (we keep providing the same hash). Otherwise the caller falls back to
/// remove-old + index-new.
async fn repoint_rename(old: &Path, new: &Path, db: &Db) -> Rename {
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
                debug!(from = %old.display(), to = %new.display(), "Watcher: renamed (repointed, no re-hash)");
                Rename::Repointed
            } else {
                Rename::Changed
            }
        }
        // Old path wasn't a tracked share (hidden/excluded before, or a DB miss).
        _ => Rename::Untracked,
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
    outboard_dir: &Path,
    ed2k_tx: Option<&crate::ed2k_index::Ed2kIndex>,
    indexing_seen: &AtomicBool,
    index_guard: &crate::index_guard::IndexGuard,
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

        // Drop excluded files: `.part`, anything under a temp/outboard dir, and
        // the daemon's own state files (identity keys, database, config) matched
        // by exact path. Removing them from the disk set means they're never
        // indexed, and any that slipped into the index before are de-indexed
        // below (in DB, absent from disk → removed).
        disk.retain(|p, _| !is_excluded(Path::new(p), excluded));

        // Apply each directory's file filter (recursive flag + extensions).
        // Files that no longer match are dropped from the disk set, so the
        // de-index pass below removes them — this is what makes editing a
        // directory's filter take effect on the next reconcile without a restart.
        disk.retain(|p, _| db::shared_dirs::dirs_share(&rows, Path::new(p)));

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
        // Skip a file a completion path is already indexing (see IndexGuard): it
        // hashes and inserts the row itself, so re-indexing here is redundant and
        // would race it. The count is still balanced (add above, sub below).
        if !index_guard.is_marked(path) {
            on_file_upsert(path, db, node_tx, outboard_dir, ed2k_tx).await;
        }
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
/// * **Anything matching an excluded path.** `starts_with` is component-wise, so
///   a *directory* entry (the temp/outboard dirs) matches everything under it —
///   the guard for the footgun of putting `temp_dir` inside `download_dir` — and
///   a *file* entry (the daemon's own identity keys, database and config) matches
///   only its exact path, so the node never serves its own secrets/state while a
///   like-named file the user has elsewhere is shared normally.
fn is_excluded(path: &Path, excluded: &[PathBuf]) -> bool {
    if path.extension().and_then(|e| e.to_str()) == Some("part") {
        return true;
    }
    excluded.iter().any(|dir| path.starts_with(dir))
}

/// Whether `path` is a regular file the share should index (not hidden, not
/// excluded). Used both when a file lands and when one is renamed into place.
fn is_shareable(path: &Path, excluded: &[PathBuf]) -> bool {
    path.is_file() && !is_hidden(path) && !is_excluded(path, excluded)
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

    #[tokio::test]
    async fn event_bridge_never_blocks_the_reader_thread() {
        // The notify reader thread hands events off from a sync context; it must
        // never block, or the inotify fd stops draining and the kernel queues
        // events unreclaimably. Mirror that hand-off: push far more events than
        // any bounded capacity with the consumer NOT reading, from a blocking
        // thread, and require it to finish promptly. A bounded `blocking_send`
        // would park at capacity forever and trip the timeout.
        let (tx, _rx) = event_bridge();
        let produced = tokio::task::spawn_blocking(move || {
            for _ in 0..100_000 {
                if tx.send(Ok(Event::default())).is_err() {
                    return 0;
                }
            }
            100_000
        });
        let n = tokio::time::timeout(Duration::from_secs(5), produced)
            .await
            .expect("reader hand-off blocked — the bridge is not unbounded")
            .unwrap();
        assert_eq!(n, 100_000, "every event handed off without blocking");
    }

    #[test]
    fn rename_events_map_to_one_pending_rename() {
        // notify on Linux delivers the paired `Both`, then the standalone `To`.
        // Both must collapse to a single pending rename on the new path, with
        // nothing left pending on the old one.
        let dir = tempfile::tempdir().unwrap();
        let new = dir.path().join("new.mkv");
        std::fs::write(&new, b"x").unwrap();
        let old = dir.path().join("old.mkv");
        let now = Instant::now();
        let mut pending = PendingOps::new(Duration::from_millis(DEBOUNCE_MS));
        let name = |mode| EventKind::Modify(notify::event::ModifyKind::Name(mode));

        let both = Event::new(name(notify::event::RenameMode::Both))
            .add_path(old.clone())
            .add_path(new.clone());
        let to = Event::new(name(notify::event::RenameMode::To)).add_path(new.clone());
        handle_event(both, &mut pending, &[], now);
        handle_event(to, &mut pending, &[], now);

        assert_eq!(pending.op(&new), Some(&Op::RenamedFrom(old.clone())));
        assert_eq!(pending.op(&old), None);
    }

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

    #[test]
    fn is_excluded_protects_own_state_files_by_exact_path() {
        // The daemon's own identity keys / database / config are passed as exact
        // file paths, so sharing the data dir never serves them.
        let excluded = vec![
            PathBuf::from("/data/identity.key"),
            PathBuf::from("/data/emule_identity.key"),
            PathBuf::from("/data/rucio.db"),
            PathBuf::from("/data/rucio.db-wal"),
            PathBuf::from("/data/config.toml"),
        ];

        assert!(is_excluded(Path::new("/data/identity.key"), &excluded));
        assert!(is_excluded(
            Path::new("/data/emule_identity.key"),
            &excluded
        ));
        assert!(is_excluded(Path::new("/data/rucio.db"), &excluded));
        assert!(is_excluded(Path::new("/data/rucio.db-wal"), &excluded));
        assert!(is_excluded(Path::new("/data/config.toml"), &excluded));

        // A like-named file the user has elsewhere is shared normally — the match
        // is the exact configured path, not the file name.
        assert!(!is_excluded(Path::new("/media/identity.key"), &excluded));
        assert!(!is_excluded(Path::new("/media/rucio.db"), &excluded));
        // And a similarly-prefixed sibling name is not caught (component-wise).
        assert!(!is_excluded(Path::new("/data/rucio.db.backup"), &excluded));
        assert!(!is_excluded(Path::new("/data/config.toml.bak"), &excluded));
    }

    async fn test_db() -> (Db, tempfile::TempDir) {
        use sqlx::sqlite::SqlitePoolOptions;
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .unwrap();
        crate::db::apply_schema(&pool).await.unwrap();
        (pool, dir)
    }

    #[tokio::test]
    async fn still_shared_tracks_dir_removal_mid_scan() {
        let (db, _d) = test_db().await;
        db::shared_dirs::insert(&db, "/data/movies", false, 1)
            .await
            .unwrap();

        let film = Path::new("/data/movies/film.mkv");
        assert!(
            db::shared_dirs::should_share(&db, film).await,
            "a file under a shared dir is still shared"
        );
        assert!(
            !db::shared_dirs::should_share(&db, Path::new("/other/x.mkv")).await,
            "a file outside every shared dir is not shared"
        );

        // Unshare the directory while its files are still queued: they must stop
        // counting as shared, which is what makes the indexing loop skip them.
        db::shared_dirs::delete(&db, "/data/movies").await.unwrap();
        assert!(
            !db::shared_dirs::should_share(&db, film).await,
            "after the dir is unshared its files are no longer shared"
        );
    }
}
