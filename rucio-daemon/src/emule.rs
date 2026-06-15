//! eMule compatibility integration for the daemon.
//!
//! This module is only compiled when the `emule-compat` feature is enabled.
//! It bridges `rucio-emule` into the daemon's download engine.

#![cfg(feature = "emule-compat")]

use anyhow::{Context, Result};
use rucio_emule::Ed2kLink;
use rucio_emule::ed2k::CHUNK_SIZE;
use rucio_emule::kad::packet::KadId;
use rucio_emule::kad::search::KadSource;
use rucio_emule::kad::task::{KadHandle, KadTaskConfig};
use rucio_emule::progress::{load_progress, save_progress};
use rucio_emule::transfer::{ActiveDownloads, DownloadEvent, DownloadOptions, Session, UploadInfo};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::db::Db;

/// Registry of running eMule download tasks: `download_id` → stop flag.
///
/// Unlike rucio downloads (driven synchronously by the main-loop engine), each
/// eMule download runs in its own spawned task. This registry lets the API stop
/// a running task promptly (set its flag) and lets the main loop avoid spawning
/// a duplicate task for an id that is already running.
pub type EmuleCancelRegistry = Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>;

/// Removes a download from the cancel registry when its task ends — on every
/// exit path (success, user stop, or error) via `Drop`.
struct RegistryGuard {
    registry: EmuleCancelRegistry,
    download_id: i64,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        if let Ok(mut reg) = self.registry.lock() {
            reg.remove(&self.download_id);
        }
    }
}

/// Load persisted eMule shared files into the upload whitelist at startup,
/// dropping any whose on-disk file has changed or vanished.
///
/// These are completed downloads we keep seeding (good-citizen policy). The
/// share lives in `emule_shared_files`, independent of the downloads list, so
/// clearing completed downloads does not stop sharing. A file is only kept if
/// its size and mtime still match what we recorded — otherwise the user has
/// modified/replaced it and we stop sharing (and forget it).
pub async fn load_shared_files(db: &Db, active_downloads: &ActiveDownloads) {
    let rows = match crate::db::emule_shared_files::list(db).await {
        Ok(r) => r,
        Err(e) => {
            warn!("Cannot list eMule shared files: {e}");
            return;
        }
    };
    let (mut loaded, mut dropped) = (0usize, 0usize);
    for row in rows {
        let path = std::path::PathBuf::from(&row.path);
        let disk_size = std::fs::metadata(&path)
            .map(|m| m.len() as i64)
            .unwrap_or(-1);
        let unchanged =
            disk_size == row.size && crate::api::shares::file_mtime_secs(&path) == row.mtime;
        if !unchanged {
            let _ = crate::db::emule_shared_files::delete_by_hash(db, &row.ed2k_hash).await;
            dropped += 1;
            continue;
        }
        let Ok(hash) = <[u8; 16]>::try_from(row.ed2k_hash.as_slice()) else {
            continue;
        };
        let size = row.size as u64;
        let num_slices = size.div_ceil(CHUNK_SIZE as u64) as usize;
        active_downloads.write().await.insert(
            hash,
            UploadInfo {
                name: row.name,
                total_size: size,
                num_slices,
                path,
                complete: true,
                hashset: row.hashset,
            },
        );
        loaded += 1;
    }
    if loaded + dropped > 0 {
        info!(loaded, dropped, "Loaded eMule shared files for seeding");
    }
}

/// Watch the downloads directory and stop sharing any seeded eMule file the
/// moment it is modified or removed on disk.
///
/// On each filesystem event we look the path up in `emule_shared_files` and, if
/// it is one of our shares, re-validate it against the recorded size+mtime
/// (exactly as the startup reconcile does). Comparing against the stored record
/// — rather than trusting the event kind — means our own just-completed file
/// (which matches what we just stored) is never dropped, while a real
/// modification/removal is. Independent of the rucio share watcher and only
/// touches files present in `emule_shared_files`.
pub fn spawn_shared_files_watcher(
    db: Db,
    active_downloads: ActiveDownloads,
    downloads_dir: std::path::PathBuf,
) {
    use notify::event::{ModifyKind, RenameMode};
    use notify::{EventKind, RecursiveMode, Watcher};

    tokio::spawn(async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<notify::Result<notify::Event>>(128);
        let mut watcher = match notify::recommended_watcher(move |res| {
            let _ = tx.blocking_send(res);
        }) {
            Ok(w) => w,
            Err(e) => {
                warn!("eMule share watcher: cannot create: {e}");
                return;
            }
        };
        // Ensure the directory exists so the watch succeeds on a fresh install
        // (downloads complete into it later).
        let _ = std::fs::create_dir_all(&downloads_dir);
        if let Err(e) = watcher.watch(&downloads_dir, RecursiveMode::Recursive) {
            warn!(dir = %downloads_dir.display(), "eMule share watcher: cannot watch: {e}");
            return;
        }
        info!(dir = %downloads_dir.display(), "Watching downloads dir for eMule share changes");

        while let Some(ev) = rx.recv().await {
            let Ok(ev) = ev else { continue };
            // Access/open events never change content; skip the DB lookups.
            if matches!(ev.kind, EventKind::Access(_)) {
                continue;
            }
            // Pure rename inside the watched tree: repoint our seeded entry
            // instead of dropping it, keeping the ed2k hash/hashset — no MD4
            // recompute. Mirrors the Rucio share watcher's rename handling.
            if matches!(
                ev.kind,
                EventKind::Modify(ModifyKind::Name(RenameMode::Both))
            ) && ev.paths.len() == 2
            {
                handle_emule_rename(&db, &active_downloads, &ev.paths[0], &ev.paths[1]).await;
                continue;
            }
            for path in &ev.paths {
                let path_str = path.to_string_lossy().into_owned();
                let row = match crate::db::emule_shared_files::get_by_path(&db, &path_str).await {
                    Ok(Some(r)) => r,
                    _ => continue, // not one of our shares (or DB error) — ignore
                };
                let disk_size = std::fs::metadata(path)
                    .map(|m| m.len() as i64)
                    .unwrap_or(-1);
                let unchanged =
                    disk_size == row.size && crate::api::shares::file_mtime_secs(path) == row.mtime;
                if unchanged {
                    continue; // genuine no-op (or our own completion) — keep sharing
                }
                let _ = crate::db::emule_shared_files::delete_by_hash(&db, &row.ed2k_hash).await;
                if let Ok(hash) = <[u8; 16]>::try_from(row.ed2k_hash.as_slice()) {
                    active_downloads.write().await.remove(&hash);
                }
                info!(path = %path.display(), "eMule shared file changed/removed — stopped sharing");
            }
        }
    });
}

/// Handle a rename of a seeded eMule file. When the content is unchanged (new
/// path's size + mtime match the recorded row) we repoint the share in place —
/// the ed2k hash and hashset are kept, no MD4 recompute — and update the upload
/// whitelist's serving path. If the content changed (a write around the rename),
/// fall back to dropping the share; a Rucio re-index will re-seed it.
async fn handle_emule_rename(
    db: &Db,
    active_downloads: &ActiveDownloads,
    old: &std::path::Path,
    new: &std::path::Path,
) {
    let old_str = old.to_string_lossy().into_owned();
    let row = match crate::db::emule_shared_files::get_by_path(db, &old_str).await {
        Ok(Some(r)) => r,
        _ => return, // not one of our seeded files — nothing to do
    };
    let Ok(hash) = <[u8; 16]>::try_from(row.ed2k_hash.as_slice()) else {
        return;
    };

    let disk_size = std::fs::metadata(new).map(|m| m.len() as i64).unwrap_or(-1);
    let unchanged = disk_size == row.size && crate::api::shares::file_mtime_secs(new) == row.mtime;

    if !unchanged {
        // Content changed alongside the rename → stop seeding the stale file.
        let _ = crate::db::emule_shared_files::delete_by_hash(db, &row.ed2k_hash).await;
        active_downloads.write().await.remove(&hash);
        info!(path = %new.display(), "eMule shared file changed on rename — stopped sharing");
        return;
    }

    let new_str = new.to_string_lossy().into_owned();
    let new_name = new
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| new_str.clone());
    if crate::db::emule_shared_files::rename_path(db, &old_str, &new_str, &new_name)
        .await
        .unwrap_or(false)
    {
        // Repoint the upload whitelist's serving path (the hash key is unchanged).
        if let Some(info) = active_downloads.write().await.get_mut(&hash) {
            info.path = new.to_path_buf();
            info.name = new_name;
        }
        debug!(from = %old.display(), to = %new.display(), "eMule seeded file renamed (repointed)");
    }
}

/// How long between rounds of re-announcing our shared files as Kad sources.
/// Hardcoded by design — eMule republishes sources roughly every 5 hours and
/// there is nothing user-tunable worth exposing here.
const SOURCE_REPUBLISH_INTERVAL: Duration = Duration::from_secs(5 * 60 * 60);
/// Shorter retry when we can't publish because the port looks firewalled, so a
/// node that becomes reachable starts seeding without waiting a full round.
const SOURCE_REPUBLISH_RETRY: Duration = Duration::from_secs(20 * 60);
/// Delay before the first round so bootstrap and the first Kad firewall checks
/// can run and the reachability verdict can settle.
const SOURCE_REPUBLISH_WARMUP: Duration = Duration::from_secs(5 * 60);
/// Spacing between individual publishes within a round, so a large share list
/// does not monopolise the single Kad search slot.
const SOURCE_PUBLISH_SPACING: Duration = Duration::from_secs(10);
/// A peer connecting to us within this window is current proof the eMule TCP
/// port is open. Mirrors the status page's "Open" verdict window.
const PUBLISH_REACHABLE_WINDOW_SECS: u64 = 20 * 60;

/// Whether the eMule TCP port looks reachable enough to publish ourselves as a
/// source. We only publish when High-ID: a recent inbound connection is direct
/// proof, and a UPnP mapping or a user-configured external IP are firm promises.
/// Publishing a firewalled source (no buddy support) would just litter the DHT
/// with an entry nobody can reach — the opposite of good citizenship.
async fn publish_reachable(
    config: &Config,
    last_inbound_at: &AtomicU64,
    upnp_external_ip: &tokio::sync::RwLock<Option<String>>,
) -> bool {
    let last = last_inbound_at.load(Ordering::Relaxed);
    if last != 0 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(last) <= PUBLISH_REACHABLE_WINDOW_SECS {
            return true;
        }
    }
    if config.network.upnp && upnp_external_ip.read().await.is_some() {
        return true;
    }
    config.emule.external_ip.is_some()
}

/// Periodically announce the files we seed (`emule_shared_files`) to the Kad DHT
/// as sources, so other clients can discover us by the canonical route instead
/// of only through client-to-client source exchange. Runs only while the eMule
/// TCP port looks reachable (see [`publish_reachable`]).
pub fn spawn_source_republisher(
    db: Db,
    kad: KadHandle,
    config: Arc<Config>,
    last_inbound_at: Arc<AtomicU64>,
    upnp_external_ip: Arc<tokio::sync::RwLock<Option<String>>>,
) {
    tokio::spawn(async move {
        tokio::time::sleep(SOURCE_REPUBLISH_WARMUP).await;
        loop {
            if !publish_reachable(&config, &last_inbound_at, &upnp_external_ip).await {
                debug!("Skipping eMule source publish — TCP port not reachable yet");
                tokio::time::sleep(SOURCE_REPUBLISH_RETRY).await;
                continue;
            }
            let files = match crate::db::emule_shared_files::list(&db).await {
                Ok(f) => f,
                Err(e) => {
                    warn!("Source republish: cannot list shared files: {e}");
                    Vec::new()
                }
            };
            if !files.is_empty() {
                info!(
                    count = files.len(),
                    "Republishing eMule shared files as Kad sources"
                );
                for row in files {
                    let Ok(bytes) = <[u8; 16]>::try_from(row.ed2k_hash.as_slice()) else {
                        continue;
                    };
                    let hash = rucio_emule::ed2k::Ed2kHash::from_bytes(bytes);
                    let stored = kad.publish_source(hash, row.size.max(0) as u64).await;
                    debug!(
                        hash = %hex::encode(row.ed2k_hash),
                        stored,
                        "Published eMule source to Kad"
                    );
                    tokio::time::sleep(SOURCE_PUBLISH_SPACING).await;
                }
            }
            tokio::time::sleep(SOURCE_REPUBLISH_INTERVAL).await;
        }
    });
}

/// Spacing between hashing two files during the one-shot startup backfill, so
/// catching up on a large pre-existing library does not monopolise disk I/O —
/// this is best-effort seeding, never urgent. Live indexing events (the channel
/// path) are not spaced: they arrive one at a time at the watcher's own pace.
const ED2K_BACKFILL_SPACING: Duration = Duration::from_secs(3);

/// Compute the ed2k hashes of files already shared on the Rucio network so they
/// can be seeded to the eMule Kad DHT as sources too — a one-shot catch-up for
/// files that existed before this run (they generate no filesystem event, so
/// the live channel never sees them).
///
/// Runs once at startup, gently: each file is hashed off the async runtime
/// (`spawn_blocking`) with [`ED2K_BACKFILL_SPACING`] between files. Files added
/// or changed *while running* are handled by [`spawn_ed2k_indexer`] instead, so
/// this is not a loop — no periodic CPU spikes.
pub fn spawn_ed2k_startup_backfill(db: Db, active_downloads: ActiveDownloads) {
    tokio::spawn(async move {
        // Snapshot the candidates once. Anything indexed after this point comes
        // in through the live channel, and anything that fails here is retried
        // on the next startup — we never re-poll on a timer.
        let candidates =
            match crate::db::emule_shared_files::list_backfill_candidates(&db, i64::MAX).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("ed2k startup backfill: cannot list candidates: {e}");
                    return;
                }
            };
        if candidates.is_empty() {
            return;
        }
        info!(
            count = candidates.len(),
            "Computing ed2k hashes for existing Rucio shares to seed on eMule"
        );
        for cand in candidates {
            backfill_path(&db, &active_downloads, std::path::PathBuf::from(cand.path)).await;
            tokio::time::sleep(ED2K_BACKFILL_SPACING).await;
        }
    });
}

/// Consume freshly-indexed Rucio share paths and seed each on eMule, hashing it
/// the moment it becomes a share — event-driven, so no periodic rescan is ever
/// needed. The sender side is the share watcher's `on_file_upsert` (live events
/// and offline-reconcile alike); it uses `try_send`, so a burst that overflows
/// the channel simply drops the surplus, which the next startup backfill picks
/// up. eMule seeding must never throttle the Rucio share pipeline.
pub fn spawn_ed2k_indexer(
    db: Db,
    active_downloads: ActiveDownloads,
    mut rx: tokio::sync::mpsc::Receiver<std::path::PathBuf>,
) {
    tokio::spawn(async move {
        while let Some(path) = rx.recv().await {
            backfill_path(&db, &active_downloads, path).await;
        }
    });
}

/// Hash one shared file and register it for eMule source seeding. Self-contained
/// and best-effort: it stats the file itself, and any error (file vanished,
/// unreadable, or changed mid-hash) is logged at debug and skipped — a later
/// startup backfill or re-index event retries it.
async fn backfill_path(db: &Db, active_downloads: &ActiveDownloads, path: std::path::PathBuf) {
    let Ok(md) = std::fs::metadata(&path) else {
        return; // gone since it was indexed
    };
    if !md.is_file() {
        return;
    }
    let size0 = md.len() as i64;
    let mtime0 = crate::api::shares::file_mtime_secs(&path);
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();

    let hash_path = path.clone();
    let computed = tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&hash_path)?;
        rucio_emule::ed2k::hash_reader_full(std::io::BufReader::new(file))
    })
    .await;
    let (hash, parts) = match computed {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            debug!(path = %path.display(), "ed2k backfill: cannot hash file: {e}");
            return;
        }
        Err(e) => {
            debug!(path = %path.display(), "ed2k backfill: hashing task failed: {e}");
            return;
        }
    };

    // Re-stat after hashing: if size or mtime moved while we were reading, the
    // file changed under us — skip; the change will re-trigger indexing and we
    // pick up the new state from there.
    let disk_size = std::fs::metadata(&path)
        .map(|m| m.len() as i64)
        .unwrap_or(-1);
    if disk_size != size0 || crate::api::shares::file_mtime_secs(&path) != mtime0 {
        debug!(path = %path.display(), "ed2k backfill: file changed mid-hash, skipping");
        return;
    }

    let size = size0.max(0) as u64;
    let hashset = rucio_emule::ed2k::finalize_hashset(&parts, size, &hash);
    let hash_bytes = *hash.as_bytes();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if let Err(e) = crate::db::emule_shared_files::upsert(
        db,
        &hash_bytes,
        &name,
        size,
        &path.to_string_lossy(),
        mtime0,
        &hashset,
        now,
    )
    .await
    {
        warn!(path = %path.display(), "ed2k backfill: cannot record share: {e}");
        return;
    }
    let num_slices = size.div_ceil(CHUNK_SIZE as u64) as usize;
    active_downloads.write().await.insert(
        hash_bytes,
        UploadInfo {
            name,
            total_size: size,
            num_slices,
            path,
            complete: true,
            hashset,
        },
    );
    debug!(hash = %hash, "ed2k backfill: now seeding Rucio share on eMule");
}

/// The `.part` and `.part.met` paths for an eMule download identified by its raw
/// 16-byte ed2k hash. Single source of truth for the temp-file naming, shared
/// by the download task and the API (cancel/delete cleanup).
pub fn part_paths(
    config: &Config,
    ed2k_hash: &[u8; 16],
) -> (std::path::PathBuf, std::path::PathBuf) {
    let hash = rucio_emule::ed2k::Ed2kHash::from_bytes(*ed2k_hash);
    let temp = &config.emule.temp_dir;
    (
        temp.join(format!("{hash}.part")),
        temp.join(format!("{hash}.part.met")),
    )
}

/// Bind the persistent Kad2 UDP socket on the configured port and spawn the
/// Kad2 background task.
///
/// The returned [`KadHandle`] is the only way to interact with Kad2 from the
/// rest of the daemon — it must **not** share the underlying socket.
/// Bind the eMule TCP listener on the configured port.
///
/// Returns a `TcpListener` ready to be passed to
/// [`rucio_emule::transfer::serve_incoming`] for High-ID operation.
/// Logs a warning and returns an error if the port cannot be bound (e.g. already
/// in use), but the rest of the daemon keeps running in Low-ID mode.
pub async fn start_emule_tcp_listener(config: &Config) -> Result<tokio::net::TcpListener> {
    let port = config.emule.tcp_port;
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .with_context(|| format!("bind eMule TCP socket on port {port}"))?;
    info!(port, "eMule TCP socket bound (High-ID mode)");
    Ok(listener)
}

pub async fn start_kad_task(config: &Config) -> Result<KadHandle> {
    let port = config.emule.udp_port;
    let socket = UdpSocket::bind(format!("0.0.0.0:{port}"))
        .await
        .with_context(|| format!("bind Kad2 UDP socket on port {port}"))?;
    info!(port, "Kad2 UDP socket bound");

    let our_id = KadId::random();
    // Our persistent eMule user hash: published as the Kad source owner ID so
    // downloaders can key TCP obfuscation to us (separate from the node KadId).
    let user_hash = crate::emule_identity::load_or_create(&crate::emule_identity::path(config))
        .unwrap_or([0u8; 16]);
    let task_cfg = KadTaskConfig {
        tcp_port: config.emule.tcp_port,
        initial_external_ip: config
            .emule
            .external_ip
            .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED),
        user_hash,
        ..KadTaskConfig::default()
    };

    let handle = rucio_emule::kad::task::spawn(Arc::new(socket), our_id, task_cfg);

    // Seed the routing table from cached/bootstrap contacts so the first
    // download does not have to wait for an on-demand bootstrap. Run it in the
    // background: the bootstrap waits on UDP replies from up to 200 peers and
    // would otherwise block startup — including the HTTP/WS server, which is
    // spawned later in `run()` — for several seconds. Downloads re-bootstrap
    // on demand if the table is still thin when they start.
    let seeds = load_kad_seeds(config, 200);
    if !seeds.is_empty() {
        let boot_handle = handle.clone();
        tokio::spawn(async move {
            info!(
                seeds = seeds.len(),
                "Bootstrapping Kad2 from cached/bootstrap contacts"
            );
            let count = boot_handle.bootstrap(seeds).await;
            info!(contacts = count, "Kad2 initial bootstrap done");
        });
    } else {
        info!("No Kad2 seeds available at startup (download nodes.dat first)");
    }

    Ok(handle)
}

/// Number of fruitless retry rounds (no sources / no progress) after which a
/// download is reported as `stalled`.  With the back-off below this is reached
/// after roughly 15 minutes.  The download keeps retrying regardless.
const STALL_AFTER_ROUNDS: u32 = 5;

/// How many times a single source is tried within one download round before it
/// is dropped from the pool. A peer that is briefly queueing us (slots full) or
/// glitches mid-transfer is returned to the back of the pool and retried this
/// many times — interleaved with other sources — instead of being lost for the
/// whole round on the first failure.
const MAX_SOURCE_ATTEMPTS: u32 = 3;

/// Exponential back-off for source-search retries.
/// Sequence: 30 s, 60 s, 2 min, 4 min, 8 min, 16 min, 30 min (cap), …
fn retry_delay_secs(attempt: u32) -> u64 {
    const BASE: u64 = 30;
    const MAX: u64 = 30 * 60;
    // Cap the shift so we never overflow u64 before the min() clamps us.
    (BASE * (1_u64 << attempt.min(10))).min(MAX)
}

/// Download progress shared across all workers of a single eMule download.
///
/// `per_slice[i]` holds the bytes fetched so far for slice `i` (its full length
/// once complete, 0 while pending), and `total` is their running sum. Every
/// worker updates the slice it owns and reads `total`, so the reported byte
/// count reflects every in-flight slice at once. Without this, each worker
/// reported only its own slice added to a global baseline captured when it
/// started — and because workers emit progress events independently and
/// interleaved, the total jumped up and down as different workers reported.
struct ProgressState {
    per_slice: Vec<u64>,
    total: u64,
}

/// Run a full eMule download pipeline using the running Kad2 task.
///
/// The `download_id` is the `emule_downloads.id` row that was already created
/// by the caller (via `db::emule_downloads::create`).  This function owns the
/// lifecycle of that row from `finding_providers` through to `completed`.
///
/// This function **never returns an error due to "no sources" or "peers
/// failed"** — those are transient conditions that trigger a retry, exactly
/// as the real eMule client behaves.  The only way a download stops is:
///
/// - It completes successfully.
/// - The user cancels it (detected by polling the DB status).
/// - A hard infrastructure error occurs (cannot read nodes.dat, cannot create
///   temp directory, I/O error on the completed file, etc.).
// Every log line in this function carries `dl = download_id` so it is clear
// which download it belongs to; the "Starting eMule download" line additionally
// logs the name + hash, so `dl` can be mapped to a file from a single line.
#[allow(clippy::too_many_arguments)]
pub async fn run_ed2k_download(
    link_str: &str,
    download_id: i64,
    config: &Arc<Config>,
    db: &Db,
    kad: &KadHandle,
    active_downloads: &ActiveDownloads,
    download_slots: &Arc<Semaphore>,
    live_stats: &crate::live_stats::LiveStatsMap,
    metrics: &Arc<crate::metrics::Metrics>,
    download_throttle: &Arc<crate::throttle::TokenBucket>,
    notifier: &crate::notifier::Notifier,
    node_tx: tokio::sync::mpsc::Sender<crate::node::messages::NodeCmd>,
    cancel: Arc<AtomicBool>,
    cancel_registry: EmuleCancelRegistry,
) -> Result<()> {
    // Deregister from the cancel registry on every exit path (the flag was
    // inserted by the spawn site before this task started).
    let _registry_guard = RegistryGuard {
        registry: cancel_registry,
        download_id,
    };

    // 1. Parse the link.
    let link = Ed2kLink::parse(link_str).with_context(|| format!("parse ed2k link: {link_str}"))?;
    info!(dl = download_id, name = %link.name, size = link.size, hash = %link.hash, "Starting eMule download");

    // 2. Bootstrap if the routing table is thin.
    let contact_count = kad.contact_count().await;
    if contact_count < 4 {
        info!(
            dl = download_id,
            contact_count, "Routing table thin — re-bootstrapping from cached/bootstrap contacts"
        );
        let seeds = load_kad_seeds(config, 200);
        if seeds.is_empty() {
            let msg = "No Kad2 seeds available (download nodes.dat first)";
            let _ =
                crate::db::emule_downloads::set_status(db, download_id, "error", Some(msg)).await;
            anyhow::bail!("{msg}");
        }
        let after = kad.bootstrap(seeds).await;
        info!(dl = download_id, contacts = after, "Kad2 re-bootstrap done");
    } else {
        // Per-download check of the shared routing table — at info this prints
        // once per concurrent download (all identical). The single
        // "Kad2 initial bootstrap done" already reports readiness.
        debug!(
            dl = download_id,
            contact_count, "Kad2 routing table ready, skipping bootstrap"
        );
    }

    // Create the temp directory and paths once — they never change.
    let emule_temp = &config.emule.temp_dir;
    std::fs::create_dir_all(emule_temp)
        .with_context(|| format!("create emule temp dir: {}", emule_temp.display()))?;
    let (part_path, met_path) = part_paths(config, link.hash.as_bytes());

    // Number of ed2k slices (one per CHUNK_SIZE block).
    let num_slices = link.size.div_ceil(CHUNK_SIZE as u64) as usize;
    // A single-part file (file_size <= CHUNK_SIZE) has no hashset: its ed2k hash
    // IS the MD4 of that one part, so we can verify directly without asking a
    // peer. Multi-part files must obtain (and verify) the hashset from a source
    // before any data is accepted.
    let single_part = num_slices <= 1;

    // Live-stats map key: eMule downloads use negative ids (see the API).
    let live_key = -download_id;

    // Register this file for partial upload serving so eMule peers can fetch
    // already-completed slices from us, building credit on the network.
    let hash_key = *link.hash.as_bytes();
    active_downloads.write().await.insert(
        hash_key,
        UploadInfo {
            name: link.name.clone(),
            total_size: link.size,
            num_slices,
            // While downloading we serve the partial slices from the .part file.
            path: part_path.clone(),
            complete: false,
            // No hashset yet — we serve it only once the file is complete.
            hashset: Vec::new(),
        },
    );

    // A download slot (`max_concurrent_downloads`) represents *actively
    // downloading*, not merely being in this loop. We claim it only once we
    // have sources and are about to transfer (see below), so downloads stuck in
    // `finding_providers` never starve ones that do have sources. It is held
    // with hysteresis: kept across short re-search rounds and released only when
    // the download falls back to `stalled` (or pauses / finishes, when this
    // function returns and the permit drops).
    let mut slot: Option<tokio::sync::OwnedSemaphorePermit> = None;

    // How long to reuse a source cache before querying Kad2 again.
    // eMule's own re-ask interval is 30 minutes; we match it to avoid
    // hammering the network with repeated source requests for the same hash.
    const SOURCE_CACHE_SECS: u64 = 30 * 60;

    // 3 + 4. Main retry loop: search → try peers → if all fail, search again.
    let mut cached_sources: Vec<KadSource> = Vec::new();
    let mut last_search_at: Option<Instant> = None;
    let mut retry_count: u32 = 0;

    // Our persistent eMule user hash, advertised in the download HELLO so a peer
    // sees the same identity whether it uploads to or downloads from us. Stored
    // on disk next to identity.key (see crate::emule_identity), not in the DB.
    let our_user_hash = crate::emule_identity::load_or_create(&crate::emule_identity::path(config))
        .unwrap_or([0u8; 16]);
    let our_nick = config.emule.nick.clone();

    loop {
        // Check for user-requested stop (cancel / pause) before doing any work.
        // The in-memory `cancel` flag makes the round abort promptly; the DB
        // status tells us *why* (pause keeps the partial file, cancel discards
        // it). Re-read the DB even when only the flag is set, to classify.
        if cancel.load(Ordering::Relaxed) || stop_reason(db, download_id).await.is_some() {
            let reason = stop_reason(db, download_id)
                .await
                .unwrap_or_else(|| "cancelled".to_string());
            info!(dl = download_id, status = %reason, "eMule download stopped by user");
            cleanup_on_stop(
                &reason,
                &part_path,
                &met_path,
                active_downloads,
                &hash_key,
                live_stats,
                live_key,
            )
            .await;
            return Ok(());
        }

        // Determine which slices have already been downloaded by consulting
        // the .part.met progress file.  Completed slices are skipped.
        let done_slices = load_progress(&met_path, num_slices);
        let done_count = done_slices.iter().filter(|&&d| d).count();
        let bytes_done: u64 = done_slices
            .iter()
            .enumerate()
            .filter(|&(_, &d)| d)
            .map(|(i, _)| {
                let start = i as u64 * CHUNK_SIZE as u64;
                (start + CHUNK_SIZE as u64).min(link.size) - start
            })
            .sum();

        if bytes_done > 0 {
            info!(
                dl = download_id,
                bytes_done, "Resuming from previous progress"
            );
            let _ = crate::db::emule_downloads::set_bytes_done(db, download_id, bytes_done).await;
        }

        // --- Search for sources (skip if cache is still fresh) ---
        let cache_age_secs = last_search_at.map_or(u64::MAX, |t| t.elapsed().as_secs());
        let needs_search = cached_sources.is_empty() || cache_age_secs >= SOURCE_CACHE_SECS;

        if needs_search {
            let _ = crate::db::emule_downloads::set_status_if_running(
                db,
                download_id,
                "finding_providers",
            )
            .await;
            info!(dl = download_id, "Searching Kad2 for sources");
            // Race the search against a pause/cancel: if the user stops the
            // download while it's queued for or running a Kad search, abandon
            // the search (dropping the future leaves the gate's queue / releases
            // its turn) instead of blocking until the lookup finishes.
            let fresh = tokio::select! {
                res = kad.search_sources(link.hash, link.size) => res,
                reason = wait_for_stop(db, download_id) => {
                    info!(dl = download_id, status = %reason, "stopped while searching for sources");
                    cleanup_on_stop(&reason, &part_path, &met_path, active_downloads, &hash_key, live_stats, live_key).await;
                    return Ok(());
                }
            };
            // Merge new peers into the cache — deduplicate by (IP, tcp_port).
            for s in fresh {
                if !cached_sources
                    .iter()
                    .any(|c| c.ip == s.ip && c.tcp_port == s.tcp_port)
                {
                    cached_sources.push(s);
                }
            }
            last_search_at = Some(Instant::now());
        } else {
            info!(
                dl = download_id,
                count = cached_sources.len(),
                cache_age_secs,
                "Reusing cached sources from previous round"
            );
        }

        if cached_sources.is_empty() {
            // After several empty rounds, surface the download as `stalled` so
            // the user can tell it is stuck (no sources) rather than just
            // starting up.  Keep retrying regardless.
            let status = if retry_count >= STALL_AFTER_ROUNDS {
                "stalled"
            } else {
                "finding_providers"
            };
            // Release the slot once stalled so a download with sources can take
            // it; within the hysteresis window we keep it across empty rounds.
            if status == "stalled" {
                slot = None;
            }
            let _ =
                crate::db::emule_downloads::set_status_if_running(db, download_id, status).await;
            {
                let mut s = live_stats.write().await;
                let e = s.entry(live_key).or_default();
                e.sources_total = cached_sources.len() as u32;
                e.sources_active = 0;
                e.pieces_in_flight = 0;
            }
            let delay = retry_delay_secs(retry_count);
            retry_count += 1;
            info!(dl = download_id,
                hash = %link.hash,
                retry_in_secs = delay,
                status,
                "No Kad2 sources found — will retry"
            );
            sleep_or_cancel(delay, &cancel).await;
            continue;
        }
        info!(
            dl = download_id,
            count = cached_sources.len(),
            "Proceeding with eMule sources"
        );

        let sources = cached_sources.clone();

        // We have sources and are about to transfer — claim a download slot now
        // (not earlier, so searching never consumes one). If all slots are busy,
        // park as `queued` with the sources cached until one frees up.
        if slot.is_none() {
            if download_slots.available_permits() == 0 {
                info!(
                    dl = download_id,
                    "Have sources — waiting for a download slot (queued)"
                );
                let _ =
                    crate::db::emule_downloads::set_status_if_running(db, download_id, "queued")
                        .await;
            }
            match download_slots.clone().acquire_owned().await {
                Ok(permit) => slot = Some(permit),
                Err(_) => {
                    // Semaphore closed — daemon shutting down. Keep the .part for
                    // resume (not a cancel), just drop the in-memory entries.
                    cleanup_on_stop(
                        "shutdown",
                        &part_path,
                        &met_path,
                        active_downloads,
                        &hash_key,
                        live_stats,
                        live_key,
                    )
                    .await;
                    return Ok(());
                }
            }
            // The user may have paused/cancelled while we waited for the slot.
            if let Some(reason) = stop_reason(db, download_id).await {
                info!(dl = download_id, status = %reason, "stopped while waiting for a slot");
                cleanup_on_stop(
                    &reason,
                    &part_path,
                    &met_path,
                    active_downloads,
                    &hash_key,
                    live_stats,
                    live_key,
                )
                .await;
                return Ok(());
            }
        }

        // --- Attempt parallel download from discovered sources ---
        let _ =
            crate::db::emule_downloads::set_status_if_running(db, download_id, "downloading").await;

        // All slices complete already (shouldn't happen since the file would
        // have been renamed, but be robust).
        if done_count == num_slices {
            info!(dl = download_id, "All slices already complete");
            break;
        }

        // Build work queue from incomplete slices.
        let remaining: VecDeque<(usize, u64, u64)> = done_slices
            .iter()
            .enumerate()
            .filter(|&(_, &d)| !d)
            .map(|(i, _)| {
                let start = i as u64 * CHUNK_SIZE as u64;
                let end = (start + CHUNK_SIZE as u64).min(link.size);
                (i, start, end)
            })
            .collect();
        let num_remaining = remaining.len();

        let work_queue = Arc::new(Mutex::new(remaining));
        let done_vec = Arc::new(Mutex::new(done_slices));

        // Per-part MD4 hashes used to verify each downloaded slice. For a
        // single-part file the ed2k hash itself is the only part hash; for a
        // multi-part file we start empty and the first source that returns a
        // hashset reproducing the ed2k root (verified with `verify_part_hashes`)
        // fills it for the whole round. Shared so every worker verifies against
        // the same trusted set.
        let part_hashes: Arc<Mutex<Option<Vec<[u8; 16]>>>> = Arc::new(Mutex::new(if single_part {
            Some(vec![*link.hash.as_bytes()])
        } else {
            None
        }));

        // Coherent, shared progress across workers (see ProgressState). Seeded
        // from the slices already on disk so the running total starts correct.
        let progress = {
            let mut st = ProgressState {
                per_slice: vec![0u64; num_slices],
                total: 0,
            };
            for (i, &done_flag) in done_vec.lock().unwrap().iter().enumerate() {
                if done_flag {
                    let s = i as u64 * CHUNK_SIZE as u64;
                    let len = (s + CHUNK_SIZE as u64).min(link.size) - s;
                    st.per_slice[i] = len;
                    st.total += len;
                }
            }
            Arc::new(Mutex::new(st))
        };

        // Filter out sources with unusable addresses.
        let valid_sources: Vec<_> = sources
            .iter()
            .filter(|s| s.tcp_port != 0 && !s.ip.is_unspecified())
            .cloned()
            .collect();

        let max_workers = config
            .emule
            .download_slots_per_file
            .clamp(1, 50)
            .min(valid_sources.len())
            .min(num_remaining);

        info!(
            dl = download_id,
            workers = max_workers,
            remaining_slices = num_remaining,
            sources = valid_sources.len(),
            "Starting parallel eMule download"
        );

        // Publish live stats for this round: each worker pulls one slice at a
        // time, so active sources ≈ pieces in flight ≈ worker count.
        {
            let mut s = live_stats.write().await;
            let e = s.entry(live_key).or_default();
            e.sources_total = valid_sources.len() as u32;
            e.sources_active = max_workers as u32;
            e.pieces_in_flight = max_workers as u32;
        }

        // Sources that currently have us waiting in their upload queue, keyed by
        // peer address → queue rank. A worker inserts its rank on OP_QUEUE_RANK
        // and clears it once the connect attempt resolves (slot granted or moved
        // on). The in-flight publisher folds this into live stats so the UI can
        // explain a download that keeps trying but isn't transferring.
        let queue_ranks: Arc<Mutex<HashMap<std::net::SocketAddrV4, u32>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Publisher task: derive the in-flight slice indices for this round as
        // (all slices) − (done) − (still queued). A worker only ever holds one
        // slice outside the queue at a time, so anything neither done nor queued
        // is being fetched right now. This avoids instrumenting every worker
        // exit path. Aborted once the round's workers finish.
        let in_flight_publisher = {
            let pub_work = work_queue.clone();
            let pub_done = done_vec.clone();
            let pub_ls = live_stats.clone();
            let pub_qranks = queue_ranks.clone();
            let pub_progress = progress.clone();
            let pub_key = live_key;
            let total = num_slices;
            let pub_file_size = link.size;
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let queued: std::collections::HashSet<usize> = pub_work
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(i, _, _)| *i)
                        .collect();
                    let done = pub_done.lock().unwrap().clone();
                    let in_flight: Vec<u32> = (0..total)
                        .filter(|i| !done.get(*i).copied().unwrap_or(false) && !queued.contains(i))
                        .map(|i| i as u32)
                        .collect();
                    // Progress is the sum of *confirmed* slices (those written to
                    // .part.met), not in-flight partials. This keeps the bar
                    // monotonic: a slice in flight that fails and is re-fetched
                    // never makes the percentage go backwards. Live speed is
                    // tracked separately, and in-flight pieces are shown above.
                    let confirmed_bytes: u64 = done
                        .iter()
                        .enumerate()
                        .filter(|&(_, &d)| d)
                        .map(|(i, _)| {
                            let s = i as u64 * CHUNK_SIZE as u64;
                            (s + CHUNK_SIZE as u64).min(pub_file_size) - s
                        })
                        .sum();
                    // Partial-aware live byte count (confirmed slices + in-flight
                    // partials) — used only to derive a smooth speed, never for
                    // the progress bar.
                    let received_live = pub_progress.lock().unwrap().total;
                    let (queued_sources, best_rank) = {
                        let qr = pub_qranks.lock().unwrap();
                        (qr.len() as u32, qr.values().copied().min())
                    };
                    let mut s = pub_ls.write().await;
                    match s.get_mut(&pub_key) {
                        Some(e) => {
                            e.in_flight_pieces = in_flight;
                            // Confirmed-slice progress (monotonic), not in-flight
                            // partials — so the bar never retreats when a slice
                            // fails and is retried.
                            e.bytes_done = Some(confirmed_bytes);
                            // Speed is derived from this partial-aware figure so it
                            // stays smooth instead of lurching one slice at a time.
                            e.received_live = Some(received_live);
                            e.queued_sources = queued_sources;
                            e.best_queue_rank = best_rank;
                        }
                        // Entry gone — download finished/cancelled, stop.
                        None => break,
                    }
                }
            })
        };

        let mut join_set: JoinSet<()> = JoinSet::new();

        let our_tcp_port = config.emule.tcp_port;

        // Minimum sustained per-source speed (bytes/sec). Only enforced when
        // there is more than one source: dropping a slow peer only helps if
        // another can pick up its slices, so with a single source we keep it
        // (a slow transfer beats none) by leaving the check disabled.
        let min_speed_bytes: u64 = if valid_sources.len() > 1 {
            config.emule.min_source_speed_kib_s as u64 * 1024
        } else {
            0
        };

        // Shared pool of every usable source, each paired with a per-round
        // failure count. Workers are NOT pinned to a single peer: a worker keeps
        // its source while it keeps serving slices, but the moment that peer
        // fails to connect or queues us past patience, the worker returns it to
        // the back of the pool (until MAX_SOURCE_ATTEMPTS) and pulls the next
        // one. This cycles through all discovered sources — and keeps knocking
        // on peers that are momentarily full — instead of freezing a worker on,
        // or permanently dropping, the first few sources.
        let source_pool: Arc<Mutex<VecDeque<(KadSource, u32)>>> = Arc::new(Mutex::new(
            valid_sources.into_iter().map(|s| (s, 0)).collect(),
        ));

        for _ in 0..max_workers {
            let work = work_queue.clone();
            let done = done_vec.clone();
            let met = met_path.clone();
            let part = part_path.clone();
            let db_w = db.clone();
            let hash = link.hash;
            let file_size = link.size;
            let metrics_w = metrics.clone();
            let progress_w = progress.clone();
            let throttle_w = download_throttle.clone();
            let cancel_w = cancel.clone();
            let pool = source_pool.clone();
            let qranks = queue_ranks.clone();
            let nick_w = our_nick.clone();
            let min_speed = min_speed_bytes;
            let part_hashes_w = part_hashes.clone();

            join_set.spawn(async move {
                // Work one source at a time: connect once, then download as many
                // slices as it will serve over that single connection before
                // moving on. The connection (and its granted upload slot) is
                // reused across slices instead of reconnecting per slice.
                'sources: loop {
                    if cancel_w.load(Ordering::Relaxed) {
                        break;
                    }
                    // Take the next source from the shared pool.
                    let (source, attempts) = match pool.lock().unwrap().pop_front() {
                        Some(pair) => pair,
                        None => break, // no sources left — worker done
                    };
                    let peer = std::net::SocketAddrV4::new(source.ip, source.tcp_port);
                    let opts = DownloadOptions {
                        timeout: Duration::from_secs(3600),
                        op_timeout: Duration::from_secs(30),
                        // Short queue wait per knock (~10 s): when a peer's slots
                        // are full we move on and re-knock it later via the pool.
                        max_queue_waits: 2,
                        file_size,
                        hash,
                        start_offset: 0,
                        peer_hash: Some(source.user_hash),
                        our_tcp_port,
                        our_user_hash,
                        our_nick: nick_w.clone(),
                        min_speed_bytes_per_sec: min_speed,
                        download_id,
                    };

                    // Connect once: HELLO + file request + upload-slot wait.
                    // Connected and Queued are transient (Connected even fires
                    // twice per attempt — plain then the obfuscated retry), so
                    // they stay at debug; only an actual transfer start is info.
                    let qr_cb = qranks.clone();
                    let mut on_connect = move |ev: DownloadEvent| match ev {
                        DownloadEvent::Connected => {
                            debug!(dl = download_id, %peer, "Connected to eMule peer")
                        }
                        DownloadEvent::Queued { rank } => {
                            // Record our queue position at this peer so the UI can
                            // show "queued at N sources (best rank M)".
                            qr_cb.lock().unwrap().insert(peer, rank);
                            debug!(dl = download_id, %peer, rank, "Queued at eMule peer")
                        }
                        DownloadEvent::Started => {
                            qr_cb.lock().unwrap().remove(&peer);
                            debug!(dl = download_id, %peer, "Peer granted upload slot")
                        }
                        _ => {}
                    };
                    let connect_result = Session::connect(peer, &opts, &mut on_connect).await;
                    // The attempt resolved (slot granted or giving up): we are no
                    // longer waiting in this peer's queue either way.
                    qranks.lock().unwrap().remove(&peer);
                    let mut session = match connect_result {
                        Ok(s) => {
                            info!(
                                dl = download_id, %peer,
                                obfuscated = s.is_obfuscated(),
                                "Peer granted upload slot — transfer starting"
                            );
                            s
                        }
                        Err(e) => {
                            debug!(dl = download_id, %peer, error = %e,
                                "Peer unavailable — trying another source");
                            // It may be momentarily full — retry later unless exhausted.
                            if attempts + 1 < MAX_SOURCE_ATTEMPTS {
                                pool.lock().unwrap().push_back((source, attempts + 1));
                            }
                            continue 'sources;
                        }
                    };

                    // Multi-part files: before accepting any data we need the
                    // verified per-part hashset. If nobody has it yet, ask this
                    // peer. We only accept a hashset that reproduces the file's
                    // ed2k root hash (verify_part_hashes), so a lying peer cannot
                    // poison verification for the whole round.
                    if !single_part && part_hashes_w.lock().unwrap().is_none() {
                        match session.request_hashset().await {
                            Ok(hs)
                                if hs.len() >= num_slices
                                    && rucio_emule::ed2k::verify_part_hashes(&hs, &hash) =>
                            {
                                let mut g = part_hashes_w.lock().unwrap();
                                if g.is_none() {
                                    *g = Some(hs);
                                }
                                info!(dl = download_id, %peer, "Obtained and verified ed2k hashset");
                            }
                            Ok(hs) if hs.len() < num_slices => {
                                debug!(dl = download_id, %peer, got = hs.len(), need = num_slices, "Peer returned a short hashset")
                            }
                            Ok(_) => {
                                debug!(dl = download_id, %peer, "Peer's hashset failed ed2k verification")
                            }
                            Err(e) => {
                                debug!(dl = download_id, %peer, error = %e, "Hashset request failed")
                            }
                        }
                    }
                    // Still no verified hashset (this peer did not provide one and
                    // no other worker has yet): we cannot verify integrity against
                    // this peer, so return it to the pool and try another source
                    // rather than download unverifiable data.
                    if !single_part && part_hashes_w.lock().unwrap().is_none() {
                        if attempts + 1 < MAX_SOURCE_ATTEMPTS {
                            pool.lock().unwrap().push_back((source, attempts + 1));
                        }
                        continue 'sources;
                    }

                    // Download slices over this one connection until the queue is
                    // empty, we're cancelled, or the session breaks.
                    loop {
                        if cancel_w.load(Ordering::Relaxed) {
                            break 'sources;
                        }
                        let (slice_idx, slice_start, slice_end) =
                            match work.lock().unwrap().pop_front() {
                                Some(s) => s,
                                None => break 'sources, // all slices taken — worker done
                            };

                        // Open the part file. `download_range` seeks to each
                        // block's absolute offset as it writes, so no initial
                        // positioning is needed here.
                        let mut file = match tokio::fs::OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(false)
                            .open(&part)
                            .await
                        {
                            Ok(f) => f,
                            Err(e) => {
                                // Filesystem error, not the peer's fault — return
                                // the slice and stop this worker.
                                warn!(dl = download_id, %peer, slice = slice_idx, error = %e, "Failed to open part file");
                                work.lock().unwrap().push_front((slice_idx, slice_start, slice_end));
                                break 'sources;
                            }
                        };

                        // Update the shared ProgressState so every in-flight slice
                        // is reflected at once. The in-flight publisher mirrors the
                        // running total into live_stats once a second, and the main
                        // loop's ws_tick is the sole emitter of DownloadProgress —
                        // keeping a single, monotonic source of the byte count
                        // instead of competing with the persisted (DB) figure.
                        let metrics_cb = metrics_w.clone();
                        let progress_cb = progress_w.clone();
                        let mut on_progress = move |ev: DownloadEvent| match ev {
                            DownloadEvent::Progress { bytes_received, .. } => {
                                // bytes_received is an absolute file offset; subtract
                                // slice_start for the bytes fetched within this slice.
                                let cur = bytes_received.saturating_sub(slice_start);
                                let delta = {
                                    let mut p = progress_cb.lock().unwrap();
                                    let prev = p.per_slice[slice_idx];
                                    let d = if cur >= prev {
                                        let d = cur - prev;
                                        p.total += d;
                                        d
                                    } else {
                                        p.total -= prev - cur;
                                        0
                                    };
                                    p.per_slice[slice_idx] = cur;
                                    d
                                };
                                // Feed the speed window incrementally so the session
                                // rate stays live instead of spiking once per slice.
                                metrics_cb.record_download_bytes(delta);
                            }
                            DownloadEvent::ChunkFailed { part_index } => {
                                warn!(dl = download_id, part_index, "eMule chunk verification failed");
                                metrics_cb.record_rejected();
                            }
                            _ => {}
                        };

                        // Expected MD4 for this slice. For single-part this is the
                        // ed2k hash; for multi-part it is the verified part hash
                        // (always Some here — we bailed above without a hashset).
                        let expected = part_hashes_w
                            .lock()
                            .unwrap()
                            .as_ref()
                            .and_then(|h| h.get(slice_idx).copied());

                        match session
                            .download_range(
                                slice_start,
                                slice_end,
                                expected,
                                &mut file,
                                &mut on_progress,
                            )
                            .await
                        {
                            Ok(_) => {
                                info!(dl = download_id, %peer, slice = slice_idx, "Slice downloaded successfully");
                                // Mark slice as done and persist progress.
                                let snapshot = {
                                    let mut d = done.lock().unwrap();
                                    d[slice_idx] = true;
                                    d.clone()
                                };
                                // Reconcile the shared total to the exact slice
                                // length — the last Progress event may have stopped
                                // short of the slice end — and account that tail in
                                // the session metrics plus the completed chunk.
                                let remainder = {
                                    let slice_len = slice_end - slice_start;
                                    let mut p = progress_w.lock().unwrap();
                                    let prev = p.per_slice[slice_idx];
                                    let rem = slice_len - prev;
                                    p.total += rem;
                                    p.per_slice[slice_idx] = slice_len;
                                    rem
                                };
                                metrics_w.record_download_bytes(remainder);
                                metrics_w.record_download_chunk();
                                // Charge this slice against the download cap. With
                                // the cap off this is instant; otherwise the worker
                                // waits here before fetching its next slice, which
                                // bounds the aggregate download rate. Low priority:
                                // Rucio downloads win the shared cap over eMule.
                                throttle_w
                                    .acquire(
                                        slice_end - slice_start,
                                        crate::throttle::Priority::Low,
                                    )
                                    .await;
                                save_progress(&met, &snapshot);
                                // Update DB with the true cumulative total so it
                                // never regresses when slices are downloaded out of
                                // file order.
                                let cumulative: u64 = snapshot
                                    .iter()
                                    .enumerate()
                                    .filter(|&(_, &d)| d)
                                    .map(|(i, _)| {
                                        let s = i as u64 * CHUNK_SIZE as u64;
                                        (s + CHUNK_SIZE as u64).min(file_size) - s
                                    })
                                    .sum();
                                let db_upd = db_w.clone();
                                tokio::spawn(async move {
                                    let _ = crate::db::emule_downloads::set_bytes_done(
                                        &db_upd,
                                        download_id,
                                        cumulative,
                                    )
                                    .await;
                                });
                                // Keep the session; fetch the next slice over it.
                            }
                            Err(e) if e.is::<rucio_emule::transfer::ChunkHashMismatch>() => {
                                // A completed part whose MD4 doesn't match the
                                // hashset — corrupt data (bad disk on the peer, a
                                // malicious peer, or a rare glitch). Treat it as a
                                // recoverable failure rather than dropping the
                                // source outright: roll back the slice, requeue it,
                                // and give the source another chance (attempts+1).
                                // A source that keeps serving corrupt parts exhausts
                                // MAX_SOURCE_ATTEMPTS and is then dropped on its own.
                                warn!(dl = download_id, %peer, slice = slice_idx,
                                    attempt = attempts + 1,
                                    "Part failed MD4 verification — corrupt data, will retry");
                                {
                                    let mut p = progress_w.lock().unwrap();
                                    let prev = p.per_slice[slice_idx];
                                    p.total -= prev;
                                    p.per_slice[slice_idx] = 0;
                                }
                                work.lock().unwrap().push_front((slice_idx, slice_start, slice_end));
                                if attempts + 1 < MAX_SOURCE_ATTEMPTS {
                                    pool.lock().unwrap().push_back((source, attempts + 1));
                                }
                                continue 'sources;
                            }
                            Err(e) => {
                                let too_slow =
                                    e.is::<rucio_emule::transfer::SlowPeer>();
                                // Roll back this slice's partial progress: it will
                                // be re-fetched from the start, so its bytes must
                                // not linger in the shared total.
                                {
                                    let mut p = progress_w.lock().unwrap();
                                    let prev = p.per_slice[slice_idx];
                                    p.total -= prev;
                                    p.per_slice[slice_idx] = 0;
                                }
                                // Return the slice for any worker to pick up.
                                work.lock().unwrap().push_front((slice_idx, slice_start, slice_end));
                                if too_slow {
                                    // Deliberately drop this source: another one
                                    // will serve the slice faster. (Only reached
                                    // when >1 source exists — see `min_speed_bytes`.)
                                    info!(dl = download_id, %peer, error = %e,
                                        "Source too slow — dropped in favour of another");
                                } else {
                                    debug!(dl = download_id, %peer, slice = slice_idx, error = %e,
                                        "Slice download failed — dropping connection, will retry");
                                    // The connection is broken; the source may have just
                                    // glitched, so retry it later unless exhausted, then
                                    // reconnect (to it or another source).
                                    if attempts + 1 < MAX_SOURCE_ATTEMPTS {
                                        pool.lock().unwrap().push_back((source, attempts + 1));
                                    }
                                }
                                continue 'sources;
                            }
                        }
                    }
                }
            });
        }

        // Wait for all workers to finish, but abort them promptly if the user
        // pauses/cancels mid-round. Aborting mid-slice is safe: an unfinished
        // slice is never marked done in .part.met, so it is simply re-fetched.
        loop {
            tokio::select! {
                res = join_set.join_next() => {
                    if res.is_none() {
                        break;
                    }
                }
                _ = async {
                    while !cancel.load(Ordering::Relaxed) {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                } => {
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    break;
                }
            }
        }
        // Round over: stop deriving in-flight indices and clear the stale set.
        in_flight_publisher.abort();
        {
            let mut s = live_stats.write().await;
            if let Some(e) = s.get_mut(&live_key) {
                e.in_flight_pieces = Vec::new();
            }
        }

        // If we aborted due to a pause/cancel, go straight back to the top so
        // the stop is handled (and partial files cleaned, on cancel) now —
        // don't fall through into the retry/backoff sleep first.
        if cancel.load(Ordering::Relaxed) {
            continue;
        }

        // Multi-part file and no source in the whole round provided a hashset
        // that reproduces the ed2k root hash: we can't verify integrity yet, so
        // we can't accept any data. Failing the download here is too harsh —
        // re-adding it is costly for the user, and the usual cause is simply
        // that none of the current sources hold the file. Fall through to the
        // retry path below, which clears the source cache, triggers a fresh Kad
        // search and tries again with backoff, exactly like a download that
        // hasn't found sources yet. (`done_count` is 0 in this case, so the
        // `!all_done` branch handles it; the loop is bounded only by the user
        // pausing or cancelling.)
        if !single_part && part_hashes.lock().unwrap().is_none() {
            info!(
                dl = download_id,
                "No source provided a valid ed2k hashset yet — searching for more sources and retrying"
            );
        }

        // Check if all slices are now done (drop guard before any await).
        let (done_count_after, all_done) = {
            let g = done_vec.lock().unwrap();
            (g.iter().filter(|&&d| d).count(), g.iter().all(|&d| d))
        };

        if !all_done {
            let new_slices = done_count_after.saturating_sub(done_count);
            if new_slices > 0 {
                // Progress made — sources are reachable; reset backoff and
                // keep the cache so the next round skips the 60 s Kad search.
                retry_count = 0;
            } else {
                // No progress at all — cached sources are likely all dead.
                // Clear them so the next round triggers a fresh Kad2 search.
                cached_sources.clear();
                last_search_at = None;
            }
            // Mark as stalled once enough rounds pass without progress.
            let status = if retry_count >= STALL_AFTER_ROUNDS {
                "stalled"
            } else {
                "finding_providers"
            };
            // Release the slot once stalled (hysteresis: kept during the window).
            if status == "stalled" {
                slot = None;
            }
            let _ =
                crate::db::emule_downloads::set_status_if_running(db, download_id, status).await;
            let delay = retry_delay_secs(retry_count);
            retry_count += 1;
            info!(dl = download_id,
                hash = %link.hash,
                new_slices,
                retry_in_secs = delay,
                status,
                "Not all slices complete — retrying"
            );
            sleep_or_cancel(delay, &cancel).await;
            continue;
        }

        break; // all slices done, proceed to finalise
    }

    // --- Download succeeded: move to final destination and compute BLAKE3 ---
    // Re-read the (possibly renamed) name from the DB so a rename applied while
    // the download was in progress takes effect; fall back to the link name.
    let final_name = match crate::db::emule_downloads::get(db, download_id).await {
        Ok(Some(r)) if !r.name.trim().is_empty() => r.name,
        _ => link.name.clone(),
    };
    // Resolve where this download lands: its category's pinned dir if it has one,
    // else the global download_dir. Resolved now (not at start) so a category
    // edited/deleted mid-download is honoured.
    let cat_id = crate::db::emule_downloads::get_category_id(db, download_id)
        .await
        .ok()
        .flatten();
    let dest_dir =
        crate::db::categories::resolve_dir(db, &config.storage.download_dir, cat_id).await;
    let final_path = dest_dir.join(&final_name);
    // Persist the finished .part into the download dir. The user may have deleted
    // that dir or revoked write access while we downloaded, so recreate it first
    // and treat any remaining failure as recoverable: keep the .part + .part.met
    // intact, mark the download errored (never a phantom "complete"), notify, and
    // bail so it can be retried once the folder is fixed.
    let saved = async {
        std::fs::create_dir_all(&dest_dir)?;
        crate::fsutil::move_file(&part_path, &final_path).await
    }
    .await;
    if let Err(e) = saved {
        let reason = format!("Couldn't save to {}: {e}", dest_dir.display());
        warn!(
            dl = download_id,
            part = %part_path.display(),
            "eMule download finished but could not be saved (keeping .part): {e}"
        );
        let _ =
            crate::db::emule_downloads::set_status(db, download_id, "error", Some(&reason)).await;
        notifier
            .notify(
                rucio_core::api::notifications::NotificationKind::Download,
                "Couldn't save download",
                format!(
                    "{final_name}: the download folder is missing or not writable — fix it and retry"
                ),
                None,
            )
            .await;
        anyhow::bail!("save completed eMule download: {e}");
    }
    // Clean up progress file.
    let _ = tokio::fs::remove_file(&met_path).await;

    // Single pass over the finished file to compute the ed2k per-chunk MD4
    // hashes (for the Kad hashset we serve to eMule peers). The Rucio identity
    // is *not* a flat BLAKE3 of the file — it is the canonical merkle-flat root
    // hash, computed separately by `index_file` below — so we no longer hash
    // BLAKE3 here.
    let path_clone = final_path.clone();
    let chunk_hashes = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<[u8; 16]>> {
        use md4::{Digest, Md4};
        use std::io::Read;
        let mut file = std::fs::File::open(&path_clone)?;
        let mut chunk_hashes: Vec<[u8; 16]> = Vec::new();
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            // Fill a full CHUNK_SIZE block (or up to EOF).
            let mut filled = 0usize;
            while filled < CHUNK_SIZE {
                match file.read(&mut buf[filled..])? {
                    0 => break,
                    n => filled += n,
                }
            }
            if filled == 0 {
                break;
            }
            chunk_hashes.push(Md4::digest(&buf[..filled]).into());
            if filled < CHUNK_SIZE {
                break;
            }
        }
        Ok(chunk_hashes)
    })
    .await
    .context("spawn_blocking for ed2k chunk hashing")?
    .with_context(|| format!("hashing {}", final_path.display()))?;

    // ed2k hashset to serve on OP_HASHSETREQUEST (empty for single-part files or
    // if no convention reproduces the known ed2k hash — then we serve none).
    let hashset = rucio_emule::ed2k::finalize_hashset(&chunk_hashes, link.size, &link.hash);

    let _ = crate::db::emule_downloads::set_completed(
        db,
        download_id,
        final_path.to_string_lossy().as_ref(),
    )
    .await;

    // Index the finished file into the Rucio share so it is announced to the
    // libp2p DHT immediately (rather than only after a restart's reconcile sees
    // it as "added"). This re-reads the file to compute the canonical
    // merkle-flat root hash — the real Rucio id — which we then report.
    let rucio_root_hex = match crate::api::shares::index_file(db, &final_path).await {
        Ok(root_hash) => {
            // Announce to the DHT so the file is shared in real time.
            let _ = node_tx
                .send(crate::node::messages::NodeCmd::StartProviding(
                    root_hash.to_vec(),
                ))
                .await;
            let hex = hex::encode(root_hash);
            info!(
                dl = download_id,
                root_hash = %hex,
                "eMule download complete — indexed and sharing on Rucio"
            );
            Some(hex)
        }
        Err(e) => {
            warn!(
                dl = download_id,
                error = %e,
                "Failed to index completed download into Rucio share"
            );
            None
        }
    };

    notifier
        .notify(
            rucio_core::api::notifications::NotificationKind::Download,
            "Download complete",
            final_name.clone(),
            rucio_root_hex,
        )
        .await;

    // Keep seeding the finished file to the Kad network (good-citizen policy),
    // decoupled from the downloads list: record it as a shared file and repoint
    // the upload whitelist entry at the final file instead of removing it. It is
    // served until the file is modified/removed on disk (checked at startup and
    // by the filesystem watcher).
    let mtime = crate::api::shares::file_mtime_secs(&final_path);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if let Err(e) = crate::db::emule_shared_files::upsert(
        db,
        link.hash.as_bytes(),
        &final_name,
        link.size,
        final_path.to_string_lossy().as_ref(),
        mtime,
        &hashset,
        now,
    )
    .await
    {
        warn!(
            dl = download_id,
            "Failed to register completed file for sharing: {e}"
        );
    }
    if let Some(info) = active_downloads.write().await.get_mut(&hash_key) {
        info.name = final_name.clone();
        info.path = final_path.clone();
        info.complete = true;
        info.hashset = hashset;
    }
    live_stats.write().await.remove(&live_key);
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns the user-requested stop status (`"cancelled"` or `"paused"`) if the
/// download has been marked to stop in the DB, or `None` if it should keep
/// running.  The download loop checks this once per round and exits cleanly,
/// leaving the status untouched (it was set by the API handler).
async fn stop_reason(db: &Db, download_id: i64) -> Option<String> {
    match crate::db::emule_downloads::get_status(db, download_id).await {
        Ok(Some(s)) if s == "cancelled" || s == "paused" => Some(s),
        _ => None,
    }
}

/// Sleep for `secs`, returning early if the cancel flag is raised, so a
/// pause/cancel during the retry backoff is acted on promptly (the caller loops
/// back to its stop check) instead of after the full delay.
async fn sleep_or_cancel(secs: u64, cancel: &AtomicBool) {
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(secs)) => {}
        _ = async {
            while !cancel.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        } => {}
    }
}

/// Tear down a download that is stopping. Drops the upload-whitelist and
/// live-stats entries, and — only on `"cancelled"` (not a pause or a shutdown) —
/// deletes the partial files so a later re-add starts clean. Called from every
/// stop/return path so the `.part` cleanup can never be missed.
async fn cleanup_on_stop(
    reason: &str,
    part_path: &std::path::Path,
    met_path: &std::path::Path,
    active_downloads: &ActiveDownloads,
    hash_key: &[u8; 16],
    live_stats: &crate::live_stats::LiveStatsMap,
    live_key: i64,
) {
    if reason == "cancelled" {
        let _ = tokio::fs::remove_file(part_path).await;
        let _ = tokio::fs::remove_file(met_path).await;
    }
    active_downloads.write().await.remove(hash_key);
    live_stats.write().await.remove(&live_key);
}

/// Resolve once the download is paused/cancelled. Used to race against a Kad
/// source search (`select!`) so pausing abandons the search immediately — a
/// queued search leaves the gate's queue, an active one releases its turn (the
/// in-flight Kad lookup then expires on its own). Polls because stop is a DB
/// status change, not a push signal.
async fn wait_for_stop(db: &Db, download_id: i64) -> String {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Some(reason) = stop_reason(db, download_id).await {
            return reason;
        }
    }
}

/// Resolve the effective `nodes.dat` path: the configured value when present,
/// otherwise the platform default (`$XDG_DATA_HOME/rucio/nodes.dat`).
pub fn effective_nodes_dat_path(config: &crate::config::Config) -> std::path::PathBuf {
    config
        .storage
        .nodes_dat_path
        .clone()
        .unwrap_or_else(|| crate::config::default_data_dir().join("nodes.dat"))
}

/// Path of the routing-table cache written by the daemon itself.
pub fn kad_cache_path(config: &crate::config::Config) -> std::path::PathBuf {
    effective_nodes_dat_path(config).with_file_name("kad_cache.dat")
}

/// Save the current Kad2 routing table to `kad_cache.dat`.
pub async fn save_kad_cache(config: &crate::config::Config, kad: &KadHandle) {
    let bytes = kad.dump_nodes_dat().await;
    if bytes.is_empty() {
        return;
    }
    let path = kad_cache_path(config);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match std::fs::write(&path, &bytes) {
        Ok(()) => info!(path = %path.display(), "Saved Kad2 routing table cache"),
        Err(e) => {
            warn!(path = %path.display(), error = %e, "Failed to save Kad2 routing table cache")
        }
    }
}

/// Load seeds from `kad_cache.dat` and from `nodes.dat`, deduplicated.
pub fn load_kad_seeds(
    config: &crate::config::Config,
    limit: usize,
) -> Vec<rucio_emule::kad::packet::Contact> {
    use rucio_emule::kad::routing::parse_nodes_dat;
    use std::collections::HashSet;

    let mut seen: HashSet<std::net::SocketAddrV4> = HashSet::new();
    let mut contacts: Vec<rucio_emule::kad::packet::Contact> = Vec::new();

    let cache_path = kad_cache_path(config);
    if let Ok(bytes) = std::fs::read(&cache_path) {
        match parse_nodes_dat(&bytes) {
            Ok(cs) => {
                info!(count = cs.len(), path = %cache_path.display(), "Loaded Kad2 routing cache");
                for c in cs {
                    if seen.insert(c.socket_addr_udp()) {
                        contacts.push(c);
                    }
                }
            }
            Err(e) => {
                warn!(path = %cache_path.display(), error = %e, "Failed to parse kad_cache.dat")
            }
        }
    }

    if contacts.len() < limit {
        let nodes_dat_path = effective_nodes_dat_path(config);
        if let Ok(bytes) = std::fs::read(&nodes_dat_path) {
            match parse_nodes_dat(&bytes) {
                Ok(cs) => {
                    for c in cs {
                        if contacts.len() >= limit {
                            break;
                        }
                        if seen.insert(c.socket_addr_udp()) {
                            contacts.push(c);
                        }
                    }
                }
                Err(e) => {
                    warn!(path = %nodes_dat_path.display(), error = %e, "Failed to parse nodes.dat")
                }
            }
        }
    }

    contacts.truncate(limit);
    contacts
}

/// Download a fresh `nodes.dat` from `url` and save it to `path`.
pub async fn bootstrap_nodes_dat(path: &std::path::Path, url: &str) -> Result<usize> {
    use rucio_core::api::emule::EMULE_USER_AGENT;

    let client = reqwest::Client::builder()
        .user_agent(EMULE_USER_AGENT)
        .build()
        .context("building HTTP client")?;

    let bytes = client
        .get(url)
        .send()
        .await
        .context("HTTP GET nodes.dat")?
        .error_for_status()
        .context("nodes.dat server returned error status")?
        .bytes()
        .await
        .context("reading nodes.dat response body")?;

    let contacts =
        rucio_emule::kad::routing::parse_nodes_dat(&bytes).context("parsing nodes.dat")?;

    if contacts.is_empty() {
        anyhow::bail!("downloaded nodes.dat contains no Kad2 contacts");
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }
    std::fs::write(path, &bytes)
        .with_context(|| format!("writing nodes.dat to {}", path.display()))?;

    Ok(contacts.len())
}
