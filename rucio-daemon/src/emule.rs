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
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::AsyncSeekExt;
use tokio::net::UdpSocket;
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::db::Db;
use rucio_core::api::ws::WsEvent;

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
    let task_cfg = KadTaskConfig {
        tcp_port: config.emule.tcp_port,
        initial_external_ip: config
            .emule
            .external_ip
            .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED),
        ..KadTaskConfig::default()
    };

    let handle = rucio_emule::kad::task::spawn(Arc::new(socket), our_id, task_cfg);

    // Seed the routing table immediately from cached/bootstrap contacts so the
    // first download does not have to wait for an on-demand bootstrap.
    let seeds = load_kad_seeds(config, 200);
    if !seeds.is_empty() {
        info!(
            seeds = seeds.len(),
            "Bootstrapping Kad2 from cached/bootstrap contacts"
        );
        let count = handle.bootstrap(seeds).await;
        info!(contacts = count, "Kad2 initial bootstrap done");
    } else {
        info!("No Kad2 seeds available at startup (download nodes.dat first)");
    }

    Ok(handle)
}

/// Number of fruitless retry rounds (no sources / no progress) after which a
/// download is reported as `stalled`.  With the back-off below this is reached
/// after roughly 15 minutes.  The download keeps retrying regardless.
const STALL_AFTER_ROUNDS: u32 = 5;

/// Exponential back-off for source-search retries.
/// Sequence: 30 s, 60 s, 2 min, 4 min, 8 min, 16 min, 30 min (cap), …
fn retry_delay_secs(attempt: u32) -> u64 {
    const BASE: u64 = 30;
    const MAX: u64 = 30 * 60;
    // Cap the shift so we never overflow u64 before the min() clamps us.
    (BASE * (1_u64 << attempt.min(10))).min(MAX)
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
#[allow(clippy::too_many_arguments)]
pub async fn run_ed2k_download(
    link_str: &str,
    download_id: i64,
    config: &Arc<Config>,
    db: &Db,
    ws_tx: &broadcast::Sender<WsEvent>,
    kad: &KadHandle,
    active_downloads: &ActiveDownloads,
    download_slots: &Arc<Semaphore>,
    live_stats: &crate::live_stats::LiveStatsMap,
) -> Result<()> {
    // 1. Parse the link.
    let link = Ed2kLink::parse(link_str).with_context(|| format!("parse ed2k link: {link_str}"))?;
    info!(name = %link.name, size = link.size, hash = %link.hash, "Starting eMule download");

    // 2. Bootstrap if the routing table is thin.
    let contact_count = kad.contact_count().await;
    if contact_count < 4 {
        info!(
            contact_count,
            "Routing table thin — re-bootstrapping from cached/bootstrap contacts"
        );
        let seeds = load_kad_seeds(config, 200);
        if seeds.is_empty() {
            let msg = "No Kad2 seeds available (download nodes.dat first)";
            let _ =
                crate::db::emule_downloads::set_status(db, download_id, "error", Some(msg)).await;
            anyhow::bail!("{msg}");
        }
        let after = kad.bootstrap(seeds).await;
        info!(contacts = after, "Kad2 re-bootstrap done");
    } else {
        info!(
            contact_count,
            "Kad2 routing table ready, skipping bootstrap"
        );
    }

    // Create the temp directory and paths once — they never change.
    let emule_temp = &config.emule.temp_dir;
    std::fs::create_dir_all(emule_temp)
        .with_context(|| format!("create emule temp dir: {}", emule_temp.display()))?;
    let part_path = emule_temp.join(format!("{}.part", link.hash));
    let met_path = emule_temp.join(format!("{}.part.met", link.hash));
    let final_path = config.storage.download_dir.join(&link.name);

    // Number of ed2k slices (one per CHUNK_SIZE block).
    let num_slices = link.size.div_ceil(CHUNK_SIZE as u64) as usize;

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
        },
    );

    // Acquire a global download slot.  When all slots are busy the download
    // parks here in the `queued` state until a running download finishes,
    // capping the total number of open peer connections across all downloads.
    let _slot = match download_slots.clone().try_acquire_owned() {
        Ok(permit) => {
            info!(
                name = %link.name,
                max_concurrent = config.emule.max_concurrent_downloads,
                slots_free = download_slots.available_permits(),
                "eMule download slot acquired"
            );
            permit
        }
        Err(_) => {
            info!(name = %link.name, "All download slots busy — queued");
            let _ = crate::db::emule_downloads::set_status(db, download_id, "queued", None).await;
            match download_slots.clone().acquire_owned().await {
                Ok(permit) => {
                    info!(
                        name = %link.name,
                        max_concurrent = config.emule.max_concurrent_downloads,
                        slots_free = download_slots.available_permits(),
                        "eMule download slot acquired after queuing"
                    );
                    permit
                }
                Err(_) => {
                    // Semaphore closed — daemon shutting down.
                    active_downloads.write().await.remove(&hash_key);
                    live_stats.write().await.remove(&live_key);
                    return Ok(());
                }
            }
        }
    };

    // How long to reuse a source cache before querying Kad2 again.
    // eMule's own re-ask interval is 30 minutes; we match it to avoid
    // hammering the network with repeated source requests for the same hash.
    const SOURCE_CACHE_SECS: u64 = 30 * 60;

    // 3 + 4. Main retry loop: search → try peers → if all fail, search again.
    let mut cached_sources: Vec<KadSource> = Vec::new();
    let mut last_search_at: Option<Instant> = None;
    let mut retry_count: u32 = 0;
    loop {
        // Check for user cancellation before doing any work.
        if is_cancelled(db, download_id).await {
            info!(name = %link.name, "eMule download cancelled by user");
            active_downloads.write().await.remove(&hash_key);
            live_stats.write().await.remove(&live_key);
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
            info!(bytes_done, name = %link.name, "Resuming from previous progress");
            let _ = crate::db::emule_downloads::set_bytes_done(db, download_id, bytes_done).await;
        }

        // --- Search for sources (skip if cache is still fresh) ---
        let cache_age_secs = last_search_at.map_or(u64::MAX, |t| t.elapsed().as_secs());
        let needs_search = cached_sources.is_empty() || cache_age_secs >= SOURCE_CACHE_SECS;

        if needs_search {
            let _ =
                crate::db::emule_downloads::set_status(db, download_id, "finding_providers", None)
                    .await;
            info!("Searching Kad2 for sources");
            let fresh = kad.search_sources(link.hash, link.size).await;
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
                count = cached_sources.len(),
                cache_age_secs, "Reusing cached sources from previous round"
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
            let _ = crate::db::emule_downloads::set_status(db, download_id, status, None).await;
            {
                let mut s = live_stats.write().await;
                let e = s.entry(live_key).or_default();
                e.sources_total = cached_sources.len() as u32;
                e.sources_active = 0;
                e.pieces_in_flight = 0;
            }
            let delay = retry_delay_secs(retry_count);
            retry_count += 1;
            info!(
                name = %link.name,
                hash = %link.hash,
                retry_in_secs = delay,
                status,
                "No Kad2 sources found — will retry"
            );
            tokio::time::sleep(Duration::from_secs(delay)).await;
            continue;
        }
        info!(
            count = cached_sources.len(),
            "Proceeding with eMule sources"
        );

        let sources = cached_sources.clone();

        // --- Attempt parallel download from discovered sources ---
        let _ = crate::db::emule_downloads::set_status(db, download_id, "downloading", None).await;

        // All slices complete already (shouldn't happen since the file would
        // have been renamed, but be robust).
        if done_count == num_slices {
            info!(name = %link.name, "All slices already complete");
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

        let mut join_set: JoinSet<()> = JoinSet::new();

        let our_tcp_port = config.emule.tcp_port;

        for source in valid_sources.into_iter().take(max_workers) {
            let peer = std::net::SocketAddrV4::new(source.ip, source.tcp_port);
            let peer_hash = source.user_hash;
            let work = work_queue.clone();
            let done = done_vec.clone();
            let met = met_path.clone();
            let part = part_path.clone();
            let ws = ws_tx.clone();
            let db_w = db.clone();
            let name_c = link.name.clone();
            let hash_hex_c = link.hash.to_hex();
            let hash = link.hash;
            let file_size = link.size;

            join_set.spawn(async move {
                let opts = DownloadOptions {
                    timeout: Duration::from_secs(3600),
                    op_timeout: Duration::from_secs(30),
                    max_queue_waits: 5,
                    file_size,
                    hash,
                    start_offset: 0,
                    peer_hash: Some(peer_hash),
                    our_tcp_port,
                };

                loop {
                    // Claim the next incomplete slice.
                    let slice_opt = { work.lock().unwrap().pop_front() };
                    let (slice_idx, slice_start, slice_end) = match slice_opt {
                        None => break,
                        Some(s) => s,
                    };

                    // Open part file seeked to this slice's start offset.
                    let file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(&part)
                        .await;
                    let mut file = match file {
                        Ok(f) => f,
                        Err(e) => {
                            warn!(%peer, slice = slice_idx, error = %e, "Failed to open part file");
                            work.lock()
                                .unwrap()
                                .push_front((slice_idx, slice_start, slice_end));
                            break;
                        }
                    };
                    if let Err(e) = file.seek(std::io::SeekFrom::Start(slice_start)).await {
                        warn!(%peer, slice = slice_idx, error = %e, "Failed to seek part file");
                        work.lock()
                            .unwrap()
                            .push_front((slice_idx, slice_start, slice_end));
                        break;
                    }

                    // Connect and perform the eMule handshake.
                    let mut on_connect = |ev: DownloadEvent| match ev {
                        DownloadEvent::Connected => info!(%peer, "Connected to eMule peer"),
                        DownloadEvent::Queued { rank } => {
                            info!(%peer, rank, "Queued at eMule peer")
                        }
                        DownloadEvent::Started => {
                            info!(%peer, "Peer granted upload slot — transfer starting")
                        }
                        _ => {}
                    };
                    let mut session = match Session::connect(peer, &opts, &mut on_connect).await {
                        Ok(s) => s,
                        Err(e) => {
                            debug!(%peer, slice = slice_idx, error = %e,
                                       "Failed to connect to eMule peer");
                            work.lock()
                                .unwrap()
                                .push_front((slice_idx, slice_start, slice_end));
                            break;
                        }
                    };

                    // Bytes from all already-completed slices (not counting the
                    // one we are about to download).  Used to report cumulative
                    // progress rather than the raw absolute offset of this slice.
                    let cumulative_before: u64 = {
                        let d = done.lock().unwrap();
                        d.iter()
                            .enumerate()
                            .filter(|&(_, &done_flag)| done_flag)
                            .map(|(i, _)| {
                                let s = i as u64 * CHUNK_SIZE as u64;
                                (s + CHUNK_SIZE as u64).min(file_size) - s
                            })
                            .sum()
                    };

                    // Download the slice.
                    let bytes_tracker = Arc::new(AtomicU64::new(cumulative_before));
                    let tracker = bytes_tracker.clone();
                    let ws_c = ws.clone();
                    let db_c = db_w.clone();
                    let name_cc = name_c.clone();
                    let hash_hex_cc = hash_hex_c.clone();
                    let mut on_progress = move |ev: DownloadEvent| match ev {
                        DownloadEvent::Progress {
                            bytes_received,
                            total,
                        } => {
                            // bytes_received is an absolute file offset; subtract
                            // slice_start to get bytes downloaded within this slice,
                            // then add cumulative_before for the running total.
                            let total_done = cumulative_before + (bytes_received - slice_start);
                            tracker.store(total_done, Ordering::Relaxed);
                            let _ = ws_c.send(WsEvent::DownloadProgress(vec![
                                rucio_core::api::downloads::DownloadResponse {
                                    id: download_id,
                                    root_hash: hash_hex_cc.clone(),
                                    name: Some(name_cc.clone()),
                                    size: Some(total),
                                    bytes_done: total_done,
                                    state: rucio_core::api::downloads::DownloadState::Downloading,
                                    error: None,
                                },
                            ]));
                        }
                        DownloadEvent::ChunkVerified { part_index } => {
                            info!(part_index, "eMule chunk verified");
                            let bytes = tracker.load(Ordering::Relaxed);
                            let db = db_c.clone();
                            tokio::spawn(async move {
                                let _ = crate::db::emule_downloads::set_bytes_done(
                                    &db,
                                    download_id,
                                    bytes,
                                )
                                .await;
                            });
                        }
                        DownloadEvent::ChunkFailed { part_index } => {
                            warn!(part_index, "eMule chunk verification failed");
                        }
                        _ => {}
                    };

                    match session
                        .download_range(slice_start, slice_end, &mut file, &mut on_progress)
                        .await
                    {
                        Ok(_) => {
                            info!(%peer, slice = slice_idx, "Slice downloaded successfully");
                            // Mark slice as done and persist progress.
                            let snapshot = {
                                let mut d = done.lock().unwrap();
                                d[slice_idx] = true;
                                d.clone()
                            };
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
                        }
                        Err(e) => {
                            debug!(%peer, slice = slice_idx, error = %e,
                                   "Slice download failed — retrying");
                            work.lock()
                                .unwrap()
                                .push_front((slice_idx, slice_start, slice_end));
                            break;
                        }
                    }
                }
            });
        }

        // Wait for all workers to finish.
        while join_set.join_next().await.is_some() {}

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
            let _ = crate::db::emule_downloads::set_status(db, download_id, status, None).await;
            let delay = retry_delay_secs(retry_count);
            retry_count += 1;
            info!(
                name = %link.name,
                hash = %link.hash,
                new_slices,
                retry_in_secs = delay,
                status,
                "Not all slices complete — retrying"
            );
            tokio::time::sleep(Duration::from_secs(delay)).await;
            continue;
        }

        break; // all slices done, proceed to finalise
    }

    // --- Download succeeded: move to final destination and compute BLAKE3 ---
    std::fs::create_dir_all(config.storage.download_dir.as_path())
        .context("create download dir")?;
    tokio::fs::rename(&part_path, &final_path)
        .await
        .with_context(|| format!("move {} → {}", part_path.display(), final_path.display()))?;
    // Clean up progress file.
    let _ = tokio::fs::remove_file(&met_path).await;

    let path_clone = final_path.clone();
    let blake3_hex = tokio::task::spawn_blocking(move || -> anyhow::Result<String> {
        let mut file = std::fs::File::open(&path_clone)?;
        let mut hasher = blake3::Hasher::new();
        std::io::copy(&mut file, &mut hasher)?;
        Ok(hasher.finalize().to_hex().to_string())
    })
    .await
    .context("spawn_blocking for BLAKE3")?
    .with_context(|| format!("BLAKE3 hash of {}", final_path.display()))?;

    let _ = crate::db::emule_downloads::set_completed(
        db,
        download_id,
        final_path.to_string_lossy().as_ref(),
    )
    .await;

    info!(
        name = %link.name,
        blake3 = %blake3_hex,
        "eMule download complete — file ready in download directory"
    );
    active_downloads.write().await.remove(&hash_key);
    live_stats.write().await.remove(&live_key);
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` if the download has been marked as `cancelled` in the DB.
async fn is_cancelled(db: &Db, download_id: i64) -> bool {
    match crate::db::emule_downloads::get_status(db, download_id).await {
        Ok(Some(s)) => s == "cancelled",
        _ => false,
    }
}

/// Resolve the effective `nodes.dat` path: the configured value when present,
/// otherwise the platform default (`$XDG_DATA_HOME/rucio/nodes.dat`).
pub fn effective_nodes_dat_path(config: &crate::config::Config) -> std::path::PathBuf {
    config.storage.nodes_dat_path.clone().unwrap_or_else(|| {
        dirs::data_local_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
            .join("rucio")
            .join("nodes.dat")
    })
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
