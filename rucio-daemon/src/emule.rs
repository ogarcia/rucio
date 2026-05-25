//! eMule compatibility integration for the daemon.
//!
//! This module is only compiled when the `emule-compat` feature is enabled.
//! It bridges `rucio-emule` into the daemon's download engine.

#![cfg(feature = "emule-compat")]

use anyhow::{Context, Result};
use rucio_emule::Ed2kLink;
use rucio_emule::kad::packet::KadId;
use rucio_emule::kad::task::{KadHandle, KadTaskConfig};
use rucio_emule::transfer::{DownloadEvent, DownloadOptions, download_file};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::config::Config;
use crate::db::Db;
use rucio_core::api::ws::WsEvent;

/// Bind the persistent Kad2 UDP socket on the configured port and spawn the
/// Kad2 background task.
///
/// The returned [`KadHandle`] is the only way to interact with Kad2 from the
/// rest of the daemon — it must **not** share the underlying socket.
pub async fn start_kad_task(config: &Config) -> Result<KadHandle> {
    let port = config.emule.kad_port;
    let socket = UdpSocket::bind(format!("0.0.0.0:{port}"))
        .await
        .with_context(|| format!("bind Kad2 UDP socket on port {port}"))?;
    info!(port, "Kad2 UDP socket bound");

    let our_id = KadId::random();
    let task_cfg = KadTaskConfig {
        tcp_port: config.emule.kad_port, // advertise same port (TCP unused for now)
        ..KadTaskConfig::default()
    };

    let handle = rucio_emule::kad::task::spawn(Arc::new(socket), our_id, task_cfg);
    Ok(handle)
}

/// Run a full eMule download pipeline using the running Kad2 task.
///
/// The `download_id` is the DB row that was already created by the caller
/// (via `db::downloads::create_emule_pending`).  This function owns the
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
pub async fn run_ed2k_download(
    link_str: &str,
    download_id: i64,
    config: &Arc<Config>,
    db: &Db,
    ws_tx: &broadcast::Sender<WsEvent>,
    kad: &KadHandle,
) -> Result<()> {
    // How long to wait before re-searching when no sources are found or all
    // peers fail.  30 minutes matches eMule's default re-ask interval.
    const SEARCH_RETRY_SECS: u64 = 30 * 60;

    // 1. Parse the link.
    let link = Ed2kLink::parse(link_str).with_context(|| format!("parse ed2k link: {link_str}"))?;
    info!(name = %link.name, size = link.size, hash = %link.hash, "Starting eMule download");

    // 2. Bootstrap if the routing table is thin.
    let contact_count = kad.contact_count().await;
    if contact_count < 4 {
        info!(
            contact_count,
            "Routing table thin — bootstrapping from nodes.dat"
        );
        let nodes_dat_path = effective_nodes_dat_path(config);
        let nodes_dat_bytes = std::fs::read(&nodes_dat_path)
            .with_context(|| format!("read nodes.dat: {}", nodes_dat_path.display()))?;
        let contacts = rucio_emule::kad::routing::parse_nodes_dat(&nodes_dat_bytes)
            .context("parse nodes.dat")?;
        info!(count = contacts.len(), "Loaded nodes.dat contacts");
        if contacts.is_empty() {
            // Hard error — we cannot proceed without bootstrap contacts.
            let msg = "nodes.dat contains no valid Kad2 contacts";
            let _ = crate::db::downloads::set_status(db, download_id, "error", Some(msg)).await;
            anyhow::bail!("{msg}");
        }
        let seeds: Vec<_> = contacts.into_iter().take(50).collect();
        let after = kad.bootstrap(seeds).await;
        info!(contacts = after, "Kad2 bootstrap done");
    } else {
        info!(
            contact_count,
            "Kad2 routing table ready, skipping bootstrap"
        );
    }

    // 3 + 4. Main retry loop: search → try peers → if all fail, search again.
    //
    // This loop never exits on its own except via `return Ok(())` (completed)
    // or `?` propagation of a hard I/O error.  Cancellation is detected by
    // checking the DB status at the top of each iteration.
    loop {
        // Check for user cancellation before doing any work.
        if is_cancelled(db, download_id).await {
            info!(name = %link.name, "eMule download cancelled by user");
            return Ok(());
        }

        // --- Search for sources ---
        let _ = crate::db::downloads::set_status(db, download_id, "finding_providers", None).await;
        info!("Searching Kad2 for sources");
        let sources = kad.search_sources(link.hash, link.size).await;

        if sources.is_empty() {
            info!(
                name = %link.name,
                hash = %link.hash,
                retry_in_secs = SEARCH_RETRY_SECS,
                "No Kad2 sources found — will retry"
            );
            tokio::time::sleep(Duration::from_secs(SEARCH_RETRY_SECS)).await;
            continue;
        }
        info!(count = sources.len(), "Found eMule sources");

        // --- Attempt to download from discovered sources ---
        let _ = crate::db::downloads::set_status(db, download_id, "downloading", None).await;

        let emule_temp = &config.storage.emule_temp_dir;
        std::fs::create_dir_all(emule_temp)
            .with_context(|| format!("create emule temp dir: {}", emule_temp.display()))?;

        let part_path = emule_temp.join(format!("{}.part", link.hash));
        let final_path = config.storage.download_dir.join(&link.name);

        let mut downloaded = false;
        for source in &sources {
            if is_cancelled(db, download_id).await {
                info!(name = %link.name, "eMule download cancelled by user");
                return Ok(());
            }

            if source.tcp_port == 0 || source.ip.is_unspecified() {
                continue;
            }
            let peer = std::net::SocketAddrV4::new(source.ip, source.tcp_port);
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&part_path)
                .await
                .with_context(|| format!("open part file: {}", part_path.display()))?;

            let opts = DownloadOptions {
                timeout: Duration::from_secs(3600),
                op_timeout: Duration::from_secs(30),
                max_queue_waits: 5,
                file_size: link.size,
                hash: link.hash,
            };

            let ws = ws_tx.clone();
            let name_clone = link.name.clone();
            let hash_hex = link.hash.to_hex();
            match download_file(peer, opts, file, move |ev| match ev {
                DownloadEvent::Connected => info!(%peer, "Connected to eMule peer"),
                DownloadEvent::Queued { rank } => info!(%peer, rank, "Queued at eMule peer"),
                DownloadEvent::Started => info!(%peer, "eMule upload started"),
                DownloadEvent::Progress {
                    bytes_received,
                    total,
                } => {
                    let _ = ws.send(WsEvent::DownloadProgress(vec![
                        rucio_core::api::downloads::DownloadResponse {
                            id: download_id,
                            root_hash: hash_hex.clone(),
                            name: Some(name_clone.clone()),
                            size: Some(total),
                            bytes_done: bytes_received,
                            state: rucio_core::api::downloads::DownloadState::Downloading,
                            error: None,
                        },
                    ]));
                }
                DownloadEvent::ChunkVerified { part_index } => {
                    info!(part_index, "eMule chunk verified");
                }
                DownloadEvent::ChunkFailed { part_index } => {
                    warn!(part_index, "eMule chunk verification failed");
                }
                DownloadEvent::Done => info!(%peer, "eMule download complete"),
            })
            .await
            {
                Ok(bytes) => {
                    info!(bytes, name = %link.name, "eMule download finished");
                    downloaded = true;
                    break;
                }
                Err(e) => {
                    warn!(%peer, error = %e, "eMule peer failed, trying next");
                    let _ = tokio::fs::remove_file(&part_path).await;
                }
            }
        }

        if !downloaded {
            warn!(
                name = %link.name,
                hash = %link.hash,
                retry_in_secs = SEARCH_RETRY_SECS,
                "All Kad2 sources failed — back to finding_providers"
            );
            tokio::time::sleep(Duration::from_secs(SEARCH_RETRY_SECS)).await;
            continue;
        }

        // --- Download succeeded: move to final destination and compute BLAKE3 ---
        std::fs::create_dir_all(config.storage.download_dir.as_path())
            .context("create download dir")?;
        tokio::fs::rename(&part_path, &final_path)
            .await
            .with_context(|| format!("move {} → {}", part_path.display(), final_path.display()))?;

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

        let _ = crate::db::downloads::set_dest_path(
            db,
            download_id,
            final_path.to_string_lossy().as_ref(),
        )
        .await;
        let _ = crate::db::downloads::set_status(db, download_id, "completed", None).await;

        info!(
            name = %link.name,
            blake3 = %blake3_hex,
            "eMule download complete — file ready in download directory"
        );
        return Ok(());
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` if the download has been marked as `cancelled` in the DB.
/// Used to allow the long-running download loop to respect user cancellations
/// without polling on a separate channel.
async fn is_cancelled(db: &Db, download_id: i64) -> bool {
    match crate::db::downloads::get_status(db, download_id).await {
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
