//! eMule compatibility integration for the daemon.
//!
//! This module is only compiled when the `emule-compat` feature is enabled.
//! It bridges `rucio-emule` into the daemon's download engine.

#![cfg(feature = "emule-compat")]

use anyhow::{Context, Result};
use rucio_emule::Ed2kLink;
use rucio_emule::kad::packet::KadId;
use rucio_emule::kad::routing::RoutingTable;
use rucio_emule::kad::search::{SearchConfig, bootstrap, search_sources};
use rucio_emule::transfer::{DownloadEvent, DownloadOptions, download_file};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::config::Config;
use crate::db::Db;
use rucio_core::api::ws::WsEvent;

/// Run a full eMule download pipeline:
///
/// 1. Parse the `ed2k://` link.
/// 2. Load `nodes.dat` and bootstrap the Kad2 routing table.
/// 3. Search the Kad2 network for sources.
/// 4. Download the file from discovered peers.
/// 5. Verify the final file with its ed2k hash.
/// 6. Compute the BLAKE3 hash for Rucio DHT integration.
pub async fn run_ed2k_download(
    link_str: &str,
    config: &Arc<Config>,
    _db: &Db,
    ws_tx: &broadcast::Sender<WsEvent>,
) -> Result<()> {
    // 1. Parse the link.
    let link = Ed2kLink::parse(link_str).with_context(|| format!("parse ed2k link: {link_str}"))?;
    info!(name = %link.name, size = link.size, hash = %link.hash, "Starting eMule download");

    // 2. Load nodes.dat.
    let nodes_dat_path = effective_nodes_dat_path(config);

    let nodes_dat_bytes = std::fs::read(&nodes_dat_path)
        .with_context(|| format!("read nodes.dat: {}", nodes_dat_path.display()))?;

    let contacts =
        rucio_emule::kad::routing::parse_nodes_dat(&nodes_dat_bytes).context("parse nodes.dat")?;
    info!(count = contacts.len(), "Loaded nodes.dat");

    if contacts.is_empty() {
        anyhow::bail!("nodes.dat contains no valid Kad2 contacts");
    }

    // 3. Bootstrap routing table.
    let mut routing_table = RoutingTable::new(KadId::random());
    let seeds: Vec<_> = contacts.iter().take(20).cloned().collect();
    routing_table.load_nodes_dat(contacts);

    bootstrap(&mut routing_table, &seeds, Duration::from_secs(5))
        .await
        .context("Kad2 bootstrap")?;
    info!(contacts = routing_table.len(), "Kad2 routing table ready");

    // 4. Search for sources.
    let search_cfg = SearchConfig {
        timeout: Duration::from_secs(30),
        request_timeout: Duration::from_secs(5),
        max_sources: 50,
        file_size: link.size,
        ..SearchConfig::default()
    };

    let results = search_sources(&link.hash, &routing_table, search_cfg)
        .await
        .context("Kad2 source search")?;

    if results.sources.is_empty() {
        anyhow::bail!("No sources found for {} ({})", link.name, link.hash);
    }
    info!(sources = results.sources.len(), "Found eMule sources");

    // 5. Download from the first available source.
    // In a production implementation we would try multiple sources and retry failed
    // chunks from others.  For now we try sources sequentially until one succeeds.
    let emule_temp = &config.storage.emule_temp_dir;
    std::fs::create_dir_all(emule_temp)
        .with_context(|| format!("create emule temp dir: {}", emule_temp.display()))?;

    let part_path = emule_temp.join(format!("{}.part", link.hash));
    let final_path = config.storage.download_dir.join(&link.name);

    let mut downloaded = false;
    for source in &results.sources {
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
                        id: 0,
                        root_hash: String::new(),
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
                warn!(%peer, error = %e, "eMule download from peer failed, trying next");
                let _ = tokio::fs::remove_file(&part_path).await;
            }
        }
    }

    if !downloaded {
        anyhow::bail!("All eMule sources failed for {}", link.name);
    }

    // 6. Move to download dir and compute BLAKE3 hash.
    std::fs::create_dir_all(config.storage.download_dir.as_path())
        .context("create download dir")?;
    tokio::fs::rename(&part_path, &final_path)
        .await
        .with_context(|| format!("move {} -> {}", part_path.display(), final_path.display()))?;

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

    info!(
        name = %link.name,
        blake3 = %blake3_hex,
        "eMule download complete — file ready in download directory"
    );

    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Resolve the effective `nodes.dat` path: the configured value when present,
/// otherwise the platform default (`$XDG_DATA_HOME/rucio/nodes.dat`).
///
/// This mirrors the logic used at startup for the auto-bootstrap task and in
/// the `GET /api/v1/emule/status` handler so all code agrees on one path.
pub fn effective_nodes_dat_path(config: &crate::config::Config) -> std::path::PathBuf {
    config.storage.nodes_dat_path.clone().unwrap_or_else(|| {
        dirs::data_local_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
            .join("rucio")
            .join("nodes.dat")
    })
}

/// Download a fresh `nodes.dat` from `url` and save it to `path`.
///
/// Returns the number of Kad2 contacts parsed from the file, or an error if
/// the download or parse failed.  Called at daemon startup when no
/// `nodes.dat` is present, and by the `POST /api/v1/emule/bootstrap` handler.
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
