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

/// Run a full eMule download pipeline using the running Kad2 task:
///
/// 1. Parse the `ed2k://` link.
/// 2. Bootstrap the Kad2 routing table from nodes.dat (if not already connected).
/// 3. Search the Kad2 network for sources via the `KadHandle`.
/// 4. Download the file from discovered peers.
/// 5. Verify the final file with its ed2k hash.
/// 6. Compute the BLAKE3 hash for Rucio DHT integration.
pub async fn run_ed2k_download(
    link_str: &str,
    config: &Arc<Config>,
    _db: &Db,
    ws_tx: &broadcast::Sender<WsEvent>,
    kad: &KadHandle,
) -> Result<()> {
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
            anyhow::bail!("nodes.dat contains no valid Kad2 contacts");
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

    // 3. Search for sources.
    info!("Searching Kad2 for sources");
    let sources = kad.search_sources(link.hash, link.size).await;

    if sources.is_empty() {
        anyhow::bail!("No sources found for {} ({})", link.name, link.hash);
    }
    info!(sources = sources.len(), "Found eMule sources");

    // 4. Download from discovered sources.
    let emule_temp = &config.storage.emule_temp_dir;
    std::fs::create_dir_all(emule_temp)
        .with_context(|| format!("create emule temp dir: {}", emule_temp.display()))?;

    let part_path = emule_temp.join(format!("{}.part", link.hash));
    let final_path = config.storage.download_dir.join(&link.name);

    let mut downloaded = false;
    for source in &sources {
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

    // 5. Move to download dir and compute BLAKE3 hash.
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
