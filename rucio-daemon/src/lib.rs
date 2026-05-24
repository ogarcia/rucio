pub mod api;
pub mod config;
pub mod db;
pub mod node;
pub mod transfer;
pub mod watcher;

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use rucio_core::api::search::SearchResultResponse;
use rucio_core::protocol::search::{SearchQuery, SearchResult};

/// Entry point for the daemon logic.
pub async fn run(config_path: Option<&std::path::Path>) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("rucio_daemon=info".parse()?)
                .add_directive("rucio_core=info".parse()?),
        )
        .init();

    let config = Arc::new(config::Config::load(config_path)?);
    info!("Starting Rucio daemon v{}", env!("CARGO_PKG_VERSION"));

    // --- Storage directories ------------------------------------------------
    // Ensure download_dir and temp_dir exist.
    for dir in [&config.storage.download_dir, &config.storage.temp_dir] {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating directory {}", dir.display()))?;
        info!(path = %dir.display(), "Storage directory ready");
    }

    // --- Database -----------------------------------------------------------
    let db = db::open(&config.storage.database_path).await?;

    // --- Node ---------------------------------------------------------------
    let mut handle = node::task::spawn(&config.node).await?;

    for addr_str in config.effective_bootstrap_peers() {
        match addr_str.parse() {
            Ok(addr) => {
                handle
                    .cmd_tx
                    .send(node::messages::NodeCmd::AddBootstrapPeer(addr))
                    .await?;
            }
            Err(e) => warn!("Invalid bootstrap peer address {addr_str}: {e}"),
        }
    }
    if !config.effective_bootstrap_peers().is_empty() {
        handle
            .cmd_tx
            .send(node::messages::NodeCmd::KadBootstrapPeersReady)
            .await?;
    }

    // Shared live node status
    let node_status = Arc::new(RwLock::new(api::NodeStatus::default()));

    // In-memory search store
    let search_store: api::SearchStore = Arc::new(RwLock::new(HashMap::new()));

    // Wait for the node to confirm it is listening
    loop {
        match handle.event_rx.recv().await {
            Some(node::messages::NodeEvent::Ready {
                peer_id,
                listen_addrs,
            }) => {
                info!(%peer_id, "Node ready");
                let mut ns = node_status.write().await;
                ns.peer_id = peer_id.to_string();
                ns.listen_addrs = listen_addrs.iter().map(|a| a.to_string()).collect();
                for addr in &ns.listen_addrs {
                    info!(%addr, "Listening");
                }
                break;
            }
            Some(node::messages::NodeEvent::FatalError(e)) => {
                anyhow::bail!("Node fatal error: {e}");
            }
            Some(_) => {}
            None => anyhow::bail!("Node task exited before becoming ready"),
        }
    }

    // Re-announce all previously shared files to Kademlia so the DHT
    // knows we are a provider even after a restart.
    match db::shares::list(&db).await {
        Ok(shares) => {
            for share in &shares {
                let _ = handle
                    .cmd_tx
                    .send(node::messages::NodeCmd::StartProviding(
                        share.root_hash.clone(),
                    ))
                    .await;
            }
            if !shares.is_empty() {
                info!("Re-announced {} share(s) to Kademlia", shares.len());
            }
        }
        Err(e) => warn!("Could not load shares for re-announcement: {e}"),
    }

    // --- Shared dirs: ensure download_dir is registered as protected --------
    {
        let dl_path = config.storage.download_dir.to_string_lossy().into_owned();
        if let Err(e) = db::shared_dirs::insert(&db, &dl_path, true, now_secs()).await {
            warn!("Could not register download_dir as protected shared dir: {e}");
        }
    }

    // --- Download engine ----------------------------------------------------
    let dest_dir = config.storage.download_dir.clone();
    let temp_dir = config.storage.temp_dir.clone();
    let mut engine =
        transfer::DownloadEngine::new(db.clone(), handle.cmd_tx.clone(), dest_dir, temp_dir);

    let (download_tx, mut download_rx) = tokio::sync::mpsc::channel::<api::DownloadRequest>(32);

    // --- Watcher service ----------------------------------------------------
    let watcher = watcher::spawn(db.clone(), handle.cmd_tx.clone());

    // Register all known shared dirs with the watcher (including download_dir
    // which was just inserted above).
    {
        let dirs = db::shared_dirs::list(&db).await.unwrap_or_default();
        for d in &dirs {
            watcher.watch(std::path::PathBuf::from(&d.path)).await;
        }
    }

    // --- API server ---------------------------------------------------------
    let app_state = api::AppState {
        db: db.clone(),
        config: Arc::clone(&config),
        node_cmd: handle.cmd_tx.clone(),
        watcher_cmd: watcher.cmd_tx.clone(),
        started_at: Instant::now(),
        node_status: Arc::clone(&node_status),
        search_store: Arc::clone(&search_store),
        download_tx,
    };

    let listen_addr = config.api.listen.clone();
    tokio::spawn(async move {
        if let Err(e) = api::serve(app_state, &listen_addr).await {
            tracing::error!("API server error: {e}");
        }
    });

    // --- Main loop ----------------------------------------------------------
    let mut manifest_tick = tokio::time::interval(tokio::time::Duration::from_secs(2));
    let mut provider_refresh_tick = tokio::time::interval(tokio::time::Duration::from_secs(60));
    // Re-announce shared files to Kademlia every 22 minutes so provider
    // records stay fresh before the 24h TTL expires.  The first tick fires
    // immediately (at t=0) but we already did a full re-announce at startup,
    // so skip it.
    let mut reprovide_tick = tokio::time::interval(tokio::time::Duration::from_secs(22 * 60));
    reprovide_tick.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl-C, shutting down");
                let _ = handle.cmd_tx.send(node::messages::NodeCmd::Shutdown).await;
                break;
            }
            _ = manifest_tick.tick() => {
                engine.tick_manifest_timeouts().await;
            }
            _ = provider_refresh_tick.tick() => {
                engine.tick_provider_refresh().await;
            }
            _ = reprovide_tick.tick() => {
                match db::shares::list(&db).await {
                    Ok(shares) => {
                        for share in &shares {
                            let _ = handle
                                .cmd_tx
                                .send(node::messages::NodeCmd::StartProviding(
                                    share.root_hash.clone(),
                                ))
                                .await;
                        }
                        if !shares.is_empty() {
                            debug!("Re-announced {} share(s) to Kademlia", shares.len());
                        }
                    }
                    Err(e) => warn!("Reprovide: could not load shares: {e}"),
                }
            }
            dl_req = download_rx.recv() => {
                if let Some(req) = dl_req {
                    match req {
                        api::DownloadRequest::Start { magnet, providers } => {
                            let peers: Vec<libp2p::PeerId> = providers
                                .iter()
                                .filter_map(|s| s.parse().ok())
                                .collect();
                            match engine.start(&magnet, peers, now_secs()).await {
                                Ok(()) => info!("Download started"),
                                Err(e) => warn!("Failed to start download: {e}"),
                            }
                        }
                        api::DownloadRequest::Cancel { download_id, root_hash } => {
                            engine.cancel(download_id, root_hash).await;
                        }
                    }
                }
            }
            event = handle.event_rx.recv() => {
                match event {
                    Some(node::messages::NodeEvent::ListenAddrAdded(addr)) => {
                        let addr_str = addr.to_string();
                        let mut ns = node_status.write().await;
                        if !ns.listen_addrs.contains(&addr_str) {
                            info!(%addr, "Listening");
                            ns.listen_addrs.push(addr_str);
                        }
                    }
                    Some(node::messages::NodeEvent::ListenAddrRemoved(addr)) => {
                        let addr_str = addr.to_string();
                        let mut ns = node_status.write().await;
                        ns.listen_addrs.retain(|a| a != &addr_str);
                    }
                    Some(node::messages::NodeEvent::PeerDiscovered { peer_id, addrs }) => {
                        node_status.write().await.connected_peers += 1;
                        let addrs_json = serde_json::to_string(
                            &addrs.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                        )
                        .unwrap_or_default();
                        let _ = db::peers::upsert(
                            &db,
                            &peer_id.to_string(),
                            &addrs_json,
                            now_secs(),
                            true,
                        )
                        .await;
                    }
                    Some(node::messages::NodeEvent::PeerExpired { .. }) => {
                        let mut ns = node_status.write().await;
                        ns.connected_peers = ns.connected_peers.saturating_sub(1);
                    }
                    Some(node::messages::NodeEvent::ObservedAddr { addr, reported_by }) => {
                        debug!(%addr, %reported_by, "Observed address");
                        let addr_str = addr.to_string();
                        let mut ns = node_status.write().await;
                        if !ns.observed_addrs.contains(&addr_str) {
                            info!(%addr, %reported_by, "New external address observed");
                            ns.observed_addrs.push(addr_str);
                        }
                    }
                    Some(node::messages::NodeEvent::ClassChanged(class)) => {
                        info!(?class, "Node class updated");
                        node_status.write().await.node_class = class;
                    }
                    Some(node::messages::NodeEvent::SearchQueryReceived(query)) => {
                        let peer_id = node_status.read().await.peer_id.clone();
                        let cmd_tx = handle.cmd_tx.clone();
                        let db2 = db.clone();
                        tokio::spawn(async move {
                            respond_to_query(query, peer_id, cmd_tx, db2).await;
                        });
                    }
                    Some(node::messages::NodeEvent::SearchResult(result)) => {
                        accumulate_result(result, &search_store).await;
                    }
                    Some(node::messages::NodeEvent::ProvidersFound { key, providers }) => {
                        if key.len() == 32 {
                            let mut root_hash = [0u8; 32];
                            root_hash.copy_from_slice(&key);
                            engine.add_providers(root_hash, providers).await;
                        }
                    }
                    Some(node::messages::NodeEvent::ChunkReceived { request_id, peer, response }) => {
                        engine.on_chunk_received(request_id, peer, response).await;
                    }
                    Some(node::messages::NodeEvent::ChunkRequested { peer, request, channel_id }) => {
                        engine.serve_chunk(peer, request, channel_id).await;
                    }
                    Some(node::messages::NodeEvent::ManifestReceived { request_id, peer, response }) => {
                        engine.on_manifest_received(request_id, peer, response, now_secs()).await;
                    }
                    Some(node::messages::NodeEvent::ManifestRequested { peer, request, channel_id }) => {
                        engine.serve_manifest(peer, request, channel_id).await;
                    }
                    Some(node::messages::NodeEvent::FatalError(e)) => {
                        tracing::error!("Node fatal error: {e}");
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Search helpers
// ---------------------------------------------------------------------------

async fn respond_to_query(
    query: SearchQuery,
    peer_id: String,
    cmd_tx: tokio::sync::mpsc::Sender<node::messages::NodeCmd>,
    db: db::Db,
) {
    let shares = match db::shares::list(&db).await {
        Ok(s) => s,
        Err(e) => {
            warn!("DB error while responding to search query: {e}");
            return;
        }
    };

    for share in shares {
        if !query.matches(&share.name) {
            continue;
        }

        let root_hash_hex = hex::encode(&share.root_hash);
        let chunk_count = (share.size as usize).div_ceil(share.chunk_size as usize);
        let magnet =
            SearchResult::magnet_from_parts(&root_hash_hex, &share.name, share.size as u64);

        let result = SearchResult {
            query_id: query.id.clone(),
            root_hash: root_hash_hex,
            name: share.name.clone(),
            size: share.size as u64,
            chunk_count,
            mime_type: share.mime_type.clone(),
            magnet,
            provider: peer_id.clone(),
        };

        if cmd_tx
            .send(node::messages::NodeCmd::PublishSearchResult(result))
            .await
            .is_err()
        {
            warn!("Node cmd channel closed; could not send search result");
            break;
        }
    }
}

async fn accumulate_result(result: SearchResult, store: &api::SearchStore) {
    let mut map = store.write().await;

    if let Some(entry) = map.get_mut(&result.query_id.0) {
        if !entry.pending {
            return;
        }
        let already_have = entry
            .results
            .iter()
            .any(|r| r.root_hash == result.root_hash);
        if !already_have {
            entry.results.push(SearchResultResponse {
                root_hash: result.root_hash,
                name: result.name,
                size: result.size,
                chunk_count: result.chunk_count,
                mime_type: result.mime_type,
                magnet: result.magnet,
                provider: result.provider,
            });
        }
    } else {
        debug!(qid = %result.query_id, "Ignoring result for unknown/expired query");
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
