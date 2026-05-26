pub mod api;
pub mod config;
pub mod db;

pub mod emule;
pub mod metrics;
pub mod node;
pub mod throttle;
pub mod transfer;
pub mod upnp;
pub mod watcher;

use anyhow::{Context, Result};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use rucio_core::api::ws::WsEvent;
use rucio_core::protocol::search::{SearchQuery, SearchResult};

/// Entry point for the daemon logic.
pub async fn run(config_path: Option<&std::path::Path>) -> Result<()> {
    rucio_core::logging::init("RUCIOD");

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

    // Seed libp2p bootstrap from previously discovered peers stored in the DB.
    // We add the most recently seen peers so Kad can reconnect faster on restart.
    let cached_peers = db::peers::list_recent(&db, 50).await.unwrap_or_default();
    let mut cached_added = 0usize;
    for row in &cached_peers {
        // Each row stores a JSON array of multiaddr strings.  We reconstruct
        // the full /p2p/<peer_id> address by appending the peer ID component.
        let addrs: Vec<String> = serde_json::from_str(&row.addrs).unwrap_or_default();
        for addr_str in &addrs {
            // Append /p2p/<peer_id> if not already present.
            let full = if addr_str.contains("/p2p/") {
                addr_str.clone()
            } else {
                format!("{}/p2p/{}", addr_str, row.peer_id)
            };
            match full.parse() {
                Ok(addr) => {
                    handle
                        .cmd_tx
                        .send(node::messages::NodeCmd::AddBootstrapPeer(addr))
                        .await?;
                    cached_added += 1;
                }
                Err(e) => debug!("Skipping cached peer addr {full}: {e}"),
            }
        }
    }
    if cached_added > 0 {
        info!(
            peers = cached_peers.len(),
            addrs = cached_added,
            "Seeded libp2p bootstrap from DB cache"
        );
    }

    if !config.effective_bootstrap_peers().is_empty() || cached_added > 0 {
        handle
            .cmd_tx
            .send(node::messages::NodeCmd::KadBootstrapPeersReady)
            .await?;
    }

    // Shared live node status
    let node_status = Arc::new(RwLock::new(api::NodeStatus::default()));

    // In-memory unified search registry
    let search_registry = Arc::new(RwLock::new(api::SearchRegistry::new()));

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
    // knows we are a provider even after a restart.  Files that no longer
    // exist on disk are pruned from the DB at this point.
    let announced = reannounce_shares(&db, &handle.cmd_tx).await;
    if announced > 0 {
        info!("Re-announced {announced} share(s) to Kademlia");
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
    let session_metrics = Arc::new(metrics::Metrics::new(metrics::instant_to_unix(
        &Instant::now(),
    )));
    let upload_throttle = Arc::new(throttle::TokenBucket::new(config.network.upload_limit_kbps));
    let download_throttle = Arc::new(throttle::TokenBucket::new(
        config.network.download_limit_kbps,
    ));
    let mut engine = transfer::DownloadEngine::new(
        db.clone(),
        handle.cmd_tx.clone(),
        dest_dir,
        temp_dir,
        Arc::clone(&session_metrics),
        Arc::clone(&upload_throttle),
        Arc::clone(&download_throttle),
    );

    // Resume any downloads that were interrupted by a previous crash or restart.
    engine.resume_interrupted().await;

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
    let (ws_tx, _) = tokio::sync::broadcast::channel::<WsEvent>(256);

    // --- Kad2 background task (emule-compat) --------------------------------
    #[cfg(feature = "emule-compat")]
    let kad_handle = {
        match crate::emule::start_kad_task(&config).await {
            Ok(h) => h,
            Err(e) => {
                warn!("Failed to start Kad2 task: {e} — eMule downloads will not work");
                // Fallback: bind ephemeral port so we at least compile.
                let socket = Arc::new(
                    tokio::net::UdpSocket::bind("0.0.0.0:0")
                        .await
                        .expect("bind fallback UDP socket"),
                );
                let port = config.emule.udp_port;
                warn!(
                    port,
                    "Falling back to ephemeral Kad2 socket — NAT will block replies"
                );
                rucio_emule::kad::task::spawn(
                    socket,
                    rucio_emule::kad::packet::KadId::random(),
                    rucio_emule::kad::task::KadTaskConfig::default(),
                )
            }
        }
    };

    // --- eMule TCP listener (emule-compat, High-ID mode) --------------------
    #[cfg(feature = "emule-compat")]
    {
        let tcp_port = config.emule.tcp_port;
        match crate::emule::start_emule_tcp_listener(&config).await {
            Ok(listener) => {
                tokio::spawn(rucio_emule::transfer::serve_incoming(listener, tcp_port));
            }
            Err(e) => {
                warn!(
                    "Failed to bind eMule TCP port {tcp_port}: {e} — running as Low-ID (slower downloads)"
                );
            }
        }
    }

    // --- UPnP port mapping --------------------------------------------------
    let external_ip = if config.network.upnp {
        let upnp_cfg = upnp::UpnpConfig {
            tcp_port: config.network.listen_port,
            #[cfg(feature = "emule-compat")]
            udp_port: Some(config.emule.udp_port),
            #[cfg(feature = "emule-compat")]
            emule_tcp_port: Some(config.emule.tcp_port),
            #[cfg(not(feature = "emule-compat"))]
            udp_port: None,
            #[cfg(not(feature = "emule-compat"))]
            emule_tcp_port: None,
        };
        upnp::spawn(upnp_cfg)
    } else {
        info!("UPnP disabled by configuration");
        Arc::new(tokio::sync::RwLock::new(None))
    };

    let app_state = api::AppState {
        db: db.clone(),
        config: Arc::clone(&config),
        node_cmd: handle.cmd_tx.clone(),
        watcher_cmd: watcher.cmd_tx.clone(),
        started_at: Instant::now(),
        node_status: Arc::clone(&node_status),
        search_registry: Arc::clone(&search_registry),
        download_tx,
        indexing_count: Arc::new(AtomicUsize::new(0)),
        ws_tx: ws_tx.clone(),
        metrics: Arc::clone(&session_metrics),
        upload_throttle: Arc::clone(&upload_throttle),
        download_throttle: Arc::clone(&download_throttle),
        #[cfg(feature = "emule-compat")]
        kad_handle: kad_handle.clone(),
        external_ip,
    };

    let listen_addr = config.api.listen.clone();
    let app_state_for_serve = app_state.clone();
    tokio::spawn(async move {
        if let Err(e) = api::serve(app_state_for_serve, &listen_addr).await {
            tracing::error!("API server error: {e}");
        }
    });

    // --- eMule: ensure nodes.dat is present (download if missing) -----------
    // On a cold start (no nodes.dat, no kad_cache.dat) the Kad2 routing table
    // is empty.  We download nodes.dat in the background and, once it lands on
    // disk, immediately feed its contacts into the running Kad2 task so the
    // node starts connecting to the eMule network without waiting for the first
    // download request.
    #[cfg(feature = "emule-compat")]
    {
        let save_path = crate::emule::effective_nodes_dat_path(&config);
        if !save_path.exists() {
            let kad_cold = kad_handle.clone();
            let config_cold = config.clone();
            tokio::spawn(async move {
                info!(path = %save_path.display(), "nodes.dat not found — downloading in background");
                match crate::emule::bootstrap_nodes_dat(
                    &save_path,
                    rucio_core::api::emule::DEFAULT_NODES_DAT_URL,
                )
                .await
                {
                    Ok(n) => {
                        info!(contacts = n, path = %save_path.display(), "nodes.dat downloaded");
                        // Feed the fresh contacts into the live Kad2 task so it
                        // starts connecting immediately (cold-start bootstrap).
                        let seeds = crate::emule::load_kad_seeds(&config_cold, 200);
                        if !seeds.is_empty() {
                            let seeded = kad_cold.bootstrap(seeds).await;
                            info!(contacts = seeded, "Kad2 cold-start bootstrap complete");
                        }
                    }
                    Err(e) => warn!("Failed to download nodes.dat: {e}"),
                }
            });
        }
    }

    // --- eMule: resume interrupted downloads --------------------------------
    #[cfg(feature = "emule-compat")]
    {
        let emule_rows = db::emule_downloads::list_resumable(&db)
            .await
            .unwrap_or_default();
        if !emule_rows.is_empty() {
            info!(
                count = emule_rows.len(),
                "Resuming interrupted eMule downloads"
            );
            for row in emule_rows {
                let config = config.clone();
                let db = db.clone();
                let ws_tx = ws_tx.clone();
                let kad = kad_handle.clone();
                tokio::spawn(async move {
                    if let Err(e) = crate::emule::run_ed2k_download(
                        &row.ed2k_link,
                        row.id,
                        &config,
                        &db,
                        &ws_tx,
                        &kad,
                    )
                    .await
                    {
                        warn!(error = %e, "eMule resumed download failed");
                    }
                });
            }
        }
    }

    // --- Main loop ----------------------------------------------------------
    let mut manifest_tick = tokio::time::interval(tokio::time::Duration::from_secs(2));
    let mut provider_refresh_tick = tokio::time::interval(tokio::time::Duration::from_secs(60));
    // Re-announce shared files to Kademlia every 22 minutes so provider
    // records stay fresh before the 24h TTL expires.  The first tick fires
    // immediately (at t=0) but we already did a full re-announce at startup,
    // so skip it.
    let mut reprovide_tick = tokio::time::interval(tokio::time::Duration::from_secs(22 * 60));
    reprovide_tick.tick().await; // consume the immediate first tick
    // Re-bootstrap libp2p Kademlia every 10 minutes if we have no peers.
    // This recovers from a failed initial bootstrap (e.g. no internet at startup).
    let mut libp2p_bootstrap_tick =
        tokio::time::interval(tokio::time::Duration::from_secs(10 * 60));
    libp2p_bootstrap_tick.tick().await; // skip immediate first tick — startup already tried
    // Push download progress and indexing count to WebSocket subscribers
    // every second (only when there are active subscribers).
    let mut ws_tick = tokio::time::interval(tokio::time::Duration::from_secs(1));
    // Advance speed windows every second.
    let mut metrics_tick = tokio::time::interval(tokio::time::Duration::from_secs(1));
    // Persist metric deltas to DB every 30 seconds.
    let mut metrics_flush_tick = tokio::time::interval(tokio::time::Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl-C, shutting down");
                let _ = handle.cmd_tx.send(node::messages::NodeCmd::Shutdown).await;
                // Flush remaining metric deltas to DB before exiting.
                let delta = session_metrics.take_delta();
                if let Err(e) = db::metrics::add(&db, &delta).await {
                    warn!("Final metrics flush failed: {e}");
                }
                // Persist the Kad2 routing table so the next startup seeds from
                // discovered contacts instead of doing a cold bootstrap.
                #[cfg(feature = "emule-compat")]
                emule::save_kad_cache(&config, &kad_handle).await;
                break;
            }
            _ = metrics_tick.tick() => {
                session_metrics.tick();
            }
            _ = metrics_flush_tick.tick() => {
                let delta = session_metrics.take_delta();
                if let Err(e) = db::metrics::add(&db, &delta).await {
                    warn!("Could not flush metrics to DB: {e}");
                }
            }
            _ = manifest_tick.tick() => {
                engine.tick_manifest_timeouts().await;
            }
            _ = provider_refresh_tick.tick() => {
                engine.tick_provider_refresh().await;
            }
            _ = reprovide_tick.tick() => {
                let announced = reannounce_shares(&db, &handle.cmd_tx).await;
                if announced > 0 {
                    debug!("Re-announced {announced} share(s) to Kademlia");
                }
            }
            _ = libp2p_bootstrap_tick.tick() => {
                let peers = node_status.read().await.connected_peers;
                if peers == 0 {
                    info!("libp2p: no connected peers — re-bootstrapping");
                    for addr_str in config.effective_bootstrap_peers() {
                        match addr_str.parse::<libp2p::Multiaddr>() {
                            Ok(addr) => {
                                let _ = handle
                                    .cmd_tx
                                    .send(node::messages::NodeCmd::AddBootstrapPeer(addr))
                                    .await;
                            }
                            Err(e) => warn!("Invalid bootstrap peer {addr_str}: {e}"),
                        }
                    }
                    if !config.effective_bootstrap_peers().is_empty() {
                        let _ = handle
                            .cmd_tx
                            .send(node::messages::NodeCmd::KadBootstrapPeersReady)
                            .await;
                    }
                } else {
                    debug!("libp2p: {peers} peer(s) connected, bootstrap not needed");
                }
            }
            _ = ws_tick.tick() => {
                if ws_tx.receiver_count() == 0 {
                    continue;
                }
                // IndexingCount
                let pending = app_state.indexing_count.load(std::sync::atomic::Ordering::Relaxed);
                let _ = ws_tx.send(WsEvent::IndexingCount { pending });
                // DownloadProgress — only when there are active downloads
                let mut active: Vec<rucio_core::api::downloads::DownloadResponse> = Vec::new();
                if let Ok(rows) = db::downloads::list(&db).await {
                    for r in rows {
                        let state = api::downloads::db_status_to_state(&r.status);
                        if matches!(
                            state,
                            rucio_core::api::downloads::DownloadState::FindingProviders
                                | rucio_core::api::downloads::DownloadState::Queued
                                | rucio_core::api::downloads::DownloadState::Downloading
                        ) {
                            active.push(rucio_core::api::downloads::DownloadResponse {
                                id: r.id,
                                root_hash: hex::encode(&r.root_hash),
                                name: Some(r.name),
                                size: Some(r.total_size as u64),
                                bytes_done: r.bytes_done as u64,
                                state,
                                error: r.error_msg,
                            });
                        }
                    }
                }
                #[cfg(feature = "emule-compat")]
                if let Ok(rows) = db::emule_downloads::list(&db).await {
                    for r in rows {
                        let state = api::downloads::db_status_to_state(&r.status);
                        if matches!(
                            state,
                            rucio_core::api::downloads::DownloadState::FindingProviders
                                | rucio_core::api::downloads::DownloadState::Downloading
                        ) {
                            active.push(rucio_core::api::downloads::DownloadResponse {
                                id: -(r.id), // negative IDs mark eMule rows in WS events
                                root_hash: hex::encode(&r.ed2k_hash),
                                name: Some(r.name),
                                size: Some(r.total_size as u64),
                                bytes_done: r.bytes_done as u64,
                                state,
                                error: r.error_msg,
                            });
                        }
                    }
                }
                if !active.is_empty() {
                    let _ = ws_tx.send(WsEvent::DownloadProgress(active));
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
                        api::DownloadRequest::StartEd2k { link, download_id } => {
                            #[cfg(feature = "emule-compat")]
                            {
                                let config = config.clone();
                                let db = db.clone();
                                let ws_tx = ws_tx.clone();
                                let kad = kad_handle.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = crate::emule::run_ed2k_download(
                                        &link, download_id, &config, &db, &ws_tx, &kad,
                                    )
                                    .await
                                    {
                                        warn!("eMule download failed: {e}");
                                    }
                                });
                            }
                            #[cfg(not(feature = "emule-compat"))]
                            {
                                let _ = (&link, download_id);
                                warn!("Received StartEd2k request but emule-compat feature is not compiled in");
                            }
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
                    Some(node::messages::NodeEvent::PeerConnected { peer_id }) => {
                        node_status.write().await.connected_peers += 1;
                        let _ = ws_tx.send(WsEvent::PeerConnected {
                            peer_id: peer_id.to_string(),
                        });
                    }
                    Some(node::messages::NodeEvent::PeerDisconnected { peer_id }) => {
                        let mut ns = node_status.write().await;
                        ns.connected_peers = ns.connected_peers.saturating_sub(1);
                        let _ = ws_tx.send(WsEvent::PeerDisconnected {
                            peer_id: peer_id.to_string(),
                        });
                    }
                    Some(node::messages::NodeEvent::PeerDiscovered { peer_id, addrs }) => {
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
                    Some(node::messages::NodeEvent::PeerExpired { .. }) => {}
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
                        node_status.write().await.node_class = class.clone();
                        let _ = ws_tx.send(WsEvent::NodeClassChanged { class });
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
                        // Push to WebSocket subscribers before accumulating so
                        // the WsEvent carries the SearchResultResponse shape.
                        let ws_result = rucio_core::api::search::SearchResultResponse {
                            root_hash: result.root_hash.clone(),
                            name: result.name.clone(),
                            size: result.size,
                            chunk_count: result.chunk_count,
                            mime_type: result.mime_type.clone(),
                            magnet: result.magnet.clone(),
                            provider: result.provider.clone(),
                        };
                        let _ = ws_tx.send(WsEvent::SearchResult(ws_result));
                        accumulate_gossip_result(result, &search_registry).await;
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
        let magnet = SearchResult::magnet_from_parts(
            &root_hash_hex,
            &share.name,
            share.size as u64,
            Some(&peer_id),
        );

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

async fn accumulate_gossip_result(result: SearchResult, registry: &api::SharedSearchRegistry) {
    let mut reg = registry.write().await;
    let query_id = result.query_id.0.clone();
    if let Some(&search_id) = reg.gossip_to_id.get(&query_id) {
        if let Some(record) = reg.records.get_mut(&search_id) {
            // Only add results to non-cancelled searches.
            if !record.cancelled {
                let already_have = record.results.iter().any(|r| {
                    matches!(
                        &r.source,
                        api::InternalSource::Rucio { root_hash, .. }
                        if *root_hash == result.root_hash
                    )
                });
                if !already_have {
                    record.results.push(api::InternalResult {
                        name: result.name.clone(),
                        size: result.size,
                        source: api::InternalSource::Rucio {
                            root_hash: result.root_hash.clone(),
                            magnet: result.magnet.clone(),
                            provider: result.provider.clone(),
                        },
                    });
                }
            }
        }
    } else {
        debug!(qid = %query_id, "Ignoring Gossip result for unknown/expired search");
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Re-announce all shared files that still exist on disk to Kademlia.
///
/// Files whose path no longer exists are silently removed from the DB so
/// they are not announced as available when the data is gone.
/// Returns the number of files successfully re-announced.
async fn reannounce_shares(
    db: &db::Db,
    cmd_tx: &tokio::sync::mpsc::Sender<node::messages::NodeCmd>,
) -> usize {
    let shares = match db::shares::list(db).await {
        Ok(s) => s,
        Err(e) => {
            warn!("Could not load shares for re-announcement: {e}");
            return 0;
        }
    };

    let mut announced = 0;
    for share in &shares {
        if !std::path::Path::new(&share.path).exists() {
            info!(
                path = %share.path,
                hash = hex::encode(&share.root_hash),
                "Shared file no longer on disk — removing from DB"
            );
            if let Err(e) = db::shares::delete_by_path_prefix(db, &share.path).await {
                warn!("Failed to remove stale share {}: {e}", share.path);
            }
            continue;
        }
        let _ = cmd_tx
            .send(node::messages::NodeCmd::StartProviding(
                share.root_hash.clone(),
            ))
            .await;
        announced += 1;
    }
    announced
}
