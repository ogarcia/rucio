pub mod api;
pub mod config;
pub mod db;

pub mod emule;
#[cfg(feature = "emule-compat")]
pub mod emule_identity;
mod fsutil;
pub mod live_stats;
pub mod metrics;
pub mod notifier;
pub mod pinset;
pub mod throttle;
pub mod transfer;
pub mod upload_stats;
pub mod upnp;
pub mod watcher;
pub mod webhooks;

// The libp2p networking layer lives in the `rucio-net` crate. Re-export it
// under the historical `node` name so existing `crate::node::…` paths and the
// `NodeCmd`/`NodeEvent` channel interface keep resolving unchanged.
pub use rucio_net as node;

use anyhow::{Context, Result};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use rucio_core::api::ws::WsEvent;
use rucio_core::protocol::search::{SearchQuery, SearchResult};

/// Resolves when the process should shut down: Ctrl-C / SIGINT, or SIGTERM
/// (what a service manager like systemd sends on `stop`). Handling SIGTERM is
/// essential — without it `systemctl stop` kills the daemon outright, skipping
/// the graceful shutdown (final metrics flush, Kad cache save, clean DB close,
/// which is what lets SQLite remove its `-wal`/`-shm` files).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            // If we can't install the SIGTERM handler, still honour Ctrl-C.
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Resolve the portable base directory from CLI flags and, if one applies,
/// export it as `RUCIOD_BASE_DIR` so the config layer roots *all* storage
/// (config, identity, database, temp, downloads, eMule caches) under it.
///
/// `base_dir` wins if given; otherwise `--portable` uses the folder containing
/// the executable (the Windows desktop shell relies on this). With neither, the
/// environment is left untouched and platform defaults (XDG/AppData) apply.
///
/// Call this from `main` **before** the async runtime is created: `set_var` is
/// only sound while no other thread reads the environment, which holds at that
/// point but not once Tokio's workers are live.
pub fn apply_base_dir_env(portable: bool, base_dir: Option<&std::path::Path>) {
    let resolved = base_dir.map(std::path::Path::to_path_buf).or_else(|| {
        if portable {
            std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(std::path::Path::to_path_buf))
        } else {
            None
        }
    });
    if let Some(dir) = resolved {
        // SAFETY: per this function's contract it runs before the runtime starts,
        // so no other thread is reading the environment concurrently.
        unsafe { std::env::set_var("RUCIOD_BASE_DIR", dir) };
    }
}

/// Entry point for the daemon logic. Runs until Ctrl-C / SIGTERM.
pub async fn run(config_path: Option<&std::path::Path>) -> Result<()> {
    run_until(config_path, shutdown_signal()).await
}

/// Like [`run`], but also shuts down gracefully when `shutdown` resolves — not
/// only on a process signal. The desktop shell passes a trigger it fires when
/// its window closes, so the daemon still flushes metrics, saves the Kad cache
/// and closes SQLite cleanly instead of being killed with the process.
pub async fn run_until<F: std::future::Future<Output = ()>>(
    config_path: Option<&std::path::Path>,
    shutdown: F,
) -> Result<()> {
    rucio_core::logging::init(
        "RUCIOD",
        "rucio_daemon=info,rucio_core=info,rucio_emule=info,rucio_net=info",
    );

    let config = Arc::new(config::Config::load(config_path)?);
    let stored_config_path = config_path.map(|p| p.to_path_buf());
    info!("Starting Rucio daemon v{}", env!("CARGO_PKG_VERSION"));

    // --- Storage directories ------------------------------------------------
    // Ensure download_dir, temp_dir and pin_dir exist.
    for dir in [
        &config.storage.download_dir,
        &config.storage.temp_dir,
        &config.storage.pin_dir,
    ] {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating directory {}", dir.display()))?;
        info!(path = %dir.display(), "Storage directory ready");
    }

    // Directories whose files must never be indexed/shared: the temp dirs,
    // where in-progress `.part` downloads live. The share watcher excludes
    // anything under these (and any `.part`), so a temp_dir nested inside the
    // download_dir can't leak partial files onto the network.
    let excluded_index_dirs = std::sync::Arc::new(vec![
        config.storage.temp_dir.clone(),
        config.emule.temp_dir.clone(),
    ]);
    // Warn (don't block — the watcher handles it) about the nested-temp footgun.
    if config
        .storage
        .temp_dir
        .starts_with(&config.storage.download_dir)
    {
        warn!(
            temp_dir = %config.storage.temp_dir.display(),
            download_dir = %config.storage.download_dir.display(),
            "temp_dir is inside download_dir; partial .part files are kept out of \
             sharing, but consider moving temp_dir outside the download directory"
        );
    }
    // pin_dir under (or equal to) a temp dir is excluded from indexing, so pinned
    // content there would silently never be shared. Nesting pin_dir inside
    // download_dir, or making them the same dir, is fine (the watcher de-nests
    // and the protected set de-dups) — only the temp overlap is a problem.
    if config.storage.pin_dir.starts_with(&config.storage.temp_dir)
        || config.storage.pin_dir.starts_with(&config.emule.temp_dir)
    {
        warn!(
            pin_dir = %config.storage.pin_dir.display(),
            "pin_dir is inside a temp directory, which is excluded from indexing; \
             pinned files there will not be shared. Move pin_dir out of the temp dir"
        );
    }

    // --- Database -----------------------------------------------------------
    let db = db::open(&config.storage.database_path).await?;

    // --- Node ---------------------------------------------------------------
    // Path conventions stay in the daemon's config; the network layer only
    // needs the resolved values.
    let net_cfg = rucio_net::NetConfig {
        identity_path: config.node.identity_path.clone(),
        listen_addrs: config.node.listen_addrs.clone(),
        behaviour: rucio_net::BehaviourConfig::full(),
    };
    // Shared upload throttle: eMule uploads (Priority::Low) and Rucio chunk
    // serving (Priority::High) draw from this one bucket, so the configured cap
    // is shared between them with Rucio taking precedence. Built here (before
    // the node task) so we can hand the network layer a limiter that paces the
    // chunk *write* at the byte level — a smooth stream instead of dumping a
    // whole 4 MiB chunk at link speed then idling.
    let upload_throttle = Arc::new(throttle::TokenBucket::new(config.network.upload_limit_kbps));
    // Session metrics, created here so the upload limiter can account bytes as
    // they're paced onto the wire (a flat speed reading) rather than the engine
    // recording a whole chunk at handoff (a spike per 4 MiB).
    let session_metrics = Arc::new(metrics::Metrics::new(metrics::instant_to_unix(
        &Instant::now(),
    )));
    let rucio_upload_limiter: rucio_net::ByteLimiter = {
        let up = Arc::clone(&upload_throttle);
        let met = Arc::clone(&session_metrics);
        Arc::new(move |bytes| {
            let up = Arc::clone(&up);
            let met = Arc::clone(&met);
            Box::pin(async move {
                up.acquire(bytes, crate::throttle::Priority::High).await;
                met.record_upload_bytes(bytes);
            })
        })
    };
    let mut handle = node::task::spawn(&net_cfg, Some(rucio_upload_limiter)).await?;

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

    // (Share re-announce and interrupted-download resume are deferred until
    // after the API server is listening — see below — so a browser reloaded
    // against a just-started daemon can open its WebSocket without waiting on
    // this best-effort startup work.)

    // --- Shared dirs: reconcile the protected set (global + category dirs) ---
    // If download_dir changed in the config (or a category dir was removed), the
    // previous one is demoted to an ordinary (removable) share.
    if let Err(e) =
        reconcile_protected_dirs(&db, &config.storage.download_dir, &config.storage.pin_dir).await
    {
        warn!("Could not reconcile protected shared dirs: {e}");
    }

    // --- Download engine ----------------------------------------------------
    let dest_dir = config.storage.download_dir.clone();
    let pin_dir = config.storage.pin_dir.clone();
    let temp_dir = config.storage.temp_dir.clone();
    let download_throttle = Arc::new(throttle::TokenBucket::new(
        config.network.download_limit_kbps,
    ));
    // Source of truth for the base/temporary limits and the toggle. Owns the
    // effective-rate logic and drives the two buckets above.
    let bandwidth = Arc::new(throttle::BandwidthState::new(
        Arc::clone(&upload_throttle),
        Arc::clone(&download_throttle),
        config.network.upload_limit_kbps,
        config.network.download_limit_kbps,
        config.network.temp_upload_limit_kbps,
        config.network.temp_download_limit_kbps,
    ));
    let upload_semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.network.max_upload_tasks.max(1),
    ));
    // Per-download live statistics, shared between the engines, the speed
    // sampler in this loop, and the API handlers.
    let live_stats: live_stats::LiveStatsMap =
        Arc::new(RwLock::new(std::collections::HashMap::new()));
    // Per-peer active-upload registry, shared between the rucio engine, the
    // eMule upload server, the per-second sampler in this loop, and the API.
    let upload_stats = Arc::new(upload_stats::UploadRegistry::new());
    // WebSocket broadcast bus and the notification service are created up front
    // so the download engine (and later the eMule task and indexing tick) can
    // record notifications. The notifier holds live toggles seeded from config.
    let (ws_tx, _) = tokio::sync::broadcast::channel::<WsEvent>(256);
    let notif_state = crate::notifier::NotificationState::from_config(&config.notifications);
    let notifier =
        crate::notifier::Notifier::new(db.clone(), ws_tx.clone(), Arc::clone(&notif_state));

    let mut engine = transfer::DownloadEngine::new(
        db.clone(),
        handle.cmd_tx.clone(),
        dest_dir,
        pin_dir,
        temp_dir,
        Arc::clone(&session_metrics),
        Arc::clone(&upload_semaphore),
        Arc::clone(&download_throttle),
        Arc::clone(&live_stats),
        Arc::clone(&upload_stats),
        notifier.clone(),
    );

    let (download_tx, mut download_rx) = tokio::sync::mpsc::channel::<api::DownloadRequest>(32);

    // Channel by which the share watcher hands freshly-indexed file paths to the
    // eMule layer so each Rucio share is also seeded to Kad as a source. Only
    // wired up when eMule is enabled; otherwise `None` and the watcher skips it.
    // Bounded + non-blocking on the watcher side, so eMule seeding can never
    // throttle the Rucio share pipeline.
    #[cfg(feature = "emule-compat")]
    let (ed2k_index_tx, ed2k_index_rx) = if config.emule.enabled {
        let (tx, rx) = tokio::sync::mpsc::channel::<std::path::PathBuf>(512);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    #[cfg(not(feature = "emule-compat"))]
    let ed2k_index_tx: Option<tokio::sync::mpsc::Sender<std::path::PathBuf>> = None;

    // --- Watcher service ----------------------------------------------------
    // Shared with AppState so watcher-driven indexing shows up in the indexing
    // status endpoint / WS, the same as manual `share add`.
    let indexing_count = Arc::new(AtomicUsize::new(0));
    // Set to true by any indexing producer the moment it enqueues work, cleared
    // by the ws_tick when the pending count is back to 0. A latch (rather than
    // sampling the count) so a batch that starts and finishes between two ticks
    // — e.g. a single small file — still fires the "indexing complete" event.
    let indexing_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (watcher, watcher_task) = watcher::spawn(
        db.clone(),
        handle.cmd_tx.clone(),
        Arc::clone(&indexing_count),
        Arc::clone(&excluded_index_dirs),
        ed2k_index_tx.clone(),
    );

    // Register all known shared dirs with the watcher (including download_dir
    // which was just inserted above).
    {
        let dirs = db::shared_dirs::list(&db).await.unwrap_or_default();
        for d in &dirs {
            watcher.watch(std::path::PathBuf::from(&d.path)).await;
        }
    }

    // Reconcile shared dirs against disk now — inotify only sees live changes,
    // so files added/removed/modified while the daemon was stopped (or any
    // inotify event the kernel dropped under load) would otherwise be missed —
    // then re-check once a day. Cheap on a stable library: it only hashes files
    // that are actually new or whose size/mtime changed.
    let reconcile_task = {
        let db = db.clone();
        let node_tx = handle.cmd_tx.clone();
        let indexing_count = indexing_count.clone();
        let excluded = Arc::clone(&excluded_index_dirs);
        let ed2k_tx = ed2k_index_tx.clone();
        let indexing_seen = Arc::clone(&indexing_seen);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(24 * 3600));
            loop {
                tick.tick().await; // fires immediately on the first iteration
                watcher::reconcile_shares(
                    &db,
                    &node_tx,
                    &indexing_count,
                    &excluded,
                    ed2k_tx.as_ref(),
                    &indexing_seen,
                )
                .await;
            }
        })
    };

    // --- API server ---------------------------------------------------------

    // In-memory whitelist of files currently being downloaded via eMule, shared
    // between the download engine (registers entries) and the upload handler
    // (reads entries to decide what to serve).
    #[cfg(feature = "emule-compat")]
    let active_downloads: rucio_emule::transfer::ActiveDownloads =
        std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));

    // Upload concurrency limiter and inbound-connection counter — created up
    // front so the status endpoint can read them whether or not the TCP
    // listener binds successfully.
    #[cfg(feature = "emule-compat")]
    let emule_upload_slots = std::sync::Arc::new(tokio::sync::Semaphore::new(
        config.emule.max_upload_slots.clamp(1, 50),
    ));
    #[cfg(feature = "emule-compat")]
    let emule_inbound_connections = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    #[cfg(feature = "emule-compat")]
    let emule_last_inbound_at = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    // Shared counters the eMule upload server bumps as it serves data; the
    // metrics tick reconciles their deltas into the session metrics.
    #[cfg(feature = "emule-compat")]
    let emule_uploaded_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    #[cfg(feature = "emule-compat")]
    let emule_uploaded_chunks = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Global cap on concurrently active eMule downloads.  Surplus downloads
    // wait in the `queued` state until a running download finishes.
    #[cfg(feature = "emule-compat")]
    let emule_download_slots = std::sync::Arc::new(tokio::sync::Semaphore::new(
        config.emule.max_concurrent_downloads.clamp(1, 50),
    ));

    // Registry of running eMule download tasks (download_id → stop flag), so
    // pause/cancel can stop a task promptly and the spawn site never launches a
    // duplicate task for an id that is already running.
    #[cfg(feature = "emule-compat")]
    let emule_cancel: crate::emule::EmuleCancelRegistry =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

    // --- Kad2 background task (emule-compat) --------------------------------
    #[cfg(feature = "emule-compat")]
    let kad_handle = if !config.emule.enabled {
        info!("eMule subsystem disabled by configuration");
        let socket = Arc::new(
            tokio::net::UdpSocket::bind("0.0.0.0:0")
                .await
                .expect("bind ephemeral UDP socket"),
        );
        rucio_emule::kad::task::spawn(
            socket,
            rucio_emule::kad::packet::KadId::random(),
            rucio_emule::kad::task::KadTaskConfig::default(),
        )
    } else {
        match crate::emule::start_kad_task(&config).await {
            Ok(h) => h,
            Err(e) => {
                warn!("Failed to start Kad2 task: {e} — eMule downloads will not work");
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
    if config.emule.enabled {
        // Reload completed eMule downloads we keep seeding into the upload
        // whitelist (dropping any whose file changed/vanished) before the
        // upload server starts accepting peers, then watch the downloads dir so
        // a file that is later modified/removed stops being shared immediately.
        crate::emule::load_shared_files(&db, &active_downloads).await;
        crate::emule::spawn_shared_files_watcher(
            db.clone(),
            active_downloads.clone(),
            config.storage.download_dir.clone(),
        );
        // Our persistent eMule user hash (credit identity), generated once and
        // reused so seeding credit accrues to a stable identity. Stored on disk
        // next to identity.key (see emule_identity) — never in the DB, which is
        // reconstructible.
        let emule_id_path = crate::emule_identity::path(&config);
        let emule_user_hash =
            crate::emule_identity::load_or_create(&emule_id_path).unwrap_or([0u8; 16]);
        let tcp_port = config.emule.tcp_port;
        match crate::emule::start_emule_tcp_listener(&config).await {
            Ok(listener) => {
                // Gate eMule uploads on the same upload throttle as libp2p, so
                // the temporary speed limit (and any base cap) covers them too.
                let up = Arc::clone(&upload_throttle);
                // eMule uploads run at Low priority so a Rucio (libp2p) upload
                // always wins the shared cap — eMule is the lure, not the product.
                let upload_limiter: rucio_emule::transfer::ByteLimiter = Arc::new(move |bytes| {
                    let up = Arc::clone(&up);
                    Box::pin(async move { up.acquire(bytes, crate::throttle::Priority::Low).await })
                });
                let upload_ctx = std::sync::Arc::new(rucio_emule::transfer::UploadContext {
                    slots: emule_upload_slots.clone(),
                    temp_dir: config.emule.temp_dir.clone(),
                    tcp_port,
                    user_hash: emule_user_hash,
                    nick: config.emule.nick.clone(),
                    downloads: active_downloads.clone(),
                    inbound_connections: emule_inbound_connections.clone(),
                    last_inbound_at: emule_last_inbound_at.clone(),
                    uploaded_bytes: emule_uploaded_bytes.clone(),
                    chunks_served: emule_uploaded_chunks.clone(),
                    upload_limiter: Some(upload_limiter),
                    upload_observer: Some(std::sync::Arc::new(upload_stats::EmuleUploadObserver(
                        Arc::clone(&upload_stats),
                    ))),
                });
                tokio::spawn(rucio_emule::transfer::serve_incoming(listener, upload_ctx));
            }
            Err(e) => {
                warn!(
                    "Failed to bind eMule TCP port {tcp_port}: {e} — running as Low-ID (slower downloads)"
                );
            }
        }
    }

    // --- UPnP port mapping --------------------------------------------------
    let (external_ip, mut upnp_handle) = if config.network.upnp {
        let upnp_cfg = upnp::UpnpConfig {
            tcp_port: config.p2p_tcp_port().unwrap_or(4321),
            #[cfg(feature = "emule-compat")]
            udp_port: if config.emule.enabled {
                Some(config.emule.udp_port)
            } else {
                None
            },
            #[cfg(feature = "emule-compat")]
            emule_tcp_port: if config.emule.enabled {
                Some(config.emule.tcp_port)
            } else {
                None
            },
            #[cfg(not(feature = "emule-compat"))]
            udp_port: None,
            #[cfg(not(feature = "emule-compat"))]
            emule_tcp_port: None,
        };
        let handle = upnp::spawn(upnp_cfg);
        (Arc::clone(&handle.external_ip), Some(handle))
    } else {
        info!("UPnP disabled by configuration");
        (Arc::new(tokio::sync::RwLock::new(None)), None)
    };

    let app_state = api::AppState {
        db: db.clone(),
        config: Arc::clone(&config),
        config_path: stored_config_path,
        node_cmd: handle.cmd_tx.clone(),
        watcher_cmd: watcher.cmd_tx.clone(),
        started_at: Instant::now(),
        node_status: Arc::clone(&node_status),
        search_registry: Arc::clone(&search_registry),
        download_tx,
        indexing_count: Arc::clone(&indexing_count),
        ws_tx: ws_tx.clone(),
        metrics: Arc::clone(&session_metrics),
        upload_throttle: Arc::clone(&upload_throttle),
        download_throttle: Arc::clone(&download_throttle),
        bandwidth: Arc::clone(&bandwidth),
        #[cfg(feature = "emule-compat")]
        kad_handle: kad_handle.clone(),
        #[cfg(feature = "emule-compat")]
        emule_active_downloads: active_downloads.clone(),
        #[cfg(feature = "emule-compat")]
        emule_upload_slots: emule_upload_slots.clone(),
        #[cfg(feature = "emule-compat")]
        emule_inbound_connections: emule_inbound_connections.clone(),
        #[cfg(feature = "emule-compat")]
        emule_last_inbound_at: emule_last_inbound_at.clone(),
        #[cfg(feature = "emule-compat")]
        emule_cancel: emule_cancel.clone(),
        external_ip,
        live_stats: Arc::clone(&live_stats),
        upload_stats: Arc::clone(&upload_stats),
        notifications: Arc::clone(&notif_state),
        indexing_seen: Arc::clone(&indexing_seen),
    };

    // --- eMule: republish our shared files as Kad sources (good citizen) ----
    #[cfg(feature = "emule-compat")]
    if config.emule.enabled {
        crate::emule::spawn_source_republisher(
            db.clone(),
            kad_handle.clone(),
            Arc::clone(&config),
            emule_last_inbound_at.clone(),
            app_state.external_ip.clone(),
        );
        // Seed files shared on the Rucio network on the eMule Kad DHT as sources
        // too — anyone holding the ed2k link finds us. One-shot catch-up for
        // pre-existing shares, plus an event-driven consumer for files indexed
        // while running (fed by the share watcher). No periodic rescan.
        crate::emule::spawn_ed2k_startup_backfill(db.clone(), active_downloads.clone());
        if let Some(rx) = ed2k_index_rx {
            crate::emule::spawn_ed2k_indexer(db.clone(), active_downloads.clone(), rx);
        }
    }

    let listen_addr = config.api.listen.clone();
    let app_state_for_serve = app_state.clone();
    let api_task = tokio::spawn(async move {
        if let Err(e) = api::serve(app_state_for_serve, &listen_addr).await {
            tracing::error!("API server error: {e}");
        }
    });

    // Best-effort startup work, run only now that the API/WebSocket server is
    // listening so a reloaded browser tab can connect promptly instead of
    // hammering a closed port while these complete.
    //
    // Re-announce previously shared files to Kademlia (and prune any that no
    // longer exist on disk) so the DHT knows we are a provider after a restart.
    let announced = reannounce_shares(&db, &handle.cmd_tx).await;
    if announced > 0 {
        info!("Re-announced {announced} share(s) to Kademlia");
    }
    // Resume any downloads interrupted by a previous crash or restart.
    engine.resume_interrupted().await;

    // --- eMule: ensure nodes.dat is present (download if missing) -----------
    // On a cold start (no nodes.dat, no kad_cache.dat) the Kad2 routing table
    // is empty.  We download nodes.dat in the background and, once it lands on
    // disk, immediately feed its contacts into the running Kad2 task so the
    // node starts connecting to the eMule network without waiting for the first
    // download request.
    #[cfg(feature = "emule-compat")]
    if config.emule.enabled {
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
    if config.emule.enabled {
        let emule_rows = db::emule_downloads::list_resumable(&db)
            .await
            .unwrap_or_default();
        if !emule_rows.is_empty() {
            info!(
                count = emule_rows.len(),
                "Resuming interrupted eMule downloads"
            );
            for row in emule_rows {
                // Register a stop flag so these resumed downloads can be
                // paused/cancelled like any other.
                let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
                emule_cancel.lock().unwrap().insert(row.id, cancel.clone());
                let config = config.clone();
                let db = db.clone();
                let kad = kad_handle.clone();
                let ad = active_downloads.clone();
                let slots = emule_download_slots.clone();
                let ls = live_stats.clone();
                let met = Arc::clone(&session_metrics);
                let dt = Arc::clone(&download_throttle);
                let reg = emule_cancel.clone();
                let notif = notifier.clone();
                tokio::spawn(async move {
                    if let Err(e) = crate::emule::run_ed2k_download(
                        &row.ed2k_link,
                        row.id,
                        &config,
                        &db,
                        &kad,
                        &ad,
                        &slots,
                        &ls,
                        &met,
                        &dt,
                        &notif,
                        cancel,
                        reg,
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
    // No periodic re-announce timer: libp2p republishes our provided keys on its
    // own (~12h, before the 24h TTL) and re-replicates them to the current
    // closest peers, which handles freshness and churn. We only need to populate
    // libp2p's in-RAM provided set, which we do from the DB on startup and once
    // a peer connects (below), plus per-file as the watcher indexes new shares.
    // De-publishing deleted files is StopProviding's job (watcher + rescan), not
    // a re-announce concern.
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
    // Cooperative pinning: poll subscribed peers' pin-sets and mirror them
    // within quota. First pass shortly after startup (let the network settle),
    // then every few minutes.
    let mut pinset_reconcile_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + tokio::time::Duration::from_secs(20),
        tokio::time::Duration::from_secs(3 * 60),
    );
    // Publish our signed peer-address record to the DHT so peers that only know
    // our PeerId (e.g. a subscriber) can resolve our current addresses. First
    // pass once addresses are likely known, then refreshed to survive IP/NAT
    // changes (the record points stable PeerId → current addresses).
    let mut peer_record_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + tokio::time::Duration::from_secs(30),
        tokio::time::Duration::from_secs(15 * 60),
    );
    // Per-download download-speed sampler state: id → (rolling window, last bytes_done).
    let mut speed_samples: std::collections::HashMap<i64, (metrics::SpeedWindow, u64)> =
        std::collections::HashMap::new();
    // Last-seen totals of the eMule upload counters, for delta reconciliation.
    #[cfg(feature = "emule-compat")]
    let mut last_emule_up_bytes = 0u64;
    #[cfg(feature = "emule-compat")]
    let mut last_emule_up_chunks = 0u64;
    // Last broadcast lifecycle state per search, to emit SearchStateChanged
    // only on actual state transitions (results carry their own WS events).
    let mut last_search_states: std::collections::HashMap<
        u64,
        rucio_core::api::searches::SearchState,
    > = std::collections::HashMap::new();
    // The startup re-announce runs before any peer is connected, so its
    // provider-publication queries reach nobody. Re-announce once more shortly
    // after the first peer connects (Kad bootstrap has populated the routing
    // table by then) so shares actually land in the DHT without waiting for the
    // 22-minute reprovide tick.
    let mut reannounced_after_connect = false;
    // Whether the last UploadProgress push carried any rows, so we can emit one
    // empty snapshot when uploads drain (clearing the client's Uploads tab)
    // without streaming an empty list every idle second.
    let mut had_uploads = false;
    // Same active→idle edge for downloads: emit one empty DownloadProgress when
    // the last active download finishes, so the client refreshes and sees the
    // terminal state (otherwise a download that completes as the last active one
    // is never streamed again and stays "Downloading 100%" until a reload).
    let mut had_downloads = false;
    // Single-use shutdown trigger (process signal or the shell's window-close).
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("Received shutdown signal, shutting down");
                // Stop the background tasks that touch the DB before we close the
                // pool, so they can't race the close and spew "closed pool"
                // errors (a half-finished share rescan was the worst offender).
                // Aborting the API task drops its listener, freeing the port at
                // once so a restart — or a second instance the user starts
                // thinking the first had stopped — can bind it immediately.
                reconcile_task.abort();
                watcher_task.abort();
                api_task.abort();
                let _ = handle.cmd_tx.send(node::messages::NodeCmd::Shutdown).await;
                // Remove UPnP mappings while the network is still up (best-effort,
                // bounded). Leases would expire on their own, but cleaning up is
                // good etiquette towards the gateway.
                if let Some(upnp) = upnp_handle.take() {
                    upnp.shutdown().await;
                }
                // Flush remaining metric deltas to DB before exiting.
                let delta = session_metrics.take_delta();
                if let Err(e) = db::metrics::add(&db, &delta).await {
                    warn!("Final metrics flush failed: {e}");
                }
                // Persist the Kad2 routing table so the next startup seeds from
                // discovered contacts instead of doing a cold bootstrap.
                #[cfg(feature = "emule-compat")]
                if config.emule.enabled {
                    emule::save_kad_cache(&config, &kad_handle).await;
                }
                // Close the SQLite pool cleanly as the last DB action: this
                // checkpoints and lets SQLite remove its -wal/-shm files,
                // confirming we shut the database down properly. Bounded so a
                // background task still holding a connection can't hang exit.
                if tokio::time::timeout(std::time::Duration::from_secs(5), db.close())
                    .await
                    .is_err()
                {
                    warn!("Timed out closing the database pool on shutdown");
                }
                break;
            }
            _ = metrics_tick.tick() => {
                // Fold the eMule upload server's counters into the session
                // metrics before the speed window is sealed for this second.
                #[cfg(feature = "emule-compat")]
                {
                    use std::sync::atomic::Ordering;
                    let up = emule_uploaded_bytes.load(Ordering::Relaxed);
                    let ch = emule_uploaded_chunks.load(Ordering::Relaxed);
                    session_metrics.record_upload_bulk(
                        up.saturating_sub(last_emule_up_bytes),
                        ch.saturating_sub(last_emule_up_chunks),
                    );
                    last_emule_up_bytes = up;
                    last_emule_up_chunks = ch;
                }
                session_metrics.tick();
                sample_download_speeds(&db, &live_stats, &mut speed_samples).await;
                // Refresh per-peer upload rates and prune finished rucio rows.
                upload_stats.sample();
            }
            _ = metrics_flush_tick.tick() => {
                let delta = session_metrics.take_delta();
                if let Err(e) = db::metrics::add(&db, &delta).await {
                    warn!("Could not flush metrics to DB: {e}");
                }
            }
            _ = peer_record_tick.tick() => {
                let _ = handle.cmd_tx.send(node::messages::NodeCmd::PublishPeerRecord).await;
            }
            _ = pinset_reconcile_tick.tick() => {
                crate::pinset::request_all_pinsets(&db, &handle.cmd_tx).await;
                // Safety sweep: catches content orphaned by a removed
                // subscription (no PinsetReceived ever fires for that peer).
                crate::pinset::evict_unwanted(&db, &handle.cmd_tx, &config.storage.pin_dir).await;
            }
            _ = manifest_tick.tick() => {
                engine.tick_manifest_timeouts().await;
                // Resume any download stalled waiting for a shared provider's
                // global per-peer slots to free up.
                engine.dispatch_idle().await;
                engine.publish_live_stats().await;
            }
            _ = provider_refresh_tick.tick() => {
                engine.tick_provider_refresh().await;
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
                // Indexing-complete notification: a producer latched `indexing_seen`
                // when it enqueued work; once the pending count drains to 0 we fire
                // once and clear the latch. Robust even if a small batch starts and
                // finishes between two ticks, and works with no WS clients connected.
                let pending = app_state.indexing_count.load(std::sync::atomic::Ordering::Relaxed);
                if pending == 0
                    && app_state
                        .indexing_seen
                        .swap(false, std::sync::atomic::Ordering::Relaxed)
                {
                    notifier
                        .notify(
                            rucio_core::api::notifications::NotificationKind::System,
                            "Indexing complete",
                            "Your shared files are up to date",
                            None,
                        )
                        .await;
                }

                if ws_tx.receiver_count() == 0 {
                    continue;
                }
                // IndexingCount
                let _ = ws_tx.send(WsEvent::IndexingCount { pending });
                // Aggregate session speeds (5-second moving average from the
                // metrics sampler).  Lets the client show live rates without
                // a separate polling request.
                let snap = app_state.metrics.session_snapshot();
                let _ = ws_tx.send(WsEvent::SessionStats {
                    download_speed: snap.download_speed,
                    upload_speed: snap.upload_speed,
                });
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
                                | rucio_core::api::downloads::DownloadState::Stalled
                        ) {
                            active.push(rucio_core::api::downloads::DownloadResponse {
                                id: r.id,
                                root_hash: hex::encode(&r.root_hash),
                                name: Some(r.name),
                                size: Some(r.total_size as u64),
                                bytes_done: r.bytes_done as u64,
                                state,
                                error: r.error_msg,
                                category_id: r.category_id,
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
                                | rucio_core::api::downloads::DownloadState::Queued
                                | rucio_core::api::downloads::DownloadState::Downloading
                                | rucio_core::api::downloads::DownloadState::Stalled
                        ) {
                            // Prefer the live byte count (with in-flight partials)
                            // over the persisted complete-slices-only figure, so
                            // the reported progress doesn't oscillate between the
                            // two sources.
                            let live_bytes = live_stats
                                .read()
                                .await
                                .get(&(-r.id))
                                .and_then(|s| s.bytes_done);
                            active.push(rucio_core::api::downloads::DownloadResponse {
                                id: -(r.id), // negative IDs mark eMule rows in WS events
                                root_hash: hex::encode(&r.ed2k_hash),
                                name: Some(r.name),
                                size: Some(r.total_size as u64),
                                bytes_done: live_bytes.unwrap_or(r.bytes_done as u64),
                                state,
                                error: r.error_msg,
                                category_id: r.category_id,
                            });
                        }
                    }
                }
                if !active.is_empty() {
                    had_downloads = true;
                    let _ = ws_tx.send(WsEvent::DownloadProgress(active));
                } else if had_downloads {
                    // Active→idle edge: one empty tick so the client notices the
                    // download(s) left the active set and refreshes their final
                    // state (e.g. Downloading 100% → Completed).
                    had_downloads = false;
                    let _ = ws_tx.send(WsEvent::DownloadProgress(Vec::new()));
                }

                // UploadProgress — peers currently downloading from us. Push a
                // full snapshot while active, and one empty snapshot on the
                // active→idle edge so the client clears its list promptly.
                let uploads = upload_stats.snapshot();
                if !uploads.is_empty() {
                    had_uploads = true;
                    let _ = ws_tx.send(WsEvent::UploadProgress(uploads));
                } else if had_uploads {
                    had_uploads = false;
                    let _ = ws_tx.send(WsEvent::UploadProgress(Vec::new()));
                }

                // SearchStateChanged — emit on lifecycle transitions (e.g. a
                // search window closing → done) so the client's search list
                // stays live without polling. Result counts ride the per-result
                // SearchResult events; here we send the authoritative count too.
                {
                    let reg = search_registry.read().await;
                    for (id, record) in reg.records.iter() {
                        let state = record.effective_state();
                        let changed = last_search_states.get(id) != Some(&state);
                        if changed {
                            last_search_states.insert(*id, state.clone());
                            let _ = ws_tx.send(WsEvent::SearchStateChanged {
                                id: *id,
                                state,
                                result_count: record.results.len(),
                                emule_queued: record.kad2_waiting,
                            });
                        }
                    }
                    last_search_states.retain(|id, _| reg.records.contains_key(id));
                }
            }
            dl_req = download_rx.recv() => {
                if let Some(req) = dl_req {
                    match req {
                        api::DownloadRequest::Start { magnet, providers, category_id } => {
                            let peers: Vec<libp2p::PeerId> = providers
                                .iter()
                                .filter_map(|s| s.parse().ok())
                                .collect();
                            match engine.start(&magnet, peers, now_secs(), category_id).await {
                                Ok(()) => info!("Download started"),
                                Err(e) => warn!("Failed to start download: {e}"),
                            }
                        }
                        api::DownloadRequest::Cancel { download_id, root_hash } => {
                            engine.cancel(download_id, root_hash).await;
                        }
                        api::DownloadRequest::Pause { download_id, root_hash } => {
                            engine.pause(download_id, root_hash).await;
                        }
                        api::DownloadRequest::Resume { download_id } => {
                            engine.resume(download_id).await;
                        }
                        api::DownloadRequest::Rename { download_id, new_name } => {
                            engine.rename(download_id, new_name).await;
                        }
                        api::DownloadRequest::StartEd2k { link, download_id } => {
                            #[cfg(feature = "emule-compat")]
                            {
                                if !config.emule.enabled {
                                    warn!("Received StartEd2k request but eMule is disabled (emule.enabled = false)");
                                } else {
                                    // Register a stop flag for this id. If one already
                                    // exists, a task is already running (e.g. a resume
                                    // that raced an unfinished task) — don't spawn a
                                    // duplicate; the live task will observe the new
                                    // status. This prevents two tasks racing on the
                                    // same .part file.
                                    let cancel = {
                                        use std::collections::hash_map::Entry;
                                        let mut reg = emule_cancel.lock().unwrap();
                                        match reg.entry(download_id) {
                                            Entry::Occupied(_) => None,
                                            Entry::Vacant(e) => {
                                                let flag = Arc::new(
                                                    std::sync::atomic::AtomicBool::new(false),
                                                );
                                                e.insert(flag.clone());
                                                Some(flag)
                                            }
                                        }
                                    };
                                    match cancel {
                                        None => info!(
                                            dl = download_id,
                                            "eMule download task already running — not spawning a duplicate"
                                        ),
                                        Some(cancel) => {
                                            let config = config.clone();
                                            let db = db.clone();
                                            let kad = kad_handle.clone();
                                            let ad = active_downloads.clone();
                                            let slots = emule_download_slots.clone();
                                            let ls = live_stats.clone();
                                            let met = Arc::clone(&session_metrics);
                                            let dt = Arc::clone(&download_throttle);
                                            let reg = emule_cancel.clone();
                                            let notif = notifier.clone();
                                            tokio::spawn(async move {
                                                if let Err(e) = crate::emule::run_ed2k_download(
                                                    &link, download_id, &config, &db, &kad, &ad,
                                                    &slots, &ls, &met, &dt, &notif, cancel, reg,
                                                )
                                                .await
                                                {
                                                    warn!("eMule download failed: {e}");
                                                }
                                            });
                                        }
                                    }
                                }
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
                        // First peer since startup: re-announce shares once the
                        // routing table has had a few seconds to fill, so their
                        // provider records actually reach the DHT.
                        if !reannounced_after_connect {
                            reannounced_after_connect = true;
                            let db2 = db.clone();
                            let tx2 = handle.cmd_tx.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                let n = reannounce_shares(&db2, &tx2).await;
                                if n > 0 {
                                    info!("Re-announced {n} share(s) after first peer connected");
                                }
                            });
                        }
                    }
                    Some(node::messages::NodeEvent::PeerDisconnected { peer_id }) => {
                        let mut ns = node_status.write().await;
                        ns.connected_peers = ns.connected_peers.saturating_sub(1);
                        let _ = ws_tx.send(WsEvent::PeerDisconnected {
                            peer_id: peer_id.to_string(),
                        });
                    }
                    Some(node::messages::NodeEvent::PeerDiscovered { peer_id, addrs }) => {
                        let is_high_id = peer_has_public_addr(&addrs);
                        let addrs_json = serde_json::to_string(
                            &addrs.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                        )
                        .unwrap_or_default();
                        let _ = db::peers::upsert(
                            &db,
                            &peer_id.to_string(),
                            &addrs_json,
                            now_secs(),
                            is_high_id,
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
                        // Accumulate, then push the added result (with its
                        // search_id) to WebSocket subscribers.
                        if let Some((search_id, api_result)) =
                            accumulate_gossip_result(result, &search_registry).await
                        {
                            let _ = ws_tx.send(WsEvent::SearchResult {
                                search_id,
                                result: api_result,
                            });
                        }
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
                    Some(node::messages::NodeEvent::ChunkRequestFailed { request_id, peer }) => {
                        engine.on_chunk_request_failed(request_id, peer).await;
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
                    Some(node::messages::NodeEvent::PinsetRequested { channel_id, .. }) => {
                        crate::pinset::serve_pinset(&db, &handle.cmd_tx, channel_id);
                    }
                    Some(node::messages::NodeEvent::PinsetReceived { peer, response, .. }) => {
                        let fetches =
                            crate::pinset::on_pinset_received(&db, peer, response, now_secs())
                                .await;
                        for item in fetches {
                            // Land in pin_dir (the mirror_pins routing in
                            // transfer.rs), with the subscription peer as a
                            // starting provider; the DHT supplies the rest.
                            let magnet = format!(
                                "rucio:{}?name={}",
                                hex::encode(item.root_hash),
                                urlencoding::encode(&item.name)
                            );
                            match engine.start(&magnet, vec![peer], now_secs(), None).await {
                                Ok(()) => info!(
                                    hash = %hex::encode(item.root_hash),
                                    "Mirror fetch started"
                                ),
                                // Already active/completed, etc. — not an error
                                // worth surfacing; the next reconcile re-evaluates.
                                Err(e) => debug!("Mirror fetch not started: {e}"),
                            }
                        }
                        // The applied pin-set may have dropped hashes this peer
                        // no longer wants — sweep them if nobody else does.
                        crate::pinset::evict_unwanted(
                            &db,
                            &handle.cmd_tx,
                            &config.storage.pin_dir,
                        )
                        .await;
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

/// Accumulate a gossip search result into its record. Returns the owning
/// `search_id` and the newly-added result (in API shape) when it was actually
/// added, so the caller can push it over the WebSocket; `None` if the search is
/// unknown/expired/cancelled or the result was a duplicate.
async fn accumulate_gossip_result(
    result: SearchResult,
    registry: &api::SharedSearchRegistry,
) -> Option<(u64, rucio_core::api::searches::SearchResult)> {
    let mut reg = registry.write().await;
    let query_id = result.query_id.0.clone();
    let Some(&search_id) = reg.gossip_to_id.get(&query_id) else {
        debug!(qid = %query_id, "Ignoring Gossip result for unknown/expired search");
        return None;
    };
    let record = reg.records.get_mut(&search_id)?;
    if record.cancelled {
        return None;
    }
    // Merge by content hash: the same file from several peers becomes one entry
    // whose provider list grows. We re-emit the existing result (same
    // result_id) so the UI updates the source count in place.
    let existing = record.results.iter_mut().enumerate().find(|(_, r)| {
        matches!(
            &r.source,
            api::InternalSource::Rucio { root_hash, .. }
            if *root_hash == result.root_hash
        )
    });
    if let Some((index, r)) = existing {
        if let api::InternalSource::Rucio { providers, .. } = &mut r.source {
            if providers.contains(&result.provider) {
                // Same provider re-announcing — nothing new to report.
                return None;
            }
            providers.push(result.provider.clone());
        }
        return Some((search_id, record.results[index].to_api(index)));
    }
    record.results.push(api::InternalResult {
        name: result.name.clone(),
        size: result.size,
        source: api::InternalSource::Rucio {
            root_hash: result.root_hash.clone(),
            magnet: result.magnet.clone(),
            providers: vec![result.provider.clone()],
        },
    });
    let index = record.results.len() - 1;
    Some((search_id, record.results[index].to_api(index)))
}

pub(crate) fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Reconcile the set of protected/shared destination directories: the global
/// `download_dir`, the `pin_dir` (where pinned content lives), plus every
/// category-pinned directory. Creates each category dir on disk (so the watcher
/// can index it) and marks the whole set protected (undeletable), demoting any
/// directory that is no longer a destination.
///
/// Called at startup and after any category create/update/delete, so the
/// protected set always matches the live configuration.
pub(crate) async fn reconcile_protected_dirs(
    db: &db::Db,
    global_download_dir: &std::path::Path,
    pin_dir: &std::path::Path,
) -> Result<()> {
    let dl_path = global_download_dir.to_string_lossy().into_owned();
    let pin_path = pin_dir.to_string_lossy().into_owned();
    let cat_dirs = db::categories::pinned_dirs(db).await.unwrap_or_else(|e| {
        warn!("Could not load category download dirs: {e}");
        Vec::new()
    });
    // Create each category dir on disk so the watcher can index it (the global
    // download_dir, temp_dir and pin_dir are created elsewhere on startup).
    for d in &cat_dirs {
        if let Err(e) = std::fs::create_dir_all(d) {
            warn!(dir = %d, "Could not create category download dir: {e}");
        }
    }
    let mut protected: Vec<&str> = Vec::with_capacity(2 + cat_dirs.len());
    protected.push(&dl_path);
    protected.push(&pin_path);
    protected.extend(cat_dirs.iter().map(String::as_str));
    db::shared_dirs::set_protected_dirs(db, &protected, now_secs()).await
}

/// Sample each active download's `bytes_done` from the DB and update its
/// smoothed download speed in the shared live-stats map.  Called once a second
/// from the main loop; the engines own the source/piece counts, this owns the
/// `speed_bps` field.
async fn sample_download_speeds(
    db: &db::Db,
    live_stats: &live_stats::LiveStatsMap,
    samples: &mut std::collections::HashMap<i64, (metrics::SpeedWindow, u64)>,
) {
    // Snapshot the active ids together with any live byte count, so the speed
    // is derived from the same smooth, partial-aware figure the WS/API report
    // rather than the persisted count, which for eMule only jumps a whole slice
    // at a time and would make the speed lurch.
    let snapshot: Vec<(i64, Option<u64>)> = {
        let g = live_stats.read().await;
        g.iter().map(|(k, v)| (*k, v.bytes_done)).collect()
    };
    if snapshot.is_empty() {
        samples.clear();
        return;
    }
    let active_ids: Vec<i64> = snapshot.iter().map(|(id, _)| *id).collect();
    for (id, live_bytes) in snapshot {
        let bytes = match live_bytes {
            Some(b) => Some(b),
            // No live figure yet (libp2p, or eMule before the first publish):
            // fall back to the persisted count.
            None if id < 0 => {
                #[cfg(feature = "emule-compat")]
                {
                    db::emule_downloads::get(db, -id)
                        .await
                        .ok()
                        .flatten()
                        .map(|r| r.bytes_done as u64)
                }
                #[cfg(not(feature = "emule-compat"))]
                {
                    None
                }
            }
            None => db::downloads::get(db, id)
                .await
                .ok()
                .flatten()
                .map(|r| r.bytes_done as u64),
        };
        let Some(bytes) = bytes else { continue };
        let entry = samples
            .entry(id)
            .or_insert_with(|| (metrics::SpeedWindow::new(), bytes));
        let delta = bytes.saturating_sub(entry.1);
        entry.1 = bytes;
        entry.0.add(delta);
        let speed = entry.0.tick();
        if let Some(s) = live_stats.write().await.get_mut(&id) {
            s.speed_bps = speed;
        }
    }
    samples.retain(|id, _| active_ids.contains(id));
}

/// Re-announce every shared file's provider record to Kademlia, read straight
/// from the DB.
///
/// Pure re-provide: it does not touch the filesystem, so its cost is one light
/// `SELECT root_hash` plus a `StartProviding` per file — important on large
/// libraries. Reconciling the index with disk (pruning files that vanished,
/// picking up new ones) is the job of the live watcher and the periodic share
/// rescan, not of this hot path.
///
/// Returns the number of files re-announced.
async fn reannounce_shares(
    db: &db::Db,
    cmd_tx: &tokio::sync::mpsc::Sender<node::messages::NodeCmd>,
) -> usize {
    let hashes = match db::shares::list_root_hashes(db).await {
        Ok(h) => h,
        Err(e) => {
            warn!("Could not load shares for re-announcement: {e}");
            return 0;
        }
    };

    let count = hashes.len();
    for hash in hashes {
        let _ = cmd_tx
            .send(node::messages::NodeCmd::StartProviding(hash))
            .await;
    }
    count
}

/// Return `true` if any of the peer's advertised addresses is publicly routable.
/// Mirrors `classify::is_public_addr` but operates on a slice of addresses
/// and is used for classifying *remote* peers (not our own node).
fn peer_has_public_addr(addrs: &[libp2p::Multiaddr]) -> bool {
    use libp2p::multiaddr::Protocol;
    use std::net::IpAddr;

    for addr in addrs {
        for proto in addr.iter() {
            let ip: IpAddr = match proto {
                Protocol::Ip4(a) => IpAddr::V4(a),
                Protocol::Ip6(a) => IpAddr::V6(a),
                _ => continue,
            };
            let is_public = match ip {
                IpAddr::V4(a) => {
                    !a.is_loopback() && !a.is_private() && !a.is_link_local() && !a.is_unspecified()
                }
                IpAddr::V6(a) => {
                    !a.is_loopback()
                        && !a.is_unspecified()
                        && (a.segments()[0] & 0xfe00) != 0xfc00 // fc00::/7 unique-local
                        && (a.segments()[0] & 0xffc0) != 0xfe80 // fe80::/10 link-local
                }
            };
            if is_public {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use rucio_core::protocol::search::{QueryId, SearchResult};

    fn gossip_result(root_hash: &str, provider: &str) -> SearchResult {
        let magnet = SearchResult::magnet_from_parts(root_hash, "movie.mkv", 1234, Some(provider));
        SearchResult {
            query_id: QueryId("q1".to_string()),
            root_hash: root_hash.to_string(),
            name: "movie.mkv".to_string(),
            size: 1234,
            chunk_count: 1,
            mime_type: None,
            magnet,
            provider: provider.to_string(),
        }
    }

    fn registry_with_search() -> api::SharedSearchRegistry {
        let mut reg = api::SearchRegistry::new();
        reg.records.insert(
            1,
            api::SearchRecord {
                id: 1,
                keywords: vec!["movie".to_string()],
                cancelled: false,
                kad2_done: false,
                kad2_waiting: false,
                results: Vec::new(),
                started_at: std::time::Instant::now(),
                gossip_query_id: "q1".to_string(),
            },
        );
        reg.gossip_to_id.insert("q1".to_string(), 1);
        std::sync::Arc::new(tokio::sync::RwLock::new(reg))
    }

    #[tokio::test]
    async fn gossip_results_merge_by_hash_into_one_entry() {
        let reg = registry_with_search();
        let hash = "a".repeat(64);

        // First provider → new entry, one source.
        let (_, r1) = accumulate_gossip_result(gossip_result(&hash, "PeerA"), &reg)
            .await
            .expect("first result accepted");
        assert_eq!(r1.peer_count, 1);
        assert_eq!(
            r1.providers.as_deref(),
            Some(["PeerA".to_string()].as_slice())
        );

        // Second provider, same file → merged into the same result_id, two sources.
        let (_, r2) = accumulate_gossip_result(gossip_result(&hash, "PeerB"), &reg)
            .await
            .expect("second provider merged");
        assert_eq!(r2.result_id, r1.result_id, "merge keeps the same result_id");
        assert_eq!(r2.peer_count, 2);
        let link = r2.download_link.unwrap();
        assert!(
            link.contains("provider=PeerA") && link.contains("provider=PeerB"),
            "download link embeds every provider: {link}"
        );

        // Only one entry exists in the record.
        assert_eq!(reg.read().await.records[&1].results.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_provider_is_not_re_emitted() {
        let reg = registry_with_search();
        let hash = "b".repeat(64);
        accumulate_gossip_result(gossip_result(&hash, "PeerA"), &reg)
            .await
            .unwrap();
        // Same provider re-announcing the same file → nothing new.
        assert!(
            accumulate_gossip_result(gossip_result(&hash, "PeerA"), &reg)
                .await
                .is_none()
        );
        assert_eq!(
            reg.read().await.records[&1].results[0].to_api(0).peer_count,
            1
        );
    }
}
