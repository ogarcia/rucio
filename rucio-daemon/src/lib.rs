pub mod api;
pub mod config;
pub mod db;
pub mod node;

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Entry point for the daemon logic.
/// Called both from the daemon's own `main.rs` and from the fat binary.
pub async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("rucio_daemon=info".parse()?)
                .add_directive("rucio_core=info".parse()?),
        )
        .init();

    let config = Arc::new(config::Config::load()?);
    info!("Starting Rucio daemon v{}", env!("CARGO_PKG_VERSION"));

    // --- Database -----------------------------------------------------------
    let db = db::open(&config.storage.database_path).await?;

    // --- Node ---------------------------------------------------------------
    let mut handle = node::task::spawn(&config.node).await?;

    // Dial bootstrap peers from config
    for addr_str in &config.network.bootstrap_peers {
        match addr_str.parse() {
            Ok(addr) => {
                handle
                    .cmd_tx
                    .send(node::messages::NodeCmd::AddBootstrapPeer(addr))
                    .await?;
            }
            Err(e) => tracing::warn!("Invalid bootstrap peer address {addr_str}: {e}"),
        }
    }

    // Shared live node status (updated as events arrive)
    let node_status = Arc::new(RwLock::new(api::NodeStatus::default()));

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

    // --- API server ---------------------------------------------------------
    let app_state = api::AppState {
        db: db.clone(),
        config: Arc::clone(&config),
        node_cmd: handle.cmd_tx.clone(),
        started_at: Instant::now(),
        node_status: Arc::clone(&node_status),
    };

    let listen_addr = config.api.listen.clone();
    tokio::spawn(async move {
        if let Err(e) = api::serve(app_state, &listen_addr).await {
            tracing::error!("API server error: {e}");
        }
    });

    // --- Main loop: forward node events, handle Ctrl-C ---------------------
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl-C, shutting down");
                let _ = handle.cmd_tx.send(node::messages::NodeCmd::Shutdown).await;
                break;
            }
            event = handle.event_rx.recv() => {
                match event {
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
                    }
                    Some(node::messages::NodeEvent::ClassChanged(class)) => {
                        info!(?class, "Node class updated");
                        node_status.write().await.node_class = class;
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

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
