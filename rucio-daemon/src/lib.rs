pub mod api;
pub mod config;
pub mod db;
pub mod node;

use anyhow::Result;
use tracing::info;

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

    let config = config::Config::load()?;
    info!("Starting Rucio daemon v{}", env!("CARGO_PKG_VERSION"));

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

    // Wait for the node to confirm it is listening
    loop {
        match handle.event_rx.recv().await {
            Some(node::messages::NodeEvent::Ready {
                peer_id,
                listen_addrs,
            }) => {
                info!(%peer_id, "Node ready");
                for addr in &listen_addrs {
                    info!(%addr, "Listening");
                }
                break;
            }
            Some(node::messages::NodeEvent::FatalError(e)) => {
                anyhow::bail!("Node fatal error: {e}");
            }
            Some(_) => {} // other events before Ready — ignore
            None => anyhow::bail!("Node task exited before becoming ready"),
        }
    }

    // TODO: start API server and DB, then drive the event loop
    // For now, keep the node alive until Ctrl-C
    tokio::signal::ctrl_c().await?;
    info!("Received Ctrl-C, shutting down");
    let _ = handle.cmd_tx.send(node::messages::NodeCmd::Shutdown).await;

    Ok(())
}
