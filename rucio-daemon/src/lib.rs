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
    info!("Config: {:?}", config);

    // TODO: initialise database, node, and API server
    Ok(())
}
