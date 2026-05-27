//! Passive DHT indexer role (SPEC phase 5, role 2).
//!
//! Records the `ADD_PROVIDER` announcements captured by `rucio-net` (via
//! [`NodeEvent::ProviderRecord`](rucio_net::NodeEvent)) into SQLite and exposes
//! a search/admin REST API. Compiled in only with the `indexer` feature and
//! activated at runtime with `--index`.

mod api;
mod db;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use libp2p::PeerId;
use tracing::{info, warn};

/// Runtime options for the indexer role.
pub struct IndexerOpts {
    pub db_path: PathBuf,
    pub api_listen: SocketAddr,
    /// Bearer token for the admin endpoints; `None` disables them.
    pub token: Option<String>,
    pub retention_days: i64,
}

/// A running indexer: owns the DB pool and drives the API + retention tasks.
pub struct Indexer {
    db: db::Db,
}

impl Indexer {
    /// Open the DB, start the REST API server and the daily retention sweep.
    pub async fn start(opts: IndexerOpts) -> Result<Self> {
        let db = db::open(&opts.db_path)
            .await
            .context("opening indexer db")?;
        info!(
            db = %opts.db_path.display(),
            retention_days = opts.retention_days,
            "Indexer enabled"
        );

        let state = api::AppState {
            db: db.clone(),
            token: opts.token,
            started_at: Instant::now(),
            retention_days: opts.retention_days,
        };
        let app = api::router(state);
        let listener = tokio::net::TcpListener::bind(opts.api_listen)
            .await
            .with_context(|| format!("binding index API on {}", opts.api_listen))?;
        info!(listen = %opts.api_listen, "Index API listening (docs at /api/docs)");
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                warn!("Index API server stopped: {e}");
            }
        });

        // Retention sweep: prune once at startup (interval's first tick is
        // immediate) then once a day.
        let rdb = db.clone();
        let days = opts.retention_days;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(24 * 3600));
            loop {
                tick.tick().await;
                match db::prune(&rdb, days).await {
                    Ok(n) if n > 0 => info!(deleted = n, "Indexer retention sweep"),
                    Ok(_) => {}
                    Err(e) => warn!("Indexer retention sweep failed: {e}"),
                }
            }
        });

        Ok(Self { db })
    }

    /// Record an observed provider announcement.
    pub async fn record(&self, hash: &[u8], provider: &PeerId) {
        let hash_hex = hex::encode(hash);
        if let Err(e) = db::upsert(&self.db, &hash_hex, &provider.to_string()).await {
            warn!("Indexer upsert failed: {e}");
        }
    }
}
