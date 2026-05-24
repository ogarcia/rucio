//! axum REST API server for the Rucio daemon.
//!
//! All handlers live in submodules; this module owns the router, the shared
//! [`AppState`], and the [`serve`] entry point.

pub mod config;
pub mod downloads;
pub mod health;
pub mod metrics;
pub mod search;
pub mod shares;
pub mod status;
#[cfg(test)]
mod tests;
pub mod ws;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

use axum::{Router, routing};
use tokio::sync::RwLock;
use tokio::sync::{broadcast, mpsc};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable as _};

use crate::config::Config;
use crate::db::Db;
use crate::metrics::Metrics;
use crate::node::messages::NodeCmd;
use crate::watcher::WatcherCmd;
use rucio_core::api::search::SearchResultResponse;
use rucio_core::api::ws::WsEvent;

// ---------------------------------------------------------------------------
// OpenAPI spec + Scalar docs
// ---------------------------------------------------------------------------

/// Custom HTML template for Scalar.
///
/// - Sets the page title to "Rucio API".
/// - Enables `operationTitleSource: "path"` so operation titles in the
///   sidebar show the URL path instead of the auto-generated summary.
/// - The `$spec` placeholder is replaced by utoipa-scalar at runtime.
const SCALAR_HTML: &str = r#"<!doctype html>
<html>
  <head>
    <title>Rucio API</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script
      id="api-reference"
      type="application/json"
      data-configuration='{"operationTitleSource":"path"}'
    >$spec</script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>
"#;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Rucio API",
        version = "1",
        description = "REST API exposed by the Rucio P2P daemon."
    ),
    paths(
        status::get_status,
        status::get_peers,
        shares::list_shares,
        shares::add_share,
        shares::indexing_status,
        shares::get_magnet,
        shares::remove_share,
        shares::remove_shares_by_path,
        downloads::list_downloads,
        downloads::start_download,
        downloads::cancel_download,
        downloads::delete_download,
        search::start_search,
        search::get_results,
        config::get_config,
        config::put_config,
        metrics::get_metrics,
        health::get_health,
    ),
    components(schemas(
        rucio_core::api::status::StatusResponse,
        rucio_core::api::status::PeersResponse,
        rucio_core::api::status::PeerResponse,
        rucio_core::api::shares::AddShareRequest,
        rucio_core::api::shares::AddShareResponse,
        rucio_core::api::shares::ShareResponse,
        rucio_core::api::shares::SharesResponse,
        rucio_core::api::downloads::StartDownloadRequest,
        rucio_core::api::downloads::DownloadState,
        rucio_core::api::downloads::DownloadResponse,
        rucio_core::api::downloads::DownloadsResponse,
        rucio_core::api::search::SearchRequest,
        rucio_core::api::search::SearchStartedResponse,
        rucio_core::api::search::SearchResultResponse,
        rucio_core::api::search::SearchResultsResponse,
        rucio_core::api::config::ConfigResponse,
        rucio_core::api::config::NodeConfig,
        rucio_core::api::config::ApiConfig,
        rucio_core::api::config::NetworkConfig,
        rucio_core::api::config::StorageConfig,
        rucio_core::protocol::node::NodeClass,
        rucio_core::api::metrics::MetricsResponse,
        rucio_core::api::metrics::SessionMetrics,
        rucio_core::api::metrics::TotalMetrics,
        rucio_core::api::metrics::HealthResponse,
    ))
)]
struct ApiDoc;

// ---------------------------------------------------------------------------
// SearchStore
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct SearchEntry {
    pub results: Vec<SearchResultResponse>,
    pub pending: bool,
    pub started_at: Instant,
}

pub type SearchStore = Arc<RwLock<HashMap<String, SearchEntry>>>;
pub const SEARCH_WINDOW_SECS: u64 = 30;

// ---------------------------------------------------------------------------
// DownloadRequest — sent from API handlers to the download engine
// ---------------------------------------------------------------------------

/// A message from an API handler to the main-loop download engine.
pub enum DownloadRequest {
    /// Start a new download.
    Start {
        magnet: String,
        /// Known providers (PeerId strings). At least one is required.
        providers: Vec<String>,
    },
    /// Cancel an in-flight download by its DB id and root hash.
    Cancel {
        download_id: i64,
        /// BLAKE3 root hash — used to purge pending manifest state in the
        /// engine even before the manifest has been received.
        root_hash: Vec<u8>,
    },
}

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

/// State shared across all API handlers via `axum::extract::State`.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub config: Arc<Config>,
    pub node_cmd: mpsc::Sender<NodeCmd>,
    /// Direct channel to the filesystem watcher service.
    pub watcher_cmd: mpsc::Sender<WatcherCmd>,
    pub started_at: Instant,
    pub node_status: Arc<RwLock<NodeStatus>>,
    pub search_store: SearchStore,
    /// Channel to request new downloads from the engine in the main loop.
    pub download_tx: mpsc::Sender<DownloadRequest>,
    /// Number of files currently being indexed in the background.
    /// Incremented when a background index task starts, decremented when done.
    pub indexing_count: Arc<AtomicUsize>,
    /// Broadcast channel for WebSocket push events.
    /// Handlers subscribe with `ws_tx.subscribe()`.
    pub ws_tx: broadcast::Sender<WsEvent>,
    /// In-memory session metrics (upload/download bytes, speeds, chunk counts).
    pub metrics: Arc<Metrics>,
}

/// Live node status kept in memory and updated by the event loop.
#[derive(Debug, Clone, Default)]
pub struct NodeStatus {
    pub peer_id: String,
    pub connected_peers: usize,
    pub listen_addrs: Vec<String>,
    /// External addresses reported by remote peers via Identify.
    /// Deduplicated; populated as peers connect and exchange Identify info.
    pub observed_addrs: Vec<String>,
    pub node_class: rucio_core::protocol::node::NodeClass,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the full API router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/ws", routing::get(ws::ws_handler))
        .route("/health", routing::get(health::get_health))
        .merge(Scalar::with_url("/api/docs", ApiDoc::openapi()).custom_html(SCALAR_HTML))
        .nest("/api/v1", v1_router())
        .with_state(state)
}

fn v1_router() -> Router<AppState> {
    Router::new()
        // status & peers
        .route("/status", routing::get(status::get_status))
        .route("/peers", routing::get(status::get_peers))
        // shares
        .route("/shares", routing::get(shares::list_shares))
        .route("/shares", routing::post(shares::add_share))
        .route("/shares/indexing", routing::get(shares::indexing_status))
        .route("/shares", routing::delete(shares::remove_shares_by_path))
        .route("/shares/{hash}", routing::delete(shares::remove_share))
        .route("/shares/{hash}/magnet", routing::get(shares::get_magnet))
        // downloads
        .route("/downloads", routing::get(downloads::list_downloads))
        .route("/downloads", routing::post(downloads::start_download))
        .route(
            "/downloads/{id}",
            routing::delete(downloads::cancel_download),
        )
        .route(
            "/downloads/{id}/history",
            routing::delete(downloads::delete_download),
        )
        // search
        .route("/search", routing::post(search::start_search))
        .route("/search/{query_id}", routing::get(search::get_results))
        // config
        .route("/config", routing::get(config::get_config))
        .route("/config", routing::put(config::put_config))
        // metrics
        .route("/metrics", routing::get(metrics::get_metrics))
}

// ---------------------------------------------------------------------------
// Serve
// ---------------------------------------------------------------------------

/// Bind and serve the API on the address from config.
pub async fn serve(state: AppState, listen: &str) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| anyhow::anyhow!("failed to bind API on {listen}: {e}"))?;

    tracing::info!(addr = listen, "API server listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
