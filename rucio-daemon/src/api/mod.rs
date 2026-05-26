//! axum REST API server for the Rucio daemon.
//!
//! All handlers live in submodules; this module owns the router, the shared
//! [`AppState`], and the [`serve`] entry point.

pub mod config;
pub mod downloads;
pub mod emule;
pub mod health;
pub mod metrics;
pub mod search;
pub mod searches;
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
use crate::throttle::TokenBucket;
use crate::watcher::WatcherCmd;
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
        downloads::start_ed2k_download,
        downloads::cancel_download,
        downloads::delete_download,
        searches::post_search,
        searches::list_searches,
        searches::get_search,
        searches::delete_search,
        searches::relaunch_search,
        config::get_config,
        config::put_config,
        metrics::get_metrics,
        health::get_health,
        emule::get_emule_status,
        emule::post_emule_bootstrap,
        emule::get_kad_search,
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
        rucio_core::api::downloads::StartEd2kDownloadRequest,
        rucio_core::api::downloads::StartEd2kDownloadResponse,
        rucio_core::api::downloads::DownloadState,
        rucio_core::api::downloads::DownloadResponse,
        rucio_core::api::downloads::DownloadsResponse,
        rucio_core::api::searches::StartSearchRequest,
        rucio_core::api::searches::SearchStartedResponse,
        rucio_core::api::searches::SearchState,
        rucio_core::api::searches::SearchSummary,
        rucio_core::api::searches::SearchListResponse,
        rucio_core::api::searches::ResultSource,
        rucio_core::api::searches::SearchResult,
        rucio_core::api::searches::SearchDetailResponse,
        rucio_core::api::config::ConfigResponse,
        rucio_core::api::config::NodeConfig,
        rucio_core::api::config::ApiConfig,
        rucio_core::api::config::NetworkConfig,
        rucio_core::api::config::StorageConfig,
        rucio_core::api::config::EmuleConfig,
        rucio_core::protocol::node::NodeClass,
        rucio_core::api::metrics::MetricsResponse,
        rucio_core::api::metrics::SessionMetrics,
        rucio_core::api::metrics::TotalMetrics,
        rucio_core::api::metrics::HealthResponse,
        rucio_core::api::emule::EmuleBootstrapRequest,
        rucio_core::api::emule::EmuleBootstrapResponse,
        rucio_core::api::emule::EmuleStatusResponse,
        emule::KadSearchHit,
        emule::KadSearchResponse,
    ))
)]
struct ApiDoc;

// ---------------------------------------------------------------------------
// SearchRegistry — unified search state (in-memory only)
// ---------------------------------------------------------------------------

pub const GOSSIP_WINDOW_SECS: u64 = 30;
pub const KAD2_TIMEOUT_SECS: u64 = 60;

/// Internal (non-serialised) representation of a single search result.
pub struct InternalResult {
    pub name: String,
    pub size: u64,
    pub source: InternalSource,
}

/// Which network produced an [`InternalResult`].
pub enum InternalSource {
    Rucio {
        root_hash: String,
        magnet: String,
        provider: String,
    },
    Emule {
        hash_hex: String,
        ed2k_link: String,
    },
}

/// One in-progress or finished unified search.
pub struct SearchRecord {
    pub id: u64,
    pub keywords: Vec<String>,
    pub cancelled: bool,
    pub kad2_done: bool,
    pub results: Vec<InternalResult>,
    pub started_at: std::time::Instant,
    /// UUID string of the Gossipsub query — used to map incoming results.
    pub gossip_query_id: String,
}

impl SearchRecord {
    /// Compute the effective state based on elapsed time and kad2_done flag.
    pub fn effective_state(&self) -> rucio_core::api::searches::SearchState {
        use rucio_core::api::searches::SearchState;
        if self.cancelled {
            return SearchState::Cancelled;
        }
        let elapsed = self.started_at.elapsed().as_secs();
        if elapsed >= KAD2_TIMEOUT_SECS || (self.kad2_done && elapsed >= GOSSIP_WINDOW_SECS) {
            SearchState::Done
        } else {
            SearchState::Running
        }
    }
}

/// In-memory store for all unified searches.
pub struct SearchRegistry {
    pub records: HashMap<u64, SearchRecord>,
    /// Maps Gossipsub query UUID string → numeric search ID.
    pub gossip_to_id: HashMap<String, u64>,
    pub next_id: u64,
}

impl SearchRegistry {
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
            gossip_to_id: HashMap::new(),
            next_id: 1,
        }
    }
}

impl Default for SearchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe handle to the search registry.
pub type SharedSearchRegistry = Arc<RwLock<SearchRegistry>>;

// ---------------------------------------------------------------------------
// DownloadRequest — sent from API handlers to the download engine
// ---------------------------------------------------------------------------

/// A message from an API handler to the main-loop download engine.
pub enum DownloadRequest {
    /// Start a new Rucio download.
    Start {
        magnet: String,
        /// Known providers (PeerId strings). At least one is required.
        providers: Vec<String>,
    },
    /// Start a new eMule download (emule-compat feature).
    StartEd2k {
        /// Full `ed2k://` link.
        link: String,
        /// The `emule_downloads.id` row already created by the API handler.
        download_id: i64,
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
    pub search_registry: SharedSearchRegistry,
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
    /// Upload bandwidth throttle (global, across all peers).  0 KB/s = unlimited.
    pub upload_throttle: Arc<TokenBucket>,
    /// Download bandwidth throttle (global, across all peers).  0 KB/s = unlimited.
    pub download_throttle: Arc<TokenBucket>,
    /// Handle to the Kad2 background task (only present with `emule-compat` feature).
    #[cfg(feature = "emule-compat")]
    pub kad_handle: rucio_emule::kad::task::KadHandle,
    /// External IP address as reported by UPnP gateway.  `None` when no gateway found.
    pub external_ip: crate::upnp::ExternalIp,
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
            "/downloads/ed2k",
            routing::post(downloads::start_ed2k_download),
        )
        .route(
            "/downloads/{id}",
            routing::delete(downloads::cancel_download),
        )
        .route(
            "/downloads/{id}/history",
            routing::delete(downloads::delete_download),
        )
        // unified searches
        .route("/searches", routing::post(searches::post_search))
        .route("/searches", routing::get(searches::list_searches))
        .route("/searches/{id}", routing::get(searches::get_search))
        .route("/searches/{id}", routing::delete(searches::delete_search))
        .route(
            "/searches/{id}/relaunch",
            routing::post(searches::relaunch_search),
        )
        // config
        .route("/config", routing::get(config::get_config))
        .route("/config", routing::put(config::put_config))
        // metrics
        .route("/metrics", routing::get(metrics::get_metrics))
        // emule
        .route("/emule/status", routing::get(emule::get_emule_status))
        .route(
            "/emule/bootstrap",
            routing::post(emule::post_emule_bootstrap),
        )
        // kad keyword search (emule-compat)
        .route("/emule/search", routing::get(emule::get_kad_search))
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
