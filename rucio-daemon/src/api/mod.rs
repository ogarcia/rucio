//! axum REST API server for the Rucio daemon.
//!
//! All handlers live in submodules; this module owns the router, the shared
//! [`AppState`], and the [`serve`] entry point.

pub mod categories;
pub mod config;
pub mod downloads;
pub mod emule;
pub mod health;
pub mod metrics;
pub mod notifications;
pub mod pins;
pub mod search;
pub mod searches;
pub mod shares;
#[cfg(feature = "web-ui")]
mod static_files;
pub mod status;
pub mod subscriptions;
#[cfg(test)]
mod tests;
pub mod uploads;
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
        shares::list_share_files,
        shares::add_share,
        shares::indexing_status,
        shares::get_magnet,
        shares::remove_share,
        shares::remove_shares_by_path,
        downloads::list_downloads,
        downloads::get_download,
        downloads::get_download_pieces,
        downloads::start_download,
        downloads::start_ed2k_download,
        downloads::cancel_download,
        downloads::pause_download,
        downloads::resume_download,
        downloads::rename_download,
        downloads::set_download_category,
        downloads::set_download_priority,
        downloads::delete_download,
        downloads::clear_history,
        searches::post_search,
        searches::list_searches,
        searches::get_search,
        searches::delete_search,
        searches::relaunch_search,
        config::get_config,
        config::put_config,
        config::get_temp_limit,
        config::put_temp_limit,
        config::get_limits,
        config::put_limits,
        config::get_notification_settings,
        config::put_notification_settings,
        config::get_download_settings,
        config::put_download_settings,
        config::get_webhooks,
        config::put_webhooks,
        config::test_webhook,
        categories::list_categories,
        categories::create_category,
        categories::update_category,
        categories::delete_category,
        pins::list_pins,
        pins::create_pin,
        pins::delete_pin,
        pins::set_pin_collection,
        subscriptions::list_subscriptions,
        subscriptions::create_subscription,
        subscriptions::delete_subscription,
        subscriptions::get_subscription,
        subscriptions::sync_subscription,
        subscriptions::list_subscription_files,
        subscriptions::refetch_subscription_file,
        subscriptions::set_subscription_collections,
        subscriptions::subscription_evictable,
        metrics::get_metrics,
        uploads::list_uploads,
        health::get_health,
        emule::get_emule_status,
        emule::post_emule_bootstrap,
        notifications::list_notifications,
        notifications::mark_all_read,
        notifications::clear_notifications,
        notifications::delete_notification,
    ),
    components(schemas(
        rucio_core::api::status::StatusResponse,
        rucio_core::api::status::PeersResponse,
        rucio_core::api::status::PeerResponse,
        rucio_core::api::shares::AddShareRequest,
        rucio_core::api::shares::AddShareResponse,
        rucio_core::api::shares::ShareResponse,
        rucio_core::api::shares::SharesResponse,
        rucio_core::api::shares::SharedDirResponse,
        rucio_core::api::shares::SharedDirsResponse,
        rucio_core::api::downloads::StartDownloadRequest,
        rucio_core::api::downloads::StartEd2kDownloadRequest,
        rucio_core::api::downloads::RenameDownloadRequest,
        rucio_core::api::downloads::SetDownloadPriorityRequest,
        rucio_core::api::downloads::StartEd2kDownloadResponse,
        rucio_core::api::downloads::DownloadState,
        rucio_core::api::downloads::DownloadPriority,
        rucio_core::api::downloads::DownloadResponse,
        rucio_core::api::downloads::DownloadsResponse,
        rucio_core::api::downloads::DownloadDetailResponse,
        rucio_core::api::downloads::DownloadPeerDetail,
        rucio_core::api::downloads::DownloadPiecesResponse,
        rucio_core::api::searches::StartSearchRequest,
        rucio_core::api::searches::SearchNetwork,
        rucio_core::api::searches::SearchStartedResponse,
        rucio_core::api::searches::SearchState,
        rucio_core::api::searches::SearchSummary,
        rucio_core::api::searches::SearchListResponse,
        rucio_core::api::searches::ResultSource,
        rucio_core::api::searches::SearchResult,
        rucio_core::api::searches::SearchDetailResponse,
        rucio_core::api::config::ConfigResponse,
        rucio_core::api::config::ConfigSnapshot,
        rucio_core::api::config::NodeConfig,
        rucio_core::api::config::ApiConfig,
        rucio_core::api::config::NetworkConfig,
        rucio_core::api::config::StorageConfig,
        rucio_core::api::config::EmuleConfig,
        rucio_core::api::config::DownloadSettings,
        rucio_core::api::config::TempLimitStatus,
        rucio_core::api::config::TempLimitRequest,
        rucio_core::api::config::SpeedLimits,
        rucio_core::protocol::node::NodeClass,
        rucio_core::api::metrics::MetricsResponse,
        rucio_core::api::metrics::SessionMetrics,
        rucio_core::api::metrics::TotalMetrics,
        rucio_core::api::metrics::HealthResponse,
        rucio_core::api::uploads::UploadNetwork,
        rucio_core::api::uploads::ActiveUpload,
        rucio_core::api::uploads::UploadsResponse,
        rucio_core::api::emule::EmuleBootstrapRequest,
        rucio_core::api::emule::EmuleBootstrapResponse,
        rucio_core::api::emule::EmuleStatusResponse,
        rucio_core::api::notifications::NotificationKind,
        rucio_core::api::notifications::NotificationDto,
        rucio_core::api::notifications::NotificationList,
        rucio_core::api::notifications::NotificationSettings,
        rucio_core::api::notifications::WebhookTestResult,
        crate::config::WebhookConfig,
        crate::config::WebhookFormat,
        rucio_core::api::categories::CategoryRequest,
        rucio_core::api::categories::CategoryResponse,
        rucio_core::api::categories::CategoriesResponse,
        rucio_core::api::categories::SetCategoryRequest,
        rucio_core::api::pins::PinRequest,
        rucio_core::api::pins::PinCollectionRequest,
        rucio_core::api::pins::PinResponse,
        rucio_core::api::pins::PinState,
        rucio_core::api::pins::PinsResponse,
        rucio_core::api::subscriptions::SubscriptionRequest,
        rucio_core::api::subscriptions::SubscriptionCollectionsRequest,
        rucio_core::api::subscriptions::SubscriptionResponse,
        rucio_core::api::subscriptions::SubscriptionsResponse,
        rucio_core::api::subscriptions::MirrorFile,
        rucio_core::api::subscriptions::MirrorFileState,
        rucio_core::api::subscriptions::SubscriptionFilesResponse,
        rucio_core::api::subscriptions::SubscriptionEvictableResponse,
    ))
)]
struct ApiDoc;

// ---------------------------------------------------------------------------
// SearchRegistry — unified search state (in-memory only)
// ---------------------------------------------------------------------------

pub const GOSSIP_WINDOW_SECS: u64 = 30;
pub const KAD2_TIMEOUT_SECS: u64 = 60;
/// Maximum number of search records kept in memory. Oldest Done/Cancelled
/// records are purged when this limit is exceeded.
pub const MAX_SEARCHES: usize = 100;

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
        /// Magnet as first received (embeds the first provider). The full
        /// provider set is rebuilt into the link at `to_api` time.
        magnet: String,
        /// All distinct providers merged for this content hash, first seen
        /// first. Always non-empty for a Rucio result.
        providers: Vec<String>,
    },
    Emule {
        hash_hex: String,
        ed2k_link: String,
        /// Summed availability (FT_SOURCES) across every Kad index node that
        /// reported this hash — eMule's "Availability" figure.
        sources: u32,
    },
}

impl InternalResult {
    /// Convert to the serialised API result. `index` is the 0-based position in
    /// the record's result list; `result_id` is reported as `index + 1`.
    pub fn to_api(&self, index: usize) -> rucio_core::api::searches::SearchResult {
        use rucio_core::api::searches::{ResultSource, SearchResult};
        match &self.source {
            InternalSource::Rucio {
                magnet, providers, ..
            } => {
                // Rebuild the link so it embeds every merged provider, not just
                // the one from the first gossip result.
                let link = rucio_core::protocol::magnet::MagnetLink::parse(magnet)
                    .map(|mut m| {
                        m.providers = providers.clone();
                        m.to_string()
                    })
                    .unwrap_or_else(|_| magnet.clone());
                SearchResult {
                    result_id: index + 1,
                    name: self.name.clone(),
                    size: self.size,
                    source: ResultSource::Rucio,
                    download_link: Some(link),
                    providers: Some(providers.clone()),
                    peer_count: providers.len() as u32,
                }
            }
            InternalSource::Emule {
                ed2k_link, sources, ..
            } => SearchResult {
                result_id: index + 1,
                name: self.name.clone(),
                size: self.size,
                source: ResultSource::Emule,
                download_link: Some(ed2k_link.clone()),
                providers: None,
                // Kad availability; floor at 1 so a found file always counts as
                // at least one source (the tag is sometimes absent).
                peer_count: (*sources).max(1),
            },
        }
    }
}

/// One in-progress or finished unified search.
pub struct SearchRecord {
    pub id: u64,
    pub keywords: Vec<String>,
    /// Which network(s) this search queries. Kept so a relaunch re-runs the
    /// same legs the search was originally created with.
    pub network: rucio_core::api::searches::SearchNetwork,
    pub cancelled: bool,
    pub kad2_done: bool,
    /// True while the Kad2 leg is queued behind another Kad search waiting for
    /// its turn (Kad runs one search at a time). Surfaced to the UI so the user
    /// sees "eMule: queued" instead of an unexplained delay.
    pub kad2_waiting: bool,
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
        /// Category to file the download under (None = global download dir).
        category_id: Option<i64>,
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
        /// Remove the DB row after cleanup (instead of leaving a `cancelled`
        /// entry). Set when a mirror download is evicted — there's no user-
        /// facing history to keep, and a stale `cancelled` row would only
        /// clutter the list. A user-initiated cancel leaves the row.
        delete_row: bool,
    },
    /// Suspend an in-flight download, keeping its partial file and progress.
    Pause {
        download_id: i64,
        /// BLAKE3 root hash — used to locate pending manifest state in the
        /// engine even before the manifest has been received.
        root_hash: Vec<u8>,
    },
    /// Resume a previously paused download by re-hydrating it from the DB.
    Resume { download_id: i64 },
    /// Rename an in-progress download: move its `.part` to `<new_name>.part`
    /// and repoint the in-memory + DB paths so it completes under the new name.
    Rename {
        download_id: i64,
        /// New file name (already sanitised to a bare file name by the handler).
        new_name: String,
    },
    /// Update a download's user priority in the live scheduler (already
    /// persisted to the DB by the handler).
    SetPriority {
        download_id: i64,
        /// Encoded priority (0 low, 1 medium, 2 high).
        priority: i64,
    },
}

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

/// State shared across all API handlers via `axum::extract::State`.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    /// Snapshot of the configuration at daemon startup.  Live-adjustable fields
    /// (bandwidth limits) are read from the token buckets instead; this value
    /// never changes after startup.
    pub config: Arc<Config>,
    /// Path of the config file on disk, used to detect pending changes in GET.
    pub config_path: Option<std::path::PathBuf>,
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
    /// Source of truth for the base/temporary bandwidth limits and the
    /// temporary-limit toggle; drives the two throttles above.
    pub bandwidth: Arc<crate::throttle::BandwidthState>,
    /// Handle to the Kad2 background task (only present with `emule-compat` feature).
    #[cfg(feature = "emule-compat")]
    pub kad_handle: rucio_emule::kad::task::KadHandle,
    /// Live whitelist of files currently being downloaded via eMule.
    #[cfg(feature = "emule-compat")]
    pub emule_active_downloads: rucio_emule::transfer::ActiveDownloads,
    /// Semaphore bounding eMule upload concurrency.  Held by `UploadContext`
    /// too; cloned here so the status endpoint can read `available_permits()`.
    #[cfg(feature = "emule-compat")]
    pub emule_upload_slots: Arc<tokio::sync::Semaphore>,
    /// Priority-aware admission gate bounding concurrent eMule downloads. Cloned
    /// here so the priority endpoint can re-rank a download still waiting for a
    /// slot; the download tasks hold the same gate to acquire/release slots.
    #[cfg(feature = "emule-compat")]
    pub emule_download_slots: Arc<crate::emule::PriorityAdmission>,
    /// Counter of inbound eMule TCP connections accepted since startup.
    #[cfg(feature = "emule-compat")]
    pub emule_inbound_connections: Arc<std::sync::atomic::AtomicU64>,
    /// Unix-seconds timestamp of the most recent inbound eMule TCP connection
    /// (`0` = none). Drives the recent-reachability connectivity verdict.
    #[cfg(feature = "emule-compat")]
    pub emule_last_inbound_at: Arc<std::sync::atomic::AtomicU64>,
    /// Registry of running eMule download tasks (download_id → stop flag), used
    /// by pause/cancel/resume to stop a task promptly without polling the DB.
    #[cfg(feature = "emule-compat")]
    pub emule_cancel: crate::emule::EmuleCancelRegistry,
    /// Sender to the eMule ed2k indexer. A directory added through the API is
    /// indexed inline by the handler, which the share watcher never sees, so the
    /// handler forwards each indexed file here to get it hashed for eMule too —
    /// otherwise its ed2k link wouldn't appear until the next restart. Also
    /// carries the eMule-hashing pending gauge shown separately in the UI.
    /// `None` when eMule is disabled at runtime.
    #[cfg(feature = "emule-compat")]
    pub ed2k_index: Option<crate::ed2k_index::Ed2kIndex>,
    /// External IP address as reported by UPnP gateway.  `None` when no gateway found.
    pub external_ip: crate::upnp::ExternalIp,
    /// Per-download live statistics (sources, pieces in flight, speed).
    pub live_stats: crate::live_stats::LiveStatsMap,
    /// Per-peer active-upload statistics (who is downloading from us, rate).
    pub upload_stats: Arc<crate::upload_stats::UploadRegistry>,
    /// Live notification toggles, updated by the settings handler and read by
    /// the notifier when deciding whether to record an event.
    pub notifications: Arc<crate::notifier::NotificationState>,
    /// Latched by any indexing producer when it enqueues work; the main loop
    /// clears it and fires an "indexing complete" notification once the pending
    /// count drains to 0.
    pub indexing_seen: Arc<std::sync::atomic::AtomicBool>,
    /// Live toggle for auto-clearing finished downloads from the history.
    /// Flipped by the settings handler; read by the download completion and
    /// cancel paths to decide whether to drop the finished entry immediately.
    pub auto_clear: Arc<std::sync::atomic::AtomicBool>,
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
    /// AutoNAT reachability state, surfaced for diagnostics alongside the class.
    pub reachability: rucio_core::protocol::node::Reachability,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the full API router.
pub fn router(state: AppState) -> Router {
    // Scalar builds its "try it" request URLs from the OpenAPI server URL. When
    // mounted under a subpath, point it at the prefix so requests hit
    // /rucio/api/... (the proxy strips the prefix) instead of /api/... at the
    // origin root. base_path is normalised to a trailing slash; drop it so the
    // server URL joins cleanly with the /api/... paths.
    let mut openapi = ApiDoc::openapi();
    let base = state.config.api.base_path.as_str();
    if base != "/" {
        openapi.servers = Some(vec![utoipa::openapi::Server::new(
            base.trim_end_matches('/'),
        )]);
    }

    let r = Router::new()
        .route("/api/ws", routing::get(ws::ws_handler))
        .route("/health", routing::get(health::get_health))
        .merge(Scalar::with_url("/api/docs", openapi).custom_html(SCALAR_HTML))
        .nest("/api/v1", v1_router());

    #[cfg(feature = "web-ui")]
    let r = r.fallback(static_files::serve);

    r.with_state(state)
}

fn v1_router() -> Router<AppState> {
    Router::new()
        // status & peers
        .route("/status", routing::get(status::get_status))
        .route("/peers", routing::get(status::get_peers))
        // shares
        .route("/shares", routing::get(shares::list_shares))
        .route("/shares/files", routing::get(shares::list_share_files))
        .route("/shares", routing::post(shares::add_share))
        .route("/shares/indexing", routing::get(shares::indexing_status))
        .route("/shares", routing::delete(shares::remove_shares_by_path))
        .route("/shares/{hash}", routing::delete(shares::remove_share))
        .route("/shares/{hash}/magnet", routing::get(shares::get_magnet))
        // downloads
        .route("/downloads", routing::get(downloads::list_downloads))
        .route("/downloads", routing::post(downloads::start_download))
        // Static path registered before /downloads/{id} so it never parses as an id.
        .route(
            "/downloads/history",
            routing::delete(downloads::clear_history),
        )
        .route(
            "/downloads/ed2k",
            routing::post(downloads::start_ed2k_download),
        )
        .route(
            "/downloads/{id}",
            routing::get(downloads::get_download).delete(downloads::delete_download),
        )
        .route(
            "/downloads/{id}/pieces",
            routing::get(downloads::get_download_pieces),
        )
        .route(
            "/downloads/{id}/cancel",
            routing::post(downloads::cancel_download),
        )
        .route(
            "/downloads/{id}/pause",
            routing::post(downloads::pause_download),
        )
        .route(
            "/downloads/{id}/resume",
            routing::post(downloads::resume_download),
        )
        .route(
            "/downloads/{id}/rename",
            routing::post(downloads::rename_download),
        )
        .route(
            "/downloads/{id}/category",
            routing::put(downloads::set_download_category),
        )
        .route(
            "/downloads/{id}/priority",
            routing::put(downloads::set_download_priority),
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
        // temporary speed-limit toggle (grouped under config)
        .route(
            "/config/temp-limit",
            routing::get(config::get_temp_limit).put(config::put_temp_limit),
        )
        // base speed limits (grouped under config)
        .route(
            "/config/limits",
            routing::get(config::get_limits).put(config::put_limits),
        )
        // notification settings + webhooks (configuration, grouped under config)
        .route(
            "/config/notifications",
            routing::get(config::get_notification_settings).put(config::put_notification_settings),
        )
        .route(
            "/config/notifications/webhooks",
            routing::get(config::get_webhooks).put(config::put_webhooks),
        )
        .route(
            "/config/notifications/webhooks/test",
            routing::post(config::test_webhook),
        )
        // download-history behaviour (auto-clear), grouped under config
        .route(
            "/config/downloads",
            routing::get(config::get_download_settings).put(config::put_download_settings),
        )
        // download categories
        .route(
            "/categories",
            routing::get(categories::list_categories).post(categories::create_category),
        )
        .route(
            "/categories/{id}",
            routing::put(categories::update_category).delete(categories::delete_category),
        )
        // pins (keep content available on purpose: fetch-and-retain)
        .route(
            "/pins",
            routing::get(pins::list_pins).post(pins::create_pin),
        )
        .route("/pins/{hash}", routing::delete(pins::delete_pin))
        .route(
            "/pins/{hash}/collection",
            routing::put(pins::set_pin_collection),
        )
        // subscriptions (cooperative pinning: mirror a peer's pin-set)
        .route(
            "/subscriptions",
            routing::get(subscriptions::list_subscriptions)
                .post(subscriptions::create_subscription),
        )
        .route(
            "/subscriptions/{peer_id}",
            routing::get(subscriptions::get_subscription)
                .delete(subscriptions::delete_subscription),
        )
        .route(
            "/subscriptions/{peer_id}/sync",
            routing::post(subscriptions::sync_subscription),
        )
        .route(
            "/subscriptions/{peer_id}/files",
            routing::get(subscriptions::list_subscription_files),
        )
        .route(
            "/subscriptions/{peer_id}/files/{hash}/refetch",
            routing::post(subscriptions::refetch_subscription_file),
        )
        .route(
            "/subscriptions/{peer_id}/collections",
            routing::put(subscriptions::set_subscription_collections),
        )
        .route(
            "/subscriptions/{peer_id}/evictable",
            routing::get(subscriptions::subscription_evictable),
        )
        // notification centre (runtime data: the bell + slideover)
        .route(
            "/notifications",
            routing::get(notifications::list_notifications)
                .delete(notifications::clear_notifications),
        )
        .route(
            "/notifications/read",
            routing::post(notifications::mark_all_read),
        )
        .route(
            "/notifications/{id}",
            routing::delete(notifications::delete_notification),
        )
        // metrics
        .route("/metrics", routing::get(metrics::get_metrics))
        // active uploads (peers downloading from us)
        .route("/uploads", routing::get(uploads::list_uploads))
        // emule
        .route("/emule/status", routing::get(emule::get_emule_status))
        .route(
            "/emule/bootstrap",
            routing::post(emule::post_emule_bootstrap),
        )
}

// ---------------------------------------------------------------------------
// Serve
// ---------------------------------------------------------------------------

/// Bind and serve the API on the address from config.
pub async fn serve(state: AppState, listen: &str) -> anyhow::Result<()> {
    // Build the router first — this generates the full OpenAPI spec and Scalar
    // docs, which is not instant. Doing it before the log (and before bind)
    // means that by the time we announce the server is ready, axum can start
    // accepting immediately, rather than the port being open while the router
    // is still being built and the WebSocket upgrade can't yet be served.
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| anyhow::anyhow!("failed to bind API on {listen}: {e}"))?;

    // The HTTP API, static frontend and the /api/ws WebSocket all become
    // reachable now — they share one router.
    tracing::info!(
        addr = listen,
        "API server listening — WebSocket /api/ws ready"
    );
    axum::serve(listener, app).await?;
    Ok(())
}
