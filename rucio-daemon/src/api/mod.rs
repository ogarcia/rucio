//! axum REST API server for the Rucio daemon.
//!
//! All handlers live in submodules; this module owns the router, the shared
//! [`AppState`], and the [`serve`] entry point.

pub mod config;
pub mod downloads;
pub mod search;
pub mod shares;
pub mod status;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::{Router, routing};
use tokio::sync::RwLock;
use tokio::sync::mpsc;

use crate::config::Config;
use crate::db::Db;
use crate::node::messages::NodeCmd;
use rucio_core::api::search::SearchResultResponse;

// ---------------------------------------------------------------------------
// SearchStore
// ---------------------------------------------------------------------------

/// In-memory accumulator for search results keyed by query_id.
#[derive(Debug)]
pub struct SearchEntry {
    pub results: Vec<SearchResultResponse>,
    /// Set to false after the TTL window closes.
    pub pending: bool,
    /// Monotonic instant when the query was started (for TTL expiry).
    pub started_at: Instant,
}

pub type SearchStore = Arc<RwLock<HashMap<String, SearchEntry>>>;

/// How long to keep a search entry open for incoming results.
pub const SEARCH_WINDOW_SECS: u64 = 30;

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

/// State shared across all API handlers via `axum::extract::State`.
///
/// Wrapped in `Arc` so axum can clone it cheaply into every request.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub config: Arc<Config>,
    /// Channel to send commands to the libp2p node task.
    pub node_cmd: mpsc::Sender<NodeCmd>,
    /// Monotonic instant when the daemon started (for uptime calculation).
    pub started_at: Instant,
    /// Current node status; updated by the node event loop.
    pub node_status: Arc<RwLock<NodeStatus>>,
    /// In-memory search result accumulator.
    pub search_store: SearchStore,
}

/// Live node status kept in memory and updated by the event loop.
#[derive(Debug, Clone, Default)]
pub struct NodeStatus {
    pub peer_id: String,
    pub connected_peers: usize,
    pub listen_addrs: Vec<String>,
    pub node_class: rucio_core::protocol::node::NodeClass,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build the full API router.
pub fn router(state: AppState) -> Router {
    Router::new().nest("/api/v1", v1_router()).with_state(state)
}

fn v1_router() -> Router<AppState> {
    Router::new()
        // status & peers
        .route("/status", routing::get(status::get_status))
        .route("/peers", routing::get(status::get_peers))
        // shares
        .route("/shares", routing::get(shares::list_shares))
        .route("/shares", routing::post(shares::add_share))
        .route("/shares", routing::delete(shares::remove_shares_by_path))
        .route("/shares/{hash}", routing::delete(shares::remove_share))
        // downloads
        .route("/downloads", routing::get(downloads::list_downloads))
        .route("/downloads", routing::post(downloads::start_download))
        .route(
            "/downloads/{id}",
            routing::delete(downloads::cancel_download),
        )
        // search
        .route("/search", routing::post(search::start_search))
        .route("/search/{query_id}", routing::get(search::get_results))
        // config
        .route("/config", routing::get(config::get_config))
        .route("/config", routing::put(config::put_config))
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
