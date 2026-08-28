//! JSON API + panel routing for the stats role. Mounted onto the shared HTTP
//! server by [`crate::http`]; the endpoints are public read-only (resource
//! usage is not sensitive), so there is no bearer token here.

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use utoipa::{IntoParams, OpenApi};

use super::Db;
use super::query::{self, HostInfo, Summary};

/// State shared across the stats handlers.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    /// The index DB, when the node also runs the indexer role. Lets the panel
    /// render the search-index counters next to resource usage. `None` when the
    /// indexer is disabled or not compiled in.
    #[cfg(feature = "indexer")]
    pub index_db: Option<crate::indexer::Db>,
}

#[derive(OpenApi)]
#[openapi(
    paths(get_resources, get_host),
    components(schemas(Summary, HostInfo)),
    tags((
        name = "Stats",
        description = "Bootstrap node statistics: resource usage and, when the \
                       indexer role runs, the search-index counters"
    ))
)]
struct ApiDoc;

/// The stats role's OpenAPI spec, for the shared server's merged docs.
pub fn openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

pub fn router(state: AppState) -> Router {
    Router::new()
        // Server-rendered dashboard.
        .route("/stats", get(super::web::panel))
        // JSON API (explicit full paths so they merge with the indexer's).
        .route("/api/v1/stats/resources", get(get_resources))
        .route("/api/v1/stats/host", get(get_host))
        .with_state(state)
}

/// Query parameter selecting the aggregation window.
#[derive(Deserialize, IntoParams)]
pub struct WindowParam {
    /// Window in seconds to aggregate over. `0` or omitted = all recorded
    /// history.
    pub window: Option<i64>,
}

fn internal(e: anyhow::Error) -> Response {
    tracing::warn!("stats query error: {e}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

/// Aggregate resource usage over a window.
///
/// Returns the peaks (peers, connections, RSS, CPU, load, FDs, threads) and
/// running totals (traffic, connection churn) you size hardware from. Peaks are
/// `NULL` until at least one sample exists; `/proc`-derived fields are `NULL` on
/// non-Linux hosts.
#[utoipa::path(
    get, path = "/api/v1/stats/resources",
    tag = "Stats",
    params(WindowParam),
    responses((status = 200, description = "Windowed resource-usage summary", body = Summary))
)]
async fn get_resources(State(s): State<AppState>, Query(p): Query<WindowParam>) -> Response {
    let window = p.window.unwrap_or(0).max(0);
    match query::summary(&s.db, window).await {
        Ok(sm) => Json(sm).into_response(),
        Err(e) => internal(e),
    }
}

/// The box this node is running on (CPU count, RAM, kernel).
///
/// Frames the summary numbers. Returns `404` before the first sample cycle has
/// recorded the host facts.
#[utoipa::path(
    get, path = "/api/v1/stats/host",
    tag = "Stats",
    responses(
        (status = 200, description = "Host facts", body = HostInfo),
        (status = 404, description = "Host facts not recorded yet")
    )
)]
async fn get_host(State(s): State<AppState>) -> Response {
    match query::host_info(&s.db).await {
        Ok(Some(h)) => Json(h).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "host info not recorded yet").into_response(),
        Err(e) => internal(e),
    }
}
