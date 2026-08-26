//! REST API for the DHT indexer — same stack as the daemon (axum + utoipa).
//! Public read endpoints live under `/api/v1`; admin endpoints require a bearer
//! token and are disabled when none is configured. The router and its OpenAPI
//! spec are composed with the other roles' onto one server by [`crate::http`],
//! which owns the listener, the `/health` probe and the merged Scalar docs.

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing,
};
use serde::{Deserialize, Serialize};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{IntoParams, Modify, OpenApi, ToSchema};

use super::db::{self, Db, HashRow, Stats};

/// State shared across all index API handlers.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    /// Bearer token guarding the admin endpoints. `None` disables them.
    pub token: Option<String>,
    pub retention_days: i64,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Rucio Index API",
        version = "1",
        description = "\
Read-only search API over the passive Rucio DHT indexer.

A `rucio-bootstrap` node built with the `indexer` feature watches provider \
records announced on the Kademlia DHT and stores each `(hash, provider)` pair, \
optionally enriched with the file name and size resolved from announcing \
peers. This API exposes that index:

- **Public** endpoints under `/api/v1` (`/search`, `/records`) need no auth.
- **Admin** endpoints under `/api/v1/admin` require a bearer token and are \
  disabled entirely when the node has no `api_token` configured.

Timestamps are Unix seconds. Pagination is `limit` (1–500, default 50) plus \
`offset` (default 0)."
    ),
    paths(search_records, list_records, admin_stats, admin_prune),
    components(schemas(HashRow, Stats, RecordsResponse, PruneResponse)),
    modifiers(&SecurityAddon),
    tags(
        (name = "Search", description = "Query the provider-record index"),
        (name = "Admin", description = "Maintenance endpoints (bearer token required)")
    )
)]
struct ApiDoc;

/// The indexer's OpenAPI spec, for merging into the shared server's docs.
pub fn openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

/// Registers the `bearer_token` security scheme referenced by the admin paths.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi
            .components
            .get_or_insert_with(utoipa::openapi::Components::default);
        components.add_security_scheme(
            "bearer_token",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
        );
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        // Server-rendered search front-end (no JS): landing + results pages.
        .route("/", routing::get(super::web::landing))
        .route("/search", routing::get(super::web::search_page))
        // JSON API. Explicit full paths (rather than a nested wildcard) so this
        // router merges cleanly with the other roles' `/api/v1/*` routes.
        .route("/api/v1/search", routing::get(search_records))
        .route("/api/v1/records", routing::get(list_records))
        .route("/api/v1/admin/stats", routing::get(admin_stats))
        .route("/api/v1/admin/prune", routing::post(admin_prune))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

/// A page of indexed hashes.
#[derive(Serialize, ToSchema)]
pub struct RecordsResponse {
    /// The matching records, most recently announced first.
    pub records: Vec<HashRow>,
    /// Number of records in this page (`records.len()`, not the global total).
    pub count: usize,
}

/// Result of an admin prune.
#[derive(Serialize, ToSchema)]
pub struct PruneResponse {
    /// Number of provider records deleted.
    pub deleted: u64,
}

/// Query parameters for `/api/v1/search`.
#[derive(Deserialize, IntoParams)]
pub struct SearchParams {
    /// Search query. Matched two ways:
    ///
    /// - as a hex **prefix** of the content hash (single whitespace-free token);
    /// - against the indexed **file name**, split into whitespace-separated
    ///   terms that must *all* appear as substrings. Matching is case- and
    ///   accent-insensitive (folded like the rucio network), so
    ///   `ghost in the shell` matches `Ghost.in.the.Shell.ARISE...` and
    ///   `camion` matches `Camión...`.
    ///
    /// An empty value returns the most recently announced records.
    pub q: String,
    /// Result order: `newest` (default), `oldest`, `providers` (most sources
    /// first — availability), or `size` (largest first).
    pub sort: Option<String>,
    /// Maximum records to return. Default 50, clamped to 1–500.
    pub limit: Option<i64>,
    /// Records to skip, for pagination. Default 0.
    pub offset: Option<i64>,
}

/// Pagination parameters for `/api/v1/records`.
#[derive(Deserialize, IntoParams)]
pub struct PageParams {
    /// Maximum records to return. Default 50, clamped to 1–500.
    pub limit: Option<i64>,
    /// Records to skip, for pagination. Default 0.
    pub offset: Option<i64>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn page(limit: Option<i64>, offset: Option<i64>) -> (i64, i64) {
    (
        limit.unwrap_or(50).clamp(1, 500),
        offset.unwrap_or(0).max(0),
    )
}

fn internal(e: anyhow::Error) -> Response {
    tracing::warn!("index query error: {e}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

/// Returns `Some(rejection)` when the request must be denied, `None` when the
/// bearer token matches and the admin request may proceed. With no token
/// configured the admin endpoints are denied (disabled), not opened.
fn reject_admin(state: &AppState, headers: &HeaderMap) -> Option<Response> {
    let Some(expected) = state.token.as_deref() else {
        return Some(
            (
                StatusCode::FORBIDDEN,
                "admin endpoints disabled: no API token configured",
            )
                .into_response(),
        );
    };
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if provided == Some(expected) {
        None
    } else {
        Some((StatusCode::UNAUTHORIZED, "invalid or missing bearer token").into_response())
    }
}

/// Search the index by hash prefix or file name.
///
/// Each result is one distinct content hash, aggregating its provider count and
/// first/last-seen timestamps, plus the file name and size when the hash has
/// been enriched. Results are ordered most-recently-announced first. See the
/// `q` parameter for the matching rules.
#[utoipa::path(
    get, path = "/api/v1/search",
    tag = "Search",
    params(SearchParams),
    responses((status = 200, description = "Matching records (newest first)", body = RecordsResponse))
)]
async fn search_records(State(s): State<AppState>, Query(p): Query<SearchParams>) -> Response {
    let (limit, offset) = page(p.limit, p.offset);
    let sort = db::Sort::parse(p.sort.as_deref().unwrap_or(""));
    match db::search(&s.db, &p.q, sort, limit, offset).await {
        Ok(records) => Json(RecordsResponse {
            count: records.len(),
            records,
        })
        .into_response(),
        Err(e) => internal(e),
    }
}

/// List the most recently announced hashes.
///
/// Equivalent to `/search` with an empty query: returns every indexed hash,
/// newest first, paginated. Useful for browsing or exporting the index.
#[utoipa::path(
    get, path = "/api/v1/records",
    tag = "Search",
    params(PageParams),
    responses((status = 200, description = "Indexed records (newest first)", body = RecordsResponse))
)]
async fn list_records(State(s): State<AppState>, Query(p): Query<PageParams>) -> Response {
    let (limit, offset) = page(p.limit, p.offset);
    match db::search(&s.db, "", db::Sort::default(), limit, offset).await {
        Ok(records) => Json(RecordsResponse {
            count: records.len(),
            records,
        })
        .into_response(),
        Err(e) => internal(e),
    }
}

/// Aggregate index statistics.
///
/// Counts over the whole index: total records, distinct hashes and providers,
/// how many hashes are enriched with a name/size, and the oldest/newest
/// timestamps. Requires a bearer token (`Authorization: Bearer <token>`).
#[utoipa::path(
    get, path = "/api/v1/admin/stats",
    tag = "Admin",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Index counters", body = Stats),
        (status = 401, description = "Missing or invalid bearer token"),
        (status = 403, description = "Admin endpoints disabled (no token configured)")
    )
)]
async fn admin_stats(State(s): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(resp) = reject_admin(&s, &headers) {
        return resp;
    }
    match db::stats(&s.db).await {
        Ok(st) => Json(st).into_response(),
        Err(e) => internal(e),
    }
}

/// Prune stale records now.
///
/// Deletes provider records not refreshed within the node's configured
/// `retention_days` and returns how many were removed. This also runs
/// periodically in the background; the endpoint forces it on demand. Requires a
/// bearer token (`Authorization: Bearer <token>`).
#[utoipa::path(
    post, path = "/api/v1/admin/prune",
    tag = "Admin",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Records pruned", body = PruneResponse),
        (status = 401, description = "Missing or invalid bearer token"),
        (status = 403, description = "Admin endpoints disabled (no token configured)")
    )
)]
async fn admin_prune(State(s): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(resp) = reject_admin(&s, &headers) {
        return resp;
    }
    match db::prune(&s.db, s.retention_days).await {
        Ok(deleted) => Json(PruneResponse { deleted }).into_response(),
        Err(e) => internal(e),
    }
}
