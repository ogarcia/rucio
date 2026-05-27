//! REST API for the DHT indexer — same stack as the daemon (axum + utoipa +
//! scalar). Public read endpoints live under `/api/v1`; admin endpoints require
//! a bearer token and are disabled when none is configured.

use std::time::Instant;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_scalar::{Scalar, Servable as _};

use super::db::{self, Db, HashRow, Stats};

/// State shared across all index API handlers.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    /// Bearer token guarding the admin endpoints. `None` disables them.
    pub token: Option<String>,
    pub started_at: Instant,
    pub retention_days: i64,
}

const SCALAR_HTML: &str = r#"<!doctype html>
<html>
  <head>
    <title>Rucio Index API</title>
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
        title = "Rucio Index API",
        version = "1",
        description = "Search API over the passive Rucio DHT provider-record index."
    ),
    paths(get_health, search_records, list_records, admin_stats, admin_prune),
    components(schemas(HashRow, Stats, HealthResponse, RecordsResponse, PruneResponse))
)]
struct ApiDoc;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", routing::get(get_health))
        .merge(Scalar::with_url("/api/docs", ApiDoc::openapi()).custom_html(SCALAR_HTML))
        .nest("/api/v1", v1_router())
        .with_state(state)
}

fn v1_router() -> Router<AppState> {
    Router::new()
        .route("/search", routing::get(search_records))
        .route("/records", routing::get(list_records))
        .route("/admin/stats", routing::get(admin_stats))
        .route("/admin/prune", routing::post(admin_prune))
}

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub uptime_secs: u64,
}

#[derive(Serialize, ToSchema)]
pub struct RecordsResponse {
    pub records: Vec<HashRow>,
    pub count: usize,
}

#[derive(Serialize, ToSchema)]
pub struct PruneResponse {
    pub deleted: u64,
}

#[derive(Deserialize, IntoParams)]
pub struct SearchParams {
    /// Hex prefix of the content hash to match.
    pub q: String,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Deserialize, IntoParams)]
pub struct PageParams {
    pub limit: Option<i64>,
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

#[utoipa::path(
    get, path = "/health",
    responses((status = 200, body = HealthResponse))
)]
async fn get_health(State(s): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        uptime_secs: s.started_at.elapsed().as_secs(),
    })
}

#[utoipa::path(
    get, path = "/api/v1/search",
    params(SearchParams),
    responses((status = 200, body = RecordsResponse))
)]
async fn search_records(State(s): State<AppState>, Query(p): Query<SearchParams>) -> Response {
    let (limit, offset) = page(p.limit, p.offset);
    match db::search(&s.db, &p.q, limit, offset).await {
        Ok(records) => Json(RecordsResponse {
            count: records.len(),
            records,
        })
        .into_response(),
        Err(e) => internal(e),
    }
}

#[utoipa::path(
    get, path = "/api/v1/records",
    params(PageParams),
    responses((status = 200, body = RecordsResponse))
)]
async fn list_records(State(s): State<AppState>, Query(p): Query<PageParams>) -> Response {
    let (limit, offset) = page(p.limit, p.offset);
    match db::search(&s.db, "", limit, offset).await {
        Ok(records) => Json(RecordsResponse {
            count: records.len(),
            records,
        })
        .into_response(),
        Err(e) => internal(e),
    }
}

#[utoipa::path(
    get, path = "/api/v1/admin/stats",
    responses(
        (status = 200, body = Stats),
        (status = 401, description = "missing/invalid token"),
        (status = 403, description = "admin disabled")
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

#[utoipa::path(
    post, path = "/api/v1/admin/prune",
    responses(
        (status = 200, body = PruneResponse),
        (status = 401, description = "missing/invalid token"),
        (status = 403, description = "admin disabled")
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
