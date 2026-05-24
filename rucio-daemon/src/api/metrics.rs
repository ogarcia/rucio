//! `GET /api/v1/metrics` — session and lifetime transfer counters.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

use rucio_core::api::metrics::{MetricsResponse, TotalMetrics};

use crate::api::AppState;

/// Retrieve transfer metrics.
///
/// Returns two counter sets:
///
/// - `session`: in-memory counters since the last daemon start (bytes,
///   speeds, chunks).  Speeds are a 5-second rolling average.
/// - `total`: cumulative counters persisted in SQLite across restarts.
#[utoipa::path(
    get,
    path = "/api/v1/metrics",
    tag = "metrics",
    summary = "Transfer metrics",
    description = "Session counters (in-memory, since last start) and lifetime totals (SQLite).",
    responses(
        (status = 200, description = "Metrics retrieved", body = MetricsResponse),
        (status = 500, description = "Database error reading totals"),
    )
)]
pub async fn get_metrics(
    State(state): State<AppState>,
) -> Result<Json<MetricsResponse>, StatusCode> {
    let session = state.metrics.session_snapshot();

    let total: TotalMetrics = crate::db::metrics::load(&state.db).await.map_err(|e| {
        tracing::warn!("metrics DB read error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(MetricsResponse { session, total }))
}
