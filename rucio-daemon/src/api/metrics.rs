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
    tag = "node",
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

    // The persisted totals only advance on the 30 s flush, so on their own they
    // step once per flush and look frozen between flushes. Overlay the
    // not-yet-flushed in-memory delta so the historical figure tracks live
    // activity. Read the delta BEFORE loading the row: should the flush task
    // interleave, this biases the (benign, display-only) race toward a
    // transient overshoot that self-corrects on the next poll rather than a
    // backwards dip. The overlay does not advance the snapshot, so the flush
    // still persists this delta exactly once.
    let unflushed = state.metrics.unflushed_delta();
    let mut total: TotalMetrics = crate::db::metrics::load(&state.db).await.map_err(|e| {
        tracing::warn!("metrics DB read error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    total.add(&unflushed);
    total.ratio =
        rucio_core::api::metrics::share_ratio(total.uploaded_bytes, total.downloaded_bytes);

    Ok(Json(MetricsResponse { session, total }))
}
