//! `GET /api/v1/metrics` — session and lifetime transfer counters.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

use rucio_core::api::metrics::{MetricsResponse, TotalMetrics};

use crate::api::AppState;

/// Transfer metrics
///
/// A point-in-time snapshot of transfer activity, in three parts:
///
/// - `session` — in-memory counters since the last daemon start: bytes moved,
///   current speeds (a 5-second rolling average) and chunk tallies. Reset to
///   zero on restart.
/// - `total` — the same byte and chunk counters accumulated across every
///   session, persisted in SQLite so they survive restarts.
/// - `download_conns` / `upload_conns` — live connection gauges: the number of
///   active `(file, peer)` transfer pairs in each direction right now. A file
///   pulled from three peers is three download connections; a file served to
///   two peers is two upload connections. Both directions are counted the same
///   way, so the two figures are directly comparable. Note these count
///   *connections*, not transfers — unlike the node status endpoint's
///   `active_downloads` / `active_uploads`, which count one per file regardless
///   of how many connections it spans.
#[utoipa::path(
    get,
    path = "/api/v1/metrics",
    tag = "node",
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

    // Live connection gauges (not accumulated): (file, peer) transfer pairs in
    // each direction, counted the same way so the two are comparable.
    let upload_conns = state.upload_stats.active_connection_count();
    // Split download connections by network: eMule downloads carry negative ids.
    let (download_conns, download_conns_emule_n) = {
        let ls = state.live_stats.read().await;
        let mut total = 0usize;
        let mut emule = 0usize;
        for (id, s) in ls.iter() {
            let n = s.sources_active as usize;
            total += n;
            if *id < 0 {
                emule += n;
            }
        }
        (total, emule)
    };

    // Per-network session split, only when the eMule subsystem is active: the
    // eMule-only slice (the rucio slice is `session` minus this, derived in the
    // UI). `None` ⇒ the view shows a single, combined column.
    let session_emule = state.emule_metrics.as_ref().map(|m| m.session_snapshot());
    let (download_conns_emule, upload_conns_emule) = if session_emule.is_some() {
        (
            Some(download_conns_emule_n),
            Some(state.upload_stats.active_connection_count_emule()),
        )
    } else {
        (None, None)
    };

    Ok(Json(MetricsResponse {
        session,
        total,
        download_conns,
        upload_conns,
        session_emule,
        download_conns_emule,
        upload_conns_emule,
    }))
}
