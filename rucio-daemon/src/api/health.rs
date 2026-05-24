//! `GET /health` — liveness probe for container orchestrators.

use axum::Json;
use axum::extract::State;

use rucio_core::api::metrics::HealthResponse;

use crate::api::AppState;

/// Return daemon health.
///
/// Always returns `200 OK` with `{ "status": "ok", "version": "..." }` as
/// long as the daemon process is running.  Container health-check tools
/// (Docker `HEALTHCHECK`, Kubernetes liveness probe) can use this endpoint
/// without any authentication.
#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    summary = "Liveness probe",
    description = "Returns 200 OK whenever the daemon is running. \
                   Suitable for Docker HEALTHCHECK and Kubernetes liveness probes.",
    responses(
        (status = 200, description = "Daemon is alive", body = HealthResponse),
    )
)]
pub async fn get_health(State(_state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}
