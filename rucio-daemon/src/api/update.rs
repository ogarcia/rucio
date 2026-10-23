//! Release update-check endpoints under `/api/v1/update`.
//!
//! `GET` returns the cached outcome without touching the network (so opening
//! the panel never phones home); `POST /check` is the panel's manual button and
//! forces a fresh check, subject to the updater's 10-minute floor.

use axum::Json;
use axum::extract::State;
use rucio_core::api::update::UpdateInfo;

use crate::api::AppState;

/// Get the cached update-check result.
///
/// Returns the last known comparison of the running version against the latest
/// GitHub release. Never contacts GitHub — it reflects the most recent
/// automatic or manual check (empty `latest` until the first check runs).
#[utoipa::path(
    get,
    path = "/api/v1/update",
    tag = "status",
    responses((status = 200, description = "Cached update-check result", body = UpdateInfo)),
)]
pub async fn get_update(State(state): State<AppState>) -> Json<UpdateInfo> {
    Json(state.updater.snapshot().await)
}

/// Check for updates now.
///
/// Forces a fresh check against GitHub and returns the result. Always available
/// regardless of the automatic-check toggle (an explicit user action), but a
/// second call within ten minutes returns the cached result instead of hitting
/// GitHub again.
#[utoipa::path(
    post,
    path = "/api/v1/update/check",
    tag = "status",
    responses((status = 200, description = "Fresh (or floor-cached) update-check result", body = UpdateInfo)),
)]
pub async fn check_update(State(state): State<AppState>) -> Json<UpdateInfo> {
    Json(state.updater.refresh().await)
}
