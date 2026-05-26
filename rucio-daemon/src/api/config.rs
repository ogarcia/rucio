//! GET /api/v1/config
//! PUT /api/v1/config

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rucio_core::api::config::{
    ApiConfig, ConfigResponse, EmuleConfig, NetworkConfig, NodeConfig, StorageConfig,
};

use crate::api::AppState;

/// Get configuration
///
/// Returns the daemon's current effective configuration — the values actually in use,
/// after applying environment variable overrides on top of the config file.
///
/// Read-only fields (`identity_path`, `api.listen`) are included for information but
/// cannot be changed via `PUT /api/v1/config`.
#[utoipa::path(
    get,
    path = "/api/v1/config",
    responses(
        (status = 200, description = "Current effective configuration.", body = ConfigResponse)
    )
)]
pub async fn get_config(State(state): State<AppState>) -> Json<ConfigResponse> {
    let cfg = &state.config;
    Json(ConfigResponse {
        node: NodeConfig {
            identity_path: cfg.node.identity_path.to_string_lossy().into_owned(),
            listen_addrs: cfg.node.listen_addrs.clone(),
        },
        api: ApiConfig {
            listen: cfg.api.listen.clone(),
        },
        network: NetworkConfig {
            bootstrap_peers: cfg.network.bootstrap_peers.clone(),
            upload_limit_kbps: cfg.network.upload_limit_kbps,
            download_limit_kbps: cfg.network.download_limit_kbps,
        },
        storage: StorageConfig {
            download_dir: cfg.storage.download_dir.to_string_lossy().into_owned(),
            temp_dir: cfg.storage.temp_dir.to_string_lossy().into_owned(),
            database_path: cfg.storage.database_path.to_string_lossy().into_owned(),
        },
        emule: EmuleConfig {
            max_parallel_peers: cfg.emule.max_parallel_peers,
        },
    })
}

/// Update configuration
///
/// Persists a new configuration to the config file on disk.
///
/// **Changes applied immediately (no restart required)**
/// - `network.upload_limit_kbps` — upload bandwidth cap (0 = unlimited).
/// - `network.download_limit_kbps` — download bandwidth cap (0 = unlimited).
///
/// **Changes that require a daemon restart**
/// - `node.listen_addrs`, `network.bootstrap_peers`, `storage.*`
///
/// Read-only fields (`node.identity_path`, `api.listen`) are silently ignored.
#[utoipa::path(
    put,
    path = "/api/v1/config",
    request_body = ConfigResponse,
    responses(
        (status = 204, description = "Configuration saved. Bandwidth limits applied immediately; other changes require a restart."),
        (status = 500, description = "Failed to write the configuration file.")
    )
)]
pub async fn put_config(
    State(state): State<AppState>,
    Json(req): Json<ConfigResponse>,
) -> StatusCode {
    // Apply bandwidth limits immediately to the running token buckets.
    state
        .upload_throttle
        .set_rate(req.network.upload_limit_kbps);
    state
        .download_throttle
        .set_rate(req.network.download_limit_kbps);

    // Build a new Config from the request and persist it.
    // Fields not exposed in the API (e.g. api.token) are preserved from
    // the running config.
    let mut new_cfg = (*state.config).clone();
    new_cfg.node.listen_addrs = req.node.listen_addrs;
    new_cfg.network.bootstrap_peers = req.network.bootstrap_peers;
    new_cfg.network.upload_limit_kbps = req.network.upload_limit_kbps;
    new_cfg.network.download_limit_kbps = req.network.download_limit_kbps;
    new_cfg.storage.download_dir = req.storage.download_dir.into();
    new_cfg.storage.temp_dir = req.storage.temp_dir.into();
    new_cfg.emule.max_parallel_peers = req.emule.max_parallel_peers.clamp(1, 50);
    // identity_path and api.listen intentionally not writable at runtime

    match new_cfg.save() {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("Failed to save config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
