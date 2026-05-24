//! GET /api/v1/config
//! PUT /api/v1/config

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rucio_core::api::config::{
    ApiConfig, ConfigResponse, NetworkConfig, NodeConfig, StorageConfig,
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
        },
        storage: StorageConfig {
            download_dir: cfg.storage.download_dir.to_string_lossy().into_owned(),
            temp_dir: cfg.storage.temp_dir.to_string_lossy().into_owned(),
            database_path: cfg.storage.database_path.to_string_lossy().into_owned(),
        },
    })
}

/// Update configuration
///
/// Persists a new configuration to the config file on disk. The daemon must be restarted
/// for most changes to take effect.
///
/// The request body should be the full object returned by `GET /api/v1/config` with the
/// desired fields modified. Fields that are read-only at runtime (`node.identity_path`,
/// `api.listen`) are accepted in the body but silently ignored — they are preserved from
/// the running configuration.
///
/// **Writable fields**
/// - `node.listen_addrs` — P2P listen multiaddrs.
/// - `network.bootstrap_peers` — DHT bootstrap peers.
/// - `storage.download_dir` — completed downloads destination.
/// - `storage.temp_dir` — in-progress `.part` files location.
#[utoipa::path(
    put,
    path = "/api/v1/config",
    request_body = ConfigResponse,
    responses(
        (status = 204, description = "Configuration saved to disk. Restart the daemon for changes to take effect."),
        (status = 500, description = "Failed to write the configuration file.")
    )
)]
pub async fn put_config(
    State(state): State<AppState>,
    Json(req): Json<ConfigResponse>,
) -> StatusCode {
    // Build a new Config from the request and persist it.
    // Fields not exposed in the API (e.g. api.token) are preserved from
    // the running config.
    let mut new_cfg = (*state.config).clone();
    new_cfg.node.listen_addrs = req.node.listen_addrs;
    new_cfg.network.bootstrap_peers = req.network.bootstrap_peers;
    new_cfg.storage.download_dir = req.storage.download_dir.into();
    new_cfg.storage.temp_dir = req.storage.temp_dir.into();
    // identity_path and api.listen intentionally not writable at runtime

    match new_cfg.save() {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("Failed to save config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
