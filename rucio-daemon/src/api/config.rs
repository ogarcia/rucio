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
            max_upload_tasks: cfg.network.max_upload_tasks,
        },
        storage: StorageConfig {
            download_dir: cfg.storage.download_dir.to_string_lossy().into_owned(),
            temp_dir: cfg.storage.temp_dir.to_string_lossy().into_owned(),
            database_path: cfg.storage.database_path.to_string_lossy().into_owned(),
        },
        emule: EmuleConfig {
            enabled: cfg.emule.enabled,
            temp_dir: cfg.emule.temp_dir.to_string_lossy().into_owned(),
            udp_port: cfg.emule.udp_port,
            tcp_port: cfg.emule.tcp_port,
            external_ip: cfg.emule.external_ip.map(|ip| ip.to_string()),
            download_slots_per_file: cfg.emule.download_slots_per_file,
            max_upload_slots: cfg.emule.max_upload_slots,
            max_concurrent_downloads: cfg.emule.max_concurrent_downloads,
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
/// - `node.listen_addrs`, `network.bootstrap_peers`, `storage.*`, `emule.*`
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
    new_cfg.network.max_upload_tasks = req.network.max_upload_tasks.max(1);
    new_cfg.storage.download_dir = req.storage.download_dir.into();
    new_cfg.storage.temp_dir = req.storage.temp_dir.into();
    new_cfg.emule.enabled = req.emule.enabled;
    new_cfg.emule.temp_dir = req.emule.temp_dir.into();
    new_cfg.emule.udp_port = req.emule.udp_port;
    new_cfg.emule.tcp_port = req.emule.tcp_port;
    new_cfg.emule.external_ip = req.emule.external_ip.and_then(|s| s.parse().ok());
    new_cfg.emule.download_slots_per_file = req.emule.download_slots_per_file.clamp(1, 50);
    new_cfg.emule.max_upload_slots = req.emule.max_upload_slots.clamp(1, 50);
    new_cfg.emule.max_concurrent_downloads = req.emule.max_concurrent_downloads.clamp(1, 50);
    // identity_path and api.listen intentionally not writable at runtime

    match new_cfg.save() {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("Failed to save config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
