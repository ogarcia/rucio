//! GET /api/v1/config
//! PUT /api/v1/config

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rucio_core::api::config::{
    ApiConfig, ConfigResponse, ConfigSnapshot, EmuleConfig, NetworkConfig, NodeConfig,
    StorageConfig, TempLimitRequest, TempLimitStatus,
};

use crate::api::AppState;
use crate::config::Config;

/// Get configuration
///
/// Returns the daemon's current effective configuration.
///
/// - Bandwidth limits (`network.upload_limit_kbps`, `network.download_limit_kbps`) reflect
///   the live values in use — they update immediately when changed via PUT.
/// - All other fields show the values from startup.
/// - If any restart-required field was changed on disk since startup, the response includes
///   a `pending` object with the full on-disk configuration.
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
    let cfg = &*state.config;

    // Bandwidth limits are live — read them from BandwidthState (the source of
    // truth), not from the startup config snapshot, so they reflect any PUT
    // made since startup. Report the *base* (normal) caps, not the buckets'
    // current rate, which may carry a temporary override.
    let current = build_snapshot(
        cfg,
        state.bandwidth.base_upload_kbps(),
        state.bandwidth.base_download_kbps(),
        state.bandwidth.temp_upload_kbps(),
        state.bandwidth.temp_download_kbps(),
    );

    // Compare startup config with the current on-disk file.  Any restart-required
    // field that differs is surfaced as a `pending` snapshot.
    // Config::load(None) resolves to the default path, same as Config::save(),
    // so this works even when the daemon was started without an explicit --config.
    let pending = Config::load(state.config_path.as_deref())
        .ok()
        .filter(|disk| {
            disk.node != cfg.node
                || disk.network.bootstrap_peers != cfg.network.bootstrap_peers
                || disk.network.max_upload_tasks != cfg.network.max_upload_tasks
                || disk.storage != cfg.storage
                || disk.emule != cfg.emule
        })
        .map(|disk| {
            Box::new(build_snapshot(
                &disk,
                disk.network.upload_limit_kbps,
                disk.network.download_limit_kbps,
                disk.network.temp_upload_limit_kbps,
                disk.network.temp_download_limit_kbps,
            ))
        });

    Json(ConfigResponse { current, pending })
}

fn build_snapshot(
    cfg: &Config,
    upload_limit_kbps: u64,
    download_limit_kbps: u64,
    temp_upload_limit_kbps: u64,
    temp_download_limit_kbps: u64,
) -> ConfigSnapshot {
    ConfigSnapshot {
        node: NodeConfig {
            identity_path: cfg.node.identity_path.to_string_lossy().into_owned(),
            listen_addrs: cfg.node.listen_addrs.clone(),
        },
        api: ApiConfig {
            listen: cfg.api.listen.clone(),
        },
        network: NetworkConfig {
            bootstrap_peers: cfg.network.bootstrap_peers.clone(),
            upload_limit_kbps,
            download_limit_kbps,
            temp_upload_limit_kbps,
            temp_download_limit_kbps,
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
    }
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
/// - `node.listen_addrs`, `network.bootstrap_peers`, `network.max_upload_tasks`,
///   `storage.*`, `emule.*`
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
    let c = req.current;

    // Apply bandwidth limits immediately via BandwidthState, which recomputes
    // the effective rate (honouring the temporary-limit toggle) and pushes it
    // to the running token buckets.
    state
        .bandwidth
        .set_base(c.network.upload_limit_kbps, c.network.download_limit_kbps);
    state.bandwidth.set_temp(
        c.network.temp_upload_limit_kbps,
        c.network.temp_download_limit_kbps,
    );

    // Build a new Config from the request and persist it.
    // Fields not exposed in the API (e.g. api.token) are preserved from
    // the startup config snapshot.
    let mut new_cfg = (*state.config).clone();
    new_cfg.node.listen_addrs = c.node.listen_addrs;
    new_cfg.network.bootstrap_peers = c.network.bootstrap_peers;
    new_cfg.network.upload_limit_kbps = c.network.upload_limit_kbps;
    new_cfg.network.download_limit_kbps = c.network.download_limit_kbps;
    new_cfg.network.temp_upload_limit_kbps = c.network.temp_upload_limit_kbps;
    new_cfg.network.temp_download_limit_kbps = c.network.temp_download_limit_kbps;
    new_cfg.network.max_upload_tasks = c.network.max_upload_tasks.max(1);
    new_cfg.storage.download_dir = c.storage.download_dir.into();
    new_cfg.storage.temp_dir = c.storage.temp_dir.into();
    new_cfg.emule.enabled = c.emule.enabled;
    new_cfg.emule.temp_dir = c.emule.temp_dir.into();
    new_cfg.emule.udp_port = c.emule.udp_port;
    new_cfg.emule.tcp_port = c.emule.tcp_port;
    new_cfg.emule.external_ip = c.emule.external_ip.and_then(|s| s.parse().ok());
    new_cfg.emule.download_slots_per_file = c.emule.download_slots_per_file.clamp(1, 50);
    new_cfg.emule.max_upload_slots = c.emule.max_upload_slots.clamp(1, 50);
    new_cfg.emule.max_concurrent_downloads = c.emule.max_concurrent_downloads.clamp(1, 50);
    // identity_path and api.listen intentionally not writable at runtime

    match new_cfg.save() {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("Failed to save config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

fn temp_limit_status(state: &AppState) -> TempLimitStatus {
    TempLimitStatus {
        active: state.bandwidth.temp_active(),
        upload_kbps: state.bandwidth.temp_upload_kbps(),
        download_kbps: state.bandwidth.temp_download_kbps(),
        effective_upload_kbps: state.bandwidth.effective_upload_kbps(),
        effective_download_kbps: state.bandwidth.effective_download_kbps(),
    }
}

/// Get temporary speed-limit status
///
/// Returns whether the temporary speed limit is engaged, its preset caps, and
/// the upload/download rates actually in force right now.
#[utoipa::path(
    get,
    path = "/api/v1/config/temp-limit",
    responses(
        (status = 200, description = "Current temporary speed-limit status.", body = TempLimitStatus)
    )
)]
pub async fn get_temp_limit(State(state): State<AppState>) -> Json<TempLimitStatus> {
    Json(temp_limit_status(&state))
}

/// Toggle the temporary speed limit
///
/// Engages or releases the temporary speed limit, applying the preset caps
/// (`network.temp_*_limit_kbps`) to the live throttles immediately. This is
/// runtime-only state and is not persisted across restarts.
#[utoipa::path(
    put,
    path = "/api/v1/config/temp-limit",
    request_body = TempLimitRequest,
    responses(
        (status = 200, description = "New temporary speed-limit status.", body = TempLimitStatus)
    )
)]
pub async fn put_temp_limit(
    State(state): State<AppState>,
    Json(req): Json<TempLimitRequest>,
) -> Json<TempLimitStatus> {
    state.bandwidth.set_temp_active(req.active);
    Json(temp_limit_status(&state))
}
