//! Configuration endpoints under `/api/v1/config`: the full config (GET/PUT),
//! the speed limits and temporary-limit toggle, and — since they are
//! configuration too — the notification toggles and outbound webhooks
//! (`/config/notifications`, `/config/notifications/webhooks`). The
//! notification-centre data itself (list/clear/read) lives in `notifications.rs`.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rucio_core::api::config::{
    ApiConfig, ConfigResponse, ConfigSnapshot, EmuleConfig, NetworkConfig, NodeConfig, SpeedLimits,
    StorageConfig, TempLimitRequest, TempLimitStatus,
};
use rucio_core::api::notifications::{NotificationSettings, WebhookTestResult};

use crate::api::AppState;
use crate::config::{Config, WebhookConfig};

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
                || disk.network.exclusive_bootstrap != cfg.network.exclusive_bootstrap
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
            exclusive_bootstrap: cfg.network.exclusive_bootstrap,
        },
        storage: StorageConfig {
            download_dir: cfg.storage.download_dir.to_string_lossy().into_owned(),
            temp_dir: cfg.storage.temp_dir.to_string_lossy().into_owned(),
            pin_dir: cfg.storage.pin_dir.to_string_lossy().into_owned(),
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
            nick: cfg.emule.nick.clone(),
            min_source_speed_kib_s: cfg.emule.min_source_speed_kib_s,
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
    // Start from the latest on-disk config, not the startup snapshot, so fields
    // this endpoint doesn't touch but that are changed at runtime through their
    // own endpoints — notably `notifications` (webhooks + toggles via
    // PUT /config/notifications/...) — are preserved instead of reverted.
    let mut new_cfg =
        Config::load(state.config_path.as_deref()).unwrap_or_else(|_| (*state.config).clone());
    new_cfg.node.listen_addrs = c.node.listen_addrs;
    new_cfg.network.bootstrap_peers = c.network.bootstrap_peers;
    new_cfg.network.upload_limit_kbps = c.network.upload_limit_kbps;
    new_cfg.network.download_limit_kbps = c.network.download_limit_kbps;
    new_cfg.network.temp_upload_limit_kbps = c.network.temp_upload_limit_kbps;
    new_cfg.network.temp_download_limit_kbps = c.network.temp_download_limit_kbps;
    new_cfg.network.max_upload_tasks = c.network.max_upload_tasks.max(1);
    new_cfg.network.exclusive_bootstrap = c.network.exclusive_bootstrap;
    new_cfg.storage.download_dir = c.storage.download_dir.into();
    new_cfg.storage.temp_dir = c.storage.temp_dir.into();
    // pin_dir was added later (serde default ""); an older client that doesn't
    // send it must not blank the configured value.
    if !c.storage.pin_dir.trim().is_empty() {
        new_cfg.storage.pin_dir = c.storage.pin_dir.into();
    }
    new_cfg.emule.enabled = c.emule.enabled;
    new_cfg.emule.temp_dir = c.emule.temp_dir.into();
    new_cfg.emule.udp_port = c.emule.udp_port;
    new_cfg.emule.tcp_port = c.emule.tcp_port;
    new_cfg.emule.external_ip = c.emule.external_ip.and_then(|s| s.parse().ok());
    new_cfg.emule.download_slots_per_file = c.emule.download_slots_per_file.clamp(1, 50);
    new_cfg.emule.max_upload_slots = c.emule.max_upload_slots.clamp(1, 50);
    new_cfg.emule.max_concurrent_downloads = c.emule.max_concurrent_downloads.clamp(1, 50);
    new_cfg.emule.nick = c.emule.nick.trim().to_string();
    new_cfg.emule.min_source_speed_kib_s = c.emule.min_source_speed_kib_s;
    // identity_path and api.listen intentionally not writable at runtime

    match new_cfg.save(state.config_path.as_deref()) {
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

/// Get the base speed limits
///
/// Returns the normal (base) upload/download caps in KB/s (0 = unlimited).
#[utoipa::path(
    get,
    path = "/api/v1/config/limits",
    responses(
        (status = 200, description = "Current base speed limits.", body = SpeedLimits)
    )
)]
pub async fn get_limits(State(state): State<AppState>) -> Json<SpeedLimits> {
    Json(SpeedLimits {
        upload_kbps: state.bandwidth.base_upload_kbps(),
        download_kbps: state.bandwidth.base_download_kbps(),
    })
}

/// Set the base speed limits
///
/// Updates the normal (base) upload/download caps (KB/s, 0 = unlimited),
/// applying them to the live throttles and persisting them to the config file.
/// A lightweight alternative to a full `PUT /config` for a quick change.
#[utoipa::path(
    put,
    path = "/api/v1/config/limits",
    request_body = SpeedLimits,
    responses(
        (status = 204, description = "Limits applied and saved."),
        (status = 500, description = "Failed to write the configuration file.")
    )
)]
pub async fn put_limits(State(state): State<AppState>, Json(req): Json<SpeedLimits>) -> StatusCode {
    // Apply live (honouring the temporary-limit toggle).
    state.bandwidth.set_base(req.upload_kbps, req.download_kbps);

    // Persist: load the latest on-disk config so a concurrent change to other
    // fields isn't clobbered, update only the limits, and save.
    let mut cfg =
        Config::load(state.config_path.as_deref()).unwrap_or_else(|_| (*state.config).clone());
    cfg.network.upload_limit_kbps = req.upload_kbps;
    cfg.network.download_limit_kbps = req.download_kbps;
    match cfg.save(state.config_path.as_deref()) {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("Failed to save config: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Get notification settings.
///
/// Returns the live notification toggles: the master switch and the per-kind
/// flags currently in effect.
#[utoipa::path(
    get,
    path = "/api/v1/config/notifications",
    tag = "config",
    responses((status = 200, description = "Current notification toggles", body = NotificationSettings)),
)]
pub async fn get_notification_settings(
    State(state): State<AppState>,
) -> Json<NotificationSettings> {
    // Read the live toggles, not the startup config snapshot (which goes stale
    // after a PUT).
    let (enabled, downloads, system) = state.notifications.snapshot();
    Json(NotificationSettings {
        enabled,
        downloads,
        system,
    })
}

/// Update notification settings.
///
/// Applies the toggles to the live notifier immediately and persists them to
/// `config.toml` (the configured webhooks are left untouched).
#[utoipa::path(
    put,
    path = "/api/v1/config/notifications",
    tag = "config",
    request_body = NotificationSettings,
    responses(
        (status = 204, description = "Settings applied and persisted"),
        (status = 500, description = "Could not persist settings"),
    )
)]
pub async fn put_notification_settings(
    State(state): State<AppState>,
    Json(req): Json<NotificationSettings>,
) -> StatusCode {
    // Apply to the live notifier immediately so the change takes effect now.
    state
        .notifications
        .update(req.enabled, req.downloads, req.system);

    // Persist: load what is currently on disk, swap only the toggles (keeping
    // the configured webhooks), and save — so we never clobber other settings.
    let mut cfg = match Config::load(state.config_path.as_deref()) {
        Ok(c) => c,
        Err(_) => (*state.config).clone(),
    };
    cfg.notifications.enabled = req.enabled;
    cfg.notifications.downloads = req.downloads;
    cfg.notifications.system = req.system;
    match cfg.save(state.config_path.as_deref()) {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("Failed to save notification settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// List notification webhooks.
///
/// Returns the configured outbound webhook targets.
#[utoipa::path(
    get,
    path = "/api/v1/config/notifications/webhooks",
    tag = "config",
    responses((status = 200, description = "Configured webhooks", body = [WebhookConfig])),
)]
pub async fn get_webhooks(State(state): State<AppState>) -> Json<Vec<WebhookConfig>> {
    Json(state.notifications.webhooks())
}

/// Update notification webhooks.
///
/// Replaces the whole webhook list, applying it to the live notifier and
/// persisting it to `config.toml` (the toggles are left untouched).
#[utoipa::path(
    put,
    path = "/api/v1/config/notifications/webhooks",
    tag = "config",
    request_body = [WebhookConfig],
    responses(
        (status = 204, description = "Webhooks applied and persisted"),
        (status = 500, description = "Could not persist webhooks"),
    )
)]
pub async fn put_webhooks(
    State(state): State<AppState>,
    Json(webhooks): Json<Vec<WebhookConfig>>,
) -> StatusCode {
    // Apply to the live notifier immediately.
    state.notifications.set_webhooks(webhooks.clone());

    // Persist: reload from disk, swap the webhook list (keeping the toggles),
    // and save — so we never clobber other settings.
    let mut cfg = match Config::load(state.config_path.as_deref()) {
        Ok(c) => c,
        Err(_) => (*state.config).clone(),
    };
    cfg.notifications.webhooks = webhooks;
    match cfg.save(state.config_path.as_deref()) {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("Failed to save webhooks: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Send a test notification to a webhook.
///
/// Does a single synchronous delivery to the webhook as posted (not necessarily
/// saved) and reports whether it succeeded — lets the user verify their setup.
#[utoipa::path(
    post,
    path = "/api/v1/config/notifications/webhooks/test",
    tag = "config",
    request_body = WebhookConfig,
    responses((status = 200, description = "Test delivery outcome", body = WebhookTestResult)),
)]
pub async fn test_webhook(Json(webhook): Json<WebhookConfig>) -> Json<WebhookTestResult> {
    let client = reqwest::Client::new();
    Json(crate::webhooks::send_test(&client, &webhook).await)
}
