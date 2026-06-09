//! Notification-centre endpoints: list/clear/mark-read the in-app
//! notifications, and read/update the per-kind toggles.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use rucio_core::api::notifications::{NotificationList, NotificationSettings, WebhookTestResult};

use crate::api::AppState;

const LIST_LIMIT: i64 = 200;

/// List notifications (newest first) and the unread count.
#[utoipa::path(
    get,
    path = "/api/v1/notifications",
    tag = "notifications",
    summary = "List notifications",
    responses(
        (status = 200, description = "Recent notifications and unread count", body = NotificationList),
    )
)]
pub async fn list_notifications(
    State(state): State<AppState>,
) -> Result<Json<NotificationList>, StatusCode> {
    let items = crate::db::notifications::list(&state.db, LIST_LIMIT)
        .await
        .map_err(internal)?;
    let unread = crate::db::notifications::unread_count(&state.db)
        .await
        .map_err(internal)?;
    Ok(Json(NotificationList { items, unread }))
}

/// Mark every notification as read.
#[utoipa::path(
    post,
    path = "/api/v1/notifications/read",
    tag = "notifications",
    summary = "Mark all notifications read",
    responses((status = 204, description = "All notifications marked read")),
)]
pub async fn mark_all_read(State(state): State<AppState>) -> StatusCode {
    match crate::db::notifications::mark_all_read(&state.db).await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("mark_all_read: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Delete every notification.
#[utoipa::path(
    delete,
    path = "/api/v1/notifications",
    tag = "notifications",
    summary = "Clear all notifications",
    responses((status = 204, description = "All notifications deleted")),
)]
pub async fn clear_notifications(State(state): State<AppState>) -> StatusCode {
    match crate::db::notifications::clear(&state.db).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("clear_notifications: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Delete a single notification by id.
#[utoipa::path(
    delete,
    path = "/api/v1/notifications/{id}",
    tag = "notifications",
    summary = "Delete a notification",
    params(("id" = i64, Path, description = "Notification id")),
    responses(
        (status = 204, description = "Notification deleted"),
        (status = 404, description = "No such notification"),
    )
)]
pub async fn delete_notification(State(state): State<AppState>, Path(id): Path<i64>) -> StatusCode {
    match crate::db::notifications::delete(&state.db, id).await {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("delete_notification: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Read the notification toggles.
#[utoipa::path(
    get,
    path = "/api/v1/notifications/settings",
    tag = "notifications",
    summary = "Get notification settings",
    responses((status = 200, description = "Current notification toggles", body = NotificationSettings)),
)]
pub async fn get_settings(State(state): State<AppState>) -> Json<NotificationSettings> {
    // Read the live toggles, not the startup config snapshot (which goes stale
    // after a PUT).
    let (enabled, downloads, system) = state.notifications.snapshot();
    Json(NotificationSettings {
        enabled,
        downloads,
        system,
    })
}

/// Update the notification toggles: apply them to the live notifier and persist
/// them to `config.toml`.
#[utoipa::path(
    put,
    path = "/api/v1/notifications/settings",
    tag = "notifications",
    request_body = NotificationSettings,
    summary = "Update notification settings",
    responses(
        (status = 204, description = "Settings applied and persisted"),
        (status = 500, description = "Could not persist settings"),
    )
)]
pub async fn put_settings(
    State(state): State<AppState>,
    Json(req): Json<NotificationSettings>,
) -> StatusCode {
    // Apply to the live notifier immediately so the change takes effect now.
    state
        .notifications
        .update(req.enabled, req.downloads, req.system);

    // Persist: load what is currently on disk, swap only the toggles (keeping
    // the configured webhooks), and save — so we never clobber other settings.
    let mut cfg = match crate::config::Config::load(state.config_path.as_deref()) {
        Ok(c) => c,
        Err(_) => (*state.config).clone(),
    };
    cfg.notifications.enabled = req.enabled;
    cfg.notifications.downloads = req.downloads;
    cfg.notifications.system = req.system;
    match cfg.save() {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("Failed to save notification settings: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// List the configured outbound webhooks.
#[utoipa::path(
    get,
    path = "/api/v1/notifications/webhooks",
    tag = "notifications",
    summary = "List notification webhooks",
    responses((status = 200, description = "Configured webhooks", body = [crate::config::WebhookConfig])),
)]
pub async fn get_webhooks(
    State(state): State<AppState>,
) -> Json<Vec<crate::config::WebhookConfig>> {
    Json(state.notifications.webhooks())
}

/// Replace the whole webhook list: apply it live and persist to `config.toml`.
#[utoipa::path(
    put,
    path = "/api/v1/notifications/webhooks",
    tag = "notifications",
    request_body = [crate::config::WebhookConfig],
    summary = "Update notification webhooks",
    responses(
        (status = 204, description = "Webhooks applied and persisted"),
        (status = 500, description = "Could not persist webhooks"),
    )
)]
pub async fn put_webhooks(
    State(state): State<AppState>,
    Json(webhooks): Json<Vec<crate::config::WebhookConfig>>,
) -> StatusCode {
    // Apply to the live notifier immediately.
    state.notifications.set_webhooks(webhooks.clone());

    // Persist: reload from disk, swap the webhook list (keeping the toggles),
    // and save — so we never clobber other settings.
    let mut cfg = match crate::config::Config::load(state.config_path.as_deref()) {
        Ok(c) => c,
        Err(_) => (*state.config).clone(),
    };
    cfg.notifications.webhooks = webhooks;
    match cfg.save() {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::error!("Failed to save webhooks: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Send a one-off test delivery to a webhook (as posted, not necessarily saved)
/// and report whether it succeeded — lets the user verify their setup.
#[utoipa::path(
    post,
    path = "/api/v1/notifications/webhooks/test",
    tag = "notifications",
    request_body = crate::config::WebhookConfig,
    summary = "Send a test notification to a webhook",
    responses((status = 200, description = "Test delivery outcome", body = WebhookTestResult)),
)]
pub async fn test_webhook(
    Json(webhook): Json<crate::config::WebhookConfig>,
) -> Json<rucio_core::api::notifications::WebhookTestResult> {
    let client = reqwest::Client::new();
    Json(crate::webhooks::send_test(&client, &webhook).await)
}

fn internal<E: std::fmt::Display>(e: E) -> StatusCode {
    tracing::error!("notifications: {e}");
    StatusCode::INTERNAL_SERVER_ERROR
}
