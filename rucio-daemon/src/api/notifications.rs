//! Notification-centre endpoints: list, clear, mark-read and delete the in-app
//! notifications (the bell + slideover). The per-kind toggles and the outbound
//! webhooks are configuration, so they live under `/config/notifications`
//! (see `config.rs`).

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use rucio_core::api::notifications::NotificationList;

use crate::api::AppState;

const LIST_LIMIT: i64 = 200;

/// List notifications.
///
/// Returns the most recent notifications (newest first, capped) and the number
/// still unread — what the notification centre and the bell badge render.
#[utoipa::path(
    get,
    path = "/api/v1/notifications",
    tag = "notifications",
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

/// Mark all notifications read.
///
/// Marks every notification as read, clearing the unread badge.
#[utoipa::path(
    post,
    path = "/api/v1/notifications/read",
    tag = "notifications",
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

/// Clear all notifications.
///
/// Deletes every notification from the centre.
#[utoipa::path(
    delete,
    path = "/api/v1/notifications",
    tag = "notifications",
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

/// Delete a notification.
///
/// Removes a single notification by id.
#[utoipa::path(
    delete,
    path = "/api/v1/notifications/{id}",
    tag = "notifications",
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

fn internal<E: std::fmt::Display>(e: E) -> StatusCode {
    tracing::error!("notifications: {e}");
    StatusCode::INTERNAL_SERVER_ERROR
}
