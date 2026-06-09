//! Notification DTOs shared between the daemon API and the web client.
//!
//! A notification is a small, persisted record of something the user may want
//! to know about after the fact — a download finishing, indexing completing —
//! surfaced in the web UI's notification centre (the bell). The model is
//! deliberately generic (kind + title + body + optional resource reference) so
//! the same record can later feed outbound webhooks without a redesign.

/// The kind of a notification. Used by the client to icon/group them and by the
/// daemon to honour the per-type enable toggles in `NotificationConfig`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum NotificationKind {
    /// A download finished (Rucio or eMule).
    Download,
    /// A system/background event (e.g. indexing finished).
    System,
}

impl NotificationKind {
    /// Stable lowercase token used as the DB `kind` column value.
    pub fn as_str(self) -> &'static str {
        match self {
            NotificationKind::Download => "download",
            NotificationKind::System => "system",
        }
    }

    /// Parse the DB `kind` column back into a variant. Unknown values map to
    /// `System` so a forward-compatible row is never dropped.
    pub fn from_db(s: &str) -> Self {
        match s {
            "download" => NotificationKind::Download,
            _ => NotificationKind::System,
        }
    }
}

/// A single notification, as stored in the DB and pushed to clients over the
/// WebSocket bus.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct NotificationDto {
    pub id: i64,
    pub kind: NotificationKind,
    pub title: String,
    pub body: String,
    /// Optional reference to the resource the notification is about (e.g. a
    /// download's blake3 root hash, hex-encoded). Lets the client deep-link to
    /// it later; ignored when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ref_key: Option<String>,
    /// Creation time, Unix seconds.
    pub created_at: i64,
    /// Whether the user has already seen it.
    pub read: bool,
}

/// Response of `GET /api/v1/notifications`: the most recent notifications plus
/// the unread count so the client can render the bell badge without a second
/// request.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct NotificationList {
    pub items: Vec<NotificationDto>,
    pub unread: i64,
}

/// The notification-centre toggles, exchanged by
/// `GET`/`PUT /api/v1/notifications/settings`. Mirrors the daemon's
/// `NotificationConfig`; `enabled` is the master switch and the per-kind flags
/// opt individual categories in or out.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct NotificationSettings {
    pub enabled: bool,
    pub downloads: bool,
    pub system: bool,
}

/// Outcome of `POST /api/v1/notifications/webhooks/test`: whether a test
/// delivery to a webhook succeeded, with the HTTP status (if the request
/// completed) or the transport error (if it didn't).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct WebhookTestResult {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
