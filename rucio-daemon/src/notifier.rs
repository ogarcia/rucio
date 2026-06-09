//! The notification service: a single place that decides whether to record a
//! notification, persists it, and pushes it to connected clients.
//!
//! Cloneable and cheap to pass around (a DB pool handle, a broadcast sender, and
//! an `Arc` of runtime toggles). Call sites — the download engine, the eMule
//! task, the indexing tick — just call [`Notifier::notify`]; the gating,
//! persistence and WebSocket push all live here.
//!
//! The toggles are runtime state (mirroring how bandwidth limits are applied
//! live) so changing them from the settings UI takes effect immediately, with
//! `config.toml` as the persisted source of truth.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rucio_core::api::notifications::{NotificationDto, NotificationKind};
use rucio_core::api::ws::WsEvent;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::config::NotificationConfig;
use crate::db::{self, Db};

/// Runtime, live-updatable copy of [`NotificationConfig`]. The settings handler
/// updates these on a config change so a toggle takes effect without a restart.
#[derive(Debug)]
pub struct NotificationState {
    enabled: AtomicBool,
    downloads: AtomicBool,
    system: AtomicBool,
}

impl NotificationState {
    /// Build the runtime state from the loaded config.
    pub fn from_config(cfg: &NotificationConfig) -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(cfg.enabled),
            downloads: AtomicBool::new(cfg.downloads),
            system: AtomicBool::new(cfg.system),
        })
    }

    /// Apply new toggle values to the live state (called after the settings
    /// handler persists a change).
    pub fn update(&self, enabled: bool, downloads: bool, system: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        self.downloads.store(downloads, Ordering::Relaxed);
        self.system.store(system, Ordering::Relaxed);
    }

    /// Current toggle values `(enabled, downloads, system)`. This is the live
    /// source of truth (the startup config snapshot goes stale after a `PUT`).
    pub fn snapshot(&self) -> (bool, bool, bool) {
        (
            self.enabled.load(Ordering::Relaxed),
            self.downloads.load(Ordering::Relaxed),
            self.system.load(Ordering::Relaxed),
        )
    }

    /// Whether a notification of `kind` should be generated right now.
    fn wants(&self, kind: NotificationKind) -> bool {
        if !self.enabled.load(Ordering::Relaxed) {
            return false;
        }
        match kind {
            NotificationKind::Download => self.downloads.load(Ordering::Relaxed),
            NotificationKind::System => self.system.load(Ordering::Relaxed),
        }
    }
}

/// Records and dispatches notifications, honouring the live toggles.
#[derive(Clone)]
pub struct Notifier {
    db: Db,
    ws_tx: broadcast::Sender<WsEvent>,
    state: Arc<NotificationState>,
    /// Outbound webhook targets (loaded from config at startup).
    webhooks: Arc<Vec<crate::config::WebhookConfig>>,
    /// Shared HTTP client for webhook delivery (cheap to clone).
    http: reqwest::Client,
}

impl Notifier {
    pub fn new(
        db: Db,
        ws_tx: broadcast::Sender<WsEvent>,
        state: Arc<NotificationState>,
        webhooks: Vec<crate::config::WebhookConfig>,
    ) -> Self {
        Self {
            db,
            ws_tx,
            state,
            webhooks: Arc::new(webhooks),
            http: reqwest::Client::new(),
        }
    }

    /// Record a notification and push it to clients — unless its category is
    /// disabled, in which case this is a no-op (nothing is persisted). Failures
    /// are logged and swallowed: a notification is never worth failing the
    /// operation that triggered it.
    pub async fn notify(
        &self,
        kind: NotificationKind,
        title: impl Into<String>,
        body: impl Into<String>,
        ref_key: Option<String>,
    ) {
        if !self.state.wants(kind) {
            return;
        }
        let (title, body) = (title.into(), body.into());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let id =
            match db::notifications::insert(&self.db, kind, &title, &body, ref_key.as_deref(), now)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    warn!("notify: cannot persist notification: {e}");
                    return;
                }
            };

        let dto = NotificationDto {
            id,
            kind,
            title,
            body,
            ref_key,
            created_at: now,
            read: false,
        };
        // Fan out to webhooks (best-effort, spawned) before the dto is moved
        // into the WS event.
        crate::webhooks::dispatch(&self.http, &self.webhooks, &dto);

        // A send error just means no client is connected; the row is persisted
        // and will be fetched on next load.
        let _ = self.ws_tx.send(WsEvent::Notification(dto));
        debug!(?kind, "notification recorded");
    }
}
