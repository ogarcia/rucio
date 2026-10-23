//! Release update check against GitHub.
//!
//! A daily poll (folded into the periodic share-reconcile task) plus an
//! on-demand refresh for the panel's manual "check now" button. A 10-minute
//! floor means repeated manual checks serve the cached result instead of
//! hammering GitHub. The check finds the latest *stable* release
//! (`/releases/latest` already excludes prereleases), compares its tag with the
//! running version, caches the outcome, and — only on the automatic path, and
//! only for a stable build — raises a one-time `System` notification when a
//! newer version first appears.
//!
//! Contacting GitHub reveals the node's IP, so the automatic check honours the
//! `update_check.enabled` toggle. The manual button bypasses the toggle (it is
//! an explicit user action) but still respects the 10-minute floor.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use rucio_core::api::notifications::NotificationKind;
use rucio_core::api::update::UpdateInfo;
use semver::Version;
use tokio::sync::Mutex;
use tracing::debug;

use crate::notifier::Notifier;

/// GitHub API endpoint for the latest stable release (excludes prereleases and
/// drafts).
const RELEASES_URL: &str = "https://api.github.com/repos/ogarcia/rucio/releases/latest";

/// Minimum spacing between network checks. A refresh requested inside this
/// window returns the cached result, so the manual button can't be used to spam
/// GitHub.
const MIN_CHECK_SPACING: Duration = Duration::from_secs(10 * 60);

/// The running build's version, as compiled in.
fn current_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Whether `latest` is a strictly newer semver than `current`. Unparseable
/// versions compare as "not newer" (fail safe: never nag on garbage).
fn is_newer(current: &str, latest: &str) -> bool {
    match (Version::parse(current), Version::parse(latest)) {
        (Ok(c), Ok(l)) => l > c,
        _ => false,
    }
}

/// Whether the running build is a prerelease/development version (e.g.
/// `0.49.0-dev`). Such builds are deliberately not nagged by the automatic
/// check — a user on a development build tracks master on purpose.
fn current_is_prerelease() -> bool {
    Version::parse(current_version())
        .map(|v| !v.pre.is_empty())
        .unwrap_or(false)
}

fn unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The subset of the GitHub release JSON we read; other fields are ignored.
#[derive(serde::Deserialize)]
struct GhRelease {
    tag_name: String,
    html_url: String,
}

#[derive(Default)]
struct UpdaterState {
    /// When the last successful network check completed.
    last_checked: Option<Instant>,
    /// The cached outcome of that check.
    info: Option<UpdateInfo>,
    /// The latest tag a notification was already raised for, so the automatic
    /// check notifies once per new version (not once per daily poll).
    notified: Option<String>,
}

/// Checks GitHub for newer releases and caches the result.
pub struct Updater {
    /// Governs the automatic daily check only; the manual refresh ignores it.
    enabled: AtomicBool,
    http: reqwest::Client,
    state: Mutex<UpdaterState>,
    notifier: Notifier,
}

impl Updater {
    pub fn new(enabled: bool, notifier: Notifier) -> Arc<Self> {
        // GitHub rejects requests without a User-Agent; identify ourselves.
        let http = reqwest::Client::builder()
            .user_agent(concat!("rucio/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Arc::new(Self {
            enabled: AtomicBool::new(enabled),
            http,
            state: Mutex::new(UpdaterState::default()),
            notifier,
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Apply the live toggle (after the settings handler persists a change).
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// The cached outcome, or a bare "current only" record if never checked.
    /// Never touches the network — used to render the panel without phoning
    /// home on every open.
    pub async fn snapshot(&self) -> UpdateInfo {
        let st = self.state.lock().await;
        st.info.clone().unwrap_or_else(|| UpdateInfo {
            current: current_version().to_string(),
            latest: None,
            newer: false,
            release_url: None,
            checked_at: None,
        })
    }

    /// Force a check for the manual button: fetch unless within the 10-minute
    /// floor, in which case the cached result is returned. Never notifies.
    pub async fn refresh(&self) -> UpdateInfo {
        self.check(false).await
    }

    /// The automatic daily check: a no-op when disabled, else a fetch that
    /// raises a one-time notification on a newly-seen newer release.
    pub async fn run_auto(&self) {
        if !self.is_enabled() {
            return;
        }
        self.check(true).await;
    }

    /// Shared path: serve the cache within the floor, else fetch and cache.
    /// When `notify` and the fetch reveals a newer stable release not yet
    /// announced (and this is not a development build), emit one `System`
    /// notification.
    async fn check(&self, notify: bool) -> UpdateInfo {
        // Within the floor: serve the cached result untouched.
        {
            let st = self.state.lock().await;
            if let (Some(last), Some(info)) = (st.last_checked, st.info.clone())
                && last.elapsed() < MIN_CHECK_SPACING
            {
                return info;
            }
        }

        let fetched = match self.fetch().await {
            Ok(info) => info,
            Err(e) => {
                debug!("update check failed: {e:#}");
                return self.snapshot().await;
            }
        };

        // Cache the fresh result and decide whether a notification is due.
        let announce = {
            let mut st = self.state.lock().await;
            st.last_checked = Some(Instant::now());
            st.info = Some(fetched.clone());
            let due = notify
                && fetched.newer
                && !current_is_prerelease()
                && st.notified.as_deref() != fetched.latest.as_deref();
            if due {
                st.notified = fetched.latest.clone();
            }
            due
        };

        if announce && let Some(latest) = fetched.latest.as_deref() {
            self.emit(latest, fetched.release_url.as_deref()).await;
        }
        fetched
    }

    async fn fetch(&self) -> anyhow::Result<UpdateInfo> {
        let release: GhRelease = self
            .http
            .get(RELEASES_URL)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .send()
            .await
            .context("GitHub release request")?
            .error_for_status()
            .context("GitHub returned an error status")?
            .json()
            .await
            .context("parsing GitHub release JSON")?;

        let current = current_version();
        // Our tags are bare semver, but strip a leading `v` defensively.
        let tag = release.tag_name.trim_start_matches('v').to_string();
        Ok(UpdateInfo {
            current: current.to_string(),
            newer: is_newer(current, &tag),
            latest: Some(tag),
            release_url: Some(release.html_url),
            checked_at: Some(unix_secs()),
        })
    }

    async fn emit(&self, latest: &str, url: Option<&str>) {
        self.notifier
            .notify(
                NotificationKind::System,
                format!("Rucio {latest} is available"),
                format!(
                    "A newer version ({latest}) has been released. Click the title to view it."
                ),
                url.map(str::to_string),
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_detects_release_bump() {
        assert!(is_newer("0.48.0", "0.49.0"));
        assert!(is_newer("0.48.0", "0.48.1"));
        assert!(is_newer("1.0.0", "1.0.1"));
    }

    #[test]
    fn newer_is_false_for_same_or_older() {
        assert!(!is_newer("0.49.0", "0.49.0"));
        assert!(!is_newer("0.49.0", "0.48.0"));
    }

    #[test]
    fn dev_build_is_ahead_of_last_release() {
        // A development build carries a prerelease tag and sorts *after* the
        // release it is based on, so the last stable release is never "newer".
        assert!(!is_newer("0.49.0-dev", "0.48.0"));
        assert!(current_is_prerelease_of("0.49.0-dev"));
        assert!(!current_is_prerelease_of("0.49.0"));
    }

    #[test]
    fn garbage_versions_never_newer() {
        assert!(!is_newer("not-a-version", "0.49.0"));
        assert!(!is_newer("0.49.0", "also-not"));
    }

    // Helper mirroring `current_is_prerelease` for an arbitrary version string.
    fn current_is_prerelease_of(v: &str) -> bool {
        Version::parse(v)
            .map(|x| !x.pre.is_empty())
            .unwrap_or(false)
    }
}
