//! Update-check DTO: the latest Rucio release seen on GitHub, compared against
//! the running build. Served by `GET /api/v1/update` (cached) and refreshed by
//! `POST /api/v1/update/check` (the panel's manual check).

/// Outcome of the update check.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct UpdateInfo {
    /// The running daemon version (crate semver, e.g. `0.49.0` or
    /// `0.49.0-dev`).
    pub current: String,
    /// Latest stable release tag on GitHub (bare semver, no `v` prefix), or
    /// `None` when no check has succeeded yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest: Option<String>,
    /// Whether `latest` is a newer stable release than `current`. `false` for a
    /// development build already ahead of the last release, or before any
    /// successful check.
    pub newer: bool,
    /// Web page of the latest release (where its notes and downloads live), or
    /// `None` when unknown. The panel links here instead of rendering notes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_url: Option<String>,
    /// Unix-seconds time of the last successful check, or `None` if never
    /// checked yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<i64>,
}
