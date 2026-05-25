//! API types for the eMule compatibility endpoints.
//!
//! These types are serialized/deserialized by both the daemon and the CLI.

/// Default URL for downloading a fresh `nodes.dat` file.
pub const DEFAULT_NODES_DAT_URL: &str = "http://upd.emule-security.net/nodes.dat";

/// POST /api/v1/emule/bootstrap — request body.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct EmuleBootstrapRequest {
    /// URL to download the `nodes.dat` file from.
    /// Defaults to [`DEFAULT_NODES_DAT_URL`] when omitted.
    #[serde(default)]
    pub url: Option<String>,
}

/// POST /api/v1/emule/bootstrap — response body.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct EmuleBootstrapResponse {
    /// Number of Kad2 contacts parsed from the downloaded file.
    pub contacts: usize,
    /// Path where `nodes.dat` was saved on the daemon host.
    pub path: String,
    /// URL that was used to download the file.
    pub url: String,
}

/// GET /api/v1/emule/status — response body.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct EmuleStatusResponse {
    /// Whether the `emule-compat` feature is compiled into this daemon binary.
    pub feature_enabled: bool,
    /// Configured path for `nodes.dat` (if any).
    pub nodes_dat_path: Option<String>,
    /// Whether the `nodes.dat` file exists and is readable.
    pub nodes_dat_present: bool,
    /// Number of Kad2 contacts in the current `nodes.dat` (0 if not present).
    pub contacts: usize,
}
