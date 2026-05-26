//! API types for the eMule compatibility endpoints.
//!
//! These types are serialized/deserialized by both the daemon and the CLI.

/// Default URL for downloading a fresh `nodes.dat` file.
pub const DEFAULT_NODES_DAT_URL: &str = "http://upd.emule-security.org/nodes.dat";

/// User-Agent sent when downloading `nodes.dat`.
///
/// Several nodes.dat mirrors filter requests that do not look like a real
/// eMule client.  We impersonate the last stable eMule release so the server
/// returns a valid binary file instead of an HTML error page.
pub const EMULE_USER_AGENT: &str = "eMule/0.60a";

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

/// Connectivity status of the eMule TCP listener as observed by the daemon.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum EmuleConnectivity {
    /// The eMule TCP port is reachable from the internet.
    /// Either a peer has connected to us, or UPnP / a configured external IP
    /// vouches for reachability.
    Open,
    /// UPnP is enabled but no mapping could be established — the port is
    /// almost certainly blocked by the gateway or by the ISP.
    Firewalled,
    /// We have no evidence either way: UPnP is disabled, no external IP is
    /// configured, and no peer has connected to us yet.
    #[default]
    Unknown,
}

/// GET /api/v1/emule/status — response body.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct EmuleStatusResponse {
    /// Whether the `emule-compat` feature is compiled into this daemon binary.
    pub feature_enabled: bool,
    /// Whether eMule is enabled at runtime (`emule.enabled` config key).
    /// Always `false` when `feature_enabled` is `false`.
    /// Defaults to `true` when deserialising from an older daemon that does not
    /// send this field.
    #[serde(default = "bool_true")]
    pub runtime_enabled: bool,
    /// Effective path for `nodes.dat` — either the configured value or the
    /// platform default.  Always present when `feature_enabled` is true.
    pub nodes_dat_path: Option<String>,
    /// Whether the `nodes.dat` file exists and is readable.
    pub nodes_dat_present: bool,
    /// Number of Kad2 contacts in the current `nodes.dat` (0 if not present).
    pub contacts: usize,
    /// Number of contacts currently in the Kad2 routing table.
    pub connected_peers: usize,
    /// Whether the node considers itself well-connected (≥ 4 Kad contacts).
    pub is_connected: bool,
    /// External IPv4 as known by the daemon (UPnP-detected or configured).
    #[serde(default)]
    pub external_ip: Option<String>,
    /// How `external_ip` was obtained: `"upnp"`, `"config"`, or absent.
    #[serde(default)]
    pub external_ip_source: Option<String>,
    /// eMule TCP port the daemon listens on for incoming peer connections.
    #[serde(default)]
    pub tcp_port: Option<u16>,
    /// Kad2 UDP port the daemon listens on.
    #[serde(default)]
    pub udp_port: Option<u16>,
    /// Inferred connectivity class of the eMule TCP port.
    #[serde(default = "EmuleConnectivity::default")]
    pub connectivity: EmuleConnectivity,
    /// Short human-readable explanation of how `connectivity` was determined.
    #[serde(default)]
    pub connectivity_reason: Option<String>,
    /// Number of eMule downloads currently in progress.
    #[serde(default)]
    pub active_downloads: usize,
    /// Maximum number of simultaneous upload connections (`emule.max_upload_slots`).
    #[serde(default)]
    pub upload_slots_total: usize,
    /// Number of upload slots currently in use.
    #[serde(default)]
    pub upload_slots_in_use: usize,
    /// Inbound eMule TCP connections accepted since the daemon started.
    /// A non-zero value is direct proof that the TCP port is reachable.
    #[serde(default)]
    pub inbound_connections: u64,
}

fn bool_true() -> bool {
    true
}
