/// GET/PUT /api/v1/config/temp-limit — temporary speed-limit toggle.
///
/// The temporary limit is runtime-only state (it does not persist across
/// restarts); the preset caps it applies live in `network.temp_*_limit_kbps`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct TempLimitStatus {
    /// Whether the temporary speed limit is currently engaged.
    pub active: bool,
    /// Preset temporary upload cap in KB/s (0 = unlimited).
    pub upload_kbps: u64,
    /// Preset temporary download cap in KB/s (0 = unlimited).
    pub download_kbps: u64,
    /// Upload rate actually in force right now (KB/s, 0 = unlimited).
    pub effective_upload_kbps: u64,
    /// Download rate actually in force right now (KB/s, 0 = unlimited).
    pub effective_download_kbps: u64,
}

/// Body of `PUT /api/v1/bandwidth/temp-limit`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct TempLimitRequest {
    /// Engage (`true`) or release (`false`) the temporary speed limit.
    pub active: bool,
}

/// GET /api/v1/config  —  PUT /api/v1/config
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ConfigResponse {
    /// Configuration values currently in effect in the running daemon.
    /// Bandwidth limits reflect the live throttle, not the on-disk value.
    pub current: ConfigSnapshot,
    /// On-disk configuration waiting for a daemon restart to take effect.
    /// Absent when there are no pending changes.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pending: Option<Box<ConfigSnapshot>>,
}

/// A point-in-time snapshot of the full daemon configuration.
///
/// Used for both `current` (values in effect right now) and `pending`
/// (values saved to disk but not yet applied — requires a restart).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ConfigSnapshot {
    pub node: NodeConfig,
    pub api: ApiConfig,
    pub network: NetworkConfig,
    pub storage: StorageConfig,
    pub emule: EmuleConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct EmuleConfig {
    /// Whether the eMule / Kad2 subsystem is enabled at runtime.
    pub enabled: bool,
    /// Directory where in-progress eMule `.part` files are stored.
    pub temp_dir: String,
    /// UDP port for the Kad2 socket.
    pub udp_port: u16,
    /// TCP port for incoming eMule peer connections.
    pub tcp_port: u16,
    /// Manually configured external IPv4 address, or `null` to auto-detect.
    #[serde(default)]
    pub external_ip: Option<String>,
    /// Number of simultaneous peer connections opened per eMule download.
    pub download_slots_per_file: usize,
    /// Maximum number of simultaneous eMule upload slots.
    pub max_upload_slots: usize,
    /// Maximum number of eMule downloads that run concurrently.
    pub max_concurrent_downloads: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct NodeConfig {
    pub identity_path: String,
    pub listen_addrs: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ApiConfig {
    pub listen: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct NetworkConfig {
    pub bootstrap_peers: Vec<String>,
    /// Upload bandwidth limit in KB/s.  0 = unlimited.
    #[serde(default)]
    pub upload_limit_kbps: u64,
    /// Download bandwidth limit in KB/s.  0 = unlimited.
    #[serde(default)]
    pub download_limit_kbps: u64,
    /// Preset upload cap in KB/s used while the temporary speed limit is on.
    #[serde(default = "default_temp_limit")]
    pub temp_upload_limit_kbps: u64,
    /// Preset download cap in KB/s used while the temporary speed limit is on.
    #[serde(default = "default_temp_limit")]
    pub temp_download_limit_kbps: u64,
    /// Maximum number of concurrent chunk-upload tasks.  Default: 64.
    #[serde(default = "default_max_upload_tasks")]
    pub max_upload_tasks: usize,
}

fn default_max_upload_tasks() -> usize {
    64
}

fn default_temp_limit() -> u64 {
    5000
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StorageConfig {
    pub download_dir: String,
    pub temp_dir: String,
    pub database_path: String,
}
