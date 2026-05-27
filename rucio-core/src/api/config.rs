/// GET /api/v1/config  —  PUT /api/v1/config
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ConfigResponse {
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
    /// Maximum number of concurrent chunk-upload tasks.  Default: 64.
    #[serde(default = "default_max_upload_tasks")]
    pub max_upload_tasks: usize,
}

fn default_max_upload_tasks() -> usize {
    64
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StorageConfig {
    pub download_dir: String,
    pub temp_dir: String,
    pub database_path: String,
}
