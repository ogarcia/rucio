/// GET /api/v1/config  —  PUT /api/v1/config
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ConfigResponse {
    pub node: NodeConfig,
    pub api: ApiConfig,
    pub network: NetworkConfig,
    pub storage: StorageConfig,
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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StorageConfig {
    pub download_dir: String,
    pub database_path: String,
}
