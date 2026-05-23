/// POST /api/v1/downloads
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StartDownloadRequest {
    pub magnet: String,
    /// PeerId of the provider (from a search result).
    pub provider: Option<String>,
}

/// State of a download.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub enum DownloadState {
    Queued,
    Downloading,
    Completed,
    Failed,
    Cancelled,
}

/// Response for a single download.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct DownloadResponse {
    pub id: i64,
    pub root_hash: String,
    pub name: Option<String>,
    pub size: Option<u64>,
    pub bytes_done: u64,
    pub state: DownloadState,
    pub error: Option<String>,
}

/// GET /api/v1/downloads
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct DownloadsResponse {
    pub downloads: Vec<DownloadResponse>,
}
