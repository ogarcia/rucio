/// POST /api/v1/downloads
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StartDownloadRequest {
    pub magnet: String,
    /// PeerIds of known providers (from search results).
    /// Optional — if omitted or empty the daemon will discover providers via
    /// Kademlia DHT automatically.  Supplying providers from a gossip search
    /// result enables an immediate fast start while DHT runs in parallel.
    #[serde(default)]
    pub providers: Vec<String>,
}

/// State of a download.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub enum DownloadState {
    /// No providers known yet; searching the DHT for peers that have this file.
    FindingProviders,
    /// Providers found; waiting to start transferring chunks.
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

/// POST /api/v1/downloads/ed2k
///
/// Queue a download from the eMule network using an `ed2k://` link.
/// Requires the `emule-compat` feature to be compiled into the daemon.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StartEd2kDownloadRequest {
    /// Full `ed2k://|file|…|…|…|/` link.
    pub link: String,
}

/// Response for POST /api/v1/downloads/ed2k.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StartEd2kDownloadResponse {
    /// Assigned download ID.
    pub id: i64,
    /// Parsed file name.
    pub name: String,
    /// File size in bytes.
    pub size: u64,
    /// ed2k hash (hex).
    pub ed2k_hash: String,
}
