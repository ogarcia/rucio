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
    /// Still trying, but stuck: no sources/providers (or unresponsive ones)
    /// found after several search rounds.  The daemon keeps retrying in the
    /// background — this is not a terminal state.
    Stalled,
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

/// GET /api/v1/downloads/{id} — full detail for a single download.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct DownloadDetailResponse {
    pub id: i64,
    /// Transport the download uses: `"rucio"` (libp2p) or `"emule"` (ed2k).
    pub kind: String,
    /// Full hash in hex — BLAKE3 for rucio downloads, MD4 for eMule.
    pub root_hash: String,
    pub name: Option<String>,
    pub size: Option<u64>,
    pub bytes_done: u64,
    pub state: DownloadState,
    pub error: Option<String>,
    /// Final destination path; absent until the download has somewhere to land.
    pub dest_path: Option<String>,
    /// Unix timestamp (seconds) when the download was added.
    pub added_at: i64,
    /// Unix timestamp (seconds) of the last status/progress update.
    pub updated_at: i64,
    /// Source link to re-add the download: a `rucio:` magnet for rucio
    /// downloads, the original `ed2k://` link for eMule downloads.
    pub link: Option<String>,
    /// Completed pieces — chunks for rucio downloads, 9.28 MB slices for eMule.
    pub pieces_done: Option<u64>,
    /// Total number of pieces.
    pub pieces_total: Option<u64>,
    /// Live: sources/providers currently known.  Present only while active.
    #[serde(default)]
    pub sources_total: Option<u32>,
    /// Live: sources/providers we are actively transferring from.
    #[serde(default)]
    pub sources_active: Option<u32>,
    /// Live: chunks/slices being fetched right now.
    #[serde(default)]
    pub pieces_in_flight: Option<u32>,
    /// Live: smoothed download speed in bytes per second.
    #[serde(default)]
    pub speed_bps: Option<u64>,
    /// Live: estimated seconds to completion, derived from speed and remaining
    /// bytes.  Absent when speed is zero or the size is unknown.
    #[serde(default)]
    pub eta_secs: Option<u64>,
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
