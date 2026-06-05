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
    /// Suspended by the user.  No transfer activity and no provider discovery.
    /// Progress is preserved on disk; resume with `POST /api/v1/downloads/:id/resume`.
    /// A paused download is **not** resumed automatically when the daemon restarts.
    Paused,
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
    /// Live: per-peer breakdown of the sources we are downloading from.
    /// libp2p only for now (empty for eMule downloads); present only while active.
    #[serde(default)]
    pub peers: Vec<DownloadPeerDetail>,
}

/// Live per-peer download detail: one source we are pulling chunks from.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct DownloadPeerDetail {
    /// Base58 PeerId (libp2p) of the source.
    pub peer_id: String,
    /// Best-known network address (multiaddr) of the peer, when one is known.
    pub address: Option<String>,
    /// Bytes received from this peer for this download so far.
    pub bytes_downloaded: u64,
    /// Chunks currently in flight to this peer.
    pub chunks_in_flight: u32,
    /// Smoothed download rate from this peer, in bytes per second.
    pub rate_bps: u64,
}

/// GET /api/v1/downloads/{id}/pieces — per-piece state for rendering a block bar.
///
/// The completed pieces are sent as a compact bitmap rather than a per-piece
/// array: a 1.5 GB libp2p download is ~6000 chunks, which is ~750 bytes as a
/// bitmap (1 bit/piece) versus ~15 KB as a JSON array of states.
///
/// The client reconstructs the three states: `done` from the bitmap, `in_flight`
/// from the index list, and everything else is pending.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct DownloadPiecesResponse {
    pub id: i64,
    /// Transport: `"rucio"` (libp2p chunks) or `"emule"` (9.28 MB slices).
    pub kind: String,
    /// Total number of pieces.
    pub pieces_total: u64,
    /// Base64 of a little-endian (LSB-first) bitmap, one bit per piece, set
    /// when the piece is complete. Bit `i` lives in `byte[i / 8] >> (i % 8)`.
    /// Length is `ceil(pieces_total / 8)` bytes before encoding.
    pub done_bitmap: String,
    /// Indices of pieces being fetched right now. Live data — empty when the
    /// download is not active. These are not reflected in `done_bitmap`.
    pub in_flight: Vec<u32>,
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

/// Request body for POST /api/v1/downloads/{id}/rename.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct RenameDownloadRequest {
    /// New file name the download will be saved as on completion. Only the
    /// final path component is kept; directory separators are stripped.
    pub name: String,
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
