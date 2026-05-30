use serde::{Deserialize, Serialize};

// ── Status ───────────────────────────────────────────────────────────────────

#[derive(Deserialize, Clone, Debug)]
pub struct StatusResponse {
    pub peer_id: String,
    pub class: String,
    pub connected_peers: usize,
    pub listen_addrs: Vec<String>,
    pub observed_addrs: Vec<String>,
    pub uptime_secs: u64,
    pub version: String,
    #[serde(default)]
    pub external_ip: Option<String>,
}

// ── eMule status ───────────────────────────────────────────────────────────

#[derive(Deserialize, Clone, Copy, Debug, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EmuleConnectivity {
    Open,
    Firewalled,
    #[default]
    Unknown,
}

#[derive(Deserialize, Clone, Debug)]
pub struct EmuleStatusResponse {
    pub feature_enabled: bool,
    #[serde(default)]
    pub runtime_enabled: bool,
    #[serde(default)]
    pub nodes_dat_present: bool,
    #[serde(default)]
    pub contacts: usize,
    #[serde(default)]
    pub connected_peers: usize,
    #[serde(default)]
    pub external_ip: Option<String>,
    #[serde(default)]
    pub tcp_port: Option<u16>,
    #[serde(default)]
    pub udp_port: Option<u16>,
    #[serde(default)]
    pub connectivity: EmuleConnectivity,
    #[serde(default)]
    pub active_downloads: usize,
    #[serde(default)]
    pub upload_slots_total: usize,
    #[serde(default)]
    pub upload_slots_in_use: usize,
    #[serde(default)]
    pub inbound_connections: u64,
}

// ── Shares ─────────────────────────────────────────────────────────────────

/// A watched directory (the unit of add/remove). GET /api/v1/shares.
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct SharedDir {
    pub path: String,
    pub protected: bool,
    pub file_count: u64,
    pub total_size: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SharedDirsResponse {
    pub dirs: Vec<SharedDir>,
}

/// A single shared file. GET /api/v1/shares/files.
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct ShareFile {
    pub root_hash: String,
    pub name: String,
    pub size: u64,
    pub path: String,
    pub magnet: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SharesFilesResponse {
    pub shares: Vec<ShareFile>,
}

/// Response to POST /api/v1/shares.
#[derive(Deserialize, Clone, Debug)]
pub struct AddShareResponse {
    pub queued: usize,
    pub errors: Vec<String>,
}

// ── Downloads ────────────────────────────────────────────────────────────────

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub enum DownloadState {
    FindingProviders,
    Queued,
    Downloading,
    Stalled,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct DownloadResponse {
    pub id: i64,
    pub root_hash: String,
    pub name: Option<String>,
    pub size: Option<u64>,
    pub bytes_done: u64,
    pub state: DownloadState,
    pub error: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct DownloadsResponse {
    pub downloads: Vec<DownloadResponse>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct DownloadDetailResponse {
    pub id: i64,
    pub kind: String,
    pub root_hash: String,
    pub name: Option<String>,
    pub size: Option<u64>,
    pub bytes_done: u64,
    pub state: DownloadState,
    pub error: Option<String>,
    pub dest_path: Option<String>,
    pub link: Option<String>,
    #[serde(default)]
    pub sources_total: Option<u32>,
    #[serde(default)]
    pub sources_active: Option<u32>,
    #[serde(default)]
    pub speed_bps: Option<u64>,
    #[serde(default)]
    pub eta_secs: Option<u64>,
}

/// GET /api/v1/downloads/{id}/pieces — per-piece state for a block bar.
#[derive(Deserialize, Clone, Debug)]
pub struct DownloadPiecesResponse {
    pub pieces_total: u64,
    /// base64 LSB-first bitmap, 1 bit/piece, set when done.
    pub done_bitmap: String,
    /// Indices being fetched right now.
    pub in_flight: Vec<u32>,
}

/// State of a single piece for rendering.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PieceState {
    Pending,
    InFlight,
    Done,
}

impl DownloadPiecesResponse {
    /// Decode the bitmap + in-flight list into a per-piece state vector.
    /// Returns an empty vector if the bitmap is malformed.
    pub fn piece_states(&self) -> Vec<PieceState> {
        use base64::Engine;
        let total = self.pieces_total as usize;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&self.done_bitmap)
            .unwrap_or_default();
        let mut states = Vec::with_capacity(total);
        for i in 0..total {
            let done = bytes
                .get(i / 8)
                .map(|b| (b >> (i % 8)) & 1 == 1)
                .unwrap_or(false);
            states.push(if done {
                PieceState::Done
            } else {
                PieceState::Pending
            });
        }
        for &idx in &self.in_flight {
            if let Some(s) = states.get_mut(idx as usize)
                && *s != PieceState::Done
            {
                *s = PieceState::InFlight;
            }
        }
        states
    }
}

/// States the daemon streams in `DownloadProgress`. A download that leaves
/// this set has reached a terminal or paused state the WS omits.
pub fn is_streamed_state(s: &DownloadState) -> bool {
    matches!(
        s,
        DownloadState::FindingProviders
            | DownloadState::Queued
            | DownloadState::Downloading
            | DownloadState::Stalled
    )
}

// ── Metrics ──────────────────────────────────────────────────────────────────

/// GET /api/v1/metrics — transfer counters for current session and all time.
#[derive(Deserialize, Clone, Debug)]
pub struct MetricsResponse {
    pub session: SessionMetrics,
    pub total: TotalMetrics,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SessionMetrics {
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub upload_speed: u64,
    pub download_speed: u64,
    pub chunks_served: u64,
    pub chunks_received: u64,
    pub chunks_rejected: u64,
    pub started_at: u64,
}

impl SessionMetrics {
    /// Seconds elapsed since the daemon started, derived from the JS clock.
    pub fn uptime_secs(&self) -> u64 {
        let now_ms = js_sys::Date::now();
        let now_secs = (now_ms / 1000.0) as u64;
        now_secs.saturating_sub(self.started_at)
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct TotalMetrics {
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub chunks_served: u64,
    pub chunks_received: u64,
    pub chunks_rejected: u64,
}

// ── Temporary speed limit ─────────────────────────────────────────────────────

#[derive(Deserialize, Clone, Debug)]
pub struct TempLimitStatus {
    pub active: bool,
    pub upload_kbps: u64,
    pub download_kbps: u64,
}

#[derive(Serialize)]
pub struct TempLimitRequest {
    pub active: bool,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct SpeedLimits {
    pub upload_kbps: u64,
    pub download_kbps: u64,
}

/// Render a KB/s rate as a human-readable cap: `Unlimited` at 0, `KB/s` below
/// 1 MB/s, `MB/s` above (whole when round, else one decimal).
pub fn format_rate_kbps(kbps: u64) -> String {
    if kbps == 0 {
        "Unlimited".to_string()
    } else if kbps >= 1024 {
        let mb = kbps as f64 / 1024.0;
        if mb.fract().abs() < 0.05 {
            format!("{mb:.0} MB/s")
        } else {
            format!("{mb:.1} MB/s")
        }
    } else {
        format!("{kbps} KB/s")
    }
}

// ── Configuration ──────────────────────────────────────────────────────────

// Mirror of the daemon's GET/PUT /api/v1/config payloads.

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct NodeConfig {
    pub identity_path: String,
    pub listen_addrs: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ApiConfig {
    pub listen: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct NetworkConfig {
    pub bootstrap_peers: Vec<String>,
    #[serde(default)]
    pub upload_limit_kbps: u64,
    #[serde(default)]
    pub download_limit_kbps: u64,
    #[serde(default)]
    pub temp_upload_limit_kbps: u64,
    #[serde(default)]
    pub temp_download_limit_kbps: u64,
    #[serde(default)]
    pub max_upload_tasks: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct StorageConfig {
    pub download_dir: String,
    pub temp_dir: String,
    pub database_path: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EmuleConfig {
    pub enabled: bool,
    pub temp_dir: String,
    pub udp_port: u16,
    pub tcp_port: u16,
    #[serde(default)]
    pub external_ip: Option<String>,
    pub download_slots_per_file: usize,
    pub max_upload_slots: usize,
    pub max_concurrent_downloads: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ConfigSnapshot {
    pub node: NodeConfig,
    pub api: ApiConfig,
    pub network: NetworkConfig,
    pub storage: StorageConfig,
    pub emule: EmuleConfig,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConfigResponse {
    pub current: ConfigSnapshot,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pending: Option<Box<ConfigSnapshot>>,
}

// ── Searches ─────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct StartSearchRequest {
    pub keywords: Vec<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SearchStartedResponse {
    pub id: u64,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SearchState {
    Running,
    Done,
    Cancelled,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ResultSource {
    Rucio,
    Emule,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SearchResult {
    pub result_id: usize,
    pub name: String,
    pub size: u64,
    pub source: ResultSource,
    #[serde(default)]
    pub download_link: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SearchDetailResponse {
    pub state: SearchState,
    pub results: Vec<SearchResult>,
    #[serde(default)]
    pub emule_queued: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SearchSummary {
    pub id: u64,
    pub keywords: Vec<String>,
    pub state: SearchState,
    pub result_count: usize,
    /// True while the eMule/Kad2 leg is queued waiting for its turn.
    #[serde(default)]
    pub emule_queued: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct SearchListResponse {
    pub searches: Vec<SearchSummary>,
}

// ── WebSocket events ─────────────────────────────────────────────────────────

// Mirrors the daemon's WsEvent: { "type": "...", "data": ... }
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum WsEvent {
    Ping,
    SessionStats {
        download_speed: u64,
        upload_speed: u64,
    },
    DownloadProgress(Vec<DownloadResponse>),
    IndexingCount {
        pending: usize,
    },
    SearchResult {
        search_id: u64,
        result: SearchResult,
    },
    SearchStateChanged {
        id: u64,
        state: SearchState,
        result_count: usize,
        #[serde(default)]
        emule_queued: bool,
    },
    // peer_id is part of the WS payload but the client only keeps a count, so
    // it isn't read — kept so the event still deserializes.
    PeerConnected {
        #[allow(dead_code)]
        peer_id: String,
    },
    PeerDisconnected {
        #[allow(dead_code)]
        peer_id: String,
    },
    NodeClassChanged {
        class: String,
    },
}

// ── Helpers ──────────────────────────────────────────────────────────────────

pub fn format_size(bytes: u64) -> String {
    const KB: f64 = 1_024.0;
    const MB: f64 = KB * 1_024.0;
    const GB: f64 = MB * 1_024.0;
    let b = bytes as f64;
    if b < KB {
        format!("{bytes} B")
    } else if b < MB {
        format!("{:.1} KB", b / KB)
    } else if b < GB {
        format!("{:.1} MB", b / MB)
    } else {
        format!("{:.2} GB", b / GB)
    }
}

pub fn format_speed(bps: u64) -> String {
    if bps == 0 {
        String::new()
    } else {
        format!("{}/s", format_size(bps))
    }
}

/// Like [`format_speed`] but always renders a value, showing `0 KB/s` at rest.
/// Used in panels (e.g. statistics) where an empty string reads as missing data.
pub fn format_speed_full(bps: u64) -> String {
    if bps == 0 {
        "0 KB/s".to_string()
    } else {
        format!("{}/s", format_size(bps))
    }
}

pub fn format_eta(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3_600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3_600, (secs % 3_600) / 60)
    }
}

pub fn format_uptime(secs: u64) -> String {
    let h = secs / 3_600;
    let m = (secs % 3_600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

pub fn class_badge(class: &str) -> (&'static str, &'static str) {
    match class {
        "HighId" => ("HighID", "badge badge-high"),
        "LowId" => ("LowID", "badge badge-low"),
        _ => ("Unknown", "badge badge-unknown"),
    }
}
