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

// ── Downloads ────────────────────────────────────────────────────────────────

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub enum DownloadState {
    FindingProviders,
    Queued,
    Downloading,
    Stalled,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Deserialize, Clone, Debug)]
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
    pub added_at: i64,
    pub updated_at: i64,
    pub link: Option<String>,
    pub pieces_done: Option<u64>,
    pub pieces_total: Option<u64>,
    #[serde(default)]
    pub sources_total: Option<u32>,
    #[serde(default)]
    pub sources_active: Option<u32>,
    #[serde(default)]
    pub pieces_in_flight: Option<u32>,
    #[serde(default)]
    pub speed_bps: Option<u64>,
    #[serde(default)]
    pub eta_secs: Option<u64>,
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
    pub id: u64,
    pub keywords: Vec<String>,
    pub state: SearchState,
    pub results: Vec<SearchResult>,
}

// ── WebSocket events ─────────────────────────────────────────────────────────

// Mirrors the daemon's WsEvent: { "type": "...", "data": ... }
// SearchResult here uses the old gossip format (api::search::SearchResultResponse).
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum WsEvent {
    DownloadProgress(Vec<DownloadResponse>),
    IndexingCount { pending: usize },
    SearchResult(WsSearchResult),
    PeerConnected { peer_id: String },
    PeerDisconnected { peer_id: String },
    NodeClassChanged { class: String },
}

/// Search result as pushed by the WS bus (Rucio gossip format).
#[derive(Deserialize, Clone, Debug)]
pub struct WsSearchResult {
    pub root_hash: String,
    pub name: String,
    pub size: u64,
    pub magnet: String,
    pub provider: String,
    #[serde(default)]
    pub mime_type: Option<String>,
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
