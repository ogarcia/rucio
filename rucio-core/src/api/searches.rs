//! Public request/response types for the unified search API.
//!
//! These types are shared between the daemon (handler logic) and the CLI
//! (HTTP client).  All searches are unified: they run Gossipsub (Rucio peers)
//! and Kad2 keyword search (eMule network) in parallel.

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Which network(s) a search should query.
///
/// Defaults to [`SearchNetwork::Both`] so a client that omits the field still
/// gets unified results. A third-party API consumer can scope a search to a
/// single protocol; the bundled web panel always searches both.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum SearchNetwork {
    /// Query only the Rucio P2P network (Gossipsub).
    Rucio,
    /// Query only the eMule/Kad2 network.
    Emule,
    /// Query both networks in parallel (the default).
    #[default]
    Both,
}

impl SearchNetwork {
    /// Whether the Rucio (Gossipsub) leg should run for this selection.
    pub fn wants_rucio(self) -> bool {
        matches!(self, SearchNetwork::Rucio | SearchNetwork::Both)
    }

    /// Whether the eMule (Kad2) leg should run for this selection.
    pub fn wants_emule(self) -> bool {
        matches!(self, SearchNetwork::Emule | SearchNetwork::Both)
    }
}

/// POST /api/v1/searches — start a unified search.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StartSearchRequest {
    pub keywords: Vec<String>,
    /// Which network(s) to query. Optional; defaults to `both`. Lets an API
    /// consumer focus on a single protocol (`rucio` or `emule`).
    #[serde(default)]
    pub network: SearchNetwork,
}

/// Response body returned by POST /api/v1/searches and POST /api/v1/searches/{id}/relaunch.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SearchStartedResponse {
    /// Numeric identifier for the new search.
    pub id: u64,
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Current lifecycle state of a search.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SearchState {
    /// Search is still open; results may still arrive.
    Running,
    /// Search window has closed; no further results will be added.
    Done,
    /// Search was explicitly cancelled by the client.
    Cancelled,
}

// ---------------------------------------------------------------------------
// List response
// ---------------------------------------------------------------------------

/// Summary of a single search (no results), used in list responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SearchSummary {
    pub id: u64,
    pub keywords: Vec<String>,
    pub state: SearchState,
    pub result_count: usize,
    /// True while the eMule/Kad2 leg of this search is queued behind another
    /// Kad search, waiting for its turn (Kad runs one search at a time). The
    /// Rucio (gossip) leg is unaffected and may still be returning results.
    #[serde(default)]
    pub emule_queued: bool,
}

/// GET /api/v1/searches response body.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SearchListResponse {
    pub searches: Vec<SearchSummary>,
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Where a search result came from.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ResultSource {
    /// Result from the Rucio P2P network (Gossipsub).
    Rucio,
    /// Result from the eMule/Kad2 network.
    Emule,
}

/// A single file result from a unified search.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SearchResult {
    /// 1-based index within this search's result list.
    pub result_id: usize,
    /// Human-readable file name.
    pub name: String,
    /// Total file size in bytes.
    pub size: u64,
    /// Which network provided this result.
    pub source: ResultSource,
    /// Download link: a `rucio:` magnet for Rucio results, or an `ed2k://` link
    /// for eMule results. For Rucio the magnet embeds every known provider.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_link: Option<String>,
    /// PeerIds of the Rucio peers known to have this file, merged across all
    /// gossip results for the same content hash. Only present for Rucio
    /// results; `None` for eMule results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub providers: Option<Vec<String>>,
    /// Number of distinct sources for this file (Rucio: merged provider count;
    /// eMule: 1). Lets the UI show how many peers have the file.
    pub peer_count: u32,
}

// ---------------------------------------------------------------------------
// Detail response
// ---------------------------------------------------------------------------

/// GET /api/v1/searches/{id} response body.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SearchDetailResponse {
    pub id: u64,
    pub keywords: Vec<String>,
    pub state: SearchState,
    pub results: Vec<SearchResult>,
    /// See [`SearchSummary::emule_queued`].
    #[serde(default)]
    pub emule_queued: bool,
}
