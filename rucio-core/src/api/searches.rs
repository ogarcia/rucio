//! Public request/response types for the unified search API.
//!
//! These types are shared between the daemon (handler logic) and the CLI
//! (HTTP client).  All searches are unified: they run Gossipsub (Rucio peers)
//! and Kad2 keyword search (eMule network) in parallel.

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// POST /api/v1/searches — start a unified search.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct StartSearchRequest {
    pub keywords: Vec<String>,
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
    /// for eMule results.  Exactly one of `download_link` / `provider` is `Some`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_link: Option<String>,
    /// PeerId of the Rucio peer that has this file.  Only present for Rucio
    /// results; `None` for eMule results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
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
}
