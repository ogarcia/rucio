/// POST /api/v1/search
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SearchRequest {
    pub keywords: Vec<String>,
}

/// Response after launching a search — returns the query_id to poll results.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SearchStartedResponse {
    pub query_id: String,
}

/// A single search result.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SearchResultResponse {
    pub root_hash: String,
    pub name: String,
    pub size: u64,
    pub chunk_count: usize,
    pub mime_type: Option<String>,
    pub magnet: String,
}

/// GET /api/v1/search/:query_id
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SearchResultsResponse {
    pub query_id: String,
    pub results: Vec<SearchResultResponse>,
    /// Whether the search window is still open (peers may still respond).
    pub pending: bool,
}
