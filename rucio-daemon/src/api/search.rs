//! POST /api/v1/search
//! GET  /api/v1/search/:query_id
//!
//! Search is asynchronous: POST starts a gossipsub query and returns a
//! query_id; GET polls for results accumulated so far.
//! The in-memory result store will be replaced with a proper state machine
//! once Gossipsub is wired up.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use rucio_core::api::search::{SearchRequest, SearchResultsResponse, SearchStartedResponse};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::api::AppState;

/// In-memory store for pending search results.
/// Key: query_id (UUID string), Value: accumulated results + pending flag.
pub type SearchStore = Arc<RwLock<HashMap<String, SearchResultsResponse>>>;

/// POST /api/v1/search
#[utoipa::path(
    post,
    path = "/api/v1/search",
    request_body = SearchRequest,
    responses(
        (status = 202, description = "Search started", body = SearchStartedResponse),
        (status = 400, description = "No keywords provided")
    )
)]
pub async fn start_search(
    State(_state): State<AppState>,
    Json(req): Json<SearchRequest>,
) -> Result<(StatusCode, Json<SearchStartedResponse>), StatusCode> {
    if req.keywords.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let query_id = Uuid::new_v4().to_string();
    // TODO: send SearchCmd to node task via node_cmd channel (Gossipsub)
    tracing::info!(query_id, keywords = ?req.keywords, "Search started (not yet implemented)");

    Ok((
        StatusCode::ACCEPTED,
        Json(SearchStartedResponse { query_id }),
    ))
}

/// GET /api/v1/search/:query_id
#[utoipa::path(
    get,
    path = "/api/v1/search/{query_id}",
    params(("query_id" = String, Path, description = "Query ID from POST /search")),
    responses(
        (status = 200, description = "Search results so far", body = SearchResultsResponse),
        (status = 404, description = "Unknown query ID")
    )
)]
pub async fn get_results(
    State(_state): State<AppState>,
    Path(query_id): Path<String>,
) -> Result<Json<SearchResultsResponse>, StatusCode> {
    // TODO: look up real results from the search state machine
    // For now, return an empty pending response for any known-format UUID.
    if Uuid::parse_str(&query_id).is_err() {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(Json(SearchResultsResponse {
        query_id,
        results: vec![],
        pending: false,
    }))
}
