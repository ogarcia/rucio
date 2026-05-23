//! POST /api/v1/search
//! GET  /api/v1/search/:query_id
//!
//! Search is asynchronous: POST publishes a Gossipsub query and returns a
//! `query_id`; GET polls for results accumulated in the in-memory SearchStore.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use rucio_core::api::search::{SearchRequest, SearchResultsResponse, SearchStartedResponse};
use rucio_core::protocol::search::SearchQuery;

use crate::api::{AppState, SearchEntry};
use crate::node::messages::NodeCmd;

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
    State(state): State<AppState>,
    Json(req): Json<SearchRequest>,
) -> Result<(StatusCode, Json<SearchStartedResponse>), StatusCode> {
    if req.keywords.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let peer_id = state.node_status.read().await.peer_id.clone();
    let query = SearchQuery::new(req.keywords.clone(), peer_id);
    let query_id = query.id.0.clone();

    // Register the query in the store before publishing so we don't miss
    // results that arrive before the response is returned to the client.
    {
        let mut store = state.search_store.write().await;
        store.insert(
            query_id.clone(),
            SearchEntry {
                results: vec![],
                pending: true,
                started_at: std::time::Instant::now(),
            },
        );
    }

    // Fire the gossipsub query (best-effort; log but don't fail the request).
    if state.node_cmd.send(NodeCmd::Search(query)).await.is_err() {
        tracing::warn!("Node cmd channel closed; search published locally only");
    }

    tracing::info!(query_id, keywords = ?req.keywords, "Search started");

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
    State(state): State<AppState>,
    Path(query_id): Path<String>,
) -> Result<Json<SearchResultsResponse>, StatusCode> {
    let mut store = state.search_store.write().await;

    let entry = store.get_mut(&query_id).ok_or(StatusCode::NOT_FOUND)?;

    // Close the window if the TTL has expired.
    if entry.pending && entry.started_at.elapsed().as_secs() >= crate::api::SEARCH_WINDOW_SECS {
        entry.pending = false;
    }

    Ok(Json(SearchResultsResponse {
        query_id,
        results: entry.results.clone(),
        pending: entry.pending,
    }))
}
