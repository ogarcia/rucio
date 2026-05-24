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

/// Start a search
///
/// Publishes a keyword search query over the Gossipsub network and returns a `query_id` to
/// poll for results.
///
/// The search is fully asynchronous and non-blocking. Results arrive as remote peers respond
/// to the Gossipsub message — poll `GET /api/v1/search/:query_id` repeatedly (e.g. every
/// second) until `pending` is `false` or until you have enough results.
///
/// The query window stays open for 30 seconds, after which `pending` becomes `false`
/// automatically. Results accumulated during the window remain available to poll even after
/// the window closes.
///
/// Matching is case-insensitive substring on the file name. Multiple keywords are ANDed
/// together on the responding peer's side.
#[utoipa::path(
    post,
    path = "/api/v1/search",
    request_body = SearchRequest,
    responses(
        (status = 202, description = "Search started. Use the returned `query_id` to poll for results.", body = SearchStartedResponse),
        (status = 400, description = "No keywords provided.")
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

/// Poll search results
///
/// Returns the results accumulated so far for a search query started with
/// `POST /api/v1/search`.
///
/// Each result represents one file offered by one peer. The same file (same root hash) may
/// appear multiple times if several peers have it — the CLI deduplicates by hash and shows a
/// source count.
///
/// `pending: true` means the query window is still open and more results may arrive.
/// `pending: false` means the 30-second window has closed; no further results will be added.
///
/// The query ID expires after the window closes — subsequent polls still return the
/// accumulated results but the entry may be evicted from memory eventually.
#[utoipa::path(
    get,
    path = "/api/v1/search/{query_id}",
    params(
        ("query_id" = String, Path, description = "Query ID returned by `POST /api/v1/search`.")
    ),
    responses(
        (status = 200, description = "Results accumulated so far, and whether the query is still open.", body = SearchResultsResponse),
        (status = 404, description = "Unknown or expired query ID.")
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
