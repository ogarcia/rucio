//! Unified search handlers.
//!
//! POST   /api/v1/searches            — start a search (Gossipsub + Kad2 in parallel)
//! GET    /api/v1/searches            — list all searches in memory
//! GET    /api/v1/searches/{id}       — get search detail + results
//! DELETE /api/v1/searches/{id}       — cancel or delete a search
//! POST   /api/v1/searches/{id}/relaunch — repeat a search with the same keywords

use std::cmp::Reverse;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use rucio_core::api::searches::{
    ResultSource, SearchDetailResponse, SearchListResponse, SearchResult, SearchStartedResponse,
    SearchState, SearchSummary, StartSearchRequest,
};
use rucio_core::protocol::search::SearchQuery;

use crate::api::{AppState, InternalSource, MAX_SEARCHES, SearchRecord, SearchRegistry};
use crate::node::messages::NodeCmd;

// ---------------------------------------------------------------------------
// POST /api/v1/searches
// ---------------------------------------------------------------------------

/// Start a unified search
///
/// Launches a keyword search on the Rucio Gossipsub network and, when the
/// `emule-compat` feature is compiled in, also on the eMule Kad2 network.
/// Both searches run in parallel.  Use `GET /api/v1/searches/{id}` to poll
/// for results.
///
/// A search is considered **Done** when 60 seconds have elapsed, or when the
/// Kad2 search finishes *and* at least 30 seconds have passed.
#[utoipa::path(
    post,
    path = "/api/v1/searches",
    request_body = StartSearchRequest,
    responses(
        (status = 202, description = "Search started.", body = SearchStartedResponse),
        (status = 400, description = "No keywords provided.")
    )
)]
pub async fn post_search(
    State(state): State<AppState>,
    Json(req): Json<StartSearchRequest>,
) -> Result<(StatusCode, Json<SearchStartedResponse>), StatusCode> {
    if req.keywords.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let search_id = start_search_internal(&state, req.keywords).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SearchStartedResponse { id: search_id }),
    ))
}

// ---------------------------------------------------------------------------
// GET /api/v1/searches
// ---------------------------------------------------------------------------

/// List all searches
///
/// Returns all searches currently held in memory, ordered newest-first.
#[utoipa::path(
    get,
    path = "/api/v1/searches",
    responses(
        (status = 200, description = "List of searches.", body = SearchListResponse)
    )
)]
pub async fn list_searches(State(state): State<AppState>) -> Json<SearchListResponse> {
    let reg = state.search_registry.read().await;
    let mut summaries: Vec<SearchSummary> = reg
        .records
        .values()
        .map(|r| SearchSummary {
            id: r.id,
            keywords: r.keywords.clone(),
            state: r.effective_state(),
            result_count: r.results.len(),
        })
        .collect();
    // Newest first.
    summaries.sort_by_key(|s| Reverse(s.id));
    Json(SearchListResponse {
        searches: summaries,
    })
}

// ---------------------------------------------------------------------------
// GET /api/v1/searches/{id}
// ---------------------------------------------------------------------------

/// Get search details
///
/// Returns the current state and all accumulated results for a search.
#[utoipa::path(
    get,
    path = "/api/v1/searches/{id}",
    params(
        ("id" = u64, Path, description = "Search ID returned by POST /api/v1/searches.")
    ),
    responses(
        (status = 200, description = "Search detail and results.", body = SearchDetailResponse),
        (status = 404, description = "Unknown search ID.")
    )
)]
pub async fn get_search(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<SearchDetailResponse>, StatusCode> {
    let reg = state.search_registry.read().await;
    let record = reg.records.get(&id).ok_or(StatusCode::NOT_FOUND)?;

    let results = record
        .results
        .iter()
        .enumerate()
        .map(|(i, r)| match &r.source {
            InternalSource::Rucio {
                magnet, provider, ..
            } => SearchResult {
                result_id: i + 1,
                name: r.name.clone(),
                size: r.size,
                source: ResultSource::Rucio,
                download_link: Some(magnet.clone()),
                provider: Some(provider.clone()),
            },
            InternalSource::Emule { ed2k_link, .. } => SearchResult {
                result_id: i + 1,
                name: r.name.clone(),
                size: r.size,
                source: ResultSource::Emule,
                download_link: Some(ed2k_link.clone()),
                provider: None,
            },
        })
        .collect();

    Ok(Json(SearchDetailResponse {
        id: record.id,
        keywords: record.keywords.clone(),
        state: record.effective_state(),
        results,
    }))
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/searches/{id}
// ---------------------------------------------------------------------------

/// Cancel or delete a search
///
/// - If the search is **Running**: marks it as cancelled.  Background tasks
///   will stop adding results.  Returns 204.
/// - If the search is **Done** or **Cancelled**: removes it from memory.
///   Returns 204.
/// - If the ID is unknown: returns 404.
#[utoipa::path(
    delete,
    path = "/api/v1/searches/{id}",
    params(
        ("id" = u64, Path, description = "Search ID.")
    ),
    responses(
        (status = 204, description = "Search cancelled or deleted."),
        (status = 404, description = "Unknown search ID.")
    )
)]
pub async fn delete_search(State(state): State<AppState>, Path(id): Path<u64>) -> StatusCode {
    let mut reg = state.search_registry.write().await;
    match reg.records.get_mut(&id) {
        None => StatusCode::NOT_FOUND,
        Some(record) => {
            if matches!(record.effective_state(), SearchState::Running) {
                record.cancelled = true;
            } else {
                // Done or already cancelled — remove from memory.
                let gossip_id = record.gossip_query_id.clone();
                reg.records.remove(&id);
                reg.gossip_to_id.remove(&gossip_id);
            }
            StatusCode::NO_CONTENT
        }
    }
}

// ---------------------------------------------------------------------------
// POST /api/v1/searches/{id}/relaunch
// ---------------------------------------------------------------------------

/// Relaunch a search
///
/// Re-runs the search query on both networks and appends any new results to
/// the **same** search record.  Existing results are kept.  The same search
/// ID is returned so the client can keep polling `GET /api/v1/searches/{id}`.
#[utoipa::path(
    post,
    path = "/api/v1/searches/{id}/relaunch",
    params(
        ("id" = u64, Path, description = "Search ID to relaunch.")
    ),
    responses(
        (status = 202, description = "Search relaunched; same ID.", body = SearchStartedResponse),
        (status = 404, description = "Unknown search ID.")
    )
)]
pub async fn relaunch_search(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<(StatusCode, Json<SearchStartedResponse>), StatusCode> {
    let peer_id = state.node_status.read().await.peer_id.clone();

    // Build a fresh gossip query (new UUID, same keywords).
    let keywords = {
        let reg = state.search_registry.read().await;
        reg.records
            .get(&id)
            .map(|r| r.keywords.clone())
            .ok_or(StatusCode::NOT_FOUND)?
    };
    let query = SearchQuery::new(keywords.clone(), peer_id);
    let new_gossip_id = query.id.0.clone();

    // Reset the existing record in place so existing results are preserved
    // and new results will be appended to the same record.
    {
        let mut reg = state.search_registry.write().await;

        let old_gossip_id = reg
            .records
            .get(&id)
            .map(|r| r.gossip_query_id.clone())
            .ok_or(StatusCode::NOT_FOUND)?;

        // Swap the gossip→id mapping for the new query UUID.
        reg.gossip_to_id.remove(&old_gossip_id);
        reg.gossip_to_id.insert(new_gossip_id.clone(), id);

        if let Some(record) = reg.records.get_mut(&id) {
            record.cancelled = false;
            record.started_at = std::time::Instant::now();
            record.gossip_query_id = new_gossip_id;
            #[cfg(not(feature = "emule-compat"))]
            {
                record.kad2_done = true;
            }
            #[cfg(feature = "emule-compat")]
            {
                record.kad2_done = false;
            }
        }
    }

    // Re-fire the Gossipsub query.
    if state.node_cmd.send(NodeCmd::Search(query)).await.is_err() {
        tracing::warn!("Node cmd channel closed; search published locally only");
    }

    tracing::info!(search_id = id, keywords = ?keywords, "Search relaunched (Gossipsub)");

    // Re-fire Kad2 keyword search if compiled in.
    #[cfg(feature = "emule-compat")]
    spawn_kad2_search(&state, id, keywords);

    Ok((StatusCode::ACCEPTED, Json(SearchStartedResponse { id })))
}

// ---------------------------------------------------------------------------
// Internal helper: start_search_internal
// ---------------------------------------------------------------------------

/// Create a new search record and fire off Gossipsub + (optionally) Kad2.
///
/// Returns the new numeric search ID.
async fn start_search_internal(state: &AppState, keywords: Vec<String>) -> Result<u64, StatusCode> {
    let peer_id = state.node_status.read().await.peer_id.clone();
    let query = SearchQuery::new(keywords.clone(), peer_id);
    let gossip_query_id = query.id.0.clone();

    // Allocate a new search ID and insert the record.
    let search_id = {
        let mut reg = state.search_registry.write().await;
        let id = reg.next_id;
        reg.next_id += 1;

        let record = SearchRecord {
            id,
            keywords: keywords.clone(),
            cancelled: false,
            // When emule-compat is not compiled there is no Kad2 search,
            // so mark kad2_done immediately so the search closes after the
            // shorter Gossipsub window (GOSSIP_WINDOW_SECS) rather than
            // waiting the full KAD2_TIMEOUT_SECS.
            #[cfg(not(feature = "emule-compat"))]
            kad2_done: true,
            #[cfg(feature = "emule-compat")]
            kad2_done: false,
            results: Vec::new(),
            started_at: std::time::Instant::now(),
            gossip_query_id: gossip_query_id.clone(),
        };
        reg.records.insert(id, record);
        reg.gossip_to_id.insert(gossip_query_id, id);

        // Auto-purge oldest finished searches if the registry is full.
        if reg.records.len() > MAX_SEARCHES {
            purge_old_searches(&mut reg);
        }

        id
    };

    // Fire the Gossipsub query (best-effort).
    if state.node_cmd.send(NodeCmd::Search(query)).await.is_err() {
        tracing::warn!("Node cmd channel closed; search published locally only");
    }

    tracing::info!(search_id, keywords = ?keywords, "Unified search started (Gossipsub)");

    // Spawn Kad2 keyword search if the feature is compiled in.
    #[cfg(feature = "emule-compat")]
    spawn_kad2_search(state, search_id, keywords);

    Ok(search_id)
}

/// Remove the oldest Done or Cancelled search records until the registry is
/// at or below `MAX_SEARCHES`.  Running searches are never removed.
fn purge_old_searches(reg: &mut SearchRegistry) {
    // Collect IDs of purgeable searches, sorted oldest first (lowest ID).
    let mut purgeable: Vec<u64> = reg
        .records
        .values()
        .filter(|r| !matches!(r.effective_state(), SearchState::Running))
        .map(|r| r.id)
        .collect();
    purgeable.sort_unstable();

    for id in purgeable {
        if reg.records.len() <= MAX_SEARCHES {
            break;
        }
        if let Some(record) = reg.records.remove(&id) {
            reg.gossip_to_id.remove(&record.gossip_query_id);
        }
    }
}

/// Spawn a background Kad2 keyword search task.
///
/// eMule Kad2 indexes files by individual words, not by full phrases.
/// We pick the longest normalized word as the main search key (which lands
/// in the right place in the DHT) and then filter results client-side so
/// that ALL words must appear in the filename.  Both the search key and
/// the filter are accent-folded and lowercased to match how eMule clients
/// normalize keywords before hashing.
#[cfg(feature = "emule-compat")]
fn spawn_kad2_search(state: &AppState, search_id: u64, keywords: Vec<String>) {
    use std::sync::Arc;

    let kad = state.kad_handle.clone();
    let reg_clone = Arc::clone(&state.search_registry);

    // Build the normalized word list used both for the main key selection
    // and for the client-side all-words filter.
    let norm_words: Vec<String> = keywords
        .iter()
        .flat_map(|k| k.split_whitespace())
        .map(normalize_for_kad)
        .filter(|w| !w.is_empty())
        .collect();

    // Main keyword = longest normalized word (eMule picks the most
    // "selective" word; longest is a good proxy).
    let main_keyword = norm_words
        .iter()
        .max_by_key(|w| w.len())
        .cloned()
        .unwrap_or_else(|| normalize_for_kad(&keywords[0]));

    tokio::spawn(async move {
        tracing::info!(search_id, main_keyword, "Kad2 keyword search started");
        let hits = kad.search_keyword(main_keyword.clone()).await;
        tracing::info!(search_id, hits = hits.len(), "Kad2 keyword search finished");

        let mut reg = reg_clone.write().await;
        if let Some(record) = reg.records.get_mut(&search_id) {
            if !record.cancelled {
                for h in &hits {
                    // Client-side filter: all words must appear in the
                    // normalized filename (handles accents + case).
                    let norm_name = normalize_for_kad(&h.name);
                    if !norm_words.iter().all(|w| norm_name.contains(w.as_str())) {
                        continue;
                    }

                    let hash_hex = hex::encode(h.hash);
                    let ed2k_link = format!(
                        "ed2k://|file|{}|{}|{}|/",
                        urlencoding::encode(&h.name),
                        h.size,
                        hash_hex,
                    );
                    // Deduplicate by ed2k hash.
                    let already_have = record.results.iter().any(|r| {
                        matches!(
                            &r.source,
                            crate::api::InternalSource::Emule { hash_hex: hx, .. }
                            if *hx == hex::encode(h.hash)
                        )
                    });
                    if !already_have {
                        record.results.push(crate::api::InternalResult {
                            name: h.name.clone(),
                            size: h.size,
                            source: crate::api::InternalSource::Emule {
                                hash_hex,
                                ed2k_link,
                            },
                        });
                    }
                }
            }
            record.kad2_done = true;
        }
    });
}

/// Normalize a string for Kad2 keyword hashing and client-side filtering.
///
/// Mirrors eMule's keyword normalization: lowercase + Latin diacritic folding.
/// Both the search key sent to the DHT and the client-side filename filter
/// must use this function so they operate in the same space.
fn normalize_for_kad(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        let lc = c.to_lowercase().next().unwrap_or(c);
        match lc {
            'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => out.push('a'),
            'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => out.push('e'),
            'ì' | 'í' | 'î' | 'ï' | 'ī' | 'ĭ' | 'į' | 'ĩ' => out.push('i'),
            'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' | 'ŏ' | 'ő' => out.push('o'),
            'ù' | 'ú' | 'û' | 'ü' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => out.push('u'),
            'ç' | 'ć' | 'ĉ' | 'č' => out.push('c'),
            'ñ' | 'ń' | 'ņ' | 'ň' => out.push('n'),
            'ý' | 'ÿ' => out.push('y'),
            'ð' | 'ď' => out.push('d'),
            'ß' => {
                out.push('s');
                out.push('s');
            }
            'æ' => {
                out.push('a');
                out.push('e');
            }
            'ł' => out.push('l'),
            'þ' => {
                out.push('t');
                out.push('h');
            }
            'ź' | 'ż' | 'ž' => out.push('z'),
            'š' | 'ś' | 'ş' | 'ŝ' => out.push('s'),
            'ř' | 'ŗ' => out.push('r'),
            'ğ' | 'ĝ' | 'ġ' => out.push('g'),
            'ħ' => out.push('h'),
            'ĵ' => out.push('j'),
            'ķ' => out.push('k'),
            'ľ' | 'ļ' | 'ĺ' => out.push('l'),
            'ţ' | 'ť' => out.push('t'),
            other => out.push(other),
        }
    }
    out
}
