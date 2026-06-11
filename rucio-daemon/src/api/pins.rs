//! Pinning endpoints: `/api/v1/pins`.
//!
//! A pin keeps content available on this node on purpose. Pinning records the
//! intent (the `pins` table) and, if the content is not already present, fetches
//! it via a normal download — which, being pinned, lands in `pin_dir` where the
//! watcher indexes and re-provides it. Unpinning removes only the intent; the
//! content is left on disk (Rucio never auto-deletes — delete the share to stop
//! hosting it).

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use rucio_core::api::pins::{
    PinCollectionRequest, PinRequest, PinResponse, PinState, PinsResponse,
};

use crate::api::{AppState, DownloadRequest};
use crate::db;
use crate::transfer::parse_magnet;

/// Download statuses that count as "present or in flight", so pinning them needs
/// no new fetch (the engine would dedup it anyway).
const PRESENT_OR_INFLIGHT: &[&str] = &[
    "finding_providers",
    "queued",
    "downloading",
    "stalled",
    "completed",
];

/// Decode a hex root hash (path param) into 32 bytes.
fn parse_root_hash(hex_str: &str) -> Option<[u8; 32]> {
    hex::decode(hex_str).ok()?.try_into().ok()
}

/// Resolve a pin's display name/size/state: `available` if shared, `fetching`
/// if an active download row exists, else `missing`.
async fn resolve_state(
    state: &AppState,
    hash: &[u8; 32],
) -> (Option<String>, Option<u64>, PinState) {
    if let Ok(Some(share)) = db::shares::get_by_hash(&state.db, hash).await {
        return (
            Some(share.name),
            Some(share.size as u64),
            PinState::Available,
        );
    }
    if let Ok(Some(dl)) = db::downloads::get_by_root_hash(&state.db, hash).await {
        let active = matches!(
            dl.status.as_str(),
            "finding_providers" | "queued" | "downloading" | "stalled"
        );
        let st = if active {
            PinState::Fetching
        } else {
            PinState::Missing
        };
        return (Some(dl.name), Some(dl.total_size as u64), st);
    }
    (None, None, PinState::Missing)
}

/// List pins.
///
/// Returns every pinned root hash with its resolved name, size and state.
#[utoipa::path(
    get,
    path = "/api/v1/pins",
    tag = "pins",
    responses((status = 200, description = "All pins", body = PinsResponse)),
)]
pub async fn list_pins(State(state): State<AppState>) -> Result<Json<PinsResponse>, StatusCode> {
    let rows = db::pins::list(&state.db).await.map_err(|e| {
        tracing::error!("list pins: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let mut pins = Vec::with_capacity(rows.len());
    for row in rows {
        let Ok(hash) = <[u8; 32]>::try_from(row.root_hash.as_slice()) else {
            continue;
        };
        let (name, size, state_) = resolve_state(&state, &hash).await;
        pins.push(PinResponse {
            root_hash: hex::encode(hash),
            name,
            size,
            state: state_,
            collection: row.collection.filter(|c| !c.is_empty()),
            added_at: row.added_at,
        });
    }
    let collections = db::pins::collections(&state.db).await.unwrap_or_default();
    Ok(Json(PinsResponse { pins, collections }))
}

/// Pin a magnet.
///
/// Records the pin intent (idempotent) and, if the content is neither present
/// nor already being fetched, starts a download for it — which, being pinned,
/// completes into `pin_dir`. Returns 200 when nothing had to be fetched, 202
/// when a fetch was started.
#[utoipa::path(
    post,
    path = "/api/v1/pins",
    tag = "pins",
    request_body = PinRequest,
    responses(
        (status = 200, description = "Pinned; content already present or in flight", body = PinResponse),
        (status = 202, description = "Pinned; a fetch was started", body = PinResponse),
        (status = 400, description = "Malformed magnet link"),
    )
)]
pub async fn create_pin(
    State(state): State<AppState>,
    Json(req): Json<PinRequest>,
) -> Result<(StatusCode, Json<PinResponse>), StatusCode> {
    let Ok(info) = parse_magnet(&req.magnet) else {
        return Err(StatusCode::BAD_REQUEST);
    };

    // Normalise the collection: trim, and treat empty as uncollected.
    let collection = req
        .collection
        .as_deref()
        .map(str::trim)
        .filter(|c| !c.is_empty());

    // Record the intent first (idempotent), so completion routes to pin_dir even
    // if the fetch is already running.
    db::pins::add(&state.db, &info.root_hash, collection, crate::now_secs())
        .await
        .map_err(|e| {
            tracing::error!("add pin: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let present = matches!(
        db::shares::get_by_hash(&state.db, &info.root_hash).await,
        Ok(Some(_))
    ) || matches!(
        db::downloads::status_by_root_hash(&state.db, &info.root_hash).await,
        Ok(Some(ref s)) if PRESENT_OR_INFLIGHT.contains(&s.as_str())
    );

    let status = if present {
        StatusCode::OK
    } else {
        // Fetch it: a normal download that, being pinned, lands in pin_dir.
        let providers: Vec<String> = req
            .providers
            .into_iter()
            .filter(|s| s.parse::<libp2p::PeerId>().is_ok())
            .collect();
        let dl_req = DownloadRequest::Start {
            magnet: req.magnet.clone(),
            providers,
            category_id: None,
        };
        if state.download_tx.send(dl_req).await.is_err() {
            tracing::error!("download channel closed while pinning");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        StatusCode::ACCEPTED
    };

    let (name, size, st) = resolve_state(&state, &info.root_hash).await;
    let resp = PinResponse {
        root_hash: hex::encode(info.root_hash),
        name: name.or(info.name),
        size: size.or(info.size),
        state: st,
        collection: collection.map(str::to_string),
        added_at: crate::now_secs() as i64,
    };
    Ok((status, Json(resp)))
}

/// Move a pin to a different collection (or clear it). The change is reflected
/// in our published pin-set on the next request from any subscriber.
#[utoipa::path(
    put,
    path = "/api/v1/pins/{hash}/collection",
    tag = "pins",
    params(("hash" = String, Path, description = "Root hash (hex)")),
    request_body = PinCollectionRequest,
    responses(
        (status = 204, description = "Collection updated"),
        (status = 400, description = "Malformed hash"),
        (status = 404, description = "Not pinned"),
    )
)]
pub async fn set_pin_collection(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Json(req): Json<PinCollectionRequest>,
) -> StatusCode {
    let Some(root_hash) = parse_root_hash(&hash) else {
        return StatusCode::BAD_REQUEST;
    };
    let collection = req
        .collection
        .as_deref()
        .map(str::trim)
        .filter(|c| !c.is_empty());
    match db::pins::set_collection(&state.db, &root_hash, collection).await {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("set pin collection: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Unpin a root hash.
///
/// Removes the pin intent only. The content stays on disk and shared; delete the
/// share explicitly to stop hosting it.
#[utoipa::path(
    delete,
    path = "/api/v1/pins/{hash}",
    tag = "pins",
    params(("hash" = String, Path, description = "Root hash (hex)")),
    responses(
        (status = 204, description = "Unpinned"),
        (status = 400, description = "Malformed hash"),
        (status = 404, description = "Not pinned"),
    )
)]
pub async fn delete_pin(State(state): State<AppState>, Path(hash): Path<String>) -> StatusCode {
    let Some(root_hash) = parse_root_hash(&hash) else {
        return StatusCode::BAD_REQUEST;
    };
    match db::pins::remove(&state.db, &root_hash).await {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("remove pin: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
