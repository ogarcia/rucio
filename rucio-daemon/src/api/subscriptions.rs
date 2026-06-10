//! Subscription endpoints: `/api/v1/subscriptions`.
//!
//! A subscription mirrors another node's published pin-set within a disk quota
//! (cooperative pinning). Creating one records the intent and kicks an immediate
//! pin-set request; the reconcile loop (see [`crate::pinset`]) then keeps the
//! mirror in sync. Removing one drops the mirror rows (ON DELETE CASCADE) and
//! sweeps any content that is now wanted by nobody.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use libp2p::PeerId;
use rucio_core::api::subscriptions::{
    MirrorFile, MirrorFileState, SubscriptionFilesResponse, SubscriptionRequest,
    SubscriptionResponse, SubscriptionsResponse, parse_peer_input,
};

use crate::api::AppState;
use crate::db;

/// Resolve a wanted mirror hash to its real state: present on disk, being
/// fetched, or still missing (no provider / queued).
async fn resolve_wanted_state(state: &AppState, hash: &[u8; 32]) -> MirrorFileState {
    if matches!(db::shares::get_by_hash(&state.db, hash).await, Ok(Some(_))) {
        return MirrorFileState::Present;
    }
    let active = matches!(
        db::downloads::status_by_root_hash(&state.db, hash).await,
        Ok(Some(ref s)) if matches!(s.as_str(), "finding_providers" | "queued" | "downloading" | "stalled")
    );
    if active {
        MirrorFileState::Fetching
    } else {
        MirrorFileState::Missing
    }
}

/// Enrich a stored subscription row with its current mirror progress.
async fn to_response(
    state: &AppState,
    sub: db::pin_subscriptions::SubscriptionRow,
) -> SubscriptionResponse {
    let used_bytes = db::mirror_pins::wanted_bytes_for_peer(&state.db, &sub.peer_id)
        .await
        .unwrap_or(0)
        .max(0) as u64;
    let (present_count, present_bytes) = db::mirror_pins::present_for_peer(&state.db, &sub.peer_id)
        .await
        .unwrap_or((0, 0));
    let rows = db::mirror_pins::list_for_peer(&state.db, &sub.peer_id)
        .await
        .unwrap_or_default();
    let wanted_count = rows
        .iter()
        .filter(|r| r.state == db::mirror_pins::STATE_WANTED)
        .count();
    let skipped_count = rows.len() - wanted_count;
    SubscriptionResponse {
        peer_id: sub.peer_id,
        quota_bytes: sub.quota_bytes.max(0) as u64,
        used_bytes,
        present_bytes: present_bytes.max(0) as u64,
        wanted_count,
        present_count: present_count.max(0) as usize,
        skipped_count,
        last_version: sub.last_version,
        last_synced_at: sub.last_synced_at,
        added_at: sub.added_at,
    }
}

/// List subscriptions.
///
/// Returns every subscribed peer with its quota and current mirror progress.
#[utoipa::path(
    get,
    path = "/api/v1/subscriptions",
    tag = "subscriptions",
    responses((status = 200, description = "All subscriptions", body = SubscriptionsResponse)),
)]
pub async fn list_subscriptions(
    State(state): State<AppState>,
) -> Result<Json<SubscriptionsResponse>, StatusCode> {
    let rows = db::pin_subscriptions::list(&state.db).await.map_err(|e| {
        tracing::error!("list subscriptions: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let mut subscriptions = Vec::with_capacity(rows.len());
    for row in rows {
        subscriptions.push(to_response(&state, row).await);
    }
    Ok(Json(SubscriptionsResponse { subscriptions }))
}

/// Subscribe to a peer's pin-set.
///
/// Accepts a bare PeerId or a `rucio-peer:` link. Idempotent: re-subscribing
/// updates the quota. Triggers an immediate pin-set request so the first sync
/// doesn't wait for the next reconcile tick.
#[utoipa::path(
    post,
    path = "/api/v1/subscriptions",
    tag = "subscriptions",
    request_body = SubscriptionRequest,
    responses(
        (status = 201, description = "Subscribed", body = SubscriptionResponse),
        (status = 400, description = "Invalid peer id or non-positive quota"),
    )
)]
pub async fn create_subscription(
    State(state): State<AppState>,
    Json(req): Json<SubscriptionRequest>,
) -> Result<(StatusCode, Json<SubscriptionResponse>), StatusCode> {
    if req.quota_bytes == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let peer_str = parse_peer_input(&req.peer);
    let Ok(peer) = peer_str.parse::<PeerId>() else {
        return Err(StatusCode::BAD_REQUEST);
    };
    // Mirroring ourselves is a no-op that would only churn the reconcile.
    if state.node_status.read().await.peer_id == peer.to_string() {
        return Err(StatusCode::BAD_REQUEST);
    }

    db::pin_subscriptions::upsert(
        &state.db,
        &peer.to_string(),
        req.quota_bytes as i64,
        crate::now_secs(),
    )
    .await
    .map_err(|e| {
        tracing::error!("upsert subscription: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Kick a sync now (best-effort).
    crate::pinset::request_one_pinset(&state.node_cmd, peer).await;

    let sub = db::pin_subscriptions::get(&state.db, &peer.to_string())
        .await
        .ok()
        .flatten();
    match sub {
        Some(row) => Ok((StatusCode::CREATED, Json(to_response(&state, row).await))),
        None => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Unsubscribe from a peer.
///
/// Drops the subscription and its mirror rows, then evicts any mirror content
/// that is now wanted by no subscription and isn't a manual pin.
#[utoipa::path(
    delete,
    path = "/api/v1/subscriptions/{peer_id}",
    tag = "subscriptions",
    params(("peer_id" = String, Path, description = "The mirrored peer's PeerId")),
    responses(
        (status = 204, description = "Unsubscribed"),
        (status = 404, description = "No such subscription"),
    )
)]
pub async fn delete_subscription(
    State(state): State<AppState>,
    Path(peer_id): Path<String>,
) -> StatusCode {
    // Accept a bare id or a link, normalised through the PeerId parser so the
    // stored base58 form is matched exactly.
    let parsed = parse_peer_input(&peer_id);
    let key = parsed.parse::<PeerId>().map(|p| p.to_string());
    let key = key.as_deref().unwrap_or(parsed);

    match db::pin_subscriptions::remove(&state.db, key).await {
        Ok(true) => {
            // Cascade dropped this peer's mirror rows; clean up orphaned content.
            crate::pinset::evict_unwanted(
                &state.db,
                &state.node_cmd,
                &state.config.storage.pin_dir,
            )
            .await;
            StatusCode::NO_CONTENT
        }
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::error!("remove subscription: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// List a subscription's mirrored files with their resolved state.
///
/// Each wanted file is resolved to `present` (on disk), `fetching` (in flight)
/// or `missing` (no provider yet); over-quota entries are `skipped`. Sorted
/// present → fetching → missing → skipped, then by size descending.
#[utoipa::path(
    get,
    path = "/api/v1/subscriptions/{peer_id}/files",
    tag = "subscriptions",
    params(("peer_id" = String, Path, description = "The mirrored peer's PeerId")),
    responses(
        (status = 200, description = "The peer's mirror files", body = SubscriptionFilesResponse),
    )
)]
pub async fn list_subscription_files(
    State(state): State<AppState>,
    Path(peer_id): Path<String>,
) -> Result<Json<SubscriptionFilesResponse>, StatusCode> {
    let parsed = parse_peer_input(&peer_id);
    let key = parsed.parse::<PeerId>().map(|p| p.to_string());
    let key = key.as_deref().unwrap_or(parsed);

    let rows = db::mirror_pins::list_for_peer(&state.db, key)
        .await
        .map_err(|e| {
            tracing::error!("list mirror files: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut files = Vec::with_capacity(rows.len());
    for r in rows {
        let state_ = if r.state == db::mirror_pins::STATE_SKIPPED {
            MirrorFileState::Skipped
        } else if let Ok(hash) = <[u8; 32]>::try_from(r.root_hash.as_slice()) {
            resolve_wanted_state(&state, &hash).await
        } else {
            MirrorFileState::Missing
        };
        files.push(MirrorFile {
            root_hash: hex::encode(&r.root_hash),
            name: r.name,
            size: r.size.max(0) as u64,
            state: state_,
        });
    }

    // Order: present, fetching, missing, skipped; then largest first.
    fn rank(s: MirrorFileState) -> u8 {
        match s {
            MirrorFileState::Present => 0,
            MirrorFileState::Fetching => 1,
            MirrorFileState::Missing => 2,
            MirrorFileState::Skipped => 3,
        }
    }
    files.sort_by(|a, b| rank(a.state).cmp(&rank(b.state)).then(b.size.cmp(&a.size)));

    Ok(Json(SubscriptionFilesResponse { files }))
}
