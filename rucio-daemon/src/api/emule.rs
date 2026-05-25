//! GET  /api/v1/emule/status
//! POST /api/v1/emule/bootstrap
//! GET  /api/v1/kad/search?q=<keyword>

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use rucio_core::api::emule::{EmuleBootstrapRequest, EmuleBootstrapResponse, EmuleStatusResponse};
use serde::{Deserialize, Serialize};

use crate::api::AppState;

// ── GET /api/v1/emule/status ─────────────────────────────────────────────────

/// eMule compatibility status
///
/// Returns whether the `emule-compat` feature is compiled in, the configured
/// `nodes.dat` path, and how many Kad2 contacts it contains.
#[utoipa::path(
    get,
    path = "/api/v1/emule/status",
    responses(
        (status = 200, description = "eMule compatibility status.", body = EmuleStatusResponse)
    )
)]
pub async fn get_emule_status(State(state): State<AppState>) -> Json<EmuleStatusResponse> {
    #[cfg(feature = "emule-compat")]
    let resp = {
        let effective_path = crate::emule::effective_nodes_dat_path(&state.config);

        let (present, contacts) = match std::fs::read(&effective_path) {
            Ok(bytes) => {
                let n = rucio_emule::kad::routing::parse_nodes_dat(&bytes)
                    .map(|v| v.len())
                    .unwrap_or(0);
                (true, n)
            }
            Err(_) => (false, 0),
        };

        let connected_peers = state.kad_handle.contact_count().await;

        EmuleStatusResponse {
            feature_enabled: true,
            nodes_dat_path: Some(effective_path.display().to_string()),
            nodes_dat_present: present,
            contacts,
            connected_peers,
            is_connected: connected_peers >= 4,
        }
    };

    #[cfg(not(feature = "emule-compat"))]
    let resp = {
        let _ = state;
        EmuleStatusResponse {
            feature_enabled: false,
            nodes_dat_path: None,
            nodes_dat_present: false,
            contacts: 0,
            connected_peers: 0,
            is_connected: false,
        }
    };

    Json(resp)
}

// ── POST /api/v1/emule/bootstrap ─────────────────────────────────────────────

/// Download and install nodes.dat
///
/// Downloads a fresh `nodes.dat` file from the given URL (or the default
/// `http://upd.emule-security.net/nodes.dat`) and saves it to
/// `storage.nodes_dat_path` configured in the daemon.
///
/// If `storage.nodes_dat_path` is not set, the file is saved to the default
/// location (`$XDG_DATA_HOME/rucio/nodes.dat`).
///
/// Returns `501 Not Implemented` when the `emule-compat` feature is not compiled in.
#[utoipa::path(
    post,
    path = "/api/v1/emule/bootstrap",
    request_body = EmuleBootstrapRequest,
    responses(
        (status = 200, description = "nodes.dat downloaded and saved.", body = EmuleBootstrapResponse),
        (status = 400, description = "Download failed or file is invalid."),
        (status = 501, description = "emule-compat feature not compiled in.")
    )
)]
pub async fn post_emule_bootstrap(
    State(state): State<AppState>,
    Json(req): Json<EmuleBootstrapRequest>,
) -> Result<Json<EmuleBootstrapResponse>, StatusCode> {
    #[cfg(not(feature = "emule-compat"))]
    {
        let _ = (state, req);
        Err(StatusCode::NOT_IMPLEMENTED)
    }

    #[cfg(feature = "emule-compat")]
    {
        use rucio_core::api::emule::DEFAULT_NODES_DAT_URL;

        let url = req
            .url
            .as_deref()
            .unwrap_or(DEFAULT_NODES_DAT_URL)
            .to_string();

        // Determine save path: use configured path or platform default.
        let save_path = crate::emule::effective_nodes_dat_path(&state.config);

        tracing::info!(url = %url, path = %save_path.display(), "Downloading nodes.dat");

        let contacts = crate::emule::bootstrap_nodes_dat(&save_path, &url)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "Failed to download nodes.dat");
                StatusCode::BAD_REQUEST
            })?;

        tracing::info!(contacts, path = %save_path.display(), "nodes.dat saved");
        Ok(Json(EmuleBootstrapResponse {
            contacts,
            path: save_path.display().to_string(),
            url,
        }))
    }
}

// ── GET /api/v1/kad/search ────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct KadSearchQuery {
    q: String,
}

#[derive(Serialize)]
pub struct KadSearchHit {
    pub hash: String,
    pub name: String,
    pub size: u64,
}

#[derive(Serialize)]
pub struct KadSearchResponse {
    pub keyword: String,
    pub hits: Vec<KadSearchHit>,
}

/// Kad2 keyword search
///
/// Sends a `KADEMLIA2_SEARCH_KEY_REQ` into the Kad network and returns matching
/// file entries (name, hash, size).  Blocks until the search times out (~60 s).
pub async fn get_kad_search(
    State(state): State<AppState>,
    Query(params): Query<KadSearchQuery>,
) -> Result<Json<KadSearchResponse>, StatusCode> {
    #[cfg(not(feature = "emule-compat"))]
    {
        let _ = (state, params);
        return Err(StatusCode::NOT_IMPLEMENTED);
    }

    #[cfg(feature = "emule-compat")]
    {
        let keyword = params.q.trim().to_string();
        if keyword.is_empty() {
            return Err(StatusCode::BAD_REQUEST);
        }
        let hits = state.kad_handle.search_keyword(keyword.clone()).await;
        Ok(Json(KadSearchResponse {
            keyword,
            hits: hits
                .into_iter()
                .map(|h| KadSearchHit {
                    hash: hex::encode(h.hash),
                    name: h.name,
                    size: h.size,
                })
                .collect(),
        }))
    }
}
