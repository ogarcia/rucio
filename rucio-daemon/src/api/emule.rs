//! GET  /api/v1/emule/status
//! POST /api/v1/emule/bootstrap
//! GET  /api/v1/emule/search?q=<keyword>

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use rucio_core::api::emule::{
    EmuleBootstrapRequest, EmuleBootstrapResponse, EmuleConnectivity, EmuleStatusResponse,
};
use serde::{Deserialize, Serialize};
use utoipa::IntoParams;

use crate::api::AppState;

// ── GET /api/v1/emule/status ─────────────────────────────────────────────────

/// eMule compatibility status
///
/// Returns the full runtime state of the eMule subsystem: whether the
/// `emule-compat` feature is compiled in and enabled at runtime, the
/// configured `nodes.dat` path and contact count, the Kad2 routing table size,
/// the eMule TCP / Kad UDP ports, the external IP (UPnP or configured), the
/// inferred TCP connectivity class (`open` / `firewalled` / `unknown`) with a
/// short explanation, the number of active downloads, upload slot usage, and
/// the count of inbound TCP connections accepted since startup.
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
        if !state.config.emule.enabled {
            EmuleStatusResponse {
                feature_enabled: true,
                runtime_enabled: false,
                nodes_dat_path: None,
                nodes_dat_present: false,
                contacts: 0,
                connected_peers: 0,
                is_connected: false,
                external_ip: None,
                external_ip_source: None,
                tcp_port: None,
                udp_port: None,
                connectivity: EmuleConnectivity::Unknown,
                connectivity_reason: None,
                active_downloads: 0,
                upload_slots_total: 0,
                upload_slots_in_use: 0,
                inbound_connections: 0,
            }
        } else {
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

            let upnp_external_ip = state.external_ip.read().await.clone();
            let configured_external_ip = state.config.emule.external_ip.map(|ip| ip.to_string());
            // Fallback: the IP our Kad2 peers report back to us (works even when
            // UPnP is off and no IP is configured).
            let kad_external_ip = state.kad_handle.external_ip().map(|ip| ip.to_string());

            let (external_ip, external_ip_source) =
                match (&upnp_external_ip, &configured_external_ip, &kad_external_ip) {
                    (Some(ip), _, _) => (Some(ip.clone()), Some("upnp".to_string())),
                    (None, Some(ip), _) => (Some(ip.clone()), Some("config".to_string())),
                    (None, None, Some(ip)) => (Some(ip.clone()), Some("peers".to_string())),
                    (None, None, None) => (None, None),
                };

            let inbound = state
                .emule_inbound_connections
                .load(std::sync::atomic::Ordering::Relaxed);

            let upload_slots_total = state.config.emule.max_upload_slots.clamp(1, 50);
            let upload_slots_in_use =
                upload_slots_total.saturating_sub(state.emule_upload_slots.available_permits());

            let (connectivity, connectivity_reason) = classify_connectivity(
                inbound,
                state.config.network.upnp,
                upnp_external_ip.as_deref(),
                configured_external_ip.as_deref(),
            );

            EmuleStatusResponse {
                feature_enabled: true,
                runtime_enabled: true,
                nodes_dat_path: Some(effective_path.display().to_string()),
                nodes_dat_present: present,
                contacts,
                connected_peers,
                is_connected: connected_peers >= 4,
                external_ip,
                external_ip_source,
                tcp_port: Some(state.config.emule.tcp_port),
                udp_port: Some(state.config.emule.udp_port),
                connectivity,
                connectivity_reason: Some(connectivity_reason),
                active_downloads: state.emule_active_downloads.read().await.len(),
                upload_slots_total,
                upload_slots_in_use,
                inbound_connections: inbound,
            }
        }
    };

    #[cfg(not(feature = "emule-compat"))]
    let resp = {
        let _ = state;
        EmuleStatusResponse {
            feature_enabled: false,
            runtime_enabled: false,
            nodes_dat_path: None,
            nodes_dat_present: false,
            contacts: 0,
            connected_peers: 0,
            is_connected: false,
            external_ip: None,
            external_ip_source: None,
            tcp_port: None,
            udp_port: None,
            connectivity: EmuleConnectivity::Unknown,
            connectivity_reason: None,
            active_downloads: 0,
            upload_slots_total: 0,
            upload_slots_in_use: 0,
            inbound_connections: 0,
        }
    };

    Json(resp)
}

/// Infer the eMule TCP port's connectivity class from the data the daemon has
/// at hand.  Strongest evidence first: an actual inbound connection proves the
/// port is open; UPnP success is a strong proxy; a manually configured external
/// IP is the user's promise; everything else is uncertain.
#[cfg(feature = "emule-compat")]
fn classify_connectivity(
    inbound: u64,
    upnp_enabled: bool,
    upnp_external_ip: Option<&str>,
    configured_external_ip: Option<&str>,
) -> (EmuleConnectivity, String) {
    if inbound > 0 {
        let s = if inbound == 1 { "" } else { "s" };
        return (
            EmuleConnectivity::Open,
            format!("{inbound} inbound connection{s} served"),
        );
    }
    if upnp_enabled {
        if upnp_external_ip.is_some() {
            return (EmuleConnectivity::Open, "UPnP mapped TCP port".to_string());
        }
        return (
            EmuleConnectivity::Firewalled,
            "UPnP enabled but no mapping established".to_string(),
        );
    }
    if configured_external_ip.is_some() {
        return (
            EmuleConnectivity::Open,
            "external IP configured by user".to_string(),
        );
    }
    (
        EmuleConnectivity::Unknown,
        "UPnP disabled and no external IP configured".to_string(),
    )
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
        if !state.config.emule.enabled {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }

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

        // Feed the seeds into the live Kad2 task so it starts bootstrapping
        // immediately without waiting for the next daemon restart.
        let seeds = crate::emule::load_kad_seeds(&state.config, 200);
        if !seeds.is_empty() {
            let seeded = state.kad_handle.bootstrap(seeds).await;
            tracing::info!(seeded, "Kad2 bootstrap triggered from API");
        }

        Ok(Json(EmuleBootstrapResponse {
            contacts,
            path: save_path.display().to_string(),
            url,
        }))
    }
}

// ── GET /api/v1/emule/search ──────────────────────────────────────────────────

#[derive(Deserialize, IntoParams)]
pub struct KadSearchQuery {
    /// Keyword to search for in the Kad network.
    q: String,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct KadSearchHit {
    pub hash: String,
    pub name: String,
    pub size: u64,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct KadSearchResponse {
    pub keyword: String,
    pub hits: Vec<KadSearchHit>,
}

/// Kad2 keyword search
///
/// Sends a `KADEMLIA2_SEARCH_KEY_REQ` into the Kad network and returns matching
/// file entries (name, hash, size).  Blocks until the search times out (~60 s).
#[utoipa::path(
    get,
    path = "/api/v1/emule/search",
    params(KadSearchQuery),
    responses(
        (status = 200, description = "Search results.", body = KadSearchResponse),
        (status = 400, description = "Empty keyword."),
        (status = 501, description = "emule-compat feature not compiled in.")
    )
)]
pub async fn get_kad_search(
    State(state): State<AppState>,
    Query(params): Query<KadSearchQuery>,
) -> Result<Json<KadSearchResponse>, StatusCode> {
    #[cfg(not(feature = "emule-compat"))]
    {
        let _ = (state, params.q);
        Err(StatusCode::NOT_IMPLEMENTED)
    }

    #[cfg(feature = "emule-compat")]
    {
        if !state.config.emule.enabled {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }

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
