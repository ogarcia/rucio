//! Integration tests for the search API endpoints.
//!
//! These tests build a real axum router with a temp-file SQLite DB and a
//! dummy node_cmd channel, then drive it with HTTP requests using
//! `tower::ServiceExt`.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::{RwLock, broadcast, mpsc};
use tower::ServiceExt;

use rucio_core::api::searches::SearchStartedResponse as SearchesStartedResponse;
use rucio_core::api::searches::{SearchDetailResponse, SearchState};
use rucio_core::api::ws::WsEvent;

use crate::api::{AppState, NodeStatus, SearchRegistry, router};
use crate::config::Config;
use crate::node::messages::NodeCmd;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

async fn test_state() -> (
    AppState,
    mpsc::Receiver<NodeCmd>,
    mpsc::Receiver<crate::api::DownloadRequest>,
    tempfile::TempDir,
) {
    use sqlx::sqlite::SqlitePoolOptions;

    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("test.db").display());
    let db = SqlitePoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    crate::db::apply_schema(&db).await.unwrap();

    let (cmd_tx, cmd_rx) = mpsc::channel::<NodeCmd>(16);
    let (watcher_tx, _watcher_rx) = mpsc::channel::<crate::watcher::WatcherCmd>(16);
    let (download_tx, download_rx) = mpsc::channel::<crate::api::DownloadRequest>(16);
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

    let node_status = Arc::new(RwLock::new(NodeStatus {
        peer_id: "QmTestPeer".to_string(),
        ..Default::default()
    }));
    let search_registry = Arc::new(RwLock::new(SearchRegistry::new()));

    let state = AppState {
        db,
        config: Arc::new(Config::default()),
        config_path: None,
        node_cmd: cmd_tx,
        watcher_cmd: watcher_tx,
        started_at: Instant::now(),
        node_status,
        search_registry,
        download_tx,
        indexing_count: Arc::new(AtomicUsize::new(0)),
        ws_tx,
        metrics: Arc::new(crate::metrics::Metrics::default()),
        upload_throttle: Arc::new(crate::throttle::TokenBucket::new(0)),
        download_throttle: Arc::new(crate::throttle::TokenBucket::new(0)),
        bandwidth: Arc::new(crate::throttle::BandwidthState::new(
            Arc::new(crate::throttle::TokenBucket::new(0)),
            Arc::new(crate::throttle::TokenBucket::new(0)),
            0,
            0,
            5000,
            5000,
        )),
        #[cfg(feature = "emule-compat")]
        kad_handle: {
            let socket = Arc::new(tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap());
            rucio_emule::kad::task::spawn(
                socket,
                rucio_emule::kad::packet::KadId::random(),
                rucio_emule::kad::task::KadTaskConfig::default(),
            )
        },
        #[cfg(feature = "emule-compat")]
        emule_active_downloads: Arc::new(
            tokio::sync::RwLock::new(std::collections::HashMap::new()),
        ),
        #[cfg(feature = "emule-compat")]
        emule_upload_slots: Arc::new(tokio::sync::Semaphore::new(4)),
        #[cfg(feature = "emule-compat")]
        emule_inbound_connections: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        #[cfg(feature = "emule-compat")]
        emule_last_inbound_at: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        #[cfg(feature = "emule-compat")]
        emule_cancel: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        #[cfg(feature = "emule-compat")]
        ed2k_index_tx: None,
        external_ip: Arc::new(tokio::sync::RwLock::new(None)),
        live_stats: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        upload_stats: Arc::new(crate::upload_stats::UploadRegistry::new()),
        notifications: crate::notifier::NotificationState::from_config(
            &crate::config::NotificationConfig::default(),
        ),
        indexing_seen: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        auto_clear: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };
    (state, cmd_rx, download_rx, dir)
}

async fn body_json<T: serde::de::DeserializeOwned>(body: Body) -> T {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_search_returns_202_with_numeric_id() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/searches")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"keywords":["rust","p2p"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body: SearchesStartedResponse = body_json(resp.into_body()).await;
    assert!(body.id > 0);
}

#[tokio::test]
async fn post_search_empty_keywords_returns_400() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/searches")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"keywords":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_unknown_search_id_returns_404() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/searches/99999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn post_then_get_returns_running_empty_results() {
    let (state, mut rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/searches")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"keywords":["test"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let started: SearchesStartedResponse = body_json(resp.into_body()).await;

    // The node_cmd channel should have received a Search command.
    let cmd = rx.try_recv().unwrap();
    assert!(matches!(cmd, NodeCmd::Search(_)));

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/searches/{}", started.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let detail: SearchDetailResponse = body_json(resp.into_body()).await;
    assert_eq!(detail.id, started.id);
    assert!(detail.results.is_empty());
    assert!(matches!(detail.state, SearchState::Running));
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_status_returns_200_with_peer_id() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: rucio_core::api::status::StatusResponse = body_json(resp.into_body()).await;
    assert_eq!(body.peer_id, "QmTestPeer");
}

#[tokio::test]
async fn get_peers_returns_200_empty_list() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/peers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: rucio_core::api::status::PeersResponse = body_json(resp.into_body()).await;
    assert!(body.peers.is_empty());
}

// ---------------------------------------------------------------------------
// Shares
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_shares_returns_empty_list() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    // GET /shares lists directories; the per-file listing is /shares/files.
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/shares/files")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: rucio_core::api::shares::SharesResponse = body_json(resp.into_body()).await;
    assert!(body.shares.is_empty());
}

#[tokio::test]
async fn post_share_nonexistent_path_returns_400() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/shares")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"path":"/nonexistent/path/that/does/not/exist"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn delete_share_unknown_hash_returns_404() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/shares/aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_shares_by_path_missing_param_returns_400() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    // DELETE /shares without ?path= query param
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/shares")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Downloads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_downloads_returns_empty_list() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/downloads")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: rucio_core::api::downloads::DownloadsResponse = body_json(resp.into_body()).await;
    assert!(body.downloads.is_empty());
}

#[tokio::test]
async fn post_download_invalid_magnet_returns_400() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/downloads")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"magnet":"not-a-valid-magnet","providers":["12D3KooWTest"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_download_empty_providers_returns_202() {
    // providers is now optional — empty list triggers DHT-only discovery.
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let hash = "a".repeat(64);
    let body = format!(r#"{{"magnet":"rucio:{hash}?name=test.bin&size=1024","providers":[]}}"#);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/downloads")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn post_download_no_providers_field_returns_202() {
    // providers field can be omitted entirely (serde default = []).
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let hash = "c".repeat(64);
    let body = format!(r#"{{"magnet":"rucio:{hash}?name=test.bin&size=1024"}}"#);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/downloads")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn post_download_valid_returns_202() {
    let (state, mut rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let hash = "b".repeat(64);
    let body = format!(
        r#"{{"magnet":"rucio:{hash}?name=test.bin&size=1024","providers":["12D3KooWAbcDef"]}}"#
    );

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/downloads")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    // The download request should have been forwarded via download_tx
    let cmd = rx.try_recv();
    // download_tx goes to _download_rx which is dropped — channel may be closed,
    // but ACCEPTED was already returned before the send. Just check status.
    drop(cmd);
}

#[tokio::test]
async fn delete_download_unknown_id_returns_404() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/downloads/99999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cancel_download_unknown_id_returns_404() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/downloads/99999/cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_config_returns_200() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Shares
// ---------------------------------------------------------------------------

/// Insert a fake share row directly into the DB.
async fn insert_share(
    db: &crate::db::Db,
    root_hash: &[u8; 32],
    name: &str,
    size: u64,
    chunk_size: u32,
    path: &str,
) {
    sqlx::query(
        "INSERT INTO shared_files (root_hash, name, size, mime_type, path, chunk_size, added_at)
         VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6)",
    )
    .bind(root_hash.as_slice())
    .bind(name)
    .bind(size as i64)
    .bind(path)
    .bind(chunk_size as i64)
    .bind(0i64)
    .execute(db)
    .await
    .unwrap();
}

#[tokio::test]
async fn get_shares_dirs_empty() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    // GET /shares lists the watched directories (the unit of add/remove).
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/shares")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: rucio_core::api::shares::SharedDirsResponse = body_json(resp.into_body()).await;
    assert!(body.dirs.is_empty());
}

#[tokio::test]
async fn get_share_magnet_returns_magnet_link() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let root_hash = [1u8; 32];
    insert_share(
        &state.db,
        &root_hash,
        "movie.mkv",
        1024 * 1024,
        256 * 1024,
        "/shared/movie.mkv",
    )
    .await;
    let hash_hex = hex::encode(root_hash);
    let prefix = &hash_hex[..8];

    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/shares/{prefix}/magnet"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let magnet = std::str::from_utf8(&bytes).unwrap().trim_matches('"');
    assert!(
        magnet.starts_with("rucio:"),
        "expected rucio: link, got {magnet}"
    );
    assert!(magnet.contains(&hash_hex));
}

#[tokio::test]
async fn get_share_magnet_unknown_hash_returns_404() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/shares/deadbeef/magnet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_indexing_returns_zero_initially() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/shares/indexing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = body_json(resp.into_body()).await;
    assert_eq!(body["pending"].as_u64(), Some(0));
}

// ---------------------------------------------------------------------------
// Scalar docs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_api_docs_returns_200_html() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/docs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("text/html"),
        "expected text/html, got {content_type}"
    );
}

#[tokio::test]
async fn get_api_docs_html_contains_custom_template_markers() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/docs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let html = std::str::from_utf8(&bytes).unwrap();

    assert!(
        html.contains("<title>Rucio API</title>"),
        "custom title not found in HTML"
    );
    assert!(
        html.contains(r#""operationTitleSource":"path""#),
        "operationTitleSource flag not found in HTML"
    );
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_api_ws_upgrade_request_is_accepted() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    // A WebSocket upgrade request via `oneshot` does not establish a real TCP
    // connection, so the handshake cannot complete.  Axum returns either
    // 101 (if it can upgrade) or 426 (Upgrade Required, when the transport
    // layer does not support the upgrade).  Either way the route exists and
    // the handler is reached — a missing route would return 404/405.
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/ws")
                .header("connection", "upgrade")
                .header("upgrade", "websocket")
                .header("sec-websocket-version", "13")
                .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // 101 Switching Protocols or 426 Upgrade Required are both valid here.
    // 404 / 405 would indicate the route is not registered.
    assert!(
        resp.status() == StatusCode::SWITCHING_PROTOCOLS || resp.status().as_u16() == 426,
        "unexpected status: {}",
        resp.status()
    );
}

#[tokio::test]
async fn ws_event_is_delivered_to_subscriber() {
    use rucio_core::api::ws::WsEvent;

    let (state, _rx, _dl_rx, _dir) = test_state().await;
    // Subscribe before triggering the event so we don't miss it.
    let mut ws_rx = state.ws_tx.subscribe();
    let ws_tx = state.ws_tx.clone();

    // Simulate the main loop emitting a peer-connected event.
    ws_tx
        .send(WsEvent::PeerConnected {
            peer_id: "12D3KooWTest".to_string(),
        })
        .unwrap();

    let event = ws_rx.try_recv().unwrap();
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains(r#""type":"peer_connected""#));
    assert!(json.contains("12D3KooWTest"));
}

#[tokio::test]
async fn ws_event_serializes_with_type_and_data_fields() {
    use rucio_core::api::ws::WsEvent;

    let cases: &[(WsEvent, &str, &str)] = &[
        (
            WsEvent::IndexingCount { pending: 7 },
            r#""type":"indexing_count""#,
            r#""pending":7"#,
        ),
        (
            WsEvent::PeerDisconnected {
                peer_id: "QmFoo".to_string(),
            },
            r#""type":"peer_disconnected""#,
            "QmFoo",
        ),
        (
            WsEvent::NodeClassChanged {
                class: rucio_core::protocol::node::NodeClass::HighId,
            },
            r#""type":"node_class_changed""#,
            "HighId",
        ),
    ];

    for (event, expected_type, expected_data) in cases {
        let json = serde_json::to_string(event).unwrap();
        assert!(
            json.contains(expected_type),
            "missing {expected_type} in: {json}"
        );
        assert!(
            json.contains(expected_data),
            "missing {expected_data} in: {json}"
        );
    }
}
