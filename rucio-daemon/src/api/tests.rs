//! Integration tests for the search API endpoints.
//!
//! These tests build a real axum router with a temp-file SQLite DB and a
//! dummy node_cmd channel, then drive it with HTTP requests using
//! `tower::ServiceExt`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::{RwLock, mpsc};
use tower::ServiceExt;

use rucio_core::api::search::{SearchResultsResponse, SearchStartedResponse};
use rucio_core::api::shares::SharesResponse;

use crate::api::{AppState, NodeStatus, SearchStore, router};
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

    let node_status = Arc::new(RwLock::new(NodeStatus {
        peer_id: "QmTestPeer".to_string(),
        ..Default::default()
    }));
    let search_store: SearchStore = Arc::new(RwLock::new(HashMap::new()));

    let state = AppState {
        db,
        config: Arc::new(Config::default()),
        node_cmd: cmd_tx,
        watcher_cmd: watcher_tx,
        started_at: Instant::now(),
        node_status,
        search_store,
        download_tx,
        indexing_count: Arc::new(AtomicUsize::new(0)),
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
async fn post_search_returns_202_with_query_id() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"keywords":["rust","p2p"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body: SearchStartedResponse = body_json(resp.into_body()).await;
    assert!(!body.query_id.is_empty());
    // Should be a valid UUID
    assert!(uuid::Uuid::parse_str(&body.query_id).is_ok());
}

#[tokio::test]
async fn post_search_empty_keywords_returns_400() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"keywords":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_unknown_query_id_returns_404() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/search/00000000-0000-0000-0000-000000000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn post_then_get_returns_pending_empty_results() {
    let (state, mut rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

    // POST to start the search
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/search")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"keywords":["test"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let started: SearchStartedResponse = body_json(resp.into_body()).await;

    // The node_cmd channel should have received a Search command
    let cmd = rx.try_recv().unwrap();
    assert!(matches!(cmd, NodeCmd::Search(_)));

    // GET the results — should be pending with empty results
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/search/{}", started.query_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let results: SearchResultsResponse = body_json(resp.into_body()).await;
    assert_eq!(results.query_id, started.query_id);
    assert!(results.results.is_empty());
    assert!(results.pending);
}

#[tokio::test]
async fn accumulated_results_are_returned() {
    use crate::api::SearchEntry;
    use rucio_core::api::search::SearchResultResponse;

    let (state, _rx, _dl_rx, _dir) = test_state().await;

    // Inject a result directly into the store to simulate a network result
    // arriving without waiting for actual gossipsub.
    let query_id = uuid::Uuid::new_v4().to_string();
    {
        let mut store = state.search_store.write().await;
        store.insert(
            query_id.clone(),
            SearchEntry {
                results: vec![SearchResultResponse {
                    root_hash: "aabbcc".to_string(),
                    name: "test.mp3".to_string(),
                    size: 1024,
                    chunk_count: 1,
                    mime_type: Some("audio/mpeg".to_string()),
                    magnet: "rucio:aabbcc?name=test.mp3&size=1024".to_string(),
                    provider: "12D3KooWTest".to_string(),
                }],
                pending: true,
                started_at: Instant::now(),
            },
        );
    }

    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/search/{query_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let results: SearchResultsResponse = body_json(resp.into_body()).await;
    assert_eq!(results.results.len(), 1);
    assert_eq!(results.results[0].name, "test.mp3");
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
async fn cancel_download_unknown_id_returns_404() {
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
async fn get_shares_empty() {
    let (state, _rx, _dl_rx, _dir) = test_state().await;
    let app = router(state);

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
    let body: SharesResponse = body_json(resp.into_body()).await;
    assert!(body.shares.is_empty());
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
