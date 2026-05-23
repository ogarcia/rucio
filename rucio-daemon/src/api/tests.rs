//! Integration tests for the search API endpoints.
//!
//! These tests build a real axum router with an in-memory SQLite DB and a
//! dummy node_cmd channel, then drive it with HTTP requests using
//! `tower::ServiceExt`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::{RwLock, mpsc};
use tower::ServiceExt;

use rucio_core::api::search::{SearchResultsResponse, SearchStartedResponse};

use crate::api::{AppState, NodeStatus, SearchStore, router};
use crate::config::Config;
use crate::node::messages::NodeCmd;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

async fn test_state() -> (AppState, mpsc::Receiver<NodeCmd>) {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;

    let opts = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .foreign_keys(true);
    let db = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .unwrap();
    crate::db::apply_schema(&db).await.unwrap();

    let (cmd_tx, cmd_rx) = mpsc::channel::<NodeCmd>(16);

    let node_status = Arc::new(RwLock::new(NodeStatus {
        peer_id: "QmTestPeer".to_string(),
        ..Default::default()
    }));
    let search_store: SearchStore = Arc::new(RwLock::new(HashMap::new()));

    let state = AppState {
        db,
        config: Arc::new(Config::default()),
        node_cmd: cmd_tx,
        started_at: Instant::now(),
        node_status,
        search_store,
    };
    (state, cmd_rx)
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
    let (state, _rx) = test_state().await;
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
    let (state, _rx) = test_state().await;
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
    let (state, _rx) = test_state().await;
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
    let (state, mut rx) = test_state().await;
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

    let (state, _rx) = test_state().await;

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
