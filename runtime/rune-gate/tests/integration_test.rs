use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

use rune_core::auth::{KeyVerifier, NoopVerifier};
use rune_core::invoker::LocalInvoker;
use rune_core::relay::Relay;
use rune_core::resolver::RoundRobinResolver;
use rune_core::rune::{GateConfig, RuneConfig, make_handler};
use rune_core::session::SessionManager;
use rune_gate::gate::{self, GateState};
use rune_store::{KeyType, RuneStore, StoreKeyVerifier, TaskStatus};

fn make_state(auth_enabled: bool) -> GateState {
    let relay = Arc::new(Relay::new());
    let resolver = Arc::new(RoundRobinResolver::new());
    let store = Arc::new(RuneStore::open_in_memory().unwrap());

    // Register echo rune
    let echo_handler = make_handler(|_ctx, input| async move { Ok(input) });
    relay
        .register(
            RuneConfig {
                name: "echo".into(),
                version: "1.0.0".into(),
                description: "test echo".into(),
                supports_stream: false,
                gate: Some(GateConfig {
                    path: "/echo".into(),
                    method: "POST".into(),
                }),
            },
            Arc::new(LocalInvoker::new(echo_handler)),
            None,
        )
        .unwrap();

    let key_verifier: Arc<dyn KeyVerifier> = if auth_enabled {
        Arc::new(StoreKeyVerifier::new(store.clone()))
    } else {
        Arc::new(NoopVerifier)
    };

    GateState {
        relay,
        resolver,
        store,
        key_verifier,
        session_mgr: Arc::new(SessionManager::new(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(35),
        )),
        auth_enabled,
        exempt_routes: vec!["/health".to_string()],
        cors_origins: vec![],
        dev_mode: !auth_enabled,
        started_at: Instant::now(),
    }
}

// ---------------------------------------------------------------------------
// Auth chain tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_full_auth_chain_reject_no_key() {
    let state = make_state(true);
    let app = gate::build_router(state, None);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
    let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["code"], "UNAUTHORIZED");
}

#[tokio::test]
async fn test_full_auth_chain_accept_valid_key() {
    let state = make_state(true);
    let key_result = state.store.create_key(KeyType::Gate, "integration test").unwrap();

    let app = gate::build_router(state, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", key_result.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"hello":"world"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["hello"], "world");
}

#[tokio::test]
async fn test_auth_reject_invalid_key() {
    let state = make_state(true);
    let app = gate::build_router(state, None);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", "Bearer rk_invalid_key_12345678")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn test_dev_mode_skips_auth() {
    let state = make_state(false); // dev mode, auth disabled
    let app = gate::build_router(state, None);

    // No auth header, should still work
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"dev":"mode"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_exempt_route_skips_auth() {
    let state = make_state(true); // auth enabled
    let app = gate::build_router(state, None);

    // /health is exempt
    let response = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

// ---------------------------------------------------------------------------
// Async task persistence tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_async_task_persisted_to_sqlite() {
    let state = make_state(false);
    let app = gate::build_router(state.clone(), None);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo?async=true")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"data":"persist me"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 202);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let task_id = json["task_id"].as_str().unwrap().to_string();

    // Wait for background task
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify task is in SQLite
    let task = state.store.get_task(&task_id).unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Completed);
    assert!(task.output.is_some());
    assert!(task.completed_at.is_some());
}

#[tokio::test]
async fn test_task_cancel_updates_sqlite_status() {
    let state = make_state(false);

    // Insert a task manually in running state
    state.store.insert_task("cancel-me", "echo", Some("test")).unwrap();
    state
        .store
        .update_task_status("cancel-me", TaskStatus::Running, None, None)
        .unwrap();

    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/tasks/cancel-me")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    // Verify SQLite reflects cancelled status
    let task = state.store.get_task("cancel-me").unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Cancelled);
}

// ---------------------------------------------------------------------------
// Key lifecycle tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_key_create_then_authenticate() {
    let state = make_state(true);

    // Create a key directly in store
    let key = state.store.create_key(KeyType::Gate, "lifecycle test").unwrap();

    // Use the key to authenticate
    let app = gate::build_router(state, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", key.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"authed":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_revoked_key_fails_auth() {
    let state = make_state(true);

    // Create and then revoke a key
    let key = state.store.create_key(KeyType::Gate, "revocable").unwrap();
    state.store.revoke_key(key.api_key.id).unwrap();

    let app = gate::build_router(state, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", key.raw_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

// ---------------------------------------------------------------------------
// Call log tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sync_call_recorded_in_logs() {
    let state = make_state(false);
    let app = gate::build_router(state.clone(), None);

    let _response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"log":"me"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Check call log was recorded
    let logs = state.store.query_logs(Some("echo"), 10).unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].rune_name, "echo");
    assert_eq!(logs[0].mode, "sync");
    assert_eq!(logs[0].status_code, 200);
}

// ---------------------------------------------------------------------------
// Management API cross-module tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mgmt_stats_reflects_calls() {
    let state = make_state(false);

    // Make a few calls first
    for i in 0..3 {
        let app = gate::build_router(state.clone(), None);
        let _response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"call":{}}}"#, i)))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // Check stats
    let app = gate::build_router(state, None);
    let response = app
        .oneshot(Request::get("/api/v1/stats").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["total_calls"], 3);
}

#[tokio::test]
async fn test_mgmt_logs_endpoint() {
    let state = make_state(false);

    // Make a call
    let app = gate::build_router(state.clone(), None);
    let _response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"check":"logs"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Query logs via API
    let app = gate::build_router(state, None);
    let response = app
        .oneshot(
            Request::get("/api/v1/logs?rune=echo&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["logs"].as_array().unwrap().len() >= 1);
}
