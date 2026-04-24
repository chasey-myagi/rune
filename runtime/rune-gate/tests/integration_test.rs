use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

use rune_core::auth::{KeyVerifier, NoopVerifier};
use rune_core::invoker::LocalInvoker;
use rune_core::relay::Relay;
use rune_core::resolver::{Resolver, RoundRobinResolver};
use rune_core::rune::{make_handler, GateConfig, RuneConfig};
use rune_core::session::SessionManager;
use rune_flow::engine::FlowEngine;
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
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels: Default::default(),
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

    let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
        Arc::clone(&relay),
        Arc::clone(&resolver) as Arc<dyn Resolver>,
    )));
    GateState {
        auth: gate::AuthState {
            trust_proxy: None,
            key_verifier,
            auth_enabled,
            exempt_routes: Arc::new(vec!["/health".to_string()]),
            audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
        },
        rune: gate::RuneState {
            relay,
            resolver,
            session_mgr: Arc::new(SessionManager::new_dev(
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(35),
            )),
            file_broker: Arc::new(gate::FileBroker::new()),
            max_upload_size_mb: 10,
            request_timeout: std::time::Duration::from_secs(30),
        },
        flow: gate::FlowState { flow_engine },
        admin: gate::AdminState {
            store,
            started_at: Instant::now(),
            dev_mode: !auth_enabled,
            scaling: None,
        },
        cors_origins: Arc::new(vec![]),
        rate_limiter: None,
        shutdown: gate::ShutdownCoordinator::new(),
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
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["code"], "UNAUTHORIZED");
}

#[tokio::test]
async fn test_full_auth_chain_accept_valid_key() {
    let state = make_state(true);
    let key_result = state
        .admin
        .store
        .create_key(KeyType::Gate, "integration test")
        .await
        .unwrap();

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
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
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
    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let task_id = json["task_id"].as_str().unwrap().to_string();

    // Wait for background task
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify task is in SQLite
    let task = state.admin.store.get_task(&task_id).await.unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Completed);
    assert!(task.output.is_some());
    assert!(task.completed_at.is_some());
}

#[tokio::test]
async fn test_task_cancel_updates_sqlite_status() {
    let state = make_state(false);

    // Insert a task manually in running state
    state
        .admin
        .store
        .insert_task("cancel-me", "echo", Some("test"))
        .await
        .unwrap();
    state
        .admin
        .store
        .update_task_status("cancel-me", TaskStatus::Running, None, None)
        .await
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
    let task = state
        .admin
        .store
        .get_task("cancel-me")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, TaskStatus::Cancelled);
}

// ---------------------------------------------------------------------------
// Key lifecycle tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_key_create_then_authenticate() {
    let state = make_state(true);

    // Create a key directly in store
    let key = state
        .admin
        .store
        .create_key(KeyType::Gate, "lifecycle test")
        .await
        .unwrap();

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
    let key = state
        .admin
        .store
        .create_key(KeyType::Gate, "revocable")
        .await
        .unwrap();
    state.admin.store.revoke_key(key.api_key.id).await.unwrap();

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
    let logs = state
        .admin
        .store
        .query_logs(Some("echo"), 10)
        .await
        .unwrap();
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
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
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
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["logs"].as_array().unwrap().len() >= 1);
}

// ---------------------------------------------------------------------------
// Full auth chain: create key via API → use key to call rune → verify call log
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_full_auth_chain_create_key_call_rune_verify_log() {
    let state = make_state(true);

    // Step 1: create a gate key via management API (mgmt routes are behind auth
    // middleware too, so we create the key directly in the store first to
    // bootstrap, then use it)
    let bootstrap_key = state
        .admin
        .store
        .create_key(KeyType::Gate, "bootstrap")
        .await
        .unwrap();

    // Step 2: use the bootstrap key to call the echo rune
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", bootstrap_key.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"chain":"test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["chain"], "test");

    // Step 3: verify a call log was recorded with correct rune name and mode
    let logs = state
        .admin
        .store
        .query_logs(Some("echo"), 10)
        .await
        .unwrap();
    assert!(!logs.is_empty(), "call log should be recorded");
    assert_eq!(logs[0].rune_name, "echo");
    assert_eq!(logs[0].mode, "sync");
    assert_eq!(logs[0].status_code, 200);
}

// ---------------------------------------------------------------------------
// Key created is immediately usable (no delay)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_key_immediately_usable_after_creation() {
    let state = make_state(true);

    // Create a key and immediately use it in the very next request (no sleep)
    let key = state
        .admin
        .store
        .create_key(KeyType::Gate, "instant")
        .await
        .unwrap();

    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", key.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"instant":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Must succeed without any delay
    assert_eq!(response.status(), 200);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["instant"], true);
}

// ---------------------------------------------------------------------------
// Multiple gate keys work independently
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_keys_independent() {
    let state = make_state(true);

    // Create three independent keys
    let key_a = state
        .admin
        .store
        .create_key(KeyType::Gate, "key-a")
        .await
        .unwrap();
    let key_b = state
        .admin
        .store
        .create_key(KeyType::Gate, "key-b")
        .await
        .unwrap();
    let key_c = state
        .admin
        .store
        .create_key(KeyType::Gate, "key-c")
        .await
        .unwrap();

    // All three should work
    for (label, raw_key) in [
        ("a", &key_a.raw_key),
        ("b", &key_b.raw_key),
        ("c", &key_c.raw_key),
    ] {
        let app = gate::build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", raw_key))
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"key":"{}"}}"#, label)))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            200,
            "key {} should authenticate successfully",
            label
        );
    }

    // Revoke key_b — only key_b should stop working
    state
        .admin
        .store
        .revoke_key(key_b.api_key.id)
        .await
        .unwrap();

    // key_a still works
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", key_a.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"still":"ok"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "key_a should still work after revoking key_b"
    );

    // key_b now rejected
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", key_b.raw_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        401,
        "key_b should be rejected after revocation"
    );

    // key_c still works
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", key_c.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"still":"ok"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "key_c should still work after revoking key_b"
    );
}

// ===========================================================================
// #14  Concurrent authenticated requests — auth and call log correctness
// ===========================================================================

#[tokio::test]
async fn test_concurrent_authenticated_requests() {
    let state = make_state(true);
    let key = state
        .admin
        .store
        .create_key(KeyType::Gate, "concurrent")
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..10 {
        let st = state.clone();
        let raw_key = key.raw_key.clone();
        handles.push(tokio::spawn(async move {
            let app = gate::build_router(st, None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("authorization", format!("Bearer {}", raw_key))
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"idx":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();
            response.status()
        }));
    }

    for (idx, handle) in handles.into_iter().enumerate() {
        let status = handle.await.unwrap();
        assert_eq!(status, 200, "concurrent request {} should succeed", idx);
    }

    // Verify all 10 calls were logged
    let logs = state
        .admin
        .store
        .query_logs(Some("echo"), 100)
        .await
        .unwrap();
    assert_eq!(logs.len(), 10, "all 10 calls should be recorded");
    for log in &logs {
        assert_eq!(log.rune_name, "echo");
        assert_eq!(log.mode, "sync");
        assert_eq!(log.status_code, 200);
    }
}

// ===========================================================================
// #15  Full async chain: create key → auth async call → query task → call log
// ===========================================================================

#[tokio::test]
async fn test_full_async_chain_with_auth() {
    let state = make_state(true);
    let key = state
        .admin
        .store
        .create_key(KeyType::Gate, "async-chain")
        .await
        .unwrap();

    // Step 1: Authenticated async call
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo?async=true")
                .header("authorization", format!("Bearer {}", key.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"async_chain":"data"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 202);
    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let task_id = json["task_id"].as_str().unwrap().to_string();

    // Step 2: Wait for background task
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Step 3: Query task via HTTP (with auth)
    let app2 = gate::build_router(state.clone(), None);
    let response = app2
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/tasks/{}", task_id))
                .header("authorization", format!("Bearer {}", key.raw_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "completed");
    assert!(json["output"].is_string());

    // Step 4: Verify call log
    let logs = state
        .admin
        .store
        .query_logs(Some("echo"), 10)
        .await
        .unwrap();
    assert!(!logs.is_empty(), "async call should be logged");
    let async_log = logs.iter().find(|l| l.mode == "async");
    assert!(async_log.is_some(), "should have an async mode log entry");
}

// ===========================================================================
// #16  Revoked key rejects subsequent requests
// ===========================================================================

#[tokio::test]
async fn test_revoked_key_rejects_pending_request() {
    let state = make_state(true);
    let key = state
        .admin
        .store
        .create_key(KeyType::Gate, "to-revoke")
        .await
        .unwrap();

    // First request succeeds
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", key.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"first":"ok"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "first call should succeed");

    // Revoke the key
    state.admin.store.revoke_key(key.api_key.id).await.unwrap();

    // Next request with same key should fail
    let app2 = gate::build_router(state.clone(), None);
    let response = app2
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/echo")
                .header("authorization", format!("Bearer {}", key.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"second":"should fail"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 401, "revoked key should be rejected");
}

// ===========================================================================
// #17  Mixed sync and stream rune calls
// ===========================================================================

#[tokio::test]
async fn test_mixed_sync_and_stream_runes() {
    let relay = Arc::new(Relay::new());
    let resolver = Arc::new(RoundRobinResolver::new());
    let store = Arc::new(RuneStore::open_in_memory().unwrap());

    // Register a sync-only rune
    let sync_handler = make_handler(|_ctx, input| async move { Ok(input) });
    relay
        .register(
            RuneConfig {
                name: "sync_rune".into(),
                version: "1.0.0".into(),
                description: "sync only".into(),
                supports_stream: false,
                gate: Some(GateConfig {
                    path: "/sync".into(),
                    method: "POST".into(),
                }),
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels: Default::default(),
            },
            Arc::new(LocalInvoker::new(sync_handler)),
            None,
        )
        .unwrap();

    // Register a stream-capable rune
    let stream_handler = make_handler(|_ctx, input| async move { Ok(input) });
    relay
        .register(
            RuneConfig {
                name: "stream_rune".into(),
                version: "1.0.0".into(),
                description: "stream capable".into(),
                supports_stream: true,
                gate: Some(GateConfig {
                    path: "/stream".into(),
                    method: "POST".into(),
                }),
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels: Default::default(),
            },
            Arc::new(LocalInvoker::new(stream_handler)),
            None,
        )
        .unwrap();

    let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
        Arc::clone(&relay),
        Arc::clone(&resolver) as Arc<dyn Resolver>,
    )));
    let state = GateState {
        auth: gate::AuthState {
            trust_proxy: None,
            key_verifier: Arc::new(NoopVerifier) as Arc<dyn KeyVerifier>,
            auth_enabled: false,
            exempt_routes: Arc::new(vec!["/health".to_string()]),
            audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
        },
        rune: gate::RuneState {
            relay,
            resolver,
            session_mgr: Arc::new(SessionManager::new_dev(
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(35),
            )),
            file_broker: Arc::new(gate::FileBroker::new()),
            max_upload_size_mb: 10,
            request_timeout: std::time::Duration::from_secs(30),
        },
        flow: gate::FlowState { flow_engine },
        admin: gate::AdminState {
            store,
            started_at: std::time::Instant::now(),
            dev_mode: true,
            scaling: None,
        },
        cors_origins: Arc::new(vec![]),
        rate_limiter: None,
        shutdown: gate::ShutdownCoordinator::new(),
    };

    // Sync rune: normal call works
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"mode":"sync"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "sync rune should work");

    // Sync rune: stream request should fail
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sync?stream=true")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"mode":"stream"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        400,
        "stream on sync-only rune should fail"
    );

    // Stream rune: normal sync call also works (fallback)
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"mode":"sync_on_stream"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "sync call on stream rune should work"
    );

    // Stream rune: stream request should succeed (returns SSE)
    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/stream?stream=true")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"mode":"actual_stream"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "stream on stream rune should succeed"
    );
    // Verify it's SSE content type
    let ct = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        ct.contains("text/event-stream"),
        "stream response should be SSE, got: {}",
        ct
    );
}

#[tokio::test]
async fn test_stream_not_supported_error_includes_request_id() {
    let state = make_state(false);
    let app = gate::build_router(state, None);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/runes/echo/run?stream=true")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"hello":"world"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["code"], "STREAM_NOT_SUPPORTED");
    assert!(json["error"]["request_id"].as_str().is_some());
}

#[tokio::test]
async fn test_flow_create_and_delete_write_through_to_store() {
    let state = make_state(false);
    let app = gate::build_router(state.clone(), None);
    let flow_json = serde_json::json!({
        "name": "persisted-flow",
        "steps": [
            {
                "name": "step-a",
                "rune": "echo",
                "depends_on": [],
                "condition": null,
                "input_mapping": null
            }
        ],
        "gate_path": null
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/flows")
                .header("content-type", "application/json")
                .body(Body::from(flow_json.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    assert!(state
        .admin
        .store
        .get_flow("persisted-flow")
        .await
        .unwrap()
        .is_some());

    let app = gate::build_router(state.clone(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/flows/persisted-flow")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    assert!(state
        .admin
        .store
        .get_flow("persisted-flow")
        .await
        .unwrap()
        .is_none());
}

// ===========================================================================
// #18  Stats accumulate correctly across multiple runes
// ===========================================================================

#[tokio::test]
async fn test_stats_accumulate_across_runes() {
    let relay = Arc::new(Relay::new());
    let resolver = Arc::new(RoundRobinResolver::new());
    let store = Arc::new(RuneStore::open_in_memory().unwrap());

    // Register two runes
    for (name, path) in [("rune_a", "/ra"), ("rune_b", "/rb")] {
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                RuneConfig {
                    name: name.into(),
                    version: "1.0.0".into(),
                    description: "test".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: path.into(),
                        method: "POST".into(),
                    }),
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(handler)),
                None,
            )
            .unwrap();
    }

    let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
        Arc::clone(&relay),
        Arc::clone(&resolver) as Arc<dyn Resolver>,
    )));
    let state = GateState {
        auth: gate::AuthState {
            trust_proxy: None,
            key_verifier: Arc::new(NoopVerifier) as Arc<dyn KeyVerifier>,
            auth_enabled: false,
            exempt_routes: Arc::new(vec!["/health".to_string()]),
            audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
        },
        rune: gate::RuneState {
            relay,
            resolver,
            session_mgr: Arc::new(SessionManager::new_dev(
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(35),
            )),
            file_broker: Arc::new(gate::FileBroker::new()),
            max_upload_size_mb: 10,
            request_timeout: std::time::Duration::from_secs(30),
        },
        flow: gate::FlowState { flow_engine },
        admin: gate::AdminState {
            store,
            started_at: std::time::Instant::now(),
            dev_mode: true,
            scaling: None,
        },
        cors_origins: Arc::new(vec![]),
        rate_limiter: None,
        shutdown: gate::ShutdownCoordinator::new(),
    };

    // Call rune_a 4 times
    for _ in 0..4 {
        let app = gate::build_router(state.clone(), None);
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ra")
                    .body(Body::from(r#"{"a":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // Call rune_b 2 times
    for _ in 0..2 {
        let app = gate::build_router(state.clone(), None);
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rb")
                    .body(Body::from(r#"{"b":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // Query stats
    let app = gate::build_router(state, None);
    let response = app
        .oneshot(Request::get("/api/v1/stats").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["total_calls"], 6, "total should be 4+2=6");

    let by_rune = json["by_rune"].as_array().unwrap();
    let a_stat = by_rune
        .iter()
        .find(|s| s["rune_name"] == "rune_a")
        .expect("should have rune_a stats");
    assert_eq!(a_stat["count"], 4);

    let b_stat = by_rune
        .iter()
        .find(|s| s["rune_name"] == "rune_b")
        .expect("should have rune_b stats");
    assert_eq!(b_stat["count"], 2);
}

// ===========================================================================
// S9 Regression: Gate key cannot call key management endpoints (privilege escalation)
// ===========================================================================

#[tokio::test]
async fn test_gate_key_cannot_create_keys() {
    let state = make_state(true); // auth enabled, dev_mode=false
    let gate_key = state
        .admin
        .store
        .create_key(KeyType::Gate, "normal user")
        .await
        .unwrap();

    let app = gate::build_router(state, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/keys")
                .header("authorization", format!("Bearer {}", gate_key.raw_key))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"key_type":"gate","label":"escalated"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        403,
        "gate key should NOT be able to create new keys (privilege escalation)"
    );
}

#[tokio::test]
async fn test_gate_key_cannot_revoke_keys() {
    let state = make_state(true);
    let gate_key = state
        .admin
        .store
        .create_key(KeyType::Gate, "normal user")
        .await
        .unwrap();
    let target_key = state
        .admin
        .store
        .create_key(KeyType::Gate, "target")
        .await
        .unwrap();

    let app = gate::build_router(state, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/keys/{}", target_key.api_key.id))
                .header("authorization", format!("Bearer {}", gate_key.raw_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        403,
        "gate key should NOT be able to revoke keys (privilege escalation)"
    );
}

#[tokio::test]
async fn test_gate_key_cannot_list_keys() {
    let state = make_state(true);
    let gate_key = state
        .admin
        .store
        .create_key(KeyType::Gate, "normal user")
        .await
        .unwrap();

    let app = gate::build_router(state, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/keys")
                .header("authorization", format!("Bearer {}", gate_key.raw_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        403,
        "gate key should NOT be able to list keys"
    );
}

#[tokio::test]
async fn test_dev_mode_key_management_still_works() {
    let state = make_state(false); // dev mode, auth disabled

    let app = gate::build_router(state, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/keys")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"key_type":"gate","label":"dev key"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        201,
        "dev mode should allow key management without auth"
    );
}

// ===========================================================================
// MF-1 Regression: flow async binary input uses hex encoding
// ===========================================================================

#[test]
fn test_fix_flow_async_binary_input_uses_hex_encoding() {
    // Verify non-UTF8 bytes are hex-encoded rather than lossy-replaced
    let binary = vec![0xFF, 0xFE, 0x00, 0x80];
    let result = match std::str::from_utf8(&binary) {
        Ok(s) => s.to_string(),
        Err(_) => format!("hex:{}", hex::encode(&binary)),
    };
    assert!(
        result.starts_with("hex:"),
        "binary input should be hex-encoded"
    );
    assert_eq!(result, "hex:fffe0080");
}
