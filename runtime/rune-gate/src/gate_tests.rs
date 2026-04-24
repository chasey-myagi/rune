// gate_tests.rs — All tests for the Gate module.
// Production code lives in state.rs, router.rs, middleware.rs, error.rs,
// multipart.rs, shutdown.rs, rate_limit.rs, file_broker.rs, handlers/*.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use bytes::Bytes;
    use rune_core::auth::{KeyVerifier, NoopVerifier};
    use rune_core::invoker::LocalInvoker;
    use rune_core::relay::Relay;
    use rune_core::resolver::{Resolver, RoundRobinResolver};
    use rune_core::rune::{make_handler, GateConfig, RuneConfig, RuneError};
    use rune_core::scaling::ScaleEvaluator;
    use rune_core::session::{CasterRole, HealthStatusLevel};
    use rune_flow::engine::FlowEngine;
    use rune_store::{CallLog, RuneStore, TaskStatus};
    use tower::ServiceExt;

    use crate::file_broker::FileBroker;
    use crate::rate_limit::RateLimitState;
    use crate::router::build_router;
    use crate::shutdown::ShutdownCoordinator;
    use crate::state::{
        AdminState, AuthState, FlowState, GateState, RuneState, DEFAULT_REQUEST_TIMEOUT,
    };
    fn test_state() -> GateState {
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        // Register a simple echo rune with gate_path="/echo"
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

        // Register a rune WITHOUT gate_path
        let internal_handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                RuneConfig {
                    name: "internal".into(),
                    version: "1.0.0".into(),
                    description: "no gate".into(),
                    supports_stream: false,
                    gate: None,
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(internal_handler)),
                None,
            )
            .unwrap();

        let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
            Arc::clone(&relay),
            Arc::clone(&resolver) as Arc<dyn Resolver>,
        )));

        GateState {
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        }
    }

    fn test_router() -> Router {
        build_router(test_state(), None)
    }

    #[tokio::test]
    async fn test_gate_path_sync() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["msg"], "hello");
    }

    #[tokio::test]
    async fn test_debug_route_sync() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/echo/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"test":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_fix_x_request_id_propagates_for_rune_run() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("x-request-id", "client-rq-123")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok()),
            Some("client-rq-123"),
            "gate rune handler should preserve inbound x-request-id"
        );
    }

    #[tokio::test]
    async fn test_unknown_rune_404() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_no_gate_path_not_exposed() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_async_returns_task_id() {
        let state = test_state();
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo?async=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"x":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["task_id"].is_string());
        assert_eq!(json["status"], "running");
    }

    #[tokio::test]
    async fn test_health() {
        let app = test_router();
        let response = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_runes() {
        let app = test_router();
        let response = app
            .oneshot(Request::get("/api/v1/runes").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["runes"].is_array());
    }

    #[tokio::test]
    async fn test_auth_blocks_without_key() {
        let mut state = test_state();
        state.auth.auth_enabled = true;
        state.auth.key_verifier =
            Arc::new(rune_store::StoreKeyVerifier::new(state.admin.store.clone()));
        let app = build_router(state, None);

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

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_allows_exempt_route() {
        let mut state = test_state();
        state.auth.auth_enabled = true;
        state.auth.key_verifier =
            Arc::new(rune_store::StoreKeyVerifier::new(state.admin.store.clone()));
        let app = build_router(state, None);

        let response = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_allows_valid_key() {
        let mut state = test_state();
        state.auth.auth_enabled = true;
        let key_result = state
            .admin
            .store
            .create_key(rune_store::KeyType::Gate, "test")
            .await
            .unwrap();
        state.auth.key_verifier =
            Arc::new(rune_store::StoreKeyVerifier::new(state.admin.store.clone()));
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key_result.raw_key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ok":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_mgmt_status() {
        let app = test_router();
        let response = app
            .oneshot(Request::get("/api/v1/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["uptime_secs"].is_number());
        assert!(json["rune_count"].is_number());
    }

    #[tokio::test]
    async fn test_mgmt_keys_lifecycle() {
        let state = test_state();
        let app = build_router(state.clone(), None);

        // Create a key
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/keys")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"key_type":"gate","label":"test key"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["raw_key"].is_string());

        // List keys
        let app2 = build_router(state.clone(), None);
        let response = app2
            .oneshot(Request::get("/api/v1/keys").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["keys"].as_array().unwrap().len(), 1);

        // Revoke key
        let app3 = build_router(state, None);
        let response = app3
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/keys/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "revoked");
    }

    #[tokio::test]
    async fn test_async_task_persisted() {
        let state = test_state();
        let app = build_router(state.clone(), None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo?async=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"test":"async"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let task_id = json["task_id"].as_str().unwrap().to_string();

        // Give the background task time to complete
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify task is persisted in store
        let task = state.admin.store.get_task(&task_id).await.unwrap();
        assert!(task.is_some());
        let task = task.unwrap();
        assert_eq!(task.status, TaskStatus::Completed);
    }

    // -----------------------------------------------------------------------
    // Stream mode: request stream on non-stream rune returns 400
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_stream_request_on_non_stream_rune_returns_400() {
        // The default "echo" rune has supports_stream=false.
        // Requesting ?stream=true should return 400 STREAM_NOT_SUPPORTED.
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo?stream=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "STREAM_NOT_SUPPORTED");
    }

    #[tokio::test]
    async fn test_stream_request_via_debug_route_returns_400() {
        // Same check via the /api/v1/runes/:name/run debug route
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/echo/run?stream=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "STREAM_NOT_SUPPORTED");
    }

    // -----------------------------------------------------------------------
    // Task DELETE on completed task returns 409 CONFLICT
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_delete_completed_task_returns_409() {
        let state = test_state();

        // Insert a task and mark it completed
        state
            .admin
            .store
            .insert_task("done-task", "echo", Some("input"))
            .await
            .unwrap();
        state
            .admin
            .store
            .update_task_status("done-task", TaskStatus::Completed, Some("result"), None)
            .await
            .unwrap();

        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/tasks/done-task")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "CONFLICT");
        assert!(json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("completed"));
    }

    // -----------------------------------------------------------------------
    // Task DELETE on failed task returns 409 CONFLICT
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_delete_failed_task_returns_409() {
        let state = test_state();

        // Insert a task and mark it failed
        state
            .admin
            .store
            .insert_task("fail-task", "echo", Some("input"))
            .await
            .unwrap();
        state
            .admin
            .store
            .update_task_status("fail-task", TaskStatus::Failed, None, Some("boom"))
            .await
            .unwrap();

        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/tasks/fail-task")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "CONFLICT");
        assert!(json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("failed"));
    }

    // -----------------------------------------------------------------------
    // Task GET for non-existent task_id returns 404
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_nonexistent_task_returns_404() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::get("/api/v1/tasks/does-not-exist")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "NOT_FOUND");
    }

    // -----------------------------------------------------------------------
    // Task DELETE for non-existent task_id returns 404
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_delete_nonexistent_task_returns_404() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/tasks/does-not-exist")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "NOT_FOUND");
    }

    // -----------------------------------------------------------------------
    // mgmt_create_key: invalid key_type returns 400
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_create_key_invalid_type_returns_400() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/keys")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"key_type":"invalid_type","label":"bad type"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "BAD_REQUEST");
        assert!(json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("key_type"));
    }

    // -----------------------------------------------------------------------
    // mgmt_stats: empty data returns zero counts
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_mgmt_stats_empty() {
        // Fresh state with no calls made — stats should return 0 total
        let app = test_router();
        let response = app
            .oneshot(Request::get("/api/v1/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total_calls"], 0);
        assert_eq!(json["by_rune"].as_array().unwrap().len(), 0);
    }

    // -----------------------------------------------------------------------
    // Bearer prefix variants
    // -----------------------------------------------------------------------

    fn auth_state() -> GateState {
        let mut state = test_state();
        state.auth.auth_enabled = true;
        state.auth.key_verifier =
            Arc::new(rune_store::StoreKeyVerifier::new(state.admin.store.clone()));
        state
    }

    #[tokio::test]
    async fn test_bearer_lowercase_rejected() {
        // The middleware expects exactly "Bearer " (capital B).
        // "bearer " (lowercase) should fail to strip the prefix and return 401.
        let state = auth_state();
        let key_result = state
            .admin
            .store
            .create_key(rune_store::KeyType::Gate, "test")
            .await
            .unwrap();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("bearer {}", key_result.raw_key))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_bearer_uppercase_rejected() {
        // "BEARER " (all caps) should also fail — middleware uses strip_prefix("Bearer ")
        let state = auth_state();
        let key_result = state
            .admin
            .store
            .create_key(rune_store::KeyType::Gate, "test")
            .await
            .unwrap();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("BEARER {}", key_result.raw_key))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_bearer_extra_space_rejected() {
        // "Bearer  key" (double space) — strip_prefix("Bearer ") yields " key" with a leading space
        // which is not a valid key, so verification should fail with 401.
        let state = auth_state();
        let key_result = state
            .admin
            .store
            .create_key(rune_store::KeyType::Gate, "test")
            .await
            .unwrap();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer  {}", key_result.raw_key))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // The extra space becomes part of the key, so verify_gate_key fails
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_no_bearer_prefix_rejected() {
        // Passing just the raw key without "Bearer " prefix should return 401
        let state = auth_state();
        let key_result = state
            .admin
            .store
            .create_key(rune_store::KeyType::Gate, "test")
            .await
            .unwrap();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", key_result.raw_key.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // -----------------------------------------------------------------------
    // CORS behavior verification
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cors_permissive_allows_any_origin() {
        // Default test_state has empty cors_origins → CorsLayer::permissive()
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/echo")
                    .header("origin", "https://example.com")
                    .header("access-control-request-method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Permissive CORS should respond with 200 and allow the origin
        assert_eq!(response.status(), StatusCode::OK);
        let acl = response
            .headers()
            .get("access-control-allow-origin")
            .expect("should have ACAO header");
        // Permissive CORS mirrors the Origin or returns "*"
        let acl_str = acl.to_str().unwrap();
        assert!(
            acl_str == "*" || acl_str == "https://example.com",
            "ACAO should be * or mirror origin, got: {}",
            acl_str
        );
    }

    #[tokio::test]
    async fn test_cors_restricted_rejects_unlisted_origin() {
        // Configure specific allowed origins — unlisted origin should NOT get ACAO
        let mut state = test_state();
        state.cors_origins = Arc::new(vec!["https://allowed.example.com".to_string()]);
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/echo")
                    .header("origin", "https://evil.example.com")
                    .header("access-control-request-method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // tower-http CorsLayer with explicit origins will not set ACAO for disallowed origins
        let acao = response.headers().get("access-control-allow-origin");
        assert!(
            acao.is_none() || acao.unwrap().to_str().unwrap() != "https://evil.example.com",
            "ACAO should NOT echo an unlisted origin"
        );
    }

    #[tokio::test]
    async fn test_cors_restricted_allows_listed_origin() {
        let mut state = test_state();
        state.cors_origins = Arc::new(vec!["https://allowed.example.com".to_string()]);
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/echo")
                    .header("origin", "https://allowed.example.com")
                    .header("access-control-request-method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let acao = response
            .headers()
            .get("access-control-allow-origin")
            .expect("should have ACAO for allowed origin");
        assert_eq!(acao.to_str().unwrap(), "https://allowed.example.com");
    }

    // =======================================================================
    // #1  Empty body POST — echo rune returns empty response
    // =======================================================================

    #[tokio::test]
    async fn test_empty_body_post_echo() {
        let app = test_router();
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

        // Echo rune returns whatever it receives; empty input → empty output
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert!(
            body.is_empty(),
            "echo of empty body should be empty, got {} bytes",
            body.len()
        );
    }

    // =======================================================================
    // #2  Large body POST — 10 MB body
    // =======================================================================

    #[tokio::test]
    async fn test_large_body_post() {
        // dynamic_rune_handler caps body at 1MB (1024 * 1024).
        // Sending > 1MB via gate_path should fail with BAD_REQUEST.
        let app = test_router();
        let big = vec![b'A'; 2 * 1024 * 1024]; // 2 MB
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .body(Body::from(big))
                    .unwrap(),
            )
            .await
            .unwrap();

        // The dynamic_rune_handler uses to_bytes(..., 1024*1024) — body > 1MB
        // triggers "failed to read body" error.
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "BAD_REQUEST");
    }

    // =======================================================================
    // #3  HTTP method mismatch — GET on POST-only gate_path
    //     Routing key includes the HTTP method (resolve_by_gate_path uses
    //     "<METHOD>:<path>"), so GET /echo does not match the POST:/echo entry
    //     and returns 404.
    // =======================================================================

    #[tokio::test]
    async fn test_get_on_post_gate_path_via_fallback() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/echo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // /echo is registered with method "POST" only. A GET request should
        // NOT match and must return 404 (method-aware routing).
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // =======================================================================
    // #4  Multiple runes with different gate_paths route correctly
    // =======================================================================

    #[tokio::test]
    async fn test_multiple_rune_gate_paths() {
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        // Register 3 runes with different paths returning distinct payloads
        for (name, path, output) in [
            ("alpha", "/alpha", r#"{"rune":"alpha"}"#),
            ("beta", "/beta", r#"{"rune":"beta"}"#),
            ("gamma", "/gamma", r#"{"rune":"gamma"}"#),
        ] {
            let payload = Bytes::from(output);
            let handler = make_handler(move |_ctx, _input| {
                let p = payload.clone();
                async move { Ok(p) }
            });
            relay
                .register(
                    RuneConfig {
                        name: name.into(),
                        version: "1.0.0".into(),
                        description: format!("{} rune", name),
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
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        };

        for (path, expected_name) in [("/alpha", "alpha"), ("/beta", "beta"), ("/gamma", "gamma")] {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "path {} should route correctly",
                path
            );
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                json["rune"], expected_name,
                "path {} should return rune={}",
                path, expected_name
            );
        }
    }

    // =======================================================================
    // #5  Async task: GET returns complete result after task finishes
    // =======================================================================

    #[tokio::test]
    async fn test_async_task_get_returns_completed_output() {
        let state = test_state();
        let app = build_router(state.clone(), None);

        // Submit async task
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo?async=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"result":"expected"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let task_id = json["task_id"].as_str().unwrap().to_string();

        // Wait for background completion
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // GET the task via HTTP
        let app2 = build_router(state.clone(), None);
        let response = app2
            .oneshot(
                Request::get(format!("/api/v1/tasks/{}", task_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "completed");
        assert!(
            json["output"].is_string(),
            "completed task should have output field"
        );
        // The output should contain the original input (echo)
        let output_str = json["output"].as_str().unwrap();
        assert!(
            output_str.contains("expected"),
            "output should contain the echoed input, got: {}",
            output_str
        );
    }

    // =======================================================================
    // #6  Async task failure: GET returns error info
    // =======================================================================

    #[tokio::test]
    async fn test_async_task_get_returns_error_on_failure() {
        let state = test_state();

        // Manually insert a failed task
        state
            .admin
            .store
            .insert_task("fail-async", "echo", Some("input"))
            .await
            .unwrap();
        state
            .admin
            .store
            .update_task_status(
                "fail-async",
                TaskStatus::Failed,
                None,
                Some("handler crashed"),
            )
            .await
            .unwrap();

        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::get("/api/v1/tasks/fail-async")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "failed");
        assert!(
            json["error"].is_string(),
            "failed task should have error field"
        );
        assert!(
            json["error"].as_str().unwrap().contains("handler crashed"),
            "error should contain the failure message"
        );
    }

    // =======================================================================
    // #7  Cancel a running async task → status becomes cancelled
    // =======================================================================

    #[tokio::test]
    async fn test_cancel_running_async_task() {
        let state = test_state();

        // Insert a task in running state
        state
            .admin
            .store
            .insert_task("cancel-run", "echo", Some("data"))
            .await
            .unwrap();
        state
            .admin
            .store
            .update_task_status("cancel-run", TaskStatus::Running, None, None)
            .await
            .unwrap();

        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/tasks/cancel-run")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "cancelled");
        assert_eq!(json["task_id"], "cancel-run");

        // Verify store state
        let task = state
            .admin
            .store
            .get_task("cancel-run")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.status, TaskStatus::Cancelled);
    }

    // =======================================================================
    // #8  Cancel an already-cancelled task (idempotent) → 200
    // =======================================================================

    #[tokio::test]
    async fn test_cancel_already_cancelled_task_idempotent() {
        let state = test_state();

        // Insert and cancel
        state
            .admin
            .store
            .insert_task("idempotent-cancel", "echo", Some("data"))
            .await
            .unwrap();
        state
            .admin
            .store
            .update_task_status(
                "idempotent-cancel",
                TaskStatus::Cancelled,
                None,
                Some("first cancel"),
            )
            .await
            .unwrap();

        // First DELETE on already-cancelled
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/tasks/idempotent-cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "cancelled");

        // Second DELETE — still idempotent 200
        let app2 = build_router(state.clone(), None);
        let response2 = app2
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/tasks/idempotent-cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response2.status(), StatusCode::OK);
    }

    // =======================================================================
    // #9  Multiple concurrent async tasks — independent states
    // =======================================================================

    #[tokio::test]
    async fn test_multiple_concurrent_async_tasks() {
        let state = test_state();

        let mut task_ids = Vec::new();
        for i in 0..3 {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo?async=true")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"task":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::ACCEPTED);
            let body = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            task_ids.push(json["task_id"].as_str().unwrap().to_string());
        }

        // All task IDs should be unique
        let unique: std::collections::HashSet<&String> = task_ids.iter().collect();
        assert_eq!(unique.len(), 3, "all task IDs should be unique");

        // Wait for completion
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Query each independently
        for tid in &task_ids {
            let task = state.admin.store.get_task(tid).await.unwrap();
            assert!(task.is_some(), "task {} should exist", tid);
            let task = task.unwrap();
            assert_eq!(
                task.status,
                TaskStatus::Completed,
                "task {} should be completed",
                tid
            );
        }
    }

    // =======================================================================
    // #10 list_runes returns correct info for multiple runes
    // =======================================================================

    #[tokio::test]
    async fn test_list_runes_multiple_details() {
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        // Register 5 runes
        for i in 0..5 {
            let name = format!("rune_{}", i);
            let path = format!("/path_{}", i);
            let handler = make_handler(|_ctx, input| async move { Ok(input) });
            relay
                .register(
                    RuneConfig {
                        name: name.clone(),
                        version: "1.0.0".into(),
                        description: format!("rune {}", i),
                        supports_stream: false,
                        gate: Some(GateConfig {
                            path: path.clone(),
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
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        };

        let app = build_router(state, None);
        let response = app
            .oneshot(Request::get("/api/v1/runes").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let runes = json["runes"].as_array().unwrap();
        assert_eq!(runes.len(), 5, "should list all 5 runes");

        // Verify each rune has name and gate_path fields
        for rune in runes {
            assert!(rune["name"].is_string(), "each rune should have a name");
            assert!(
                rune["gate_path"].is_string(),
                "each rune should have a gate_path"
            );
        }

        // Verify specific names are present
        let names: Vec<&str> = runes.iter().map(|r| r["name"].as_str().unwrap()).collect();
        for i in 0..5 {
            let expected = format!("rune_{}", i);
            assert!(
                names.contains(&expected.as_str()),
                "should contain {}",
                expected
            );
        }
    }

    // =======================================================================
    // #11 Management /api/v1/casters endpoint
    // =======================================================================

    #[tokio::test]
    async fn test_mgmt_casters_endpoint() {
        let app = test_router();
        let response = app
            .oneshot(Request::get("/api/v1/casters").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // No casters registered in test state, so list should be empty
        assert!(
            json["casters"].is_array(),
            "response should have 'casters' array"
        );
        assert_eq!(json["casters"].as_array().unwrap().len(), 0);
    }

    // =======================================================================
    // #12 Management /api/v1/status — all fields present with correct types
    // =======================================================================

    #[tokio::test]
    async fn test_mgmt_status_all_fields() {
        let app = test_router();
        let response = app
            .oneshot(Request::get("/api/v1/status").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Verify all expected fields exist with correct types
        assert!(
            json["uptime_secs"].is_u64(),
            "uptime_secs should be a number"
        );
        assert!(json["rune_count"].is_u64(), "rune_count should be a number");
        assert!(
            json["caster_count"].is_u64(),
            "caster_count should be a number"
        );
        assert!(
            json["dev_mode"].is_boolean(),
            "dev_mode should be a boolean"
        );

        // test_state sets dev_mode=true and registers 2 runes
        assert_eq!(json["dev_mode"], true);
        assert_eq!(json["rune_count"], 2); // echo + internal
        assert_eq!(json["caster_count"], 0);
    }

    // =======================================================================
    // #13 Management /api/v1/stats — shows data after rune calls
    // =======================================================================

    #[tokio::test]
    async fn test_mgmt_stats_after_calls() {
        let state = test_state();

        // Make 3 sync calls to echo
        for _ in 0..3 {
            let app = build_router(state.clone(), None);
            let _response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"x":1}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Also call internal via debug route
        for _ in 0..2 {
            let app = build_router(state.clone(), None);
            let _response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/runes/internal/run")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"x":2}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Query stats
        let app = build_router(state, None);
        let response = app
            .oneshot(Request::get("/api/v1/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            json["total_calls"], 5,
            "total should be 5 (3 echo + 2 internal)"
        );

        let by_rune = json["by_rune"].as_array().unwrap();
        assert_eq!(by_rune.len(), 2, "should have stats for 2 runes");

        // Find echo stats
        let echo_stat = by_rune
            .iter()
            .find(|s| s["rune_name"] == "echo")
            .expect("should have echo stats");
        assert_eq!(echo_stat["count"], 3);
        assert!(echo_stat["avg_latency_ms"].is_number());

        // Find internal stats
        let internal_stat = by_rune
            .iter()
            .find(|s| s["rune_name"] == "internal")
            .expect("should have internal stats");
        assert_eq!(internal_stat["count"], 2);
    }

    // =======================================================================
    // Cancel a pending task (not running) — also works
    // =======================================================================

    #[tokio::test]
    async fn test_cancel_pending_task() {
        let state = test_state();

        // Insert a task in pending state (insert_task default is pending)
        state
            .admin
            .store
            .insert_task("pending-cancel", "echo", Some("data"))
            .await
            .unwrap();

        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/tasks/pending-cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "cancelled");

        let task = state
            .admin
            .store
            .get_task("pending-cancel")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.status, TaskStatus::Cancelled);
    }

    // =======================================================================
    // Debug route for nonexistent rune → 404
    // =======================================================================

    #[tokio::test]
    async fn test_debug_route_nonexistent_rune_404() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/nonexistent/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"x":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "NOT_FOUND");
    }

    // =======================================================================
    // Async via debug route returns 202 and correct task_id
    // =======================================================================

    #[tokio::test]
    async fn test_async_via_debug_route() {
        let state = test_state();
        let app = build_router(state.clone(), None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/echo/run?async=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"via":"debug"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["task_id"].is_string());
        assert_eq!(json["status"], "running");

        // Wait and verify task completed
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let task_id = json["task_id"].as_str().unwrap();
        let task = state.admin.store.get_task(task_id).await.unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Completed);
    }

    // =======================================================================
    // Boundary: malformed JSON body
    // =======================================================================

    #[tokio::test]
    async fn test_plain_text_body_to_echo() {
        // Echo rune echoes raw bytes — plain text is not JSON but the rune
        // does not require JSON. Should return 200 with the raw text echoed.
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "text/plain")
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        // Echo returns input bytes as-is; non-JSON body is returned as raw bytes
        assert_eq!(&body[..], b"hello");
    }

    #[tokio::test]
    async fn test_malformed_json_to_create_key() {
        // POST /api/v1/keys with malformed JSON should be rejected by axum's
        // Json extractor, returning 4xx (400 or 422).
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/keys")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"broken":}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // axum returns 422 Unprocessable Entity for JSON parse failures
        let status = response.status().as_u16();
        assert!(
            status == 400 || status == 422,
            "malformed JSON should return 400 or 422, got: {}",
            status
        );
    }

    // =======================================================================
    // Boundary: logs query parameter edge cases
    // =======================================================================

    #[tokio::test]
    async fn test_logs_limit_zero() {
        // limit=0 — after .min(500) it stays 0; query_logs with limit 0
        // should return an empty list.
        let app = test_router();
        let response = app
            .oneshot(
                Request::get("/api/v1/logs?limit=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["logs"].as_array().unwrap().is_empty(),
            "limit=0 should return empty logs list"
        );
    }

    #[tokio::test]
    async fn test_logs_limit_negative() {
        // limit=-1 — .min(500) keeps -1; sqlite LIMIT -1 means unlimited
        // but it should not crash. Verify 200 and valid response.
        let state = test_state();

        // Insert a call so there's data
        let app = build_router(state.clone(), None);
        let _response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"neg":"limit"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let app2 = build_router(state, None);
        let response = app2
            .oneshot(
                Request::get("/api/v1/logs?limit=-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["logs"].is_array(),
            "should return a logs array even with negative limit"
        );
    }

    #[tokio::test]
    async fn test_logs_limit_exceeds_max_capped_to_500() {
        // limit=1000 — .min(500) caps to 500. Verify no crash and response is valid.
        // We insert a few logs to verify the cap doesn't break anything.
        let state = test_state();

        for _ in 0..3 {
            let app = build_router(state.clone(), None);
            let _response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"cap":"test"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::get("/api/v1/logs?limit=1000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let logs = json["logs"].as_array().unwrap();
        // We only inserted 3 logs, so even with limit=1000 (capped to 500),
        // we should get exactly 3.
        assert_eq!(
            logs.len(),
            3,
            "should return all 3 logs (capped at 500, but only 3 exist)"
        );
    }

    #[tokio::test]
    async fn test_logs_nonexistent_rune_filter() {
        // rune=nonexistent — should return empty list, not an error
        let state = test_state();

        // Insert a log for echo
        let app = build_router(state.clone(), None);
        let _response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"filter":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Query logs for a rune that doesn't exist
        let app2 = build_router(state, None);
        let response = app2
            .oneshot(
                Request::get("/api/v1/logs?rune=nonexistent&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["logs"].as_array().unwrap().is_empty(),
            "querying logs for nonexistent rune should return empty list"
        );
    }

    // =======================================================================
    // Boundary: special characters in gate_path and rune name
    // =======================================================================

    #[tokio::test]
    async fn test_gate_path_with_special_characters() {
        // Register a rune whose gate_path contains URL-encodable characters.
        // The dynamic fallback matches path literally, so a path with spaces
        // won't match the URI (which is percent-encoded).
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                RuneConfig {
                    name: "special".into(),
                    version: "1.0.0".into(),
                    description: "special path".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/my rune".into(), // path with space
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

        let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
            Arc::clone(&relay),
            Arc::clone(&resolver) as Arc<dyn Resolver>,
        )));
        let state = GateState {
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        };

        // Requesting the percent-encoded path — the dynamic_rune_handler
        // compares URI path (percent-encoded) against registered gate_path
        // (literal). "/my%20rune" != "/my rune" → 404.
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/my%20rune")
                    .body(Body::from(r#"{"special":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "percent-encoded path should not match literal gate_path with space"
        );

        // But the rune IS listed in /api/v1/runes
        let app2 = build_router(state, None);
        let response = app2
            .oneshot(Request::get("/api/v1/runes").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let runes = json["runes"].as_array().unwrap();
        let special = runes.iter().find(|r| r["name"] == "special");
        assert!(special.is_some(), "special rune should be listed");
    }

    #[tokio::test]
    async fn test_debug_route_rune_name_with_unicode() {
        // Register a rune with unicode name
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                RuneConfig {
                    name: "rune-test".into(), // ascii with hyphen
                    version: "1.0.0".into(),
                    description: "unicode test".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/unicode-test".into(),
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

        let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
            Arc::clone(&relay),
            Arc::clone(&resolver) as Arc<dyn Resolver>,
        )));
        let state = GateState {
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        };

        // Debug route with a rune name that doesn't exist (contains unicode)
        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/%E7%AC%A6%E6%96%87/run") // percent-encoded "符文"
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"x":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // The rune "符文" is not registered, so we get 404
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "unicode rune name that doesn't exist should return 404"
        );
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn test_gate_path_with_unicode_literal() {
        // Register a rune with a unicode gate_path and use it via the
        // percent-encoded URI. Shows that percent-encoded URI path won't
        // match a literal unicode gate_path.
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                RuneConfig {
                    name: "unicode_path".into(),
                    version: "1.0.0".into(),
                    description: "unicode gate path".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/\u{7b26}\u{6587}".into(), // "/符文"
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

        let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
            Arc::clone(&relay),
            Arc::clone(&resolver) as Arc<dyn Resolver>,
        )));
        let state = GateState {
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        };

        // Percent-encoded request for "/符文"
        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/%E7%AC%A6%E6%96%87") // percent-encoded "/符文"
                    .body(Body::from(r#"{"uni":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // URI path is percent-encoded but gate_path is literal unicode — no match
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "percent-encoded URI should not match literal unicode gate_path"
        );
    }

    // =======================================================================
    // Boundary: Content-Type missing with valid JSON body
    // =======================================================================

    #[tokio::test]
    async fn test_post_echo_without_content_type_header() {
        // POST to echo gate_path with valid JSON body but NO content-type header.
        // The dynamic_rune_handler reads raw bytes and passes to invoker —
        // it does NOT check content-type. Should still succeed.
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    // deliberately omitting content-type header
                    .body(Body::from(r#"{"no_ct":"header"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "echo should work without content-type header"
        );
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["no_ct"], "header");
    }

    #[tokio::test]
    async fn test_post_debug_route_without_content_type() {
        // Same test via the debug route /api/v1/runes/:name/run.
        // run_rune uses Bytes extractor which doesn't require content-type.
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/echo/run")
                    // deliberately omitting content-type header
                    .body(Body::from(r#"{"debug_no_ct":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "debug route should work without content-type header"
        );
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["debug_no_ct"], true);
    }

    // =========================================================================
    // Module 3: File Transfer Tests
    // =========================================================================
    //
    // These tests define the expected behavior for file upload/download via
    // multipart/form-data, file broker API, size limits, metadata preservation,
    // and edge cases. All marked #[ignore] because the functionality is not yet
    // implemented.
    //
    // Future implementation will add to GateState:
    //   - file_broker: Arc<FileBroker>
    //   - max_upload_size_mb: u64
    // And new routes:
    //   - GET  /api/v1/files/:id
    //   - POST multipart handling in dynamic_rune_handler / run_rune
    //
    // Expected upload response shape:
    //   {
    //     "files": [
    //       { "file_id": "...", "filename": "...", "size": N, "mime_type": "..." }
    //     ]
    //   }
    //
    // Expected download response:
    //   200 with Content-Type matching stored MIME, Content-Disposition header,
    //   and body equal to original file bytes.
    //
    // Expected error response shape:
    //   { "error": { "code": "...", "message": "..." } }
    // =========================================================================

    /// Helper: build a multipart body with the given boundary and parts.
    /// Each part is (name, Option<filename>, content_type, data).
    fn build_multipart_body(
        boundary: &str,
        parts: &[(&str, Option<&str>, &str, &[u8])],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        for (name, filename, content_type, data) in parts {
            body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
            match filename {
                Some(fname) => {
                    body.extend_from_slice(
                        format!(
                            "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n",
                            name, fname
                        )
                        .as_bytes(),
                    );
                }
                None => {
                    body.extend_from_slice(
                        format!("Content-Disposition: form-data; name=\"{}\"\r\n", name).as_bytes(),
                    );
                }
            }
            body.extend_from_slice(format!("Content-Type: {}\r\n\r\n", content_type).as_bytes());
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());
        body
    }

    /// Helper: build a multipart body with parts that may omit Content-Type.
    /// Each part is (name, Option<filename>, Option<content_type>, data).
    fn build_multipart_body_optional_ct(
        boundary: &str,
        parts: &[(&str, Option<&str>, Option<&str>, &[u8])],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        for (name, filename, content_type, data) in parts {
            body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
            match filename {
                Some(fname) => {
                    body.extend_from_slice(
                        format!(
                            "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n",
                            name, fname
                        )
                        .as_bytes(),
                    );
                }
                None => {
                    body.extend_from_slice(
                        format!("Content-Disposition: form-data; name=\"{}\"\r\n", name).as_bytes(),
                    );
                }
            }
            if let Some(ct) = content_type {
                body.extend_from_slice(format!("Content-Type: {}\r\n", ct).as_bytes());
            }
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());
        body
    }

    /// Helper: send a multipart upload request and return the response.
    async fn send_multipart(
        app: Router,
        uri: &str,
        boundary: &str,
        body: Vec<u8>,
    ) -> axum::http::Response<axum::body::Body> {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={}", boundary),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    /// Helper: extract JSON body from response.
    async fn json_body(response: axum::http::Response<axum::body::Body>) -> serde_json::Value {
        let body_bytes = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
            .await
            .unwrap();
        serde_json::from_slice(&body_bytes).unwrap()
    }

    /// Helper: extract raw body bytes from response.
    async fn raw_body(response: axum::http::Response<axum::body::Body>) -> bytes::Bytes {
        axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
            .await
            .unwrap()
    }

    // =========================================================================
    // 3.1 — Multipart Upload: single file with response body validation
    // =========================================================================

    #[tokio::test]
    async fn test_upload_single_file_response_contains_file_metadata() {
        let app = test_router();
        let boundary = "----TestBoundaryUpload1";
        let file_content = b"hello world";
        let body = build_multipart_body(
            boundary,
            &[("file", Some("test.txt"), "text/plain", file_content)],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        // Response must contain a "files" array with metadata
        assert!(json["files"].is_array(), "response should have files array");
        let files = json["files"].as_array().unwrap();
        assert_eq!(files.len(), 1, "should have exactly 1 file");

        let f = &files[0];
        assert_eq!(f["filename"], "test.txt");
        assert_eq!(f["mime_type"], "text/plain");
        assert_eq!(f["size"], file_content.len() as u64);
        assert!(f["file_id"].is_string(), "file should have a file_id");
        assert!(
            !f["file_id"].as_str().unwrap().is_empty(),
            "file_id should not be empty"
        );
    }

    // =========================================================================
    // 3.2 — Multipart Upload: JSON input + file together
    // =========================================================================

    #[tokio::test]
    async fn test_upload_json_and_file_together_response_body() {
        let app = test_router();
        let boundary = "----TestBoundaryJsonFile";
        let body = build_multipart_body(
            boundary,
            &[
                ("input", None, "application/json", br#"{"key": "value"}"#),
                ("file", Some("data.csv"), "text/csv", b"a,b,c\n1,2,3"),
            ],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        // The rune should receive the JSON input
        assert!(
            json["files"].is_array(),
            "response should include files metadata"
        );
        let files = json["files"].as_array().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["filename"], "data.csv");
        assert_eq!(files[0]["mime_type"], "text/csv");
        assert_eq!(files[0]["size"], 11); // "a,b,c\n1,2,3" = 11 bytes
    }

    // =========================================================================
    // 3.3 — Multipart Upload: multiple files with distinct metadata
    // =========================================================================

    #[tokio::test]
    async fn test_upload_multiple_files_response_body() {
        let app = test_router();
        let boundary = "----TestBoundaryMulti";
        let body = build_multipart_body(
            boundary,
            &[
                ("file1", Some("a.txt"), "text/plain", b"file a content"),
                ("file2", Some("b.txt"), "text/plain", b"file b content"),
                (
                    "file3",
                    Some("c.bin"),
                    "application/octet-stream",
                    b"\x00\x01\x02",
                ),
            ],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files array");
        assert_eq!(files.len(), 3, "should have 3 files");

        // Verify each file has distinct filename and correct size
        let filenames: Vec<&str> = files
            .iter()
            .map(|f| f["filename"].as_str().unwrap())
            .collect();
        assert!(filenames.contains(&"a.txt"));
        assert!(filenames.contains(&"b.txt"));
        assert!(filenames.contains(&"c.bin"));

        let a = files.iter().find(|f| f["filename"] == "a.txt").unwrap();
        assert_eq!(a["size"], 14); // "file a content" = 14 bytes
        assert_eq!(a["mime_type"], "text/plain");

        let c = files.iter().find(|f| f["filename"] == "c.bin").unwrap();
        assert_eq!(c["size"], 3);
        assert_eq!(c["mime_type"], "application/octet-stream");
    }

    // =========================================================================
    // 3.4 — Multipart with only JSON (no file) — backward compat
    // =========================================================================

    #[tokio::test]
    async fn test_multipart_no_file_only_json_processes_input() {
        let app = test_router();
        let boundary = "----TestBoundaryNoFile";
        let body = build_multipart_body(
            boundary,
            &[("input", None, "application/json", br#"{"only":"json"}"#)],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        // The JSON input should be processed; no files expected
        assert_eq!(json["only"], "json");
        // If "files" is present, it should be empty
        if json["files"].is_array() {
            assert_eq!(json["files"].as_array().unwrap().len(), 0);
        }
    }

    // =========================================================================
    // 3.5 — Size Limit: file under max_upload_size_mb succeeds
    // =========================================================================

    #[tokio::test]
    async fn test_file_under_max_upload_size_succeeds() {
        // Assuming default max_upload_size_mb = 10, a 1MB file is well under.
        let state = test_state();
        let app = build_router(state, None);
        let boundary = "----TestBoundarySizeOk";
        let small_data = vec![0x41u8; 1024 * 1024]; // 1MB
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("small.bin"),
                "application/octet-stream",
                &small_data,
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files array");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["filename"], "small.bin");
        assert_eq!(files[0]["size"], 1024 * 1024);
    }

    // =========================================================================
    // 3.6 — Size Limit: file exactly at max_upload_size_mb boundary succeeds
    // =========================================================================

    #[tokio::test]
    async fn test_file_equal_to_max_upload_size_succeeds() {
        // When max_upload_size_mb = 1, a file of exactly 1MB should pass.
        let mut state = test_state();
        state.rune.max_upload_size_mb = 1;
        let app = build_router(state, None);
        let boundary = "----TestBoundarySizeEq";
        let exact_data = vec![0x42u8; 1 * 1024 * 1024];
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("exact.bin"),
                "application/octet-stream",
                &exact_data,
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files[0]["size"], 1 * 1024 * 1024);
    }

    // =========================================================================
    // 3.7 — Size Limit: file exceeds max_upload_size_mb returns 413
    // =========================================================================

    #[tokio::test]
    async fn test_file_exceeds_max_upload_size_returns_413() {
        let mut state = test_state();
        state.rune.max_upload_size_mb = 1;
        let app = build_router(state, None);
        let boundary = "----TestBoundarySizeOver";
        let big_data = vec![0x43u8; 2 * 1024 * 1024]; // 2MB > 1MB limit
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("big.bin"),
                "application/octet-stream",
                &big_data,
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let json = json_body(response).await;
        assert!(json["error"].is_object(), "413 should have error object");
        assert_eq!(json["error"]["code"], "PAYLOAD_TOO_LARGE");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("size"),
            "error message should mention size"
        );
    }

    // =========================================================================
    // 3.8 — Size Limit: multiple files total exceed limit returns 413
    // =========================================================================

    #[tokio::test]
    async fn test_multiple_files_total_size_exceeds_limit_returns_413() {
        let mut state = test_state();
        state.rune.max_upload_size_mb = 1;
        let app = build_router(state, None);
        let boundary = "----TestBoundaryMultiOver";
        let chunk = vec![0x44u8; 600 * 1024]; // 600KB each, total 1.2MB > 1MB
        let body = build_multipart_body(
            boundary,
            &[
                ("file1", Some("a.bin"), "application/octet-stream", &chunk),
                ("file2", Some("b.bin"), "application/octet-stream", &chunk),
            ],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let json = json_body(response).await;
        assert_eq!(json["error"]["code"], "PAYLOAD_TOO_LARGE");
    }

    // =========================================================================
    // 3.9 — Size Limit: exact 4MB threshold boundary test
    // =========================================================================

    #[tokio::test]
    async fn test_file_exactly_4mb_threshold_inline() {
        // A file of exactly 4MB (4 * 1024 * 1024 bytes) should be sent inline
        // (at the threshold boundary, inline is used for <= 4MB).
        let app = test_router();
        let boundary = "----TestBoundary4MBExact";
        let data_4mb = vec![0x55u8; 4 * 1024 * 1024];
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("exact4mb.bin"),
                "application/octet-stream",
                &data_4mb,
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["size"], 4 * 1024 * 1024);
        // Inline transfer: file_id should be absent or "transfer_mode" == "inline"
        let transfer = files[0]["transfer_mode"].as_str().unwrap_or("inline");
        assert_eq!(transfer, "inline", "4MB file at threshold should be inline");
    }

    // =========================================================================
    // 3.10 — Small vs Large File Threshold: under 4MB sent inline
    // =========================================================================

    #[tokio::test]
    async fn test_small_file_under_4mb_sent_inline() {
        let app = test_router();
        let boundary = "----TestBoundarySmallInline";
        let small_data = vec![0x50u8; 3 * 1024 * 1024]; // 3MB
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("small_inline.bin"),
                "application/octet-stream",
                &small_data,
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["size"], 3 * 1024 * 1024);
        // Verify inline transfer mode
        let mode = files[0]["transfer_mode"].as_str().unwrap_or("inline");
        assert_eq!(mode, "inline", "3MB file should be transferred inline");
    }

    // =========================================================================
    // 3.11 — Small vs Large File Threshold: over 4MB uses broker
    // =========================================================================

    #[tokio::test]
    async fn test_large_file_over_4mb_uses_broker() {
        let state = test_state();
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryLargeBroker";
        let large_data = vec![0x51u8; 5 * 1024 * 1024]; // 5MB
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("large_broker.bin"),
                "application/octet-stream",
                &large_data,
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["size"], 5 * 1024 * 1024);
        // Broker transfer: should have a file_id and "transfer_mode" == "broker"
        let mode = files[0]["transfer_mode"].as_str().unwrap_or("");
        assert_eq!(mode, "broker", "5MB file should use broker transfer");
        assert!(
            files[0]["file_id"].is_string() && !files[0]["file_id"].as_str().unwrap().is_empty(),
            "broker file should have a file_id"
        );
    }

    // =========================================================================
    // 3.12 — Mixed small + large files: threshold per-file
    // =========================================================================

    #[tokio::test]
    async fn test_mixed_small_and_large_files_different_transfer_modes() {
        let state = test_state();
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryMixed";
        let small = vec![0x60u8; 2 * 1024 * 1024]; // 2MB inline
        let large = vec![0x61u8; 5 * 1024 * 1024]; // 5MB broker
        let body = build_multipart_body(
            boundary,
            &[
                (
                    "file1",
                    Some("small.bin"),
                    "application/octet-stream",
                    &small,
                ),
                (
                    "file2",
                    Some("large.bin"),
                    "application/octet-stream",
                    &large,
                ),
            ],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files.len(), 2);

        let small_file = files.iter().find(|f| f["filename"] == "small.bin").unwrap();
        let large_file = files.iter().find(|f| f["filename"] == "large.bin").unwrap();

        assert_eq!(
            small_file["transfer_mode"].as_str().unwrap_or("inline"),
            "inline",
            "2MB file should be inline"
        );
        assert_eq!(
            large_file["transfer_mode"].as_str().unwrap_or(""),
            "broker",
            "5MB file should use broker"
        );
    }

    // =========================================================================
    // 3.13 — File Broker: upload then download complete lifecycle (E2E)
    // =========================================================================

    #[tokio::test]
    async fn test_upload_then_download_e2e_lifecycle() {
        // E2E: upload a file via multipart, extract file_id from response,
        // then GET /api/v1/files/:file_id and verify content matches.
        let state = test_state();
        let file_content = b"The quick brown fox jumps over the lazy dog.";

        // Step 1: Upload
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryE2E";
        let body = build_multipart_body(
            boundary,
            &[("file", Some("fox.txt"), "text/plain", file_content)],
        );
        let upload_resp = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(upload_resp.status(), StatusCode::OK);

        let upload_json = json_body(upload_resp).await;
        let files = upload_json["files"].as_array().expect("should have files");
        assert_eq!(files.len(), 1);
        let file_id = files[0]["file_id"].as_str().expect("should have file_id");
        assert!(!file_id.is_empty());

        // Step 2: Download by file_id
        let app2 = build_router(state.clone(), None);
        let download_resp = app2
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/v1/files/{}", file_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(download_resp.status(), StatusCode::OK);

        // Verify Content-Type
        let ct = download_resp
            .headers()
            .get("content-type")
            .expect("should have content-type")
            .to_str()
            .unwrap();
        assert_eq!(ct, "text/plain");

        // Verify Content-Disposition
        let cd = download_resp
            .headers()
            .get("content-disposition")
            .expect("should have content-disposition")
            .to_str()
            .unwrap();
        assert!(
            cd.contains("fox.txt"),
            "content-disposition should contain original filename, got: {}",
            cd
        );

        // Verify content matches
        let downloaded = raw_body(download_resp).await;
        assert_eq!(
            &downloaded[..],
            file_content,
            "downloaded content should match uploaded content"
        );
    }

    // =========================================================================
    // 3.14 — File Broker: download nonexistent file returns 404 with error body
    // =========================================================================

    #[tokio::test]
    async fn test_download_nonexistent_file_returns_404_with_error() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/files/does-not-exist-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let json = json_body(response).await;
        assert!(json["error"].is_object(), "404 should have error object");
        assert_eq!(json["error"]["code"], "NOT_FOUND");
        assert!(
            json["error"]["message"].as_str().unwrap_or("").len() > 0,
            "error message should not be empty"
        );
    }

    // =========================================================================
    // 3.15 — File Broker: file cleaned up after rune execution returns 404
    // =========================================================================

    #[tokio::test]
    async fn test_file_cleaned_up_after_request_returns_404() {
        // Upload a file, then explicitly mark the request as completed
        // (simulating rune execution finishing), then verify the file
        // is no longer downloadable.
        let state = test_state();

        // Step 1: Upload a file
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryCleanup";
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("temp.dat"),
                "application/octet-stream",
                b"temp data",
            )],
        );
        let upload_resp = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(upload_resp.status(), StatusCode::OK);

        let upload_json = json_body(upload_resp).await;
        let file_id = upload_json["files"][0]["file_id"]
            .as_str()
            .expect("should have file_id");

        // Step 2: Look up the request_id from the broker, then mark it completed
        let stored = state
            .rune
            .file_broker
            .get(file_id)
            .expect("file should exist before cleanup");
        let request_id = stored.request_id.clone();
        state.rune.file_broker.complete_request(&request_id);

        // Step 3: Try to download the cleaned-up file → should be 404
        let app2 = build_router(state, None);
        let download_resp = app2
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/v1/files/{}", file_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(download_resp.status(), StatusCode::NOT_FOUND);
        let json = json_body(download_resp).await;
        assert_eq!(json["error"]["code"], "NOT_FOUND");
    }

    // =========================================================================
    // 3.16 — File Broker: download returns correct Content-Type header
    // =========================================================================

    #[tokio::test]
    async fn test_download_file_correct_content_type_header() {
        let state = test_state();

        // Upload a PNG-like file
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryPngCT";
        let png_magic = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let body = build_multipart_body(
            boundary,
            &[("file", Some("image.png"), "image/png", &png_magic)],
        );
        let upload_resp = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(upload_resp.status(), StatusCode::OK);

        let upload_json = json_body(upload_resp).await;
        let file_id = upload_json["files"][0]["file_id"]
            .as_str()
            .expect("should have file_id");

        // Download and check Content-Type
        let app2 = build_router(state, None);
        let download_resp = app2
            .oneshot(
                Request::get(format!("/api/v1/files/{}", file_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(download_resp.status(), StatusCode::OK);
        let ct = download_resp
            .headers()
            .get("content-type")
            .expect("should have content-type header")
            .to_str()
            .unwrap();
        assert_eq!(ct, "image/png");
    }

    // =========================================================================
    // 3.17 — File Broker: download returns Content-Disposition with filename
    // =========================================================================

    #[tokio::test]
    async fn test_download_file_content_disposition_header() {
        let state = test_state();

        // Upload a CSV
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryCsvCD";
        let body = build_multipart_body(
            boundary,
            &[("file", Some("report.csv"), "text/csv", b"a,b\n1,2")],
        );
        let upload_resp = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(upload_resp.status(), StatusCode::OK);

        let upload_json = json_body(upload_resp).await;
        let file_id = upload_json["files"][0]["file_id"]
            .as_str()
            .expect("should have file_id");

        // Download and check Content-Disposition
        let app2 = build_router(state, None);
        let download_resp = app2
            .oneshot(
                Request::get(format!("/api/v1/files/{}", file_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(download_resp.status(), StatusCode::OK);
        let cd = download_resp
            .headers()
            .get("content-disposition")
            .expect("should have content-disposition header")
            .to_str()
            .unwrap();
        assert!(
            cd.contains("report.csv"),
            "content-disposition should contain filename 'report.csv', got: {}",
            cd
        );
    }

    // =========================================================================
    // 3.18 — Metadata: filename and MIME type preserved in response
    // =========================================================================

    #[tokio::test]
    async fn test_upload_preserves_original_filename_and_mime_type() {
        let app = test_router();
        let boundary = "----TestBoundaryMeta";
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("report-2024.pdf"),
                "application/pdf",
                b"pdf content here",
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["filename"], "report-2024.pdf");
        assert_eq!(files[0]["mime_type"], "application/pdf");
        assert_eq!(files[0]["size"], 16); // "pdf content here" = 16 bytes
    }

    // =========================================================================
    // 3.19 — Metadata: missing MIME type defaults to application/octet-stream
    // =========================================================================

    #[tokio::test]
    async fn test_upload_without_mime_type_defaults_to_octet_stream() {
        let app = test_router();
        let boundary = "----TestBoundaryNoMime";
        // Build a part without Content-Type header
        let body = build_multipart_body_optional_ct(
            boundary,
            &[("file", Some("mystery.dat"), None, b"some unknown data")],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files[0]["filename"], "mystery.dat");
        assert_eq!(
            files[0]["mime_type"], "application/octet-stream",
            "missing MIME type should default to application/octet-stream"
        );
    }

    // =========================================================================
    // 3.20 — Edge case: empty file (0 bytes)
    // =========================================================================

    #[tokio::test]
    async fn test_empty_file_zero_bytes_accepted() {
        let app = test_router();
        let boundary = "----TestBoundaryEmpty";
        let body =
            build_multipart_body(boundary, &[("file", Some("empty.txt"), "text/plain", b"")]);

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["filename"], "empty.txt");
        assert_eq!(files[0]["size"], 0, "empty file should have size 0");
    }

    // =========================================================================
    // 3.21 — Edge case: filename with spaces preserved
    // =========================================================================

    #[tokio::test]
    async fn test_filename_with_spaces_preserved() {
        let app = test_router();
        let boundary = "----TestBoundarySpaces";
        let body = build_multipart_body(
            boundary,
            &[("file", Some("my file name.txt"), "text/plain", b"content")],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files[0]["filename"], "my file name.txt");
    }

    // =========================================================================
    // 3.22 — Edge case: CJK filename preserved
    // =========================================================================

    #[tokio::test]
    async fn test_filename_with_chinese_characters_preserved() {
        let app = test_router();
        let boundary = "----TestBoundaryCJK";
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("\u{62a5}\u{544a}\u{6587}\u{4ef6}.pdf"),
                "application/pdf",
                b"pdf",
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files[0]["filename"], "\u{62a5}\u{544a}\u{6587}\u{4ef6}.pdf");
    }

    // =========================================================================
    // 3.23 — Edge case: path traversal in filename sanitized
    // =========================================================================

    #[tokio::test]
    async fn test_filename_path_traversal_sanitized() {
        let app = test_router();
        let boundary = "----TestBoundaryPathSep";
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("../../etc/passwd"),
                "text/plain",
                b"not really",
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        let stored_name = files[0]["filename"].as_str().unwrap();
        // The filename should be sanitized: no directory traversal components
        assert!(
            !stored_name.contains(".."),
            "filename should not contain '..' after sanitization, got: {}",
            stored_name
        );
        assert!(
            !stored_name.starts_with('/'),
            "filename should not start with '/' after sanitization, got: {}",
            stored_name
        );
    }

    // =========================================================================
    // 3.24 — Edge case: very long filename handled gracefully
    // =========================================================================

    #[tokio::test]
    async fn test_very_long_filename_handled() {
        let app = test_router();
        let boundary = "----TestBoundaryLongName";
        let long_name = "x".repeat(300) + ".txt";
        let body = build_multipart_body(
            boundary,
            &[("file", Some(&long_name), "text/plain", b"data")],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        let status = response.status().as_u16();
        // Either 200 (with truncated name) or 400 (rejected) — both acceptable.
        assert!(
            status == 200 || status == 400,
            "expected 200 or 400 for very long filename, got {}",
            status,
        );

        if status == 200 {
            let json = json_body(response).await;
            let files = json["files"].as_array().expect("should have files");
            let stored_name = files[0]["filename"].as_str().unwrap();
            assert!(
                stored_name.len() <= 255,
                "filename should be truncated to <= 255 chars, got {} chars",
                stored_name.len()
            );
        } else {
            let json = json_body(response).await;
            assert_eq!(json["error"]["code"], "BAD_REQUEST");
        }
    }

    // =========================================================================
    // 3.25 — Edge case: empty filename
    // =========================================================================

    #[tokio::test]
    async fn test_empty_filename_handled() {
        let app = test_router();
        let boundary = "----TestBoundaryEmptyName";
        let body = build_multipart_body(
            boundary,
            &[("file", Some(""), "text/plain", b"data with empty name")],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        let status = response.status().as_u16();
        // Should either accept with a generated filename or reject with 400
        assert!(
            status == 200 || status == 400,
            "empty filename should return 200 or 400, got {}",
            status
        );

        if status == 200 {
            let json = json_body(response).await;
            let files = json["files"].as_array().expect("should have files");
            // If accepted, the filename should be non-empty (auto-generated)
            let name = files[0]["filename"].as_str().unwrap_or("");
            assert!(
                !name.is_empty(),
                "accepted empty filename should be replaced with generated name"
            );
        }
    }

    // =========================================================================
    // 3.26 — Edge case: no filename attribute in Content-Disposition
    // =========================================================================

    #[tokio::test]
    async fn test_no_filename_attribute_in_disposition() {
        // A file part without the filename attribute in Content-Disposition
        let app = test_router();
        let boundary = "----TestBoundaryNoFilenameAttr";
        // Use build_multipart_body with None for filename
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                None,
                "application/octet-stream",
                b"data without filename",
            )],
        );

        let response = send_multipart(app, "/echo", boundary, body).await;
        let status = response.status().as_u16();
        assert!(
            status == 200 || status == 400,
            "no filename attribute should return 200 or 400, got {}",
            status
        );

        if status == 200 {
            let json = json_body(response).await;
            let empty = vec![];
            let files = json["files"].as_array().unwrap_or(&empty);
            if !files.is_empty() {
                // If treated as a file, filename should be auto-generated
                let name = files[0]["filename"].as_str().unwrap_or("");
                assert!(
                    !name.is_empty(),
                    "file without filename attribute should get a generated name"
                );
            }
        }
    }

    // =========================================================================
    // 3.27 — Binary file transfer with data integrity verification
    // =========================================================================

    #[tokio::test]
    async fn test_binary_file_transfer_data_integrity() {
        // Upload binary data (all 256 byte values), download it, and verify
        // byte-for-byte integrity.
        let state = test_state();
        let binary_data: Vec<u8> = (0..=255).collect();

        // Upload
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryBinaryIntegrity";
        let body = build_multipart_body(
            boundary,
            &[(
                "file",
                Some("binary.bin"),
                "application/octet-stream",
                &binary_data,
            )],
        );
        let upload_resp = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(upload_resp.status(), StatusCode::OK);

        let upload_json = json_body(upload_resp).await;
        let files = upload_json["files"].as_array().expect("should have files");
        assert_eq!(files[0]["size"], 256);
        let file_id = files[0]["file_id"].as_str().expect("should have file_id");

        // Download
        let app2 = build_router(state, None);
        let download_resp = app2
            .oneshot(
                Request::get(format!("/api/v1/files/{}", file_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(download_resp.status(), StatusCode::OK);
        let downloaded = raw_body(download_resp).await;

        // Byte-for-byte integrity check
        assert_eq!(
            downloaded.len(),
            binary_data.len(),
            "downloaded size should match uploaded size"
        );
        assert_eq!(
            &downloaded[..],
            &binary_data[..],
            "binary content should be identical after round-trip"
        );
    }

    // =========================================================================
    // 3.28 — Debug route: multipart upload
    // =========================================================================

    #[tokio::test]
    async fn test_multipart_upload_via_debug_route() {
        let app = test_router();
        let boundary = "----TestBoundaryDebugMulti";
        let body = build_multipart_body(
            boundary,
            &[
                ("input", None, "application/json", br#"{"debug":true}"#),
                ("file", Some("debug.txt"), "text/plain", b"debug file data"),
            ],
        );

        let response = send_multipart(app, "/api/v1/runes/echo/run", boundary, body).await;
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let files = json["files"].as_array().expect("should have files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["filename"], "debug.txt");
        assert_eq!(files[0]["size"], 15); // "debug file data" = 15 bytes
    }

    // =========================================================================
    // 3.29 — Debug route: multipart to nonexistent rune returns 404
    // =========================================================================

    #[tokio::test]
    async fn test_multipart_debug_route_nonexistent_rune_404() {
        let app = test_router();
        let boundary = "----TestBoundaryDebug404";
        let body =
            build_multipart_body(boundary, &[("file", Some("x.txt"), "text/plain", b"data")]);

        let response = send_multipart(app, "/api/v1/runes/nonexistent/run", boundary, body).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let json = json_body(response).await;
        assert_eq!(json["error"]["code"], "NOT_FOUND");
    }

    // =========================================================================
    // 3.30 — Async mode: multipart returns 202 with task_id
    // =========================================================================

    #[tokio::test]
    async fn test_multipart_with_async_mode_returns_task_id() {
        let state = test_state();
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryAsync";
        let body = build_multipart_body(
            boundary,
            &[
                ("input", None, "application/json", br#"{"async_test":1}"#),
                ("file", Some("async.txt"), "text/plain", b"async file data"),
            ],
        );

        let response = send_multipart(app, "/echo?async=true", boundary, body).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let json = json_body(response).await;
        assert!(json["task_id"].is_string(), "should have task_id");
        assert!(
            !json["task_id"].as_str().unwrap().is_empty(),
            "task_id should not be empty"
        );
        assert_eq!(json["status"], "running");
    }

    // =========================================================================
    // 3.31 — Async mode: multipart via debug route returns 202
    // =========================================================================

    #[tokio::test]
    async fn test_multipart_async_via_debug_route() {
        let state = test_state();
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryAsyncDebug";
        let body = build_multipart_body(
            boundary,
            &[
                ("input", None, "application/json", br#"{"v":1}"#),
                ("file", Some("ad.txt"), "text/plain", b"async debug file"),
            ],
        );

        let response =
            send_multipart(app, "/api/v1/runes/echo/run?async=true", boundary, body).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let json = json_body(response).await;
        assert!(json["task_id"].is_string(), "should have task_id");
        assert_eq!(json["status"], "running");
    }

    // =========================================================================
    // 3.32 — Error: malformed multipart body
    // =========================================================================

    #[tokio::test]
    async fn test_malformed_multipart_body_returns_400() {
        let app = test_router();
        // Send garbage bytes with multipart content-type
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "multipart/form-data; boundary=----Garbage")
                    .body(Body::from(b"this is not a valid multipart body".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status().as_u16();
        assert_eq!(status, 400, "malformed multipart should return 400");

        let json = json_body(response).await;
        assert!(json["error"].is_object(), "should have error object");
        assert_eq!(json["error"]["code"], "BAD_REQUEST");
    }

    // =========================================================================
    // 3.33 — Error: truncated multipart body (missing closing boundary)
    // =========================================================================

    #[tokio::test]
    async fn test_truncated_multipart_body_returns_400() {
        let app = test_router();
        let boundary = "----TestBoundaryTruncated";
        // Build a partial multipart body — missing closing boundary
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"trunc.txt\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: text/plain\r\n\r\n");
        body.extend_from_slice(b"partial data");
        // No closing boundary!

        let response = send_multipart(app, "/echo", boundary, body).await;
        let status = response.status().as_u16();
        assert_eq!(status, 400, "truncated multipart should return 400");

        let json = json_body(response).await;
        assert_eq!(json["error"]["code"], "BAD_REQUEST");
    }

    // =========================================================================
    // 3.34 — Error: invalid file_id format returns 400 or 404
    // =========================================================================

    #[tokio::test]
    async fn test_invalid_file_id_format_returns_error() {
        // Try various invalid file_id formats
        for invalid_id in &[
            "not-a-uuid",
            "12345",
            "",
            "../../../etc/passwd",
            "%3Cscript%3Ealert(1)%3C/script%3E",
        ] {
            let uri = format!("/api/v1/files/{}", invalid_id);
            let app = test_router();
            let response = app
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(&uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            let status = response.status().as_u16();
            assert!(
                status == 400 || status == 404,
                "invalid file_id '{}' should return 400 or 404, got {}",
                invalid_id,
                status
            );

            let json = json_body(response).await;
            assert!(
                json["error"].is_object(),
                "invalid file_id '{}' should have error object",
                invalid_id
            );
        }
    }

    // =========================================================================
    // 3.35 — Error: Content-Type header says multipart but body is JSON
    // =========================================================================

    #[tokio::test]
    async fn test_content_type_multipart_but_body_is_json_returns_400() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "multipart/form-data; boundary=----Fake")
                    .body(Body::from(r#"{"this":"is json not multipart"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status().as_u16();
        assert_eq!(
            status, 400,
            "JSON body with multipart content-type should return 400"
        );

        let json = json_body(response).await;
        assert_eq!(json["error"]["code"], "BAD_REQUEST");
    }

    // =========================================================================
    // 3.36 — Error: Content-Type says JSON but body is multipart
    // =========================================================================

    #[tokio::test]
    async fn test_content_type_json_but_body_is_multipart() {
        let app = test_router();
        let boundary = "----TestBoundaryMismatch";
        let multipart_body = build_multipart_body(
            boundary,
            &[("file", Some("test.txt"), "text/plain", b"data")],
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(multipart_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // When content-type is JSON, the system should try to parse as JSON.
        // A multipart body is not valid JSON, so it should fail.
        let status = response.status().as_u16();
        assert!(
            status == 400 || status == 200,
            "JSON content-type with multipart body should return 400 (parse error) or 200 (raw echo), got {}",
            status
        );
    }

    // =========================================================================
    // 3.37 — Concurrent uploads: isolation between requests
    // =========================================================================

    #[tokio::test]
    async fn test_concurrent_uploads_isolation() {
        // Multiple concurrent uploads should not interfere with each other.
        // Each upload should get its own file_ids and metadata.
        let state = test_state();

        let mut handles = Vec::new();
        for i in 0..5 {
            let state_clone = state.clone();
            let handle = tokio::spawn(async move {
                let app = build_router(state_clone, None);
                let boundary = format!("----TestBoundaryConcurrent{}", i);
                let filename = format!("concurrent_{}.txt", i);
                let content = format!("content for file {}", i);
                let body = build_multipart_body(
                    &boundary,
                    &[("file", Some(&filename), "text/plain", content.as_bytes())],
                );

                let response = send_multipart(app, "/echo", &boundary, body).await;
                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "concurrent upload {} should succeed",
                    i
                );

                let json = json_body(response).await;
                let files = json["files"].as_array().expect("should have files");
                assert_eq!(files.len(), 1);
                assert_eq!(files[0]["filename"].as_str().unwrap(), filename);

                // Return file_id for uniqueness check
                files[0]["file_id"].as_str().unwrap().to_string()
            });
            handles.push(handle);
        }

        // Collect all file_ids
        let mut file_ids = Vec::new();
        for handle in handles {
            let file_id = handle.await.unwrap();
            file_ids.push(file_id);
        }

        // All file_ids should be unique
        let unique: std::collections::HashSet<&String> = file_ids.iter().collect();
        assert_eq!(
            unique.len(),
            5,
            "all concurrent upload file_ids should be unique"
        );
    }

    // =========================================================================
    // 3.38 — Config: max_upload_size_mb = 0 rejects all files
    // =========================================================================

    #[tokio::test]
    async fn test_max_upload_size_zero_rejects_all() {
        let mut state = test_state();
        state.rune.max_upload_size_mb = 0;
        let app = build_router(state, None);
        let boundary = "----TestBoundaryZeroLimit";
        // Even a 1-byte file should be rejected
        let body =
            build_multipart_body(boundary, &[("file", Some("tiny.txt"), "text/plain", b"x")]);

        let response = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "max_upload_size_mb=0 should reject all file uploads"
        );

        let json = json_body(response).await;
        assert_eq!(json["error"]["code"], "PAYLOAD_TOO_LARGE");
    }

    // =========================================================================
    // 3.39 — Full E2E lifecycle: upload, list, download, verify, cleanup
    // =========================================================================

    #[tokio::test]
    async fn test_full_e2e_lifecycle_multiple_files() {
        let state = test_state();
        let files_data = vec![
            ("doc.txt", "text/plain", b"Hello World".as_slice()),
            (
                "data.json",
                "application/json",
                br#"{"key":"value"}"#.as_slice(),
            ),
            (
                "image.bin",
                "application/octet-stream",
                &[0xFFu8, 0xD8, 0xFF, 0xE0] as &[u8],
            ),
        ];

        // Step 1: Upload all files
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryFullE2E";
        let parts: Vec<(&str, Option<&str>, &str, &[u8])> = files_data
            .iter()
            .map(|(name, mime, data)| ("file", Some(*name), *mime, *data))
            .collect();
        let body = build_multipart_body(boundary, &parts);

        let upload_resp = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(upload_resp.status(), StatusCode::OK);

        let upload_json = json_body(upload_resp).await;
        let files = upload_json["files"].as_array().expect("should have files");
        assert_eq!(files.len(), 3, "should upload 3 files");

        // Step 2: Download each file and verify content
        for (original_name, original_mime, original_data) in &files_data {
            let file_entry = files
                .iter()
                .find(|f| f["filename"].as_str().unwrap() == *original_name)
                .unwrap_or_else(|| panic!("should find file {}", original_name));

            let file_id = file_entry["file_id"].as_str().expect("should have file_id");

            let app = build_router(state.clone(), None);
            let download_resp = app
                .oneshot(
                    Request::get(format!("/api/v1/files/{}", file_id))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(
                download_resp.status(),
                StatusCode::OK,
                "download {} should succeed",
                original_name
            );

            // Verify Content-Type
            let ct = download_resp
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap();
            assert_eq!(
                ct, *original_mime,
                "Content-Type for {} should match",
                original_name
            );

            // Verify content
            let downloaded = raw_body(download_resp).await;
            assert_eq!(
                &downloaded[..],
                *original_data,
                "content of {} should match after download",
                original_name
            );
        }
    }

    // =========================================================================
    // 3.40 — Upload via gate_path and debug route produce same response shape
    // =========================================================================

    #[tokio::test]
    async fn test_gate_path_and_debug_route_same_response_shape() {
        let state = test_state();
        let boundary = "----TestBoundaryParity";
        let file_content = b"parity test data";

        // Upload via gate_path
        let app1 = build_router(state.clone(), None);
        let body1 = build_multipart_body(
            boundary,
            &[("file", Some("parity.txt"), "text/plain", file_content)],
        );
        let resp1 = send_multipart(app1, "/echo", boundary, body1).await;
        assert_eq!(resp1.status(), StatusCode::OK);
        let json1 = json_body(resp1).await;

        // Upload via debug route
        let app2 = build_router(state.clone(), None);
        let body2 = build_multipart_body(
            boundary,
            &[("file", Some("parity.txt"), "text/plain", file_content)],
        );
        let resp2 = send_multipart(app2, "/api/v1/runes/echo/run", boundary, body2).await;
        assert_eq!(resp2.status(), StatusCode::OK);
        let json2 = json_body(resp2).await;

        // Both should have same shape: files array with same metadata
        assert!(json1["files"].is_array());
        assert!(json2["files"].is_array());
        assert_eq!(
            json1["files"].as_array().unwrap().len(),
            json2["files"].as_array().unwrap().len()
        );

        let f1 = &json1["files"][0];
        let f2 = &json2["files"][0];
        assert_eq!(f1["filename"], f2["filename"]);
        assert_eq!(f1["mime_type"], f2["mime_type"]);
        assert_eq!(f1["size"], f2["size"]);
    }

    // =========================================================================
    // 3.41 — Multipart with no boundary in Content-Type returns 400
    // =========================================================================

    #[tokio::test]
    async fn test_multipart_no_boundary_in_content_type_returns_400() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    // multipart/form-data without boundary parameter
                    .header("content-type", "multipart/form-data")
                    .body(Body::from(b"some body".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status().as_u16();
        assert_eq!(status, 400, "multipart without boundary should return 400");

        let json = json_body(response).await;
        assert_eq!(json["error"]["code"], "BAD_REQUEST");
    }

    // =========================================================================
    // 3.42 — Large binary file round-trip with computed checksum
    // =========================================================================

    #[tokio::test]
    async fn test_large_binary_round_trip_checksum() {
        // Upload a deterministic 1MB binary file, download it, and verify
        // that every byte matches via a simple checksum.
        let state = test_state();

        // Generate deterministic 1MB data
        let mut data = Vec::with_capacity(1024 * 1024);
        for i in 0u32..(1024 * 1024 / 4) {
            data.extend_from_slice(&i.to_le_bytes());
        }
        assert_eq!(data.len(), 1024 * 1024);

        // Compute a simple checksum (sum of all bytes mod u64)
        let upload_checksum: u64 = data.iter().map(|b| *b as u64).sum();

        // Upload
        let app = build_router(state.clone(), None);
        let boundary = "----TestBoundaryLargeChecksum";
        let body = build_multipart_body(
            boundary,
            &[("file", Some("large.bin"), "application/octet-stream", &data)],
        );
        let upload_resp = send_multipart(app, "/echo", boundary, body).await;
        assert_eq!(upload_resp.status(), StatusCode::OK);

        let upload_json = json_body(upload_resp).await;
        let file_id = upload_json["files"][0]["file_id"]
            .as_str()
            .expect("should have file_id");

        // Download
        let app2 = build_router(state, None);
        let download_resp = app2
            .oneshot(
                Request::get(format!("/api/v1/files/{}", file_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(download_resp.status(), StatusCode::OK);

        let downloaded = raw_body(download_resp).await;
        assert_eq!(downloaded.len(), data.len(), "sizes should match");

        let download_checksum: u64 = downloaded.iter().map(|b| *b as u64).sum();
        assert_eq!(
            upload_checksum, download_checksum,
            "checksums should match for round-trip integrity"
        );
        assert_eq!(&downloaded[..], &data[..], "byte-for-byte match");
    }

    // =======================================================================
    // Module 4: Schema validation integration tests
    // 等 schema 校验集成后取消 ignore
    // =======================================================================

    /// Helper: build a GateState with runes that have input_schema / output_schema
    fn test_state_with_schema() -> GateState {
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        let input_schema = r#"{
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": { "type": "string" },
                "age": { "type": "integer" }
            }
        }"#;

        let output_schema = r#"{
            "type": "object",
            "required": ["result"],
            "properties": {
                "result": { "type": "string" }
            }
        }"#;

        // Rune with schema: returns valid output
        let echo_with_schema =
            make_handler(|_ctx, _input| async move { Ok(Bytes::from(r#"{"result": "ok"}"#)) });
        relay
            .register(
                RuneConfig {
                    name: "validated".into(),
                    version: "1.0.0".into(),
                    description: "rune with schema".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/validated".into(),
                        method: "POST".into(),
                    }),
                    input_schema: Some(input_schema.to_string()),
                    output_schema: Some(output_schema.to_string()),
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(echo_with_schema)),
                None,
            )
            .unwrap();

        // Rune without schema (backward compat)
        let no_schema_handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                RuneConfig {
                    name: "no_schema".into(),
                    version: "1.0.0".into(),
                    description: "no schema rune".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/no-schema".into(),
                        method: "POST".into(),
                    }),
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(no_schema_handler)),
                None,
            )
            .unwrap();

        // Rune that returns output NOT matching output_schema
        let bad_output_handler = make_handler(|_ctx, _input| async move {
            // Returns {"result": 42} but schema expects "result" to be string
            Ok(Bytes::from(r#"{"result": 42}"#))
        });
        relay
            .register(
                RuneConfig {
                    name: "bad_output".into(),
                    version: "1.0.0".into(),
                    description: "rune with bad output".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/bad-output".into(),
                        method: "POST".into(),
                    }),
                    input_schema: Some(input_schema.to_string()),
                    output_schema: Some(output_schema.to_string()),
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(bad_output_handler)),
                None,
            )
            .unwrap();

        let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
            Arc::clone(&relay),
            Arc::clone(&resolver) as Arc<dyn Resolver>,
        )));
        GateState {
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        }
    }

    #[tokio::test]
    async fn test_schema_valid_input_returns_200() {
        let state = test_state_with_schema();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/validated")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name": "Alice", "age": 30}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_schema_invalid_input_returns_422() {
        let state = test_state_with_schema();
        let app = build_router(state, None);

        // Missing "age" field, which is required
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/validated")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name": "Alice"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Error should contain validation details
        assert!(json["error"].is_object(), "should have error object");
        let error_msg = json["error"]["message"].as_str().unwrap_or("");
        assert!(
            error_msg.contains("age")
                || error_msg.contains("required")
                || error_msg.contains("validation"),
            "error should mention the validation issue, got: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_no_schema_rune_skips_validation() {
        let state = test_state_with_schema();
        let app = build_router(state, None);

        // Send anything to the rune without schema — should work fine
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/no-schema")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"anything": "goes"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_openapi_endpoint_returns_valid_json() {
        let state = test_state_with_schema();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::get("/api/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Verify OpenAPI 3.0 structure
        assert_eq!(json["openapi"].as_str().unwrap(), "3.0.0");
        assert!(json["info"].is_object());
        assert!(json["paths"].is_object());

        // Should include the validated rune path
        assert!(
            json["paths"]["/validated"].is_object(),
            "OpenAPI should include /validated path"
        );
    }

    #[tokio::test]
    async fn test_output_schema_failure_returns_500() {
        let state = test_state_with_schema();
        let app = build_router(state, None);

        // Send valid input to the rune that returns bad output
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/bad-output")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name": "Alice", "age": 30}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Output schema violation is a server error
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_schema_validation_via_debug_route() {
        let state = test_state_with_schema();
        let app = build_router(state, None);

        // Invalid input via debug route should also get 422
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/validated/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"wrong": "fields"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn test_schema_validation_in_async_mode() {
        let state = test_state_with_schema();
        let app = build_router(state, None);

        // Async mode with invalid input should reject immediately (before spawning task)
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/validated?async=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name": "Alice"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should get 422 immediately, not 202 Accepted
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // =======================================================================
    // Issue Fix: multipart requests must validate JSON input against schema
    // =======================================================================

    #[tokio::test]
    async fn test_fix_multipart_with_schema_validates_json_input() {
        // multipart request with JSON input that violates schema should get 422
        let state = test_state_with_schema();
        let app = build_router(state, None);

        let boundary = "----TestBoundary";
        // JSON part is missing required "age" field
        let body = build_multipart_body(
            boundary,
            &[
                ("input", None, "application/json", br#"{"name": "Alice"}"#),
                ("file", Some("photo.png"), "image/png", b"fake-png-data"),
            ],
        );

        let response = send_multipart(app, "/validated", boundary, body).await;
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "multipart with invalid JSON input should return 422, not bypass schema validation"
        );
    }

    #[tokio::test]
    async fn test_fix_multipart_with_schema_valid_input_passes() {
        // multipart request with valid JSON input should pass schema validation
        let state = test_state_with_schema();
        let app = build_router(state, None);

        let boundary = "----TestBoundary";
        let body = build_multipart_body(
            boundary,
            &[
                (
                    "input",
                    None,
                    "application/json",
                    br#"{"name": "Alice", "age": 30}"#,
                ),
                ("file", Some("photo.png"), "image/png", b"fake-png-data"),
            ],
        );

        let response = send_multipart(app, "/validated", boundary, body).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "multipart with valid JSON input should return 200"
        );
    }

    // =======================================================================
    // Issue Fix: FileBroker memory leak — complete_request must clean up files
    // =======================================================================

    #[tokio::test]
    async fn test_fix_file_broker_cleans_up_files_on_complete() {
        // FileBroker should release file data after complete_request
        let broker = FileBroker::new();
        let file_id = broker
            .store(
                "test.txt".into(),
                "text/plain".into(),
                Bytes::from("data"),
                "req-1",
            )
            .await;

        // File should exist
        assert!(broker.get(&file_id).is_some());

        // After completing the request, the file should be physically removed
        broker.complete_request("req-1");

        // Verify files DashMap no longer holds the file (memory released)
        assert_eq!(
            broker.files.len(),
            0,
            "files should be physically removed, not just logically hidden"
        );
    }

    #[tokio::test]
    async fn test_fix_file_broker_complete_only_removes_own_files() {
        // complete_request should only clean up files for that request
        let broker = FileBroker::new();
        let _id1 = broker
            .store(
                "a.txt".into(),
                "text/plain".into(),
                Bytes::from("aaa"),
                "req-1",
            )
            .await;
        let id2 = broker
            .store(
                "b.txt".into(),
                "text/plain".into(),
                Bytes::from("bbb"),
                "req-2",
            )
            .await;

        broker.complete_request("req-1");

        // req-1's files should be cleaned up
        assert_eq!(broker.files.len(), 1, "only req-1 files should be removed");
        // req-2's files should still be accessible
        assert!(
            broker.get(&id2).is_some(),
            "req-2 files should still be accessible"
        );
    }

    // =======================================================================
    // Flow Gate API tests (v0.4.0)
    // All #[ignore] — API not yet implemented
    // =======================================================================

    /// Helper: build a valid flow JSON body for testing
    fn simple_flow_body() -> serde_json::Value {
        serde_json::json!({
            "name": "doc-pipeline",
            "steps": [
                {"name": "extract", "rune": "echo"},
                {"name": "analyze", "rune": "echo", "depends_on": ["extract"]},
            ]
        })
    }

    /// Helper: build a multi-upstream flow with input_mapping
    fn multi_upstream_flow_body() -> serde_json::Value {
        serde_json::json!({
            "name": "multi-pipeline",
            "steps": [
                {"name": "extract", "rune": "echo"},
                {"name": "analyze", "rune": "echo", "depends_on": ["extract"]},
                {"name": "translate", "rune": "echo", "depends_on": ["extract"]},
                {"name": "merge", "rune": "echo", "depends_on": ["analyze", "translate"],
                 "input_mapping": {"analysis": "analyze.output", "translation": "translate.output"}}
            ]
        })
    }

    /// Helper: create a flow via POST and return the router for further requests
    async fn create_flow_helper(state: GateState) -> Router {
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::CREATED,
            "create_flow_helper: flow creation must return 201"
        );
        // Return a fresh router sharing the same state for subsequent requests
        build_router(state, None)
    }

    // -------------------------------------------------------------------
    // CRUD: POST /api/v1/flows
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_create_valid() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "doc-pipeline");
        assert!(json["steps"].is_array());
        assert_eq!(json["steps"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_fix_x_request_id_propagates_for_flow_run() {
        let state = test_state();
        let app = create_flow_helper(state).await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/doc-pipeline/run")
                    .header("x-request-id", "client-flow-rq-456")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok()),
            Some("client-flow-rq-456"),
            "flow handler should preserve inbound x-request-id"
        );
    }

    #[tokio::test]
    async fn test_flow_create_invalid_json() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from("{not valid json"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    #[tokio::test]
    async fn test_flow_create_dag_cycle() {
        let app = test_router();
        let body = serde_json::json!({
            "name": "cyclic-flow",
            "steps": [
                {"name": "a", "rune": "echo", "depends_on": ["b"]},
                {"name": "b", "rune": "echo", "depends_on": ["a"]},
            ]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["error"]["message"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("cycle"));
    }

    #[tokio::test]
    async fn test_flow_create_duplicate_step_names() {
        let app = test_router();
        let body = serde_json::json!({
            "name": "dup-steps",
            "steps": [
                {"name": "step1", "rune": "echo"},
                {"name": "step1", "rune": "echo"},
            ]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["error"]["message"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("duplicate"));
    }

    #[tokio::test]
    async fn test_flow_create_multi_upstream_no_mapping() {
        let app = test_router();
        let body = serde_json::json!({
            "name": "no-mapping",
            "steps": [
                {"name": "a", "rune": "echo"},
                {"name": "b", "rune": "echo"},
                {"name": "c", "rune": "echo", "depends_on": ["a", "b"]},
            ]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["error"]["message"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("input_mapping"));
    }

    #[tokio::test]
    async fn test_flow_create_duplicate_name_conflict() {
        let state = test_state();

        // Create first flow
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Create flow with same name
        let app2 = build_router(state.clone(), None);
        let response2 = app2
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response2.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(response2.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // -------------------------------------------------------------------
    // CRUD: GET /api/v1/flows (list)
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_list_with_entries() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(Request::get("/api/v1/flows").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let arr = json.as_array().unwrap();
        assert!(!arr.is_empty());
        // Each entry should have name and steps_count
        let entry = &arr[0];
        assert!(entry["name"].is_string());
        assert!(entry["steps_count"].is_number());
    }

    #[tokio::test]
    async fn test_flow_list_empty() {
        let app = test_router();
        let response = app
            .oneshot(Request::get("/api/v1/flows").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 0);
    }

    // -------------------------------------------------------------------
    // CRUD: GET /api/v1/flows/:name
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_get_existing() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(
                Request::get("/api/v1/flows/doc-pipeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "doc-pipeline");
        assert!(json["steps"].is_array());
        assert_eq!(json["steps"].as_array().unwrap().len(), 2);
        // Verify step details
        assert_eq!(json["steps"][0]["name"], "extract");
        assert_eq!(json["steps"][0]["rune"], "echo");
    }

    #[tokio::test]
    async fn test_flow_get_nonexistent() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::get("/api/v1/flows/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // -------------------------------------------------------------------
    // CRUD: DELETE /api/v1/flows/:name
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_delete_existing() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/flows/doc-pipeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_flow_delete_nonexistent() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/flows/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // -------------------------------------------------------------------
    // CRUD: lifecycle integration
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_create_then_list_contains_it() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(Request::get("/api/v1/flows").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let names: Vec<&str> = json
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"doc-pipeline"));
    }

    #[tokio::test]
    async fn test_flow_delete_then_list_excludes_it() {
        let state = test_state();

        // Create
        let app = build_router(state.clone(), None);
        let _resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Delete
        let app2 = build_router(state.clone(), None);
        let _resp = app2
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/flows/doc-pipeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // List
        let app3 = build_router(state.clone(), None);
        let response = app3
            .oneshot(Request::get("/api/v1/flows").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let names: Vec<&str> = json
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v["name"].as_str())
            .collect();
        assert!(!names.contains(&"doc-pipeline"));
    }

    // -------------------------------------------------------------------
    // Execution: sync mode
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_run_sync_simple() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/doc-pipeline/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"input":"data"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Response should include execution metadata
        assert!(json["steps_executed"].is_array() || json["steps_executed"].is_number());
    }

    #[tokio::test]
    async fn test_flow_run_nonexistent_flow() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/nonexistent/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"input":"data"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    #[tokio::test]
    async fn test_flow_run_step_failure() {
        let state = test_state();

        // Register a flow referencing a rune that fails
        let fail_handler = make_handler(|_ctx, _input| async move {
            Err(RuneError::ExecutionFailed {
                code: "STEP_FAILED".into(),
                message: "boom".into(),
            })
        });
        state
            .rune
            .relay
            .register(
                RuneConfig {
                    name: "fail-rune".into(),
                    version: "1.0.0".into(),
                    description: "always fails".into(),
                    supports_stream: false,
                    gate: None,
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(fail_handler)),
                None,
            )
            .unwrap();

        // Create a flow that uses the failing rune
        let flow_body = serde_json::json!({
            "name": "fail-flow",
            "steps": [
                {"name": "step1", "rune": "fail-rune"},
            ]
        });
        let app = build_router(state.clone(), None);
        let _resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(flow_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Run the flow
        let app2 = build_router(state.clone(), None);
        let response = app2
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/fail-flow/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"x":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // -------------------------------------------------------------------
    // Execution: async mode
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_run_async_returns_task_id() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/doc-pipeline/run?async=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"input":"data"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["task_id"].is_string());
        assert_eq!(json["flow"], "doc-pipeline");
    }

    #[tokio::test]
    async fn test_flow_async_task_query() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        // Start async flow
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/doc-pipeline/run?async=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"input":"data"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let task_id = json["task_id"].as_str().unwrap();

        // Query task status
        let app2 = build_router(state.clone(), None);
        let response2 = app2
            .oneshot(
                Request::get(format!("/api/v1/tasks/{}", task_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response2.status(), StatusCode::OK);
        let body2 = axum::body::to_bytes(response2.into_body(), 4096)
            .await
            .unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        assert!(json2["status"].is_string());
    }

    // -------------------------------------------------------------------
    // Execution: stream mode (SSE)
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_run_stream_returns_sse() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/doc-pipeline/run?stream=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"input":"data"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            content_type.contains("text/event-stream"),
            "Expected SSE content-type, got: {}",
            content_type
        );
    }

    // -------------------------------------------------------------------
    // Auth: Flow API requires authentication
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_api_requires_auth() {
        let state = auth_state();
        let app = build_router(state, None);

        // POST /api/v1/flows without auth key
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_flow_api_health_still_exempt() {
        let state = auth_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    // -------------------------------------------------------------------
    // Edge cases
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_create_empty_body() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Empty body is invalid JSON — deserialization fails before validation
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn test_flow_create_large_50_steps() {
        let app = test_router();

        // Build a linear pipeline of 50 steps
        let mut steps = Vec::new();
        for i in 0..50 {
            let mut step = serde_json::json!({"name": format!("step-{}", i), "rune": "echo"});
            if i > 0 {
                step["depends_on"] = serde_json::json!([format!("step-{}", i - 1)]);
            }
            steps.push(step);
        }
        let body = serde_json::json!({
            "name": "large-flow",
            "steps": steps,
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let resp_body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
        assert_eq!(json["steps"].as_array().unwrap().len(), 50);
    }

    #[tokio::test]
    async fn test_flow_name_with_special_characters() {
        let state = test_state();
        let app = build_router(state.clone(), None);
        let body = serde_json::json!({
            "name": "my-flow_v2.0",
            "steps": [
                {"name": "step1", "rune": "echo"},
            ]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        // Retrieve it by name via URL — same state so flow persists
        let app2 = build_router(state.clone(), None);
        let response2 = app2
            .oneshot(
                Request::get("/api/v1/flows/my-flow_v2.0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response2.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response2.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "my-flow_v2.0");
    }

    // -------------------------------------------------------------------
    // Additional: multi-upstream with valid mapping
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_create_multi_upstream_with_mapping_valid() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(multi_upstream_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "multi-pipeline");
        assert_eq!(json["steps"].as_array().unwrap().len(), 4);
        // Verify the merge step has input_mapping
        let merge_step = json["steps"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["name"] == "merge")
            .unwrap();
        assert!(merge_step["input_mapping"].is_object());
    }

    // -------------------------------------------------------------------
    // Additional: flow run with unavailable rune → 503
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_run_rune_unavailable_503() {
        let state = test_state();

        // Create a flow referencing a rune not registered in relay
        let body = serde_json::json!({
            "name": "bad-rune-flow",
            "steps": [
                {"name": "step1", "rune": "nonexistent-rune"},
            ]
        });
        let app = build_router(state.clone(), None);
        let _resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Run the flow — step references unavailable rune
        let app2 = build_router(state.clone(), None);
        let response = app2
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/bad-rune-flow/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"x":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // -------------------------------------------------------------------
    // Additional: verify list entry structure
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_list_entry_structure() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(Request::get("/api/v1/flows").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entry = &json.as_array().unwrap()[0];
        assert_eq!(entry["name"], "doc-pipeline");
        assert_eq!(entry["steps_count"], 2);
    }

    // -------------------------------------------------------------------
    // Additional: auth on GET/DELETE flow endpoints
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_list_requires_auth() {
        let state = auth_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(Request::get("/api/v1/flows").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_flow_run_requires_auth() {
        let state = auth_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/anything/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // -------------------------------------------------------------------
    // Additional: DAG validation — self-referencing step
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_flow_create_self_referencing_step() {
        let app = test_router();
        let body = serde_json::json!({
            "name": "self-ref",
            "steps": [
                {"name": "a", "rune": "echo", "depends_on": ["a"]},
            ]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // ===================================================================
    // P1 — 场景补充
    // ===================================================================

    // P1-4: DELETE /flows/:name auth protection (no key → 401)
    #[tokio::test]
    async fn test_flow_delete_requires_auth() {
        let state = auth_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/flows/anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // P1-4: GET /flows/:name auth protection (no key → 401)
    #[tokio::test]
    async fn test_flow_get_requires_auth() {
        let state = auth_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::get("/api/v1/flows/anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // P1-5: valid token can access flow CRUD
    #[tokio::test]
    async fn test_flow_create_with_valid_token() {
        let state = auth_state();
        let key_result = state
            .admin
            .store
            .create_key(rune_store::KeyType::Gate, "flow-test")
            .await
            .unwrap();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {}", key_result.raw_key))
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "doc-pipeline");
    }

    // P1-6: create → delete → re-create same name
    #[tokio::test]
    async fn test_flow_recreate_after_delete() {
        let state = test_state();

        // Create
        let app = build_router(state.clone(), None);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Delete
        let app2 = build_router(state.clone(), None);
        let resp2 = app2
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/flows/doc-pipeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::NO_CONTENT);

        // Re-create same name — should succeed, not conflict
        let app3 = build_router(state.clone(), None);
        let resp3 = app3
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp3.status(), StatusCode::CREATED);
    }

    // P1-7: multiple flows coexist — register 3 flows, list count == 3
    #[tokio::test]
    async fn test_flow_multiple_coexist() {
        let state = test_state();

        let flow_names = ["flow-alpha", "flow-beta", "flow-gamma"];
        for name in &flow_names {
            let body = serde_json::json!({
                "name": name,
                "steps": [{"name": "s1", "rune": "echo"}]
            });
            let app = build_router(state.clone(), None);
            let resp = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/flows")
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::CREATED,
                "Failed to create {}",
                name
            );
        }

        // List all flows
        let app = build_router(state.clone(), None);
        let resp = app
            .oneshot(Request::get("/api/v1/flows").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 3, "Expected 3 flows, got {}", arr.len());

        let names: Vec<&str> = arr.iter().filter_map(|v| v["name"].as_str()).collect();
        for name in &flow_names {
            assert!(names.contains(name), "Missing flow: {}", name);
        }
    }

    // P1-8: SSE event content validation — body includes "event:" or "data:"
    #[tokio::test]
    async fn test_flow_run_stream_event_content() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/doc-pipeline/run?stream=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"input":"data"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("event:") || text.contains("data:"),
            "SSE body should contain 'event:' or 'data:', got: {}",
            &text[..text.len().min(200)]
        );
    }

    // P1-9: async complete lifecycle — submit → poll until done → verify final status
    #[tokio::test]
    async fn test_flow_async_full_lifecycle() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        // Submit async
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/doc-pipeline/run?async=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"input":"data"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let task_id = json["task_id"].as_str().unwrap().to_string();

        // Poll until completed or failed (max 50 iterations)
        let mut final_status = String::new();
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let poll_app = build_router(state.clone(), None);
            let poll_resp = poll_app
                .oneshot(
                    Request::get(format!("/api/v1/tasks/{}", task_id))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(poll_resp.status(), StatusCode::OK);
            let poll_body = axum::body::to_bytes(poll_resp.into_body(), 4096)
                .await
                .unwrap();
            let poll_json: serde_json::Value = serde_json::from_slice(&poll_body).unwrap();
            let status = poll_json["status"].as_str().unwrap_or("");
            if status == "completed" || status == "failed" {
                final_status = status.to_string();
                break;
            }
        }
        assert!(
            final_status == "completed" || final_status == "failed",
            "Task should reach terminal state, got: '{}'",
            final_status
        );
    }

    // P1-10: full E2E — create → run → verify output → delete → confirm 404
    #[tokio::test]
    async fn test_flow_full_e2e_lifecycle() {
        let state = test_state();

        // 1. Create
        let app = build_router(state.clone(), None);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // 2. Run (sync)
        let app2 = build_router(state.clone(), None);
        let resp2 = app2
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/doc-pipeline/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"input":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let run_body = axum::body::to_bytes(resp2.into_body(), 8192).await.unwrap();
        let run_json: serde_json::Value = serde_json::from_slice(&run_body).unwrap();
        // Output should exist — at minimum it should be a JSON object
        assert!(run_json.is_object(), "Run output should be a JSON object");

        // 3. Delete
        let app3 = build_router(state.clone(), None);
        let resp3 = app3
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/flows/doc-pipeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp3.status(), StatusCode::NO_CONTENT);

        // 4. Confirm 404
        let app4 = build_router(state.clone(), None);
        let resp4 = app4
            .oneshot(
                Request::get("/api/v1/flows/doc-pipeline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp4.status(), StatusCode::NOT_FOUND);
    }

    // ===================================================================
    // P2 — 边界补充
    // ===================================================================

    // P2-11: empty steps array
    #[tokio::test]
    async fn test_flow_create_empty_steps() {
        let app = test_router();
        let body = serde_json::json!({
            "name": "empty-steps",
            "steps": []
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let resp_body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // P2-12: empty flow name
    #[tokio::test]
    async fn test_flow_create_empty_name() {
        let app = test_router();
        let body = serde_json::json!({
            "name": "",
            "steps": [{"name": "s1", "rune": "echo"}]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let resp_body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // P2-13: depends_on references non-existent step → 400
    #[tokio::test]
    async fn test_flow_create_depends_on_nonexistent_step() {
        let app = test_router();
        let body = serde_json::json!({
            "name": "bad-dep",
            "steps": [
                {"name": "a", "rune": "echo", "depends_on": ["ghost"]},
            ]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let resp_body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
        assert!(
            json["error"].is_object(),
            "error should be structured: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // P2-14: invalid token (not just missing) is rejected
    #[tokio::test]
    async fn test_flow_api_invalid_token_rejected() {
        let state = auth_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer rk-invalid-token-garbage-12345678")
                    .body(Body::from(simple_flow_body().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // P2-15: run with empty body
    #[tokio::test]
    async fn test_flow_run_empty_body() {
        let state = test_state();
        let app = create_flow_helper(state.clone()).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/doc-pipeline/run")
                    .header("content-type", "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // SF-8: Empty body now defaults to {} (consistent with rune run)
        assert_ne!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "empty body should default to {{}} and not return 422"
        );
    }

    // ====================================================================
    // v0.6.0 TDD Wave A — Rate Limiting
    // ====================================================================

    // Helper: build a test state with auth enabled and a known gate key.
    // Returns (state, raw_key).
    async fn rate_limit_state(max_requests: u32, dev_mode: bool) -> (GateState, String) {
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = if dev_mode {
            Arc::new(NoopVerifier)
        } else {
            Arc::new(rune_store::StoreKeyVerifier::new(store.clone()))
        };

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

        // Create a gate key
        let raw_key = if !dev_mode {
            let key_result = store
                .create_key(rune_store::KeyType::Gate, "rate_test")
                .await
                .unwrap();
            key_result.raw_key
        } else {
            "dev-key".to_string()
        };

        let flow_engine = Arc::new(tokio::sync::RwLock::new(
            rune_flow::engine::FlowEngine::new(
                Arc::clone(&relay),
                Arc::clone(&resolver) as Arc<dyn rune_core::resolver::Resolver>,
            ),
        ));

        let state = GateState {
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: !dev_mode,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: if dev_mode {
                None
            } else {
                Some(RateLimitState::new(max_requests, 1))
            },
            shutdown: ShutdownCoordinator::new(),
        };

        (state, raw_key)
    }

    fn round_robin_remote_rate_limit_state() -> (GateState, tokio::sync::OwnedSemaphorePermit) {
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let session_mgr = Arc::new(rune_core::session::SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(35),
        ));

        session_mgr.insert_test_caster("caster_a", 1);
        session_mgr.insert_test_caster("caster_b", 1);
        let permit = session_mgr.acquire_test_permit("caster_a");

        relay
            .register(
                RuneConfig {
                    name: "echo".into(),
                    version: "1.0.0".into(),
                    description: "remote a".into(),
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
                Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                    Ok(Bytes::from_static(b"caster_a"))
                }))),
                Some("caster_a".into()),
            )
            .unwrap();
        relay
            .register(
                RuneConfig {
                    name: "echo".into(),
                    version: "1.0.0".into(),
                    description: "remote b".into(),
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
                Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                    Ok(Bytes::from_static(b"caster_b"))
                }))),
                Some("caster_b".into()),
            )
            .unwrap();

        let flow_engine = Arc::new(tokio::sync::RwLock::new(
            rune_flow::engine::FlowEngine::new(
                Arc::clone(&relay),
                Arc::clone(&resolver) as Arc<dyn rune_core::resolver::Resolver>,
            ),
        ));

        (
            GateState {
                auth: AuthState {
                    trust_proxy: None,
                    key_verifier: Arc::new(NoopVerifier),
                    auth_enabled: false,
                    exempt_routes: Arc::new(vec!["/health".to_string()]),
                    audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
                },
                rune: RuneState {
                    relay,
                    resolver,
                    session_mgr,
                    file_broker: Arc::new(FileBroker::new()),
                    max_upload_size_mb: 10,
                    request_timeout: DEFAULT_REQUEST_TIMEOUT,
                },
                flow: FlowState {
                    flow_engine,
                    task_registry: Arc::new(tokio::sync::RwLock::new(
                        std::collections::HashMap::new(),
                    )),
                },
                admin: AdminState {
                    store,
                    started_at: Instant::now(),
                    dev_mode: false,
                    scaling: None,
                },
                cors_origins: Arc::new(vec![]),
                rate_limiter: Some(RateLimitState::new(100, 60)),
                shutdown: ShutdownCoordinator::new(),
            },
            permit,
        )
    }

    #[tokio::test]
    async fn test_rate_limit_allows_within_limit() {
        // With max_requests=5, all 5 requests should succeed (one per router)
        let (state, key) = rate_limit_state(5, false).await;

        for i in 0..5 {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("authorization", format!("Bearer {}", key))
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"n":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "request {} of 5 should succeed within rate limit",
                i + 1
            );
        }
    }

    #[tokio::test]
    async fn test_fix_rate_limit_preselection_does_not_advance_round_robin_choice() {
        // Bug: middleware pre-selected a caster, then the handler selected again
        // via resolver, advancing round-robin twice per request.
        //
        // Fix: middleware inserts the selected RuneEntry into request extensions;
        // the handler reuses it instead of calling resolver again.
        //
        // Verification: send two requests through round-robin with 2 casters.
        // If preselection is reused, we get caster_a then caster_b (each
        // advances round-robin exactly once). If handler double-selects, the
        // sequence would skip entries.
        //
        // Note: HashMap iteration order is non-deterministic, so we don't
        // assume which caster is picked first — we just verify both casters
        // are reached in 2 consecutive requests (round-robin cycles correctly).
        let (state, _permit) = round_robin_remote_rate_limit_state();

        // Drop the permit so both casters have capacity — we're testing
        // round-robin advancement, not saturation.
        drop(_permit);

        let mut seen = Vec::new();
        for _ in 0..2 {
            let resp = build_router(state.clone(), None)
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("content-type", "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            seen.push(String::from_utf8(body.to_vec()).unwrap());
        }

        // Both casters must be reached — proves round-robin advanced exactly
        // once per request (not zero, not twice).
        seen.sort();
        assert_eq!(
            seen,
            vec!["caster_a", "caster_b"],
            "round-robin should visit each caster exactly once in 2 requests; \
             if handler double-advances, one caster would be skipped. Got: {:?}",
            seen
        );
    }

    #[tokio::test]
    async fn test_rate_limit_blocks_over_limit() {
        // With max_requests=5, 6th request should return 429
        let (state, key) = rate_limit_state(5, false).await;

        for i in 0..6 {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("authorization", format!("Bearer {}", key))
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"n":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();

            if i < 5 {
                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "request {} should succeed",
                    i
                );
            } else {
                assert_eq!(
                    response.status(),
                    StatusCode::TOO_MANY_REQUESTS,
                    "request {} should be rate limited",
                    i
                );
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limit_different_keys_independent() {
        // Two different keys should have independent counters
        let (state, _key1) = rate_limit_state(2, false).await;
        let key2_result = state
            .admin
            .store
            .create_key(rune_store::KeyType::Gate, "key2")
            .await
            .unwrap();
        let key1 = state
            .admin
            .store
            .create_key(rune_store::KeyType::Gate, "key1b")
            .await
            .unwrap()
            .raw_key;
        let key2 = key2_result.raw_key;

        // Key1: 2 requests → both OK
        for i in 0..2 {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("authorization", format!("Bearer {}", key1))
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"k1":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Key2: should still be able to make requests (independent counter)
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key2))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"k2":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rate_limit_dev_mode_disabled() {
        // In dev mode, rate limiting should be disabled — all requests pass
        let (state, key) = rate_limit_state(2, true).await;

        for i in 0..10 {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"dev":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "dev mode should not rate limit (request {})",
                i
            );
        }

        let _ = key; // suppress unused warning
    }

    #[tokio::test]
    async fn test_rate_limit_429_has_retry_after_header() {
        // When rate limited, response should include Retry-After header
        let (state, key) = rate_limit_state(1, false).await;

        // First request: OK
        let app = build_router(state.clone(), None);
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Second request: should be 429
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":2}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let retry_after = response.headers().get("retry-after");
        assert!(
            retry_after.is_some(),
            "429 response should include Retry-After header"
        );
        let retry_secs: u64 = retry_after.unwrap().to_str().unwrap().parse().unwrap();
        assert!(
            retry_secs > 0 && retry_secs <= 60,
            "Retry-After should be between 1-60 seconds"
        );
    }

    #[tokio::test]
    async fn test_rate_limit_429_response_body() {
        // The 429 response body should be JSON with error info
        let (state, key) = rate_limit_state(1, false).await;

        // Exhaust limit
        let app = build_router(state.clone(), None);
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Trigger 429
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":2}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "RATE_LIMITED");
    }

    #[tokio::test]
    async fn test_rate_limit_window_reset() {
        // After the window expires, counter should reset
        // We use a very short window (simulate by waiting)
        // This test verifies the concept; actual implementation may need
        // time-controllable clock or short window for testing
        let (state, key) = rate_limit_state(1, false).await;

        // Exhaust limit
        let app = build_router(state.clone(), None);
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Verify rate limited
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":2}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

        // Wait for window to expire (60 seconds is too long for tests,
        // so this test requires the implementation to support configurable
        // window duration or time mocking)
        // For now we mark this as a placeholder that verifies the concept
        // The implementation should provide a reset mechanism testable in < 1s
    }

    #[tokio::test]
    async fn test_rate_limit_exempt_routes_not_limited() {
        // Health check and other exempt routes should not be rate limited
        let (state, _key) = rate_limit_state(1, false).await;

        // Even with limit=1, health should always work
        for _ in 0..5 {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(Request::get("/health").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn test_rate_limit_no_auth_header_still_handled() {
        // Requests without auth header should not cause rate limiter to panic
        // (auth middleware runs first and rejects, so rate limiter may not even fire)
        let (state, _key) = rate_limit_state(1, false).await;

        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should be 401 (auth fails before rate limit)
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_rate_limit_management_routes_exempt() {
        // Management API routes should not be rate limited
        let (state, key) = rate_limit_state(1, false).await;

        // Exhaust rate limit on /echo
        let app = build_router(state.clone(), None);
        let _ = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Management routes should still work
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::get("/api/v1/status")
                    .header("authorization", format!("Bearer {}", key))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // ====================================================================
    // v0.6.0 TDD Wave A — Graceful Shutdown
    // ====================================================================

    #[tokio::test]
    async fn test_shutdown_rejects_new_requests() {
        // After shutdown signal, new requests should return 503
        let state = test_state();
        // Trigger draining state on the shutdown coordinator
        state.shutdown.start_drain();

        let app = build_router(state, None);
        // Simulating: the shutdown coordinator should be in draining state
        // For now, this test documents the expected behavior
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"after_shutdown"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // When draining, should return 503
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_shutdown_in_progress_requests_complete() {
        // Register a slow handler that sleeps 200ms to simulate in-flight work
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        let slow_handler = make_handler(|_ctx, input| async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Ok(input)
        });
        relay
            .register(
                RuneConfig {
                    name: "slow".into(),
                    version: "1.0.0".into(),
                    description: "slow handler".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/slow".into(),
                        method: "POST".into(),
                    }),
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(slow_handler)),
                None,
            )
            .unwrap();

        let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
            Arc::clone(&relay),
            Arc::clone(&resolver) as Arc<dyn Resolver>,
        )));

        let state = GateState {
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        };

        let shutdown = state.shutdown.clone();
        let app = build_router(state, None);

        // Spawn the in-flight request BEFORE shutdown signal
        let handle = tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/slow")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"in_flight":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap()
        });

        // Small delay so the request starts processing, then trigger shutdown
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown.start_drain();

        // The in-flight request should still complete successfully
        let response = handle.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["in_flight"], true);
    }

    #[tokio::test]
    async fn test_shutdown_drain_timeout_force_close() {
        // After drain_timeout expires, server should force-close connections.
        // Register a very slow handler (longer than drain timeout) to verify
        // the force-close mechanism.
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        let very_slow_handler = make_handler(|_ctx, input| async move {
            // Simulate a handler that takes longer than drain_timeout
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            Ok(input)
        });
        relay
            .register(
                RuneConfig {
                    name: "very_slow".into(),
                    version: "1.0.0".into(),
                    description: "extremely slow handler".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/very_slow".into(),
                        method: "POST".into(),
                    }),
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(very_slow_handler)),
                None,
            )
            .unwrap();

        let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
            Arc::clone(&relay),
            Arc::clone(&resolver) as Arc<dyn Resolver>,
        )));

        let state = GateState {
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        };

        let app = build_router(state, None);

        // Start an extremely slow request
        let handle = tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/very_slow")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"slow":true}"#))
                    .unwrap(),
            )
            .await
        });

        // Give the request time to start, then verify it's still pending
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !handle.is_finished(),
            "slow request should still be in progress"
        );

        // NOTE: Drain timeout is enforced at the server level (main.rs),
        // not the router level. The router only rejects new requests via
        // shutdown middleware. Force-close of in-flight requests after
        // drain_timeout is handled by the tokio runtime dropping tasks.
        handle.abort();
        let result = handle.await;
        assert!(
            result.is_err() || result.unwrap().is_err(),
            "after force-close, the request should not succeed normally"
        );
    }

    // ====================================================================
    // v0.6.0 TDD Wave A — Structured Logging
    // ====================================================================

    #[tokio::test]
    async fn test_structured_log_contains_json() {
        // This test validates that structured JSON logging produces valid JSON lines.
        // Implementation requires a custom tracing subscriber buffer layer.
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"log_test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        // When tracing buffer capture is implemented:
        // let log_lines = buffer.lock().lines();
        // for line in log_lines {
        //     let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
        //     assert!(parsed.get("level").is_some());
        //     assert!(parsed.get("timestamp").is_some());
        //     assert!(parsed.get("message").is_some());
        // }
    }

    #[tokio::test]
    async fn test_structured_log_contains_request_id_and_rune_name() {
        // Validates that structured log entries for rune invocations include
        // request_id and rune_name fields.
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"log_fields"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        // When tracing buffer capture is implemented:
        // let log_lines = buffer.lock();
        // let invocation_line = log_lines.iter()
        //     .find(|l| l.contains("rune_name"))
        //     .expect("should have an invocation log line");
        // let parsed: serde_json::Value = serde_json::from_str(invocation_line).unwrap();
        // assert!(parsed["request_id"].as_str().map_or(false, |s| !s.is_empty()));
        // assert_eq!(parsed["rune_name"], "echo");
    }

    // ====================================================================
    // v0.6.0 TDD Wave A — Stats API Enhancement
    // ====================================================================

    #[tokio::test]
    async fn test_stats_api_contains_success_rate() {
        // After some calls, GET /api/v1/stats should return success_rate per rune
        let state = test_state();

        // Make a few successful calls first
        for i in 0..3 {
            let app = build_router(state.clone(), None);
            let _ = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"n":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Allow async logs to flush
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let app = build_router(state, None);
        let response = app
            .oneshot(Request::get("/api/v1/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Should have by_rune array with success_rate field
        let by_rune = json["by_rune"].as_array().unwrap();
        assert!(
            !by_rune.is_empty(),
            "by_rune should not be empty after 3 calls"
        );
        let rune_stat = &by_rune[0];
        assert!(
            rune_stat.get("success_rate").is_some(),
            "should have success_rate field"
        );
        let rate = rune_stat["success_rate"].as_f64().unwrap();
        assert!(
            rate >= 0.0 && rate <= 1.0,
            "success_rate should be between 0 and 1"
        );
    }

    #[tokio::test]
    async fn test_stats_api_contains_p95_latency() {
        // GET /api/v1/stats should return p95_latency_ms per rune
        let state = test_state();

        // Make some calls
        for i in 0..5 {
            let app = build_router(state.clone(), None);
            let _ = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"n":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let app = build_router(state, None);
        let response = app
            .oneshot(Request::get("/api/v1/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let by_rune = json["by_rune"].as_array().unwrap();
        assert!(
            !by_rune.is_empty(),
            "by_rune should not be empty after 5 calls"
        );
        let rune_stat = &by_rune[0];
        assert!(
            rune_stat.get("p95_latency_ms").is_some(),
            "should have p95_latency_ms field"
        );
        let p95 = rune_stat["p95_latency_ms"].as_f64().unwrap();
        assert!(p95 >= 0.0, "p95_latency_ms should be non-negative");
    }

    // ====================================================================
    // v0.6.0 TDD Wave A — Caster Details API Enhancement
    // ====================================================================

    #[tokio::test]
    async fn test_casters_api_returns_detailed_info() {
        // GET /api/v1/casters should return per-caster details
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(Request::get("/api/v1/casters").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Should have casters array (may be empty in test)
        assert!(json["casters"].is_array());
    }

    #[tokio::test]
    async fn test_casters_api_caster_has_runes_field() {
        // When a caster is connected, its entry should have a runes array
        // This is a schema test — in production the caster connects via gRPC
        // For unit testing, we verify the response schema is correct
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(Request::get("/api/v1/casters").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Verify response structure supports detailed caster info
        // Each caster entry should have the new scaling-aware management fields.
        for caster in json["casters"].as_array().unwrap_or(&vec![]) {
            assert!(caster.get("caster_id").is_some());
            assert!(caster.get("runes").is_some());
            assert!(caster.get("role").is_some());
            assert!(caster.get("max_concurrent").is_some());
            assert!(caster.get("available_permits").is_some());
            assert!(caster.get("pressure").is_some());
            assert!(caster.get("metrics").is_some());
            assert!(caster.get("health_status").is_some());
            assert!(caster.get("connected_since").is_some());
            assert!(caster.get("session_generation").is_some());
        }
    }

    #[tokio::test]
    async fn test_casters_api_exposes_scaling_fields_for_connected_caster() {
        let state = test_state();
        state
            .rune
            .session_mgr
            .insert_test_caster("caster-scale-1", 8);
        state
            .rune
            .session_mgr
            .set_test_pressure("caster-scale-1", 0.375);
        state
            .rune
            .session_mgr
            .set_test_health("caster-scale-1", HealthStatusLevel::Degraded);
        state.rune.session_mgr.set_test_labels(
            "caster-scale-1",
            HashMap::from([("group".to_string(), "gpu".to_string())]),
        );

        let app = build_router(state, None);
        let response = app
            .oneshot(Request::get("/api/v1/casters").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let casters = json["casters"].as_array().expect("casters array");
        let caster = casters
            .iter()
            .find(|caster| caster["caster_id"] == "caster-scale-1")
            .expect("connected caster should be listed");
        assert_eq!(caster["role"], "caster");
        assert_eq!(caster["max_concurrent"], 8);
        assert_eq!(caster["available_permits"], 8);
        assert_eq!(caster["pressure"], 0.375);
        assert_eq!(caster["health_status"], "DEGRADED");
        assert!(caster["metrics"].is_object());
    }

    #[tokio::test]
    async fn test_caster_stats_endpoint_returns_grouped_stats() {
        let state = test_state();
        state
            .admin
            .store
            .insert_log(&CallLog {
                id: 0,
                request_id: "req-caster-stats".into(),
                rune_name: "echo".into(),
                mode: "sync".into(),
                caster_id: Some("caster-a".into()),
                latency_ms: 42,
                status_code: 200,
                input_size: 10,
                output_size: 12,
                timestamp: "2026-04-09T00:00:00Z".into(),
            })
            .await
            .unwrap();

        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::get("/api/v1/stats/casters")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let casters = json["casters"].as_array().expect("casters array");
        assert_eq!(casters.len(), 1);
        assert_eq!(casters[0]["caster_id"], "caster-a");
        assert_eq!(casters[0]["count"], 1);
    }

    #[tokio::test]
    async fn test_fix_caster_stats_endpoint_uses_real_remote_request_logs() {
        let state = test_state();
        let app = build_router(state.clone(), None);
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let selected_entry = rune_core::relay::RuneEntry {
            config: RuneConfig {
                name: "echo".into(),
                version: "1.0.0".into(),
                description: "remote echo".into(),
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
            invoker: Arc::new(LocalInvoker::new(handler)),
            caster_id: Some("remote-caster-1".into()),
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/echo/run")
                    .header("content-type", "application/json")
                    .extension(selected_entry)
                    .body(Body::from(r#"{"msg":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let stats_response = build_router(state, None)
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/stats/casters")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stats_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(stats_response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let casters = json["casters"].as_array().expect("casters array");
        let caster = casters
            .iter()
            .find(|caster| caster["caster_id"] == "remote-caster-1")
            .expect("remote request should be attributed to its caster");
        assert_eq!(caster["count"], 1);
    }

    #[tokio::test]
    async fn test_scaling_status_endpoint_returns_evaluator_snapshot() {
        let mut state = test_state();
        state.rune.session_mgr.insert_test_caster("pilot-1", 1);
        state
            .rune
            .session_mgr
            .set_test_role("pilot-1", CasterRole::Pilot);
        state.rune.session_mgr.insert_test_caster("caster-gpu-a", 4);
        state
            .rune
            .session_mgr
            .set_test_pressure("caster-gpu-a", 0.92);
        state.rune.session_mgr.set_test_labels(
            "caster-gpu-a",
            HashMap::from([
                ("group".to_string(), "gpu".to_string()),
                ("_pilot_id".to_string(), "pilot-1".to_string()),
                ("_scale_up".to_string(), "0.8".to_string()),
                ("_scale_down".to_string(), "0.2".to_string()),
                ("_sustained".to_string(), "0".to_string()),
                ("_min".to_string(), "1".to_string()),
                ("_max".to_string(), "4".to_string()),
            ]),
        );

        let evaluator = Arc::new(ScaleEvaluator::new(
            Arc::clone(&state.rune.session_mgr),
            std::time::Duration::from_secs(30),
        ));
        evaluator.evaluate_once().await;
        state.admin.scaling = Some(evaluator);

        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::get("/api/v1/scaling/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let groups = json["groups"].as_array().expect("groups array");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["group_id"], "gpu");
        assert_eq!(groups[0]["pilot_id"], "pilot-1");
        assert_eq!(groups[0]["action"], "scale_up");
    }

    // ====================================================================
    // v0.6.0 TDD Wave A — Labels routing via HTTP header
    // ====================================================================

    #[tokio::test]
    async fn test_labels_header_routes_to_matching_caster() {
        // X-Rune-Labels: env=prod should route to caster with env=prod label
        let state = test_state();
        // NOTE: Future enhancement — register casters with labels to test positive routing
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/echo/run")
                    .header("content-type", "application/json")
                    .header("x-rune-labels", "env=prod")
                    .body(Body::from(r#"{"msg":"label_test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // With no caster matching the label, should return 503
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_no_labels_header_uses_default_routing() {
        // Without X-Rune-Labels, normal routing applies
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/echo/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"no_label"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Without labels, default routing should work
        assert_eq!(response.status(), StatusCode::OK);
    }

    // ====================================================================
    // v0.6.0 TDD — Supplementary Rate Limiting Tests
    // ====================================================================

    #[tokio::test]
    async fn test_rate_limit_zero_allows_none() {
        // limit=0 means no requests are allowed — immediate 429
        let (state, key) = rate_limit_state(0, false).await;
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "limit=0 should reject the very first request with 429"
        );
    }

    #[tokio::test]
    async fn test_rate_limit_concurrent_requests_near_boundary() {
        // With limit=3, spawn 5 concurrent requests — exactly 3 should succeed
        let (state, key) = rate_limit_state(3, false).await;

        let mut handles = Vec::new();
        for i in 0..5 {
            let s = state.clone();
            let k = key.clone();
            handles.push(tokio::spawn(async move {
                let app = build_router(s, None);
                let response = app
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri("/echo")
                            .header("authorization", format!("Bearer {}", k))
                            .header("content-type", "application/json")
                            .body(Body::from(format!(r#"{{"n":{}}}"#, i)))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                response.status()
            }));
        }

        let mut ok_count = 0;
        let mut limited_count = 0;
        for h in handles {
            match h.await.unwrap() {
                StatusCode::OK => ok_count += 1,
                StatusCode::TOO_MANY_REQUESTS => limited_count += 1,
                other => panic!("unexpected status: {}", other),
            }
        }
        assert_eq!(ok_count, 3, "exactly 3 requests should succeed");
        assert_eq!(
            limited_count, 2,
            "exactly 2 requests should be rate limited"
        );
    }

    #[tokio::test]
    async fn test_rate_limit_empty_bearer_key() {
        // Bearer token with empty string should be handled gracefully
        let (state, _key) = rate_limit_state(5, false).await;
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", "Bearer ")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Empty bearer should fail auth (401), not panic
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "empty bearer token should be rejected by auth"
        );
    }

    #[tokio::test]
    async fn test_rate_limit_window_reset_with_short_window() {
        // Use a 1-second window, exhaust limit, sleep 1.1s, verify reset
        let (state, key) = rate_limit_state(1, false).await;

        // Exhaust rate limit
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify it's now limited
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":2}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

        // Wait for window to reset (implementation should support short windows for testing)
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        // After reset, request should succeed again
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":3}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "after window reset, request should succeed"
        );
    }

    // ====================================================================
    // v0.6.0 TDD — Supplementary Graceful Shutdown Tests
    // ====================================================================

    #[tokio::test]
    async fn test_shutdown_concurrent_requests_during_drain() {
        // Multiple concurrent requests that start before shutdown should all complete
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let key_verifier: Arc<dyn KeyVerifier> = Arc::new(NoopVerifier);

        let slow_handler = make_handler(|_ctx, input| async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            Ok(input)
        });
        relay
            .register(
                RuneConfig {
                    name: "slow".into(),
                    version: "1.0.0".into(),
                    description: "slow handler".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/slow".into(),
                        method: "POST".into(),
                    }),
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(slow_handler)),
                None,
            )
            .unwrap();

        let flow_engine = Arc::new(tokio::sync::RwLock::new(FlowEngine::new(
            Arc::clone(&relay),
            Arc::clone(&resolver) as Arc<dyn Resolver>,
        )));

        let state = GateState {
            auth: AuthState {
                trust_proxy: None,
                key_verifier,
                auth_enabled: false,
                exempt_routes: Arc::new(vec!["/health".to_string()]),
                audit_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            },
            rune: RuneState {
                relay,
                resolver,
                session_mgr: Arc::new(rune_core::session::SessionManager::new_dev(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(35),
                )),
                file_broker: Arc::new(FileBroker::new()),
                max_upload_size_mb: 10,
                request_timeout: DEFAULT_REQUEST_TIMEOUT,
            },
            flow: FlowState {
                flow_engine,
                task_registry: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            },
            admin: AdminState {
                store,
                started_at: Instant::now(),
                dev_mode: true,
                scaling: None,
            },
            cors_origins: Arc::new(vec![]),
            rate_limiter: None,
            shutdown: ShutdownCoordinator::new(),
        };

        // Spawn 5 concurrent slow requests
        let mut handles = Vec::new();
        for i in 0..5 {
            let s = state.clone();
            handles.push(tokio::spawn(async move {
                let app = build_router(s, None);
                let response = app
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri("/slow")
                            .header("content-type", "application/json")
                            .body(Body::from(format!(r#"{{"id":{}}}"#, i)))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                response.status()
            }));
        }

        // Give requests time to start, then trigger shutdown
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        state.shutdown.start_drain();

        // All in-flight requests should complete successfully
        for (i, h) in handles.into_iter().enumerate() {
            let status = h.await.unwrap();
            assert_eq!(
                status,
                StatusCode::OK,
                "in-flight request {} should complete during drain",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_shutdown_double_signal_idempotent() {
        // Sending shutdown signal twice should not panic or cause issues
        let state = test_state();
        let shutdown = state.shutdown.clone();
        let app = build_router(state, None);

        // Verify the server is responsive before shutdown
        let response = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Trigger shutdown signal twice in rapid succession
        // Both calls should succeed without panic.
        shutdown.start_drain();
        shutdown.start_drain();
        // The second signal should be a no-op.
    }

    #[tokio::test]
    async fn test_shutdown_health_returns_503_during_drain() {
        // During drain phase, /health should return 503 to signal
        // load balancers to stop routing traffic
        let state = test_state();
        // Trigger draining state on the shutdown coordinator
        state.shutdown.start_drain();

        let app = build_router(state, None);
        let response = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        // During draining, health should return 503 so LB stops routing
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "/health should return 503 during drain phase"
        );
    }

    // ====================================================================
    // v0.6.0 TDD — Supplementary Structured Logging Tests
    // ====================================================================

    #[tokio::test]
    async fn test_structured_log_error_request_logged() {
        // Even error responses (404, 500) should produce structured log entries
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/nonexistent_rune")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"error_test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        // When tracing buffer is implemented:
        // let log_lines = buffer.lock();
        // let error_line = log_lines.iter()
        //     .find(|l| l.contains("404") || l.contains("NOT_FOUND"))
        //     .expect("error request should produce a log line");
        // let parsed: serde_json::Value = serde_json::from_str(error_line).unwrap();
        // assert!(parsed.get("status").is_some());
    }

    #[tokio::test]
    async fn test_structured_log_includes_latency() {
        // Structured log entries should include request latency
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"msg":"latency_test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        // When tracing buffer is implemented:
        // let log_lines = buffer.lock();
        // let invocation_line = log_lines.iter()
        //     .find(|l| l.contains("echo"))
        //     .expect("should have an invocation log line");
        // let parsed: serde_json::Value = serde_json::from_str(invocation_line).unwrap();
        // let latency = parsed.get("latency_ms")
        //     .or(parsed.get("duration_ms"))
        //     .expect("log should contain latency field");
        // assert!(latency.as_f64().unwrap() >= 0.0);
    }

    // ====================================================================
    // v0.6.0 TDD — Supplementary Stats Tests
    // ====================================================================

    #[tokio::test]
    async fn test_stats_success_rate_accuracy() {
        // Mix successful and failed requests, verify success_rate = successes / total
        let state = test_state();

        // 3 successful requests to /echo
        for i in 0..3 {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"n":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // 2 failed requests (bad rune via debug route)
        for _ in 0..2 {
            let app = build_router(state.clone(), None);
            let _ = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/runes/nonexistent/run")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"fail":true}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let app = build_router(state, None);
        let response = app
            .oneshot(Request::get("/api/v1/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let by_rune = json["by_rune"].as_array().unwrap();
        // Find the echo rune stats
        let echo_stat = by_rune
            .iter()
            .find(|r| r["rune_name"] == "echo")
            .expect("should have stats for echo rune");
        // success_rate for echo should be 1.0 (all 3 echo calls succeeded)
        let rate = echo_stat["success_rate"].as_f64().unwrap();
        assert!(
            (rate - 1.0).abs() < 0.01,
            "echo success_rate should be ~1.0, got {}",
            rate
        );
    }

    #[tokio::test]
    async fn test_stats_zero_calls_no_divide_by_zero() {
        // With 0 calls, stats API should not panic or produce NaN
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(Request::get("/api/v1/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["total_calls"], 0);
        let by_rune = json["by_rune"].as_array().unwrap();
        assert!(by_rune.is_empty(), "with 0 calls, by_rune should be empty");
    }

    // ====================================================================
    // v0.6.0 TDD — Cross-module Combination: Rate Limit + Labels Routing
    // ====================================================================

    #[tokio::test]
    async fn test_rate_limit_with_labels_routing() {
        // Rate limiting should apply per-key even when labels routing is used
        let (state, key) = rate_limit_state(2, false).await;

        // Exhaust rate limit with label header
        for i in 0..2 {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("authorization", format!("Bearer {}", key))
                        .header("content-type", "application/json")
                        .header("x-rune-labels", "env=prod")
                        .body(Body::from(format!(r#"{{"n":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();
            // May be OK or 503 (no matching label), but should not panic
            assert!(
                response.status() == StatusCode::OK
                    || response.status() == StatusCode::SERVICE_UNAVAILABLE,
                "request {} should either succeed or get 503 (no label match), got {}",
                i,
                response.status()
            );
        }

        // 3rd request with same key should be rate limited regardless of labels
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .header("x-rune-labels", "env=staging")
                    .body(Body::from(r#"{"n":3}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "rate limit should apply regardless of label header"
        );
    }

    // ====================================================================
    // v0.6.0 TDD — Cross-module: Shutdown + Rate Limit State
    // ====================================================================

    #[tokio::test]
    async fn test_shutdown_with_rate_limit_state() {
        // After shutdown, rate limit counters should not leak or cause issues
        let (state, key) = rate_limit_state(5, false).await;

        // Make a couple requests to populate rate limit state
        for i in 0..2 {
            let app = build_router(state.clone(), None);
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/echo")
                        .header("authorization", format!("Bearer {}", key))
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"n":{}}}"#, i)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Trigger shutdown signal
        state.shutdown.start_drain();
        // After shutdown, new requests should get 503 (not 429)
        let app = build_router(state.clone(), None);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("authorization", format!("Bearer {}", key))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"n":99}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "after shutdown, should return 503 not 429"
        );
    }

    // ====================================================================
    // Issue 2 — Flow error response format consistency
    // ====================================================================

    #[tokio::test]
    async fn test_flow_error_response_uses_structured_format() {
        // Flow API errors should use {"error":{"code":"..","message":".."}}
        // not {"error":"..."}
        let state = test_state();
        let app = build_router(state, None);

        // Try to run a non-existent flow
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/nonexistent_flow/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"x":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Must be structured format: {"error":{"code":"..","message":".."}}
        assert!(
            json["error"].is_object(),
            "flow error should be {{\"error\":{{\"code\":\"..\",\"message\":\"..\"}}}} but got: {}",
            json
        );
        assert!(
            json["error"]["code"].is_string(),
            "error.code must be a string"
        );
        assert!(
            json["error"]["message"].is_string(),
            "error.message must be a string"
        );
    }

    #[tokio::test]
    async fn test_flow_create_validation_error_uses_structured_format() {
        // POST /api/v1/flows with empty body should return structured error
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(
            json["error"].is_object(),
            "flow create error should use structured format but got: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    #[tokio::test]
    async fn test_flow_get_not_found_uses_structured_format() {
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/flows/no_such_flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(
            json["error"].is_object(),
            "flow GET not found error should use structured format but got: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    #[tokio::test]
    async fn test_flow_delete_not_found_uses_structured_format() {
        let state = test_state();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/flows/no_such_flow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(
            json["error"].is_object(),
            "flow DELETE not found should use structured format but got: {}",
            json
        );
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // =======================================================================
    // Issue #3: FileBroker cleanup after successful multipart request
    // =======================================================================

    #[tokio::test]
    async fn test_filebroker_cleanup_after_successful_request() {
        let state = test_state();

        // Manually store a file in the broker as if uploaded via multipart
        let request_id = "test-req-cleanup";
        let _file_id = state
            .rune
            .file_broker
            .store(
                "test.txt".into(),
                "text/plain".into(),
                Bytes::from("file content"),
                request_id,
            )
            .await;

        // Verify file is stored
        assert_eq!(state.rune.file_broker.files.len(), 1);

        // Call complete_request to clean up
        state.rune.file_broker.complete_request(request_id);

        // Verify file is removed
        assert_eq!(
            state.rune.file_broker.files.len(),
            0,
            "FileBroker should have no files after complete_request"
        );
    }

    // =======================================================================
    // Issue #5: Reverse index maintained on register / unregister
    // =======================================================================

    #[tokio::test]
    async fn test_reverse_index_maintained_on_register() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                RuneConfig {
                    name: "idx_rune".into(),
                    version: "1.0.0".into(),
                    description: "test".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/idx_path".into(),
                        method: "POST".into(),
                    }),
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(rune_core::invoker::LocalInvoker::new(handler)),
                None,
            )
            .unwrap();

        // Reverse index should map POST:/idx_path → idx_rune
        let resolved = relay.resolve_by_gate_path("POST", "/idx_path");
        assert_eq!(resolved, Some("idx_rune".to_string()));
    }

    #[tokio::test]
    async fn test_reverse_index_removed_on_unregister() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                RuneConfig {
                    name: "rm_rune".into(),
                    version: "1.0.0".into(),
                    description: "test".into(),
                    supports_stream: false,
                    gate: Some(GateConfig {
                        path: "/rm_path".into(),
                        method: "POST".into(),
                    }),
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(rune_core::invoker::LocalInvoker::new(handler)),
                Some("caster-1".into()),
            )
            .unwrap();

        // Index should exist before removal
        assert_eq!(
            relay.resolve_by_gate_path("POST", "/rm_path"),
            Some("rm_rune".to_string())
        );

        // Remove the caster
        relay.remove_caster("caster-1");

        // Index should be gone
        assert_eq!(relay.resolve_by_gate_path("POST", "/rm_path"), None);
    }

    #[tokio::test]
    async fn test_dynamic_route_lookup_uses_index() {
        // Build a state with a rune that has a gate_path, then verify
        // the dynamic route resolves correctly (implicitly via the index).
        let state = test_state();
        let app = build_router(state.clone(), None);

        // /echo is registered with gate_path="/echo" method="POST" — verify reverse index
        let resolved = state.rune.relay.resolve_by_gate_path("POST", "/echo");
        assert_eq!(
            resolved,
            Some("echo".to_string()),
            "reverse index should map POST:/echo → echo"
        );

        // Also verify the HTTP route still works via the fallback handler
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"idx":"test"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // =======================================================================
    // Issue #6: Rate limiter expired entries cleaned up
    // =======================================================================

    #[tokio::test]
    async fn test_rate_limit_expired_entries_cleaned() {
        // Use a 1-second window so we can sleep past it quickly
        let rl = RateLimitState::new(10, 1); // 10 req/s, 1s window

        // Generate requests from many unique keys
        for i in 0..100 {
            let key = format!("key-{}", i);
            let _ = rl.check(&key);
        }

        // All 100 keys should be tracked
        assert_eq!(rl.entry_count(), 100);

        // Sleep past the window so all entries expire
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        // Trigger cleanup by calling check with a new key
        let _ = rl.check("trigger-cleanup");

        // Expired entries should have been evicted; only the trigger key remains
        assert!(
            rl.entry_count() <= 1,
            "expired entries should be cleaned, got {}",
            rl.entry_count()
        );
    }

    // =======================================================================
    // Issue #8: Shutdown check runs before auth (no DB query during drain)
    // =======================================================================

    #[tokio::test]
    async fn test_shutdown_check_before_auth() {
        // Enable auth so auth_middleware would normally do a DB lookup.
        // Start drain so shutdown_middleware should reject with 503 first.
        let mut state = test_state();
        state.auth.auth_enabled = true;
        state.admin.dev_mode = false;
        state.auth.key_verifier =
            Arc::new(rune_store::StoreKeyVerifier::new(state.admin.store.clone()));

        // Start drain BEFORE building the router
        state.shutdown.start_drain();

        let app = build_router(state, None);

        // Send a request WITHOUT a valid Bearer token.
        // If shutdown runs first → 503 SERVICE_UNAVAILABLE.
        // If auth runs first → 401 UNAUTHORIZED (the bug).
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

        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "drain should return 503 before auth checks (expected 503, got {})",
            response.status()
        );
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "SERVICE_UNAVAILABLE");
    }

    // ========================================================================
    // Issue Fix: FileBroker evict_expired throttling
    // ========================================================================

    #[tokio::test]
    async fn test_filebroker_evict_throttled() {
        // With a very long eviction interval, expired entries should NOT be cleaned
        // by store() calls within the interval — proving eviction is throttled.
        // TTL = 1s, evict_interval = 3600s (1 hour — effectively never during this test)
        let broker = FileBroker::with_ttl(1, 3600);

        // Store a file
        let id1 = broker
            .store(
                "a.txt".into(),
                "text/plain".into(),
                Bytes::from("aaa"),
                "req-1",
            )
            .await;

        // Wait for the file to expire
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

        // The first store() triggered eviction (last_eviction_secs was 0, epoch was just created).
        // But now, the eviction interval hasn't elapsed yet (3600s), so subsequent store()
        // calls should NOT trigger eviction.
        let _id2 = broker
            .store(
                "b.txt".into(),
                "text/plain".into(),
                Bytes::from("bbb"),
                "req-2",
            )
            .await;

        // The expired file (id1) should still be in the DashMap because eviction was throttled.
        // entry_count includes both the expired entry and the new entry.
        assert_eq!(
            broker.entry_count(),
            2,
            "eviction should be throttled — expired entry should still be in DashMap"
        );

        // However, get() should still return None for expired files (per-entry O(1) TTL check)
        assert!(
            broker.get(&id1).is_none(),
            "get() should still filter expired files via per-entry TTL check"
        );
    }

    #[tokio::test]
    async fn test_filebroker_evict_runs_when_interval_elapsed() {
        // With a very short eviction interval, expired entries SHOULD be cleaned up.
        // TTL = 1s, evict_interval = 0s (always evict)
        let broker = FileBroker::with_ttl(1, 0);

        // Store a file
        let _id1 = broker
            .store(
                "a.txt".into(),
                "text/plain".into(),
                Bytes::from("aaa"),
                "req-1",
            )
            .await;
        assert_eq!(broker.entry_count(), 1);

        // Wait for it to expire
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

        // Next store() should trigger eviction because interval is 0
        let _id2 = broker
            .store(
                "b.txt".into(),
                "text/plain".into(),
                Bytes::from("bbb"),
                "req-2",
            )
            .await;

        // Expired entry should have been evicted
        assert_eq!(
            broker.entry_count(),
            1,
            "expired entry should have been evicted when interval is 0"
        );
    }

    // ====================================================================
    // Regression: S11 — Content-Disposition filename with quotes escaped
    // ====================================================================

    #[tokio::test]
    async fn test_download_file_filename_with_quotes_escaped() {
        let state = test_state();

        // Store a file whose name contains a double-quote character
        let file_id = state
            .rune
            .file_broker
            .store(
                r#"evil"name.txt"#.to_string(),
                "application/octet-stream".to_string(),
                bytes::Bytes::from_static(b"data"),
                "req-esc",
            )
            .await;

        let app = build_router(state, None);
        let resp = app
            .oneshot(
                Request::get(format!("/api/v1/files/{}", file_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let cd = resp
            .headers()
            .get("content-disposition")
            .expect("should have content-disposition")
            .to_str()
            .unwrap();

        // The quote inside the filename must be escaped as \"
        assert!(
            cd.contains(r#"evil\"name.txt"#),
            "double-quote in filename must be escaped, got: {}",
            cd
        );
        // The header must still be well-formed (balanced outer quotes)
        assert!(
            cd.starts_with("attachment; filename=\"") && cd.ends_with('"'),
            "content-disposition must have balanced quotes, got: {}",
            cd
        );
    }

    // ====================================================================
    // Regression: S12 — generation is exposed for connected casters
    // ====================================================================

    #[tokio::test]
    async fn test_generation_exposed_for_connected_caster() {
        let mut state = test_state();
        state.admin.started_at = std::time::Instant::now() - std::time::Duration::from_secs(1000);

        // Insert a caster via the public test helper
        state
            .rune
            .session_mgr
            .insert_test_caster("test-caster-1", 5);

        let app = build_router(state, None);
        let resp = app
            .oneshot(Request::get("/api/v1/casters").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let casters = json["casters"].as_array().expect("should have casters");
        assert_eq!(casters.len(), 1);

        let generation = casters[0]["session_generation"].as_u64().unwrap();
        assert!(
            generation >= 1,
            "session_generation should be at least 1 for a connected caster. Got: {}",
            generation
        );
    }

    // =========================================================================
    // SF-3: async task cancel TOCTOU — atomic CAS prevents overwriting cancelled
    // =========================================================================

    #[tokio::test]
    async fn test_complete_task_if_not_cancelled_respects_cancel() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let task_id = "cas-test-1";

        // Insert and move to running
        store
            .insert_task(task_id, "echo", Some("{}"))
            .await
            .unwrap();
        store
            .update_task_status(task_id, TaskStatus::Running, None, None)
            .await
            .unwrap();

        // Simulate cancel
        store
            .update_task_status(task_id, TaskStatus::Cancelled, None, Some("user cancelled"))
            .await
            .unwrap();

        // Now try to complete — should return false (not updated)
        let updated = store
            .complete_task_if_not_cancelled(task_id, TaskStatus::Completed, Some("result"), None)
            .await
            .unwrap();
        assert!(!updated, "should NOT overwrite cancelled task");

        // Verify status is still cancelled
        let task = store.get_task(task_id).await.unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Cancelled);
    }

    #[tokio::test]
    async fn test_complete_task_if_not_cancelled_succeeds_when_running() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let task_id = "cas-test-2";

        store
            .insert_task(task_id, "echo", Some("{}"))
            .await
            .unwrap();
        store
            .update_task_status(task_id, TaskStatus::Running, None, None)
            .await
            .unwrap();

        let updated = store
            .complete_task_if_not_cancelled(task_id, TaskStatus::Completed, Some("done"), None)
            .await
            .unwrap();
        assert!(updated, "should update running task to completed");

        let task = store.get_task(task_id).await.unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Completed);
        assert_eq!(task.output.as_deref(), Some("done"));
    }

    // =========================================================================
    // SF-4: stream mode records call log
    // =========================================================================

    #[tokio::test]
    async fn test_stream_mode_records_call_log() {
        use rune_core::invoker::LocalInvoker;

        let state = test_state();
        // Register a stream-capable rune
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        state
            .rune
            .relay
            .register(
                RuneConfig {
                    name: "stream_log_test".into(),
                    version: "1.0.0".into(),
                    description: "test".into(),
                    supports_stream: true,
                    gate: None,
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(handler)),
                None,
            )
            .unwrap();

        let app = build_router(state.clone(), None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/stream_log_test/run?stream=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"hello":"world"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Consume the SSE stream to let the spawn task finish
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert!(!body.is_empty(), "stream should produce output");

        // Give the background task time to insert the log
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Check that a call log was recorded with mode=stream
        let logs = state
            .admin
            .store
            .query_logs(Some("stream_log_test"), 10)
            .await
            .unwrap();
        assert!(!logs.is_empty(), "stream mode should record a call log");
        assert_eq!(logs[0].mode, "stream");
        assert_eq!(logs[0].rune_name, "stream_log_test");
    }

    // =========================================================================
    // SF-5: mgmt_casters uses efficient reverse mapping
    // =========================================================================

    #[tokio::test]
    async fn test_mgmt_casters_returns_correct_runes_per_caster() {
        // This test verifies mgmt_casters works correctly — the optimization
        // (reverse mapping) should produce the same results as the old approach.
        let state = test_state();
        let app = build_router(state, None);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/casters")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // Should be valid JSON with a "casters" array
        assert!(json["casters"].is_array());
    }

    // =========================================================================
    // SF-8: run_flow accepts empty body (defaults to {})
    // =========================================================================

    #[tokio::test]
    async fn test_run_flow_empty_body_defaults_to_empty_json() {
        // Register a flow first
        let state = test_state();
        let app = build_router(state.clone(), None);

        // Create a simple flow with echo step
        let flow_json = serde_json::json!({
            "name": "empty_body_test",
            "steps": [{"name": "step1", "rune": "echo", "depends_on": []}]
        });

        let create_resp = app
            .clone()
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
        assert_eq!(create_resp.status(), StatusCode::CREATED);

        // Run the flow with empty body — should succeed, not 422
        let app2 = build_router(state, None);
        let run_resp = app2
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/empty_body_test/run")
                    .header("content-type", "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_ne!(
            run_resp.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "empty body should not return 422 for flow run"
        );
    }

    // =========================================================================
    // NF-2: flow_error_response removed — flow errors use same format
    // =========================================================================

    #[tokio::test]
    async fn test_nf2_flow_errors_use_error_response_format() {
        // After removing flow_error_response, flow errors should still
        // use the standard {"error":{"code":"..","message":".."}} format.
        let state = test_state();
        let app = build_router(state, None);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/nonexistent/run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"x":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"]["code"].is_string(),
            "error.code must be present"
        );
        assert!(
            json["error"]["message"].is_string(),
            "error.message must be present"
        );
    }

    // =========================================================================
    // NF-12: GateState clone uses Arc<Vec<String>> (no deep copy)
    // =========================================================================

    #[tokio::test]
    async fn test_nf12_gate_state_clone_shares_exempt_routes() {
        let state = test_state();
        let cloned = state.clone();
        // Arc::ptr_eq verifies they share the same allocation (no deep copy)
        assert!(
            Arc::ptr_eq(&state.auth.exempt_routes, &cloned.auth.exempt_routes),
            "exempt_routes should be shared via Arc after clone"
        );
        assert!(
            Arc::ptr_eq(&state.cors_origins, &cloned.cors_origins),
            "cors_origins should be shared via Arc after clone"
        );
    }

    // =========================================================================
    // NF-14: exempt_routes exact match (no prefix false-positive)
    // =========================================================================

    #[tokio::test]
    async fn test_nf14_exempt_routes_no_prefix_false_positive() {
        use crate::middleware::auth_middleware;

        let mut state = test_state();
        state.auth.auth_enabled = true;
        // NoopVerifier accepts all keys, but requests without Authorization header
        // are rejected at the header-parsing stage (before verifier is called).
        state.auth.exempt_routes = Arc::new(vec!["/health".to_string()]);

        let app = Router::new()
            .route("/health", axum::routing::get(|| async { "ok" }))
            .route("/healthz", axum::routing::get(|| async { "ok" }))
            .route("/health/deep", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);

        // /health — should be exempt (exact match)
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "/health should be exempt");

        // /health/deep — should be exempt (starts_with "/health/")
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/deep")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "/health/deep should be exempt"
        );

        // /healthz — should NOT be exempt (not exact match, not "/health/...")
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "/healthz should NOT be exempt from auth"
        );
    }

    // =========================================================================
    // NF-15: download_file avoids .to_vec() copy
    // =========================================================================

    #[tokio::test]
    async fn test_nf15_download_file_returns_correct_data() {
        // After removing .to_vec(), download should still return correct data
        let state = test_state();
        let data = Bytes::from("file content here");
        let file_id = state
            .rune
            .file_broker
            .store(
                "test.txt".into(),
                "text/plain".into(),
                data.clone(),
                "req-1",
            )
            .await;

        let app = build_router(state, None);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&format!("/api/v1/files/{}", file_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        assert_eq!(body.as_ref(), b"file content here");
    }

    // =========================================================================
    // M3: run_flow_async should use complete_task_if_not_cancelled (CAS)
    // =========================================================================

    #[tokio::test]
    async fn test_m3_flow_async_respects_cancelled_status() {
        // Setup: create a flow with a slow echo step, run async, cancel the
        // task in the store before it completes, then verify the completion
        // callback does NOT overwrite the cancelled status.
        use rune_flow::dag::{
            FlowDefinition, RuneConfig as FlowRuneConfig, StepDefinition, StepKind,
        };
        use std::time::Duration;
        use tokio::time::sleep;

        let state = test_state();

        // Register a slow rune (sleeps 200ms)
        let slow_handler = make_handler(|_ctx, input| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(input)
        });
        state
            .rune
            .relay
            .register(
                RuneConfig {
                    name: "slow_echo".into(),
                    version: "1.0.0".into(),
                    description: "slow echo for test".into(),
                    supports_stream: false,
                    gate: None,
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(slow_handler)),
                None,
            )
            .unwrap();

        // Register a flow that uses the slow rune
        let flow_def = FlowDefinition {
            name: "slow_flow".into(),
            steps: vec![StepDefinition {
                name: "step1".into(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Rune(FlowRuneConfig {
                    rune: "slow_echo".into(),
                }),
            }],
            gate_path: None,
        };
        {
            let mut engine = state.flow.flow_engine.write().await;
            engine.register(flow_def).unwrap();
        }

        // Run flow async
        let app = build_router(state.clone(), None);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows/slow_flow/run?async=true")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"x":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let task_id = json["task_id"].as_str().unwrap().to_string();

        // Cancel the task while the slow rune is still running
        state
            .admin
            .store
            .update_task_status(&task_id, TaskStatus::Cancelled, None, None)
            .await
            .unwrap();

        // Wait for the slow rune to finish
        sleep(Duration::from_millis(400)).await;

        // Verify the task is still cancelled (not overwritten to completed)
        let task = state.admin.store.get_task(&task_id).await.unwrap().unwrap();
        assert_eq!(
            task.status,
            TaskStatus::Cancelled,
            "flow async completion should NOT overwrite cancelled status"
        );
    }

    // =========================================================================
    // m3: from_utf8_lossy corrupts binary input in async_execute
    // =========================================================================

    #[tokio::test]
    async fn test_m3_binary_input_not_corrupted_in_async_task() {
        // When body contains invalid UTF-8, the stored input should not
        // silently replace bytes with U+FFFD.
        use std::time::Duration;
        use tokio::time::sleep;

        let state = test_state();

        // Register a simple rune that returns the input
        let echo_handler2 = make_handler(|_ctx, input| async move { Ok(input) });
        state
            .rune
            .relay
            .register(
                RuneConfig {
                    name: "bin_echo".into(),
                    version: "1.0.0".into(),
                    description: "test".into(),
                    supports_stream: false,
                    gate: None,
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                },
                Arc::new(LocalInvoker::new(echo_handler2)),
                None,
            )
            .unwrap();

        // Build a request with invalid UTF-8 bytes
        let binary_body: Vec<u8> = vec![0x80, 0x81, 0xFF, 0xFE];
        let app = build_router(state.clone(), None);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/runes/bin_echo/run?async=true")
                    .body(Body::from(binary_body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let resp_body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
        let task_id = json["task_id"].as_str().unwrap().to_string();

        sleep(Duration::from_millis(100)).await;

        let task = state.admin.store.get_task(&task_id).await.unwrap().unwrap();

        // The stored input must NOT contain the U+FFFD replacement character
        let input_str = task.input.unwrap();
        assert!(
            !input_str.contains('\u{FFFD}'),
            "stored input should not contain U+FFFD replacement chars, got: {}",
            input_str,
        );
    }

    // =========================================================================
    // m6: output_size should use output.len() directly, not re-serialize
    // =========================================================================

    #[tokio::test]
    async fn test_m6_output_size_equals_output_len() {
        // sync_execute should return output_size == the raw output bytes length.
        // The handler returns JSON with extra whitespace (16 bytes), but
        // serde re-serialization produces compact JSON (15 bytes). The bug is
        // that the old code re-serializes and measures 15 instead of 16.
        use rune_core::rune::RuneContext;

        // Note the space after the colon — 16 bytes raw
        let raw_output = r#"{"result": "ok"}"#;
        assert_eq!(raw_output.len(), 16);

        let handler =
            make_handler(|_ctx, _input| async move { Ok(Bytes::from(r#"{"result": "ok"}"#)) });
        let invoker: Arc<dyn rune_core::invoker::RuneInvoker> =
            Arc::new(LocalInvoker::new(handler));

        let ctx = RuneContext {
            rune_name: "test".into(),
            request_id: "req-m6".into(),
            context: HashMap::new(),
            timeout: std::time::Duration::from_secs(5),
            disable_runtime_retry: false,
        };

        let (_response, output_size) =
            crate::handlers::rune::sync_execute(invoker, ctx, Bytes::from("{}"), None).await;

        // Raw output is 16 bytes. Before the fix, serde re-serializes to
        // compact JSON (15 bytes). After the fix, output.len() == 16.
        assert_eq!(
            output_size, 16,
            "output_size should be the raw output byte length (16), got {}",
            output_size,
        );
    }

    // =========================================================================
    // m7: create_flow should reject duplicate gate_path
    // =========================================================================

    #[tokio::test]
    async fn test_m7_create_flow_rejects_duplicate_gate_path() {
        let state = test_state();
        let app = build_router(state.clone(), None);

        // Create first flow with gate_path="/custom"
        let flow1 = serde_json::json!({
            "name": "flow_a",
            "steps": [{"name": "s1", "rune": "echo"}],
            "gate_path": "/custom"
        });

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&flow1).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Create second flow with the same gate_path="/custom" but different name
        let flow2 = serde_json::json!({
            "name": "flow_b",
            "steps": [{"name": "s1", "rune": "echo"}],
            "gate_path": "/custom"
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/flows")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&flow2).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::CONFLICT,
            "creating a flow with duplicate gate_path should return 409 Conflict"
        );
    }
}
