//! End-to-end integration tests for the Rune runtime.
//!
//! Each test spawns a real HTTP server on a random port and exercises the
//! API through `reqwest` over TCP — no tower `oneshot` shortcuts.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Client;
use tokio::net::TcpListener;

use rune_core::auth::{KeyVerifier, NoopVerifier};
use rune_core::invoker::{LocalInvoker, LocalStreamInvoker};
use rune_core::relay::Relay;
use rune_core::resolver::{Resolver, RoundRobinResolver};
use rune_core::rune::{
    GateConfig, RuneConfig, RuneContext, RuneError, StreamRuneHandler, StreamSender, make_handler,
};
use rune_core::session::SessionManager;
use rune_flow::engine::FlowEngine;
use rune_gate::gate::{self, GateState};
use rune_store::{RuneStore, StoreKeyVerifier};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Minimal echo stream handler for SSE tests.
struct EchoStreamHandler;

#[async_trait::async_trait]
impl StreamRuneHandler for EchoStreamHandler {
    async fn execute(
        &self,
        _ctx: RuneContext,
        input: Bytes,
        tx: StreamSender,
    ) -> Result<(), RuneError> {
        // Echo the input as three chunks
        let text = String::from_utf8_lossy(&input).to_string();
        for i in 0..3 {
            let chunk = format!("chunk_{}: {}", i, text);
            tx.emit(Bytes::from(chunk)).await?;
        }
        tx.end().await
    }
}

/// Build a GateState with an echo rune (and optionally more runes).
/// Returns (GateState, Arc<RuneStore>) so tests can interact with the store.
fn build_test_state(auth_enabled: bool) -> (GateState, Arc<RuneStore>) {
    let relay = Arc::new(Relay::new());
    let resolver: Arc<dyn Resolver> = Arc::new(RoundRobinResolver::new());
    let store = Arc::new(RuneStore::open_in_memory().unwrap());

    // -- echo rune (sync, with gate_path /echo) --
    let echo_handler = make_handler(|_ctx, input| async move { Ok(input) });
    relay
        .register(
            RuneConfig {
                name: "echo".into(),
                version: "1.0.0".into(),
                description: "echo test rune".into(),
                supports_stream: false,
                gate: Some(GateConfig {
                    path: "/echo".into(),
                    method: "POST".into(),
                }),
                input_schema: None,
                output_schema: None,
                priority: 0, labels: Default::default(),
            },
            Arc::new(LocalInvoker::new(echo_handler)),
            None,
        )
        .unwrap();

    // -- echo_stream rune (stream-capable, with gate_path /echo-stream) --
    relay
        .register(
            RuneConfig {
                name: "echo_stream".into(),
                version: "1.0.0".into(),
                description: "stream echo rune".into(),
                supports_stream: true,
                gate: Some(GateConfig {
                    path: "/echo-stream".into(),
                    method: "POST".into(),
                }),
                input_schema: None,
                output_schema: None,
                priority: 0, labels: Default::default(),
            },
            Arc::new(LocalStreamInvoker::new(Arc::new(EchoStreamHandler))),
            None,
        )
        .unwrap();

    // -- validated_rune: has input_schema --
    let validated_handler = make_handler(|_ctx, input| async move { Ok(input) });
    let input_schema = r#"{
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"}
        },
        "required": ["name"]
    }"#;
    relay
        .register(
            RuneConfig {
                name: "validated".into(),
                version: "1.0.0".into(),
                description: "validated rune".into(),
                supports_stream: false,
                gate: Some(GateConfig {
                    path: "/validated".into(),
                    method: "POST".into(),
                }),
                input_schema: Some(input_schema.to_string()),
                output_schema: None,
                priority: 0, labels: Default::default(),
            },
            Arc::new(LocalInvoker::new(validated_handler)),
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
        Arc::clone(&resolver),
    )));

    let state = GateState {
        relay,
        resolver,
        store: store.clone(),
        key_verifier,
        session_mgr: Arc::new(SessionManager::new(
            Duration::from_secs(10),
            Duration::from_secs(35),
        )),
        auth_enabled,
        exempt_routes: vec!["/health".to_string()],
        cors_origins: vec![],
        dev_mode: !auth_enabled,
        started_at: Instant::now(),
        file_broker: Arc::new(gate::FileBroker::new()),
        max_upload_size_mb: 1, // 1MB for upload tests
        flow_engine,
        rate_limiter: None,
        shutdown: gate::ShutdownCoordinator::new(),
    };

    (state, store)
}

/// Spawn a real HTTP server on a random port and return (base_url, join_handle).
async fn spawn_server(state: GateState) -> (String, tokio::task::JoinHandle<()>) {
    let router = gate::build_router(state, None);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);

    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // Give the server a moment to start accepting connections
    tokio::time::sleep(Duration::from_millis(20)).await;

    (base_url, handle)
}

fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

// ===========================================================================
// 1. Server startup & health check
// ===========================================================================

#[tokio::test]
async fn e2e_health_returns_200() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c.get(format!("{}/health", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "ok");
}

#[tokio::test]
async fn e2e_status_returns_uptime_and_rune_count() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .get(format!("{}/api/v1/status", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json["uptime_secs"].as_u64().is_some());
    assert!(json["rune_count"].as_u64().unwrap() >= 3); // echo, echo_stream, validated
}

// ===========================================================================
// 2. Rune invocation
// ===========================================================================

#[tokio::test]
async fn e2e_echo_rune_via_api() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let payload = serde_json::json!({"msg": "hello"});
    let resp = c
        .post(format!("{}/api/v1/runes/echo/run", base))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["msg"], "hello");
}

#[tokio::test]
async fn e2e_echo_rune_via_gate_path() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let payload = serde_json::json!({"msg": "gate"});
    let resp = c
        .post(format!("{}/echo", base))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["msg"], "gate");
}

#[tokio::test]
async fn e2e_call_nonexistent_rune_returns_404() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!("{}/api/v1/runes/nonexistent/run", base))
        .body("{}")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn e2e_call_nonexistent_gate_path_returns_404() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!("{}/no-such-path", base))
        .body("{}")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// ===========================================================================
// 3. Async invocation (202 → task polling)
// ===========================================================================

#[tokio::test]
async fn e2e_async_invocation_returns_202_and_task_completes() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let payload = serde_json::json!({"data": "async_test"});
    let resp = c
        .post(format!("{}/api/v1/runes/echo/run?async=true", base))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let json: serde_json::Value = resp.json().await.unwrap();
    let task_id = json["task_id"].as_str().unwrap().to_string();
    assert_eq!(json["status"], "running");

    // Poll for completion (with timeout)
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let task_resp = c
            .get(format!("{}/api/v1/tasks/{}", base, task_id))
            .send()
            .await
            .unwrap();
        assert_eq!(task_resp.status(), 200);
        let task_json: serde_json::Value = task_resp.json().await.unwrap();
        if task_json["status"] == "completed" {
            // Check output contains our data
            let output_str = task_json["output"].as_str().unwrap();
            let output: serde_json::Value = serde_json::from_str(output_str).unwrap();
            assert_eq!(output["data"], "async_test");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "task did not complete within 5 seconds"
        );
    }
}

// ===========================================================================
// 4. Streaming (SSE)
// ===========================================================================

#[tokio::test]
async fn e2e_stream_invocation_returns_sse_events() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!(
            "{}/api/v1/runes/echo_stream/run?stream=true",
            base
        ))
        .body("hello_stream")
        .header("content-type", "text/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    // SSE format: "event: message\ndata: ...\n\n"
    assert!(body.contains("event: message"), "should have message events");
    assert!(
        body.contains("chunk_0: hello_stream"),
        "first chunk should contain input"
    );
    assert!(body.contains("event: done"), "should have done event");
}

#[tokio::test]
async fn e2e_stream_request_on_non_stream_rune_returns_400() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!("{}/api/v1/runes/echo/run?stream=true", base))
        .body("{}")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json["error"]["code"]
        .as_str()
        .unwrap()
        .contains("STREAM_NOT_SUPPORTED"));
}

// ===========================================================================
// 5. Key management
// ===========================================================================

#[tokio::test]
async fn e2e_key_create_list_revoke() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Create a key
    let resp = c
        .post(format!("{}/api/v1/keys", base))
        .json(&serde_json::json!({"key_type": "gate", "label": "e2e-test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let created: serde_json::Value = resp.json().await.unwrap();
    let raw_key = created["raw_key"].as_str().unwrap().to_string();
    assert!(raw_key.starts_with("rk_"));
    let key_id = created["key"]["id"].as_i64().unwrap();

    // List keys — should include the new key
    let resp = c
        .get(format!("{}/api/v1/keys", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let list: serde_json::Value = resp.json().await.unwrap();
    let keys = list["keys"].as_array().unwrap();
    assert!(keys.iter().any(|k| k["id"].as_i64() == Some(key_id)));

    // Revoke the key
    let resp = c
        .delete(format!("{}/api/v1/keys/{}", base, key_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let revoked: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(revoked["status"], "revoked");
}

// ===========================================================================
// 6. Auth enforcement
// ===========================================================================

#[tokio::test]
async fn e2e_auth_enabled_no_key_returns_401() {
    let (state, _store) = build_test_state(true);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!("{}/api/v1/runes/echo/run", base))
        .body("{}")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn e2e_auth_enabled_health_exempt() {
    let (state, _store) = build_test_state(true);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // /health is exempt from auth
    let resp = c.get(format!("{}/health", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn e2e_auth_enabled_with_valid_key_returns_200() {
    let (state, store) = build_test_state(true);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Create a gate key directly through the store
    let key_result = store
        .create_key(rune_store::KeyType::Gate, "e2e-auth-test").await
        .unwrap();

    let resp = c
        .post(format!("{}/api/v1/runes/echo/run", base))
        .header("authorization", format!("Bearer {}", key_result.raw_key))
        .json(&serde_json::json!({"auth": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["auth"], true);
}

#[tokio::test]
async fn e2e_auth_enabled_with_invalid_key_returns_401() {
    let (state, _store) = build_test_state(true);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!("{}/api/v1/runes/echo/run", base))
        .header("authorization", "Bearer rk_invalid_key_value")
        .body("{}")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

// ===========================================================================
// 7. Flow management
// ===========================================================================

#[tokio::test]
async fn e2e_flow_crud_lifecycle() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let flow_def = serde_json::json!({
        "name": "e2e_flow",
        "steps": [
            {"name": "s1", "rune": "echo", "depends_on": []},
        ],
        "gate_path": null
    });

    // Create flow
    let resp = c
        .post(format!("{}/api/v1/flows", base))
        .json(&flow_def)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Get flow
    let resp = c
        .get(format!("{}/api/v1/flows/e2e_flow", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["name"], "e2e_flow");

    // List flows
    let resp = c
        .get(format!("{}/api/v1/flows", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let list: serde_json::Value = resp.json().await.unwrap();
    let flows = list.as_array().unwrap();
    assert!(flows.iter().any(|f| f["name"] == "e2e_flow"));

    // Run flow
    let resp = c
        .post(format!("{}/api/v1/flows/e2e_flow/run", base))
        .json(&serde_json::json!({"test": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["steps_executed"], 1);

    // Delete flow
    let resp = c
        .delete(format!("{}/api/v1/flows/e2e_flow", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Verify deletion
    let resp = c
        .get(format!("{}/api/v1/flows/e2e_flow", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn e2e_flow_duplicate_name_returns_409() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let flow_def = serde_json::json!({
        "name": "dup_flow",
        "steps": [{"name": "s1", "rune": "echo", "depends_on": []}],
        "gate_path": null
    });

    // Create first
    let resp = c
        .post(format!("{}/api/v1/flows", base))
        .json(&flow_def)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Create duplicate
    let resp = c
        .post(format!("{}/api/v1/flows", base))
        .json(&flow_def)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
}

#[tokio::test]
async fn e2e_flow_cyclic_dag_returns_400() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // s1 depends on s2, s2 depends on s1 → cycle
    let flow_def = serde_json::json!({
        "name": "cyclic_flow",
        "steps": [
            {"name": "s1", "rune": "echo", "depends_on": ["s2"]},
            {"name": "s2", "rune": "echo", "depends_on": ["s1"]},
        ],
        "gate_path": null
    });

    let resp = c
        .post(format!("{}/api/v1/flows", base))
        .json(&flow_def)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let json: serde_json::Value = resp.json().await.unwrap();
    let error = json["error"].as_str().unwrap();
    assert!(
        error.contains("cycle"),
        "error should mention cycle: {}",
        error
    );
}

#[tokio::test]
async fn e2e_flow_empty_body_returns_422() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!("{}/api/v1/flows", base))
        .body("")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
}

#[tokio::test]
async fn e2e_run_nonexistent_flow_returns_404() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!("{}/api/v1/flows/ghost/run", base))
        .json(&serde_json::json!({"data": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// ===========================================================================
// 8. Schema validation
// ===========================================================================

#[tokio::test]
async fn e2e_schema_valid_input_returns_200() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!("{}/api/v1/runes/validated/run", base))
        .json(&serde_json::json!({"name": "Alice", "age": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn e2e_schema_invalid_input_returns_422() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Missing required "name" field
    let resp = c
        .post(format!("{}/api/v1/runes/validated/run", base))
        .json(&serde_json::json!({"age": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["error"]["code"], "VALIDATION_FAILED");
}

#[tokio::test]
async fn e2e_schema_wrong_type_returns_422() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // "age" should be integer, not string
    let resp = c
        .post(format!("{}/api/v1/runes/validated/run", base))
        .json(&serde_json::json!({"name": "Bob", "age": "not_a_number"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
}

// ===========================================================================
// 9. File upload (multipart)
// ===========================================================================

#[tokio::test]
async fn e2e_multipart_upload_returns_200_with_file_metadata() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let form = reqwest::multipart::Form::new()
        .text("input", r#"{"msg":"with_file"}"#)
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"hello file content".to_vec())
                .file_name("test.txt")
                .mime_str("text/plain")
                .unwrap(),
        );

    let resp = c
        .post(format!("{}/api/v1/runes/echo/run", base))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let files = json["files"].as_array().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0]["filename"], "test.txt");
    assert_eq!(files[0]["mime_type"], "text/plain");
    assert!(files[0]["size"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn e2e_multipart_upload_exceeding_limit_returns_413() {
    let (state, _store) = build_test_state(false);
    // max_upload_size_mb = 1 in our test state
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Create a file larger than 1MB
    let big_data = vec![0u8; 2 * 1024 * 1024]; // 2MB
    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(big_data)
            .file_name("big.bin")
            .mime_str("application/octet-stream")
            .unwrap(),
    );

    let resp = c
        .post(format!("{}/api/v1/runes/echo/run", base))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
}

// ===========================================================================
// 10. Logs and stats
// ===========================================================================

#[tokio::test]
async fn e2e_logs_recorded_after_call() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Make a call to generate a log entry
    let _resp = c
        .post(format!("{}/api/v1/runes/echo/run", base))
        .json(&serde_json::json!({"log_test": true}))
        .send()
        .await
        .unwrap();

    // Query logs
    let resp = c
        .get(format!("{}/api/v1/logs", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let logs = json["logs"].as_array().unwrap();
    assert!(
        !logs.is_empty(),
        "should have at least one log entry after calling echo"
    );
    assert_eq!(logs[0]["rune_name"], "echo");
}

#[tokio::test]
async fn e2e_logs_filter_by_rune() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Call echo rune
    c.post(format!("{}/api/v1/runes/echo/run", base))
        .json(&serde_json::json!({"x": 1}))
        .send()
        .await
        .unwrap();

    // Call validated rune
    c.post(format!("{}/api/v1/runes/validated/run", base))
        .json(&serde_json::json!({"name": "test"}))
        .send()
        .await
        .unwrap();

    // Filter logs for echo only
    let resp = c
        .get(format!("{}/api/v1/logs?rune=echo", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let logs = json["logs"].as_array().unwrap();
    for log in logs {
        assert_eq!(log["rune_name"], "echo");
    }
}

#[tokio::test]
async fn e2e_stats_reflect_calls() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Make two calls
    c.post(format!("{}/api/v1/runes/echo/run", base))
        .json(&serde_json::json!({"a": 1}))
        .send()
        .await
        .unwrap();
    c.post(format!("{}/api/v1/runes/echo/run", base))
        .json(&serde_json::json!({"b": 2}))
        .send()
        .await
        .unwrap();

    let resp = c
        .get(format!("{}/api/v1/stats", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json["total_calls"].as_i64().unwrap() >= 2);
    let by_rune = json["by_rune"].as_array().unwrap();
    let echo_stat = by_rune.iter().find(|r| r["rune_name"] == "echo");
    assert!(echo_stat.is_some());
    assert!(echo_stat.unwrap()["count"].as_i64().unwrap() >= 2);
}

// ===========================================================================
// 11. List runes
// ===========================================================================

#[tokio::test]
async fn e2e_list_runes_shows_all_registered() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .get(format!("{}/api/v1/runes", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let runes = json["runes"].as_array().unwrap();
    let names: Vec<&str> = runes.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"echo"));
    assert!(names.contains(&"echo_stream"));
    assert!(names.contains(&"validated"));
}

// ===========================================================================
// 12. Task management
// ===========================================================================

#[tokio::test]
async fn e2e_get_nonexistent_task_returns_404() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .get(format!("{}/api/v1/tasks/nonexistent-id", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn e2e_async_task_cancel() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Use a handler that takes time — echo is instant, but we can still test
    // the cancellation API on a completed task (should return CONFLICT)
    let resp = c
        .post(format!("{}/api/v1/runes/echo/run?async=true", base))
        .json(&serde_json::json!({"cancel_test": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let json: serde_json::Value = resp.json().await.unwrap();
    let task_id = json["task_id"].as_str().unwrap().to_string();

    // Wait for completion
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Try to cancel completed task — should get 409 CONFLICT
    let resp = c
        .delete(format!("{}/api/v1/tasks/{}", base, task_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
}

// ===========================================================================
// 13. OpenAPI
// ===========================================================================

#[tokio::test]
async fn e2e_openapi_json_available() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .get(format!("{}/api/v1/openapi.json", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    // Should have an "openapi" version field
    assert!(json["openapi"].as_str().is_some());
    assert!(json["paths"].is_object());
}

// ===========================================================================
// 14. Casters endpoint (empty in test)
// ===========================================================================

#[tokio::test]
async fn e2e_casters_returns_empty_list() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .get(format!("{}/api/v1/casters", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let casters = json["casters"].as_array().unwrap();
    assert!(casters.is_empty());
}

// ===========================================================================
// 15. Flow async execution
// ===========================================================================

#[tokio::test]
async fn e2e_flow_async_run() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Create a flow first
    let flow_def = serde_json::json!({
        "name": "async_flow",
        "steps": [{"name": "s1", "rune": "echo", "depends_on": []}],
        "gate_path": null
    });
    let resp = c
        .post(format!("{}/api/v1/flows", base))
        .json(&flow_def)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Run flow async
    let resp = c
        .post(format!("{}/api/v1/flows/async_flow/run?async=true", base))
        .json(&serde_json::json!({"flow_data": "test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["flow"], "async_flow");
    let task_id = json["task_id"].as_str().unwrap().to_string();

    // Poll for completion
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let resp = c
            .get(format!("{}/api/v1/tasks/{}", base, task_id))
            .send()
            .await
            .unwrap();
        let task: serde_json::Value = resp.json().await.unwrap();
        if task["status"] == "completed" {
            break;
        }
        assert!(Instant::now() < deadline, "flow async task did not complete");
    }
}

// ===========================================================================
// 16. Flow stream execution
// ===========================================================================

#[tokio::test]
async fn e2e_flow_stream_run() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Create a flow
    let flow_def = serde_json::json!({
        "name": "stream_flow",
        "steps": [{"name": "s1", "rune": "echo", "depends_on": []}],
        "gate_path": null
    });
    c.post(format!("{}/api/v1/flows", base))
        .json(&flow_def)
        .send()
        .await
        .unwrap();

    // Run flow in stream mode
    let resp = c
        .post(format!(
            "{}/api/v1/flows/stream_flow/run?stream=true",
            base
        ))
        .json(&serde_json::json!({"stream_data": 42}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("event: result") || body.contains("event: done"),
        "should have SSE events in stream flow response"
    );
}

// ===========================================================================
// 17. Edge cases
// ===========================================================================

#[tokio::test]
async fn e2e_empty_body_to_echo_returns_200() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .post(format!("{}/api/v1/runes/echo/run", base))
        .body("")
        .send()
        .await
        .unwrap();
    // echo returns whatever it gets, empty body is OK
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn e2e_flow_empty_name_returns_400() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let flow_def = serde_json::json!({
        "name": "",
        "steps": [{"name": "s1", "rune": "echo", "depends_on": []}],
        "gate_path": null
    });

    let resp = c
        .post(format!("{}/api/v1/flows", base))
        .json(&flow_def)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn e2e_flow_no_steps_returns_400() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let flow_def = serde_json::json!({
        "name": "empty_steps",
        "steps": [],
        "gate_path": null
    });

    let resp = c
        .post(format!("{}/api/v1/flows", base))
        .json(&flow_def)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn e2e_flow_run_empty_body_returns_422() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Create a valid flow
    let flow_def = serde_json::json!({
        "name": "needs_input",
        "steps": [{"name": "s1", "rune": "echo", "depends_on": []}],
        "gate_path": null
    });
    c.post(format!("{}/api/v1/flows", base))
        .json(&flow_def)
        .send()
        .await
        .unwrap();

    // Run with empty body
    let resp = c
        .post(format!("{}/api/v1/flows/needs_input/run", base))
        .body("")
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
}

#[tokio::test]
async fn e2e_delete_nonexistent_flow_returns_404() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .delete(format!("{}/api/v1/flows/no_such_flow", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// ===========================================================================
// 18. File download
// ===========================================================================

#[tokio::test]
async fn e2e_file_download_nonexistent_returns_404() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    let resp = c
        .get(format!("{}/api/v1/files/nonexistent-file-id", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// ===========================================================================
// 19. Multiple rune calls from same server
// ===========================================================================

#[tokio::test]
async fn e2e_multiple_rune_calls_on_same_server() {
    let (state, _store) = build_test_state(false);
    let (base, _h) = spawn_server(state).await;
    let c = client();

    // Call echo via API
    let r1 = c
        .post(format!("{}/api/v1/runes/echo/run", base))
        .json(&serde_json::json!({"call": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);

    // Call echo via gate_path
    let r2 = c
        .post(format!("{}/echo", base))
        .json(&serde_json::json!({"call": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);

    // Call validated via API
    let r3 = c
        .post(format!("{}/api/v1/runes/validated/run", base))
        .json(&serde_json::json!({"name": "multi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r3.status(), 200);

    // Call validated via gate_path
    let r4 = c
        .post(format!("{}/validated", base))
        .json(&serde_json::json!({"name": "gate_multi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r4.status(), 200);

    // Stats should reflect all calls
    let resp = c
        .get(format!("{}/api/v1/stats", base))
        .send()
        .await
        .unwrap();
    let stats: serde_json::Value = resp.json().await.unwrap();
    assert!(stats["total_calls"].as_i64().unwrap() >= 4);
}
