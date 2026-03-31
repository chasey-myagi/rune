use std::sync::Arc;
use std::convert::Infallible;
use axum::{
    Router, Json,
    body::Body,
    extract::{Path, State, Query, DefaultBodyLimit},
    http::StatusCode,
    response::{IntoResponse, Sse, sse::Event},
    routing::{get, post},
};
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc;
use rune_core::rune::{RuneContext, RuneError};
use rune_core::relay::Relay;
use rune_flow::engine::FlowEngine;

/// 异步 Task 状态
#[derive(Clone, serde::Serialize)]
pub struct TaskInfo {
    pub task_id: String,
    pub status: String,          // "pending" | "running" | "completed" | "failed"
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

/// Gate 共享状态
#[derive(Clone)]
pub struct GateState {
    pub relay: Arc<Relay>,
    pub tasks: Arc<DashMap<String, TaskInfo>>,
    pub flow_engine: Arc<FlowEngine>,
}

#[derive(serde::Deserialize, Default)]
pub struct RunParams {
    pub stream: Option<bool>,
    #[serde(rename = "async")]
    pub async_mode: Option<bool>,
}

pub fn build_router(state: GateState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/runes", get(list_runes))
        .route("/api/v1/runes/:name/run", post(run_rune))
        .route("/api/v1/tasks/:id", get(get_task))
        .route("/api/v1/flows", get(list_flows))
        .route("/api/v1/flows/:name/run", post(run_flow))
        .fallback(dynamic_gate_handler)
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024)) // 2MB 全局 body 限制
        .with_state(state)
}

// TODO: 正式版应根据 GateConfig.method 过滤 HTTP method
async fn dynamic_gate_handler(
    State(state): State<GateState>,
    req: axum::http::Request<Body>,
) -> axum::response::Response {
    let path = req.uri().path().to_string();

    // 以 /api/ 开头的路径不应走 gate_path 匹配 — 这是拼错的 API 端点
    if path.starts_with("/api/") {
        return error_response(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            &format!("unknown API endpoint: {}", path),
        );
    }

    let (rune_name, invoker) = match state.relay.resolve_by_gate_path(&path) {
        Some(found) => found,
        None => return error_response(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            &format!("no rune mapped to path '{}'", path),
        ),
    };

    const MAX_BODY_SIZE: usize = 2 * 1024 * 1024; // 2MB
    let body_bytes = match axum::body::to_bytes(req.into_body(), MAX_BODY_SIZE).await {
        Ok(b) => b,
        Err(_) => return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "PAYLOAD_TOO_LARGE",
            "request body too large",
        ),
    };

    let request_id = unique_request_id();
    let ctx = RuneContext { rune_name, request_id };

    match invoker.invoke(ctx, body_bytes).await {
        Ok(output) => {
            match serde_json::from_slice::<serde_json::Value>(&output) {
                Ok(json) => (StatusCode::OK, Json(json)).into_response(),
                Err(_) => (StatusCode::OK, output).into_response(),
            }
        }
        Err(e) => map_error(e),
    }
}

async fn health() -> &'static str { "ok" }

async fn list_runes(State(state): State<GateState>) -> impl IntoResponse {
    let runes: Vec<serde_json::Value> = state.relay.list()
        .into_iter()
        .map(|(name, gate_path)| serde_json::json!({"name": name, "gate_path": gate_path}))
        .collect();
    Json(serde_json::json!({"runes": runes}))
}

async fn get_task(
    State(state): State<GateState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.tasks.get(&id) {
        Some(task) => (StatusCode::OK, Json(serde_json::json!(task.value().clone()))).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": {"code": "NOT_FOUND", "message": "task not found"}}))).into_response(),
    }
}

async fn run_rune(
    State(state): State<GateState>,
    Path(name): Path<String>,
    Query(params): Query<RunParams>,
    body: Bytes,
) -> impl IntoResponse {
    let invoker = match state.relay.resolve(&name) {
        Some(inv) => inv,
        None => return error_response(StatusCode::NOT_FOUND, "NOT_FOUND", &format!("rune '{}' not found", name)),
    };

    let request_id = unique_request_id();
    let ctx = RuneContext { rune_name: name.clone(), request_id: request_id.clone() };

    // async 模式
    if params.async_mode.unwrap_or(false) {
        let task_id = request_id.clone();
        state.tasks.insert(task_id.clone(), TaskInfo {
            task_id: task_id.clone(),
            status: "running".into(),
            result: None,
            error: None,
        });

        let tasks = Arc::clone(&state.tasks);
        let body_clone = body.clone();
        tokio::spawn(async move {
            match invoker.invoke(ctx, body_clone).await {
                Ok(output) => {
                    let result = serde_json::from_slice(&output).unwrap_or(serde_json::Value::Null);
                    tasks.insert(task_id.clone(), TaskInfo {
                        task_id, status: "completed".into(), result: Some(result), error: None,
                    });
                }
                Err(e) => {
                    tasks.insert(task_id.clone(), TaskInfo {
                        task_id, status: "failed".into(), result: None,
                        error: Some(serde_json::json!({"message": e.to_string()})),
                    });
                }
            }
        });

        return (StatusCode::ACCEPTED, Json(serde_json::json!({
            "task_id": request_id,
            "status": "running",
        }))).into_response();
    }

    // stream 模式（SSE）——POC 用 invoker 同步执行后模拟 chunk
    // 真正的流式需要 StreamInvoker，这里演示 SSE 框架能力
    if params.stream.unwrap_or(false) {
        let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(32);

        tokio::spawn(async move {
            match invoker.invoke(ctx, body).await {
                Ok(output) => {
                    // 把结果分成多个 chunk 发（模拟流式）
                    let output_str = String::from_utf8_lossy(&output);
                    let chunk_size = 20;
                    let chars: Vec<char> = output_str.chars().collect();
                    for chunk in chars.chunks(chunk_size) {
                        let s: String = chunk.iter().collect();
                        let event = Event::default()
                            .event("message")
                            .data(s);
                        if tx.send(Ok(event)).await.is_err() { break; }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    let _ = tx.send(Ok(Event::default().event("done").data("[DONE]"))).await;
                }
                Err(e) => {
                    let _ = tx.send(Ok(Event::default().event("error").data(e.to_string()))).await;
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        return Sse::new(stream).into_response();
    }

    // sync 模式（默认）
    match invoker.invoke(ctx, body).await {
        Ok(output) => {
            match serde_json::from_slice::<serde_json::Value>(&output) {
                Ok(json) => (StatusCode::OK, Json(json)).into_response(),
                Err(_) => (StatusCode::OK, output).into_response(),
            }
        }
        Err(e) => map_error(e),
    }
}

fn error_response(status: StatusCode, code: &str, msg: &str) -> axum::response::Response {
    (status, Json(serde_json::json!({"error": {"code": code, "message": msg}}))).into_response()
}

fn map_error(e: RuneError) -> axum::response::Response {
    let (status, code) = match &e {
        RuneError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "INVALID_INPUT"),
        RuneError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
        RuneError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE"),
        RuneError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "TIMEOUT"),
        RuneError::ExecutionFailed { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "EXECUTION_FAILED"),
        RuneError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL"),
    };
    error_response(status, code, &e.to_string())
}

async fn list_flows(State(state): State<GateState>) -> impl IntoResponse {
    let flows: Vec<&str> = state.flow_engine.list();
    Json(serde_json::json!({"flows": flows}))
}

async fn run_flow(
    State(state): State<GateState>,
    Path(name): Path<String>,
    Query(params): Query<RunParams>,
    body: Bytes,
) -> impl IntoResponse {
    // async 模式
    if params.async_mode.unwrap_or(false) {
        let task_id = unique_request_id();
        state.tasks.insert(task_id.clone(), TaskInfo {
            task_id: task_id.clone(), status: "running".into(), result: None, error: None,
        });

        let engine = Arc::clone(&state.flow_engine);
        let tasks = Arc::clone(&state.tasks);
        let flow_name = name.clone();
        let body_clone = body.clone();
        let tid = task_id.clone();
        tokio::spawn(async move {
            match engine.execute(&flow_name, body_clone).await {
                Ok(result) => {
                    let val = serde_json::from_slice(&result.output).unwrap_or(serde_json::Value::Null);
                    tasks.insert(tid.clone(), TaskInfo {
                        task_id: tid, status: "completed".into(),
                        result: Some(serde_json::json!({"output": val, "steps_executed": result.steps_executed})),
                        error: None,
                    });
                }
                Err(e) => {
                    tasks.insert(tid.clone(), TaskInfo {
                        task_id: tid, status: "failed".into(), result: None,
                        error: Some(serde_json::json!({"message": e.to_string()})),
                    });
                }
            }
        });

        return (StatusCode::ACCEPTED, Json(serde_json::json!({
            "task_id": task_id, "status": "running",
        }))).into_response();
    }

    // sync 模式
    match state.flow_engine.execute(&name, body).await {
        Ok(result) => {
            match serde_json::from_slice::<serde_json::Value>(&result.output) {
                Ok(json) => (StatusCode::OK, Json(serde_json::json!({
                    "output": json,
                    "steps_executed": result.steps_executed,
                }))).into_response(),
                Err(_) => (StatusCode::OK, result.output).into_response(),
            }
        }
        Err(e) => {
            let status = match &e {
                rune_flow::engine::FlowError::FlowNotFound(_) => StatusCode::NOT_FOUND,
                rune_flow::engine::FlowError::StepFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            };
            error_response(status, "FLOW_ERROR", &e.to_string())
        }
    }
}

fn unique_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap()
        .as_secs();
    format!("r-{:x}-{:x}", ts, seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use rune_core::rune::{RuneConfig, make_handler};
    use rune_core::invoker::LocalInvoker;
    use tower::ServiceExt;

    fn test_state() -> GateState {
        let relay = Arc::new(Relay::new());
        let flow_engine = Arc::new(FlowEngine::new(Arc::clone(&relay)));
        GateState {
            relay,
            tasks: Arc::new(DashMap::new()),
            flow_engine,
        }
    }

    fn register_echo_rune(relay: &Relay, name: &str, gate_path: Option<&str>) {
        let handler = make_handler(|_ctx, input| async move {
            let body: serde_json::Value = serde_json::from_slice(&input)
                .unwrap_or(serde_json::Value::Null);
            let resp = serde_json::json!({"echo": body});
            Ok(Bytes::from(serde_json::to_vec(&resp).unwrap()))
        });
        let config = RuneConfig {
            name: name.into(),
            description: "".into(),
            gate_path: gate_path.map(|s| s.to_string()),
        };
        relay.register(config, Arc::new(LocalInvoker::new(handler)), None);
    }

    async fn body_to_json(body: Body) -> serde_json::Value {
        let bytes = body.collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    }

    async fn body_to_bytes(body: Body) -> Bytes {
        body.collect().await.unwrap().to_bytes()
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_success() {
        let state = test_state();
        register_echo_rune(&state.relay, "echo", Some("/echo"));
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/echo")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"hello":"world"}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["echo"]["hello"], "world");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_not_found_json() {
        let state = test_state();
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/nonexistent")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["error"]["code"], "NOT_FOUND");
        assert!(
            json["error"]["message"].as_str().unwrap().contains("/nonexistent"),
            "error message should contain the request path"
        );
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_explicit_routes_unaffected() {
        let state = test_state();
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_to_bytes(resp.into_body()).await;
        assert_eq!(&bytes[..], b"ok");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_invoker_error_mapping() {
        let state = test_state();
        let handler = make_handler(|_ctx, _input| async move {
            Err(rune_core::rune::RuneError::InvalidInput("bad input".into()))
        });
        let config = RuneConfig {
            name: "bad".into(),
            description: "".into(),
            gate_path: Some("/bad".into()),
        };
        state.relay.register(config, Arc::new(LocalInvoker::new(handler)), None);
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/bad")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_non_json_response() {
        let state = test_state();
        let handler = make_handler(|_ctx, _input| async move {
            Ok(Bytes::from("plain text"))
        });
        let config = RuneConfig {
            name: "plain".into(),
            description: "".into(),
            gate_path: Some("/plain".into()),
        };
        state.relay.register(config, Arc::new(LocalInvoker::new(handler)), None);
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/plain")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_to_bytes(resp.into_body()).await;
        assert_eq!(&bytes[..], b"plain text");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_body_passthrough() {
        let state = test_state();
        // Handler that returns input as-is (raw bytes, not JSON)
        let handler = make_handler(|_ctx, input| async move {
            Ok(input)
        });
        let config = RuneConfig {
            name: "passthrough".into(),
            description: "".into(),
            gate_path: Some("/passthrough".into()),
        };
        state.relay.register(config, Arc::new(LocalInvoker::new(handler)), None);
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/passthrough")
            .body(Body::from("raw body content"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_to_bytes(resp.into_body()).await;
        assert_eq!(&bytes[..], b"raw body content");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_get_method() {
        let state = test_state();
        register_echo_rune(&state.relay, "echo", Some("/echo"));
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method("GET")
            .uri("/echo")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_to_json(resp.into_body()).await;
        // GET with empty body → echo handler receives null
        assert_eq!(json["echo"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_query_string_ignored() {
        let state = test_state();
        register_echo_rune(&state.relay, "echo", Some("/echo"));
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/echo?foo=bar&baz=1")
            .body(Body::from("{}"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ========== P0: map_error full branch coverage ==========

    fn clone_rune_error(e: &RuneError) -> RuneError {
        match e {
            RuneError::NotFound(s) => RuneError::NotFound(s.clone()),
            RuneError::Unavailable => RuneError::Unavailable,
            RuneError::Timeout => RuneError::Timeout,
            RuneError::ExecutionFailed { code, message } => RuneError::ExecutionFailed { code: code.clone(), message: message.clone() },
            RuneError::Internal(e) => RuneError::Internal(anyhow::anyhow!("{}", e)),
            RuneError::InvalidInput(s) => RuneError::InvalidInput(s.clone()),
        }
    }

    async fn invoke_error_rune(error: RuneError, gate_path: &str) -> axum::response::Response {
        let state = test_state();
        let err_clone = clone_rune_error(&error);
        let handler = make_handler(move |_ctx, _input| {
            let e = clone_rune_error(&err_clone);
            async move { Err(e) }
        });
        let config = RuneConfig {
            name: "err".into(),
            description: "".into(),
            gate_path: Some(gate_path.into()),
        };
        state.relay.register(config, Arc::new(LocalInvoker::new(handler)), None);
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method("POST")
            .uri(gate_path)
            .body(Body::empty())
            .unwrap();
        app.oneshot(req).await.unwrap()
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_error_not_found() {
        let resp = invoke_error_rune(RuneError::NotFound("x".into()), "/err-nf").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["error"]["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_error_unavailable() {
        let resp = invoke_error_rune(RuneError::Unavailable, "/err-ua").await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["error"]["code"], "UNAVAILABLE");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_error_timeout() {
        let resp = invoke_error_rune(RuneError::Timeout, "/err-to").await;
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["error"]["code"], "TIMEOUT");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_error_execution_failed() {
        let resp = invoke_error_rune(
            RuneError::ExecutionFailed { code: "E01".into(), message: "boom".into() },
            "/err-ef",
        ).await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["error"]["code"], "EXECUTION_FAILED");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_error_internal() {
        let resp = invoke_error_rune(
            RuneError::Internal(anyhow::anyhow!("oops")),
            "/err-int",
        ).await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["error"]["code"], "INTERNAL");
    }

    // ========== P0: body size limit ==========

    #[tokio::test]
    async fn test_fix_dynamic_gate_body_too_large() {
        let state = test_state();
        register_echo_rune(&state.relay, "echo", Some("/echo"));
        let app = build_router(state);

        // 3MB body exceeds 2MB limit
        let large_body = vec![b'x'; 3 * 1024 * 1024];
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/echo")
            .body(Body::from(large_body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["error"]["code"], "PAYLOAD_TOO_LARGE");
    }

    // ========== P1: builtin route priority ==========

    #[tokio::test]
    async fn test_fix_dynamic_gate_builtin_route_priority() {
        let state = test_state();
        // Register a rune with gate_path="/health" — should NOT override the explicit /health route
        register_echo_rune(&state.relay, "health_rune", Some("/health"));
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = body_to_bytes(resp.into_body()).await;
        assert_eq!(&bytes[..], b"ok", "explicit /health route should take priority over fallback");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_body_exact_limit() {
        let state = test_state();
        register_echo_rune(&state.relay, "echo", Some("/echo"));
        let app = build_router(state);

        // Exactly 2MB body should be accepted
        let exact_body = vec![b'x'; 2 * 1024 * 1024];
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/echo")
            .body(Body::from(exact_body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "exactly 2MB body should be accepted");
    }

    #[tokio::test]
    async fn test_fix_dynamic_gate_body_one_over_limit() {
        let state = test_state();
        register_echo_rune(&state.relay, "echo", Some("/echo"));
        let app = build_router(state);

        // 2MB + 1 byte should be rejected
        let over_body = vec![b'x'; 2 * 1024 * 1024 + 1];
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/echo")
            .body(Body::from(over_body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE, "2MB+1 body should be rejected");
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["error"]["code"], "PAYLOAD_TOO_LARGE");
    }

    // ========== Issue 2 regression: body size limit on /api/v1/runes/:name/run ==========

    #[tokio::test]
    async fn test_fix_run_rune_body_too_large() {
        let state = test_state();
        register_echo_rune(&state.relay, "echo", None);
        let app = build_router(state);

        // 3MB body exceeds global 2MB limit
        let large_body = vec![b'x'; 3 * 1024 * 1024];
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/runes/echo/run")
            .body(Body::from(large_body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn test_fix_run_flow_body_too_large() {
        let state = test_state();
        let app = build_router(state);

        // 3MB body exceeds global 2MB limit
        let large_body = vec![b'x'; 3 * 1024 * 1024];
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/flows/test/run")
            .body(Body::from(large_body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    // ========== Issue: /api/ prefix should not fall through to gate_path matching ==========

    #[tokio::test]
    async fn test_fix_dynamic_gate_api_prefix_not_gate_path() {
        let state = test_state();
        let app = build_router(state);

        let req = axum::http::Request::builder()
            .method("GET")
            .uri("/api/v1/typo")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let json = body_to_json(resp.into_body()).await;
        assert_eq!(json["error"]["code"], "NOT_FOUND");
        let msg = json["error"]["message"].as_str().unwrap();
        assert!(
            msg.contains("unknown API endpoint"),
            "should say 'unknown API endpoint', got: {}",
            msg
        );
        assert!(
            !msg.contains("no rune mapped"),
            "should NOT say 'no rune mapped', got: {}",
            msg
        );
    }
}
