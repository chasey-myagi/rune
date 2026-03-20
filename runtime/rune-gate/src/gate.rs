use std::convert::Infallible;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Sse, sse::Event},
    routing::{get, post},
};
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc;

use rune_core::invoker::RuneInvoker;
use rune_core::relay::Relay;
use rune_core::resolver::Resolver;
use rune_core::rune::{RuneContext, RuneError};

/// 异步 Task 状态
#[derive(Clone, serde::Serialize)]
pub struct TaskInfo {
    pub task_id: String,
    pub status: String, // "pending" | "running" | "completed" | "failed" | "cancelled"
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

/// Gate 共享状态
#[derive(Clone)]
pub struct GateState {
    pub relay: Arc<Relay>,
    pub resolver: Arc<dyn Resolver>,
    pub tasks: Arc<DashMap<String, TaskInfo>>,
    pub session_mgr: Arc<rune_core::session::SessionManager>,
}

#[derive(serde::Deserialize, Default)]
pub struct RunParams {
    pub stream: Option<bool>,
    #[serde(rename = "async")]
    pub async_mode: Option<bool>,
}

/// 构建 Gate Router，支持可选的额外路由（供 rune-server 注入 flow 路由等）
pub fn build_router(state: GateState, extra_routes: Option<Router<GateState>>) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/api/v1/runes", get(list_runes))
        .route("/api/v1/runes/:name/run", post(run_rune))
        .route("/api/v1/tasks/{id}", get(get_task).delete(delete_task));

    if let Some(extra) = extra_routes {
        router = router.merge(extra);
    }

    router
        .fallback(dynamic_rune_handler)
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn list_runes(State(state): State<GateState>) -> impl IntoResponse {
    let runes: Vec<serde_json::Value> = state
        .relay
        .list()
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
        Some(task) => {
            (StatusCode::OK, Json(serde_json::json!(task.value().clone()))).into_response()
        }
        None => error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "task not found"),
    }
}

async fn delete_task(
    State(state): State<GateState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.tasks.get(&id) {
        None => error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "task not found"),
        Some(task) => {
            let status = task.status.clone();
            drop(task);
            match status.as_str() {
                "completed" | "failed" => error_response(
                    StatusCode::CONFLICT,
                    "CONFLICT",
                    &format!("task already {}", status),
                ),
                "cancelled" => {
                    // 幂等：已经取消
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({"task_id": id, "status": "cancelled"})),
                    )
                        .into_response()
                }
                _ => {
                    // running 或 pending — 执行取消
                    state.session_mgr.cancel_by_request_id(&id, "cancelled by user").await;
                    state.tasks.insert(
                        id.clone(),
                        TaskInfo {
                            task_id: id.clone(),
                            status: "cancelled".into(),
                            result: None,
                            error: Some(serde_json::json!({"message": "cancelled by user"})),
                        },
                    );
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({"task_id": id, "status": "cancelled"})),
                    )
                        .into_response()
                }
            }
        }
    }
}

async fn run_rune(
    State(state): State<GateState>,
    Path(name): Path<String>,
    Query(params): Query<RunParams>,
    body: Bytes,
) -> impl IntoResponse {
    execute_rune(&state, &name, params, body).await
}

/// gate_path 动态路由：通过 Relay 查找匹配 path 的 rune
async fn dynamic_rune_handler(
    State(state): State<GateState>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();

    let body = match axum::body::to_bytes(req.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "failed to read body",
            );
        }
    };

    // 查找哪个 rune 的 gate_path 匹配当前路径
    let rune_name = {
        let mut found = None;
        for (name, gate_path) in state.relay.list() {
            if let Some(gp) = gate_path {
                if gp == path {
                    found = Some(name);
                    break;
                }
            }
        }
        found
    };

    let rune_name = match rune_name {
        Some(name) => name,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                &format!("no rune mapped to path '{}'", path),
            );
        }
    };

    let params: RunParams = serde_urlencoded::from_str(&query).unwrap_or_default();

    execute_rune(&state, &rune_name, params, Bytes::from(body.to_vec())).await
}

/// 通用 rune 执行逻辑，供 run_rune（调试路由）和 dynamic_rune_handler 共用
async fn execute_rune(
    state: &GateState,
    rune_name: &str,
    params: RunParams,
    body: Bytes,
) -> axum::response::Response {
    let invoker = match state.relay.resolve(rune_name, &*state.resolver) {
        Some(inv) => inv,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                &format!("rune '{}' not found", rune_name),
            );
        }
    };

    // 检查 supports_stream
    if params.stream.unwrap_or(false) {
        if let Some(entries) = state.relay.find(rune_name) {
            if let Some(first) = entries.value().first() {
                if !first.config.supports_stream {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "STREAM_NOT_SUPPORTED",
                        &format!("rune '{}' does not support streaming", rune_name),
                    );
                }
            }
        }
    }

    let request_id = unique_request_id();
    let ctx = RuneContext {
        rune_name: rune_name.to_string(),
        request_id: request_id.clone(),
        context: Default::default(),
        timeout: std::time::Duration::from_secs(30),
    };

    // async 模式
    if params.async_mode.unwrap_or(false) {
        return async_execute(state, invoker, ctx, body, request_id).await;
    }

    // stream 模式（真正流式，通过 invoke_stream）
    if params.stream.unwrap_or(false) {
        return stream_execute(state, &request_id, invoker, ctx, body).await;
    }

    // sync 模式（默认）
    sync_execute(invoker, ctx, body).await
}

async fn sync_execute(
    invoker: Arc<dyn RuneInvoker>,
    ctx: RuneContext,
    body: Bytes,
) -> axum::response::Response {
    match invoker.invoke_once(ctx, body).await {
        Ok(output) => match serde_json::from_slice::<serde_json::Value>(&output) {
            Ok(json) => (StatusCode::OK, Json(json)).into_response(),
            Err(_) => (StatusCode::OK, output).into_response(),
        },
        Err(e) => map_error(e),
    }
}

async fn stream_execute(
    state: &GateState,
    request_id: &str,
    invoker: Arc<dyn RuneInvoker>,
    ctx: RuneContext,
    body: Bytes,
) -> axum::response::Response {
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(32);

    let state_clone = state.clone();
    let req_id = request_id.to_string();
    tokio::spawn(async move {
        match invoker.invoke_stream(ctx, body).await {
            Ok(mut stream_rx) => {
                while let Some(chunk) = stream_rx.recv().await {
                    match chunk {
                        Ok(data) => {
                            let event = Event::default()
                                .event("message")
                                .data(String::from_utf8_lossy(&data).to_string());
                            if tx.send(Ok(event)).await.is_err() {
                                // SSE client disconnected — cancel the request
                                state_clone.session_mgr.cancel_by_request_id(&req_id, "SSE client disconnected").await;
                                return;
                            }
                        }
                        Err(e) => {
                            let _ = tx
                                .send(Ok(Event::default().event("error").data(e.to_string())))
                                .await;
                            break;
                        }
                    }
                }
                let _ = tx
                    .send(Ok(Event::default().event("done").data("[DONE]")))
                    .await;
            }
            Err(e) => {
                let _ = tx
                    .send(Ok(Event::default().event("error").data(e.to_string())))
                    .await;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Sse::new(stream).into_response()
}

async fn async_execute(
    state: &GateState,
    invoker: Arc<dyn RuneInvoker>,
    ctx: RuneContext,
    body: Bytes,
    request_id: String,
) -> axum::response::Response {
    let task_id = request_id.clone();
    state.tasks.insert(
        task_id.clone(),
        TaskInfo {
            task_id: task_id.clone(),
            status: "running".into(),
            result: None,
            error: None,
        },
    );

    let tasks = Arc::clone(&state.tasks);
    tokio::spawn(async move {
        let result = invoker.invoke_once(ctx, body).await;

        // Check if task was cancelled during execution — don't overwrite
        if let Some(task) = tasks.get(&task_id) {
            if task.status == "cancelled" {
                return;
            }
        }

        match result {
            Ok(output) => {
                let val =
                    serde_json::from_slice(&output).unwrap_or(serde_json::Value::Null);
                tasks.insert(
                    task_id.clone(),
                    TaskInfo {
                        task_id,
                        status: "completed".into(),
                        result: Some(val),
                        error: None,
                    },
                );
            }
            Err(e) => {
                tasks.insert(
                    task_id.clone(),
                    TaskInfo {
                        task_id,
                        status: "failed".into(),
                        result: None,
                        error: Some(serde_json::json!({"message": e.to_string()})),
                    },
                );
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "task_id": request_id,
            "status": "running",
        })),
    )
        .into_response()
}

fn error_response(status: StatusCode, code: &str, msg: &str) -> axum::response::Response {
    (
        status,
        Json(serde_json::json!({"error": {"code": code, "message": msg}})),
    )
        .into_response()
}

fn map_error(e: RuneError) -> axum::response::Response {
    let (status, code) = match &e {
        RuneError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "INVALID_INPUT"),
        RuneError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
        RuneError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE"),
        RuneError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "TIMEOUT"),
        RuneError::Cancelled => (
            StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
            "CANCELLED",
        ),
        RuneError::ExecutionFailed { .. } => {
            (StatusCode::INTERNAL_SERVER_ERROR, "EXECUTION_FAILED")
        }
        RuneError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL"),
    };
    error_response(status, code, &e.to_string())
}

fn unique_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    format!("r-{:x}-{:x}", ts, seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use axum::Router;
    use tower::ServiceExt; // for oneshot
    use rune_core::invoker::LocalInvoker;
    use rune_core::relay::Relay;
    use rune_core::resolver::RoundRobinResolver;
    use rune_core::rune::{GateConfig, RuneConfig, make_handler};

    fn test_state() -> GateState {
        let relay = Arc::new(Relay::new());
        let resolver = Arc::new(RoundRobinResolver::new());

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
                },
                Arc::new(LocalInvoker::new(internal_handler)),
                None,
            )
            .unwrap();

        GateState {
            relay,
            resolver,
            tasks: Arc::new(DashMap::new()),
            session_mgr: Arc::new(rune_core::session::SessionManager::new()),
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
        // "internal" has no gate_path — should not be accessible via /internal
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
            .oneshot(
                Request::get("/api/v1/runes")
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
        assert!(json["runes"].is_array());
    }
}
