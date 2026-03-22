use std::convert::Infallible;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Sse, sse::Event},
    routing::{delete, get, post},
};
use bytes::Bytes;
use tokio::sync::mpsc;
use tower_http::cors::{Any, CorsLayer};

use rune_core::auth::KeyVerifier;
use rune_core::invoker::RuneInvoker;
use rune_core::relay::Relay;
use rune_core::resolver::Resolver;
use rune_core::rune::{RuneContext, RuneError};
use rune_store::{CallLog, RuneStore, TaskStatus};

/// Gate shared state
#[derive(Clone)]
pub struct GateState {
    pub relay: Arc<Relay>,
    pub resolver: Arc<dyn Resolver>,
    pub store: Arc<RuneStore>,
    pub key_verifier: Arc<dyn KeyVerifier>,
    pub session_mgr: Arc<rune_core::session::SessionManager>,
    pub auth_enabled: bool,
    pub exempt_routes: Vec<String>,
    pub cors_origins: Vec<String>,
    pub dev_mode: bool,
    pub started_at: Instant,
}

#[derive(serde::Deserialize, Default)]
pub struct RunParams {
    pub stream: Option<bool>,
    #[serde(rename = "async")]
    pub async_mode: Option<bool>,
}

/// Build the Gate Router with auth middleware, CORS, and management API.
pub fn build_router(state: GateState, extra_routes: Option<Router<GateState>>) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/api/v1/runes", get(list_runes))
        .route("/api/v1/runes/:name/run", post(run_rune))
        .route("/api/v1/tasks/:id", get(get_task).delete(delete_task))
        // Management API
        .route("/api/v1/status", get(mgmt_status))
        .route("/api/v1/casters", get(mgmt_casters))
        .route("/api/v1/stats", get(mgmt_stats))
        .route("/api/v1/logs", get(mgmt_logs))
        .route("/api/v1/keys", get(mgmt_list_keys).post(mgmt_create_key))
        .route("/api/v1/keys/:id", delete(mgmt_revoke_key));

    if let Some(extra) = extra_routes {
        router = router.merge(extra);
    }

    // Build CORS layer
    let cors = if state.cors_origins.is_empty() {
        CorsLayer::permissive()
    } else {
        let origins: Vec<_> = state
            .cors_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any)
    };

    router
        .fallback(dynamic_rune_handler)
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state, auth_middleware))
        .layer(cors)
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

async fn auth_middleware(
    State(state): State<GateState>,
    req: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    if !state.auth_enabled {
        return next.run(req).await;
    }

    let path = req.uri().path().to_string();
    if state.exempt_routes.iter().any(|r| path.starts_with(r)) {
        return next.run(req).await;
    }

    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string());

    match auth_header {
        Some(key) => {
            if state.key_verifier.verify_gate_key(&key).await {
                next.run(req).await
            } else {
                error_response(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "invalid api key")
            }
        }
        None => error_response(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "missing authorization header",
        ),
    }
}

// ---------------------------------------------------------------------------
// Core handlers
// ---------------------------------------------------------------------------

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
    match state.store.get_task(&id) {
        Ok(Some(task)) => (StatusCode::OK, Json(serde_json::json!(task))).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "task not found"),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string()),
    }
}

async fn delete_task(
    State(state): State<GateState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.store.get_task(&id) {
        Ok(None) => error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "task not found"),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string()),
        Ok(Some(task)) => match task.status {
            TaskStatus::Completed | TaskStatus::Failed => error_response(
                StatusCode::CONFLICT,
                "CONFLICT",
                &format!("task already {}", task.status.as_str()),
            ),
            TaskStatus::Cancelled => (
                StatusCode::OK,
                Json(serde_json::json!({"task_id": id, "status": "cancelled"})),
            )
                .into_response(),
            _ => {
                // running or pending — cancel
                state
                    .session_mgr
                    .cancel_by_request_id(&id, "cancelled by user")
                    .await;
                let _ = state.store.update_task_status(
                    &id,
                    TaskStatus::Cancelled,
                    None,
                    Some("cancelled by user"),
                );
                (
                    StatusCode::OK,
                    Json(serde_json::json!({"task_id": id, "status": "cancelled"})),
                )
                    .into_response()
            }
        },
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

/// gate_path dynamic routing: find rune by matching path
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

/// Unified rune execution logic for both debug and dynamic routes
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

    // Check supports_stream
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

    // Determine mode string for call log
    let mode = if params.async_mode.unwrap_or(false) {
        "async"
    } else if params.stream.unwrap_or(false) {
        "stream"
    } else {
        "sync"
    };

    // async mode
    if params.async_mode.unwrap_or(false) {
        return async_execute(state, invoker, ctx, body, request_id).await;
    }

    // stream mode
    if params.stream.unwrap_or(false) {
        return stream_execute(state, &request_id, invoker, ctx, body).await;
    }

    // sync mode (default)
    let start = Instant::now();
    let input_size = body.len() as i64;
    let rune_name_owned = rune_name.to_string();
    let req_id = request_id.clone();

    let response = sync_execute(invoker, ctx, body).await;

    // Record call log (best-effort)
    let latency_ms = start.elapsed().as_millis() as i64;
    let status_code = response.status().as_u16() as i32;
    let _ = state.store.insert_log(&CallLog {
        id: 0,
        request_id: req_id,
        rune_name: rune_name_owned,
        mode: mode.into(),
        caster_id: None,
        latency_ms,
        status_code,
        input_size,
        output_size: 0, // not easily available after response is built
        timestamp: rune_store::now_iso8601(),
    });

    response
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
                                state_clone
                                    .session_mgr
                                    .cancel_by_request_id(&req_id, "SSE client disconnected")
                                    .await;
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
    let rune_name = ctx.rune_name.clone();
    let input_str = String::from_utf8_lossy(&body).to_string();

    // Insert task into store
    if let Err(e) = state.store.insert_task(&task_id, &rune_name, Some(&input_str)) {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        );
    }
    if let Err(e) =
        state
            .store
            .update_task_status(&task_id, TaskStatus::Running, None, None)
    {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        );
    }

    let store = Arc::clone(&state.store);
    let start = Instant::now();
    let input_size = body.len() as i64;
    let rune_name_log = rune_name.clone();

    tokio::spawn(async move {
        let result = invoker.invoke_once(ctx, body).await;

        // Check if task was cancelled during execution
        if let Ok(Some(task)) = store.get_task(&task_id) {
            if task.status == TaskStatus::Cancelled {
                return;
            }
        }

        let (status, output_size) = match result {
            Ok(ref output) => {
                let output_str = String::from_utf8_lossy(output).to_string();
                let size = output.len() as i64;
                let _ = store.update_task_status(
                    &task_id,
                    TaskStatus::Completed,
                    Some(&output_str),
                    None,
                );
                (200i32, size)
            }
            Err(ref e) => {
                let _ = store.update_task_status(
                    &task_id,
                    TaskStatus::Failed,
                    None,
                    Some(&e.to_string()),
                );
                (500i32, 0i64)
            }
        };

        // Record call log
        let latency_ms = start.elapsed().as_millis() as i64;
        let _ = store.insert_log(&CallLog {
            id: 0,
            request_id: task_id,
            rune_name: rune_name_log,
            mode: "async".into(),
            caster_id: None,
            latency_ms,
            status_code: status,
            input_size,
            output_size,
            timestamp: rune_store::now_iso8601(),
        });
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

// ---------------------------------------------------------------------------
// Management API handlers
// ---------------------------------------------------------------------------

async fn mgmt_status(State(state): State<GateState>) -> impl IntoResponse {
    let uptime_secs = state.started_at.elapsed().as_secs();
    let caster_count = state.session_mgr.caster_count();
    let rune_count = state.relay.list().len();

    Json(serde_json::json!({
        "uptime_secs": uptime_secs,
        "caster_count": caster_count,
        "rune_count": rune_count,
        "dev_mode": state.dev_mode,
    }))
}

async fn mgmt_casters(State(state): State<GateState>) -> impl IntoResponse {
    let casters = state.session_mgr.list_caster_ids();
    Json(serde_json::json!({"casters": casters}))
}

async fn mgmt_stats(State(state): State<GateState>) -> impl IntoResponse {
    match state.store.call_stats() {
        Ok((total, by_rune)) => {
            let rune_stats: Vec<serde_json::Value> = by_rune
                .into_iter()
                .map(|(name, count, avg_latency)| {
                    serde_json::json!({
                        "rune_name": name,
                        "count": count,
                        "avg_latency_ms": avg_latency,
                    })
                })
                .collect();
            Json(serde_json::json!({
                "total_calls": total,
                "by_rune": rune_stats,
            }))
            .into_response()
        }
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}

#[derive(serde::Deserialize, Default)]
pub struct LogQuery {
    pub rune: Option<String>,
    pub limit: Option<i64>,
}

async fn mgmt_logs(
    State(state): State<GateState>,
    Query(params): Query<LogQuery>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(50).min(500);
    match state.store.query_logs(params.rune.as_deref(), limit) {
        Ok(logs) => Json(serde_json::json!({"logs": logs})).into_response(),
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}

#[derive(serde::Deserialize)]
pub struct CreateKeyRequest {
    pub key_type: String,
    pub label: String,
}

async fn mgmt_create_key(
    State(state): State<GateState>,
    Json(req): Json<CreateKeyRequest>,
) -> impl IntoResponse {
    let key_type = match req.key_type.as_str() {
        "gate" => rune_store::KeyType::Gate,
        "caster" => rune_store::KeyType::Caster,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "key_type must be 'gate' or 'caster'",
            )
        }
    };

    match state.store.create_key(key_type, &req.label) {
        Ok(result) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "raw_key": result.raw_key,
                "key": result.api_key,
            })),
        )
            .into_response(),
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}

async fn mgmt_list_keys(State(state): State<GateState>) -> impl IntoResponse {
    match state.store.list_keys() {
        Ok(keys) => Json(serde_json::json!({"keys": keys})).into_response(),
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}

async fn mgmt_revoke_key(
    State(state): State<GateState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    match state.store.revoke_key(id) {
        Ok(()) => Json(serde_json::json!({"status": "revoked", "id": id})).into_response(),
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
    use rune_core::auth::NoopVerifier;
    use rune_core::invoker::LocalInvoker;
    use rune_core::relay::Relay;
    use rune_core::resolver::RoundRobinResolver;
    use rune_core::rune::{GateConfig, RuneConfig, make_handler};
    use tower::ServiceExt;

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
            store,
            key_verifier,
            session_mgr: Arc::new(rune_core::session::SessionManager::new(
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(35),
            )),
            auth_enabled: false,
            exempt_routes: vec!["/health".to_string()],
            cors_origins: vec![],
            dev_mode: true,
            started_at: Instant::now(),
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

    #[tokio::test]
    async fn test_auth_blocks_without_key() {
        let mut state = test_state();
        state.auth_enabled = true;
        state.key_verifier = Arc::new(rune_store::StoreKeyVerifier::new(state.store.clone()));
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
        state.auth_enabled = true;
        state.key_verifier = Arc::new(rune_store::StoreKeyVerifier::new(state.store.clone()));
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
        state.auth_enabled = true;
        let key_result = state
            .store
            .create_key(rune_store::KeyType::Gate, "test")
            .unwrap();
        state.key_verifier = Arc::new(rune_store::StoreKeyVerifier::new(state.store.clone()));
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
            .oneshot(
                Request::get("/api/v1/keys")
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
        let task = state.store.get_task(&task_id).unwrap();
        assert!(task.is_some());
        let task = task.unwrap();
        assert_eq!(task.status, TaskStatus::Completed);
    }
}
