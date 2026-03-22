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
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
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
            .store
            .insert_task("done-task", "echo", Some("input"))
            .unwrap();
        state
            .store
            .update_task_status("done-task", TaskStatus::Completed, Some("result"), None)
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
            .store
            .insert_task("fail-task", "echo", Some("input"))
            .unwrap();
        state
            .store
            .update_task_status("fail-task", TaskStatus::Failed, None, Some("boom"))
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
                        r#"{"key_type":"admin","label":"bad type"}"#,
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
            .oneshot(
                Request::get("/api/v1/stats")
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
        assert_eq!(json["total_calls"], 0);
        assert_eq!(json["by_rune"].as_array().unwrap().len(), 0);
    }

    // -----------------------------------------------------------------------
    // Bearer prefix variants
    // -----------------------------------------------------------------------

    fn auth_state() -> GateState {
        let mut state = test_state();
        state.auth_enabled = true;
        state.key_verifier =
            Arc::new(rune_store::StoreKeyVerifier::new(state.store.clone()));
        state
    }

    #[tokio::test]
    async fn test_bearer_lowercase_rejected() {
        // The middleware expects exactly "Bearer " (capital B).
        // "bearer " (lowercase) should fail to strip the prefix and return 401.
        let state = auth_state();
        let key_result = state
            .store
            .create_key(rune_store::KeyType::Gate, "test")
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
            .store
            .create_key(rune_store::KeyType::Gate, "test")
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
            .store
            .create_key(rune_store::KeyType::Gate, "test")
            .unwrap();
        let app = build_router(state, None);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/echo")
                    .header(
                        "authorization",
                        format!("Bearer  {}", key_result.raw_key),
                    )
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
            .store
            .create_key(rune_store::KeyType::Gate, "test")
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
        state.cors_origins = vec!["https://allowed.example.com".to_string()];
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
            acao.is_none()
                || acao.unwrap().to_str().unwrap() != "https://evil.example.com",
            "ACAO should NOT echo an unlisted origin"
        );
    }

    #[tokio::test]
    async fn test_cors_restricted_allows_listed_origin() {
        let mut state = test_state();
        state.cors_origins = vec!["https://allowed.example.com".to_string()];
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
        assert_eq!(
            acao.to_str().unwrap(),
            "https://allowed.example.com"
        );
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
        assert!(body.is_empty(), "echo of empty body should be empty, got {} bytes", body.len());
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
    // #3  HTTP method mismatch — GET on POST-only gate_path (dynamic fallback)
    //     The dynamic_rune_handler does not enforce method; it simply matches
    //     path. This test documents actual behavior.
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

        // The dynamic fallback matches any method if the path matches.
        // Echo with empty body → 200 OK with empty body.
        assert_eq!(response.status(), StatusCode::OK);
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
                    },
                    Arc::new(LocalInvoker::new(handler)),
                    None,
                )
                .unwrap();
        }

        let state = GateState {
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

            assert_eq!(response.status(), StatusCode::OK, "path {} should route correctly", path);
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                json["rune"], expected_name,
                "path {} should return rune={}", path, expected_name
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
        assert!(json["output"].is_string(), "completed task should have output field");
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
            .store
            .insert_task("fail-async", "echo", Some("input"))
            .unwrap();
        state
            .store
            .update_task_status(
                "fail-async",
                TaskStatus::Failed,
                None,
                Some("handler crashed"),
            )
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
        assert!(json["error"].is_string(), "failed task should have error field");
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
            .store
            .insert_task("cancel-run", "echo", Some("data"))
            .unwrap();
        state
            .store
            .update_task_status("cancel-run", TaskStatus::Running, None, None)
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
        let task = state.store.get_task("cancel-run").unwrap().unwrap();
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
            .store
            .insert_task("idempotent-cancel", "echo", Some("data"))
            .unwrap();
        state
            .store
            .update_task_status("idempotent-cancel", TaskStatus::Cancelled, None, Some("first cancel"))
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
            let task = state.store.get_task(tid).unwrap();
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
                    },
                    Arc::new(LocalInvoker::new(handler)),
                    None,
                )
                .unwrap();
        }

        let state = GateState {
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
        };

        let app = build_router(state, None);
        let response = app
            .oneshot(
                Request::get("/api/v1/runes")
                    .body(Body::empty())
                    .unwrap(),
            )
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
            assert!(rune["gate_path"].is_string(), "each rune should have a gate_path");
        }

        // Verify specific names are present
        let names: Vec<&str> = runes.iter().map(|r| r["name"].as_str().unwrap()).collect();
        for i in 0..5 {
            let expected = format!("rune_{}", i);
            assert!(names.contains(&expected.as_str()), "should contain {}", expected);
        }
    }

    // =======================================================================
    // #11 Management /api/v1/casters endpoint
    // =======================================================================

    #[tokio::test]
    async fn test_mgmt_casters_endpoint() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::get("/api/v1/casters")
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
        // No casters registered in test state, so list should be empty
        assert!(json["casters"].is_array(), "response should have 'casters' array");
        assert_eq!(json["casters"].as_array().unwrap().len(), 0);
    }

    // =======================================================================
    // #12 Management /api/v1/status — all fields present with correct types
    // =======================================================================

    #[tokio::test]
    async fn test_mgmt_status_all_fields() {
        let app = test_router();
        let response = app
            .oneshot(
                Request::get("/api/v1/status")
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

        // Verify all expected fields exist with correct types
        assert!(json["uptime_secs"].is_u64(), "uptime_secs should be a number");
        assert!(json["rune_count"].is_u64(), "rune_count should be a number");
        assert!(json["caster_count"].is_u64(), "caster_count should be a number");
        assert!(json["dev_mode"].is_boolean(), "dev_mode should be a boolean");

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
            .oneshot(
                Request::get("/api/v1/stats")
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

        assert_eq!(json["total_calls"], 5, "total should be 5 (3 echo + 2 internal)");

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
            .store
            .insert_task("pending-cancel", "echo", Some("data"))
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

        let task = state.store.get_task("pending-cancel").unwrap().unwrap();
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
        let task = state.store.get_task(task_id).unwrap().unwrap();
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
        assert!(json["logs"].is_array(), "should return a logs array even with negative limit");
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
        assert_eq!(logs.len(), 3, "should return all 3 logs (capped at 500, but only 3 exist)");
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
                },
                Arc::new(LocalInvoker::new(handler)),
                None,
            )
            .unwrap();

        let state = GateState {
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
                },
                Arc::new(LocalInvoker::new(handler)),
                None,
            )
            .unwrap();

        let state = GateState {
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
                },
                Arc::new(LocalInvoker::new(handler)),
                None,
            )
            .unwrap();

        let state = GateState {
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
}
