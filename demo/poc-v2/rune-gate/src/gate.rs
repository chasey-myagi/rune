use std::sync::Arc;
use axum::{
    Router, Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use bytes::Bytes;
use rune_core::rune::{RuneContext, RuneError};
use rune_core::relay::Relay;

/// Gate 的共享状态
#[derive(Clone)]
pub struct GateState {
    pub relay: Arc<Relay>,
}

/// 构建 Gate 路由
pub fn build_router(state: GateState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/runes", get(list_runes))
        .route("/api/v1/runes/:name/run", post(run_rune))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn list_runes(State(state): State<GateState>) -> impl IntoResponse {
    let runes: Vec<serde_json::Value> = state.relay.list()
        .into_iter()
        .map(|(name, gate_path)| {
            serde_json::json!({
                "name": name,
                "gate_path": gate_path,
            })
        })
        .collect();
    Json(serde_json::json!({ "runes": runes }))
}

async fn run_rune(
    State(state): State<GateState>,
    Path(name): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    let invoker = match state.relay.resolve(&name) {
        Some(inv) => inv,
        None => return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": { "code": "NOT_FOUND", "message": format!("rune '{}' not found", name) } })),
        ).into_response(),
    };

    let ctx = RuneContext {
        rune_name: name.clone(),
        request_id: unique_request_id(),
    };

    match invoker.invoke(ctx, body).await {
        Ok(output) => {
            // 尝试返回 JSON，否则返回 raw bytes
            match serde_json::from_slice::<serde_json::Value>(&output) {
                Ok(json) => (StatusCode::OK, Json(json)).into_response(),
                Err(_) => (StatusCode::OK, output).into_response(),
            }
        }
        Err(e) => {
            let (status, code) = match &e {
                RuneError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "INVALID_INPUT"),
                RuneError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
                RuneError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE"),
                RuneError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "TIMEOUT"),
                RuneError::ExecutionFailed { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "EXECUTION_FAILED"),
                RuneError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL"),
            };
            (status, Json(serde_json::json!({
                "error": { "code": code, "message": e.to_string() }
            }))).into_response()
        }
    }
}

fn unique_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap()
        .as_millis() as u64;
    format!("r-{:x}-{:x}", ts, seq)
}
