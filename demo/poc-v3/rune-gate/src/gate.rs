use std::sync::Arc;
use std::convert::Infallible;
use axum::{
    Router, Json,
    extract::{Path, State, Query},
    http::StatusCode,
    response::{IntoResponse, Sse, sse::Event},
    routing::{get, post},
};
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc;
use rune_core::rune::{RuneContext, RuneError};
use rune_core::relay::Relay;

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
        .with_state(state)
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

fn unique_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap()
        .as_millis() as u64;
    format!("r-{:x}-{:x}", ts, seq)
}
