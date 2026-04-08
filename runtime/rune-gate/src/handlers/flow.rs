use std::convert::Infallible;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{sse::Event, IntoResponse, Sse},
    Json,
};
use bytes::Bytes;
use rune_flow::dag::FlowDefinition;
use rune_flow::engine::FlowRunner;
use rune_store::{StoreError, TaskStatus};
use tokio::sync::mpsc;

use crate::error::{error_response, error_response_with_id, map_flow_error};
use crate::state::{unique_request_id, GateState, RunParams};

pub async fn create_flow(
    State(state): State<GateState>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let body = match axum::body::to_bytes(req.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "failed to read body",
            )
        }
    };

    if body.is_empty() {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_INPUT",
            "empty body: expected JSON",
        );
    }

    let flow: FlowDefinition = match serde_json::from_slice(&body) {
        Ok(f) => f,
        Err(e) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "INVALID_INPUT",
                &format!("invalid JSON: {}", e),
            );
        }
    };

    // Validate: name must not be empty
    if flow.name.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "INVALID_INPUT",
            "flow name must not be empty",
        );
    }

    // Validate: steps must not be empty
    if flow.steps.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "INVALID_INPUT",
            "flow must have at least one step",
        );
    }

    // Register in engine first (validates DAG + holds write lock for
    // atomicity), then persist to store. If store fails, rollback engine.
    let mut engine = state.flow.flow_engine.write().await;

    if engine.get(&flow.name).is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "CONFLICT",
            &format!("flow '{}' already exists", flow.name),
        );
    }

    if let Err(e) = engine.register(flow.clone()) {
        return error_response(StatusCode::BAD_REQUEST, "DAG_ERROR", &e.to_string());
    }

    match state.admin.store.create_flow(&flow).await {
        Ok(()) => {}
        Err(StoreError::DuplicateFlow(_)) => {
            engine.remove(&flow.name);
            return error_response(
                StatusCode::CONFLICT,
                "CONFLICT",
                &format!("flow '{}' already exists", flow.name),
            );
        }
        Err(e) => {
            engine.remove(&flow.name);
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                &e.to_string(),
            );
        }
    }

    (StatusCode::CREATED, Json(serde_json::json!(flow))).into_response()
}

pub async fn list_flows(State(state): State<GateState>) -> impl IntoResponse {
    let engine = state.flow.flow_engine.read().await;
    let entries: Vec<serde_json::Value> = engine
        .list()
        .iter()
        .filter_map(|name| {
            engine.get(name).map(|flow| {
                serde_json::json!({
                    "name": flow.name,
                    "steps_count": flow.steps.len(),
                    "gate_path": flow.gate_path,
                })
            })
        })
        .collect();
    Json(serde_json::Value::Array(entries))
}

pub async fn get_flow(
    State(state): State<GateState>,
    Path(name): Path<String>,
) -> axum::response::Response {
    let engine = state.flow.flow_engine.read().await;
    match engine.get(&name) {
        Some(flow) => (StatusCode::OK, Json(serde_json::json!(flow))).into_response(),
        None => error_response(
            StatusCode::NOT_FOUND,
            "FLOW_NOT_FOUND",
            &format!("flow '{}' not found", name),
        ),
    }
}

pub async fn delete_flow(
    State(state): State<GateState>,
    Path(name): Path<String>,
) -> axum::response::Response {
    match state.admin.store.delete_flow(&name).await {
        Ok(true) => {
            let mut engine = state.flow.flow_engine.write().await;
            engine.remove(&name);
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => {
            // In dev mode, built-in flows (pipeline, single, empty) are
            // registered in-memory without a store row. Fall back to
            // removing from the engine directly.
            let mut engine = state.flow.flow_engine.write().await;
            if engine.remove(&name) {
                StatusCode::NO_CONTENT.into_response()
            } else {
                error_response(
                    StatusCode::NOT_FOUND,
                    "FLOW_NOT_FOUND",
                    &format!("flow '{}' not found", name),
                )
            }
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        ),
    }
}

pub async fn run_flow(
    State(state): State<GateState>,
    Path(name): Path<String>,
    Query(params): Query<RunParams>,
    req: axum::extract::Request,
) -> axum::response::Response {
    let request_id = unique_request_id();
    let body = match axum::body::to_bytes(req.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return error_response_with_id(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "failed to read body",
                Some(&request_id),
            )
        }
    };

    // Default to empty JSON object when body is empty (consistent with rune run)
    let body = if body.is_empty() {
        Bytes::from_static(b"{}")
    } else {
        body
    };

    // Validate JSON
    if serde_json::from_slice::<serde_json::Value>(&body).is_err() {
        return error_response_with_id(
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_INPUT",
            "invalid JSON",
            Some(&request_id),
        );
    }

    // Clone the flow definition and a lightweight runner, then release the
    // read lock.  This prevents long-running flow executions from blocking
    // flow registration (which needs a write lock).
    let (flow_def, runner) = {
        let engine = state.flow.flow_engine.read().await;
        let flow = match engine.get(&name) {
            Some(f) => f.clone(),
            None => {
                return error_response_with_id(
                    StatusCode::NOT_FOUND,
                    "FLOW_NOT_FOUND",
                    &format!("flow '{}' not found", name),
                    Some(&request_id),
                );
            }
        };
        let runner = engine.runner();
        (flow, runner)
    }; // lock released here

    // Stream mode
    if params.stream.unwrap_or(false) {
        return run_flow_stream(body, flow_def, runner).await;
    }

    // Async mode
    if params.async_mode.unwrap_or(false) {
        return run_flow_async(&state, &name, body, flow_def, runner).await;
    }

    // Sync mode (default) — no lock held during execution.
    match runner.execute_flow(&flow_def, body).await {
        Ok(result) => {
            let output_json = serde_json::from_slice::<serde_json::Value>(&result.output)
                .unwrap_or(serde_json::Value::Null);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "output": output_json,
                    "steps_executed": result.steps_executed,
                })),
            )
                .into_response()
        }
        Err(e) => map_flow_error(e, Some(&request_id)),
    }
}

pub async fn run_flow_async(
    state: &GateState,
    flow_name: &str,
    body: Bytes,
    flow_def: FlowDefinition,
    runner: FlowRunner,
) -> axum::response::Response {
    let task_id = unique_request_id();
    let input_str = String::from_utf8_lossy(&body).to_string();

    if let Err(e) = state
        .admin
        .store
        .insert_task(&task_id, &format!("flow:{}", flow_name), Some(&input_str))
        .await
    {
        return error_response_with_id(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
            Some(&task_id),
        );
    }
    let _ = state
        .admin
        .store
        .update_task_status(&task_id, TaskStatus::Running, None, None)
        .await;

    let store = Arc::clone(&state.admin.store);
    let tid = task_id.clone();

    tokio::spawn(async move {
        match runner.execute_flow(&flow_def, body).await {
            Ok(result) => {
                let output_str = String::from_utf8_lossy(&result.output).to_string();
                let val = serde_json::from_str::<serde_json::Value>(&output_str)
                    .unwrap_or(serde_json::Value::Null);
                let combined = serde_json::json!({
                    "output": val,
                    "steps_executed": result.steps_executed,
                });
                let _ = store
                    .update_task_status(
                        &tid,
                        TaskStatus::Completed,
                        Some(&combined.to_string()),
                        None,
                    )
                    .await;
            }
            Err(e) => {
                let _ = store
                    .update_task_status(&tid, TaskStatus::Failed, None, Some(&e.to_string()))
                    .await;
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "task_id": task_id,
            "flow": flow_name,
            "status": "running",
        })),
    )
        .into_response()
}

pub async fn run_flow_stream(
    body: Bytes,
    flow_def: FlowDefinition,
    runner: FlowRunner,
) -> axum::response::Response {
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(32);

    tokio::spawn(async move {
        match runner.execute_flow(&flow_def, body).await {
            Ok(result) => {
                let output_json = serde_json::from_slice::<serde_json::Value>(&result.output)
                    .unwrap_or(serde_json::Value::Null);
                let msg = serde_json::json!({
                    "output": output_json,
                    "steps_executed": result.steps_executed,
                });
                let _ = tx
                    .send(Ok(Event::default().event("result").data(msg.to_string())))
                    .await;
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
