use std::convert::Infallible;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{sse::Event, IntoResponse, Sse},
    Json,
};
use bytes::Bytes;
use rune_core::trace::TRACE_ID_KEY;
use rune_flow::dag::FlowDefinition;
use rune_flow::engine::{FlowProgressEvent, FlowRunner};
use rune_store::{StoreError, TaskStatus};
use tokio::sync::mpsc;

use crate::error::{error_response, error_response_with_id, map_flow_error};
use crate::state::{unique_request_id, GateState, RunParams};
use crate::trace_headers::{
    apply_trace_response_headers, context_from_headers, request_id_from_headers,
};

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

    // Phase 1: Register in engine under write lock (validates DAG + conflicts).
    // Release lock before I/O to avoid blocking concurrent reads.
    // Returns a registration epoch used for safe rollback in Phase 2.
    let reg_epoch = {
        let mut engine = state.flow.flow_engine.write().await;

        if engine.get(&flow.name).is_some() {
            return error_response(
                StatusCode::CONFLICT,
                "CONFLICT",
                &format!("flow '{}' already exists", flow.name),
            );
        }

        // Check gate_path conflict with existing flows
        if let Some(ref gate_path) = flow.gate_path {
            if let Some(existing) = engine.find_by_gate_path(gate_path, &flow.name) {
                return error_response(
                    StatusCode::CONFLICT,
                    "CONFLICT",
                    &format!(
                        "gate_path '{}' is already used by flow '{}'",
                        gate_path, existing
                    ),
                );
            }
        }

        match engine.register(flow.clone()) {
            Ok(epoch) => epoch,
            Err(e) => return error_response(StatusCode::BAD_REQUEST, "DAG_ERROR", &e.to_string()),
        }
    }; // Write lock released here — concurrent reads are no longer blocked.

    // Phase 2: Persist to store without holding engine lock.
    // On failure, rollback using the epoch so we only remove the flow we
    // registered (not a different flow that may have registered with the
    // same name after our lock release).
    match state.admin.store.create_flow(&flow).await {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::json!(flow))).into_response(),
        Err(StoreError::DuplicateFlow(_)) => {
            state
                .flow
                .flow_engine
                .write()
                .await
                .remove_if_epoch_matches(&flow.name, reg_epoch);
            error_response(
                StatusCode::CONFLICT,
                "CONFLICT",
                &format!("flow '{}' already exists", flow.name),
            )
        }
        Err(e) => {
            state
                .flow
                .flow_engine
                .write()
                .await
                .remove_if_epoch_matches(&flow.name, reg_epoch);
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                &e.to_string(),
            )
        }
    }
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
    let request_id = request_id_from_headers(req.headers()).unwrap_or_else(unique_request_id);
    let trace_context = context_from_headers(req.headers());
    tracing::info!(
        trace_id = %trace_context.get(TRACE_ID_KEY).map(String::as_str).unwrap_or(""),
        request_id = %request_id,
        flow_name = %name,
        mode = if params.async_mode.unwrap_or(false) {
            "async"
        } else if params.stream.unwrap_or(false) {
            "stream"
        } else {
            "sync"
        },
        "flow invoke"
    );
    let body = match axum::body::to_bytes(req.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return traced_response(
                error_response_with_id(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    "failed to read body",
                    Some(&request_id),
                ),
                &request_id,
                &trace_context,
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
        return traced_response(
            error_response_with_id(
                StatusCode::UNPROCESSABLE_ENTITY,
                "INVALID_INPUT",
                "invalid JSON",
                Some(&request_id),
            ),
            &request_id,
            &trace_context,
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
                return traced_response(
                    error_response_with_id(
                        StatusCode::NOT_FOUND,
                        "FLOW_NOT_FOUND",
                        &format!("flow '{}' not found", name),
                        Some(&request_id),
                    ),
                    &request_id,
                    &trace_context,
                );
            }
        };
        let runner = engine.runner();
        (flow, runner)
    }; // lock released here

    // Stream mode
    if params.stream.unwrap_or(false) {
        return run_flow_stream(
            body,
            flow_def,
            runner,
            trace_context.clone(),
            request_id.clone(),
        )
        .await;
    }

    // Async mode
    if params.async_mode.unwrap_or(false) {
        return run_flow_async(
            &state,
            &name,
            body,
            flow_def,
            runner,
            trace_context.clone(),
            request_id.clone(),
        )
        .await;
    }

    // Sync mode (default) — no lock held during execution.
    match runner
        .execute_flow_with_context(
            &flow_def,
            body,
            trace_context.clone(),
            Some(request_id.clone()),
        )
        .await
    {
        Ok(result) => {
            let output_json = match std::str::from_utf8(&result.output) {
                Ok(s) => serde_json::from_str::<serde_json::Value>(s)
                    .unwrap_or_else(|_| serde_json::Value::String(s.to_string())),
                Err(_) => serde_json::Value::String(format!("hex:{}", hex::encode(&result.output))),
            };
            traced_response(
                (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "output": output_json,
                        "steps_executed": result.steps_executed,
                    })),
                )
                    .into_response(),
                &request_id,
                &trace_context,
            )
        }
        Err(e) => traced_response(
            map_flow_error(e, Some(&request_id)),
            &request_id,
            &trace_context,
        ),
    }
}

pub async fn run_flow_async(
    state: &GateState,
    flow_name: &str,
    body: Bytes,
    flow_def: FlowDefinition,
    runner: FlowRunner,
    trace_context: std::collections::HashMap<String, String>,
    request_id: String,
) -> axum::response::Response {
    let task_id = request_id.clone();
    let input_str = match std::str::from_utf8(&body) {
        Ok(s) => s.to_string(),
        Err(_) => format!("hex:{}", hex::encode(&body)),
    };

    // Insert task and mark it Running atomically.  A single transaction
    // prevents a crash between the two steps from leaving the task in
    // a permanently-pending state.
    if let Err(e) = state
        .admin
        .store
        .insert_task_and_start(&task_id, &format!("flow:{}", flow_name), Some(&input_str))
        .await
    {
        return traced_response(
            error_response_with_id(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                &e.to_string(),
                Some(&request_id),
            ),
            &request_id,
            &trace_context,
        );
    }

    let store = Arc::clone(&state.admin.store);
    let tid = task_id.clone();
    let response_trace_context = trace_context.clone();
    let response_request_id = request_id.clone();

    // Spawn the execution task and keep the JoinHandle so that panics are
    // detected: a panic inside tokio::spawn drops silently unless the handle
    // is awaited.  The monitor task marks the DB entry as Failed rather than
    // leaving it permanently in the `running` state.
    let exec_handle = tokio::spawn(async move {
        match runner
            .execute_flow_with_context(&flow_def, body, trace_context, Some(request_id))
            .await
        {
            Ok(result) => {
                // Binary outputs are encoded as "hex:..." — preserve that string
                // rather than trying to JSON-parse it (which would silently lose data).
                let output_val = match std::str::from_utf8(&result.output) {
                    Ok(s) => serde_json::from_str::<serde_json::Value>(s)
                        .unwrap_or_else(|_| serde_json::Value::String(s.to_string())),
                    Err(_) => {
                        serde_json::Value::String(format!("hex:{}", hex::encode(&result.output)))
                    }
                };
                let combined = serde_json::json!({
                    "output": output_val,
                    "steps_executed": result.steps_executed,
                });
                // CAS: only complete if not already cancelled
                let _ = store
                    .complete_task_if_not_cancelled(
                        &tid,
                        TaskStatus::Completed,
                        Some(&combined.to_string()),
                        None,
                    )
                    .await;
            }
            Err(e) => {
                // CAS: only mark failed if not already cancelled
                let _ = store
                    .complete_task_if_not_cancelled(
                        &tid,
                        TaskStatus::Failed,
                        None,
                        Some(&e.to_string()),
                    )
                    .await;
            }
        }
    });

    // Panic monitor: if the exec task panics, the DB entry would otherwise be
    // permanently stuck in `running`. Detect the panic and mark as Failed.
    let store_mon = Arc::clone(&state.admin.store);
    let tid_mon = task_id.clone();
    tokio::spawn(async move {
        if let Err(e) = exec_handle.await {
            if e.is_panic() {
                tracing::error!(task_id = %tid_mon, "async flow task panicked");
                let _ = store_mon
                    .complete_task_if_not_cancelled(
                        &tid_mon,
                        TaskStatus::Failed,
                        None,
                        Some("internal panic"),
                    )
                    .await;
            }
        }
    });

    traced_response(
        (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "task_id": task_id,
                "flow": flow_name,
                "status": "running",
            })),
        )
            .into_response(),
        &response_request_id,
        &response_trace_context,
    )
}

pub async fn run_flow_stream(
    body: Bytes,
    flow_def: FlowDefinition,
    runner: FlowRunner,
    trace_context: std::collections::HashMap<String, String>,
    request_id: String,
) -> axum::response::Response {
    let (event_tx, event_rx) = mpsc::channel::<Result<Event, Infallible>>(64);
    let response_trace_context = trace_context.clone();
    let response_request_id = request_id.clone();

    let (progress_tx, mut progress_rx) = mpsc::channel::<FlowProgressEvent>(64);

    // Clone event_tx before it is moved into the exec task.
    let fwd_tx = event_tx.clone();
    // Also clone for the disconnect-detection task (before fwd_tx is moved below).
    let dc_tx = fwd_tx.clone();

    // Main execution task — keep JoinHandle so the DC task can select! on completion.
    let runner = runner.with_progress(progress_tx);
    let exec_handle = tokio::spawn(async move {
        match runner
            .execute_flow_with_context(&flow_def, body, trace_context.clone(), Some(request_id))
            .await
        {
            Ok(result) => {
                // Binary outputs are encoded as "hex:..." — preserve that string rather
                // than silently dropping non-UTF-8 data (which serde_json::Null would do).
                let output_json = match std::str::from_utf8(&result.output) {
                    Ok(s) => serde_json::from_str::<serde_json::Value>(s)
                        .unwrap_or_else(|_| serde_json::Value::String(s.to_string())),
                    Err(_) => {
                        serde_json::Value::String(format!("hex:{}", hex::encode(&result.output)))
                    }
                };
                let msg = serde_json::json!({
                    "output": output_json,
                    "steps_executed": result.steps_executed,
                });
                let _ = event_tx
                    .send(Ok(Event::default().event("result").data(msg.to_string())))
                    .await;
                let _ = event_tx
                    .send(Ok(Event::default().event("done").data("[DONE]")))
                    .await;
            }
            Err(e) => {
                let _ = event_tx
                    .send(Ok(Event::default().event("error").data(e.to_string())))
                    .await;
            }
        }
    });
    let exec_abort = exec_handle.abort_handle();

    // Disconnect detection: abort exec when the client disconnects (dc_tx.closed() fires).
    // Uses select! so this task exits naturally when exec completes normally — avoiding the
    // deadlock where closed() would wait for event_rx to drop while to_bytes() waits for all
    // senders to drop.
    tokio::spawn(async move {
        tokio::select! {
            _ = exec_handle => {}           // exec finished; exit without aborting
            _ = dc_tx.closed() => { exec_abort.abort(); }  // client disconnected; abort exec
        }
    });

    // Progress forwarding task: serialise FlowProgressEvents to SSE "flow_progress" events.
    // When exec completes (or is aborted), runner drops → progress_tx drops → progress_rx
    // returns None and this loop exits naturally.
    tokio::spawn(async move {
        while let Some(evt) = progress_rx.recv().await {
            let data = serde_json::to_string(&evt).unwrap_or_default();
            let sse_evt = Event::default().event("flow_progress").data(data);
            if fwd_tx.send(Ok(sse_evt)).await.is_err() {
                break;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(event_rx);
    let mut response = Sse::new(stream).into_response();
    apply_trace_response_headers(&mut response, &response_request_id, &response_trace_context);
    response
}

fn traced_response(
    mut response: axum::response::Response,
    request_id: &str,
    context: &std::collections::HashMap<String, String>,
) -> axum::response::Response {
    apply_trace_response_headers(&mut response, request_id, context);
    response
}
