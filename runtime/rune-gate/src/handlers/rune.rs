use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{sse::Event, IntoResponse, Sse},
    Json,
};
use bytes::Bytes;
use tokio::sync::mpsc;

use rune_core::invoker::RuneInvoker;
use rune_core::rune::RuneContext;
use rune_schema::validator::{validate_input, validate_output};
use rune_store::{CallLog, TaskStatus};

use crate::error::{error_response, error_response_with_id, map_error};
use crate::multipart::{is_multipart, parse_multipart, FileMetadata};
use crate::state::{unique_request_id, GateState, RunParams};

pub async fn run_rune(
    State(state): State<GateState>,
    Path(name): Path<String>,
    Query(params): Query<RunParams>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // Extract X-Rune-Labels header for label-based routing
    let labels = parse_labels_header(
        req.headers()
            .get("x-rune-labels")
            .and_then(|v| v.to_str().ok()),
    );

    // Read body with appropriate size limit
    let max_body = if is_multipart(&content_type) {
        (state.rune.max_upload_size_mb as usize + 10) * 1024 * 1024
    } else {
        1024 * 1024
    };

    let body = match axum::body::to_bytes(req.into_body(), max_body).await {
        Ok(b) => b,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "failed to read body",
            );
        }
    };

    execute_rune(&state, &name, params, body, &content_type, &labels).await
}

/// gate_path dynamic routing: find rune by matching path
pub async fn dynamic_rune_handler(
    State(state): State<GateState>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // Extract X-Rune-Labels header for label-based routing
    let labels = parse_labels_header(
        req.headers()
            .get("x-rune-labels")
            .and_then(|v| v.to_str().ok()),
    );

    // Read body with appropriate size limit
    let max_body = if is_multipart(&content_type) {
        (state.rune.max_upload_size_mb as usize + 10) * 1024 * 1024
    } else {
        1024 * 1024
    };

    let body = match axum::body::to_bytes(req.into_body(), max_body).await {
        Ok(b) => b,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "failed to read body",
            );
        }
    };

    let rune_name = state.rune.relay.resolve_by_gate_path(&method, &path);

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

    execute_rune(&state, &rune_name, params, body, &content_type, &labels).await
}

/// Parse X-Rune-Labels header value into a HashMap.
/// Format: "key1=value1,key2=value2"
pub fn parse_labels_header(
    header_value: Option<&str>,
) -> std::collections::HashMap<String, String> {
    let mut labels = std::collections::HashMap::new();
    if let Some(val) = header_value {
        for pair in val.split(',') {
            let pair = pair.trim();
            if let Some((k, v)) = pair.split_once('=') {
                labels.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    labels
}

/// Unified rune execution logic for both debug and dynamic routes
pub async fn execute_rune(
    state: &GateState,
    rune_name: &str,
    params: RunParams,
    body: Bytes,
    content_type: &str,
    labels: &std::collections::HashMap<String, String>,
) -> axum::response::Response {
    // Use label-based routing if labels are provided
    let invoker = if labels.is_empty() {
        state.rune.relay.resolve(rune_name, &*state.rune.resolver)
    } else {
        state
            .rune
            .relay
            .resolve_with_labels(rune_name, labels, &*state.rune.resolver)
    };
    let invoker = match invoker {
        Some(inv) => inv,
        None => {
            // If labels were specified but no match, return 503 (no matching caster)
            if !labels.is_empty() {
                return error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "NO_MATCHING_CASTER",
                    &format!("no caster matching labels for rune '{}'", rune_name),
                );
            }
            return error_response(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                &format!("rune '{}' not found", rune_name),
            );
        }
    };

    // Get RuneConfig for schema validation and stream support (single lookup)
    let (input_schema, output_schema, supports_stream) =
        if let Some(entries) = state.rune.relay.find(rune_name) {
            if let Some(first) = entries.value().first() {
                (
                    first.config.input_schema.clone(),
                    first.config.output_schema.clone(),
                    first.config.supports_stream,
                )
            } else {
                (None, None, false)
            }
        } else {
            (None, None, false)
        };

    let request_id = unique_request_id();

    // Check supports_stream
    if params.stream.unwrap_or(false) && !supports_stream {
        return error_response_with_id(
            StatusCode::BAD_REQUEST,
            "STREAM_NOT_SUPPORTED",
            &format!("rune '{}' does not support streaming", rune_name),
            Some(&request_id),
        );
    }

    // Handle multipart vs regular body
    let (rune_input, file_metadata, file_ids) = if is_multipart(content_type) {
        match parse_multipart(state, content_type, body, &request_id).await {
            Ok(result) => {
                let rune_input = result.json_input.unwrap_or_default();
                (rune_input, Some(result.files), result.file_ids)
            }
            Err(err_response) => return err_response,
        }
    } else {
        (body, None, Vec::new())
    };

    // Input schema validation — also validate multipart when JSON input is present
    let should_validate = file_metadata.is_none() || !rune_input.is_empty();
    if should_validate {
        if let Err(e) = validate_input(input_schema.as_deref(), &rune_input) {
            return error_response_with_id(
                StatusCode::UNPROCESSABLE_ENTITY,
                "VALIDATION_FAILED",
                &e.to_string(),
                Some(&request_id),
            );
        }
    }
    let ctx = RuneContext {
        rune_name: rune_name.to_string(),
        request_id: request_id.clone(),
        context: Default::default(),
        timeout: state.rune.request_timeout,
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
        return async_execute(state, invoker, ctx, rune_input, request_id).await;
    }

    // stream mode
    if params.stream.unwrap_or(false) {
        return stream_execute(state, &request_id, invoker, ctx, rune_input).await;
    }

    // sync mode (default)
    let start = Instant::now();
    let input_size = rune_input.len() as i64;
    let rune_name_owned = rune_name.to_string();
    let req_id = request_id.clone();

    let has_files = file_metadata.is_some();
    let (response, output_size) = if let Some(files) = file_metadata {
        sync_execute_multipart(
            invoker,
            ctx,
            rune_input,
            output_schema,
            files,
            &file_ids,
            state,
        )
        .await
    } else {
        sync_execute(invoker, ctx, rune_input, output_schema).await
    };

    // Record call log (best-effort)
    let latency_ms = start.elapsed().as_millis() as i64;
    let status_code = response.status().as_u16() as i32;
    let _ = state
        .admin
        .store
        .insert_log(&CallLog {
            id: 0,
            request_id: req_id,
            rune_name: rune_name_owned,
            mode: mode.into(),
            caster_id: None,
            latency_ms,
            status_code,
            input_size,
            output_size,
            timestamp: rune_store::now_iso8601(),
        })
        .await;

    // Schedule deferred file cleanup: give clients 60s to download, then clean up.
    // TTL (5 min) in FileBroker acts as a safety net if this task is dropped.
    if has_files {
        let broker = state.rune.file_broker.clone();
        let rid = request_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            broker.complete_request(&rid);
        });
    }

    response
}

pub async fn sync_execute(
    invoker: Arc<dyn RuneInvoker>,
    ctx: RuneContext,
    body: Bytes,
    output_schema: Option<String>,
) -> (axum::response::Response, i64) {
    let request_id = ctx.request_id.clone();
    match invoker.invoke_once(ctx, body).await {
        Ok(output) => {
            // Output schema validation
            if let Err(e) = validate_output(output_schema.as_deref(), &output) {
                return (
                    error_response_with_id(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "OUTPUT_VALIDATION_FAILED",
                        &e.to_string(),
                        Some(&request_id),
                    ),
                    0,
                );
            }
            match serde_json::from_slice::<serde_json::Value>(&output) {
                Ok(json) => {
                    let output_size = serde_json::to_vec(&json)
                        .map(|bytes| bytes.len() as i64)
                        .unwrap_or(output.len() as i64);
                    ((StatusCode::OK, Json(json)).into_response(), output_size)
                }
                Err(_) => {
                    let output_size = output.len() as i64;
                    ((StatusCode::OK, output).into_response(), output_size)
                }
            }
        }
        Err(e) => (map_error(e, Some(&request_id)), 0),
    }
}

pub async fn sync_execute_multipart(
    invoker: Arc<dyn RuneInvoker>,
    ctx: RuneContext,
    body: Bytes,
    output_schema: Option<String>,
    files: Vec<FileMetadata>,
    file_ids: &[String],
    state: &GateState,
) -> (axum::response::Response, i64) {
    let request_id = ctx.request_id.clone();
    // If there are no files, just run the normal path
    if files.is_empty() {
        return sync_execute(invoker, ctx, body, output_schema).await;
    }

    match invoker.invoke_once(ctx, body).await {
        Ok(output) => {
            // Build response: merge handler output with files metadata
            let files_json: Vec<serde_json::Value> = files
                .iter()
                .map(|f| {
                    serde_json::json!({
                        "file_id": f.file_id,
                        "filename": f.filename,
                        "mime_type": f.mime_type,
                        "size": f.size,
                        "transfer_mode": f.transfer_mode,
                    })
                })
                .collect();

            let mut response_json = if output.is_empty() {
                serde_json::json!({})
            } else {
                match serde_json::from_slice::<serde_json::Value>(&output) {
                    Ok(json) => json,
                    Err(_) => serde_json::json!({"output": String::from_utf8_lossy(&output)}),
                }
            };

            if let Some(obj) = response_json.as_object_mut() {
                obj.insert("files".to_string(), serde_json::json!(files_json));
            } else {
                response_json = serde_json::json!({
                    "output": response_json,
                    "files": files_json,
                });
            }

            let output_size = serde_json::to_vec(&response_json)
                .map(|bytes| bytes.len() as i64)
                .unwrap_or_default();
            (
                (StatusCode::OK, Json(response_json)).into_response(),
                output_size,
            )
        }
        Err(e) => {
            // Clean up files on error
            for fid in file_ids {
                state.rune.file_broker.remove(fid);
            }
            (map_error(e, Some(&request_id)), 0)
        }
    }
}

pub async fn stream_execute(
    state: &GateState,
    request_id: &str,
    invoker: Arc<dyn RuneInvoker>,
    ctx: RuneContext,
    body: Bytes,
) -> axum::response::Response {
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(32);

    let state_clone = state.clone();
    let req_id = request_id.to_string();
    let rune_name_log = ctx.rune_name.clone();
    let input_size = body.len() as i64;
    let start = Instant::now();
    tokio::spawn(async move {
        let mut status_code: i32 = 200;
        let mut output_size: i64 = 0;
        match invoker.invoke_stream(ctx, body).await {
            Ok(mut stream_rx) => {
                while let Some(chunk) = stream_rx.recv().await {
                    match chunk {
                        Ok(data) => {
                            output_size += data.len() as i64;
                            let event = Event::default()
                                .event("message")
                                .data(String::from_utf8_lossy(&data));
                            if tx.send(Ok(event)).await.is_err() {
                                state_clone
                                    .rune
                                    .session_mgr
                                    .cancel_by_request_id(&req_id, "SSE client disconnected")
                                    .await;
                                status_code = 499;
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx
                                .send(Ok(Event::default().event("error").data(e.to_string())))
                                .await;
                            status_code = 500;
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
                status_code = 500;
            }
        }

        // Record call log for stream mode (best-effort)
        let latency_ms = start.elapsed().as_millis() as i64;
        let _ = state_clone
            .admin
            .store
            .insert_log(&CallLog {
                id: 0,
                request_id: req_id,
                rune_name: rune_name_log,
                mode: "stream".into(),
                caster_id: None,
                latency_ms,
                status_code,
                input_size,
                output_size,
                timestamp: rune_store::now_iso8601(),
            })
            .await;
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Sse::new(stream).into_response()
}

pub async fn async_execute(
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
    if let Err(e) = state
        .admin
        .store
        .insert_task(&task_id, &rune_name, Some(&input_str))
        .await
    {
        return error_response_with_id(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
            Some(&request_id),
        );
    }
    if let Err(e) = state
        .admin
        .store
        .update_task_status(&task_id, TaskStatus::Running, None, None)
        .await
    {
        return error_response_with_id(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
            Some(&request_id),
        );
    }

    let store = Arc::clone(&state.admin.store);
    let start = Instant::now();
    let input_size = body.len() as i64;
    let rune_name_log = rune_name.clone();

    tokio::spawn(async move {
        let result = invoker.invoke_once(ctx, body).await;

        // Atomically complete the task only if it has not been cancelled (CAS).
        let (status, output_size) = match result {
            Ok(ref output) => {
                let output_str = String::from_utf8_lossy(output).to_string();
                let size = output.len() as i64;
                let updated = store
                    .complete_task_if_not_cancelled(
                        &task_id,
                        TaskStatus::Completed,
                        Some(&output_str),
                        None,
                    )
                    .await
                    .unwrap_or(false);
                if !updated {
                    // Task was cancelled — skip call log
                    return;
                }
                (200i32, size)
            }
            Err(ref e) => {
                let updated = store
                    .complete_task_if_not_cancelled(
                        &task_id,
                        TaskStatus::Failed,
                        None,
                        Some(&e.to_string()),
                    )
                    .await
                    .unwrap_or(false);
                if !updated {
                    return;
                }
                (500i32, 0i64)
            }
        };

        // Record call log
        let latency_ms = start.elapsed().as_millis() as i64;
        let _ = store
            .insert_log(&CallLog {
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
            })
            .await;
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
