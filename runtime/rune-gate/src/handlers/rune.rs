use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Emit a single request's business metrics after execution completes.
fn record_rune_metrics(rune_name: &str, mode: &'static str, status_code: i32, latency_ms: i64) {
    let status: &'static str = if status_code < 400 {
        "ok"
    } else if status_code == 499 {
        // 499 Client Closed Request — client-side disconnect, not a server error.
        "client_disconnect"
    } else {
        "error"
    };
    let labels = vec![
        metrics::Label::new("rune", rune_name.to_owned()),
        metrics::Label::new("mode", mode),
        metrics::Label::new("status", status),
    ];
    metrics::histogram!("rune_request_duration_seconds", labels.clone())
        .record(latency_ms.max(0) as f64 / 1000.0);
    metrics::counter!("rune_requests_total", labels).increment(1);
    // Exclude 499 from rune_errors_total: client disconnects are not server faults.
    if status_code >= 400 && status_code != 499 {
        let err_labels = vec![
            metrics::Label::new("rune", rune_name.to_owned()),
            metrics::Label::new("mode", mode),
        ];
        metrics::counter!("rune_errors_total", err_labels).increment(1);
    }
}

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    http::StatusCode,
    response::{sse::Event, IntoResponse, Sse},
    Json,
};
use bytes::Bytes;
use tokio::sync::mpsc;

use rune_core::invoker::RuneInvoker;
use rune_core::rune::RuneContext;
use rune_core::trace::TRACE_ID_KEY;
use rune_schema::validator::{validate_input, validate_output};
use rune_store::{CallLog, TaskStatus};

use crate::error::{error_response, error_response_with_id, map_error};
use crate::multipart::{is_multipart, parse_multipart, FileMetadata};
use crate::state::{unique_request_id, GateState, RunParams};
use crate::trace_headers::{
    apply_trace_response_headers, context_from_headers, request_id_from_headers,
};

pub struct ExecuteRequest<'a> {
    pub state: &'a GateState,
    pub rune_name: &'a str,
    pub params: RunParams,
    pub body: Bytes,
    pub content_type: &'a str,
    pub labels: &'a HashMap<String, String>,
    pub headers: &'a HeaderMap,
    pub selected_entry: Option<rune_core::relay::RuneEntry>,
}

pub async fn run_rune(
    State(state): State<GateState>,
    Path(name): Path<String>,
    Query(params): Query<RunParams>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let headers = req.headers().clone();
    let selected_entry = req
        .extensions()
        .get::<rune_core::relay::RuneEntry>()
        .cloned();
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

    execute_rune(ExecuteRequest {
        state: &state,
        rune_name: &name,
        params,
        body,
        content_type: &content_type,
        labels: &labels,
        headers: &headers,
        selected_entry,
    })
    .await
}

/// gate_path dynamic routing: find rune by matching path
pub async fn dynamic_rune_handler(
    State(state): State<GateState>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let headers = req.headers().clone();
    let selected_entry = req
        .extensions()
        .get::<rune_core::relay::RuneEntry>()
        .cloned();
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

    let params: RunParams = match serde_urlencoded::from_str(&query) {
        Ok(p) => p,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "invalid query parameters",
            );
        }
    };

    execute_rune(ExecuteRequest {
        state: &state,
        rune_name: &rune_name,
        params,
        body,
        content_type: &content_type,
        labels: &labels,
        headers: &headers,
        selected_entry,
    })
    .await
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
pub async fn execute_rune(req: ExecuteRequest<'_>) -> axum::response::Response {
    let ExecuteRequest {
        state,
        rune_name,
        params,
        body,
        content_type,
        labels,
        headers,
        selected_entry,
    } = req;
    let request_id = request_id_from_headers(headers).unwrap_or_else(unique_request_id);
    let context = context_from_headers(headers);
    let mode = if params.async_mode.unwrap_or(false) {
        "async"
    } else if params.stream.unwrap_or(false) {
        "stream"
    } else {
        "sync"
    };
    tracing::info!(
        trace_id = %context.get(TRACE_ID_KEY).map(String::as_str).unwrap_or(""),
        request_id = %request_id,
        rune_name = %rune_name,
        mode = %mode,
        "gate invoke"
    );

    let selected_entry = match selected_entry.or_else(|| {
        if labels.is_empty() {
            state
                .rune
                .relay
                .select_entry(rune_name, &*state.rune.resolver)
        } else {
            state
                .rune
                .relay
                .select_entry_with_labels(rune_name, labels, &*state.rune.resolver)
        }
    }) {
        Some(entry) => entry,
        None => {
            // If labels were specified but no match, return 503 (no matching caster)
            if !labels.is_empty() {
                return traced_response(
                    error_response_with_id(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "NO_MATCHING_CASTER",
                        &format!("no caster matching labels for rune '{}'", rune_name),
                        Some(&request_id),
                    ),
                    &request_id,
                    &context,
                );
            }
            return traced_response(
                error_response_with_id(
                    StatusCode::NOT_FOUND,
                    "NOT_FOUND",
                    &format!("rune '{}' not found", rune_name),
                    Some(&request_id),
                ),
                &request_id,
                &context,
            );
        }
    };
    let invoker = Arc::clone(&selected_entry.invoker);
    let selected_caster_id = selected_entry.caster_id.clone();

    // Keep validation/stream capability aligned with the selected candidate.
    let input_schema = selected_entry.config.input_schema.clone();
    let output_schema = selected_entry.config.output_schema.clone();
    let supports_stream = selected_entry.config.supports_stream;

    // Check supports_stream
    if params.stream.unwrap_or(false) && !supports_stream {
        return traced_response(
            error_response_with_id(
                StatusCode::BAD_REQUEST,
                "STREAM_NOT_SUPPORTED",
                &format!("rune '{}' does not support streaming", rune_name),
                Some(&request_id),
            ),
            &request_id,
            &context,
        );
    }

    // Handle multipart vs regular body
    let (rune_input, file_metadata, file_ids) = if is_multipart(content_type) {
        match parse_multipart(state, content_type, body, &request_id).await {
            Ok(result) => {
                let rune_input = result.json_input.unwrap_or_default();
                (rune_input, Some(result.files), result.file_ids)
            }
            Err(err_response) => return traced_response(err_response, &request_id, &context),
        }
    } else {
        (body, None, Vec::new())
    };

    // Input schema validation — also validate multipart when JSON input is present
    let should_validate = file_metadata.is_none() || !rune_input.is_empty();
    if should_validate {
        if let Err(e) = validate_input(input_schema.as_deref(), &rune_input) {
            return traced_response(
                error_response_with_id(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "VALIDATION_FAILED",
                    &e.to_string(),
                    Some(&request_id),
                ),
                &request_id,
                &context,
            );
        }
    }
    let ctx = RuneContext {
        rune_name: rune_name.to_string(),
        request_id: request_id.clone(),
        context,
        timeout: state.rune.request_timeout,
        disable_runtime_retry: false,
    };

    // async mode
    if params.async_mode.unwrap_or(false) {
        return async_execute(
            state,
            invoker,
            ctx,
            rune_input,
            request_id,
            selected_caster_id,
        )
        .await;
    }

    // stream mode
    if params.stream.unwrap_or(false) {
        return stream_execute(
            state,
            &request_id,
            invoker,
            ctx,
            rune_input,
            selected_caster_id,
        )
        .await;
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

    // Record call log (best-effort) and emit metrics.
    let latency_ms = start.elapsed().as_millis() as i64;
    let status_code = response.status().as_u16() as i32;
    record_rune_metrics(&rune_name_owned, mode, status_code, latency_ms);
    let _ = state
        .admin
        .store
        .insert_log(&CallLog {
            id: 0,
            request_id: req_id,
            rune_name: rune_name_owned,
            mode: mode.into(),
            caster_id: selected_caster_id,
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
    let trace_context = ctx.context.clone();
    let request_id = ctx.request_id.clone();
    match invoker.invoke_once(ctx, body).await {
        Ok(output) => {
            // Output schema validation
            if let Err(e) = validate_output(output_schema.as_deref(), &output) {
                let mut response = error_response_with_id(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "OUTPUT_VALIDATION_FAILED",
                    &e.to_string(),
                    Some(&request_id),
                );
                apply_trace_response_headers(&mut response, &request_id, &trace_context);
                return (response, 0);
            }
            match serde_json::from_slice::<serde_json::Value>(&output) {
                Ok(json) => {
                    let output_size = output.len() as i64;
                    let mut response = (StatusCode::OK, Json(json)).into_response();
                    apply_trace_response_headers(&mut response, &request_id, &trace_context);
                    (response, output_size)
                }
                Err(_) => {
                    let output_size = output.len() as i64;
                    let mut response = (StatusCode::OK, output).into_response();
                    apply_trace_response_headers(&mut response, &request_id, &trace_context);
                    (response, output_size)
                }
            }
        }
        Err(e) => {
            let mut response = map_error(e, Some(&request_id));
            apply_trace_response_headers(&mut response, &request_id, &trace_context);
            (response, 0)
        }
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
    let trace_context = ctx.context.clone();
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
                    Err(_) => {
                        serde_json::json!({"output": format!("hex:{}", hex::encode(&output))})
                    }
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
            let mut response = (StatusCode::OK, Json(response_json)).into_response();
            apply_trace_response_headers(&mut response, &request_id, &trace_context);
            (response, output_size)
        }
        Err(e) => {
            // Clean up files on error
            for fid in file_ids {
                state.rune.file_broker.remove(fid);
            }
            let mut response = map_error(e, Some(&request_id));
            apply_trace_response_headers(&mut response, &request_id, &trace_context);
            (response, 0)
        }
    }
}

pub async fn stream_execute(
    state: &GateState,
    request_id: &str,
    invoker: Arc<dyn RuneInvoker>,
    ctx: RuneContext,
    body: Bytes,
    caster_id: Option<String>,
) -> axum::response::Response {
    let trace_context = ctx.context.clone();
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
                            // Non-UTF-8 binary output is encoded as "hex:<lowercase-hex>".
                            // Clients should check for this prefix to recover the raw bytes.
                            let event = Event::default().event("message").data(
                                match std::str::from_utf8(&data) {
                                    Ok(s) => s.to_owned(),
                                    Err(_) => format!("hex:{}", hex::encode(&data)),
                                },
                            );
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

        // Record call log for stream mode (best-effort) and emit metrics.
        let latency_ms = start.elapsed().as_millis() as i64;
        record_rune_metrics(&rune_name_log, "stream", status_code, latency_ms);
        let _ = state_clone
            .admin
            .store
            .insert_log(&CallLog {
                id: 0,
                request_id: req_id,
                rune_name: rune_name_log,
                mode: "stream".into(),
                caster_id,
                latency_ms,
                status_code,
                input_size,
                output_size,
                timestamp: rune_store::now_iso8601(),
            })
            .await;
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut response = Sse::new(stream).into_response();
    apply_trace_response_headers(&mut response, request_id, &trace_context);
    response
}

pub async fn async_execute(
    state: &GateState,
    invoker: Arc<dyn RuneInvoker>,
    ctx: RuneContext,
    body: Bytes,
    request_id: String,
    caster_id: Option<String>,
) -> axum::response::Response {
    let trace_context = ctx.context.clone();
    let task_id = request_id.clone();
    let rune_name = ctx.rune_name.clone();
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
        .insert_task_and_start(&task_id, &rune_name, Some(&input_str))
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
    let start = Instant::now();
    let input_size = body.len() as i64;
    let rune_name_log = rune_name.clone();

    tokio::spawn(async move {
        let result = invoker.invoke_once(ctx, body).await;

        // Atomically complete the task only if it has not been cancelled (CAS).
        let (status, output_size) = match result {
            Ok(ref output) => {
                let output_str = match std::str::from_utf8(output) {
                    Ok(s) => s.to_string(),
                    Err(_) => format!("hex:{}", hex::encode(output)),
                };
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

        // Record call log and emit metrics.
        let latency_ms = start.elapsed().as_millis() as i64;
        record_rune_metrics(&rune_name_log, "async", status, latency_ms);
        let _ = store
            .insert_log(&CallLog {
                id: 0,
                request_id: task_id,
                rune_name: rune_name_log,
                mode: "async".into(),
                caster_id,
                latency_ms,
                status_code: status,
                input_size,
                output_size,
                timestamp: rune_store::now_iso8601(),
            })
            .await;
    });

    traced_response(
        (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "task_id": request_id,
                "status": "running",
            })),
        )
            .into_response(),
        &request_id,
        &trace_context,
    )
}

fn traced_response(
    mut response: axum::response::Response,
    request_id: &str,
    context: &HashMap<String, String>,
) -> axum::response::Response {
    apply_trace_response_headers(&mut response, request_id, context);
    response
}
