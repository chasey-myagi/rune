use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use rune_core::rune::RuneError;
use rune_store::TaskStatus;

use crate::error::error_response;
use crate::state::GateState;

#[derive(serde::Deserialize, Default)]
pub struct ListTasksQuery {
    pub status: Option<String>,
    pub rune: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

pub async fn list_tasks(
    State(state): State<GateState>,
    Query(params): Query<ListTasksQuery>,
) -> impl IntoResponse {
    let status = params.status.as_deref().and_then(TaskStatus::parse);
    let limit = params.limit.unwrap_or(50).clamp(0, 500);
    let offset = params.offset.unwrap_or(0);
    match state
        .admin
        .store
        .list_tasks(status, params.rune.as_deref(), limit, offset)
        .await
    {
        Ok(tasks) => Json(serde_json::json!({"tasks": tasks})).into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        ),
    }
}

pub async fn get_task(State(state): State<GateState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.admin.store.get_task(&id).await {
        Ok(Some(task)) => (StatusCode::OK, Json(serde_json::json!(task))).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "task not found"),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        ),
    }
}

/// Helper: update store to Cancelled and return the appropriate response.
async fn mark_cancelled_in_store(
    store: &rune_store::RuneStore,
    id: &str,
    note: Option<&str>,
) -> axum::response::Response {
    if let Err(e) = store
        .update_task_status(id, TaskStatus::Cancelled, None, Some("cancelled by user"))
        .await
    {
        tracing::error!("Failed to update task status in store: {}", e);
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &format!("store update failed: {}", e),
        );
    }
    let mut body = serde_json::json!({"task_id": id, "status": "cancelled"});
    if let Some(n) = note {
        body["note"] = serde_json::json!(n);
    }
    (StatusCode::OK, Json(body)).into_response()
}

pub async fn delete_task(
    State(state): State<GateState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let task = match state.admin.store.get_task(&id).await {
        Ok(None) => return error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "task not found"),
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                &e.to_string(),
            )
        }
        Ok(Some(t)) => t,
    };

    match task.status {
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
            // First check the local async-flow registry (tasks spawned by
            // run_flow_async are not in the session_mgr request index).
            if let Some(handle) = state.flow.task_registry.write().await.remove(&id) {
                handle.abort();
                return mark_cancelled_in_store(&state.admin.store, &id, None).await;
            }
            match state
                .rune
                .session_mgr
                .cancel_by_request_id(&id, "cancelled by user")
                .await
            {
                Ok(true) => mark_cancelled_in_store(&state.admin.store, &id, None).await,
                Ok(false) => {
                    // Request already gone from index — task likely finished just now.
                    // Return current state so the client can see what happened.
                    match state.admin.store.get_task(&id).await {
                        Ok(Some(task)) => {
                            (StatusCode::OK, Json(serde_json::json!(task))).into_response()
                        }
                        Ok(None) => {
                            error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "task not found")
                        }
                        Err(e) => error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "INTERNAL",
                            &e.to_string(),
                        ),
                    }
                }
                Err(RuneError::Unavailable) => {
                    // Caster disconnected; we cannot confirm the task is stopped.
                    // Still mark as cancelled per user intent, but return 202 Accepted.
                    if let Err(e) = state
                        .admin
                        .store
                        .update_task_status(
                            &id,
                            TaskStatus::Cancelled,
                            None,
                            Some("cancelled by user"),
                        )
                        .await
                    {
                        tracing::error!("Failed to update task status in store: {}", e);
                        return error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "INTERNAL",
                            &format!("store update failed: {}", e),
                        );
                    }
                    (
                        StatusCode::ACCEPTED,
                        Json(serde_json::json!({
                            "task_id": id,
                            "status": "cancelled",
                            "note": "caster was unavailable"
                        })),
                    )
                        .into_response()
                }
                Err(_) => error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL",
                    "cancel failed",
                ),
            }
        }
    }
}
