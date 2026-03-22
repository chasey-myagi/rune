use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use rune_store::TaskStatus;

use crate::error::error_response;
use crate::state::GateState;

pub async fn get_task(
    State(state): State<GateState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.store.get_task(&id).await {
        Ok(Some(task)) => (StatusCode::OK, Json(serde_json::json!(task))).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "task not found"),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string()),
    }
}

pub async fn delete_task(
    State(state): State<GateState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.store.get_task(&id).await {
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
                ).await;
                (
                    StatusCode::OK,
                    Json(serde_json::json!({"task_id": id, "status": "cancelled"})),
                )
                    .into_response()
            }
        },
    }
}
