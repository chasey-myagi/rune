use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::error::error_response;
use crate::state::GateState;

pub async fn download_file(
    State(state): State<GateState>,
    Path(file_id): Path<String>,
) -> axum::response::Response {
    // Files are cleaned up by complete_request() after the rune handler returns.
    // Use get() here so downloads can be retried if the connection drops.
    match state.rune.file_broker.get(&file_id) {
        Some(stored) => {
            let headers = [
                (
                    axum::http::header::CONTENT_TYPE,
                    stored.mime_type.clone(),
                ),
                (
                    axum::http::header::CONTENT_DISPOSITION,
                    format!(
                        "attachment; filename=\"{}\"",
                        stored.filename.replace('\\', "\\\\").replace('"', "\\\"")
                    ),
                ),
            ];
            match stored.data_async().await {
                Ok(data) => (StatusCode::OK, headers, data).into_response(),
                Err(_) => error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "FILE_READ_ERROR",
                    "failed to read file data",
                ),
            }
        }
        None => error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "file not found"),
    }
}
