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
    match state.file_broker.get(&file_id) {
        Some(stored) => {
            let headers = [
                (
                    axum::http::header::CONTENT_TYPE,
                    stored.mime_type.clone(),
                ),
                (
                    axum::http::header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", stored.filename),
                ),
            ];
            (StatusCode::OK, headers, stored.data.to_vec()).into_response()
        }
        None => error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "file not found"),
    }
}
