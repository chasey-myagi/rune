use axum::{Json, http::StatusCode, response::IntoResponse};
use rune_core::rune::RuneError;
use rune_flow::engine::FlowError;

pub fn error_response(status: StatusCode, code: &str, msg: &str) -> axum::response::Response {
    (
        status,
        Json(serde_json::json!({"error": {"code": code, "message": msg}})),
    )
        .into_response()
}

pub fn map_error(e: RuneError) -> axum::response::Response {
    let (status, code) = match &e {
        RuneError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "INVALID_INPUT"),
        RuneError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
        RuneError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE"),
        RuneError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "TIMEOUT"),
        RuneError::Cancelled => (
            StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
            "CANCELLED",
        ),
        RuneError::ExecutionFailed { .. } => {
            (StatusCode::INTERNAL_SERVER_ERROR, "EXECUTION_FAILED")
        }
        RuneError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL"),
    };
    error_response(status, code, &e.to_string())
}

pub fn flow_error_response(status: StatusCode, msg: &str) -> axum::response::Response {
    (
        status,
        Json(serde_json::json!({"error": msg})),
    )
        .into_response()
}

pub fn map_flow_error(e: FlowError) -> axum::response::Response {
    match &e {
        FlowError::FlowNotFound(_) => {
            flow_error_response(StatusCode::NOT_FOUND, &e.to_string())
        }
        FlowError::StepFailed { source, .. } => {
            let status = match source {
                RuneError::NotFound(_) => StatusCode::SERVICE_UNAVAILABLE,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            flow_error_response(status, &e.to_string())
        }
        FlowError::DagError(_) => {
            flow_error_response(StatusCode::BAD_REQUEST, &e.to_string())
        }
        FlowError::NoTerminalStep => {
            flow_error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}
