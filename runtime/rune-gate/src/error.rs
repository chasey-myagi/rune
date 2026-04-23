use axum::{http::StatusCode, response::IntoResponse, Json};
use rune_core::rune::RuneError;
use rune_flow::engine::FlowError;

pub fn error_response(status: StatusCode, code: &str, msg: &str) -> axum::response::Response {
    error_response_with_id(status, code, msg, None)
}

pub fn error_response_with_id(
    status: StatusCode,
    code: &str,
    msg: &str,
    request_id: Option<&str>,
) -> axum::response::Response {
    let mut error = serde_json::json!({
        "code": code,
        "message": msg,
    });
    if let Some(request_id) = request_id {
        if let Some(obj) = error.as_object_mut() {
            obj.insert(
                "request_id".to_string(),
                serde_json::Value::String(request_id.to_string()),
            );
        }
    }
    (status, Json(serde_json::json!({ "error": error }))).into_response()
}

pub fn map_error(e: RuneError, request_id: Option<&str>) -> axum::response::Response {
    let (status, code, msg): (StatusCode, &str, std::borrow::Cow<str>) = match &e {
        RuneError::InvalidInput(_) => (
            StatusCode::BAD_REQUEST,
            "INVALID_INPUT",
            e.to_string().into(),
        ),
        RuneError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND", e.to_string().into()),
        RuneError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "UNAVAILABLE",
            e.to_string().into(),
        ),
        RuneError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "TIMEOUT", e.to_string().into()),
        RuneError::Cancelled => (
            StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
            "CANCELLED",
            e.to_string().into(),
        ),
        RuneError::ExecutionFailed { .. } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "EXECUTION_FAILED",
            e.to_string().into(),
        ),
        RuneError::RateLimited { .. } => (
            StatusCode::TOO_MANY_REQUESTS,
            "RATE_LIMITED",
            e.to_string().into(),
        ),
        RuneError::CircuitOpen { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "CIRCUIT_OPEN",
            e.to_string().into(),
        ),
        RuneError::Unauthorized(_) => (
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            e.to_string().into(),
        ),
        RuneError::Forbidden(_) => (StatusCode::FORBIDDEN, "FORBIDDEN", e.to_string().into()),
        RuneError::Internal(_) => {
            // Log the full error chain server-side but return a generic message to the
            // client to prevent leaking internal details (file paths, connection strings, etc.)
            // Use {:?} to expand the full anyhow chain including all context layers.
            tracing::error!(error = ?e, "internal rune error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                "internal server error".into(),
            )
        }
    };
    error_response_with_id(status, code, &msg, request_id)
}

pub fn map_flow_error(e: FlowError, request_id: Option<&str>) -> axum::response::Response {
    match &e {
        FlowError::FlowNotFound(_) => error_response_with_id(
            StatusCode::NOT_FOUND,
            "FLOW_NOT_FOUND",
            &e.to_string(),
            request_id,
        ),
        FlowError::StepFailed { source, .. } => {
            let (status, code) = match source {
                RuneError::NotFound(_) => (StatusCode::SERVICE_UNAVAILABLE, "STEP_UNAVAILABLE"),
                RuneError::CircuitOpen { .. } => {
                    (StatusCode::SERVICE_UNAVAILABLE, "STEP_CIRCUIT_OPEN")
                }
                RuneError::RateLimited { .. } => {
                    (StatusCode::TOO_MANY_REQUESTS, "STEP_RATE_LIMITED")
                }
                RuneError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "STEP_UNAUTHORIZED"),
                RuneError::Forbidden(_) => (StatusCode::FORBIDDEN, "STEP_FORBIDDEN"),
                RuneError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "STEP_UNAVAILABLE"),
                RuneError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "STEP_TIMEOUT"),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "STEP_FAILED"),
            };
            error_response_with_id(status, code, &e.to_string(), request_id)
        }
        FlowError::DagError(_) => error_response_with_id(
            StatusCode::BAD_REQUEST,
            "DAG_ERROR",
            &e.to_string(),
            request_id,
        ),
        FlowError::NoTerminalStep => error_response_with_id(
            StatusCode::INTERNAL_SERVER_ERROR,
            "NO_TERMINAL_STEP",
            &e.to_string(),
            request_id,
        ),
        FlowError::SerializationFailed(_) => error_response_with_id(
            StatusCode::INTERNAL_SERVER_ERROR,
            "SERIALIZATION_FAILED",
            &e.to_string(),
            request_id,
        ),
        FlowError::MapItemsNotArray(_) => error_response_with_id(
            StatusCode::BAD_REQUEST,
            "MAP_ITEMS_NOT_ARRAY",
            &e.to_string(),
            request_id,
        ),
        FlowError::FlowStepNotFound(_) => error_response_with_id(
            StatusCode::UNPROCESSABLE_ENTITY,
            "FLOW_STEP_NOT_FOUND",
            &e.to_string(),
            request_id,
        ),
        FlowError::CircularFlowRef(_) => error_response_with_id(
            StatusCode::BAD_REQUEST,
            "CIRCULAR_FLOW_REF",
            &e.to_string(),
            request_id,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_response_with_request_id_includes_request_id() {
        let response = error_response_with_id(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "bad input",
            Some("req-123"),
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let bytes = runtime
            .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"]["request_id"], "req-123");
    }

    #[test]
    fn error_response_without_request_id_omits_request_id() {
        let response = error_response(StatusCode::BAD_REQUEST, "BAD_REQUEST", "bad input");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let bytes = runtime
            .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(json["error"].get("request_id").is_none());
    }

    #[test]
    fn map_error_supports_new_variants() {
        let rate_limited = map_error(
            RuneError::RateLimited {
                retry_after_secs: 5,
            },
            Some("req-1"),
        );
        assert_eq!(rate_limited.status(), StatusCode::TOO_MANY_REQUESTS);

        let circuit_open = map_error(
            RuneError::CircuitOpen {
                rune_name: "echo".to_string(),
            },
            None,
        );
        assert_eq!(circuit_open.status(), StatusCode::SERVICE_UNAVAILABLE);

        let unauthorized = map_error(RuneError::Unauthorized("no key".to_string()), None);
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let forbidden = map_error(RuneError::Forbidden("no admin".to_string()), None);
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
    }
}
