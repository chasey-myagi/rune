use axum::{
    extract::State,
    http::StatusCode,
    middleware::Next,
};

use crate::error::error_response;
use crate::state::GateState;

pub async fn auth_middleware(
    State(state): State<GateState>,
    req: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    if !state.auth_enabled {
        return next.run(req).await;
    }

    let path = req.uri().path().to_string();
    if state.exempt_routes.iter().any(|r| path == *r || path.starts_with(&format!("{}/", r))) {
        return next.run(req).await;
    }

    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string());

    match auth_header {
        Some(key) => {
            if state.key_verifier.verify_gate_key(&key).await {
                next.run(req).await
            } else {
                error_response(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "invalid api key")
            }
        }
        None => error_response(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "missing authorization header",
        ),
    }
}

pub async fn shutdown_middleware(
    State(state): State<GateState>,
    req: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    if state.shutdown.is_draining() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "SERVICE_UNAVAILABLE",
            "server is shutting down",
        );
    }
    next.run(req).await
}

pub async fn rate_limit_middleware(
    State(state): State<GateState>,
    req: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    // Skip rate limiting in dev mode or if not configured
    if state.dev_mode {
        return next.run(req).await;
    }

    let rate_limiter = match &state.rate_limiter {
        Some(rl) => rl,
        None => return next.run(req).await,
    };

    let path = req.uri().path().to_string();

    // Exempt routes (e.g. /health)
    if state.exempt_routes.iter().any(|r| path == *r || path.starts_with(&format!("{}/", r))) {
        return next.run(req).await;
    }

    // Management routes are exempt from rate limiting
    if path.starts_with("/api/v1/") {
        let mgmt_prefixes = [
            "/api/v1/status", "/api/v1/casters", "/api/v1/stats",
            "/api/v1/logs", "/api/v1/keys", "/api/v1/runes",
            "/api/v1/openapi.json", "/api/v1/flows", "/api/v1/tasks",
            "/api/v1/files",
        ];
        if mgmt_prefixes.iter().any(|p| path.starts_with(p)) {
            return next.run(req).await;
        }
    }

    // Extract rate limit key from Authorization header
    let rate_key = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("__anonymous__")
        .to_string();

    match rate_limiter.check(&rate_key) {
        Ok(()) => next.run(req).await,
        Err(retry_after) => {
            let mut response = error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                "rate limit exceeded",
            );
            response.headers_mut().insert(
                "retry-after",
                axum::http::HeaderValue::from_str(&retry_after.to_string()).unwrap(),
            );
            response
        }
    }
}
