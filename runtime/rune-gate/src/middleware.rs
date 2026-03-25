use axum::{
    extract::State,
    http::StatusCode,
    middleware::Next,
};

use crate::error::error_response;
use crate::state::GateState;

/// Zero-allocation exempt route check.
/// Matches exact path or path prefix followed by '/'.
fn is_exempt(path: &str, exempt_routes: &[String]) -> bool {
    exempt_routes.iter().any(|r| {
        path == r.as_str()
            || (path.starts_with(r.as_str())
                && (r.ends_with('/') || path.as_bytes().get(r.len()) == Some(&b'/')))
    })
}

pub async fn auth_middleware(
    State(state): State<GateState>,
    req: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    if !state.auth.auth_enabled {
        return next.run(req).await;
    }

    let path = req.uri().path().to_string();
    if is_exempt(&path, &state.auth.exempt_routes) {
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
            if state.auth.key_verifier.verify_gate_key(&key).await {
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
    if state.admin.dev_mode {
        return next.run(req).await;
    }

    let rate_limiter = match &state.rate_limiter {
        Some(rl) => rl,
        None => return next.run(req).await,
    };

    let path = req.uri().path().to_string();

    // Exempt routes (e.g. /health)
    if is_exempt(&path, &state.auth.exempt_routes) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_exempt_exact_match() {
        let routes = vec!["/health".to_string(), "/ready".to_string()];
        assert!(is_exempt("/health", &routes));
        assert!(is_exempt("/ready", &routes));
        assert!(!is_exempt("/other", &routes));
    }

    #[test]
    fn test_is_exempt_prefix_match() {
        let routes = vec!["/api/v1/status".to_string()];
        assert!(is_exempt("/api/v1/status", &routes));
        assert!(is_exempt("/api/v1/status/detail", &routes));
        assert!(!is_exempt("/api/v1/statusx", &routes));
    }

    #[test]
    fn test_is_exempt_empty_routes() {
        let routes: Vec<String> = vec![];
        assert!(!is_exempt("/anything", &routes));
    }

    #[test]
    fn test_is_exempt_root_path() {
        let routes = vec!["/".to_string()];
        assert!(is_exempt("/", &routes));
        assert!(is_exempt("/anything", &routes));
    }
}
