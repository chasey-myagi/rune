use std::sync::Arc;

use axum::{extract::State, http::StatusCode, middleware::Next};
use sha2::{Digest, Sha256};

use crate::error::error_response;
use crate::handlers::rune::parse_labels_header;
use crate::state::GateState;

/// Zero-allocation exempt route check.
/// Matches exact path or path prefix followed by '/'.
fn is_exempt(path: &str, exempt_routes: &[String]) -> bool {
    exempt_routes.iter().any(|r| {
        path == r.as_str()
            || (r.as_str() != "/"
                && path.starts_with(r.as_str())
                && (r.ends_with('/') || path.as_bytes().get(r.len()) == Some(&b'/')))
    })
}

/// Compute a rate-limit key from the raw Authorization header value.
///
/// The bearer token is hashed so that the raw secret is never stored as a
/// map key — this prevents attackers from flooding the rate-limiter with
/// fabricated tokens to exhaust memory. v1.2.0 uses the first 128 bits of a
/// SHA-256 digest to keep the key stable across Rust versions and platforms.
fn rate_key_from_header(auth_header: Option<&str>) -> String {
    match auth_header.and_then(|v| v.strip_prefix("Bearer ")) {
        Some(token) => {
            let hash = Sha256::digest(token.as_bytes());
            let truncated = &hash[..16];
            format!("k_{}", hex::encode(truncated))
        }
        None => "__anonymous__".to_string(),
    }
}

fn is_management_route(path: &str) -> bool {
    let exact_routes = [
        "/api/v1/runes",
        "/api/v1/status",
        "/api/v1/casters",
        "/api/v1/stats",
        "/api/v1/logs",
        "/api/v1/keys",
        "/api/v1/openapi.json",
        "/api/v1/tasks",
    ];
    let prefix_routes = [
        "/api/v1/keys/",
        "/api/v1/tasks/",
        "/api/v1/files/",
        "/api/v1/flows",
    ];

    exact_routes.contains(&path) || prefix_routes.iter().any(|prefix| path.starts_with(prefix))
}

fn debug_rune_name(path: &str) -> Option<&str> {
    let prefix = "/api/v1/runes/";
    let suffix = "/run";
    path.strip_prefix(prefix)?.strip_suffix(suffix)
}

fn rate_limited_response(retry_after: u64, scope: &str) -> axum::response::Response {
    let mut response = error_response(
        StatusCode::TOO_MANY_REQUESTS,
        "RATE_LIMITED",
        "rate limit exceeded",
    );
    response.headers_mut().insert(
        "retry-after",
        axum::http::HeaderValue::from_str(&retry_after.to_string()).unwrap(),
    );
    response.headers_mut().insert(
        "x-rate-limit-scope",
        axum::http::HeaderValue::from_str(scope).unwrap(),
    );
    response
}

fn resolve_rune_name_for_rate_limit(state: &GateState, method: &str, path: &str) -> Option<String> {
    debug_rune_name(path)
        .map(ToString::to_string)
        .or_else(|| state.rune.relay.resolve_by_gate_path(method, path))
}

/// Resolve the concrete rune entry once so middleware and handlers can
/// reuse the same scheduling decision.
fn select_rune_entry(
    state: &GateState,
    rune_name: &str,
    labels: &std::collections::HashMap<String, String>,
) -> Option<rune_core::relay::RuneEntry> {
    select_rune_entry_with_resolver(&state.rune.relay, &state.rune.resolver, rune_name, labels)
}

fn select_rune_entry_with_resolver(
    relay: &rune_core::relay::Relay,
    resolver: &Arc<dyn rune_core::resolver::Resolver>,
    rune_name: &str,
    labels: &std::collections::HashMap<String, String>,
) -> Option<rune_core::relay::RuneEntry> {
    if labels.is_empty() {
        relay.select_entry(rune_name, resolver.as_ref())
    } else {
        relay.select_entry_with_labels(rune_name, labels, resolver.as_ref())
    }
}

/// Best-effort capacity check **after** entry selection.
///
/// Note: this is inherently TOCTOU — between `select_entry` and this check
/// another request may have claimed the last permit.  The actual concurrency
/// gate is the `SessionManager::acquire_permit` semaphore at invocation time;
/// this function only serves as an early-rejection optimisation to avoid
/// unnecessary work further down the pipeline.
fn caster_capacity_exhausted(
    session_mgr: &rune_core::session::SessionManager,
    selected_entry: &rune_core::relay::RuneEntry,
) -> bool {
    match selected_entry.caster_id.as_deref() {
        Some(caster_id) => session_mgr.available_permits(caster_id) == 0,
        None => false,
    }
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
    mut req: axum::extract::Request,
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
    if is_management_route(&path) {
        return next.run(req).await;
    }

    let method = req.method().to_string();
    let labels = parse_labels_header(
        req.headers()
            .get("x-rune-labels")
            .and_then(|v| v.to_str().ok()),
    );
    let rune_name = resolve_rune_name_for_rate_limit(&state, &method, &path);

    // Extract rate limit key from Authorization header (hashed to avoid leaking tokens)
    let rate_key = rate_key_from_header(
        req.headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok()),
    );

    if let Err(retry_after) = rate_limiter.check(&rate_key) {
        return rate_limited_response(retry_after, "global");
    }

    if let Some(rune_name) = rune_name {
        if let Err(retry_after) = rate_limiter.check_rune(&rate_key, &rune_name) {
            return rate_limited_response(retry_after, "per-rune");
        }

        // Pre-select a caster via the resolver and stash in request extensions.
        // The handler MUST reuse this entry instead of calling the resolver
        // again — otherwise round-robin (or other stateful resolvers) would
        // advance twice per request, and the capacity check below would apply
        // to a different caster than the one the handler ultimately invokes.
        if let Some(selected_entry) = select_rune_entry(&state, &rune_name, &labels) {
            if caster_capacity_exhausted(&state.rune.session_mgr, &selected_entry) {
                return rate_limited_response(1, "caster");
            }
            req.extensions_mut().insert(selected_entry);
        }
    }

    next.run(req).await
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
        // After fix: "/" should NOT match everything via prefix
        assert!(!is_exempt("/anything", &routes));
    }

    /// Regression m1: is_exempt("/") must NOT match all routes.
    #[test]
    fn test_fix_is_exempt_root_does_not_match_all() {
        let routes = vec!["/".to_string(), "/health".to_string()];
        // "/" exact match
        assert!(is_exempt("/", &routes));
        // "/health" exact match via second entry
        assert!(is_exempt("/health", &routes));
        // "/foo" must NOT match — "/" should be exact only
        assert!(
            !is_exempt("/foo", &routes),
            "is_exempt with '/' in exempt list must not match arbitrary paths"
        );
        assert!(!is_exempt("/anything/else", &routes));
    }

    #[test]
    fn test_fix_rate_key_is_hashed() {
        let raw_token = "sk-abc123secretkey";
        let key = rate_key_from_header(Some(&format!("Bearer {raw_token}")));

        // Key must NOT contain the raw token
        assert!(
            !key.contains(raw_token),
            "rate key must not contain the raw bearer token"
        );
        // Key must be deterministic
        let key2 = rate_key_from_header(Some(&format!("Bearer {raw_token}")));
        assert_eq!(key, key2, "same token must produce the same rate key");

        // Different tokens must produce different keys
        let key3 = rate_key_from_header(Some("Bearer other-token"));
        assert_ne!(
            key, key3,
            "different tokens must produce different rate keys"
        );
    }

    #[test]
    fn test_fix_flows_subroutes_are_management() {
        // Regression: /api/v1/flows/:name and /api/v1/flows/:name/run
        // should be exempt from rate limiting as management routes.
        assert!(
            is_management_route("/api/v1/flows/my-pipeline"),
            "GET/DELETE /api/v1/flows/:name must be a management route"
        );
        assert!(
            is_management_route("/api/v1/flows/my-pipeline/run"),
            "POST /api/v1/flows/:name/run must be a management route"
        );
        // Exact match still works
        assert!(is_management_route("/api/v1/flows"));
    }

    #[test]
    fn test_fix_caster_capacity_respects_resolver() {
        // Regression: caster_capacity_exhausted should use the resolver to
        // determine which candidate will actually be picked, not just check
        // if *any* candidate has free permits. When the resolver would pick
        // a saturated caster (e.g. health-aware preferring healthy over
        // degraded), a degraded caster with free permits should NOT suppress
        // the 429.

        use rune_core::invoker::LocalInvoker;
        use rune_core::resolver::{HealthAwareResolver, RoundRobinResolver};
        use rune_core::rune::{make_handler, RuneConfig};
        use rune_core::session::{HealthStatusLevel, SessionManager};
        use std::sync::Arc;
        use std::time::Duration;

        let session_mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(30),
        ));
        // caster_a: healthy, max_concurrent=1 (will be saturated)
        session_mgr.insert_test_caster("caster_a", 1);
        // caster_b: degraded, max_concurrent=10 (has plenty of capacity)
        session_mgr.insert_test_caster("caster_b", 10);
        session_mgr.set_test_health("caster_b", HealthStatusLevel::Degraded);

        // Saturate caster_a by acquiring its only permit
        let _permit = session_mgr.acquire_test_permit("caster_a");

        let relay = Arc::new(rune_core::relay::Relay::new());
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                Arc::new(LocalInvoker::new(handler.clone())),
                Some("caster_a".into()),
            )
            .unwrap();
        relay
            .register(
                RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                Arc::new(LocalInvoker::new(handler)),
                Some("caster_b".into()),
            )
            .unwrap();

        // The resolver would pick caster_a (healthy, rank=2) over caster_b
        // (degraded, rank=1). caster_a is saturated.
        let resolver: Arc<dyn rune_core::resolver::Resolver> = Arc::new(HealthAwareResolver::new(
            Arc::new(RoundRobinResolver::new()),
            session_mgr.clone(),
        ));

        let selected = select_rune_entry_with_resolver(
            &relay,
            &resolver,
            "echo",
            &std::collections::HashMap::new(),
        )
        .expect("resolver should select a candidate");
        assert!(
            caster_capacity_exhausted(&session_mgr, &selected),
            "should report exhausted because the resolver would pick the saturated healthy caster"
        );
        assert_eq!(selected.caster_id.as_deref(), Some("caster_a"));
    }

    #[test]
    fn test_fix_rate_key_anonymous_without_bearer() {
        assert_eq!(rate_key_from_header(None), "__anonymous__");
        assert_eq!(
            rate_key_from_header(Some("Basic dXNlcjpwYXNz")),
            "__anonymous__"
        );
    }
}
