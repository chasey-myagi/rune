use axum::{
    middleware,
    routing::{delete, get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};

use crate::handlers;
use crate::middleware::{
    auth_middleware, rate_limit_middleware, security_headers_middleware, shutdown_middleware,
};
use crate::state::GateState;

/// Build the Gate Router with auth middleware, CORS, and management API.
pub fn build_router(state: GateState, extra_routes: Option<Router<GateState>>) -> Router {
    let mut router = Router::new()
        .route("/health", get(handlers::mgmt::health))
        .route("/ready", get(handlers::mgmt::ready))
        .route("/api/v1/runes", get(handlers::mgmt::list_runes))
        .route("/api/v1/runes/{name}/run", post(handlers::rune::run_rune))
        .route("/api/v1/tasks", get(handlers::task::list_tasks))
        .route(
            "/api/v1/tasks/{id}",
            get(handlers::task::get_task).delete(handlers::task::delete_task),
        )
        // Management API
        .route("/api/v1/status", get(handlers::mgmt::mgmt_status))
        .route("/api/v1/casters", get(handlers::mgmt::mgmt_casters))
        .route("/api/v1/stats", get(handlers::mgmt::mgmt_stats))
        .route(
            "/api/v1/stats/casters",
            get(handlers::mgmt::mgmt_caster_stats),
        )
        .route("/api/v1/logs", get(handlers::mgmt::mgmt_logs))
        .route(
            "/api/v1/scaling/status",
            get(handlers::mgmt::mgmt_scaling_status),
        )
        .route(
            "/api/v1/keys",
            get(handlers::mgmt::mgmt_list_keys).post(handlers::mgmt::mgmt_create_key),
        )
        .route("/api/v1/keys/{id}", delete(handlers::mgmt::mgmt_revoke_key))
        .route(
            "/api/v1/openapi.json",
            get(handlers::openapi::openapi_handler),
        )
        .route("/api/v1/files/{id}", get(handlers::file::download_file))
        // Flow API
        .route(
            "/api/v1/flows",
            post(handlers::flow::create_flow).get(handlers::flow::list_flows),
        )
        .route(
            "/api/v1/flows/{name}",
            get(handlers::flow::get_flow).delete(handlers::flow::delete_flow),
        )
        .route("/api/v1/flows/{name}/run", post(handlers::flow::run_flow));

    if let Some(extra) = extra_routes {
        router = router.merge(extra);
    }

    // Build CORS layer. Permissive mode (allow any origin) is a footgun: any
    // web page can make authenticated requests if the browser stores credentials.
    // Only use it for local development; configure cors_origins in production.
    let cors = if state.cors_origins.is_empty() {
        tracing::warn!(
            "CORS is set to permissive (allow-all origins). \
             Set cors_origins in configuration to restrict cross-origin access."
        );
        CorsLayer::permissive()
    } else {
        let origins: Vec<_> = state
            .cors_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any)
    };

    // Axum onion model: last-added layer executes first (outermost).
    // Desired execution order: security_headers → CORS → shutdown → auth → rate_limit → handler
    // So we add them in reverse: rate_limit, auth, shutdown, cors, security_headers.
    router
        .fallback(handlers::rune::dynamic_rune_handler)
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .layer(middleware::from_fn_with_state(state, shutdown_middleware))
        .layer(cors)
        .layer(middleware::from_fn(security_headers_middleware))
}
