use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};

use crate::error::error_response;
use crate::state::{CreateKeyRequest, GateState, LogQuery};

/// Extract the Bearer token from the Authorization header.
fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

/// Check that the caller holds an admin key. Skipped in dev mode (auth disabled).
/// Returns Some(error_response) if the caller is NOT an admin, None if OK.
async fn require_admin(state: &GateState, headers: &HeaderMap) -> Option<axum::response::Response> {
    // In dev mode, auth is disabled entirely — allow everything.
    if state.admin.dev_mode {
        return None;
    }

    let raw_key = match extract_bearer(headers) {
        Some(k) => k,
        None => {
            return Some(error_response(
                StatusCode::UNAUTHORIZED,
                "UNAUTHORIZED",
                "missing authorization header",
            ))
        }
    };

    if state.auth.key_verifier.verify_admin_key(&raw_key).await {
        None
    } else {
        Some(error_response(
            StatusCode::FORBIDDEN,
            "FORBIDDEN",
            "admin key required for key management operations",
        ))
    }
}

pub async fn health(State(state): State<GateState>) -> axum::response::Response {
    if state.shutdown.is_draining() {
        return (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response();
    }
    (StatusCode::OK, "ok").into_response()
}

pub async fn list_runes(State(state): State<GateState>) -> impl IntoResponse {
    let runes: Vec<serde_json::Value> = state
        .rune
        .relay
        .list()
        .into_iter()
        .map(|(name, gate_path)| serde_json::json!({"name": name, "gate_path": gate_path}))
        .collect();
    Json(serde_json::json!({"runes": runes}))
}

pub async fn mgmt_status(State(state): State<GateState>) -> impl IntoResponse {
    let uptime_secs = state.admin.started_at.elapsed().as_secs();
    let caster_count = state.rune.session_mgr.caster_count();
    let rune_count = state.rune.relay.list().len();

    Json(serde_json::json!({
        "uptime_secs": uptime_secs,
        "caster_count": caster_count,
        "rune_count": rune_count,
        "dev_mode": state.admin.dev_mode,
    }))
}

pub async fn mgmt_casters(State(state): State<GateState>) -> impl IntoResponse {
    let caster_ids = state.rune.session_mgr.list_caster_ids();

    // Build caster_id → rune names reverse mapping in one pass over entries
    let mut caster_runes: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for (name, _) in state.rune.relay.list() {
        if let Some(entries) = state.rune.relay.find(&name) {
            for e in entries.value().values() {
                if let Some(ref cid) = e.caster_id {
                    caster_runes
                        .entry(cid.clone())
                        .or_default()
                        .push(name.clone());
                }
            }
        }
    }

    let casters: Vec<serde_json::Value> = caster_ids
        .iter()
        .map(|cid| {
            let runes = caster_runes.remove(cid.as_str()).unwrap_or_default();
            let available_permits = state.rune.session_mgr.available_permits(cid);
            let max_concurrent = state.rune.session_mgr.max_concurrent(cid);
            let health = state.rune.session_mgr.health_info(cid).unwrap_or_default();
            let role = state
                .rune
                .session_mgr
                .caster_role(cid)
                .unwrap_or(rune_core::session::CasterRole::Caster);
            serde_json::json!({
                "caster_id": cid,
                "runes": runes,
                "role": role.as_str(),
                "max_concurrent": max_concurrent,
                "available_permits": available_permits,
                "pressure": health.pressure,
                "metrics": health.metrics,
                "health_status": format!("{:?}", health.status).to_uppercase(),
                "session_generation": state.rune.session_mgr.session_generation(cid)
                    .unwrap_or(0),
            })
        })
        .collect();
    Json(serde_json::json!({"casters": casters}))
}

pub async fn mgmt_stats(State(state): State<GateState>) -> impl IntoResponse {
    match state.admin.store.call_stats_enhanced().await {
        Ok((total, by_rune)) => {
            let rune_stats: Vec<serde_json::Value> = by_rune
                .into_iter()
                .map(|(name, count, avg_latency, success_rate, p95_latency)| {
                    serde_json::json!({
                        "rune_name": name,
                        "count": count,
                        "avg_latency_ms": avg_latency,
                        "success_rate": success_rate,
                        "p95_latency_ms": p95_latency,
                    })
                })
                .collect();
            Json(serde_json::json!({
                "total_calls": total,
                "by_rune": rune_stats,
            }))
            .into_response()
        }
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        ),
    }
}

pub async fn mgmt_caster_stats(State(state): State<GateState>) -> impl IntoResponse {
    match state.admin.store.call_stats_by_caster().await {
        Ok(stats) => Json(serde_json::json!({ "casters": stats })).into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        ),
    }
}

pub async fn mgmt_scaling_status(State(state): State<GateState>) -> impl IntoResponse {
    let groups = state
        .admin
        .scaling
        .as_ref()
        .map(|scaling| scaling.snapshot_status())
        .unwrap_or_default();
    Json(serde_json::json!({ "groups": groups })).into_response()
}

pub async fn mgmt_logs(
    State(state): State<GateState>,
    Query(params): Query<LogQuery>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(50).min(500);
    match state
        .admin
        .store
        .query_logs(params.rune.as_deref(), limit)
        .await
    {
        Ok(logs) => Json(serde_json::json!({"logs": logs})).into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        ),
    }
}

pub async fn mgmt_create_key(
    State(state): State<GateState>,
    headers: HeaderMap,
    Json(req): Json<CreateKeyRequest>,
) -> axum::response::Response {
    if let Some(denied) = require_admin(&state, &headers).await {
        return denied;
    }

    let key_type = match req.key_type.as_str() {
        "gate" => rune_store::KeyType::Gate,
        "caster" => rune_store::KeyType::Caster,
        "admin" => rune_store::KeyType::Admin,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "key_type must be 'gate', 'caster', or 'admin'",
            )
        }
    };

    match state.admin.store.create_key(key_type, &req.label).await {
        Ok(result) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "raw_key": result.raw_key,
                "key": result.api_key,
            })),
        )
            .into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        ),
    }
}

pub async fn mgmt_list_keys(
    State(state): State<GateState>,
    headers: HeaderMap,
) -> axum::response::Response {
    if let Some(denied) = require_admin(&state, &headers).await {
        return denied;
    }

    match state.admin.store.list_keys().await {
        Ok(keys) => Json(serde_json::json!({"keys": keys})).into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        ),
    }
}

pub async fn mgmt_revoke_key(
    State(state): State<GateState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> axum::response::Response {
    if let Some(denied) = require_admin(&state, &headers).await {
        return denied;
    }

    match state.admin.store.revoke_key(id).await {
        Ok(()) => Json(serde_json::json!({"status": "revoked", "id": id})).into_response(),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &e.to_string(),
        ),
    }
}
