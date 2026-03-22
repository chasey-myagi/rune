use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::error::error_response;
use crate::state::{CreateKeyRequest, GateState, LogQuery};

pub async fn health(State(state): State<GateState>) -> axum::response::Response {
    if state.shutdown.is_draining() {
        return (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response();
    }
    (StatusCode::OK, "ok").into_response()
}

pub async fn list_runes(State(state): State<GateState>) -> impl IntoResponse {
    let runes: Vec<serde_json::Value> = state
        .relay
        .list()
        .into_iter()
        .map(|(name, gate_path)| serde_json::json!({"name": name, "gate_path": gate_path}))
        .collect();
    Json(serde_json::json!({"runes": runes}))
}

pub async fn mgmt_status(State(state): State<GateState>) -> impl IntoResponse {
    let uptime_secs = state.started_at.elapsed().as_secs();
    let caster_count = state.session_mgr.caster_count();
    let rune_count = state.relay.list().len();

    Json(serde_json::json!({
        "uptime_secs": uptime_secs,
        "caster_count": caster_count,
        "rune_count": rune_count,
        "dev_mode": state.dev_mode,
    }))
}

pub async fn mgmt_casters(State(state): State<GateState>) -> impl IntoResponse {
    let caster_ids = state.session_mgr.list_caster_ids();
    let casters: Vec<serde_json::Value> = caster_ids
        .iter()
        .map(|cid| {
            // Collect rune names registered by this caster
            let runes: Vec<String> = state.relay.list()
                .iter()
                .filter_map(|(name, _)| {
                    if let Some(entries) = state.relay.find(name) {
                        if entries.value().iter().any(|e| e.caster_id.as_deref() == Some(cid)) {
                            return Some(name.clone());
                        }
                    }
                    None
                })
                .collect();
            let current_load = state.session_mgr.available_permits(cid);
            serde_json::json!({
                "caster_id": cid,
                "runes": runes,
                "current_load": current_load,
                "connected_since": state.started_at.elapsed().as_secs(),
            })
        })
        .collect();
    Json(serde_json::json!({"casters": casters}))
}

pub async fn mgmt_stats(State(state): State<GateState>) -> impl IntoResponse {
    match state.store.call_stats_enhanced().await {
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
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}


pub async fn mgmt_logs(
    State(state): State<GateState>,
    Query(params): Query<LogQuery>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(50).min(500);
    match state.store.query_logs(params.rune.as_deref(), limit).await {
        Ok(logs) => Json(serde_json::json!({"logs": logs})).into_response(),
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}


pub async fn mgmt_create_key(
    State(state): State<GateState>,
    Json(req): Json<CreateKeyRequest>,
) -> impl IntoResponse {
    let key_type = match req.key_type.as_str() {
        "gate" => rune_store::KeyType::Gate,
        "caster" => rune_store::KeyType::Caster,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "key_type must be 'gate' or 'caster'",
            )
        }
    };

    match state.store.create_key(key_type, &req.label).await {
        Ok(result) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "raw_key": result.raw_key,
                "key": result.api_key,
            })),
        )
            .into_response(),
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}

pub async fn mgmt_list_keys(State(state): State<GateState>) -> impl IntoResponse {
    match state.store.list_keys().await {
        Ok(keys) => Json(serde_json::json!({"keys": keys})).into_response(),
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}

pub async fn mgmt_revoke_key(
    State(state): State<GateState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    match state.store.revoke_key(id).await {
        Ok(()) => Json(serde_json::json!({"status": "revoked", "id": id})).into_response(),
        Err(e) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &e.to_string())
        }
    }
}
