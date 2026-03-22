use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
};
use rune_schema::openapi::{generate_openapi, RuneInfo};

use crate::state::GateState;

pub async fn openapi_handler(State(state): State<GateState>) -> impl IntoResponse {
    let rune_infos: Vec<RuneInfo> = state
        .relay
        .list()
        .into_iter()
        .filter_map(|(name, _gate_path)| {
            let entries = state.relay.find(&name)?;
            let first = entries.value().first()?;
            let config = &first.config;
            Some(RuneInfo {
                name: config.name.clone(),
                gate_path: config.gate.as_ref().map(|g| g.path.clone()),
                gate_method: config
                    .gate
                    .as_ref()
                    .map(|g| g.method.clone())
                    .unwrap_or_else(|| "POST".to_string()),
                input_schema: config.input_schema.clone(),
                output_schema: config.output_schema.clone(),
                description: config.description.clone(),
            })
        })
        .collect();

    let openapi = generate_openapi(&rune_infos);
    (StatusCode::OK, Json(openapi)).into_response()
}
