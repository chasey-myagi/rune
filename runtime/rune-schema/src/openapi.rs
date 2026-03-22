use serde_json::{json, Value};

pub struct RuneInfo {
    pub name: String,
    pub gate_path: Option<String>,
    pub gate_method: String,
    pub input_schema: Option<String>,
    pub output_schema: Option<String>,
    pub description: String,
}

/// Generate OpenAPI 3.0 JSON from a list of registered Runes.
pub fn generate_openapi(runes: &[RuneInfo]) -> Value {
    let mut paths = serde_json::Map::new();

    for rune in runes {
        let gate_path = match &rune.gate_path {
            Some(p) => p.clone(),
            None => continue, // skip runes without gate_path
        };

        let method = rune.gate_method.to_lowercase();

        // Build operation object
        let mut operation = serde_json::Map::new();
        operation.insert("summary".to_string(), json!(rune.description));
        operation.insert("operationId".to_string(), json!(rune.name));

        // Add requestBody if input_schema is present
        if let Some(ref input_schema_str) = rune.input_schema {
            if let Ok(schema_value) = serde_json::from_str::<Value>(input_schema_str) {
                operation.insert(
                    "requestBody".to_string(),
                    json!({
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": schema_value
                            }
                        }
                    }),
                );
            }
        }

        // Build responses
        let mut responses = serde_json::Map::new();
        if let Some(ref output_schema_str) = rune.output_schema {
            if let Ok(schema_value) = serde_json::from_str::<Value>(output_schema_str) {
                responses.insert(
                    "200".to_string(),
                    json!({
                        "description": "Successful response",
                        "content": {
                            "application/json": {
                                "schema": schema_value
                            }
                        }
                    }),
                );
            }
        } else {
            responses.insert(
                "200".to_string(),
                json!({
                    "description": "Successful response"
                }),
            );
        }
        operation.insert("responses".to_string(), Value::Object(responses));

        // Insert into paths, merging if path already exists (different methods)
        let path_item = paths
            .entry(gate_path)
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if let Value::Object(ref mut map) = path_item {
            map.insert(method, Value::Object(operation));
        }
    }

    json!({
        "openapi": "3.0.0",
        "info": {
            "title": "Rune API",
            "version": "1.0.0"
        },
        "paths": Value::Object(paths)
    })
}
