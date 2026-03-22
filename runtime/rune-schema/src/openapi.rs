use serde_json::Value;

pub struct RuneInfo {
    pub name: String,
    pub gate_path: Option<String>,
    pub gate_method: String,
    pub input_schema: Option<String>,
    pub output_schema: Option<String>,
    pub description: String,
}

/// Generate OpenAPI 3.0 JSON from a list of registered Runes.
pub fn generate_openapi(_runes: &[RuneInfo]) -> Value {
    todo!()
}
