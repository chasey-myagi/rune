#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("validation failed: {message}")]
    ValidationFailed { message: String, path: String },
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
}

/// Validate input JSON against a JSON Schema string.
/// Returns Ok(()) if valid or schema is None.
pub fn validate_input(_schema: Option<&str>, _input: &[u8]) -> Result<(), SchemaError> {
    todo!()
}

/// Validate output JSON against a JSON Schema string.
pub fn validate_output(_schema: Option<&str>, _output: &[u8]) -> Result<(), SchemaError> {
    todo!()
}
