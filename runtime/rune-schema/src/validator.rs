#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("validation failed: {message}")]
    ValidationFailed { message: String, path: String },
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
}

/// Validate input JSON against a JSON Schema string.
/// Returns Ok(()) if valid or schema is None.
pub fn validate_input(schema: Option<&str>, input: &[u8]) -> Result<(), SchemaError> {
    do_validate(schema, input)
}

/// Validate output JSON against a JSON Schema string.
pub fn validate_output(schema: Option<&str>, output: &[u8]) -> Result<(), SchemaError> {
    do_validate(schema, output)
}

fn do_validate(schema: Option<&str>, data: &[u8]) -> Result<(), SchemaError> {
    let schema_str = match schema {
        Some(s) => s,
        None => return Ok(()),
    };

    // Parse schema string as JSON
    let schema_value: serde_json::Value = serde_json::from_str(schema_str)
        .map_err(|e| SchemaError::InvalidSchema(e.to_string()))?;

    // Build validator from schema
    let validator = jsonschema::validator_for(&schema_value)
        .map_err(|e| SchemaError::InvalidSchema(e.to_string()))?;

    // Parse input data as JSON
    let instance: serde_json::Value = serde_json::from_slice(data)
        .map_err(|e| SchemaError::ValidationFailed {
            message: format!("invalid JSON: {}", e),
            path: String::new(),
        })?;

    // Validate
    let errors: Vec<_> = validator.iter_errors(&instance).collect();
    if errors.is_empty() {
        Ok(())
    } else {
        let messages: Vec<String> = errors
            .iter()
            .map(|e| format!("{} at path {}", e, e.instance_path))
            .collect();
        Err(SchemaError::ValidationFailed {
            message: messages.join("; "),
            path: errors[0].instance_path.to_string(),
        })
    }
}
