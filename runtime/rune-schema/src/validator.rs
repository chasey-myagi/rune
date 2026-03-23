use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock, OnceLock};

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("validation failed: {message}")]
    ValidationFailed { message: String, path: String },
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
}

// ---------------------------------------------------------------------------
// Validator cache: keyed by hash of schema string
// ---------------------------------------------------------------------------

type ValidatorCache = RwLock<HashMap<u64, Arc<jsonschema::Validator>>>;

static VALIDATOR_CACHE: OnceLock<ValidatorCache> = OnceLock::new();

fn cache() -> &'static ValidatorCache {
    VALIDATOR_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn hash_schema(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

fn get_or_compile(schema_str: &str) -> Result<Arc<jsonschema::Validator>, SchemaError> {
    let key = hash_schema(schema_str);

    // Fast path: read lock
    {
        let cache = cache().read().unwrap();
        if let Some(v) = cache.get(&key) {
            return Ok(Arc::clone(v));
        }
    }

    // Slow path: compile and insert with write lock
    let schema_value: serde_json::Value = serde_json::from_str(schema_str)
        .map_err(|e| SchemaError::InvalidSchema(e.to_string()))?;
    let validator = jsonschema::validator_for(&schema_value)
        .map_err(|e| SchemaError::InvalidSchema(e.to_string()))?;
    let v = Arc::new(validator);

    let mut cache = cache().write().unwrap();
    cache.insert(key, Arc::clone(&v));
    Ok(v)
}

/// Clear the validator cache. Mainly used for benchmarking cold-start scenarios.
pub fn clear_validator_cache() {
    if let Some(c) = VALIDATOR_CACHE.get() {
        c.write().unwrap().clear();
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

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

    // Get compiled validator from cache (or compile + cache)
    let validator = get_or_compile(schema_str)?;

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
