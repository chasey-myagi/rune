use anyhow::{Context, Result};
use serde_json::Value;

use crate::client::RuneClient;

/// Resolve the JSON input for a `rune call` invocation.
///
/// Priority: `--input` flag > `--input-file` flag > default `{}`.
///
/// The resolved value **must** be valid JSON. Plain strings like `"hello"` or
/// malformed JSON like `"{bad"` are rejected with a clear error message.
pub fn resolve_input(input: Option<&str>, input_file: Option<&str>) -> Result<Value> {
    if let Some(raw) = input {
        let value: Value = serde_json::from_str(raw)
            .map_err(|e| anyhow::anyhow!("Invalid JSON input: {}", e))?;
        return Ok(value);
    }

    if let Some(path) = input_file {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read input file: {}", path))?;
        let value: Value = serde_json::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Invalid JSON in file '{}': {}", path, e))?;
        return Ok(value);
    }

    // Default: empty object
    Ok(serde_json::json!({}))
}

/// Execute `rune call <name>` with the resolved input.
///
/// Dispatches to sync / stream / async based on flags, then optionally
/// formats the output as JSON.
pub async fn run(
    client: &RuneClient,
    name: &str,
    input: Option<&str>,
    input_file: Option<&str>,
    stream: bool,
    async_mode: bool,
    timeout_secs: u64,
    json_output: bool,
) -> Result<()> {
    let payload = resolve_input(input, input_file)?;
    let payload_str = serde_json::to_string(&payload)?;
    let timeout = std::time::Duration::from_secs(timeout_secs);

    let call_future = async {
        if stream {
            let output = client.call_rune_stream(name, Some(&payload_str)).await?;
            if json_output {
                let result = serde_json::json!({ "output": output.trim_end() });
                crate::output::print_json(&result);
            }
        } else {
            let result = if async_mode {
                client.call_rune_async(name, Some(&payload_str)).await?
            } else {
                client.call_rune(name, Some(&payload_str)).await?
            };
            if json_output {
                crate::output::print_json_compact(&result);
            } else {
                crate::output::print_json(&result);
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    match tokio::time::timeout(timeout, call_future).await {
        Ok(result) => result,
        Err(_) => anyhow::bail!("Request timed out after {}s", timeout_secs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // 1. Valid JSON passes through
    #[test]
    fn test_resolve_input_valid_json() {
        let result = resolve_input(Some(r#"{"key": "value"}"#), None);
        assert!(result.is_ok());
        let val = result.unwrap();
        assert_eq!(val["key"], "value");
    }

    // 2. Invalid JSON returns error containing "Invalid JSON input"
    #[test]
    fn test_resolve_input_invalid_json_errors() {
        let result = resolve_input(Some("{bad json"), None);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid JSON input"),
            "Expected 'Invalid JSON input' in error, got: {}",
            err_msg
        );
    }

    // 3. None input defaults to empty object {}
    #[test]
    fn test_resolve_input_none_defaults_to_empty_object() {
        let result = resolve_input(None, None);
        assert!(result.is_ok());
        let val = result.unwrap();
        assert_eq!(val, serde_json::json!({}));
    }

    // 4. Plain string (not JSON) returns error — no auto-wrapping!
    #[test]
    fn test_resolve_input_plain_string_errors() {
        let result = resolve_input(Some("hello"), None);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid JSON input"),
            "Expected 'Invalid JSON input' in error, got: {}",
            err_msg
        );
    }

    // 5. Read JSON from a file via --input-file
    #[test]
    fn test_resolve_input_from_file() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, r#"{{"name": "rune"}}"#).unwrap();
        let path = tmp.path().to_str().unwrap();

        let result = resolve_input(None, Some(path));
        assert!(result.is_ok());
        let val = result.unwrap();
        assert_eq!(val["name"], "rune");
    }

    // 6. File not found returns error
    #[test]
    fn test_resolve_input_file_not_found() {
        let result = resolve_input(None, Some("/tmp/nonexistent_rune_test_file_42.json"));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Failed to read input file"),
            "Expected 'Failed to read input file' in error, got: {}",
            err_msg
        );
    }

    // 7. File with invalid JSON returns error
    #[test]
    fn test_resolve_input_file_invalid_json() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, "not valid json {{").unwrap();
        let path = tmp.path().to_str().unwrap();

        let result = resolve_input(None, Some(path));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid JSON in file"),
            "Expected 'Invalid JSON in file' in error, got: {}",
            err_msg
        );
    }
}
