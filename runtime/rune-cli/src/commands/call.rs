use anyhow::Result;

use crate::client::RuneClient;

pub async fn run(
    client: &RuneClient,
    name: &str,
    input: Option<&str>,
    stream: bool,
    async_mode: bool,
    input_file: Option<&str>,
    _timeout: u64,
    json_mode: bool,
) -> Result<()> {
    let input_json = resolve_input(input, input_file)?;
    if stream {
        client.call_rune_stream(name, Some(&input_json)).await?;
    } else if async_mode {
        let result = client.call_rune_async(name, Some(&input_json)).await?;
        crate::output::print_json(&result);
    } else {
        let result = client.call_rune(name, Some(&input_json)).await?;
        if json_mode {
            crate::output::print_json(&result);
        } else {
            crate::output::print_json(&result);
        }
    }
    Ok(())
}

/// Resolve input from argument, file, or stdin. Must be valid JSON.
fn resolve_input(input: Option<&str>, input_file: Option<&str>) -> Result<String> {
    let raw = if let Some(file) = input_file {
        std::fs::read_to_string(file)
            .map_err(|e| anyhow::anyhow!("Failed to read input file '{}': {}", file, e))?
    } else if let Some(s) = input {
        s.to_string()
    } else {
        "{}".to_string()
    };

    // Validate JSON
    serde_json::from_str::<serde_json::Value>(&raw)
        .map_err(|e| anyhow::anyhow!("Invalid JSON input: {}", e))?;

    Ok(raw)
}
