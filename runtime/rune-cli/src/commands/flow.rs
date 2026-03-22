use anyhow::{Context, Result};

use super::super::client::RuneClient;

/// Register a flow from a YAML or JSON file.
pub async fn register(client: &RuneClient, file: &str) -> Result<()> {
    let content =
        std::fs::read_to_string(file).with_context(|| format!("Failed to read file: {}", file))?;

    // Try parsing as JSON first, then YAML
    let definition: serde_json::Value = if file.ends_with(".json") {
        serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse JSON from: {}", file))?
    } else {
        // Assume YAML (.yaml, .yml, or anything else)
        serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from: {}", file))?
    };

    let result = client.register_flow(definition).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}

/// List all registered flows.
pub async fn list(client: &RuneClient) -> Result<()> {
    let result = client.list_flows().await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}

/// Run a flow by name with optional input.
pub async fn run(client: &RuneClient, name: &str, input: Option<&str>) -> Result<()> {
    let result = client.run_flow(name, input).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}

/// Delete a flow by name.
pub async fn delete(client: &RuneClient, name: &str) -> Result<()> {
    let result = client.delete_flow(name).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}
