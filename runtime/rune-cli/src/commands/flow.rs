use anyhow::{Context, Result};

use crate::client::RuneClient;

pub async fn register(client: &RuneClient, file: &str, json_mode: bool) -> Result<()> {
    // Validate file extension
    let ext = std::path::Path::new(file)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    if !["json", "yaml", "yml"].contains(&ext) {
        anyhow::bail!(
            "Unsupported file format '.{}'. Use .yaml, .yml, or .json",
            ext
        );
    }

    let content =
        std::fs::read_to_string(file).with_context(|| format!("Failed to read file: {}", file))?;

    let definition: serde_json::Value = if ext == "json" {
        serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse JSON from: {}", file))?
    } else {
        serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from: {}", file))?
    };

    let result = client.register_flow(definition).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        let name = result.get("name").and_then(|v| v.as_str()).unwrap_or(file);
        eprintln!("Flow '{}' registered successfully.", name);
    }
    Ok(())
}

pub async fn list(client: &RuneClient, _json_mode: bool) -> Result<()> {
    let result = client.list_flows().await?;
    // TODO(M2): text mode table output
    crate::output::print_json(&result);
    Ok(())
}

pub async fn get(client: &RuneClient, name: &str, _json_mode: bool) -> Result<()> {
    let result = client.get_flow(name).await?;
    crate::output::print_json(&result);
    Ok(())
}

pub async fn run(
    client: &RuneClient,
    name: &str,
    input: Option<&str>,
    _json_mode: bool,
) -> Result<()> {
    let result = client.run_flow(name, input).await?;
    // TODO(M2): text mode table output
    crate::output::print_json(&result);
    Ok(())
}

pub async fn delete(client: &RuneClient, name: &str, json_mode: bool) -> Result<()> {
    let result = client.delete_flow(name).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        eprintln!("Flow '{}' deleted", name);
    }
    Ok(())
}
