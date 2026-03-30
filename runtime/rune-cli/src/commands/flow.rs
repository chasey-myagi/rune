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
        eprintln!("Flow registered successfully");
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn list(client: &RuneClient, json_mode: bool) -> Result<()> {
    let result = client.list_flows().await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn get(client: &RuneClient, name: &str, json_mode: bool) -> Result<()> {
    let path = client.build_path("/api/v1/flows/{name}", &[("name", name)]);
    let req = client.get_request(&path);
    let result = client.send_json(req).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn run(
    client: &RuneClient,
    name: &str,
    input: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let result = client.run_flow(name, input).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
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
