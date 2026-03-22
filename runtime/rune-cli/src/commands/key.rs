use anyhow::Result;

use super::super::client::RuneClient;

/// Create a new API key.
pub async fn create(client: &RuneClient, key_type: &str, label: &str) -> Result<()> {
    let result = client.create_key(key_type, label).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}

/// List all API keys.
pub async fn list(client: &RuneClient) -> Result<()> {
    let result = client.list_keys().await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}

/// Revoke an API key by ID.
pub async fn revoke(client: &RuneClient, key_id: &str) -> Result<()> {
    let result = client.revoke_key(key_id).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}
