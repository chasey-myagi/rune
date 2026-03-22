use anyhow::Result;

use super::super::client::RuneClient;

/// Retrieve logs, optionally filtered by Rune name.
pub async fn logs(client: &RuneClient, rune: Option<&str>, limit: u32) -> Result<()> {
    let result = client.get_logs(rune, limit).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}

/// Show runtime statistics.
pub async fn stats(client: &RuneClient) -> Result<()> {
    let result = client.get_stats().await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}
