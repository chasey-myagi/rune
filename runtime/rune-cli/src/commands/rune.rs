use anyhow::Result;

use super::super::client::RuneClient;

/// List all online Runes.
pub async fn list(client: &RuneClient) -> Result<()> {
    let result = client.list_runes().await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}

/// Call a Rune (sync, stream, or async depending on flags).
pub async fn call(
    client: &RuneClient,
    name: &str,
    input: Option<&str>,
    stream: bool,
    async_mode: bool,
) -> Result<()> {
    if stream {
        // Streaming mode — SSE events are printed to stdout within call_rune_stream
        client.call_rune_stream(name, input).await?;
    } else if async_mode {
        let result = client.call_rune_async(name, input).await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&result).unwrap_or_default()
        );
    } else {
        let result = client.call_rune(name, input).await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&result).unwrap_or_default()
        );
    }
    Ok(())
}

/// Query an async task by ID.
pub async fn task(client: &RuneClient, id: &str) -> Result<()> {
    let result = client.get_task(id).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).unwrap_or_default()
    );
    Ok(())
}
