use anyhow::Result;

use crate::client::RuneClient;

pub async fn run(
    client: &RuneClient,
    rune: Option<&str>,
    limit: u32,
    _follow: bool,
    json_mode: bool,
) -> Result<()> {
    let result = client.get_logs(rune, limit).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn stats(client: &RuneClient, json_mode: bool) -> Result<()> {
    let result = client.get_stats().await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}
