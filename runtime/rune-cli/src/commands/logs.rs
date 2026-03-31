use anyhow::Result;

use crate::client::RuneClient;

pub async fn run(
    client: &RuneClient,
    rune: Option<&str>,
    limit: u32,
    _json_mode: bool,
) -> Result<()> {
    let result = client.get_logs(rune, limit).await?;
    // TODO(M2): text mode table output
    crate::output::print_json(&result);
    Ok(())
}

pub async fn stats(client: &RuneClient, _json_mode: bool) -> Result<()> {
    let result = client.get_stats().await?;
    // TODO(M2): text mode table output
    crate::output::print_json(&result);
    Ok(())
}
