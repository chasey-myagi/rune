use anyhow::Result;

use crate::client::RuneClient;

pub async fn run(client: &RuneClient, _json_mode: bool) -> Result<()> {
    let result = client.casters().await?;
    // TODO(M2): text mode table output
    crate::output::print_json(&result);
    Ok(())
}
