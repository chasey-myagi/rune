use anyhow::Result;

use crate::client::RuneClient;

pub async fn run(client: &RuneClient, json_mode: bool) -> Result<()> {
    let result = client.casters().await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result); // placeholder -- future task adds table
    }
    Ok(())
}
