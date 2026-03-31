use anyhow::Result;

use crate::client::RuneClient;

pub async fn create(
    client: &RuneClient,
    key_type: &str,
    label: &str,
    _json_mode: bool,
) -> Result<()> {
    let result = client.create_key(key_type, label).await?;
    // TODO(M2): text mode table output
    crate::output::print_json(&result);
    Ok(())
}

pub async fn list(client: &RuneClient, _json_mode: bool) -> Result<()> {
    let result = client.list_keys().await?;
    // TODO(M2): text mode table output
    crate::output::print_json(&result);
    Ok(())
}

pub async fn revoke(client: &RuneClient, key_id: &str, _json_mode: bool) -> Result<()> {
    let result = client.revoke_key(key_id).await?;
    // TODO(M2): text mode table output
    crate::output::print_json(&result);
    Ok(())
}
