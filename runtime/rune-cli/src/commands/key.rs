use anyhow::Result;

use crate::client::RuneClient;

pub async fn create(
    client: &RuneClient,
    key_type: &str,
    label: &str,
    json_mode: bool,
) -> Result<()> {
    let result = client.create_key(key_type, label).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result); // placeholder -- future task adds formatted output
    }
    Ok(())
}

pub async fn list(client: &RuneClient, json_mode: bool) -> Result<()> {
    let result = client.list_keys().await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn revoke(client: &RuneClient, key_id: &str, json_mode: bool) -> Result<()> {
    let result = client.revoke_key(key_id).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}
