use anyhow::{Context, Result};
use rune_store::RuneStore;

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

pub async fn bootstrap(db_path: &str, label: &str, force: bool, json_mode: bool) -> Result<()> {
    let store = RuneStore::open(db_path)
        .with_context(|| format!("failed to open database at '{db_path}'"))?;

    if store.has_admin_key_sync()? && !force {
        anyhow::bail!(
            "admin key(s) already exist in '{}'; rerun with --force to create another one",
            db_path
        );
    }

    let result = store.bootstrap_admin_key(label)?;

    if json_mode {
        crate::output::print_json(&serde_json::json!({
            "raw_key": result.raw_key,
            "key": result.api_key,
        }));
    } else {
        println!("Admin key created successfully.");
        println!("Key: {}", result.raw_key);
        println!("Label: {}", result.api_key.label);
        println!("Database: {}", db_path);
        println!();
        println!("Set it for follow-up commands:");
        println!("  export RUNE_KEY={}", result.raw_key);
    }

    if reqwest::Client::new()
        .get("http://127.0.0.1:50060/health")
        .send()
        .await
        .map(|resp| resp.status().is_success())
        .unwrap_or(false)
    {
        eprintln!(
            "A local Rune runtime appears to be running; restart it if the new admin key is not picked up immediately."
        );
    }

    Ok(())
}
