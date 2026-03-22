use anyhow::Result;

/// Create a new API key.
pub async fn create(_key_type: &str, _label: &str) -> Result<()> {
    todo!()
}

/// List all API keys.
pub async fn list() -> Result<()> {
    todo!()
}

/// Revoke an API key by ID.
pub async fn revoke(_key_id: &str) -> Result<()> {
    todo!()
}
