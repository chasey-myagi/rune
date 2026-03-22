use anyhow::Result;

/// Retrieve logs, optionally filtered by Rune name.
pub async fn logs(_rune: Option<&str>, _limit: u32) -> Result<()> {
    todo!()
}

/// Show runtime statistics.
pub async fn stats() -> Result<()> {
    todo!()
}
