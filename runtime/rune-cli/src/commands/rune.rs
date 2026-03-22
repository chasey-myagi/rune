use anyhow::Result;

/// List all online Runes.
pub async fn list() -> Result<()> {
    todo!()
}

/// Call a Rune (sync, stream, or async depending on flags).
pub async fn call(
    _name: &str,
    _input: Option<&str>,
    _stream: bool,
    _async_mode: bool,
) -> Result<()> {
    todo!()
}

/// Query an async task by ID.
pub async fn task(_id: &str) -> Result<()> {
    todo!()
}
