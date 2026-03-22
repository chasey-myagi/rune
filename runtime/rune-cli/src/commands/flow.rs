use anyhow::Result;

/// Register a flow from a YAML or JSON file.
pub async fn register(_file: &str) -> Result<()> {
    todo!()
}

/// List all registered flows.
pub async fn list() -> Result<()> {
    todo!()
}

/// Run a flow by name with optional input.
pub async fn run(_name: &str, _input: Option<&str>) -> Result<()> {
    todo!()
}

/// Delete a flow by name.
pub async fn delete(_name: &str) -> Result<()> {
    todo!()
}
