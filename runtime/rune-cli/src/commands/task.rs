use anyhow::Result;

use crate::client::RuneClient;

pub async fn get(client: &RuneClient, id: &str, _json: bool) -> Result<()> {
    let result = client.get_task(id).await?;
    crate::output::print_json(&result);
    Ok(())
}

pub async fn list(client: &RuneClient, _json: bool) -> Result<()> {
    // TODO: need GET /api/v1/tasks endpoint
    let _ = client;
    eprintln!("task list: not yet implemented");
    Ok(())
}

pub async fn wait(client: &RuneClient, id: &str, timeout: u64, _json: bool) -> Result<()> {
    let _ = (client, id, timeout);
    eprintln!("task wait: not yet implemented");
    Ok(())
}

pub async fn delete(client: &RuneClient, id: &str, _json: bool) -> Result<()> {
    let _ = (client, id);
    eprintln!("task delete: not yet implemented");
    Ok(())
}
