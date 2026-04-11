pub mod binary;
pub mod docker;
pub mod state;

use anyhow::Result;
use state::RuntimeState;

/// Wait for Runtime to become healthy by polling /health.
///
/// `log_hint` is shown on timeout to help the user diagnose (e.g. "docker logs rune-runtime").
pub async fn wait_for_healthy(base_url: &str, timeout_secs: u64, log_hint: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/health", base_url);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

    loop {
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "Runtime failed to start within {}s. Check logs with:\n  {}",
                timeout_secs,
                log_hint
            );
        }

        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
}

/// Check if the recorded runtime is still alive.
pub fn is_runtime_alive(state: &RuntimeState) -> bool {
    match state.mode {
        state::RuntimeMode::Docker => state
            .container_id
            .as_ref()
            .map(|id| docker::is_container_running(id))
            .unwrap_or(false),
        state::RuntimeMode::Binary => state.pid.map(binary::is_process_alive).unwrap_or(false),
    }
}
