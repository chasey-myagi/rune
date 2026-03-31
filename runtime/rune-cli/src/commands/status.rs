use anyhow::Result;
use serde_json::json;

use crate::client::RuneClient;
use crate::output;
use crate::runtime::{self, state};

/// Run the status command: check local state + remote API health.
pub async fn run(client: &RuneClient, json_mode: bool) -> Result<()> {
    // Local state info
    let local_state = state::read_state()?;
    let is_alive = local_state
        .as_ref()
        .map(|s| runtime::is_runtime_alive(s))
        .unwrap_or(false);

    // Remote API check
    let api_status: Option<serde_json::Value> = client.status().await.ok();
    let api_reachable = api_status.is_some();

    if json_mode {
        let result = json!({
            "running": is_alive || api_reachable,
            "local_state": local_state.as_ref().map(|s| json!({
                "mode": format!("{:?}", s.mode).to_lowercase(),
                "container_id": s.container_id,
                "pid": s.pid,
                "http_port": s.http_port,
                "grpc_port": s.grpc_port,
                "started_at": s.started_at.to_rfc3339(),
                "dev_mode": s.dev_mode,
            })),
            "api": api_status,
        });
        output::print_json(&result);
        return Ok(());
    }

    // Text output
    match (&local_state, is_alive) {
        (Some(s), true) => {
            let id_info = match s.mode {
                state::RuntimeMode::Docker => {
                    format!("docker: {}", s.container_id.as_deref().unwrap_or("?"))
                }
                state::RuntimeMode::Binary => {
                    format!("PID {}", s.pid.unwrap_or(0))
                }
            };
            let elapsed = chrono::Utc::now().signed_duration_since(s.started_at);
            let uptime = format_duration(elapsed);

            println!("Runtime:    running ({})", id_info);
            println!("Uptime:     {}", uptime);
            println!("HTTP:       http://127.0.0.1:{}", s.http_port);
            println!("gRPC:       127.0.0.1:{}", s.grpc_port);
            if s.dev_mode {
                println!("Mode:       development");
            }
        }
        (Some(_), false) => {
            println!("Runtime:    not running (stale state)");
            eprintln!("Hint: Run `rune stop` to clean up, then `rune start`");
        }
        (None, _) => {
            if api_reachable {
                println!("Runtime:    running (remote: {})", client.base_url);
            } else {
                println!("Runtime:    not running");
                eprintln!("\nStart with: rune start");
                return Ok(());
            }
        }
    }

    // API details
    if let Some(ref status) = api_status {
        println!();
        if let Some(runes) = status.get("runes_count") {
            println!("Runes:      {}", runes);
        }
        if let Some(casters) = status.get("casters_count") {
            println!("Casters:    {}", casters);
        }
        if let Some(version) = status.get("version") {
            println!("Version:    {}", version);
        }
    } else if is_alive {
        println!();
        eprintln!("API:        unreachable ({})", client.base_url);
    }

    Ok(())
}

/// Format a chrono::Duration into a human-readable string.
pub fn format_duration(d: chrono::Duration) -> String {
    let total_secs = d.num_seconds();
    if total_secs < 60 {
        format!("{}s", total_secs)
    } else if total_secs < 3600 {
        format!("{}m {}s", total_secs / 60, total_secs % 60)
    } else {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration_seconds() {
        let d = chrono::Duration::seconds(45);
        assert_eq!(format_duration(d), "45s");
    }

    #[test]
    fn test_format_duration_minutes() {
        let d = chrono::Duration::seconds(125);
        assert_eq!(format_duration(d), "2m 5s");
    }

    #[test]
    fn test_format_duration_hours() {
        let d = chrono::Duration::seconds(7380);
        assert_eq!(format_duration(d), "2h 3m");
    }

    #[test]
    fn test_format_duration_zero() {
        let d = chrono::Duration::seconds(0);
        assert_eq!(format_duration(d), "0s");
    }
}
