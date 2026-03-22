use anyhow::Result;

use super::super::client::RuneClient;
use super::super::daemon;

/// Start the Runtime process.
pub async fn start(_dev: bool, _config: Option<&str>) -> Result<()> {
    // Check if already running
    if let Ok(Some(pid)) = daemon::read_pid() {
        if daemon::is_process_alive(pid) {
            eprintln!("Runtime is already running (PID {})", pid);
            return Ok(());
        }
    }

    println!("To start the Rune Runtime, run:");
    println!();
    if _dev {
        println!("  rune-server --dev");
    } else if let Some(cfg) = _config {
        println!("  rune-server --config {}", cfg);
    } else {
        println!("  rune-server");
    }
    println!();
    println!("Or run it in the background:");
    println!("  rune-server &");

    Ok(())
}

/// Stop the background Runtime by sending SIGTERM to the PID.
pub async fn stop() -> Result<()> {
    match daemon::read_pid()? {
        Some(pid) => {
            if !daemon::is_process_alive(pid) {
                println!("Runtime process (PID {}) is not running. Cleaning up PID file.", pid);
                daemon::remove_pid_file()?;
                return Ok(());
            }

            println!("Stopping Runtime (PID {})...", pid);

            daemon::send_signal(pid, libc::SIGTERM)?;

            daemon::remove_pid_file()?;
            println!("Runtime stopped.");
            Ok(())
        }
        None => {
            eprintln!("No PID file found. Runtime may not be running.");
            Ok(())
        }
    }
}

/// Check Runtime status via PID file and HTTP health check.
pub async fn status(client: &RuneClient) -> Result<()> {
    // First check PID file
    match daemon::read_pid()? {
        Some(pid) => {
            if daemon::is_process_alive(pid) {
                println!("Runtime process: running (PID {})", pid);
            } else {
                println!("Runtime process: dead (stale PID {})", pid);
            }
        }
        None => {
            println!("Runtime process: no PID file");
        }
    }

    // Then try HTTP status
    match client.status().await {
        Ok(status) => {
            println!(
                "Runtime API:     connected ({})",
                client.base_url
            );
            println!(
                "{}",
                serde_json::to_string_pretty(&status).unwrap_or_default()
            );
        }
        Err(e) => {
            eprintln!(
                "Runtime API:     unreachable ({}) — {}",
                client.base_url, e
            );
        }
    }

    Ok(())
}
