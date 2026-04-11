use crate::runtime::{self, binary, docker, state};
use anyhow::Result;

pub async fn run(force: bool, timeout: u64) -> Result<()> {
    let current = match state::read_state()? {
        Some(s) => s,
        None => {
            eprintln!("Runtime is not running (no state file found).");
            return Ok(());
        }
    };

    if !runtime::is_runtime_alive(&current) {
        eprintln!("Runtime is not running (stale state). Cleaning up...");
        state::remove_state()?;
        return Ok(());
    }

    match current.mode {
        state::RuntimeMode::Docker => {
            let cid = current.container_id.as_deref().unwrap_or("unknown");
            if force {
                eprintln!(
                    "Force-stopping Runtime (container {})...",
                    &cid[..12.min(cid.len())]
                );
                docker::kill_container(cid)?;
            } else {
                eprintln!(
                    "Stopping Runtime (container {})...",
                    &cid[..12.min(cid.len())]
                );
                docker::stop_container(cid, timeout)?;
            }
        }
        state::RuntimeMode::Binary => {
            let pid = current
                .pid
                .ok_or_else(|| anyhow::anyhow!("Binary mode state has no PID"))?;
            if force {
                eprintln!("Force-stopping Runtime (PID {})...", pid);
                binary::send_sigkill(pid)?;
            } else {
                eprintln!("Stopping Runtime (PID {})...", pid);
                binary::send_sigterm(pid)?;

                // Wait for process to exit
                let deadline =
                    tokio::time::Instant::now() + std::time::Duration::from_secs(timeout);

                loop {
                    if !binary::is_process_alive(pid) {
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        eprintln!(
                            "Graceful shutdown timed out after {}s. Sending SIGKILL...",
                            timeout
                        );
                        binary::send_sigkill(pid)?;
                        // Brief wait for SIGKILL to take effect
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
    }

    state::remove_state()?;
    eprintln!("Runtime stopped.");
    Ok(())
}
