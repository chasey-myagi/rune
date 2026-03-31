use anyhow::{Context, Result};
use std::process::Command;

/// Check if Docker CLI is available.
pub fn is_docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Start a Runtime container. Returns the container ID.
pub fn start_container(
    image: &str,
    tag: &str,
    http_port: u16,
    grpc_port: u16,
    dev: bool,
) -> Result<String> {
    let image_ref = format!("{}:{}", image, tag);
    let http_mapping = format!("{}:50060", http_port);
    let grpc_mapping = format!("{}:50070", grpc_port);

    let mut args = vec![
        "run",
        "-d",
        "--name",
        "rune-runtime",
        "-p",
        &http_mapping,
        "-p",
        &grpc_mapping,
    ];

    // Pass dev mode as environment variable
    if dev {
        args.extend_from_slice(&["-e", "RUNE_DEV=true"]);
    }

    args.push(&image_ref);

    if dev {
        args.push("--dev");
    }

    let output = Command::new("docker")
        .args(&args)
        .output()
        .context("Failed to execute 'docker run'")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);

        if stderr.contains("is already in use") {
            anyhow::bail!(
                "Container name 'rune-runtime' is already in use.\n\
                 Run `rune stop` first, or `docker rm -f rune-runtime`"
            );
        }
        if stderr.contains("address already in use") || stderr.contains("port is already allocated")
        {
            anyhow::bail!(
                "Port {} or {} is already in use.\n\
                 Use --http-port / --grpc-port to specify different ports",
                http_port,
                grpc_port
            );
        }

        anyhow::bail!("docker run failed: {}", stderr.trim());
    }

    let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(container_id)
}

/// Stop and remove a container by ID.
pub fn stop_container(container_id: &str, timeout: u64) -> Result<()> {
    let status = Command::new("docker")
        .args(["stop", "--time", &timeout.to_string(), container_id])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .context("Failed to execute 'docker stop'")?;

    if !status.success() {
        // Container may already be stopped -- try rm anyway
        eprintln!("Warning: docker stop returned non-zero, attempting cleanup...");
    }

    // Remove the container
    let _ = Command::new("docker")
        .args(["rm", "-f", container_id])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}

/// Force-kill a container.
pub fn kill_container(container_id: &str) -> Result<()> {
    Command::new("docker")
        .args(["kill", container_id])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .context("Failed to execute 'docker kill'")?;

    let _ = Command::new("docker")
        .args(["rm", "-f", container_id])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}

/// Check if a container is running.
pub fn is_container_running(container_id: &str) -> bool {
    Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", container_id])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
        .unwrap_or(false)
}
