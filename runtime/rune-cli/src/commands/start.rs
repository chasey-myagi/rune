use crate::runtime::{self, binary, docker, state};
use anyhow::Result;

const DEFAULT_IMAGE: &str = "ghcr.io/chasey-myagi/rune-server";
const DEFAULT_TAG: &str = "latest";
const DEFAULT_HTTP_PORT: u16 = 50060;
const DEFAULT_GRPC_PORT: u16 = 50070;

struct CliConfig {
    http_port: Option<u16>,
    grpc_port: Option<u16>,
    image: Option<String>,
    tag: Option<String>,
}

/// Read port / image / tag defaults from ~/.rune/config.toml.
/// Missing or malformed config is silently ignored (CLI tool should not panic on config absence).
fn load_cli_config() -> CliConfig {
    let none = CliConfig {
        http_port: None,
        grpc_port: None,
        image: None,
        tag: None,
    };
    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return none,
    };
    let path = home.join(".rune").join("config.toml");
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return none,
    };
    let doc: toml::Value = match toml::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("warning: failed to parse ~/.rune/config.toml: {e}");
            return none;
        }
    };
    let rt = match doc.get("runtime") {
        Some(v) => v,
        None => return none,
    };
    // Use try_from to avoid silent truncation of out-of-range port values.
    let http_port = rt
        .get("http_port")
        .and_then(|v| v.as_integer())
        .and_then(|v| u16::try_from(v).ok());
    let grpc_port = rt
        .get("grpc_port")
        .and_then(|v| v.as_integer())
        .and_then(|v| u16::try_from(v).ok());
    let image = rt.get("image").and_then(|v| v.as_str()).map(str::to_owned);
    let tag = rt.get("tag").and_then(|v| v.as_str()).map(str::to_owned);
    CliConfig {
        http_port,
        grpc_port,
        image,
        tag,
    }
}

pub async fn run(
    dev: bool,
    binary_path: Option<String>,
    image: Option<String>,
    tag: Option<String>,
    http_port: Option<u16>,
    grpc_port: Option<u16>,
    foreground: bool,
) -> Result<()> {
    let cfg = load_cli_config();
    let http_port = http_port.or(cfg.http_port).unwrap_or(DEFAULT_HTTP_PORT);
    let grpc_port = grpc_port.or(cfg.grpc_port).unwrap_or(DEFAULT_GRPC_PORT);
    let image = image.or(cfg.image);
    let tag = tag.or(cfg.tag);

    // Check if already running
    if let Ok(Some(existing)) = state::read_state() {
        if runtime::is_runtime_alive(&existing) {
            let id_info = match existing.mode {
                state::RuntimeMode::Docker => {
                    format!(
                        "container {}",
                        existing.container_id.as_deref().unwrap_or("?")
                    )
                }
                state::RuntimeMode::Binary => {
                    format!("PID {}", existing.pid.unwrap_or(0))
                }
            };
            eprintln!("Runtime is already running ({}).", id_info);
            eprintln!("Run `rune stop` first to restart.");
            return Ok(());
        }
        // Stale state -- clean up
        eprintln!("Cleaning up stale runtime state...");
        state::remove_state()?;
    }

    if let Some(ref bin) = binary_path {
        start_binary(bin, dev, http_port, grpc_port, foreground).await
    } else {
        start_docker(
            image.as_deref().unwrap_or(DEFAULT_IMAGE),
            tag.as_deref().unwrap_or(DEFAULT_TAG),
            dev,
            http_port,
            grpc_port,
        )
        .await
    }
}

async fn start_docker(
    image: &str,
    tag: &str,
    dev: bool,
    http_port: u16,
    grpc_port: u16,
) -> Result<()> {
    if !docker::is_docker_available() {
        anyhow::bail!(
            "Docker is not installed or not running.\n\
             Install Docker: https://docs.docker.com/get-docker/\n\
             Or use a local binary: rune start --binary /path/to/rune-server"
        );
    }

    eprintln!("Starting Runtime (docker)...");

    let container_id = docker::start_container(image, tag, http_port, grpc_port, dev)?;

    let rt_state = state::RuntimeState::new_docker(&container_id, http_port, grpc_port, dev);
    state::write_state(&rt_state)?;

    let base_url = format!("http://127.0.0.1:{}", http_port);
    eprintln!("Waiting for Runtime to be ready...");

    match runtime::wait_for_healthy(&base_url, 30, "docker logs rune-runtime").await {
        Ok(()) => {
            eprintln!(
                "Runtime started (docker: {}).",
                &container_id[..12.min(container_id.len())]
            );
            eprintln!("  HTTP: http://127.0.0.1:{}", http_port);
            eprintln!("  gRPC: 127.0.0.1:{}", grpc_port);
            if dev {
                eprintln!("  Mode: development (no auth)");
            }
            Ok(())
        }
        Err(e) => {
            // Cleanup on failure
            eprintln!("Startup failed, cleaning up...");
            let _ = docker::stop_container(&container_id, 5);
            state::remove_state()?;
            Err(e)
        }
    }
}

async fn start_binary(
    path: &str,
    dev: bool,
    http_port: u16,
    grpc_port: u16,
    foreground: bool,
) -> Result<()> {
    if !binary::is_binary_available(path) {
        anyhow::bail!("Binary not found: {}", path);
    }

    if foreground {
        eprintln!("Starting Runtime (binary, foreground)...");
        eprintln!("Press Ctrl+C to stop.");

        // In foreground mode, exec directly -- don't write state
        let status = tokio::process::Command::new(path)
            .args(if dev { vec!["--dev"] } else { vec![] })
            .env("RUNE_SERVER__HTTP_PORT", http_port.to_string())
            .env("RUNE_SERVER__GRPC_PORT", grpc_port.to_string())
            .status()
            .await?;

        if !status.success() {
            anyhow::bail!("Runtime exited with status: {}", status);
        }
        return Ok(());
    }

    eprintln!("Starting Runtime (binary)...");

    let pid = binary::spawn_binary(path, dev, http_port, grpc_port)?;

    let rt_state = state::RuntimeState::new_binary(pid, http_port, grpc_port, dev);
    state::write_state(&rt_state)?;

    let base_url = format!("http://127.0.0.1:{}", http_port);
    eprintln!("Waiting for Runtime to be ready...");

    let log_hint = format!("check process output (PID {})", pid);
    match runtime::wait_for_healthy(&base_url, 30, &log_hint).await {
        Ok(()) => {
            eprintln!("Runtime started (PID {}).", pid);
            eprintln!("  HTTP: http://127.0.0.1:{}", http_port);
            eprintln!("  gRPC: 127.0.0.1:{}", grpc_port);
            Ok(())
        }
        Err(e) => {
            eprintln!("Startup failed, cleaning up...");
            let _ = binary::send_sigterm(pid);
            state::remove_state()?;
            Err(e)
        }
    }
}
