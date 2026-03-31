use anyhow::{Context, Result};
use std::process::Command;

/// Check if a binary exists and is executable.
pub fn is_binary_available(path: &str) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .map(|m| !m.is_dir() && (m.permissions().mode() & 0o111 != 0))
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        std::fs::metadata(path)
            .map(|m| !m.is_dir())
            .unwrap_or(false)
    }
}

/// Spawn the runtime binary in the background. Returns the child PID.
pub fn spawn_binary(path: &str, dev: bool, http_port: u16, grpc_port: u16) -> Result<u32> {
    let mut cmd = Command::new(path);

    if dev {
        cmd.arg("--dev");
    }

    cmd.env("RUNE_SERVER__HTTP_PORT", http_port.to_string());
    cmd.env("RUNE_SERVER__GRPC_PORT", grpc_port.to_string());

    // Detach from terminal
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    let child = cmd
        .spawn()
        .with_context(|| format!("Failed to start binary: {}", path))?;

    Ok(child.id())
}

/// Check whether a process with the given PID is alive.
pub fn is_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

/// Send SIGTERM to a process.
pub fn send_sigterm(pid: u32) -> Result<()> {
    send_signal(pid, libc::SIGTERM)
}

/// Send SIGKILL to a process.
pub fn send_sigkill(pid: u32) -> Result<()> {
    send_signal(pid, libc::SIGKILL)
}

fn send_signal(pid: u32, sig: i32) -> Result<()> {
    if pid == 0 {
        anyhow::bail!("Invalid PID: 0 would signal the entire process group");
    }
    #[cfg(unix)]
    {
        let ret = unsafe { libc::kill(pid as i32, sig) };
        if ret != 0 {
            anyhow::bail!(
                "kill({}, {}) failed: {}",
                pid,
                sig,
                std::io::Error::last_os_error()
            );
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (pid, sig);
        anyhow::bail!("Signal sending is not supported on this platform")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pid_zero_rejected() {
        assert!(!is_process_alive(0));
        assert!(send_sigterm(0).is_err());
    }

    #[test]
    fn test_own_process_alive() {
        let pid = std::process::id();
        assert!(is_process_alive(pid));
    }
}
