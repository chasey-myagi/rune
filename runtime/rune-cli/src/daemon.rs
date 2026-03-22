use anyhow::Result;
use std::path::PathBuf;

/// Default PID file location: ~/.rune/rune.pid
pub fn pid_file_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("cannot determine home directory"))?;
    Ok(home.join(".rune").join("rune.pid"))
}

/// Write a PID to the PID file.
pub fn write_pid(pid: u32) -> Result<()> {
    let path = pid_file_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, pid.to_string())?;
    Ok(())
}

/// Read the PID from the PID file, if it exists.
pub fn read_pid() -> Result<Option<u32>> {
    let path = pid_file_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)?;
    let pid: u32 = content.trim().parse()?;
    Ok(Some(pid))
}

/// Remove the PID file.
pub fn remove_pid_file() -> Result<()> {
    let path = pid_file_path()?;
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(())
}

/// Send a signal to a process, with PID validation.
///
/// Rejects PID 0 (which would signal the entire process group) and ensures
/// the PID is positive before calling libc::kill.
pub fn send_signal(pid: u32, sig: i32) -> Result<()> {
    if pid == 0 {
        anyhow::bail!("invalid PID: 0 would signal the entire process group");
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
        anyhow::bail!("send_signal is not supported on this platform")
    }
}

/// Check whether a process with the given PID is still alive.
pub fn is_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    // On Unix, signal 0 checks existence without actually sending a signal.
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
