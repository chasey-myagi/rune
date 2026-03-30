use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeState {
    pub mode: RuntimeMode,
    /// Docker container ID (if mode == Docker)
    pub container_id: Option<String>,
    /// Process PID (if mode == Binary)
    pub pid: Option<u32>,
    pub http_port: u16,
    pub grpc_port: u16,
    pub started_at: DateTime<Utc>,
    pub version: String,
    pub dev_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeMode {
    Docker,
    Binary,
}

impl RuntimeState {
    pub fn new_docker(container_id: &str, http_port: u16, grpc_port: u16, dev: bool) -> Self {
        Self {
            mode: RuntimeMode::Docker,
            container_id: Some(container_id.to_string()),
            pid: None,
            http_port,
            grpc_port,
            started_at: Utc::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            dev_mode: dev,
        }
    }

    pub fn new_binary(pid: u32, http_port: u16, grpc_port: u16, dev: bool) -> Self {
        Self {
            mode: RuntimeMode::Binary,
            container_id: None,
            pid: Some(pid),
            http_port,
            grpc_port,
            started_at: Utc::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            dev_mode: dev,
        }
    }
}

/// Path to the state file: ~/.rune/state.json
pub fn state_file_path() -> Result<PathBuf> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    Ok(home.join(".rune").join("state.json"))
}

/// Read current runtime state, if it exists.
pub fn read_state() -> Result<Option<RuntimeState>> {
    let path = state_file_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read state file: {}", path.display()))?;
    let state: RuntimeState = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse state file: {}", path.display()))?;
    Ok(Some(state))
}

/// Write runtime state to disk.
pub fn write_state(state: &RuntimeState) -> Result<()> {
    let path = state_file_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(state)?;
    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write state file: {}", path.display()))?;
    Ok(())
}

/// Remove the state file.
pub fn remove_state() -> Result<()> {
    let path = state_file_path()?;
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docker_state_roundtrip() {
        let state = RuntimeState::new_docker("abc123", 50060, 50070, true);
        let json = serde_json::to_string_pretty(&state).unwrap();
        let parsed: RuntimeState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.mode, RuntimeMode::Docker);
        assert_eq!(parsed.container_id.as_deref(), Some("abc123"));
        assert!(parsed.pid.is_none());
        assert_eq!(parsed.http_port, 50060);
        assert_eq!(parsed.grpc_port, 50070);
        assert!(parsed.dev_mode);
    }

    #[test]
    fn test_binary_state_roundtrip() {
        let state = RuntimeState::new_binary(12345, 50060, 50070, false);
        let json = serde_json::to_string_pretty(&state).unwrap();
        let parsed: RuntimeState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.mode, RuntimeMode::Binary);
        assert!(parsed.container_id.is_none());
        assert_eq!(parsed.pid, Some(12345));
        assert!(!parsed.dev_mode);
    }

    #[test]
    fn test_state_file_path_contains_rune_dir() {
        let path = state_file_path().unwrap();
        assert!(path.ends_with(".rune/state.json"));
    }
}
