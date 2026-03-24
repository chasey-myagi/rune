use anyhow::{Context, Result};

const DEFAULT_CONFIG: &str = r#"# Rune Runtime Configuration

[server]
# HTTP listen address
http_host = "0.0.0.0"
http_port = 50060
# gRPC listen address
grpc_host = "0.0.0.0"
grpc_port = 50070

[auth]
# Enable API key authentication
enabled = false

[log]
# Log level: trace, debug, info, warn, error
level = "info"
"#;

/// Generate default configuration file at ~/.rune/config.toml.
pub async fn init() -> Result<()> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    let config_dir = home.join(".rune");
    let config_path = config_dir.join("config.toml");

    if config_path.exists() {
        eprintln!(
            "Configuration file already exists: {}",
            config_path.display()
        );
        eprintln!("To overwrite, delete it first and run `rune config init` again.");
        return Ok(());
    }

    std::fs::create_dir_all(&config_dir)
        .with_context(|| format!("Failed to create directory: {}", config_dir.display()))?;

    std::fs::write(&config_path, DEFAULT_CONFIG)
        .with_context(|| format!("Failed to write config: {}", config_path.display()))?;

    println!("Configuration written to {}", config_path.display());
    Ok(())
}

/// Show current configuration.
pub async fn show() -> Result<()> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    let config_path = home.join(".rune").join("config.toml");

    if !config_path.exists() {
        eprintln!(
            "No configuration file found at {}",
            config_path.display()
        );
        eprintln!("Run `rune config init` to create one.");
        return Ok(());
    }

    let content = std::fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read config: {}", config_path.display()))?;

    println!("# {}", config_path.display());
    println!("{}", content);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// S10 regression: DEFAULT_CONFIG must be valid TOML parseable by AppConfig's schema.
    /// Since rune-cli doesn't depend on rune-core, we verify the TOML structure directly.
    #[test]
    fn test_default_config_is_valid_toml() {
        let parsed: toml::Value =
            toml::from_str(DEFAULT_CONFIG).expect("DEFAULT_CONFIG must be valid TOML");

        // [server] section must have http_port / grpc_port, NOT "addr"
        let server = parsed.get("server").expect("must have [server] section");
        assert!(
            server.get("http_port").is_some(),
            "server must have 'http_port', found keys: {:?}",
            server
        );
        assert!(
            server.get("grpc_port").is_some(),
            "server must have 'grpc_port', found keys: {:?}",
            server
        );
        assert!(
            server.get("addr").is_none(),
            "server must NOT have legacy 'addr' field"
        );
    }
}
