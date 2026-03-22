use anyhow::{Context, Result};

const DEFAULT_CONFIG: &str = r#"# Rune Runtime Configuration

[server]
# Listen address
addr = "0.0.0.0:50060"

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
