use anyhow::{Context, Result};

const DEFAULT_CONFIG: &str = r#"# Rune CLI Configuration

[runtime]
# Startup mode: "docker" (default) or "binary"
mode = "docker"

# Docker image
image = "ghcr.io/chasey-myagi/rune-server"
tag = "latest"

# Ports
http_port = 50060
grpc_port = 50070

# Uncomment to use local binary instead of Docker:
# binary = "/usr/local/bin/rune-server"

[auth]
# enabled = false

[output]
format = "text"
color = "auto"
"#;

pub async fn init() -> Result<()> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
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

pub async fn show() -> Result<()> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
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

pub async fn path() -> Result<()> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    println!("{}", home.join(".rune").join("config.toml").display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid_toml() {
        let parsed: toml::Value =
            toml::from_str(DEFAULT_CONFIG).expect("DEFAULT_CONFIG must be valid TOML");
        let runtime = parsed.get("runtime").expect("must have [runtime] section");
        assert!(runtime.get("http_port").is_some());
        assert!(runtime.get("grpc_port").is_some());
        assert_eq!(runtime.get("mode").unwrap().as_str(), Some("docker"));
    }
}
