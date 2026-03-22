use rune_core::config::{
    AppConfig, AuthConfig, GateServerConfig, LogConfig, RateLimitConfig, ResolverConfig,
    ServerConfig, SessionConfig, StoreConfig,
};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

#[test]
fn test_default_config() {
    let config = AppConfig::default();

    // Server defaults
    assert_eq!(config.server.grpc_host, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert_eq!(config.server.grpc_port, 50070);
    assert_eq!(config.server.http_host, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert_eq!(config.server.http_port, 50060);
    assert!(!config.server.dev_mode);
    assert_eq!(config.server.drain_timeout_secs, 15);

    // Auth defaults
    assert!(config.auth.enabled);
    assert!(config.auth.exempt_routes.is_empty());

    // Store defaults
    assert_eq!(config.store.db_path, "rune.db");
    assert_eq!(config.store.log_retention_days, 30);

    // Session defaults
    assert_eq!(config.session.heartbeat_interval_secs, 10);
    assert_eq!(config.session.heartbeat_timeout_secs, 35);
    assert_eq!(config.session.max_request_timeout_secs, 30);

    // Gate defaults
    assert!(config.gate.cors_origins.is_empty());
    assert_eq!(config.gate.max_upload_size_mb, 10);

    // Resolver defaults
    assert_eq!(config.resolver.strategy, "round_robin");

    // Rate limit defaults
    assert_eq!(config.rate_limit.requests_per_minute, 600);

    // Log defaults
    assert_eq!(config.log.level, "info");
    assert!(config.log.file.is_none());

    // Accessor methods preserve backward-compatible values
    assert_eq!(
        config.grpc_addr(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 50070)
    );
    assert_eq!(
        config.http_addr(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 50060)
    );
    assert_eq!(config.heartbeat_interval(), Duration::from_secs(10));
    assert_eq!(config.heartbeat_timeout(), Duration::from_secs(35));
    assert_eq!(config.default_timeout(), Duration::from_secs(30));
}

#[test]
fn test_load_from_toml() {
    let toml_content = r#"
[server]
grpc_port = 9090
http_port = 8080

[auth]
enabled = false

[session]
heartbeat_interval_secs = 5
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.server.grpc_port, 9090);
    assert_eq!(config.server.http_port, 8080);
    assert!(!config.auth.enabled);
    assert_eq!(config.session.heartbeat_interval_secs, 5);
    // Unset fields use defaults
    assert_eq!(config.store.db_path, "rune.db");
    assert_eq!(config.session.heartbeat_timeout_secs, 35);
    assert_eq!(config.server.grpc_host, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
}

#[test]
fn test_partial_toml() {
    let toml_content = r#"
[log]
level = "debug"
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.log.level, "debug");
    // Everything else is default
    assert_eq!(config.server.grpc_port, 50070);
    assert_eq!(config.server.http_port, 50060);
    assert!(config.auth.enabled);
    assert_eq!(config.session.heartbeat_interval_secs, 10);
    assert_eq!(config.rate_limit.requests_per_minute, 600);
}

#[test]
fn test_invalid_toml() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, "this is [[[not valid toml").unwrap();

    let result = AppConfig::from_file(path.to_str().unwrap());
    assert!(result.is_err());
}

#[test]
fn test_dev_mode_overrides() {
    let mut config = AppConfig::default();
    // Verify pre-conditions
    assert!(config.auth.enabled);
    assert_eq!(config.server.grpc_host, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert_eq!(config.server.http_host, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert!(!config.server.dev_mode);

    config.apply_dev_mode();

    assert!(config.server.dev_mode);
    assert_eq!(config.server.grpc_host, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(config.server.http_host, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert!(!config.auth.enabled);
}

#[test]
fn test_env_var_override() {
    // Use unique env var names to avoid parallel test interference
    // These are the actual RUNE_ prefixed vars the system uses
    std::env::set_var("RUNE_SERVER__GRPC_PORT", "12345");
    std::env::set_var("RUNE_LOG__LEVEL", "trace");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert_eq!(config.server.grpc_port, 12345);
    assert_eq!(config.log.level, "trace");

    // Clean up
    std::env::remove_var("RUNE_SERVER__GRPC_PORT");
    std::env::remove_var("RUNE_LOG__LEVEL");
}

#[test]
fn test_env_var_override_bool_and_ip() {
    std::env::set_var("RUNE_AUTH__ENABLED", "false");
    std::env::set_var("RUNE_SERVER__GRPC_HOST", "127.0.0.1");
    std::env::set_var("RUNE_STORE__LOG_RETENTION_DAYS", "7");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert!(!config.auth.enabled);
    assert_eq!(config.server.grpc_host, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(config.store.log_retention_days, 7);

    std::env::remove_var("RUNE_AUTH__ENABLED");
    std::env::remove_var("RUNE_SERVER__GRPC_HOST");
    std::env::remove_var("RUNE_STORE__LOG_RETENTION_DAYS");
}

#[test]
fn test_env_var_invalid_value_ignored() {
    std::env::set_var("RUNE_SERVER__GRPC_PORT", "not_a_number");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    // Invalid value should be silently ignored, keeping default
    assert_eq!(config.server.grpc_port, 50070);

    std::env::remove_var("RUNE_SERVER__GRPC_PORT");
}

#[test]
fn test_missing_file_uses_defaults() {
    let config = AppConfig::from_file("/nonexistent/path/to/rune.toml").unwrap();
    let default_config = AppConfig::default();

    assert_eq!(config.server.grpc_port, default_config.server.grpc_port);
    assert_eq!(config.server.http_port, default_config.server.http_port);
    assert_eq!(config.auth.enabled, default_config.auth.enabled);
    assert_eq!(config.log.level, default_config.log.level);
    assert_eq!(
        config.session.heartbeat_interval_secs,
        default_config.session.heartbeat_interval_secs
    );
}

#[test]
fn test_grpc_addr_method() {
    let mut config = AppConfig::default();
    config.server.grpc_host = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    config.server.grpc_port = 9999;

    assert_eq!(
        config.grpc_addr(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9999)
    );
}

#[test]
fn test_http_addr_method() {
    let mut config = AppConfig::default();
    config.server.http_host = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
    config.server.http_port = 3000;

    assert_eq!(
        config.http_addr(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 3000)
    );
}

#[test]
fn test_config_generate_default_toml() {
    let config = AppConfig::default();
    let toml_str = config.to_toml().unwrap();

    // Should be valid TOML that can be parsed back
    let parsed: AppConfig = toml::from_str(&toml_str).unwrap();
    assert_eq!(parsed.server.grpc_port, 50070);
    assert_eq!(parsed.server.http_port, 50060);
    assert_eq!(parsed.log.level, "info");
    assert!(parsed.auth.enabled);
    assert_eq!(parsed.session.heartbeat_interval_secs, 10);
    assert_eq!(parsed.store.db_path, "rune.db");
    assert_eq!(parsed.resolver.strategy, "round_robin");
}

#[test]
fn test_config_roundtrip_with_custom_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");

    let mut config = AppConfig::default();
    config.server.grpc_port = 7777;
    config.auth.enabled = false;
    config.auth.exempt_routes = vec!["/health".to_string(), "/metrics".to_string()];
    config.log.level = "debug".to_string();
    config.log.file = Some("/var/log/rune.log".to_string());
    config.gate.cors_origins = vec!["http://localhost:3000".to_string()];

    let toml_str = config.to_toml().unwrap();
    std::fs::write(&path, &toml_str).unwrap();

    let loaded = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(loaded.server.grpc_port, 7777);
    assert!(!loaded.auth.enabled);
    assert_eq!(loaded.auth.exempt_routes.len(), 2);
    assert_eq!(loaded.auth.exempt_routes[0], "/health");
    assert_eq!(loaded.log.level, "debug");
    assert_eq!(loaded.log.file.as_deref(), Some("/var/log/rune.log"));
    assert_eq!(loaded.gate.cors_origins.len(), 1);
}

#[test]
fn test_empty_toml_file_uses_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, "").unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    let default_config = AppConfig::default();

    assert_eq!(config.server.grpc_port, default_config.server.grpc_port);
    assert_eq!(config.auth.enabled, default_config.auth.enabled);
}

#[test]
fn test_unknown_keys_ignored() {
    let toml_content = r#"
[server]
grpc_port = 9090
unknown_field = "should be ignored"

[unknown_section]
key = "value"
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    // Should not error on unknown keys
    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.server.grpc_port, 9090);
}
