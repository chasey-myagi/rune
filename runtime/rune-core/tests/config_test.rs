use rune_core::config::AppConfig;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;
use std::time::Duration;

/// Serialize env-var tests to avoid races from set_var/remove_var across threads.
static ENV_LOCK: Mutex<()> = Mutex::new(());

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
    assert_eq!(config.auth.exempt_routes, vec!["/health".to_string()]);

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
    let _guard = ENV_LOCK.lock().unwrap();
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
    let _guard = ENV_LOCK.lock().unwrap();
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
    let _guard = ENV_LOCK.lock().unwrap();
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

// ---- Scenario 1: env override takes priority over TOML file ----

#[test]
fn test_env_overrides_toml_value() {
    let _guard = ENV_LOCK.lock().unwrap();

    // TOML sets grpc_port=9090, env overrides to 11111
    let toml_content = r#"
[server]
grpc_port = 9090

[auth]
enabled = true
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    std::env::set_var("RUNE_SERVER__GRPC_PORT", "11111");
    std::env::set_var("RUNE_AUTH__ENABLED", "false");

    let mut config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    // Before env override: TOML values
    assert_eq!(config.server.grpc_port, 9090);
    assert!(config.auth.enabled);

    config.apply_env_overrides();
    // After env override: env wins
    assert_eq!(config.server.grpc_port, 11111);
    assert!(!config.auth.enabled);

    std::env::remove_var("RUNE_SERVER__GRPC_PORT");
    std::env::remove_var("RUNE_AUTH__ENABLED");
}

// ---- Scenario 2: port boundary values ----

#[test]
fn test_port_boundary_zero() {
    let toml_content = r#"
[server]
grpc_port = 0
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.server.grpc_port, 0);
    // SocketAddr should also accept port 0 (OS assigns ephemeral port)
    assert_eq!(config.grpc_addr().port(), 0);
}

#[test]
fn test_port_boundary_max_valid() {
    let toml_content = r#"
[server]
grpc_port = 65535
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.server.grpc_port, 65535);
    assert_eq!(config.grpc_addr().port(), 65535);
}

#[test]
fn test_port_boundary_overflow_rejected() {
    // 65536 exceeds u16 range — TOML deserialization should fail
    let toml_content = r#"
[server]
grpc_port = 65536
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let result = AppConfig::from_file(path.to_str().unwrap());
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("grpc_port"),
        "error should mention the offending field, got: {}",
        err_msg
    );
}

// ---- Scenario 3: invalid IP env var is silently ignored ----

#[test]
fn test_invalid_ip_env_var_ignored() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("RUNE_SERVER__GRPC_HOST", "not_an_ip");

    let mut config = AppConfig::default();
    let original_host = config.server.grpc_host;
    config.apply_env_overrides();

    // Invalid IP should be silently ignored, keeping the default
    assert_eq!(config.server.grpc_host, original_host);
    assert_eq!(config.server.grpc_host, IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    std::env::remove_var("RUNE_SERVER__GRPC_HOST");
}

// ---- Scenario 4: dev_mode overrides TOML auth.enabled=true ----

#[test]
fn test_dev_mode_overrides_toml_auth_enabled() {
    let toml_content = r#"
[auth]
enabled = true

[server]
grpc_port = 9999
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let mut config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert!(config.auth.enabled, "TOML explicitly set auth.enabled=true");
    assert_eq!(config.server.grpc_port, 9999);

    config.apply_dev_mode();

    // apply_dev_mode() must disable auth even when TOML set it to true
    assert!(!config.auth.enabled);
    assert!(config.server.dev_mode);
    assert_eq!(config.server.grpc_host, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(config.server.http_host, IpAddr::V4(Ipv4Addr::LOCALHOST));
    // Non-dev_mode fields should be preserved
    assert_eq!(config.server.grpc_port, 9999);
}

// ---- Scenario 5: TOML type mismatch errors ----

#[test]
fn test_toml_type_mismatch_port_as_string() {
    let toml_content = r#"
[server]
grpc_port = "abc"
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let result = AppConfig::from_file(path.to_str().unwrap());
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("grpc_port"),
        "error should reference the mistyped field, got: {}",
        err_msg
    );
}

#[test]
fn test_toml_type_mismatch_bool_as_integer() {
    let toml_content = r#"
[auth]
enabled = 123
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let result = AppConfig::from_file(path.to_str().unwrap());
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("enabled"),
        "error should reference the mistyped field, got: {}",
        err_msg
    );
}
