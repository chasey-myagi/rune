use rune_core::config::AppConfig;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
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
    assert_eq!(config.store.reader_pool_size, 4);
    assert_eq!(config.store.key_cache_ttl_secs, 60);
    assert_eq!(config.store.key_cache_negative_ttl_secs, 30);

    // Session defaults
    assert_eq!(config.session.heartbeat_interval_secs, 10);
    assert_eq!(config.session.heartbeat_timeout_secs, 35);
    assert_eq!(config.session.max_request_timeout_secs, 30);

    // Gate defaults
    assert!(config.gate.cors_origins.is_empty());
    assert_eq!(config.gate.max_upload_size_mb, 10);

    // Resolver defaults
    assert_eq!(config.resolver.strategy, "round_robin");

    // Retry defaults
    assert!(config.retry.enabled);
    assert_eq!(config.retry.max_retries, 3);
    assert_eq!(config.retry.base_delay_ms, 100);
    assert_eq!(config.retry.max_delay_ms, 5_000);
    assert_eq!(config.retry.backoff_multiplier, 2.0);
    assert_eq!(
        config.retry.retryable_errors,
        vec!["unavailable", "internal"]
    );
    assert!(config.retry.circuit_breaker.enabled);
    assert_eq!(config.retry.circuit_breaker.failure_threshold, 5);
    assert_eq!(config.retry.circuit_breaker.success_threshold, 2);
    assert_eq!(config.retry.circuit_breaker.reset_timeout_ms, 30_000);
    assert_eq!(config.retry.circuit_breaker.half_open_max_permits, 1);

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
    std::env::set_var("RUNE_STORE__READER_POOL_SIZE", "8");
    std::env::set_var("RUNE_STORE__KEY_CACHE_TTL_SECS", "120");
    std::env::set_var("RUNE_STORE__KEY_CACHE_NEGATIVE_TTL_SECS", "15");
    std::env::set_var("RUNE_RETRY__MAX_RETRIES", "5");
    std::env::set_var("RUNE_RETRY__CIRCUIT_BREAKER__FAILURE_THRESHOLD", "9");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert!(!config.auth.enabled);
    assert_eq!(config.server.grpc_host, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(config.store.log_retention_days, 7);
    assert_eq!(config.store.reader_pool_size, 8);
    assert_eq!(config.store.key_cache_ttl_secs, 120);
    assert_eq!(config.store.key_cache_negative_ttl_secs, 15);
    assert_eq!(config.retry.max_retries, 5);
    assert_eq!(config.retry.circuit_breaker.failure_threshold, 9);

    std::env::remove_var("RUNE_AUTH__ENABLED");
    std::env::remove_var("RUNE_SERVER__GRPC_HOST");
    std::env::remove_var("RUNE_STORE__LOG_RETENTION_DAYS");
    std::env::remove_var("RUNE_STORE__READER_POOL_SIZE");
    std::env::remove_var("RUNE_STORE__KEY_CACHE_TTL_SECS");
    std::env::remove_var("RUNE_STORE__KEY_CACHE_NEGATIVE_TTL_SECS");
    std::env::remove_var("RUNE_RETRY__MAX_RETRIES");
    std::env::remove_var("RUNE_RETRY__CIRCUIT_BREAKER__FAILURE_THRESHOLD");
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

// ---- IPv6 address configuration ----

#[test]
fn test_ipv6_address_in_toml() {
    let toml_content = r#"
[server]
grpc_host = "::1"
grpc_port = 50070
http_host = "::1"
http_port = 50060
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.server.grpc_host, IpAddr::V6(Ipv6Addr::LOCALHOST));
    assert_eq!(config.server.http_host, IpAddr::V6(Ipv6Addr::LOCALHOST));

    // Verify grpc_addr() produces a valid IPv6 SocketAddr
    let addr = config.grpc_addr();
    assert_eq!(
        addr,
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 50070)
    );
    assert!(addr.is_ipv6());

    let http = config.http_addr();
    assert_eq!(
        http,
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 50060)
    );
    assert!(http.is_ipv6());
}

#[test]
fn test_ipv6_unspecified_in_toml() {
    let toml_content = r#"
[server]
grpc_host = "::"
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.server.grpc_host, IpAddr::V6(Ipv6Addr::UNSPECIFIED));
}

// ---- IPv6 via environment variable ----

#[test]
fn test_ipv6_env_var_override() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("RUNE_SERVER__GRPC_HOST", "::1");
    std::env::set_var("RUNE_SERVER__HTTP_HOST", "::1");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert_eq!(config.server.grpc_host, IpAddr::V6(Ipv6Addr::LOCALHOST));
    assert_eq!(config.server.http_host, IpAddr::V6(Ipv6Addr::LOCALHOST));

    std::env::remove_var("RUNE_SERVER__GRPC_HOST");
    std::env::remove_var("RUNE_SERVER__HTTP_HOST");
}

// ---- GRPC_HOST / HTTP_HOST env override ----

#[test]
fn test_env_var_override_grpc_host() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("RUNE_SERVER__GRPC_HOST", "10.0.0.5");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert_eq!(
        config.server.grpc_host,
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))
    );

    std::env::remove_var("RUNE_SERVER__GRPC_HOST");
}

#[test]
fn test_env_var_override_http_host() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("RUNE_SERVER__HTTP_HOST", "192.168.1.100");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert_eq!(
        config.server.http_host,
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100))
    );

    std::env::remove_var("RUNE_SERVER__HTTP_HOST");
}

// ---- Multiple env vars applied simultaneously ----

#[test]
fn test_multiple_env_vars_all_take_effect() {
    let _guard = ENV_LOCK.lock().unwrap();

    std::env::set_var("RUNE_SERVER__GRPC_HOST", "10.0.0.1");
    std::env::set_var("RUNE_SERVER__GRPC_PORT", "9999");
    std::env::set_var("RUNE_SERVER__HTTP_HOST", "10.0.0.2");
    std::env::set_var("RUNE_SERVER__HTTP_PORT", "8888");
    std::env::set_var("RUNE_SERVER__DEV_MODE", "true");
    std::env::set_var("RUNE_AUTH__ENABLED", "false");
    std::env::set_var("RUNE_STORE__DB_PATH", "/tmp/custom.db");
    std::env::set_var("RUNE_LOG__LEVEL", "warn");
    std::env::set_var("RUNE_RESOLVER__STRATEGY", "random");
    std::env::set_var("RUNE_RATE_LIMIT__REQUESTS_PER_MINUTE", "1200");
    std::env::set_var("RUNE_SESSION__HEARTBEAT_INTERVAL_SECS", "20");
    std::env::set_var("RUNE_SESSION__HEARTBEAT_TIMEOUT_SECS", "60");
    std::env::set_var("RUNE_SESSION__MAX_REQUEST_TIMEOUT_SECS", "45");
    std::env::set_var("RUNE_SERVER__DRAIN_TIMEOUT_SECS", "30");
    std::env::set_var("RUNE_GATE__MAX_UPLOAD_SIZE_MB", "50");
    std::env::set_var("RUNE_STORE__LOG_RETENTION_DAYS", "90");
    std::env::set_var("RUNE_STORE__READER_POOL_SIZE", "16");
    std::env::set_var("RUNE_STORE__KEY_CACHE_TTL_SECS", "300");
    std::env::set_var("RUNE_STORE__KEY_CACHE_NEGATIVE_TTL_SECS", "45");
    std::env::set_var("RUNE_RETRY__ENABLED", "false");
    std::env::set_var("RUNE_RETRY__MAX_RETRIES", "6");
    std::env::set_var("RUNE_RETRY__BASE_DELAY_MS", "250");
    std::env::set_var("RUNE_RETRY__MAX_DELAY_MS", "8000");
    std::env::set_var("RUNE_RETRY__BACKOFF_MULTIPLIER", "3.5");
    std::env::set_var("RUNE_RETRY__CIRCUIT_BREAKER__ENABLED", "false");
    std::env::set_var("RUNE_RETRY__CIRCUIT_BREAKER__FAILURE_THRESHOLD", "7");
    std::env::set_var("RUNE_RETRY__CIRCUIT_BREAKER__SUCCESS_THRESHOLD", "4");
    std::env::set_var("RUNE_RETRY__CIRCUIT_BREAKER__RESET_TIMEOUT_MS", "120000");
    std::env::set_var("RUNE_RETRY__CIRCUIT_BREAKER__HALF_OPEN_MAX_PERMITS", "3");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert_eq!(
        config.server.grpc_host,
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))
    );
    assert_eq!(config.server.grpc_port, 9999);
    assert_eq!(
        config.server.http_host,
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))
    );
    assert_eq!(config.server.http_port, 8888);
    assert!(config.server.dev_mode);
    assert!(!config.auth.enabled);
    assert_eq!(config.store.db_path, "/tmp/custom.db");
    assert_eq!(config.log.level, "warn");
    assert_eq!(config.resolver.strategy, "random");
    assert_eq!(config.rate_limit.requests_per_minute, 1200);
    assert_eq!(config.session.heartbeat_interval_secs, 20);
    assert_eq!(config.session.heartbeat_timeout_secs, 60);
    assert_eq!(config.session.max_request_timeout_secs, 45);
    assert_eq!(config.server.drain_timeout_secs, 30);
    assert_eq!(config.gate.max_upload_size_mb, 50);
    assert_eq!(config.store.log_retention_days, 90);
    assert_eq!(config.store.reader_pool_size, 16);
    assert_eq!(config.store.key_cache_ttl_secs, 300);
    assert_eq!(config.store.key_cache_negative_ttl_secs, 45);
    assert!(!config.retry.enabled);
    assert_eq!(config.retry.max_retries, 6);
    assert_eq!(config.retry.base_delay_ms, 250);
    assert_eq!(config.retry.max_delay_ms, 8000);
    assert_eq!(config.retry.backoff_multiplier, 3.5);
    assert!(!config.retry.circuit_breaker.enabled);
    assert_eq!(config.retry.circuit_breaker.failure_threshold, 7);
    assert_eq!(config.retry.circuit_breaker.success_threshold, 4);
    assert_eq!(config.retry.circuit_breaker.reset_timeout_ms, 120000);
    assert_eq!(config.retry.circuit_breaker.half_open_max_permits, 3);

    // Clean up all env vars
    std::env::remove_var("RUNE_SERVER__GRPC_HOST");
    std::env::remove_var("RUNE_SERVER__GRPC_PORT");
    std::env::remove_var("RUNE_SERVER__HTTP_HOST");
    std::env::remove_var("RUNE_SERVER__HTTP_PORT");
    std::env::remove_var("RUNE_SERVER__DEV_MODE");
    std::env::remove_var("RUNE_AUTH__ENABLED");
    std::env::remove_var("RUNE_STORE__DB_PATH");
    std::env::remove_var("RUNE_LOG__LEVEL");
    std::env::remove_var("RUNE_RESOLVER__STRATEGY");
    std::env::remove_var("RUNE_RATE_LIMIT__REQUESTS_PER_MINUTE");
    std::env::remove_var("RUNE_SESSION__HEARTBEAT_INTERVAL_SECS");
    std::env::remove_var("RUNE_SESSION__HEARTBEAT_TIMEOUT_SECS");
    std::env::remove_var("RUNE_SESSION__MAX_REQUEST_TIMEOUT_SECS");
    std::env::remove_var("RUNE_SERVER__DRAIN_TIMEOUT_SECS");
    std::env::remove_var("RUNE_GATE__MAX_UPLOAD_SIZE_MB");
    std::env::remove_var("RUNE_STORE__LOG_RETENTION_DAYS");
    std::env::remove_var("RUNE_STORE__READER_POOL_SIZE");
    std::env::remove_var("RUNE_STORE__KEY_CACHE_TTL_SECS");
    std::env::remove_var("RUNE_STORE__KEY_CACHE_NEGATIVE_TTL_SECS");
    std::env::remove_var("RUNE_RETRY__ENABLED");
    std::env::remove_var("RUNE_RETRY__MAX_RETRIES");
    std::env::remove_var("RUNE_RETRY__BASE_DELAY_MS");
    std::env::remove_var("RUNE_RETRY__MAX_DELAY_MS");
    std::env::remove_var("RUNE_RETRY__BACKOFF_MULTIPLIER");
    std::env::remove_var("RUNE_RETRY__CIRCUIT_BREAKER__ENABLED");
    std::env::remove_var("RUNE_RETRY__CIRCUIT_BREAKER__FAILURE_THRESHOLD");
    std::env::remove_var("RUNE_RETRY__CIRCUIT_BREAKER__SUCCESS_THRESHOLD");
    std::env::remove_var("RUNE_RETRY__CIRCUIT_BREAKER__RESET_TIMEOUT_MS");
    std::env::remove_var("RUNE_RETRY__CIRCUIT_BREAKER__HALF_OPEN_MAX_PERMITS");
}

// ---- Long db_path value ----

#[test]
fn test_long_db_path_value() {
    // 500-character path ("/data/" = 6 chars + 494 = 500)
    let long_path = format!("/data/{}", "a".repeat(494));
    assert_eq!(long_path.len(), 500);

    let toml_content = format!(
        r#"
[store]
db_path = "{}"
"#,
        long_path
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.store.db_path, long_path);
    assert_eq!(config.store.db_path.len(), 500);
}

// ---- Special characters in db_path ----

#[test]
fn test_special_characters_in_db_path() {
    let toml_content = r#"
[store]
db_path = "/data/my project/数据库/rune.db"
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.store.db_path, "/data/my project/数据库/rune.db");
    assert!(config.store.db_path.contains(' '));
    assert!(config.store.db_path.contains("数据库"));
}

// ---- exempt_routes with multiple routes ----

#[test]
fn test_multiple_exempt_routes() {
    let toml_content = r#"
[auth]
enabled = true
exempt_routes = ["/health", "/metrics", "/ready", "/api/public"]
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.auth.exempt_routes.len(), 4);
    assert_eq!(config.auth.exempt_routes[0], "/health");
    assert_eq!(config.auth.exempt_routes[1], "/metrics");
    assert_eq!(config.auth.exempt_routes[2], "/ready");
    assert_eq!(config.auth.exempt_routes[3], "/api/public");
}

#[test]
fn test_empty_exempt_routes() {
    let toml_content = r#"
[auth]
exempt_routes = []
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert!(config.auth.exempt_routes.is_empty());
}

// ---- Duration accessor methods with custom values ----

#[test]
fn test_heartbeat_interval_custom() {
    let mut config = AppConfig::default();
    config.session.heartbeat_interval_secs = 3;
    assert_eq!(config.heartbeat_interval(), Duration::from_secs(3));
}

#[test]
fn test_heartbeat_timeout_custom() {
    let mut config = AppConfig::default();
    config.session.heartbeat_timeout_secs = 120;
    assert_eq!(config.heartbeat_timeout(), Duration::from_secs(120));
}

#[test]
fn test_default_timeout_custom() {
    let mut config = AppConfig::default();
    config.session.max_request_timeout_secs = 60;
    assert_eq!(config.default_timeout(), Duration::from_secs(60));
}

#[test]
fn test_duration_methods_zero() {
    // Edge case: zero-second durations
    let mut config = AppConfig::default();
    config.session.heartbeat_interval_secs = 0;
    config.session.heartbeat_timeout_secs = 0;
    config.session.max_request_timeout_secs = 0;

    assert_eq!(config.heartbeat_interval(), Duration::from_secs(0));
    assert_eq!(config.heartbeat_timeout(), Duration::from_secs(0));
    assert_eq!(config.default_timeout(), Duration::from_secs(0));
}

// ---- drain_timeout configuration ----

#[test]
fn test_drain_timeout_from_toml() {
    let toml_content = r#"
[server]
drain_timeout_secs = 60
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.server.drain_timeout_secs, 60);
}

#[test]
fn test_drain_timeout_env_override() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("RUNE_SERVER__DRAIN_TIMEOUT_SECS", "45");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert_eq!(config.server.drain_timeout_secs, 45);

    std::env::remove_var("RUNE_SERVER__DRAIN_TIMEOUT_SECS");
}

// ---- rate_limit and resolver strategy via TOML ----

#[test]
fn test_rate_limit_from_toml() {
    let toml_content = r#"
[rate_limit]
requests_per_minute = 100
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.rate_limit.requests_per_minute, 100);
}

#[test]
fn test_resolver_strategy_from_toml() {
    let toml_content = r#"
[resolver]
strategy = "random"
"#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");
    std::fs::write(&path, toml_content).unwrap();

    let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
    assert_eq!(config.resolver.strategy, "random");
}

#[test]
fn test_resolver_strategy_env_override() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("RUNE_RESOLVER__STRATEGY", "least_connections");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert_eq!(config.resolver.strategy, "least_connections");

    std::env::remove_var("RUNE_RESOLVER__STRATEGY");
}

#[test]
fn test_rate_limit_env_override() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("RUNE_RATE_LIMIT__REQUESTS_PER_MINUTE", "999");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert_eq!(config.rate_limit.requests_per_minute, 999);

    std::env::remove_var("RUNE_RATE_LIMIT__REQUESTS_PER_MINUTE");
}

// ---- LOG__FILE env override ----

#[test]
fn test_log_file_env_override() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("RUNE_LOG__FILE", "/var/log/rune.log");

    let mut config = AppConfig::default();
    config.apply_env_overrides();

    assert_eq!(config.log.file.as_deref(), Some("/var/log/rune.log"));

    std::env::remove_var("RUNE_LOG__FILE");
}

#[test]
fn test_log_file_env_empty_clears_value() {
    let _guard = ENV_LOCK.lock().unwrap();
    // First set a log file via TOML simulation
    let mut config = AppConfig::default();
    config.log.file = Some("/tmp/rune.log".to_string());

    // Then set env to empty string to clear it
    std::env::set_var("RUNE_LOG__FILE", "");
    config.apply_env_overrides();

    assert!(
        config.log.file.is_none(),
        "empty RUNE_LOG__FILE should clear the value"
    );

    std::env::remove_var("RUNE_LOG__FILE");
}

// ---- Full TOML roundtrip with all sections ----

#[test]
fn test_full_config_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rune.toml");

    let mut config = AppConfig::default();
    config.server.grpc_host = IpAddr::V6(Ipv6Addr::LOCALHOST);
    config.server.grpc_port = 12345;
    config.server.http_host = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
    config.server.http_port = 54321;
    config.server.dev_mode = true;
    config.server.drain_timeout_secs = 30;
    config.auth.enabled = false;
    config.auth.exempt_routes = vec!["/health".into(), "/metrics".into(), "/ready".into()];
    config.store.db_path = "/custom/path/rune.db".to_string();
    config.store.log_retention_days = 7;
    config.store.reader_pool_size = 12;
    config.store.key_cache_ttl_secs = 180;
    config.store.key_cache_negative_ttl_secs = 25;
    config.retry.enabled = false;
    config.retry.max_retries = 8;
    config.retry.base_delay_ms = 150;
    config.retry.max_delay_ms = 12_000;
    config.retry.backoff_multiplier = 1.5;
    config.retry.retryable_errors = vec!["timeout".into(), "internal".into()];
    config.retry.circuit_breaker.enabled = false;
    config.retry.circuit_breaker.failure_threshold = 11;
    config.retry.circuit_breaker.success_threshold = 3;
    config.retry.circuit_breaker.reset_timeout_ms = 45_000;
    config.retry.circuit_breaker.half_open_max_permits = 2;
    config.session.heartbeat_interval_secs = 5;
    config.session.heartbeat_timeout_secs = 20;
    config.session.max_request_timeout_secs = 60;
    config.gate.cors_origins = vec!["http://localhost:3000".into(), "https://example.com".into()];
    config.gate.max_upload_size_mb = 50;
    config.resolver.strategy = "random".to_string();
    config.rate_limit.requests_per_minute = 1200;
    config.log.level = "trace".to_string();
    config.log.file = Some("/var/log/rune.log".to_string());

    let toml_str = config.to_toml().unwrap();
    std::fs::write(&path, &toml_str).unwrap();

    let loaded = AppConfig::from_file(path.to_str().unwrap()).unwrap();

    // Verify every single field survives the roundtrip
    assert_eq!(loaded.server.grpc_host, IpAddr::V6(Ipv6Addr::LOCALHOST));
    assert_eq!(loaded.server.grpc_port, 12345);
    assert_eq!(
        loaded.server.http_host,
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))
    );
    assert_eq!(loaded.server.http_port, 54321);
    assert!(loaded.server.dev_mode);
    assert_eq!(loaded.server.drain_timeout_secs, 30);
    assert!(!loaded.auth.enabled);
    assert_eq!(
        loaded.auth.exempt_routes,
        vec!["/health", "/metrics", "/ready"]
    );
    assert_eq!(loaded.store.db_path, "/custom/path/rune.db");
    assert_eq!(loaded.store.log_retention_days, 7);
    assert_eq!(loaded.store.reader_pool_size, 12);
    assert_eq!(loaded.store.key_cache_ttl_secs, 180);
    assert_eq!(loaded.store.key_cache_negative_ttl_secs, 25);
    assert!(!loaded.retry.enabled);
    assert_eq!(loaded.retry.max_retries, 8);
    assert_eq!(loaded.retry.base_delay_ms, 150);
    assert_eq!(loaded.retry.max_delay_ms, 12_000);
    assert_eq!(loaded.retry.backoff_multiplier, 1.5);
    assert_eq!(loaded.retry.retryable_errors, vec!["timeout", "internal"]);
    assert!(!loaded.retry.circuit_breaker.enabled);
    assert_eq!(loaded.retry.circuit_breaker.failure_threshold, 11);
    assert_eq!(loaded.retry.circuit_breaker.success_threshold, 3);
    assert_eq!(loaded.retry.circuit_breaker.reset_timeout_ms, 45_000);
    assert_eq!(loaded.retry.circuit_breaker.half_open_max_permits, 2);
    assert_eq!(loaded.session.heartbeat_interval_secs, 5);
    assert_eq!(loaded.session.heartbeat_timeout_secs, 20);
    assert_eq!(loaded.session.max_request_timeout_secs, 60);
    assert_eq!(
        loaded.gate.cors_origins,
        vec!["http://localhost:3000", "https://example.com"]
    );
    assert_eq!(loaded.gate.max_upload_size_mb, 50);
    assert_eq!(loaded.resolver.strategy, "random");
    assert_eq!(loaded.rate_limit.requests_per_minute, 1200);
    assert_eq!(loaded.log.level, "trace");
    assert_eq!(loaded.log.file.as_deref(), Some("/var/log/rune.log"));
}
