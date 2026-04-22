use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::telemetry::TelemetryConfig;

// ---------------------------------------------------------------------------
// Sub-config structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub grpc_host: IpAddr,
    pub grpc_port: u16,
    pub http_host: IpAddr,
    pub http_port: u16,
    pub dev_mode: bool,
    pub drain_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_host: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            grpc_port: 50070,
            http_host: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            http_port: 50060,
            dev_mode: false,
            drain_timeout_secs: 15,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub enabled: bool,
    pub exempt_routes: Vec<String>,
    /// HMAC secret for API key hashing. When set, keys are hashed with
    /// HMAC-SHA256 instead of plain SHA-256. Existing SHA-256 keys remain
    /// verifiable via automatic fallback.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hmac_secret: Option<String>,
    /// Bootstrap-only admin key imported from env on startup.
    /// Deliberately excluded from TOML serialization/deserialization.
    #[serde(skip)]
    pub initial_admin_key: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            exempt_routes: vec!["/health".to_string()],
            hmac_secret: None,
            initial_admin_key: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StoreConfig {
    pub db_path: String,
    pub log_retention_days: u32,
    pub task_retention_days: u32,
    pub reader_pool_size: usize,
    pub key_cache_ttl_secs: u64,
    pub key_cache_negative_ttl_secs: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            db_path: "rune.db".to_string(),
            log_retention_days: 30,
            task_retention_days: 7,
            reader_pool_size: 4,
            key_cache_ttl_secs: 60,
            key_cache_negative_ttl_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionConfig {
    pub heartbeat_interval_secs: u64,
    pub heartbeat_timeout_secs: u64,
    pub max_request_timeout_secs: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_secs: 10,
            heartbeat_timeout_secs: 35,
            max_request_timeout_secs: 30,
        }
    }
}

/// Server-level gate configuration (CORS, upload limits, etc.).
///
/// Not to be confused with `rune::GateConfig` which describes a single
/// Rune's HTTP route (path + method).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GateServerConfig {
    pub cors_origins: Vec<String>,
    pub max_upload_size_mb: u64,
}

impl Default for GateServerConfig {
    fn default() -> Self {
        Self {
            cors_origins: Vec::new(),
            max_upload_size_mb: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ResolverConfig {
    pub strategy: String,
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self {
            strategy: "round_robin".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CircuitBreakerConfig {
    pub enabled: bool,
    pub failure_threshold: u32,
    pub success_threshold: u32,
    pub reset_timeout_ms: u64,
    pub half_open_max_permits: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: 5,
            success_threshold: 2,
            reset_timeout_ms: 30_000,
            half_open_max_permits: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    pub enabled: bool,
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
    pub retryable_errors: Vec<String>,
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 5_000,
            backoff_multiplier: 2.0,
            // Timeout is excluded: the runtime does not cancel the remote caster
            // on timeout, so retrying would start a second concurrent execution.
            retryable_errors: vec!["unavailable".to_string(), "internal".to_string()],
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    /// Maximum requests allowed per `window_secs` window.
    #[serde(alias = "requests_per_minute")]
    pub max_requests: u32,
    /// Sliding window size in seconds. Default 60.
    pub window_secs: u64,
    pub per_rune: HashMap<String, PerRuneRateLimit>,
    pub default_caster_max_concurrent: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerRuneRateLimit {
    #[serde(alias = "requests_per_minute")]
    pub max_requests: u32,
}

impl Default for PerRuneRateLimit {
    fn default() -> Self {
        Self { max_requests: 60 }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 600,
            window_secs: 60,
            per_rune: HashMap::new(),
            default_caster_max_concurrent: 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScalingConfig {
    pub enabled: bool,
    pub eval_interval_secs: u64,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            eval_interval_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    pub level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_path: Option<String>,
}

// ---------------------------------------------------------------------------
// AppConfig — top-level configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub store: StoreConfig,
    pub session: SessionConfig,
    pub gate: GateServerConfig,
    pub resolver: ResolverConfig,
    pub retry: RetryConfig,
    pub rate_limit: RateLimitConfig,
    pub scaling: ScalingConfig,
    pub log: LogConfig,
    pub telemetry: TelemetryConfig,
    pub tls: TlsConfig,
}

impl AppConfig {
    /// Load configuration from a TOML file (accepts any path type).
    ///
    /// If the file does not exist, returns default configuration.
    /// If the file exists but contains invalid TOML, returns an error.
    pub fn from_path(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path.as_ref()) {
            Ok(content) => {
                let config: AppConfig = toml::from_str(&content)?;
                Ok(config)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Load configuration from a TOML file path string.
    ///
    /// Convenience wrapper around [`from_path`](Self::from_path).
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Self::from_path(path)
    }

    /// Load configuration with default file search.
    ///
    /// Priority: explicit path > ./rune.toml > ~/.config/rune/rune.toml > defaults.
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        if let Some(p) = path {
            return Self::from_file(p);
        }

        // Try ./rune.toml
        if std::path::Path::new("rune.toml").exists() {
            return Self::from_file("rune.toml");
        }

        // Try ~/.config/rune/rune.toml
        if let Some(config_dir) = dirs::config_dir() {
            let user_config = config_dir.join("rune").join("rune.toml");
            if user_config.exists() {
                return Self::from_path(&user_config);
            }
        }

        Ok(Self::default())
    }

    /// Serialize the current configuration to a TOML string.
    pub fn to_toml(&self) -> anyhow::Result<String> {
        Ok(toml::to_string_pretty(self)?)
    }

    /// Apply development mode overrides: bind to localhost, disable auth.
    pub fn apply_dev_mode(&mut self) {
        self.server.dev_mode = true;
        self.server.grpc_host = IpAddr::V4(Ipv4Addr::LOCALHOST);
        self.server.http_host = IpAddr::V4(Ipv4Addr::LOCALHOST);
        self.auth.enabled = false;
    }

    /// Validate configuration values. Call after [`apply_env_overrides`](Self::apply_env_overrides)
    /// to catch invalid final config (e.g. zero `window_secs` set via env).
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.rate_limit.window_secs > 0,
            "rate_limit.window_secs must be > 0"
        );

        if self.retry.enabled {
            anyhow::ensure!(
                self.retry.backoff_multiplier > 0.0,
                "retry.backoff_multiplier must be > 0"
            );
            anyhow::ensure!(
                self.retry.max_delay_ms >= self.retry.base_delay_ms,
                "retry.max_delay_ms must be >= base_delay_ms"
            );
        }

        if self.retry.circuit_breaker.enabled {
            anyhow::ensure!(
                self.retry.circuit_breaker.failure_threshold > 0,
                "circuit_breaker.failure_threshold must be > 0"
            );
            anyhow::ensure!(
                self.retry.circuit_breaker.reset_timeout_ms > 0,
                "circuit_breaker.reset_timeout_ms must be > 0"
            );
        }

        Ok(())
    }

    /// Apply environment variable overrides.
    ///
    /// Format: `RUNE_{SECTION}__{FIELD}` (double underscore separates section
    /// from field). Invalid values are silently ignored.
    pub fn apply_env_overrides(&mut self) {
        macro_rules! env_override {
            ($var:expr, $field:expr, $ty:ty) => {
                if let Ok(val) = std::env::var($var) {
                    match val.parse::<$ty>() {
                        Ok(parsed) => $field = parsed,
                        Err(e) => {
                            tracing::warn!(env = $var, value = %val, error = %e, "failed to parse env override, using default");
                        }
                    }
                }
            };
        }

        macro_rules! env_override_string {
            ($var:expr, $field:expr) => {
                if let Ok(v) = std::env::var($var) {
                    $field = v;
                }
            };
        }

        // Server
        env_override!("RUNE_SERVER__GRPC_HOST", self.server.grpc_host, IpAddr);
        env_override!("RUNE_SERVER__GRPC_PORT", self.server.grpc_port, u16);
        env_override!("RUNE_SERVER__HTTP_HOST", self.server.http_host, IpAddr);
        env_override!("RUNE_SERVER__HTTP_PORT", self.server.http_port, u16);
        env_override!("RUNE_SERVER__DEV_MODE", self.server.dev_mode, bool);
        env_override!(
            "RUNE_SERVER__DRAIN_TIMEOUT_SECS",
            self.server.drain_timeout_secs,
            u64
        );

        // Auth
        env_override!("RUNE_AUTH__ENABLED", self.auth.enabled, bool);
        if let Ok(v) = std::env::var("RUNE_AUTH__HMAC_SECRET") {
            self.auth.hmac_secret = if v.is_empty() { None } else { Some(v) };
        }
        if let Ok(v) = std::env::var("RUNE_AUTH__INITIAL_ADMIN_KEY") {
            self.auth.initial_admin_key = if v.is_empty() { None } else { Some(v) };
        }

        // Store
        env_override_string!("RUNE_STORE__DB_PATH", self.store.db_path);
        env_override!(
            "RUNE_STORE__LOG_RETENTION_DAYS",
            self.store.log_retention_days,
            u32
        );
        env_override!(
            "RUNE_STORE__TASK_RETENTION_DAYS",
            self.store.task_retention_days,
            u32
        );
        env_override!(
            "RUNE_STORE__READER_POOL_SIZE",
            self.store.reader_pool_size,
            usize
        );
        env_override!(
            "RUNE_STORE__KEY_CACHE_TTL_SECS",
            self.store.key_cache_ttl_secs,
            u64
        );
        env_override!(
            "RUNE_STORE__KEY_CACHE_NEGATIVE_TTL_SECS",
            self.store.key_cache_negative_ttl_secs,
            u64
        );

        // Session
        env_override!(
            "RUNE_SESSION__HEARTBEAT_INTERVAL_SECS",
            self.session.heartbeat_interval_secs,
            u64
        );
        env_override!(
            "RUNE_SESSION__HEARTBEAT_TIMEOUT_SECS",
            self.session.heartbeat_timeout_secs,
            u64
        );
        env_override!(
            "RUNE_SESSION__MAX_REQUEST_TIMEOUT_SECS",
            self.session.max_request_timeout_secs,
            u64
        );

        // Gate
        env_override!(
            "RUNE_GATE__MAX_UPLOAD_SIZE_MB",
            self.gate.max_upload_size_mb,
            u64
        );

        // Resolver
        env_override_string!("RUNE_RESOLVER__STRATEGY", self.resolver.strategy);

        // Retry
        env_override!("RUNE_RETRY__ENABLED", self.retry.enabled, bool);
        env_override!("RUNE_RETRY__MAX_RETRIES", self.retry.max_retries, u32);
        env_override!("RUNE_RETRY__BASE_DELAY_MS", self.retry.base_delay_ms, u64);
        env_override!("RUNE_RETRY__MAX_DELAY_MS", self.retry.max_delay_ms, u64);
        env_override!(
            "RUNE_RETRY__BACKOFF_MULTIPLIER",
            self.retry.backoff_multiplier,
            f64
        );
        env_override!(
            "RUNE_RETRY__CIRCUIT_BREAKER__ENABLED",
            self.retry.circuit_breaker.enabled,
            bool
        );
        env_override!(
            "RUNE_RETRY__CIRCUIT_BREAKER__FAILURE_THRESHOLD",
            self.retry.circuit_breaker.failure_threshold,
            u32
        );
        env_override!(
            "RUNE_RETRY__CIRCUIT_BREAKER__SUCCESS_THRESHOLD",
            self.retry.circuit_breaker.success_threshold,
            u32
        );
        env_override!(
            "RUNE_RETRY__CIRCUIT_BREAKER__RESET_TIMEOUT_MS",
            self.retry.circuit_breaker.reset_timeout_ms,
            u64
        );
        env_override!(
            "RUNE_RETRY__CIRCUIT_BREAKER__HALF_OPEN_MAX_PERMITS",
            self.retry.circuit_breaker.half_open_max_permits,
            u32
        );

        // Rate limit
        env_override!(
            "RUNE_RATE_LIMIT__REQUESTS_PER_MINUTE",
            self.rate_limit.max_requests,
            u32
        );
        env_override!(
            "RUNE_RATE_LIMIT__WINDOW_SECS",
            self.rate_limit.window_secs,
            u64
        );
        env_override!(
            "RUNE_RATE_LIMIT__DEFAULT_CASTER_MAX_CONCURRENT",
            self.rate_limit.default_caster_max_concurrent,
            u32
        );

        // Scaling
        env_override!("RUNE_SCALING__ENABLED", self.scaling.enabled, bool);
        env_override!(
            "RUNE_SCALING__EVAL_INTERVAL_SECS",
            self.scaling.eval_interval_secs,
            u64
        );

        // Log
        env_override_string!("RUNE_LOG__LEVEL", self.log.level);
        if let Ok(v) = std::env::var("RUNE_LOG__FILE") {
            self.log.file = if v.is_empty() { None } else { Some(v) };
        }

        // Telemetry
        if let Ok(v) = std::env::var("RUNE_TELEMETRY__OTLP_ENDPOINT") {
            self.telemetry.otlp_endpoint = if v.is_empty() { None } else { Some(v) };
        }
        if let Ok(v) = std::env::var("RUNE_TELEMETRY__PROMETHEUS_PORT") {
            if v.is_empty() {
                self.telemetry.prometheus_port = None;
            } else if let Ok(port) = v.parse::<u16>() {
                self.telemetry.prometheus_port = Some(port);
            }
        }

        // TLS
        if let Ok(v) = std::env::var("RUNE_TLS__CERT_PATH") {
            self.tls.cert_path = if v.is_empty() { None } else { Some(v) };
        }
        if let Ok(v) = std::env::var("RUNE_TLS__KEY_PATH") {
            self.tls.key_path = if v.is_empty() { None } else { Some(v) };
        }
    }

    // -----------------------------------------------------------------------
    // Backward-compatible accessor methods
    // -----------------------------------------------------------------------

    pub fn grpc_addr(&self) -> SocketAddr {
        SocketAddr::new(self.server.grpc_host, self.server.grpc_port)
    }

    pub fn http_addr(&self) -> SocketAddr {
        SocketAddr::new(self.server.http_host, self.server.http_port)
    }

    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.session.heartbeat_interval_secs)
    }

    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_secs(self.session.heartbeat_timeout_secs)
    }

    pub fn default_timeout(&self) -> Duration {
        Duration::from_secs(self.session.max_request_timeout_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_config_default_is_disabled() {
        let config = TlsConfig::default();
        assert!(config.cert_path.is_none());
        assert!(config.key_path.is_none());
    }

    #[test]
    fn tls_config_from_toml() {
        let toml_str = r#"
        [tls]
        cert_path = "/etc/rune/cert.pem"
        key_path = "/etc/rune/key.pem"
        "#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.tls.cert_path.as_deref(), Some("/etc/rune/cert.pem"));
        assert_eq!(config.tls.key_path.as_deref(), Some("/etc/rune/key.pem"));
    }

    #[test]
    fn tls_config_partial_is_err_or_none() {
        // 只配置 cert 没配置 key，应该能解析但在启动时会报错
        let toml_str = r#"
        [tls]
        cert_path = "/etc/rune/cert.pem"
        "#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert!(config.tls.cert_path.is_some());
        assert!(config.tls.key_path.is_none());
    }

    #[test]
    fn test_fix_from_path_with_pathbuf() {
        // Regression test for M-5: from_path must accept PathBuf directly
        // without going through to_str() which can fail on non-UTF-8 paths.
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("test.toml");
        std::fs::write(
            &config_path,
            r#"
[server]
http_port = 19999
"#,
        )
        .unwrap();

        let config = AppConfig::from_path(&config_path).unwrap();
        assert_eq!(config.server.http_port, 19999);
        // dir auto-cleaned on drop
    }

    #[test]
    fn test_fix_from_path_missing_file_returns_default() {
        let config =
            AppConfig::from_path("/tmp/rune_test_m5_nonexistent/does_not_exist.toml").unwrap();
        assert_eq!(config.server.http_port, 50060); // default
    }

    #[test]
    fn initial_admin_key_is_env_only() {
        std::env::set_var(
            "RUNE_AUTH__INITIAL_ADMIN_KEY",
            "rk_envonly1234567890abcdef1234567890",
        );
        let mut config = AppConfig::default();
        config.apply_env_overrides();
        assert_eq!(
            config.auth.initial_admin_key.as_deref(),
            Some("rk_envonly1234567890abcdef1234567890")
        );
        std::env::remove_var("RUNE_AUTH__INITIAL_ADMIN_KEY");
    }

    #[test]
    fn initial_admin_key_is_ignored_in_toml() {
        let toml_str = r#"
        [auth]
        initial_admin_key = "rk_should_be_ignored"
        "#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert!(config.auth.initial_admin_key.is_none());
    }

    #[test]
    fn scaling_config_from_toml() {
        let toml_str = r#"
        [scaling]
        enabled = true
        eval_interval_secs = 12
        "#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert!(config.scaling.enabled);
        assert_eq!(config.scaling.eval_interval_secs, 12);
    }

    #[test]
    fn scaling_config_env_overrides_apply() {
        std::env::set_var("RUNE_SCALING__ENABLED", "true");
        std::env::set_var("RUNE_SCALING__EVAL_INTERVAL_SECS", "9");

        let mut config = AppConfig::default();
        config.apply_env_overrides();

        assert!(config.scaling.enabled);
        assert_eq!(config.scaling.eval_interval_secs, 9);

        std::env::remove_var("RUNE_SCALING__ENABLED");
        std::env::remove_var("RUNE_SCALING__EVAL_INTERVAL_SECS");
    }
}
