use serde::{Deserialize, Serialize};

/// Telemetry configuration for OpenTelemetry tracing and Prometheus metrics.
///
/// When all fields are `None`, the system falls back to plain `tracing_subscriber::fmt` logging.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// OTLP gRPC endpoint (e.g. "http://localhost:4317").
    /// When set, an OpenTelemetry tracing layer is added.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,

    /// TCP port to expose Prometheus `/metrics` endpoint.
    /// When set, a Prometheus metrics exporter is started on `0.0.0.0:<port>`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prometheus_port: Option<u16>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            prometheus_port: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_config_default_is_disabled() {
        let config = TelemetryConfig::default();
        assert!(config.otlp_endpoint.is_none());
        assert!(config.prometheus_port.is_none());
    }

    #[test]
    fn telemetry_config_deserialize_empty_toml() {
        let config: TelemetryConfig = toml::from_str("").unwrap();
        assert!(config.otlp_endpoint.is_none());
        assert!(config.prometheus_port.is_none());
    }

    #[test]
    fn telemetry_config_deserialize_with_otlp() {
        let toml_str = r#"otlp_endpoint = "http://localhost:4317""#;
        let config: TelemetryConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.otlp_endpoint.as_deref(),
            Some("http://localhost:4317")
        );
        assert!(config.prometheus_port.is_none());
    }

    #[test]
    fn telemetry_config_deserialize_with_prometheus() {
        let toml_str = r#"prometheus_port = 9090"#;
        let config: TelemetryConfig = toml::from_str(toml_str).unwrap();
        assert!(config.otlp_endpoint.is_none());
        assert_eq!(config.prometheus_port, Some(9090));
    }

    #[test]
    fn telemetry_config_deserialize_full() {
        let toml_str = r#"
otlp_endpoint = "http://otel-collector:4317"
prometheus_port = 9464
"#;
        let config: TelemetryConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.otlp_endpoint.as_deref(),
            Some("http://otel-collector:4317")
        );
        assert_eq!(config.prometheus_port, Some(9464));
    }

    #[test]
    fn telemetry_config_roundtrip_serialize() {
        let config = TelemetryConfig {
            otlp_endpoint: Some("http://localhost:4317".to_string()),
            prometheus_port: Some(9090),
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let deserialized: TelemetryConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.otlp_endpoint, deserialized.otlp_endpoint);
        assert_eq!(config.prometheus_port, deserialized.prometheus_port);
    }

    #[test]
    fn app_config_with_telemetry_section() {
        use crate::config::AppConfig;

        let toml_str = r#"
[telemetry]
otlp_endpoint = "http://localhost:4317"
prometheus_port = 9090
"#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.telemetry.otlp_endpoint.as_deref(),
            Some("http://localhost:4317")
        );
        assert_eq!(config.telemetry.prometheus_port, Some(9090));
    }

    #[test]
    fn app_config_without_telemetry_section_uses_defaults() {
        use crate::config::AppConfig;

        let config: AppConfig = toml::from_str("").unwrap();
        assert!(config.telemetry.otlp_endpoint.is_none());
        assert!(config.telemetry.prometheus_port.is_none());
    }
}
