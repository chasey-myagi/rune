//! Configuration types for the Rune SDK.

use std::collections::HashMap;

/// Gate routing configuration for exposing a Rune as an HTTP endpoint.
#[derive(Debug, Clone)]
pub struct GateConfig {
    /// HTTP path, e.g. "/translate".
    pub path: String,
    /// HTTP method. Defaults to "POST".
    pub method: String,
}

impl GateConfig {
    /// Create a new GateConfig with the given path and default method "POST".
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            method: "POST".into(),
        }
    }

    /// Create a new GateConfig with path and method.
    pub fn with_method(path: impl Into<String>, method: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            method: method.into(),
        }
    }
}

/// Configuration for declaring a Rune.
#[derive(Debug, Clone)]
pub struct RuneConfig {
    /// Unique name of this Rune.
    pub name: String,
    /// Semantic version string. Defaults to "0.0.0".
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// JSON Schema for validating input (as serde_json::Value).
    pub input_schema: Option<serde_json::Value>,
    /// JSON Schema for validating output (as serde_json::Value).
    pub output_schema: Option<serde_json::Value>,
    /// Whether this Rune supports streaming responses.
    pub supports_stream: bool,
    /// Gate HTTP endpoint configuration.
    pub gate: Option<GateConfig>,
    /// Priority for load balancing (higher = preferred).
    pub priority: i32,
}

impl Default for RuneConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            version: "0.0.0".into(),
            description: String::new(),
            input_schema: None,
            output_schema: None,
            supports_stream: false,
            gate: None,
            priority: 0,
        }
    }
}

impl RuneConfig {
    /// Create a RuneConfig with the given name and defaults for all other fields.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }
}

/// A file attachment carried with a request or response.
#[derive(Debug, Clone)]
pub struct FileAttachment {
    /// Original filename.
    pub filename: String,
    /// Raw file data.
    pub data: bytes::Bytes,
    /// MIME type string.
    pub mime_type: String,
}

/// Configuration for the Caster instance.
#[derive(Debug, Clone)]
pub struct CasterConfig {
    /// gRPC endpoint of the Rune Runtime. Default: "localhost:50070".
    pub runtime: String,
    /// API key for authentication.
    pub key: Option<String>,
    /// Unique identifier for this Caster instance. Auto-generated if None.
    pub caster_id: Option<String>,
    /// Maximum concurrent request handling. Default: 10.
    pub max_concurrent: u32,
    /// Heartbeat interval in seconds. Default: 10.0.
    pub heartbeat_interval_secs: f64,
    /// Initial reconnect delay in seconds. Default: 1.0.
    pub reconnect_base_delay_secs: f64,
    /// Maximum reconnect delay in seconds. Default: 30.0.
    pub reconnect_max_delay_secs: f64,
    /// Caster labels for metadata.
    pub labels: HashMap<String, String>,
    /// Optional auto-scaling policy for Pilot discovery and scaling metadata.
    pub scale_policy: Option<ScalePolicy>,
    /// Optional load report metadata included with health updates.
    pub load_report: Option<LoadReport>,
}

impl Default for CasterConfig {
    fn default() -> Self {
        Self {
            runtime: "localhost:50070".into(),
            key: None,
            caster_id: None,
            max_concurrent: 10,
            heartbeat_interval_secs: 10.0,
            reconnect_base_delay_secs: 1.0,
            reconnect_max_delay_secs: 30.0,
            labels: HashMap::new(),
            scale_policy: None,
            load_report: None,
        }
    }
}

/// Scaling metadata advertised to the Runtime and local Pilot.
#[derive(Debug, Clone)]
pub struct ScalePolicy {
    pub group: String,
    pub scale_up_threshold: f64,
    pub scale_down_threshold: f64,
    pub sustained_secs: u64,
    pub min_replicas: u32,
    pub max_replicas: u32,
    pub spawn_command: String,
    pub shutdown_signal: String,
}

impl ScalePolicy {
    pub fn new(group: impl Into<String>, spawn_command: impl Into<String>) -> Self {
        Self {
            group: group.into(),
            spawn_command: spawn_command.into(),
            ..Default::default()
        }
    }
}

impl Default for ScalePolicy {
    fn default() -> Self {
        Self {
            group: String::new(),
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.2,
            sustained_secs: 30,
            min_replicas: 1,
            max_replicas: 1,
            spawn_command: String::new(),
            shutdown_signal: "SIGTERM".into(),
        }
    }
}

/// Additional load telemetry sent with health reports.
#[derive(Debug, Clone, Default)]
pub struct LoadReport {
    /// Explicit pressure override. When `None`, computed automatically
    /// from `active_requests / max_concurrent`.
    pub pressure: Option<f64>,
    pub metrics: HashMap<String, f64>,
}
