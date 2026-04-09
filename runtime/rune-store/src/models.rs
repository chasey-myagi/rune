use serde::{Deserialize, Serialize};

/// API key record stored in the database.
/// Note: the raw key is never stored — only its SHA-256 hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: i64,
    pub key_prefix: String,
    /// Always `None` when returned from list operations (security).
    #[serde(skip_serializing)]
    pub key_hash: Option<String>,
    pub key_type: KeyType,
    pub label: String,
    pub created_at: String,
    pub revoked_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyType {
    Caster,
    Gate,
    Admin,
}

impl KeyType {
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyType::Caster => "caster",
            KeyType::Gate => "gate",
            KeyType::Admin => "admin",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "caster" => Some(KeyType::Caster),
            "gate" => Some(KeyType::Gate),
            "admin" => Some(KeyType::Admin),
            _ => None,
        }
    }
}

/// Task record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: String,
    pub rune_name: String,
    pub status: TaskStatus,
    pub input: Option<String>,
    pub output: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Completed => "completed",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(TaskStatus::Pending),
            "running" => Some(TaskStatus::Running),
            "completed" => Some(TaskStatus::Completed),
            "failed" => Some(TaskStatus::Failed),
            "cancelled" => Some(TaskStatus::Cancelled),
            _ => None,
        }
    }
}

/// Call log entry for observability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallLog {
    pub id: i64,
    pub request_id: String,
    pub rune_name: String,
    pub mode: String,
    pub caster_id: Option<String>,
    pub latency_ms: i64,
    pub status_code: i32,
    pub input_size: i64,
    pub output_size: i64,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasterCallStats {
    pub caster_id: String,
    pub count: i64,
    pub avg_latency_ms: i64,
    pub success_rate: f64,
    pub p95_latency_ms: f64,
}

/// Rune snapshot — latest known state of a registered rune.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuneSnapshot {
    pub rune_name: String,
    pub version: String,
    pub description: String,
    pub supports_stream: bool,
    pub gate_path: String,
    pub gate_method: String,
    pub last_seen: String,
}
