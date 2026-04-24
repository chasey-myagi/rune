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
    /// RFC3339 timestamp of the last successful authentication. `None` if never used.
    pub last_used_at: Option<String>,
    /// Source IP of the last successful authentication. `None` if never used.
    pub last_used_ip: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Unknown(String),
}

impl serde::Serialize for TaskStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for TaskStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self::from_db(&s))
    }
}

impl TaskStatus {
    pub fn as_str(&self) -> &str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Completed => "completed",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
            TaskStatus::Unknown(s) => s.as_str(),
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

    /// Parse a status string from the database. Unknown values are preserved
    /// rather than silently downgraded to Pending.
    pub fn from_db(s: &str) -> Self {
        Self::parse(s).unwrap_or_else(|| TaskStatus::Unknown(s.to_string()))
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
