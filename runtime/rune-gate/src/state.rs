use std::sync::Arc;
use std::time::Instant;

use rune_core::auth::KeyVerifier;
use rune_core::relay::Relay;
use rune_core::resolver::Resolver;
use rune_flow::engine::FlowEngine;
use rune_store::RuneStore;

use crate::file_broker::FileBroker;
use crate::rate_limit::RateLimitState;
use crate::shutdown::ShutdownCoordinator;

/// Gate shared state
#[derive(Clone)]
pub struct GateState {
    pub relay: Arc<Relay>,
    pub resolver: Arc<dyn Resolver>,
    pub store: Arc<RuneStore>,
    pub key_verifier: Arc<dyn KeyVerifier>,
    pub session_mgr: Arc<rune_core::session::SessionManager>,
    pub auth_enabled: bool,
    pub exempt_routes: Vec<String>,
    pub cors_origins: Vec<String>,
    pub dev_mode: bool,
    pub started_at: Instant,
    pub file_broker: Arc<FileBroker>,
    pub max_upload_size_mb: u64,
    pub flow_engine: Arc<tokio::sync::RwLock<FlowEngine>>,
    pub rate_limiter: Option<RateLimitState>,
    pub shutdown: ShutdownCoordinator,
}

#[derive(serde::Deserialize, Default)]
pub struct RunParams {
    pub stream: Option<bool>,
    #[serde(rename = "async")]
    pub async_mode: Option<bool>,
}

#[derive(serde::Deserialize, Default)]
pub struct LogQuery {
    pub rune: Option<String>,
    pub limit: Option<i64>,
}

#[derive(serde::Deserialize)]
pub struct CreateKeyRequest {
    pub key_type: String,
    pub label: String,
}

pub fn unique_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    format!("r-{:x}-{:x}", ts, seq)
}
