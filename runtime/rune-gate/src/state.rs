use std::sync::Arc;
use std::time::{Duration, Instant};

use rune_core::auth::KeyVerifier;
use rune_core::relay::Relay;
use rune_core::resolver::Resolver;
use rune_core::scaling::ScaleEvaluator;
use rune_flow::engine::FlowEngine;
use rune_store::RuneStore;

use crate::file_broker::FileBroker;
use crate::rate_limit::RateLimitState;
use crate::shutdown::ShutdownCoordinator;

/// Default request timeout (used when request_timeout is not set).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Authentication-related state.
#[derive(Clone)]
pub struct AuthState {
    pub key_verifier: Arc<dyn KeyVerifier>,
    pub auth_enabled: bool,
    pub exempt_routes: Arc<Vec<String>>,
}

/// Rune execution-related state.
#[derive(Clone)]
pub struct RuneState {
    pub relay: Arc<Relay>,
    pub resolver: Arc<dyn Resolver>,
    pub session_mgr: Arc<rune_core::session::SessionManager>,
    pub file_broker: Arc<FileBroker>,
    pub max_upload_size_mb: u64,
    /// Timeout for individual rune/flow step invocations (from config).
    pub request_timeout: Duration,
}

/// Flow engine state.
#[derive(Clone)]
pub struct FlowState {
    pub flow_engine: Arc<tokio::sync::RwLock<FlowEngine>>,
}

/// Administration / operational state.
#[derive(Clone)]
pub struct AdminState {
    pub store: Arc<RuneStore>,
    pub started_at: Instant,
    pub dev_mode: bool,
    pub scaling: Option<Arc<ScaleEvaluator>>,
}

/// Gate shared state — composed of semantic sub-states.
#[derive(Clone)]
pub struct GateState {
    pub auth: AuthState,
    pub rune: RuneState,
    pub flow: FlowState,
    pub admin: AdminState,
    pub cors_origins: Arc<Vec<String>>,
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
    rune_core::time_utils::unique_request_id()
}
