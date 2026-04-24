use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Maximum concurrent background audit-log writes per gate instance.
/// SQLite writes are serialized; without backpressure, high QPS can
/// spawn unbounded tokio tasks and exhaust the blocking thread pool.
pub const DEFAULT_AUDIT_MAX_CONCURRENT: usize = 64;

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

/// Pre-parsed CIDR for fast IP matching at request time.
/// Parsed once at startup so the hot path never re-parses strings.
#[derive(Clone, Debug)]
pub struct TrustedCidr {
    pub network: IpAddr,
    pub prefix_len: u8,
}

impl TrustedCidr {
    /// Parse a CIDR string (e.g. "10.0.0.0/8") at init time.
    pub fn parse(cidr: &str) -> Result<Self, String> {
        let Some((network_str, prefix_str)) = cidr.split_once('/') else {
            return Err(format!("malformed CIDR '{}': missing '/'", cidr));
        };
        let prefix_len = prefix_str
            .trim()
            .parse::<u8>()
            .map_err(|_| format!("malformed CIDR '{}': prefix is not a number", cidr))?;
        let network = network_str
            .trim()
            .parse::<IpAddr>()
            .map_err(|_| format!("malformed CIDR '{}': network address is invalid", cidr))?;

        match network {
            IpAddr::V4(_) if prefix_len > 32 => {
                return Err(format!("malformed CIDR '{}': IPv4 prefix > 32", cidr));
            }
            IpAddr::V6(_) if prefix_len > 128 => {
                return Err(format!("malformed CIDR '{}': IPv6 prefix > 128", cidr));
            }
            _ => {}
        }

        Ok(Self {
            network,
            prefix_len,
        })
    }

    /// Check if `addr` is inside this CIDR range.
    pub fn contains(&self, addr: &IpAddr) -> bool {
        match (addr, self.network) {
            (IpAddr::V4(a), IpAddr::V4(n)) => {
                let a_u32 = u32::from_be_bytes(a.octets());
                let n_u32 = u32::from_be_bytes(n.octets());
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    !0u32 << (32 - self.prefix_len)
                };
                (a_u32 & mask) == (n_u32 & mask)
            }
            (IpAddr::V6(a), IpAddr::V6(n)) => {
                let a_u128 = u128::from_be_bytes(a.octets());
                let n_u128 = u128::from_be_bytes(n.octets());
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    !0u128 << (128 - self.prefix_len)
                };
                (a_u128 & mask) == (n_u128 & mask)
            }
            _ => false,
        }
    }
}

/// Authentication-related state.
#[derive(Clone)]
pub struct AuthState {
    pub key_verifier: Arc<dyn KeyVerifier>,
    pub auth_enabled: bool,
    pub exempt_routes: Arc<Vec<String>>,
    /// CIDR ranges of trusted reverse proxies. If None, the direct peer IP
    /// is used for audit logging instead of X-Forwarded-For.
    pub trust_proxy: Option<Arc<Vec<TrustedCidr>>>,
    /// Semaphore that limits concurrent background audit-log writes.
    /// Prevents unbounded task spawning under high QPS.
    pub audit_semaphore: Arc<tokio::sync::Semaphore>,
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
