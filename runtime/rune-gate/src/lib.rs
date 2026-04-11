pub mod error;
pub mod file_broker;
pub mod handlers;
pub mod middleware;
pub mod multipart;
pub mod rate_limit;
pub mod router;
pub mod shutdown;
pub mod state;
pub(crate) mod trace_headers;

/// Backward-compatible re-export module.
/// External code can still use `rune_gate::gate::GateState`, etc.
pub mod gate;

// Also re-export at crate root for convenience
pub use file_broker::{FileBroker, StoredFile};
pub use rate_limit::RateLimitState;
pub use router::build_router;
pub use shutdown::ShutdownCoordinator;
pub use state::{AdminState, AuthState, FlowState, GateState, RuneState};
pub use state::{CreateKeyRequest, LogQuery, RunParams};

#[cfg(test)]
mod gate_tests;
