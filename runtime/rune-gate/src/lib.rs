pub mod shutdown;
pub mod rate_limit;
pub mod file_broker;
pub mod error;
pub mod multipart;
pub mod state;
pub mod middleware;
pub mod handlers;
pub mod router;

/// Backward-compatible re-export module.
/// External code can still use `rune_gate::gate::GateState`, etc.
pub mod gate;

// Also re-export at crate root for convenience
pub use state::{GateState, AuthState, RuneState, FlowState, AdminState};
pub use router::build_router;
pub use shutdown::ShutdownCoordinator;
pub use rate_limit::RateLimitState;
pub use file_broker::{FileBroker, StoredFile};
pub use state::{RunParams, LogQuery, CreateKeyRequest};

#[cfg(test)]
mod gate_tests;
