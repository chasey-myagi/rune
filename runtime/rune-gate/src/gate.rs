//! Backward-compatible re-export module.
//!
//! All types previously defined in `gate.rs` have been moved to dedicated modules.
//! This module re-exports them so that `rune_gate::gate::GateState`, etc. still work.

pub use crate::error::{error_response, map_error, map_flow_error};
pub use crate::file_broker::{FileBroker, StoredFile};
pub use crate::multipart::{build_multipart_body, is_multipart, sanitize_filename, FileMetadata};
pub use crate::rate_limit::RateLimitState;
pub use crate::router::build_router;
pub use crate::shutdown::ShutdownCoordinator;
pub use crate::state::unique_request_id;
pub use crate::state::{
    AdminState, AuthState, CreateKeyRequest, FlowState, GateState, LogQuery, RunParams, RuneState,
    DEFAULT_REQUEST_TIMEOUT,
};
