//! Execution context passed to rune handlers.

use std::collections::HashMap;

use tokio_util::sync::CancellationToken;

/// Execution context passed to every Rune handler invocation.
#[derive(Debug, Clone)]
pub struct RuneContext {
    /// Name of the Rune being invoked.
    pub rune_name: String,
    /// Unique request ID for this invocation.
    pub request_id: String,
    /// Arbitrary key-value context from the caller.
    pub context: HashMap<String, String>,
    /// Cancellation token that fires when the request is cancelled.
    pub cancellation: CancellationToken,
}
