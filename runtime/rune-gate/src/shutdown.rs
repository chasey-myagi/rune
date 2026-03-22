use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

/// Coordinates graceful shutdown across the gate layer.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    draining: Arc<AtomicBool>,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        Self {
            draining: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal draining mode — new requests should be rejected with 503.
    pub fn start_drain(&self) {
        self.draining.store(true, AtomicOrdering::SeqCst);
    }

    /// Check if the server is in draining mode.
    pub fn is_draining(&self) -> bool {
        self.draining.load(AtomicOrdering::SeqCst)
    }
}
