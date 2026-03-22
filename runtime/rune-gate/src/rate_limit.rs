use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;

/// Per-key rate limit counter: (count, window_start)
#[derive(Clone)]
pub struct RateLimitState {
    counters: Arc<DashMap<String, (u32, Instant)>>,
    requests_per_window: u32,
    window_secs: u64,
}

impl RateLimitState {
    pub fn new(requests_per_window: u32, window_secs: u64) -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
            requests_per_window,
            window_secs,
        }
    }

    /// Check if a request from the given key is allowed.
    /// Returns Ok(()) if allowed, Err(retry_after_secs) if rate limited.
    pub fn check(&self, key: &str) -> Result<(), u64> {
        let now = Instant::now();
        let mut entry = self.counters.entry(key.to_string()).or_insert((0, now));
        let (count, window_start) = entry.value_mut();

        // Check if window has expired
        let elapsed = now.duration_since(*window_start).as_secs();
        if elapsed >= self.window_secs {
            // Reset window
            *count = 1;
            *window_start = now;
            return Ok(());
        }

        // Within window — check count
        if *count >= self.requests_per_window {
            let retry_after = self.window_secs - elapsed;
            return Err(retry_after.max(1));
        }

        *count += 1;
        Ok(())
    }
}
