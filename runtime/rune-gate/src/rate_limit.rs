use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;

/// Per-key rate limit counter: (count, window_start)
#[derive(Clone)]
pub struct RateLimitState {
    counters: Arc<DashMap<String, (u32, Instant)>>,
    requests_per_window: u32,
    window_secs: u64,
    /// Epoch-relative timestamp (secs) of last eviction, used to throttle cleanup
    last_eviction_secs: Arc<AtomicU64>,
    epoch: Instant,
}

impl RateLimitState {
    pub fn new(requests_per_window: u32, window_secs: u64) -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
            requests_per_window,
            window_secs,
            last_eviction_secs: Arc::new(AtomicU64::new(0)),
            epoch: Instant::now(),
        }
    }

    /// Check if a request from the given key is allowed.
    /// Returns Ok(()) if allowed, Err(retry_after_secs) if rate limited.
    pub fn check(&self, key: &str) -> Result<(), u64> {
        let now = Instant::now();

        // Throttled eviction: only run at most once per half-window to avoid O(n) per request
        let now_secs = now.duration_since(self.epoch).as_secs();
        let evict_interval = (self.window_secs / 2).max(1);
        let last = self.last_eviction_secs.load(Ordering::Relaxed);
        if now_secs.saturating_sub(last) >= evict_interval {
            // CAS to prevent multiple threads evicting simultaneously
            if self
                .last_eviction_secs
                .compare_exchange(last, now_secs, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.evict_expired(now);
            }
        }

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

    /// Remove entries whose window has expired. Throttled to run at most once per half-window.
    fn evict_expired(&self, now: Instant) {
        self.counters.retain(|_, (_, window_start)| {
            now.duration_since(*window_start).as_secs() < self.window_secs
        });
    }

    /// Return the number of tracked keys (for testing / diagnostics).
    pub fn entry_count(&self) -> usize {
        self.counters.len()
    }
}
