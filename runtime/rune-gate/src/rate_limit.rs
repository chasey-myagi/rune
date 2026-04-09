use rune_core::config::PerRuneRateLimit;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;

/// Per-key rate limit counter: (count, window_start)
#[derive(Clone)]
pub struct RateLimitState {
    counters: Arc<DashMap<String, (u32, Instant)>>,
    per_rune_counters: Arc<DashMap<String, (u32, Instant)>>,
    requests_per_window: u32,
    window_secs: u64,
    exact_per_rune_limits: Arc<HashMap<String, u32>>,
    wildcard_per_rune_limits: Arc<Vec<(String, u32)>>,
    /// Epoch-relative timestamp (secs) of last eviction, used to throttle cleanup
    last_eviction_secs: Arc<AtomicU64>,
    epoch: Instant,
}

impl RateLimitState {
    pub fn new(requests_per_window: u32, window_secs: u64) -> Self {
        Self::with_per_rune_limits(requests_per_window, window_secs, HashMap::new())
    }

    pub fn with_per_rune_limits(
        requests_per_window: u32,
        window_secs: u64,
        per_rune: HashMap<String, PerRuneRateLimit>,
    ) -> Self {
        let mut exact = HashMap::new();
        let mut wildcard = Vec::new();
        for (pattern, rule) in per_rune {
            if let Some(prefix) = pattern.strip_suffix('*') {
                wildcard.push((prefix.to_string(), rule.requests_per_minute));
            } else {
                exact.insert(pattern, rule.requests_per_minute);
            }
        }
        wildcard.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

        Self {
            counters: Arc::new(DashMap::new()),
            per_rune_counters: Arc::new(DashMap::new()),
            requests_per_window,
            window_secs,
            exact_per_rune_limits: Arc::new(exact),
            wildcard_per_rune_limits: Arc::new(wildcard),
            last_eviction_secs: Arc::new(AtomicU64::new(0)),
            epoch: Instant::now(),
        }
    }

    /// Check if a request from the given key is allowed.
    /// Returns Ok(()) if allowed, Err(retry_after_secs) if rate limited.
    pub fn check(&self, key: &str) -> Result<(), u64> {
        let now = Instant::now();
        self.maybe_evict(now);
        Self::check_counter(
            &self.counters,
            key,
            self.requests_per_window,
            self.window_secs,
            now,
        )
    }

    pub fn check_rune(&self, key: &str, rune_name: &str) -> Result<(), u64> {
        let requests_per_window = match self.per_rune_limit_for(rune_name) {
            Some(limit) => limit,
            None => return Ok(()),
        };

        let now = Instant::now();
        self.maybe_evict(now);
        let composite_key = format!("{key}:{rune_name}");
        Self::check_counter(
            &self.per_rune_counters,
            &composite_key,
            requests_per_window,
            self.window_secs,
            now,
        )
    }

    /// Throttled eviction: only runs at most once per half-window to avoid
    /// O(n) cleanup on every request. Uses CAS to prevent concurrent eviction.
    fn maybe_evict(&self, now: Instant) {
        let now_secs = now.duration_since(self.epoch).as_secs();
        let evict_interval = (self.window_secs / 2).max(1);
        let last = self.last_eviction_secs.load(Ordering::Relaxed);
        if now_secs.saturating_sub(last) >= evict_interval
            && self
                .last_eviction_secs
                .compare_exchange(last, now_secs, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.evict_expired(now);
        }
    }

    fn per_rune_limit_for(&self, rune_name: &str) -> Option<u32> {
        if let Some(limit) = self.exact_per_rune_limits.get(rune_name) {
            return Some(*limit);
        }

        self.wildcard_per_rune_limits
            .iter()
            .find(|(prefix, _)| rune_name.starts_with(prefix))
            .map(|(_, limit)| *limit)
    }

    fn check_counter(
        counters: &DashMap<String, (u32, Instant)>,
        key: &str,
        requests_per_window: u32,
        window_secs: u64,
        now: Instant,
    ) -> Result<(), u64> {
        let mut entry = counters.entry(key.to_string()).or_insert((0, now));
        let (count, window_start) = entry.value_mut();

        // Fixed-window rate limiter: at the boundary between two consecutive
        // windows a client can issue up to 2× the configured limit (tail of old
        // window + head of new window).  Acceptable for most use-cases; switch
        // to sliding-window or token-bucket if stricter burst control is needed.
        let elapsed = now.duration_since(*window_start).as_secs();
        if elapsed >= window_secs {
            // Reset window
            *count = 1;
            *window_start = now;
            return Ok(());
        }

        // Within window — check count
        if *count >= requests_per_window {
            let retry_after = window_secs - elapsed;
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
        self.per_rune_counters.retain(|_, (_, window_start)| {
            now.duration_since(*window_start).as_secs() < self.window_secs
        });
    }

    /// Return the number of tracked keys (for testing / diagnostics).
    pub fn entry_count(&self) -> usize {
        self.counters.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_rune_limiter_exact_match() {
        let limiter = RateLimitState::with_per_rune_limits(
            100,
            60,
            HashMap::from([(
                "translate".to_string(),
                PerRuneRateLimit {
                    requests_per_minute: 1,
                },
            )]),
        );

        assert!(limiter.check_rune("k_1", "translate").is_ok());
        assert!(limiter.check_rune("k_1", "translate").is_err());
    }

    #[test]
    fn per_rune_limiter_wildcard_match() {
        let limiter = RateLimitState::with_per_rune_limits(
            100,
            60,
            HashMap::from([(
                "ai.*".to_string(),
                PerRuneRateLimit {
                    requests_per_minute: 1,
                },
            )]),
        );

        assert!(limiter.check_rune("k_1", "ai.translate").is_ok());
        assert!(limiter.check_rune("k_1", "ai.translate").is_err());
    }

    #[test]
    fn per_rune_limiter_longest_prefix_wins() {
        let limiter = RateLimitState::with_per_rune_limits(
            100,
            60,
            HashMap::from([
                (
                    "ai.*".to_string(),
                    PerRuneRateLimit {
                        requests_per_minute: 5,
                    },
                ),
                (
                    "ai.translate.*".to_string(),
                    PerRuneRateLimit {
                        requests_per_minute: 1,
                    },
                ),
            ]),
        );

        assert!(limiter.check_rune("k_1", "ai.translate.en").is_ok());
        assert!(limiter.check_rune("k_1", "ai.translate.en").is_err());
    }
}
