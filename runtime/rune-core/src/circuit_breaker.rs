use crate::config::CircuitBreakerConfig;
use crate::rune::RuneError;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CBState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, Serialize)]
pub struct CircuitBreakerSnapshot {
    pub caster_id: String,
    pub state: CBState,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since_ms: Option<u128>,
}

pub struct CircuitBreaker {
    inner: Mutex<CBInner>,
    config: CircuitBreakerConfig,
    caster_id: String,
}

struct CBInner {
    state: CBState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    half_open_permits: u32,
    opened_at: Option<Instant>,
    last_activity_at: Instant,
}

impl CircuitBreaker {
    pub fn new(caster_id: String, config: CircuitBreakerConfig) -> Self {
        let now = Instant::now();
        Self {
            inner: Mutex::new(CBInner {
                state: CBState::Closed,
                consecutive_failures: 0,
                consecutive_successes: 0,
                half_open_permits: 0,
                opened_at: None,
                last_activity_at: now,
            }),
            config,
            caster_id,
        }
    }

    pub fn can_execute(self: &Arc<Self>) -> Result<CBPermit, RuneError> {
        if !self.config.enabled {
            return Ok(CBPermit::new(Arc::clone(self), false));
        }

        let mut inner = self.inner.lock();
        inner.last_activity_at = Instant::now();
        match inner.state {
            CBState::Closed => Ok(CBPermit::new(Arc::clone(self), false)),
            CBState::Open => {
                let reset_timeout = Duration::from_millis(self.config.reset_timeout_ms);
                let can_probe = inner
                    .opened_at
                    .map(|opened_at| opened_at.elapsed() >= reset_timeout)
                    .unwrap_or(true);
                if !can_probe {
                    return Err(RuneError::CircuitOpen {
                        rune_name: self.caster_id.clone(),
                    });
                }

                inner.state = CBState::HalfOpen;
                inner.consecutive_successes = 0;
                inner.half_open_permits = 0;
                inner.opened_at = None;
                self.take_half_open_permit(&mut inner)
            }
            CBState::HalfOpen => self.take_half_open_permit(&mut inner),
        }
    }

    pub fn record_success(&self) {
        if !self.config.enabled {
            return;
        }

        let mut inner = self.inner.lock();
        inner.last_activity_at = Instant::now();
        match inner.state {
            CBState::Closed => {
                inner.consecutive_failures = 0;
            }
            CBState::HalfOpen => {
                inner.consecutive_failures = 0;
                inner.consecutive_successes += 1;
                if inner.consecutive_successes >= self.config.success_threshold.max(1) {
                    inner.state = CBState::Closed;
                    inner.consecutive_successes = 0;
                    inner.half_open_permits = 0;
                    inner.opened_at = None;
                }
            }
            CBState::Open => {}
        }
    }

    pub fn record_failure(&self) {
        if !self.config.enabled {
            return;
        }

        let mut inner = self.inner.lock();
        inner.last_activity_at = Instant::now();
        match inner.state {
            CBState::Closed => {
                inner.consecutive_successes = 0;
                inner.consecutive_failures += 1;
                if inner.consecutive_failures >= self.config.failure_threshold.max(1) {
                    transition_to_open(&mut inner);
                }
            }
            CBState::HalfOpen | CBState::Open => {
                transition_to_open(&mut inner);
            }
        }
    }

    pub fn state(&self) -> CBState {
        self.inner.lock().state
    }

    pub fn snapshot(&self) -> CircuitBreakerSnapshot {
        let inner = self.inner.lock();
        CircuitBreakerSnapshot {
            caster_id: self.caster_id.clone(),
            state: inner.state,
            consecutive_failures: inner.consecutive_failures,
            consecutive_successes: inner.consecutive_successes,
            since_ms: inner
                .opened_at
                .map(|opened_at| opened_at.elapsed().as_millis()),
        }
    }

    /// Returns true if the breaker has been idle for longer than `max_idle`.
    /// All states (Closed, Open, HalfOpen) are eligible for cleanup — a zombie
    /// breaker that has not seen any activity for hours should not keep eating
    /// memory regardless of its state.
    pub fn is_stale(&self, max_idle: Duration) -> bool {
        let inner = self.inner.lock();
        inner.last_activity_at.elapsed() > max_idle
    }

    fn take_half_open_permit(self: &Arc<Self>, inner: &mut CBInner) -> Result<CBPermit, RuneError> {
        let max_permits = self.config.half_open_max_permits.max(1);
        if inner.half_open_permits >= max_permits {
            return Err(RuneError::CircuitOpen {
                rune_name: self.caster_id.clone(),
            });
        }
        inner.half_open_permits += 1;
        Ok(CBPermit::new(Arc::clone(self), true))
    }

    fn release_half_open_permit(&self) {
        if !self.config.enabled {
            return;
        }

        let mut inner = self.inner.lock();
        if inner.state == CBState::HalfOpen && inner.half_open_permits > 0 {
            inner.half_open_permits -= 1;
        }
    }
}

fn transition_to_open(inner: &mut CBInner) {
    inner.state = CBState::Open;
    inner.opened_at = Some(Instant::now());
    inner.consecutive_successes = 0;
    inner.half_open_permits = 0;
}

pub struct CBPermit {
    breaker: Arc<CircuitBreaker>,
    counts_toward_half_open: bool,
}

impl CBPermit {
    fn new(breaker: Arc<CircuitBreaker>, counts_toward_half_open: bool) -> Self {
        Self {
            breaker,
            counts_toward_half_open,
        }
    }
}

impl Drop for CBPermit {
    fn drop(&mut self) {
        if self.counts_toward_half_open {
            self.breaker.release_half_open_permit();
        }
    }
}

pub struct CircuitBreakerRegistry {
    breakers: DashMap<String, Arc<CircuitBreaker>>,
    config: CircuitBreakerConfig,
}

impl CircuitBreakerRegistry {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            breakers: DashMap::new(),
            config,
        }
    }

    pub fn get_or_create(&self, caster_id: &str) -> Arc<CircuitBreaker> {
        self.breakers
            .entry(caster_id.to_string())
            .or_insert_with(|| {
                Arc::new(CircuitBreaker::new(
                    caster_id.to_string(),
                    self.config.clone(),
                ))
            })
            .clone()
    }

    pub fn remove(&self, caster_id: &str) {
        self.breakers.remove(caster_id);
    }

    /// Remove breakers that have been idle for longer than `max_idle`.
    /// All states (Closed, Open, HalfOpen) are eligible — a zombie breaker
    /// that has not seen any activity for hours should not keep eating memory.
    /// When a HalfOpen breaker is removed, the next request will recreate it
    /// as Closed (starting fresh with zero failure history).
    pub fn cleanup_stale(&self, max_idle: Duration) {
        self.breakers
            .retain(|_, breaker| !breaker.is_stale(max_idle));
    }

    pub fn states(&self) -> Vec<CircuitBreakerSnapshot> {
        let mut states: Vec<_> = self
            .breakers
            .iter()
            .map(|entry| entry.value().snapshot())
            .collect();
        states.sort_by(|left, right| left.caster_id.cmp(&right.caster_id));
        states
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 2,
            success_threshold: 2,
            reset_timeout_ms: 5,
            half_open_max_permits: 1,
            ..Default::default()
        }
    }

    #[test]
    fn opens_after_threshold_failures() {
        let breaker = Arc::new(CircuitBreaker::new("test-caster".into(), config()));

        breaker.record_failure();
        assert_eq!(breaker.state(), CBState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), CBState::Open);
        assert!(breaker.can_execute().is_err());
    }

    #[test]
    fn half_open_probe_can_close_breaker() {
        let breaker = Arc::new(CircuitBreaker::new("test-caster".into(), config()));

        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), CBState::Open);

        std::thread::sleep(Duration::from_millis(10));
        let permit = breaker.can_execute().expect("should allow probe");
        assert_eq!(breaker.state(), CBState::HalfOpen);
        breaker.record_success();
        drop(permit);
        assert_eq!(breaker.state(), CBState::HalfOpen);

        let permit = breaker.can_execute().expect("should allow second probe");
        breaker.record_success();
        drop(permit);
        assert_eq!(breaker.state(), CBState::Closed);
    }

    #[test]
    fn half_open_failure_reopens_breaker() {
        let breaker = Arc::new(CircuitBreaker::new("test-caster".into(), config()));

        breaker.record_failure();
        breaker.record_failure();
        std::thread::sleep(Duration::from_millis(10));

        let permit = breaker.can_execute().expect("should allow probe");
        breaker.record_failure();
        drop(permit);

        assert_eq!(breaker.state(), CBState::Open);
    }

    #[test]
    fn half_open_max_permits_reports_caster_id() {
        let breaker = Arc::new(CircuitBreaker::new("my-caster".into(), config()));

        // Trip to Open
        breaker.record_failure();
        breaker.record_failure();

        // Wait for reset timeout, transition to HalfOpen
        std::thread::sleep(Duration::from_millis(10));
        let _permit = breaker.can_execute().expect("first probe should succeed");

        // Second probe should be rejected — max_permits is 1
        match breaker.can_execute() {
            Err(RuneError::CircuitOpen { rune_name }) => {
                assert_eq!(rune_name, "my-caster", "CircuitOpen must carry caster_id");
            }
            Err(other) => panic!("expected CircuitOpen, got: {:?}", other),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }
}
