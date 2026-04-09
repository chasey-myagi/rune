use crate::config::CircuitBreakerConfig;
use crate::rune::RuneError;
use dashmap::DashMap;
use serde::Serialize;
use std::sync::{Arc, Mutex};
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
}

struct CBInner {
    state: CBState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    half_open_permits: u32,
    opened_at: Option<Instant>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            inner: Mutex::new(CBInner {
                state: CBState::Closed,
                consecutive_failures: 0,
                consecutive_successes: 0,
                half_open_permits: 0,
                opened_at: None,
            }),
            config,
        }
    }

    pub fn can_execute(self: &Arc<Self>) -> Result<CBPermit, RuneError> {
        if !self.config.enabled {
            return Ok(CBPermit::new(Arc::clone(self), false));
        }

        // Safety: we deliberately recover from mutex poisoning here.
        // CircuitBreaker inner state (counters + state enum) is always
        // self-consistent — a panic during a previous lock hold cannot
        // leave it in a partially-updated state because each method
        // writes all dependent fields atomically within a single
        // critical section.  Propagating the poison would bring down
        // the entire runtime for a single caster's transient failure.
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
                        rune_name: String::new(),
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

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).state
    }

    pub fn snapshot(&self, caster_id: &str) -> CircuitBreakerSnapshot {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        CircuitBreakerSnapshot {
            caster_id: caster_id.to_string(),
            state: inner.state,
            consecutive_failures: inner.consecutive_failures,
            consecutive_successes: inner.consecutive_successes,
            since_ms: inner
                .opened_at
                .map(|opened_at| opened_at.elapsed().as_millis()),
        }
    }

    fn take_half_open_permit(self: &Arc<Self>, inner: &mut CBInner) -> Result<CBPermit, RuneError> {
        let max_permits = self.config.half_open_max_permits.max(1);
        if inner.half_open_permits >= max_permits {
            return Err(RuneError::CircuitOpen {
                rune_name: String::new(),
            });
        }
        inner.half_open_permits += 1;
        Ok(CBPermit::new(Arc::clone(self), true))
    }

    fn release_half_open_permit(&self) {
        if !self.config.enabled {
            return;
        }

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
            .or_insert_with(|| Arc::new(CircuitBreaker::new(self.config.clone())))
            .clone()
    }

    pub fn remove(&self, caster_id: &str) {
        self.breakers.remove(caster_id);
    }

    pub fn states(&self) -> Vec<CircuitBreakerSnapshot> {
        let mut states: Vec<_> = self
            .breakers
            .iter()
            .map(|entry| entry.value().snapshot(entry.key()))
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
        }
    }

    #[test]
    fn opens_after_threshold_failures() {
        let breaker = Arc::new(CircuitBreaker::new(config()));

        breaker.record_failure();
        assert_eq!(breaker.state(), CBState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), CBState::Open);
        assert!(breaker.can_execute().is_err());
    }

    #[test]
    fn half_open_probe_can_close_breaker() {
        let breaker = Arc::new(CircuitBreaker::new(config()));

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
        let breaker = Arc::new(CircuitBreaker::new(config()));

        breaker.record_failure();
        breaker.record_failure();
        std::thread::sleep(Duration::from_millis(10));

        let permit = breaker.can_execute().expect("should allow probe");
        breaker.record_failure();
        drop(permit);

        assert_eq!(breaker.state(), CBState::Open);
    }
}
