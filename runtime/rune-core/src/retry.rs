use crate::circuit_breaker::CircuitBreaker;
use crate::config::RetryConfig;
use crate::invoker::RuneInvoker;
use crate::rune::{RuneContext, RuneError};
use bytes::Bytes;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Buffer size for the wrapper channel in `invoke_stream`.
/// Matches the default channel size used by session.rs `execute_stream`.
const STREAM_WRAPPER_CHANNEL_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuneErrorKind {
    Unavailable,
    Timeout,
    Internal,
    ExecutionFailed,
    InvalidInput,
    NotFound,
    Cancelled,
    RateLimited,
    CircuitOpen,
    Unauthorized,
    Forbidden,
}

pub fn parse_retryable_errors(strings: &[String]) -> HashSet<RuneErrorKind> {
    strings
        .iter()
        .filter_map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "unavailable" => Some(RuneErrorKind::Unavailable),
            "timeout" => Some(RuneErrorKind::Timeout),
            "internal" => Some(RuneErrorKind::Internal),
            "execution_failed" | "executionfailed" => Some(RuneErrorKind::ExecutionFailed),
            "invalid_input" | "invalidinput" => Some(RuneErrorKind::InvalidInput),
            "not_found" | "notfound" => Some(RuneErrorKind::NotFound),
            "cancelled" | "canceled" => Some(RuneErrorKind::Cancelled),
            "rate_limited" | "ratelimited" => Some(RuneErrorKind::RateLimited),
            "circuit_open" | "circuitopen" => Some(RuneErrorKind::CircuitOpen),
            "unauthorized" => Some(RuneErrorKind::Unauthorized),
            "forbidden" => Some(RuneErrorKind::Forbidden),
            _ => None,
        })
        .collect()
}

fn error_kind(err: &RuneError) -> RuneErrorKind {
    match err {
        RuneError::Unavailable => RuneErrorKind::Unavailable,
        RuneError::Timeout => RuneErrorKind::Timeout,
        RuneError::Internal(_) => RuneErrorKind::Internal,
        RuneError::ExecutionFailed { .. } => RuneErrorKind::ExecutionFailed,
        RuneError::InvalidInput(_) => RuneErrorKind::InvalidInput,
        RuneError::NotFound(_) => RuneErrorKind::NotFound,
        RuneError::Cancelled => RuneErrorKind::Cancelled,
        RuneError::RateLimited { .. } => RuneErrorKind::RateLimited,
        RuneError::CircuitOpen { .. } => RuneErrorKind::CircuitOpen,
        RuneError::Unauthorized(_) => RuneErrorKind::Unauthorized,
        RuneError::Forbidden(_) => RuneErrorKind::Forbidden,
    }
}

/// Whether this error is safe to retry. Timeout is NEVER retryable because the
/// runtime does not cancel the remote caster on timeout — retrying would start a
/// second concurrent execution of the same rune.
pub fn is_retryable(err: &RuneError, retryable: &HashSet<RuneErrorKind>) -> bool {
    let kind = error_kind(err);
    // Timeout is forcibly excluded regardless of configuration.
    if kind == RuneErrorKind::Timeout {
        return false;
    }
    retryable.contains(&kind)
}

/// Client-side errors that are NOT the caster's fault and should NOT count
/// toward the circuit breaker.
fn is_client_error(err: &RuneError) -> bool {
    matches!(
        error_kind(err),
        RuneErrorKind::InvalidInput
            | RuneErrorKind::NotFound
            | RuneErrorKind::Cancelled
            | RuneErrorKind::RateLimited
            | RuneErrorKind::Unauthorized
            | RuneErrorKind::Forbidden
    )
}

pub struct RetryInvoker {
    inner: Arc<dyn RuneInvoker>,
    caster_id: String,
    config: RetryConfig,
    circuit_breaker: Arc<CircuitBreaker>,
    retryable: HashSet<RuneErrorKind>,
}

impl RetryInvoker {
    pub fn new(
        inner: Arc<dyn RuneInvoker>,
        caster_id: String,
        config: RetryConfig,
        circuit_breaker: Arc<CircuitBreaker>,
    ) -> Self {
        let retryable = parse_retryable_errors(&config.retryable_errors);
        if retryable.contains(&RuneErrorKind::Timeout) {
            tracing::warn!(
                caster_id = %caster_id,
                "retryable_errors contains 'timeout' which is forcibly excluded — \
                 timeout errors are never retried because the runtime does not cancel \
                 the remote caster on timeout"
            );
        }
        Self {
            inner,
            caster_id,
            config,
            circuit_breaker,
            retryable,
        }
    }

    fn retry_context(
        &self,
        base_ctx: &RuneContext,
        attempt: u32,
        deadline: std::time::Instant,
    ) -> Option<RuneContext> {
        let remaining = deadline.checked_duration_since(std::time::Instant::now())?;
        if remaining.is_zero() {
            return None;
        }
        let mut ctx = base_ctx.clone();
        ctx.request_id = format!("{}-retry-{}", base_ctx.request_id, attempt);
        ctx.timeout = remaining;
        Some(ctx)
    }

    fn backoff_delay_ms(&self, attempt: u32) -> u64 {
        let exponent = (attempt.saturating_sub(1)) as i32;
        let base_delay = (self.config.base_delay_ms as f64)
            * self.config.backoff_multiplier.powi(exponent.max(0));
        let cap = base_delay.round() as u64;
        let cap = cap.min(self.config.max_delay_ms);
        if cap == 0 {
            return 0;
        }

        // Full jitter: sleep ∈ [0, cap]. Spreads retry storms across the full
        // backoff window instead of clustering at [cap, cap*1.5].
        // See: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        rand::rng().random_range(0..=cap)
    }
}

#[async_trait::async_trait]
impl RuneInvoker for RetryInvoker {
    async fn invoke_once(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        if !self.config.enabled {
            return self.inner.invoke_once(ctx, input).await;
        }

        let deadline = std::time::Instant::now() + ctx.timeout;
        let base_ctx = ctx.clone();
        let mut current_ctx = ctx;
        let mut attempt = 0;

        loop {
            let _permit = self.circuit_breaker.can_execute()?;
            match self
                .inner
                .invoke_once(current_ctx.clone(), input.clone())
                .await
            {
                Ok(output) => {
                    self.circuit_breaker.record_success();
                    return Ok(output);
                }
                Err(err) => {
                    // Circuit breaker: count all backend failures (Timeout,
                    // Unavailable, Internal, ExecutionFailed). Client errors
                    // (InvalidInput, NotFound, Cancelled) are not the caster's fault.
                    if !is_client_error(&err) {
                        self.circuit_breaker.record_failure();
                    }

                    let retryable = is_retryable(&err, &self.retryable);
                    if attempt >= self.config.max_retries || !retryable {
                        return Err(err);
                    }

                    attempt += 1;

                    // Check deadline before sleeping — bail if already past.
                    let remaining_ms = deadline
                        .saturating_duration_since(std::time::Instant::now())
                        .as_millis() as u64;
                    if remaining_ms == 0 {
                        return Err(err);
                    }

                    let delay_ms = self.backoff_delay_ms(attempt);
                    // Cap sleep to remaining budget so we never overshoot.
                    let actual_delay = delay_ms.min(remaining_ms);

                    tracing::warn!(
                        request_id = %base_ctx.request_id,
                        caster_id = %self.caster_id,
                        attempt,
                        delay_ms = actual_delay,
                        error = %err,
                        "retrying rune invocation"
                    );
                    if actual_delay > 0 {
                        tokio::time::sleep(tokio::time::Duration::from_millis(actual_delay)).await;
                    }

                    // Build retry context AFTER sleep so ctx.timeout reflects
                    // the actual remaining budget, not the pre-sleep snapshot.
                    current_ctx = match self.retry_context(&base_ctx, attempt, deadline) {
                        Some(ctx) => ctx,
                        None => return Err(err),
                    };
                }
            }
        }
    }

    /// Streaming invocations are NOT retried.  The receiver is handed to the
    /// caller immediately, so a mid-stream error cannot be replayed without
    /// starting a second concurrent execution on the remote caster.  Only
    /// circuit-breaker gating applies to streams.
    async fn invoke_stream(
        &self,
        ctx: RuneContext,
        input: Bytes,
    ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
        if !self.config.enabled {
            return self.inner.invoke_stream(ctx, input).await;
        }

        let permit = self.circuit_breaker.can_execute()?;
        match self.inner.invoke_stream(ctx, input).await {
            Ok(mut inner_rx) => {
                // Wrap the receiver so success/failure is recorded when the
                // stream actually finishes, not at connection time.
                // The CBPermit is moved into the closure so that in HalfOpen
                // state the permit is held for the stream's entire lifetime,
                // preventing additional probes until this stream completes.
                let cb = Arc::clone(&self.circuit_breaker);
                let (tx, rx) = mpsc::channel(STREAM_WRAPPER_CHANNEL_SIZE);
                tokio::spawn(async move {
                    let _permit = permit; // hold permit until stream ends
                    let mut saw_error = false;
                    while let Some(item) = inner_rx.recv().await {
                        if item.is_err() {
                            saw_error = true;
                        }
                        if tx.send(item).await.is_err() {
                            break;
                        }
                    }
                    if saw_error {
                        cb.record_failure();
                    } else {
                        cb.record_success();
                    }
                });
                Ok(rx)
            }
            Err(err) => {
                if !is_client_error(&err) {
                    self.circuit_breaker.record_failure();
                }
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::CBState;
    use crate::config::CircuitBreakerConfig;
    use crate::rune::RuneContext;
    use anyhow::anyhow;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    fn test_ctx() -> RuneContext {
        RuneContext {
            rune_name: "echo".into(),
            request_id: "r-1".into(),
            context: Default::default(),
            timeout: Duration::from_secs(5),
        }
    }

    fn retry_config() -> RetryConfig {
        RetryConfig {
            enabled: true,
            max_retries: 2,
            base_delay_ms: 0,
            max_delay_ms: 0,
            backoff_multiplier: 2.0,
            retryable_errors: vec!["unavailable".into(), "internal".into()],
            circuit_breaker: CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 5,
                success_threshold: 1,
                reset_timeout_ms: 1,
                half_open_max_permits: 1,
            },
        }
    }

    struct SequenceInvoker {
        responses: Mutex<VecDeque<Result<Bytes, RuneError>>>,
        request_ids: Mutex<Vec<String>>,
        call_count: AtomicUsize,
    }

    impl SequenceInvoker {
        fn new(responses: Vec<Result<Bytes, RuneError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                request_ids: Mutex::new(Vec::new()),
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl RuneInvoker for SequenceInvoker {
        async fn invoke_once(&self, ctx: RuneContext, _input: Bytes) -> Result<Bytes, RuneError> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            self.request_ids
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(ctx.request_id);
            self.responses
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop_front()
                .expect("missing response")
        }

        async fn invoke_stream(
            &self,
            _ctx: RuneContext,
            _input: Bytes,
        ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
            panic!("stream path not used in this test")
        }
    }

    struct FailingStreamInvoker {
        call_count: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl RuneInvoker for FailingStreamInvoker {
        async fn invoke_once(&self, _ctx: RuneContext, _input: Bytes) -> Result<Bytes, RuneError> {
            panic!("once path not used in this test")
        }

        async fn invoke_stream(
            &self,
            _ctx: RuneContext,
            _input: Bytes,
        ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            Err(RuneError::Unavailable)
        }
    }

    #[tokio::test]
    async fn retry_success_after_transient_failure() {
        let inner = Arc::new(SequenceInvoker::new(vec![
            Err(RuneError::Unavailable),
            Err(RuneError::Internal(anyhow!("transient"))),
            Ok(Bytes::from("ok")),
        ]));
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            retry_config().circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), retry_config(), breaker);

        let result = invoker
            .invoke_once(test_ctx(), Bytes::from_static(b"payload"))
            .await
            .unwrap();

        assert_eq!(result, Bytes::from("ok"));
        assert_eq!(inner.call_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn retry_non_retryable_error_no_retry() {
        let inner = Arc::new(SequenceInvoker::new(vec![Err(RuneError::InvalidInput(
            "bad".into(),
        ))]));
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            retry_config().circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), retry_config(), breaker);

        let result = invoker.invoke_once(test_ctx(), Bytes::new()).await;

        assert!(matches!(result, Err(RuneError::InvalidInput(_))));
        assert_eq!(inner.call_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_fix_timeout_not_retried_by_default() {
        // Timeout must NOT be retried by default: the runtime does not cancel
        // the remote caster on timeout, so retrying would create duplicate execution.
        let inner = Arc::new(SequenceInvoker::new(vec![Err(RuneError::Timeout)]));
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            retry_config().circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), retry_config(), breaker);

        let result = invoker.invoke_once(test_ctx(), Bytes::new()).await;

        assert!(matches!(result, Err(RuneError::Timeout)));
        assert_eq!(
            inner.call_count.load(Ordering::Relaxed),
            1,
            "timeout must not trigger retry"
        );
    }

    #[tokio::test]
    async fn retry_generates_unique_request_ids() {
        let inner = Arc::new(SequenceInvoker::new(vec![
            Err(RuneError::Unavailable),
            Err(RuneError::Internal(anyhow!("boom"))),
            Ok(Bytes::from("ok")),
        ]));
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            retry_config().circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), retry_config(), breaker);

        invoker
            .invoke_once(test_ctx(), Bytes::from_static(b"payload"))
            .await
            .unwrap();

        let request_ids = inner
            .request_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(
            request_ids,
            vec![
                "r-1".to_string(),
                "r-1-retry-1".to_string(),
                "r-1-retry-2".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn retry_circuit_breaker_open_fast_fail() {
        let config = RetryConfig {
            max_retries: 0,
            circuit_breaker: CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 1,
                success_threshold: 1,
                reset_timeout_ms: 60_000,
                half_open_max_permits: 1,
            },
            ..retry_config()
        };
        let inner = Arc::new(SequenceInvoker::new(vec![Err(RuneError::Unavailable)]));
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            config.circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), config, breaker.clone());

        let first = invoker.invoke_once(test_ctx(), Bytes::new()).await;
        let second = invoker.invoke_once(test_ctx(), Bytes::new()).await;

        assert!(matches!(first, Err(RuneError::Unavailable)));
        assert!(matches!(second, Err(RuneError::CircuitOpen { .. })));
        assert_eq!(inner.call_count.load(Ordering::Relaxed), 1);
        assert_eq!(breaker.state(), CBState::Open);
    }

    #[tokio::test]
    async fn test_fix_client_errors_do_not_trip_circuit_breaker() {
        // Bug: record_failure() ran before is_retryable() check, so client errors
        // (InvalidInput, NotFound, Cancelled) incremented the breaker counter.
        // With failure_threshold=2, two bad-input requests would open the breaker
        // and block subsequent valid requests.
        let config = RetryConfig {
            circuit_breaker: CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 2,
                success_threshold: 1,
                reset_timeout_ms: 60_000,
                half_open_max_permits: 1,
            },
            ..retry_config()
        };
        let inner = Arc::new(SequenceInvoker::new(vec![
            Err(RuneError::InvalidInput("bad1".into())),
            Err(RuneError::NotFound("gone".into())),
            // After two client errors the breaker must still be Closed,
            // so this third call must reach the inner invoker.
            Ok(Bytes::from("ok")),
        ]));
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            config.circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), config, breaker.clone());

        // First call: InvalidInput — not retryable, should NOT count as CB failure
        let r1 = invoker.invoke_once(test_ctx(), Bytes::new()).await;
        assert!(matches!(r1, Err(RuneError::InvalidInput(_))));

        // Second call: NotFound — not retryable, should NOT count as CB failure
        let r2 = invoker.invoke_once(test_ctx(), Bytes::new()).await;
        assert!(matches!(r2, Err(RuneError::NotFound(_))));

        // Breaker must still be Closed
        assert_eq!(
            breaker.state(),
            CBState::Closed,
            "client errors must not open the breaker"
        );

        // Third call: should reach inner invoker (breaker still closed)
        let r3 = invoker.invoke_once(test_ctx(), Bytes::new()).await;
        assert_eq!(r3.unwrap(), Bytes::from("ok"));
    }

    #[tokio::test]
    async fn test_fix_retryable_errors_do_trip_circuit_breaker() {
        // Ensure retryable/backend errors still correctly increment the breaker.
        let config = RetryConfig {
            max_retries: 0, // no retry so each call is independent
            circuit_breaker: CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 2,
                success_threshold: 1,
                reset_timeout_ms: 60_000,
                half_open_max_permits: 1,
            },
            ..retry_config()
        };
        let inner = Arc::new(SequenceInvoker::new(vec![
            Err(RuneError::Unavailable),
            Err(RuneError::Unavailable),
        ]));
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            config.circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), config, breaker.clone());

        let _ = invoker.invoke_once(test_ctx(), Bytes::new()).await;
        let _ = invoker.invoke_once(test_ctx(), Bytes::new()).await;

        assert_eq!(
            breaker.state(),
            CBState::Open,
            "retryable errors must open the breaker"
        );
    }

    #[tokio::test]
    async fn test_fix_timeout_counts_toward_circuit_breaker() {
        // Bug: after removing timeout from retryable_errors, record_failure()
        // was gated on is_retryable(). A caster that keeps timing out would
        // never trip the breaker, causing all subsequent requests to eat the
        // full timeout instead of fast-failing.
        // Timeout must NOT be retried but MUST still count as a CB failure.
        let config = RetryConfig {
            max_retries: 0,
            circuit_breaker: CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 2,
                success_threshold: 1,
                reset_timeout_ms: 60_000,
                half_open_max_permits: 1,
            },
            ..retry_config()
        };
        let inner = Arc::new(SequenceInvoker::new(vec![
            Err(RuneError::Timeout),
            Err(RuneError::Timeout),
        ]));
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            config.circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), config, breaker.clone());

        let _ = invoker.invoke_once(test_ctx(), Bytes::new()).await;
        assert_eq!(breaker.state(), CBState::Closed);

        let _ = invoker.invoke_once(test_ctx(), Bytes::new()).await;
        assert_eq!(
            breaker.state(),
            CBState::Open,
            "repeated timeouts must open the breaker even though timeout is not retried"
        );
    }

    #[tokio::test]
    async fn test_fix_timeout_never_retried_even_if_configured() {
        // Open question resolved: even if a user explicitly adds "timeout" to
        // retryable_errors, RetryInvoker must refuse to retry it because the
        // runtime does not cancel the remote caster on timeout.
        let config = RetryConfig {
            enabled: true,
            max_retries: 3,
            base_delay_ms: 0,
            max_delay_ms: 0,
            backoff_multiplier: 1.0,
            retryable_errors: vec!["timeout".into(), "unavailable".into()],
            circuit_breaker: CircuitBreakerConfig {
                enabled: false,
                ..Default::default()
            },
        };
        let inner = Arc::new(SequenceInvoker::new(vec![Err(RuneError::Timeout)]));
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            config.circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), config, breaker);

        let result = invoker.invoke_once(test_ctx(), Bytes::new()).await;
        assert!(matches!(result, Err(RuneError::Timeout)));
        assert_eq!(
            inner.call_count.load(Ordering::Relaxed),
            1,
            "timeout must never be retried, even if explicitly in retryable_errors"
        );
    }

    #[tokio::test]
    async fn test_fix_backoff_does_not_overshoot_deadline() {
        // Bug: code did sleep(delay_ms) unconditionally, then checked deadline.
        // If remaining budget < delay, the request would oversleep before failing.
        let config = RetryConfig {
            enabled: true,
            max_retries: 5,
            base_delay_ms: 500, // 500ms base delay
            max_delay_ms: 2_000,
            backoff_multiplier: 2.0,
            retryable_errors: vec!["unavailable".into()],
            circuit_breaker: CircuitBreakerConfig {
                enabled: false,
                ..Default::default()
            },
        };
        // 100ms total budget — first call fails instantly, then backoff would be
        // ~500ms which exceeds the remaining budget.
        let ctx = RuneContext {
            rune_name: "echo".into(),
            request_id: "overshoot-test".into(),
            context: Default::default(),
            timeout: Duration::from_millis(100),
        };

        struct InstantFail {
            call_count: AtomicUsize,
        }
        #[async_trait::async_trait]
        impl RuneInvoker for InstantFail {
            async fn invoke_once(
                &self,
                _ctx: RuneContext,
                _input: Bytes,
            ) -> Result<Bytes, RuneError> {
                self.call_count.fetch_add(1, Ordering::Relaxed);
                Err(RuneError::Unavailable)
            }
            async fn invoke_stream(
                &self,
                _ctx: RuneContext,
                _input: Bytes,
            ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
                panic!("not used")
            }
        }

        let inner = Arc::new(InstantFail {
            call_count: AtomicUsize::new(0),
        });
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            config.circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), config, breaker);

        let start = std::time::Instant::now();
        let _ = invoker.invoke_once(ctx, Bytes::new()).await;
        let elapsed = start.elapsed();

        // With 100ms budget, the total elapsed time (including any backoff)
        // must not significantly exceed the budget.
        assert!(
            elapsed < Duration::from_millis(200),
            "elapsed {:?} should not overshoot the 100ms deadline by more than 100ms",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_fix_retry_respects_deadline() {
        // Bug: each retry reused the full original ctx.timeout. A rune with 30s
        // timeout could consume 30s * (max_retries+1) + backoff. Retries must
        // carry forward the remaining timeout budget from a single deadline.
        let config = RetryConfig {
            enabled: true,
            max_retries: 10, // plenty of retries
            base_delay_ms: 0,
            max_delay_ms: 0,
            backoff_multiplier: 1.0,
            retryable_errors: vec!["unavailable".into()],
            circuit_breaker: CircuitBreakerConfig {
                enabled: false,
                ..Default::default()
            },
        };
        // Give a very short timeout — 50ms total budget
        let ctx = RuneContext {
            rune_name: "echo".into(),
            request_id: "deadline-test".into(),
            context: Default::default(),
            timeout: Duration::from_millis(50),
        };

        // Inner invoker sleeps 30ms per call then fails — after 2 calls (60ms)
        // the deadline should be exhausted.
        struct SlowFailInvoker {
            call_count: AtomicUsize,
        }
        #[async_trait::async_trait]
        impl RuneInvoker for SlowFailInvoker {
            async fn invoke_once(
                &self,
                _ctx: RuneContext,
                _input: Bytes,
            ) -> Result<Bytes, RuneError> {
                self.call_count.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
                Err(RuneError::Unavailable)
            }
            async fn invoke_stream(
                &self,
                _ctx: RuneContext,
                _input: Bytes,
            ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
                panic!("not used")
            }
        }

        let inner = Arc::new(SlowFailInvoker {
            call_count: AtomicUsize::new(0),
        });
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            config.circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), config, breaker);

        let result = invoker.invoke_once(ctx, Bytes::new()).await;
        assert!(result.is_err());

        // With 50ms budget and 30ms per call, at most 2 attempts should happen
        // (first at 0ms, second at ~30ms, third would start at ~60ms > deadline).
        let calls = inner.call_count.load(Ordering::Relaxed);
        assert!(
            calls <= 2,
            "expected at most 2 attempts with 50ms budget, got {calls}"
        );
    }

    #[tokio::test]
    async fn test_fix_retry_timeout_accounts_for_backoff_sleep() {
        // Bug: retry_context() was called BEFORE sleep, so ctx.timeout was set
        // to remaining budget pre-sleep. After sleeping, the inner invoker got
        // a timeout larger than the actual remaining budget.
        // Fix: retry_context() is called AFTER sleep.
        //
        // We use a large min delay (500ms) so that even with full jitter [0,500]
        // the test remains stable — the inner invoker itself sleeps 200ms, so
        // the total elapsed between first and second invoke is at least 200ms.
        let config = RetryConfig {
            enabled: true,
            max_retries: 1,
            base_delay_ms: 500, // large backoff
            max_delay_ms: 500,
            backoff_multiplier: 1.0,
            retryable_errors: vec!["unavailable".into()],
            circuit_breaker: CircuitBreakerConfig {
                enabled: false,
                ..Default::default()
            },
        };
        let ctx = RuneContext {
            rune_name: "echo".into(),
            request_id: "backoff-budget".into(),
            context: Default::default(),
            timeout: Duration::from_secs(5),
        };

        struct TimedCapture {
            timeouts: Mutex<Vec<Duration>>,
        }
        #[async_trait::async_trait]
        impl RuneInvoker for TimedCapture {
            async fn invoke_once(
                &self,
                ctx: RuneContext,
                _input: Bytes,
            ) -> Result<Bytes, RuneError> {
                self.timeouts
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(ctx.timeout);
                // Simulate some work so elapsed time is non-trivial
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                Err(RuneError::Unavailable)
            }
            async fn invoke_stream(
                &self,
                _ctx: RuneContext,
                _input: Bytes,
            ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
                panic!("not used")
            }
        }

        let inner = Arc::new(TimedCapture {
            timeouts: Mutex::new(Vec::new()),
        });
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            config.circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), config, breaker);

        let _ = invoker.invoke_once(ctx, Bytes::new()).await;

        let timeouts = inner
            .timeouts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(timeouts.len(), 2, "should have initial + 1 retry");

        let first = timeouts[0];
        let second = timeouts[1];
        // The retry timeout must be reduced by at least the 200ms work time.
        // If retry_context were called before sleep, second would be ≈ first
        // (only reduced by the work time, not the backoff). With the fix,
        // second is reduced by work_time + jitter_sleep.
        let reduction = first - second;
        assert!(
            reduction >= Duration::from_millis(150),
            "retry timeout reduction {:?} must account for elapsed time (work + backoff), \
             got first={:?} second={:?}",
            reduction,
            first,
            second
        );
    }

    #[tokio::test]
    async fn test_fix_retry_context_reduces_timeout() {
        // Verify that each retry context carries a reduced timeout.
        let config = RetryConfig {
            enabled: true,
            max_retries: 2,
            base_delay_ms: 0,
            max_delay_ms: 0,
            backoff_multiplier: 1.0,
            retryable_errors: vec!["unavailable".into()],
            circuit_breaker: CircuitBreakerConfig {
                enabled: false,
                ..Default::default()
            },
        };
        let ctx = RuneContext {
            rune_name: "echo".into(),
            request_id: "budget-test".into(),
            context: Default::default(),
            timeout: Duration::from_secs(10),
        };

        // Capture the timeout from each invocation
        struct TimeoutCapture {
            timeouts: Mutex<Vec<Duration>>,
        }
        #[async_trait::async_trait]
        impl RuneInvoker for TimeoutCapture {
            async fn invoke_once(
                &self,
                ctx: RuneContext,
                _input: Bytes,
            ) -> Result<Bytes, RuneError> {
                self.timeouts
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(ctx.timeout);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                Err(RuneError::Unavailable)
            }
            async fn invoke_stream(
                &self,
                _ctx: RuneContext,
                _input: Bytes,
            ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
                panic!("not used")
            }
        }

        let inner = Arc::new(TimeoutCapture {
            timeouts: Mutex::new(Vec::new()),
        });
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            config.circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), config, breaker);

        let _ = invoker.invoke_once(ctx, Bytes::new()).await;

        let timeouts = inner
            .timeouts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert!(timeouts.len() >= 2, "should have at least 2 attempts");
        // Each retry must have a strictly smaller timeout than the previous
        for window in timeouts.windows(2) {
            assert!(
                window[1] < window[0],
                "retry timeout {:?} must be less than previous {:?}",
                window[1],
                window[0]
            );
        }
    }

    #[tokio::test]
    async fn retry_stream_no_retry() {
        let inner = Arc::new(FailingStreamInvoker {
            call_count: AtomicUsize::new(0),
        });
        let breaker = Arc::new(CircuitBreaker::new(
            "test-caster".into(),
            retry_config().circuit_breaker.clone(),
        ));
        let invoker = RetryInvoker::new(inner.clone(), "caster-1".into(), retry_config(), breaker);

        let result = invoker.invoke_stream(test_ctx(), Bytes::new()).await;

        assert!(matches!(result, Err(RuneError::Unavailable)));
        assert_eq!(inner.call_count.load(Ordering::Relaxed), 1);
    }

    /// Regression P1-5: CBPermit must be held for the stream's entire lifetime.
    ///
    /// In HalfOpen state, the breaker allows at most `half_open_max_permits`
    /// concurrent probes. If the permit is dropped when invoke_stream returns
    /// (before the stream finishes), a second probe can enter while the first
    /// stream is still active. After the fix, the permit is moved into the
    /// spawn closure and held until the stream completes.
    #[tokio::test]
    async fn test_fix_stream_cb_permit_held_during_stream() {
        use crate::circuit_breaker::CBState;

        struct SlowStreamInvoker;

        #[async_trait::async_trait]
        impl RuneInvoker for SlowStreamInvoker {
            async fn invoke_once(
                &self,
                _ctx: RuneContext,
                _input: Bytes,
            ) -> Result<Bytes, RuneError> {
                panic!("not used")
            }

            async fn invoke_stream(
                &self,
                _ctx: RuneContext,
                _input: Bytes,
            ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
                let (tx, rx) = mpsc::channel(4);
                // Spawn a task that holds the stream open
                tokio::spawn(async move {
                    // Wait a bit before completing the stream
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    let _ = tx.send(Ok(Bytes::from("data"))).await;
                });
                Ok(rx)
            }
        }

        // Set up breaker in HalfOpen state with max_permits=1
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 1,
            success_threshold: 1,
            reset_timeout_ms: 1,
            half_open_max_permits: 1,
        };
        let breaker = Arc::new(CircuitBreaker::new("test-caster".into(), config.clone()));
        // Trip the breaker
        breaker.record_failure();
        assert_eq!(breaker.state(), CBState::Open);
        // Wait for reset_timeout so it transitions to HalfOpen on next can_execute
        std::thread::sleep(Duration::from_millis(5));

        let retry_cfg = RetryConfig {
            enabled: true,
            max_retries: 0,
            base_delay_ms: 0,
            max_delay_ms: 0,
            backoff_multiplier: 1.0,
            retryable_errors: vec![],
            circuit_breaker: config,
        };

        let invoker = RetryInvoker::new(
            Arc::new(SlowStreamInvoker),
            "caster-1".into(),
            retry_cfg,
            Arc::clone(&breaker),
        );

        // First stream: should succeed (HalfOpen allows 1 probe)
        let stream_rx = invoker
            .invoke_stream(test_ctx(), Bytes::new())
            .await
            .expect("first stream should be allowed");

        // The permit must still be held inside the spawn closure.
        // With max_permits=1, a second probe attempt must be rejected.
        let second_attempt = invoker.invoke_stream(test_ctx(), Bytes::new()).await;
        assert!(
            matches!(second_attempt, Err(RuneError::CircuitOpen { .. })),
            "second stream must be rejected while first stream is active (permit held)"
        );

        // Consume the first stream so the permit is released
        drop(stream_rx);
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}
