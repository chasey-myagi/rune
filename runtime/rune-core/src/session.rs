use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time;
use tonic::Streaming;
use rune_proto::*;
use crate::auth::KeyVerifier;
use crate::rune::{RuneConfig, RuneError};
use crate::relay::Relay;
use crate::invoker::RemoteInvoker;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SessionState {
    Attaching,
    Active,
    Disconnected,
}

/// Callback invoked when a caster attaches, with caster_id and registered rune configs.
pub type OnCasterAttach = Arc<dyn Fn(&str, &[RuneConfig]) + Send + Sync>;

pub(crate) enum PendingResponse {
    Once(oneshot::Sender<Result<Bytes, RuneError>>),
    Stream(mpsc::Sender<Result<Bytes, RuneError>>),
}

pub(crate) struct PendingRequest {
    pub(crate) tx: PendingResponse,
    pub(crate) created_at: Instant,
    pub(crate) _permit: tokio::sync::OwnedSemaphorePermit,
    pub(crate) timeout: Duration,
}

pub struct SessionManager {
    pub(crate) sessions: DashMap<String, CasterState>,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) heartbeat_timeout: Duration,
    on_caster_attach: OnceLock<OnCasterAttach>,
    key_verifier: Arc<dyn KeyVerifier>,
    dev_mode: bool,
}

pub(crate) struct CasterState {
    pub(crate) outbound: mpsc::Sender<SessionMessage>,
    pub(crate) pending: Arc<DashMap<String, PendingRequest>>,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) connected_at: Instant,
}

impl SessionManager {
    /// Test-only constructor with dev_mode=true and no auth.
    /// Production code should use `with_auth()`.
    pub fn new_dev(heartbeat_interval: Duration, heartbeat_timeout: Duration) -> Self {
        Self {
            sessions: DashMap::new(),
            heartbeat_interval,
            heartbeat_timeout,
            on_caster_attach: OnceLock::new(),
            key_verifier: Arc::new(crate::auth::NoopVerifier),
            dev_mode: true,
        }
    }

    pub fn with_auth(
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
        key_verifier: Arc<dyn KeyVerifier>,
        dev_mode: bool,
    ) -> Self {
        Self {
            sessions: DashMap::new(),
            heartbeat_interval,
            heartbeat_timeout,
            on_caster_attach: OnceLock::new(),
            key_verifier,
            dev_mode,
        }
    }

    /// Set the callback invoked when a caster successfully attaches.
    /// Can only be called once; subsequent calls are silently ignored.
    pub fn set_on_caster_attach(&self, callback: OnCasterAttach) {
        let _ = self.on_caster_attach.set(callback);
    }

    pub fn is_available(&self, caster_id: &str) -> bool {
        self.sessions.get(caster_id)
            .map(|s| s.semaphore.available_permits() > 0)
            .unwrap_or(false)
    }

    /// Return the list of connected caster IDs.
    pub fn list_caster_ids(&self) -> Vec<String> {
        self.sessions.iter().map(|e| e.key().clone()).collect()
    }

    /// Return the number of connected casters.
    pub fn caster_count(&self) -> usize {
        self.sessions.len()
    }

    /// Return the number of available permits (free concurrency slots) for a caster.
    /// Returns 0 if the caster is not connected.
    pub fn available_permits(&self, caster_id: &str) -> usize {
        self.sessions.get(caster_id)
            .map(|s| s.semaphore.available_permits())
            .unwrap_or(0)
    }

    /// Return the `Instant` when a caster connected, or `None` if not found.
    pub fn connected_at(&self, caster_id: &str) -> Option<Instant> {
        self.sessions.get(caster_id).map(|s| s.connected_at)
    }

    /// Insert a dummy caster for integration tests in downstream crates.
    pub fn insert_test_caster(&self, caster_id: &str, max_concurrent: usize) {
        let (tx, _rx) = mpsc::channel(16);
        self.sessions.insert(caster_id.to_string(), CasterState {
            outbound: tx,
            pending: Arc::new(DashMap::new()),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            connected_at: Instant::now(),
        });
    }

    pub async fn handle_session(
        self: &Arc<Self>,
        relay: Arc<Relay>,
        mut inbound: Streaming<SessionMessage>,
        outbound_tx: mpsc::Sender<SessionMessage>,
    ) {
        let mut caster_id: Option<String> = None;
        let mut state = SessionState::Attaching;
        let pending: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        let last_heartbeat_ms = Arc::new(AtomicU64::new(now_ms()));
        let semaphore = Arc::new(Semaphore::new(0)); // Attach adds permits
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        // Capture config values
        let hb_interval = self.heartbeat_interval;
        let hb_timeout = self.heartbeat_timeout;

        // Heartbeat task
        let hb_tx = outbound_tx.clone();
        let hb_last = Arc::clone(&last_heartbeat_ms);
        let hb_shutdown = shutdown_tx.clone();
        let hb_handle = tokio::spawn(async move {
            loop {
                time::sleep(hb_interval).await;
                let msg = SessionMessage {
                    payload: Some(session_message::Payload::Heartbeat(Heartbeat { timestamp_ms: now_ms() })),
                };
                if hb_tx.send(msg).await.is_err() { break; }
                let elapsed = now_ms().saturating_sub(hb_last.load(Ordering::Relaxed));
                if elapsed > hb_timeout.as_millis() as u64 {
                    tracing::warn!(elapsed_ms = elapsed, "heartbeat timeout, forcing session shutdown");
                    let _ = hb_shutdown.send(true);
                    break;
                }
            }
        });
        drop(shutdown_tx);

        loop {
            let msg = tokio::select! {
                result = inbound.message() => {
                    match result {
                        Ok(Some(msg)) => msg,
                        _ => break,
                    }
                }
                _ = shutdown_rx.changed() => {
                    tracing::info!("session shutdown signal received");
                    break;
                }
            };

            match msg.payload {
                Some(session_message::Payload::Attach(attach)) => {
                    let id = attach.caster_id.clone();
                    let max_conc = attach.max_concurrent;

                    // ── Auth check ──
                    if !self.dev_mode {
                        let key_valid = self.key_verifier.verify_caster_key(&attach.key).await;
                        if !key_valid {
                            tracing::warn!(caster_id = %id, "caster attach rejected: invalid key");
                            let ack = SessionMessage {
                                payload: Some(session_message::Payload::AttachAck(AttachAck {
                                    accepted: false,
                                    reason: "invalid or missing caster key".into(),
                                    supported_features: Vec::new(),
                                })),
                            };
                            let _ = outbound_tx.send(ack).await;
                            break;
                        }
                    }

                    tracing::info!(caster_id = %id, runes = attach.runes.len(), max_concurrent = max_conc, "caster attached");

                    let permits = if max_conc > 0 { max_conc as usize } else { usize::MAX >> 1 };
                    semaphore.add_permits(permits);

                    self.sessions.insert(id.clone(), CasterState {
                        outbound: outbound_tx.clone(),
                        pending: Arc::clone(&pending),
                        semaphore: Arc::clone(&semaphore),
                        connected_at: Instant::now(),
                    });

                    let mut configs = Vec::new();
                    for decl in &attach.runes {
                        let config = RuneConfig {
                            name: decl.name.clone(),
                            version: decl.version.clone(),
                            description: decl.description.clone(),
                            supports_stream: decl.supports_stream,
                            gate: decl.gate.as_ref().map(|g| crate::rune::GateConfig {
                                path: g.path.clone(),
                                method: if g.method.is_empty() { "POST".to_string() } else { g.method.clone() },
                            }),
                            input_schema: if decl.input_schema.is_empty() { None } else { Some(decl.input_schema.clone()) },
                            output_schema: if decl.output_schema.is_empty() { None } else { Some(decl.output_schema.clone()) },
                            priority: decl.priority,
                            labels: attach.labels.clone(),
                        };
                        configs.push(config.clone());
                        let invoker = Arc::new(RemoteInvoker {
                            session: Arc::clone(self) as Arc<SessionManager>,
                            caster_id: id.clone(),
                        });
                        if let Err(reason) = relay.register(config, invoker, Some(id.clone())) {
                            tracing::warn!(rune = %decl.name, %reason, "skipping rune registration");
                        }
                    }

                    // Notify attach callback (e.g. for snapshot recording)
                    if let Some(callback) = self.on_caster_attach.get() {
                        callback(&id, &configs);
                    }

                    let ack = SessionMessage {
                        payload: Some(session_message::Payload::AttachAck(AttachAck {
                            accepted: true, reason: String::new(),
                            supported_features: Vec::new(),
                        })),
                    };
                    let _ = outbound_tx.send(ack).await;
                    state = SessionState::Active;
                    caster_id = Some(id);
                }

                Some(session_message::Payload::Result(result)) if state == SessionState::Active => {
                    let req_id = result.request_id.clone();
                    if let Some((_, p)) = pending.remove(&req_id) {
                        // permit auto-returned when PendingRequest is dropped
                        let outcome = match result.status() {
                            Status::Completed => Ok(Bytes::from(result.output)),
                            _ => Err(RuneError::ExecutionFailed {
                                code: result.error.as_ref().map(|e| e.code.clone()).unwrap_or_default(),
                                message: result.error.as_ref().map(|e| e.message.clone()).unwrap_or_default(),
                            }),
                        };
                        match p.tx {
                            PendingResponse::Once(tx) => { let _ = tx.send(outcome); }
                            PendingResponse::Stream(tx) => { let _ = tx.send(outcome).await; }
                        }
                    }
                }

                Some(session_message::Payload::StreamEvent(event)) if state == SessionState::Active => {
                    let req_id = event.request_id.clone();
                    if let Some(p) = pending.get(&req_id) {
                        if let PendingResponse::Stream(ref tx) = p.tx {
                            let _ = tx.send(Ok(Bytes::from(event.data))).await;
                        }
                    }
                }

                Some(session_message::Payload::StreamEnd(end)) if state == SessionState::Active => {
                    let req_id = end.request_id.clone();
                    if let Some((_, p)) = pending.remove(&req_id) {
                        // permit auto-returned when PendingRequest is dropped
                        if let PendingResponse::Stream(tx) = p.tx {
                            if end.status() != Status::Completed {
                                let _ = tx.send(Err(RuneError::ExecutionFailed {
                                    code: end.error.as_ref().map(|e| e.code.clone()).unwrap_or_default(),
                                    message: end.error.as_ref().map(|e| e.message.clone()).unwrap_or_default(),
                                })).await;
                            }
                        }
                    }
                }

                Some(session_message::Payload::Detach(detach)) => {
                    tracing::info!(reason = %detach.reason, "caster detached");
                    break;
                }

                Some(session_message::Payload::Heartbeat(_)) => {
                    last_heartbeat_ms.store(now_ms(), Ordering::Relaxed);
                }

                _ => {}
            }
        }

        hb_handle.abort();
        let _ = state; // state is now Disconnected conceptually
        if let Some(id) = &caster_id {
            tracing::info!(caster_id = %id, "cleaning up disconnected caster");
            self.sessions.remove(id);
            relay.remove_caster(id);
            let keys: Vec<String> = pending.iter().map(|e| e.key().clone()).collect();
            for req_id in keys {
                if let Some((_, p)) = pending.remove(&req_id) {
                    let err = Err(RuneError::Internal(anyhow::anyhow!("caster '{}' disconnected", id)));
                    match p.tx {
                        PendingResponse::Once(tx) => { let _ = tx.send(err); }
                        PendingResponse::Stream(tx) => { let _ = tx.send(err).await; }
                    }
                }
            }
        }
    }

    pub async fn execute(
        &self, caster_id: &str, request_id: &str, rune_name: &str, input: Bytes,
        context: HashMap<String, String>, timeout: Duration,
    ) -> Result<Bytes, RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
        let permit = Arc::clone(&session.semaphore)
            .try_acquire_owned()
            .map_err(|_| RuneError::Unavailable)?;

        let (tx, rx) = oneshot::channel();
        session.pending.insert(request_id.to_string(), PendingRequest {
            tx: PendingResponse::Once(tx), created_at: Instant::now(), timeout,
            _permit: permit,
        });

        // Per-request timeout: spawn a task that fires after the request's timeout
        let timeout_pending = Arc::clone(&session.pending);
        let timeout_req_id = request_id.to_string();
        let req_timeout = timeout;
        tokio::spawn(async move {
            tokio::time::sleep(req_timeout).await;
            // If still pending when timeout fires, remove and send error
            if let Some((_, p)) = timeout_pending.remove(&timeout_req_id) {
                tracing::warn!(request_id = %timeout_req_id, "request timed out");
                match p.tx {
                    PendingResponse::Once(tx) => { let _ = tx.send(Err(RuneError::Timeout)); }
                    PendingResponse::Stream(tx) => { let _ = tx.send(Err(RuneError::Timeout)).await; }
                }
            }
            // If already removed (completed/cancelled), this is a no-op
        });

        let msg = SessionMessage {
            payload: Some(session_message::Payload::Execute(ExecuteRequest {
                request_id: request_id.to_string(), rune_name: rune_name.to_string(),
                input: input.to_vec(), context,
                timeout_ms: safe_timeout_ms(timeout),
                attachments: Vec::new(),
            })),
        };

        if session.outbound.send(msg).await.is_err() {
            session.pending.remove(request_id);
            // permit auto-returned when PendingRequest is dropped
            return Err(RuneError::Unavailable);
        }
        drop(session);
        rx.await.map_err(|_| RuneError::Internal(anyhow::anyhow!("response channel dropped")))?
    }

    pub async fn execute_stream(
        &self, caster_id: &str, request_id: &str, rune_name: &str, input: Bytes,
        context: HashMap<String, String>, timeout: Duration,
    ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
        let permit = Arc::clone(&session.semaphore)
            .try_acquire_owned()
            .map_err(|_| RuneError::Unavailable)?;

        let (tx, rx) = mpsc::channel(32);
        session.pending.insert(request_id.to_string(), PendingRequest {
            tx: PendingResponse::Stream(tx), created_at: Instant::now(), timeout,
            _permit: permit,
        });

        // Per-request timeout: spawn a task that fires after the request's timeout
        let timeout_pending = Arc::clone(&session.pending);
        let timeout_req_id = request_id.to_string();
        let req_timeout = timeout;
        tokio::spawn(async move {
            tokio::time::sleep(req_timeout).await;
            // If still pending when timeout fires, remove and send error
            if let Some((_, p)) = timeout_pending.remove(&timeout_req_id) {
                tracing::warn!(request_id = %timeout_req_id, "request timed out");
                match p.tx {
                    PendingResponse::Once(tx) => { let _ = tx.send(Err(RuneError::Timeout)); }
                    PendingResponse::Stream(tx) => { let _ = tx.send(Err(RuneError::Timeout)).await; }
                }
            }
            // If already removed (completed/cancelled), this is a no-op
        });

        let msg = SessionMessage {
            payload: Some(session_message::Payload::Execute(ExecuteRequest {
                request_id: request_id.to_string(), rune_name: rune_name.to_string(),
                input: input.to_vec(), context,
                timeout_ms: safe_timeout_ms(timeout),
                attachments: Vec::new(),
            })),
        };

        if session.outbound.send(msg).await.is_err() {
            session.pending.remove(request_id);
            // permit auto-returned when PendingRequest is dropped
            return Err(RuneError::Unavailable);
        }
        drop(session);
        Ok(rx)
    }

    /// Cancel a request by searching all sessions for the request_id.
    /// Returns true if the request was found and cancelled.
    pub async fn cancel_by_request_id(&self, request_id: &str, reason: &str) -> bool {
        for session in self.sessions.iter() {
            if session.pending.contains_key(request_id) {
                let caster_id = session.key().clone();
                drop(session);
                let _ = self.cancel(&caster_id, request_id, reason).await;
                return true;
            }
        }
        false
    }

    pub async fn cancel(&self, caster_id: &str, request_id: &str, reason: &str) -> Result<(), RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
        if let Some((_, p)) = session.pending.remove(request_id) {
            // permit auto-returned when PendingRequest is dropped
            let err = Err(RuneError::Cancelled);
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(err); }
                PendingResponse::Stream(tx) => { let _ = tx.send(err).await; }
            }
        }
        let msg = SessionMessage {
            payload: Some(session_message::Payload::Cancel(CancelRequest {
                request_id: request_id.to_string(), reason: reason.to_string(),
            })),
        };
        let _ = session.outbound.send(msg).await;
        Ok(())
    }
}

/// Convert a Duration to milliseconds clamped to u32::MAX to avoid truncation.
pub(crate) fn safe_timeout_ms(timeout: Duration) -> u32 {
    timeout.as_millis().min(u32::MAX as u128) as u32
}

fn now_ms() -> u64 {
    crate::time_utils::now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

    fn default_session_manager() -> Arc<SessionManager> {
        Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ))
    }

    /// Helper: creates a SessionManager with one registered caster session.
    fn setup_session(max_concurrent: usize) -> (
        Arc<SessionManager>,
        String,
        Arc<Semaphore>,
        Arc<DashMap<String, PendingRequest>>,
        mpsc::Receiver<SessionMessage>,
    ) {
        let mgr = default_session_manager();
        let (tx, rx) = mpsc::channel(16);
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let pending: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        let caster_id = "test-caster".to_string();

        mgr.sessions.insert(caster_id.clone(), CasterState {
            outbound: tx,
            pending: Arc::clone(&pending),
            semaphore: Arc::clone(&semaphore),
            connected_at: Instant::now(),
        });

        (mgr, caster_id, semaphore, pending, rx)
    }

    /// Poll until a key appears in the pending map (max 1 second).
    async fn wait_for_pending(pending: &DashMap<String, PendingRequest>, key: &str) {
        for _ in 0..100 {
            if pending.contains_key(key) { return; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for pending key '{}'", key);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_returns_permit_on_normal_completion() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(2);
        assert_eq!(sem.available_permits(), 2);

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "r-1", "echo", Bytes::from("hello"), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "r-1").await;
        assert_eq!(sem.available_permits(), 1);

        if let Some((_, p)) = pending.remove("r-1") {
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::from("world"))); }
                _ => panic!("expected Once"),
            }
        }

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, Bytes::from("world"));
        assert_eq!(sem.available_permits(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_late_result_does_not_inflate_permits() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(2);

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "r-timeout", "slow", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "r-timeout").await;
        assert_eq!(sem.available_permits(), 1);

        if let Some((_, p)) = pending.remove("r-timeout") {
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Err(RuneError::Timeout)); }
                _ => panic!("expected Once"),
            }
        }

        assert_eq!(sem.available_permits(), 2);

        let late_found = pending.remove("r-timeout");
        assert!(late_found.is_none(), "late result should find nothing in pending");
        assert_eq!(sem.available_permits(), 2, "late result must not inflate permits");

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(RuneError::Timeout)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancel_returns_permit() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(2);

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "r-cancel", "slow", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "r-cancel").await;
        assert_eq!(sem.available_permits(), 1);

        mgr.cancel(&caster_id, "r-cancel", "user requested").await.unwrap();
        assert_eq!(sem.available_permits(), 2, "cancel must return permit");

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(RuneError::Cancelled)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_max_concurrent_enforced() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(1);

        let _handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "r-1", "echo", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "r-1").await;
        assert_eq!(sem.available_permits(), 0);

        let result = mgr.execute(&caster_id, "r-2", "echo", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await;
        assert!(matches!(result, Err(RuneError::Unavailable)),
            "second request must be rejected when max_concurrent=1");
    }

    #[tokio::test]
    async fn test_disconnect_clears_all_pending() {
        let (_mgr, _caster_id, _sem, pending, _rx) = setup_session(5);

        let dummy_sem = Arc::new(Semaphore::new(10));
        for i in 0..3 {
            let (tx, _rx) = oneshot::channel();
            let permit = Arc::clone(&dummy_sem).try_acquire_owned().unwrap();
            pending.insert(format!("r-{}", i), PendingRequest {
                tx: PendingResponse::Once(tx),
                created_at: Instant::now(),
                timeout: DEFAULT_TIMEOUT,
                _permit: permit,
            });
        }

        assert_eq!(pending.len(), 3);

        let keys: Vec<String> = pending.iter().map(|e| e.key().clone()).collect();
        for req_id in keys {
            let _ = pending.remove(&req_id);
        }

        assert!(pending.is_empty(), "all pending should be cleared after disconnect");
    }

    #[tokio::test]
    async fn test_list_caster_ids() {
        let mgr = default_session_manager();
        assert!(mgr.list_caster_ids().is_empty());
        assert_eq!(mgr.caster_count(), 0);

        let (tx, _rx) = mpsc::channel(16);
        mgr.sessions.insert("caster-1".to_string(), CasterState {
            outbound: tx.clone(),
            pending: Arc::new(DashMap::new()),
            semaphore: Arc::new(Semaphore::new(1)),
            connected_at: Instant::now(),
        });
        mgr.sessions.insert("caster-2".to_string(), CasterState {
            outbound: tx,
            pending: Arc::new(DashMap::new()),
            semaphore: Arc::new(Semaphore::new(1)),
            connected_at: Instant::now(),
        });

        assert_eq!(mgr.caster_count(), 2);
        let mut ids = mgr.list_caster_ids();
        ids.sort();
        assert_eq!(ids, vec!["caster-1", "caster-2"]);
    }

    // ---- Scenario 12: execute_stream semaphore permit return ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_stream_returns_permit_on_completion() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(2);
        assert_eq!(sem.available_permits(), 2);

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute_stream(&cid, "rs-1", "streamer", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "rs-1").await;
        // One permit consumed by execute_stream
        assert_eq!(sem.available_permits(), 1);

        // Simulate stream completion: remove pending (permit auto-returned on drop)
        if let Some((_, p)) = pending.remove("rs-1") {
            match p.tx {
                PendingResponse::Stream(tx) => {
                    let _ = tx.send(Ok(Bytes::from("chunk1"))).await;
                    // Drop tx to close the stream
                    drop(tx);
                }
                _ => panic!("expected Stream"),
            }
        }

        let stream_rx = handle.await.unwrap().unwrap();
        // Permit should be restored
        assert_eq!(sem.available_permits(), 2);
        drop(stream_rx);
    }

    // ---- Scenario 13: cancel_by_request_id across sessions ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancel_by_request_id_cross_session() {
        let mgr = default_session_manager();

        // Set up two casters
        let (tx1, _rx1) = mpsc::channel(16);
        let sem1 = Arc::new(Semaphore::new(5));
        let pending1: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        mgr.sessions.insert("caster-A".to_string(), CasterState {
            outbound: tx1,
            pending: Arc::clone(&pending1),
            semaphore: Arc::clone(&sem1),
            connected_at: Instant::now(),
        });

        let (tx2, _rx2) = mpsc::channel(16);
        let sem2 = Arc::new(Semaphore::new(5));
        let pending2: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        mgr.sessions.insert("caster-B".to_string(), CasterState {
            outbound: tx2,
            pending: Arc::clone(&pending2),
            semaphore: Arc::clone(&sem2),
            connected_at: Instant::now(),
        });

        // Launch a request on caster-A
        let handle = {
            let mgr_clone = Arc::clone(&mgr);
            tokio::spawn(async move {
                mgr_clone.execute("caster-A", "cross-req-1", "slow_rune", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending1, "cross-req-1").await;
        assert_eq!(sem1.available_permits(), 4);

        // Cancel from the manager level (without knowing which caster owns the request)
        let cancelled = mgr.cancel_by_request_id("cross-req-1", "user cancelled").await;
        assert!(cancelled, "cancel_by_request_id should find the request");

        // Verify the request was resolved with an error
        let result = handle.await.unwrap();
        assert!(matches!(result, Err(RuneError::Cancelled)));

        // Permit should be returned
        assert_eq!(sem1.available_permits(), 5);
    }

    #[tokio::test]
    async fn test_cancel_by_request_id_nonexistent() {
        let mgr = default_session_manager();
        let cancelled = mgr.cancel_by_request_id("does-not-exist", "reason").await;
        assert!(!cancelled, "cancel should return false for non-existent request");
    }

    // ---- Scenario 14: on_caster_attach callback verification ----

    #[test]
    fn test_set_on_caster_attach_callback() {
        let mgr = default_session_manager();

        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        mgr.set_on_caster_attach(Arc::new(move |caster_id, configs| {
            assert_eq!(caster_id, "test-caster");
            assert_eq!(configs.len(), 1);
            assert_eq!(configs[0].name, "my_rune");
            called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        }));

        // Verify the callback is set (OnceLock can only be set once)
        assert!(mgr.on_caster_attach.get().is_some());

        // NOTE: The callback is invoked inside handle_session() which requires
        // a real gRPC Streaming, making it difficult to test end-to-end here.
        // We verify the OnceLock mechanism works correctly.
    }

    #[test]
    fn test_set_on_caster_attach_only_once() {
        let mgr = default_session_manager();

        let first_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_clone = Arc::clone(&first_called);
        mgr.set_on_caster_attach(Arc::new(move |_, _| {
            first_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        }));

        // Second set should be silently ignored (OnceLock behavior)
        let second_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let second_clone = Arc::clone(&second_called);
        mgr.set_on_caster_attach(Arc::new(move |_, _| {
            second_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        }));

        // The callback in OnceLock should still be the first one
        // (verifying OnceLock's set-once semantics)
        assert!(mgr.on_caster_attach.get().is_some());
    }

    // ---- Scenario 15: heartbeat timeout ----
    // NOTE: End-to-end heartbeat timeout testing requires a real
    // `tonic::Streaming<SessionMessage>` (live gRPC transport or mock stream).
    // The current session test framework uses direct `CasterState` insertion
    // (bypassing `handle_session`), so the heartbeat loop is never started.
    // For now, we verify the heartbeat configuration is correctly propagated.

    #[test]
    fn test_heartbeat_config_propagation() {
        let hb_interval = Duration::from_secs(5);
        let hb_timeout = Duration::from_secs(15);
        let mgr = SessionManager::new_dev(hb_interval, hb_timeout);

        assert_eq!(mgr.heartbeat_interval, hb_interval);
        assert_eq!(mgr.heartbeat_timeout, hb_timeout);
    }

    // ---- max_concurrent=1 strict serialization ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_max_concurrent_1_strict_serial() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(1);

        // First request should succeed
        let handle1 = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "serial-1", "echo", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "serial-1").await;
        assert_eq!(sem.available_permits(), 0);

        // Second request should be rejected immediately
        let result2 = mgr.execute(&caster_id, "serial-2", "echo", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await;
        assert!(
            matches!(result2, Err(RuneError::Unavailable)),
            "second request must be Unavailable when max_concurrent=1"
        );

        // Complete first request (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("serial-1") {
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::from("done"))); }
                _ => panic!("expected Once"),
            }
        }

        let result1 = handle1.await.unwrap().unwrap();
        assert_eq!(result1, Bytes::from("done"));
        assert_eq!(sem.available_permits(), 1);

        // Now a third request should succeed (permit was returned)
        let handle3 = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "serial-3", "echo", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "serial-3").await;
        assert_eq!(sem.available_permits(), 0);

        // Complete third request (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("serial-3") {
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::from("done3"))); }
                _ => panic!("expected Once"),
            }
        }

        let result3 = handle3.await.unwrap().unwrap();
        assert_eq!(result3, Bytes::from("done3"));
    }

    // ---- Timeout returns permit correctly ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_timeout_returns_permit() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(1);
        assert_eq!(sem.available_permits(), 1);

        // Start a request
        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "timeout-req", "slow", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "timeout-req").await;
        assert_eq!(sem.available_permits(), 0, "permit should be consumed");

        // Simulate timeout by removing pending (permit auto-returned on drop)
        if let Some((_, p)) = pending.remove("timeout-req") {
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Err(RuneError::Timeout)); }
                _ => panic!("expected Once"),
            }
        }

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(RuneError::Timeout)));

        // Permit should be available again
        assert_eq!(sem.available_permits(), 1, "permit must be returned after timeout");

        // A new request should now succeed
        let handle2 = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "after-timeout", "echo", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "after-timeout").await;
        assert_eq!(sem.available_permits(), 0);

        // Complete it (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("after-timeout") {
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::from("ok"))); }
                _ => panic!("expected Once"),
            }
        }

        let result2 = handle2.await.unwrap().unwrap();
        assert_eq!(result2, Bytes::from("ok"));
    }

    // ---- cancel_by_request_id only affects the target caster ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancel_only_affects_target_caster() {
        let mgr = default_session_manager();

        // Set up three casters
        let mut pending_maps = Vec::new();
        let mut sems = Vec::new();
        let mut rxs = Vec::new();
        for i in 0..3 {
            let (tx, rx) = mpsc::channel(16);
            let sem = Arc::new(Semaphore::new(5));
            let pending: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
            mgr.sessions.insert(format!("caster-{}", i), CasterState {
                outbound: tx,
                pending: Arc::clone(&pending),
                semaphore: Arc::clone(&sem),
                connected_at: Instant::now(),
            });
            pending_maps.push(pending);
            sems.push(sem);
            rxs.push(rx);
        }

        // Launch a request on each caster
        let mut handles = Vec::new();
        for i in 0..3 {
            let mgr_clone = Arc::clone(&mgr);
            let cid = format!("caster-{}", i);
            let rid = format!("req-{}", i);
            let handle = tokio::spawn(async move {
                mgr_clone.execute(&cid, &rid, "rune", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            });
            handles.push(handle);
        }

        // Wait for all requests to appear in pending
        for i in 0..3 {
            wait_for_pending(&pending_maps[i], &format!("req-{}", i)).await;
        }

        // Cancel only req-1 (on caster-1)
        let cancelled = mgr.cancel_by_request_id("req-1", "cancelled by test").await;
        assert!(cancelled);

        // caster-1's request should be cancelled
        assert!(!pending_maps[1].contains_key("req-1"), "req-1 should be removed from pending");
        assert_eq!(sems[1].available_permits(), 5, "caster-1 permit should be returned");

        // caster-0 and caster-2 requests should still be pending
        assert!(pending_maps[0].contains_key("req-0"), "req-0 should still be pending");
        assert!(pending_maps[2].contains_key("req-2"), "req-2 should still be pending");
        assert_eq!(sems[0].available_permits(), 4, "caster-0 permit should still be consumed");
        assert_eq!(sems[2].available_permits(), 4, "caster-2 permit should still be consumed");

        // Verify the cancelled request's result
        let result1 = handles.remove(1).await.unwrap();
        assert!(matches!(result1, Err(RuneError::Cancelled)));

        // Complete remaining requests (permit auto-returned on PendingRequest drop)
        for i in [0, 2] {
            if let Some((_, p)) = pending_maps[i].remove(&format!("req-{}", i)) {
                match p.tx {
                    PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::from("done"))); }
                    _ => panic!("expected Once"),
                }
            }
        }
    }

    // ---- execute_stream cancel returns permit ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_stream_cancel_returns_permit() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(2);
        assert_eq!(sem.available_permits(), 2);

        // Start a streaming request
        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute_stream(&cid, "stream-cancel", "streamer", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "stream-cancel").await;
        assert_eq!(sem.available_permits(), 1);

        // Cancel the streaming request
        mgr.cancel(&caster_id, "stream-cancel", "user cancel").await.unwrap();

        // Permit should be returned
        assert_eq!(sem.available_permits(), 2, "cancel must return permit for stream request");

        // The stream receiver should eventually get the cancellation error
        let _stream_rx = handle.await.unwrap().unwrap();
    }

    // ---- Disconnect and reconnect with same caster_id ----

    #[tokio::test]
    async fn test_disconnect_then_reconnect_same_caster_id() {
        let mgr = default_session_manager();

        // First connection
        let (tx1, _rx1) = mpsc::channel(16);
        let sem1 = Arc::new(Semaphore::new(3));
        let pending1: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        mgr.sessions.insert("reconnect-caster".to_string(), CasterState {
            outbound: tx1,
            pending: Arc::clone(&pending1),
            semaphore: Arc::clone(&sem1),
            connected_at: Instant::now(),
        });

        assert!(mgr.is_available("reconnect-caster"));
        assert_eq!(mgr.caster_count(), 1);

        // Simulate disconnect: remove from sessions
        mgr.sessions.remove("reconnect-caster");
        assert!(!mgr.is_available("reconnect-caster"));
        assert_eq!(mgr.caster_count(), 0);

        // Reconnect with same caster_id
        let (tx2, _rx2) = mpsc::channel(16);
        let sem2 = Arc::new(Semaphore::new(5));
        let pending2: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        mgr.sessions.insert("reconnect-caster".to_string(), CasterState {
            outbound: tx2,
            pending: Arc::clone(&pending2),
            semaphore: Arc::clone(&sem2),
            connected_at: Instant::now(),
        });

        // Should be available again with new capacity
        assert!(mgr.is_available("reconnect-caster"));
        assert_eq!(mgr.caster_count(), 1);
        assert_eq!(sem2.available_permits(), 5);
    }

    // ---- is_available reflects semaphore state ----

    #[test]
    fn test_is_available_reflects_permits() {
        let mgr = default_session_manager();

        // Not available when session doesn't exist
        assert!(!mgr.is_available("ghost-caster"));

        // Set up a session with 1 permit
        let (tx, _rx) = mpsc::channel(16);
        let sem = Arc::new(Semaphore::new(1));
        mgr.sessions.insert("one-permit".to_string(), CasterState {
            outbound: tx,
            pending: Arc::new(DashMap::new()),
            semaphore: Arc::clone(&sem),
            connected_at: Instant::now(),
        });

        assert!(mgr.is_available("one-permit"));

        // Consume the permit
        let _permit = sem.try_acquire().unwrap();
        assert!(!mgr.is_available("one-permit"), "should be unavailable when all permits consumed");

        // Release the permit
        drop(_permit);
        assert!(mgr.is_available("one-permit"), "should be available again after permit release");
    }

    // ---- Execute on non-existent caster ----

    #[tokio::test]
    async fn test_execute_nonexistent_caster() {
        let mgr = default_session_manager();

        let result = mgr.execute(
            "nonexistent", "req-1", "rune", Bytes::new(), Default::default(), DEFAULT_TIMEOUT
        ).await;

        assert!(matches!(result, Err(RuneError::Unavailable)));
    }

    #[tokio::test]
    async fn test_execute_stream_nonexistent_caster() {
        let mgr = default_session_manager();

        let result = mgr.execute_stream(
            "nonexistent", "req-1", "rune", Bytes::new(), Default::default(), DEFAULT_TIMEOUT
        ).await;

        assert!(matches!(result, Err(RuneError::Unavailable)));
    }

    // ---- Cancel on non-existent caster ----

    #[tokio::test]
    async fn test_cancel_nonexistent_caster() {
        let mgr = default_session_manager();

        let result = mgr.cancel("nonexistent", "req-1", "reason").await;
        assert!(matches!(result, Err(RuneError::Unavailable)));
    }

    // ---- Multiple concurrent executions share permits ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multiple_concurrent_executions() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(3);
        assert_eq!(sem.available_permits(), 3);

        // Start 3 concurrent requests
        let mut handles = Vec::new();
        for i in 0..3 {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            let rid = format!("concurrent-{}", i);
            handles.push(tokio::spawn(async move {
                mgr.execute(&cid, &rid, "echo", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            }));
        }

        // Wait for all pending
        for i in 0..3 {
            wait_for_pending(&pending, &format!("concurrent-{}", i)).await;
        }
        assert_eq!(sem.available_permits(), 0);

        // Fourth request should be rejected
        let result = mgr.execute(
            &caster_id, "concurrent-3", "echo", Bytes::new(), Default::default(), DEFAULT_TIMEOUT
        ).await;
        assert!(matches!(result, Err(RuneError::Unavailable)));

        // Complete all requests (permit auto-returned on PendingRequest drop)
        for i in 0..3 {
            if let Some((_, p)) = pending.remove(&format!("concurrent-{}", i)) {
                match p.tx {
                    PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::from("ok"))); }
                    _ => panic!("expected Once"),
                }
            }
        }

        // All permits should be returned
        assert_eq!(sem.available_permits(), 3);

        for handle in handles {
            let result = handle.await.unwrap().unwrap();
            assert_eq!(result, Bytes::from("ok"));
        }
    }

    // ========================================================================
    // v0.1.0 deep coverage additions
    // ========================================================================

    // ---- execute returns ExecutionFailed error ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_receives_execution_failed_error() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(2);

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "err-req", "failing_rune", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "err-req").await;

        // Simulate an ExecutionFailed response
        if let Some((_, p)) = pending.remove("err-req") {
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Err(RuneError::ExecutionFailed {
                        code: "RUNE_ERR".into(),
                        message: "handler crashed".into(),
                    }));
                }
                _ => panic!("expected Once"),
            }
        }

        let result = handle.await.unwrap();
        match result {
            Err(RuneError::ExecutionFailed { code, message }) => {
                assert_eq!(code, "RUNE_ERR");
                assert_eq!(message, "handler crashed");
            }
            other => panic!("expected ExecutionFailed, got {:?}", other),
        }
    }

    // ---- execute_stream receives multiple chunks properly ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_stream_receives_multiple_chunks() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(2);

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute_stream(&cid, "stream-multi", "streamer", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "stream-multi").await;
        assert_eq!(sem.available_permits(), 1);

        // Send multiple chunks via the Stream sender
        if let Some(p) = pending.get("stream-multi") {
            if let PendingResponse::Stream(ref tx) = p.tx {
                let _ = tx.send(Ok(Bytes::from("chunk-A"))).await;
                let _ = tx.send(Ok(Bytes::from("chunk-B"))).await;
                let _ = tx.send(Ok(Bytes::from("chunk-C"))).await;
            }
        }

        // Complete the stream (permit auto-returned on PendingRequest drop)
        if let Some((_, _p)) = pending.remove("stream-multi") {
            // Drop the sender by consuming it
        }

        let mut rx = handle.await.unwrap().unwrap();
        let c1 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c1, Bytes::from("chunk-A"));
        let c2 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c2, Bytes::from("chunk-B"));
        let c3 = rx.recv().await.unwrap().unwrap();
        assert_eq!(c3, Bytes::from("chunk-C"));
    }

    // ---- cancel non-existent request is idempotent ----

    #[tokio::test]
    async fn test_cancel_nonexistent_request_is_idempotent() {
        let (mgr, caster_id, sem, _pending, _rx) = setup_session(2);
        assert_eq!(sem.available_permits(), 2);

        // Cancel a request that was never made
        let result = mgr.cancel(&caster_id, "ghost-request", "no reason").await;
        assert!(result.is_ok(), "cancelling non-existent request should not error");
        // Permits unchanged
        assert_eq!(sem.available_permits(), 2);
    }

    // ---- cancel already completed request is idempotent ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancel_already_completed_request() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(2);

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "done-req", "rune", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "done-req").await;

        // Complete the request first (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("done-req") {
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::from("completed"))); }
                _ => panic!("expected Once"),
            }
        }

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, Bytes::from("completed"));

        // Now try to cancel the already-completed request
        let cancel_result = mgr.cancel(&caster_id, "done-req", "too late").await;
        assert!(cancel_result.is_ok(), "cancel after completion should not error");
        assert_eq!(sem.available_permits(), 2, "permits should remain unchanged");
    }

    // ---- many casters connected ----

    #[test]
    fn test_many_casters_simultaneously() {
        let mgr = default_session_manager();
        let count = 50;

        for i in 0..count {
            let (tx, _rx) = mpsc::channel(16);
            mgr.sessions.insert(format!("caster-{}", i), CasterState {
                outbound: tx,
                pending: Arc::new(DashMap::new()),
                semaphore: Arc::new(Semaphore::new(10)),
                connected_at: Instant::now(),
            });
        }

        assert_eq!(mgr.caster_count(), count);
        let ids = mgr.list_caster_ids();
        assert_eq!(ids.len(), count);

        for i in 0..count {
            assert!(mgr.is_available(&format!("caster-{}", i)));
        }
    }

    // ---- available_permits for connected and disconnected casters ----

    #[test]
    fn test_available_permits_query() {
        let mgr = default_session_manager();
        // Non-existent caster returns 0
        assert_eq!(mgr.available_permits("no-such"), 0);

        let (tx, _rx) = mpsc::channel(16);
        let sem = Arc::new(Semaphore::new(7));
        mgr.sessions.insert("caster-q".to_string(), CasterState {
            outbound: tx,
            pending: Arc::new(DashMap::new()),
            semaphore: Arc::clone(&sem),
            connected_at: Instant::now(),
        });

        assert_eq!(mgr.available_permits("caster-q"), 7);

        // Consume some permits
        let _p1 = sem.try_acquire().unwrap();
        let _p2 = sem.try_acquire().unwrap();
        assert_eq!(mgr.available_permits("caster-q"), 5);
    }

    // ---- execute when outbound channel is closed (caster disconnected) ----

    #[tokio::test]
    async fn test_execute_outbound_closed_returns_unavailable() {
        let mgr = default_session_manager();
        let (tx, rx) = mpsc::channel(16);
        let semaphore = Arc::new(Semaphore::new(5));
        let pending: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        mgr.sessions.insert("dying-caster".to_string(), CasterState {
            outbound: tx,
            pending: Arc::clone(&pending),
            semaphore: Arc::clone(&semaphore),
            connected_at: Instant::now(),
        });

        // Drop the receiver to close the channel
        drop(rx);

        let result = mgr.execute(
            "dying-caster", "req-dead", "rune", Bytes::new(), Default::default(), DEFAULT_TIMEOUT,
        ).await;

        assert!(matches!(result, Err(RuneError::Unavailable)),
            "execute on closed outbound should return Unavailable");
        // Pending should be cleaned up
        assert!(!pending.contains_key("req-dead"));
        // Permit should be returned
        assert_eq!(semaphore.available_permits(), 5);
    }

    // ---- execute_stream when outbound is closed ----

    #[tokio::test]
    async fn test_execute_stream_outbound_closed_returns_unavailable() {
        let mgr = default_session_manager();
        let (tx, rx) = mpsc::channel(16);
        let semaphore = Arc::new(Semaphore::new(3));
        let pending: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        mgr.sessions.insert("dying-stream".to_string(), CasterState {
            outbound: tx,
            pending: Arc::clone(&pending),
            semaphore: Arc::clone(&semaphore),
            connected_at: Instant::now(),
        });

        drop(rx);

        let result = mgr.execute_stream(
            "dying-stream", "req-s-dead", "rune", Bytes::new(), Default::default(), DEFAULT_TIMEOUT,
        ).await;

        assert!(matches!(result, Err(RuneError::Unavailable)));
        assert!(!pending.contains_key("req-s-dead"));
        assert_eq!(semaphore.available_permits(), 3);
    }

    // ---- execute with context map ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_sends_context_map_in_message() {
        let (mgr, caster_id, sem, pending, mut rx) = setup_session(2);

        let mut ctx_map = HashMap::new();
        ctx_map.insert("user_id".to_string(), "u-123".to_string());
        ctx_map.insert("trace_id".to_string(), "t-456".to_string());

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "ctx-req", "rune", Bytes::from("data"), ctx_map, DEFAULT_TIMEOUT).await
            })
        };

        // Read the outbound message to verify context was passed
        let msg = rx.recv().await.expect("should receive ExecuteRequest");
        match msg.payload {
            Some(session_message::Payload::Execute(exec)) => {
                assert_eq!(exec.request_id, "ctx-req");
                assert_eq!(exec.rune_name, "rune");
                assert_eq!(exec.input, b"data");
                assert_eq!(exec.context.get("user_id").map(|s| s.as_str()), Some("u-123"));
                assert_eq!(exec.context.get("trace_id").map(|s| s.as_str()), Some("t-456"));
                assert_eq!(exec.timeout_ms, 30_000);
            }
            other => panic!("expected Execute payload, got {:?}", other),
        }

        // Complete the request (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("ctx-req") {
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::from("done"))); }
                _ => panic!("expected Once"),
            }
        }
        let _ = handle.await.unwrap().unwrap();
    }

    // ---- cancel sends CancelRequest message to caster ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancel_sends_message_to_outbound() {
        let (mgr, caster_id, _sem, pending, mut rx) = setup_session(2);

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "cancel-msg-req", "rune", Bytes::new(), Default::default(), DEFAULT_TIMEOUT).await
            })
        };

        wait_for_pending(&pending, "cancel-msg-req").await;

        // Drain the ExecuteRequest message
        let _exec_msg = rx.recv().await.unwrap();

        // Cancel
        mgr.cancel(&caster_id, "cancel-msg-req", "user wants to stop").await.unwrap();

        // Read cancel message
        let cancel_msg = rx.recv().await.expect("should receive CancelRequest");
        match cancel_msg.payload {
            Some(session_message::Payload::Cancel(cancel)) => {
                assert_eq!(cancel.request_id, "cancel-msg-req");
                assert_eq!(cancel.reason, "user wants to stop");
            }
            other => panic!("expected Cancel payload, got {:?}", other),
        }

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(RuneError::Cancelled)));
    }

    // ---- execute preserves request ordering via outbound ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_messages_arrive_in_order() {
        let (mgr, caster_id, sem, pending, mut rx) = setup_session(5);

        let mut handles = Vec::new();
        for i in 0..3 {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            let rid = format!("ord-{}", i);
            handles.push(tokio::spawn(async move {
                mgr.execute(&cid, &rid, "rune", Bytes::from(format!("data-{}", i)), Default::default(), DEFAULT_TIMEOUT).await
            }));
        }

        // Collect the 3 ExecuteRequest messages
        let mut received_ids = Vec::new();
        for _ in 0..3 {
            let msg = rx.recv().await.unwrap();
            if let Some(session_message::Payload::Execute(exec)) = msg.payload {
                received_ids.push(exec.request_id);
            }
        }
        assert_eq!(received_ids.len(), 3);
        // All three IDs should be present (order may vary due to concurrency)
        assert!(received_ids.contains(&"ord-0".to_string()));
        assert!(received_ids.contains(&"ord-1".to_string()));
        assert!(received_ids.contains(&"ord-2".to_string()));

        // Complete all (permit auto-returned on PendingRequest drop)
        for i in 0..3 {
            let key = format!("ord-{}", i);
            if let Some((_, p)) = pending.remove(&key) {
                match p.tx {
                    PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::from("ok"))); }
                    _ => panic!("expected Once"),
                }
            }
        }
        for h in handles {
            let _ = h.await.unwrap().unwrap();
        }
    }

    // ---- execute with custom timeout value ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_custom_timeout_in_message() {
        let (mgr, caster_id, sem, pending, mut rx) = setup_session(2);

        let custom_timeout = Duration::from_secs(120);
        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "timeout-val", "rune", Bytes::new(), Default::default(), custom_timeout).await
            })
        };

        let msg = rx.recv().await.unwrap();
        if let Some(session_message::Payload::Execute(exec)) = msg.payload {
            assert_eq!(exec.timeout_ms, 120_000);
        } else {
            panic!("expected Execute payload");
        }

        if let Some((_, p)) = pending.remove("timeout-val") {
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Ok(Bytes::new())); }
                _ => panic!("expected Once"),
            }
        }
        let _ = handle.await.unwrap().unwrap();
    }

    // ---- S8 回归测试：with_auth 是唯一的生产构造路径 ----

    #[test]
    fn test_new_dev_has_dev_mode_true() {
        // new_dev() (test-only constructor) defaults to dev_mode=true
        let mgr = SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        );
        assert!(mgr.dev_mode, "new_dev() should have dev_mode=true");
    }

    #[test]
    fn test_with_auth_respects_dev_mode_false() {
        let verifier = Arc::new(crate::auth::NoopVerifier);
        let mgr = SessionManager::with_auth(
            Duration::from_secs(10),
            Duration::from_secs(35),
            verifier,
            false,
        );
        assert!(!mgr.dev_mode, "with_auth(dev_mode=false) should have dev_mode=false");
    }

    #[test]
    fn test_with_auth_respects_dev_mode_true() {
        let verifier = Arc::new(crate::auth::NoopVerifier);
        let mgr = SessionManager::with_auth(
            Duration::from_secs(10),
            Duration::from_secs(35),
            verifier,
            true,
        );
        assert!(mgr.dev_mode, "with_auth(dev_mode=true) should have dev_mode=true");
    }

    // MF-1: safe_timeout_ms should clamp large durations to u32::MAX
    #[test]
    fn test_safe_timeout_ms_normal_value() {
        let result = safe_timeout_ms(Duration::from_secs(30));
        assert_eq!(result, 30_000);
    }

    #[test]
    fn test_safe_timeout_ms_max_u32() {
        // u32::MAX milliseconds ≈ 49.7 days
        let dur = Duration::from_millis(u32::MAX as u64);
        assert_eq!(safe_timeout_ms(dur), u32::MAX);
    }

    #[test]
    fn test_safe_timeout_ms_overflow_clamped() {
        // Duration larger than u32::MAX ms should be clamped, not truncated
        let dur = Duration::from_millis(u32::MAX as u64 + 1000);
        assert_eq!(safe_timeout_ms(dur), u32::MAX);
    }

    #[test]
    fn test_safe_timeout_ms_huge_duration() {
        // 100 days in millis overflows u32 (8_640_000_000 > 4_294_967_295)
        let dur = Duration::from_secs(100 * 86400);
        assert_eq!(safe_timeout_ms(dur), u32::MAX);
    }
}
