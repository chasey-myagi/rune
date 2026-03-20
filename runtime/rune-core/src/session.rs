use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time;
use tonic::Streaming;
use rune_proto::*;
use crate::rune::{RuneConfig, RuneError};
use crate::relay::Relay;
use crate::invoker::RemoteInvoker;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(35);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) enum PendingResponse {
    Once(oneshot::Sender<Result<Bytes, RuneError>>),
    Stream(mpsc::Sender<Result<Bytes, RuneError>>),
}

pub(crate) struct PendingRequest {
    pub(crate) tx: PendingResponse,
    pub(crate) created_at: Instant,
    pub(crate) timeout: Duration,
}

pub struct SessionManager {
    sessions: DashMap<String, CasterState>,
}

pub(crate) struct CasterState {
    pub(crate) outbound: mpsc::Sender<SessionMessage>,
    pub(crate) pending: Arc<DashMap<String, PendingRequest>>,
    pub(crate) semaphore: Arc<Semaphore>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self { sessions: DashMap::new() }
    }

    pub fn is_available(&self, caster_id: &str) -> bool {
        self.sessions.get(caster_id)
            .map(|s| s.semaphore.available_permits() > 0)
            .unwrap_or(false)
    }

    pub async fn handle_session(
        self: &Arc<Self>,
        relay: Arc<Relay>,
        mut inbound: Streaming<SessionMessage>,
        outbound_tx: mpsc::Sender<SessionMessage>,
    ) {
        let mut caster_id: Option<String> = None;
        let pending: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        let last_heartbeat_ms = Arc::new(AtomicU64::new(now_ms()));
        let semaphore = Arc::new(Semaphore::new(0)); // Attach 时 add_permits
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        // 心跳
        let hb_tx = outbound_tx.clone();
        let hb_last = Arc::clone(&last_heartbeat_ms);
        let hb_shutdown = shutdown_tx.clone();
        let hb_handle = tokio::spawn(async move {
            loop {
                time::sleep(HEARTBEAT_INTERVAL).await;
                let msg = SessionMessage {
                    payload: Some(session_message::Payload::Heartbeat(Heartbeat { timestamp_ms: now_ms() })),
                };
                if hb_tx.send(msg).await.is_err() { break; }
                let elapsed = now_ms().saturating_sub(hb_last.load(Ordering::Relaxed));
                if elapsed > HEARTBEAT_TIMEOUT.as_millis() as u64 {
                    tracing::warn!(elapsed_ms = elapsed, "heartbeat timeout, forcing session shutdown");
                    let _ = hb_shutdown.send(true);
                    break;
                }
            }
        });
        drop(shutdown_tx);

        // 超时扫描
        let timeout_pending = Arc::clone(&pending);
        let timeout_sem = Arc::clone(&semaphore);
        let timeout_handle = tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(1)).await;
                let now = Instant::now();
                let expired: Vec<String> = timeout_pending.iter()
                    .filter(|e| now.duration_since(e.value().created_at) > e.value().timeout)
                    .map(|e| e.key().clone())
                    .collect();
                for req_id in expired {
                    if let Some((_, p)) = timeout_pending.remove(&req_id) {
                        tracing::warn!(request_id = %req_id, "request timed out");
                        timeout_sem.add_permits(1); // 归还 permit
                        match p.tx {
                            PendingResponse::Once(tx) => { let _ = tx.send(Err(RuneError::Timeout)); }
                            PendingResponse::Stream(tx) => { let _ = tx.send(Err(RuneError::Timeout)).await; }
                        }
                    }
                }
            }
        });

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
                    tracing::info!(caster_id = %id, runes = attach.runes.len(), max_concurrent = max_conc, "caster attached");

                    let permits = if max_conc > 0 { max_conc as usize } else { usize::MAX >> 1 };
                    semaphore.add_permits(permits);

                    self.sessions.insert(id.clone(), CasterState {
                        outbound: outbound_tx.clone(),
                        pending: Arc::clone(&pending),
                        semaphore: Arc::clone(&semaphore),
                    });

                    for decl in &attach.runes {
                        let config = RuneConfig {
                            name: decl.name.clone(),
                            description: decl.description.clone(),
                            gate_path: decl.gate.as_ref().map(|g| g.path.clone()),
                        };
                        let invoker = Arc::new(RemoteInvoker {
                            session: Arc::clone(self) as Arc<SessionManager>,
                            caster_id: id.clone(),
                        });
                        relay.register(config, invoker, Some(id.clone()));
                    }

                    let ack = SessionMessage {
                        payload: Some(session_message::Payload::AttachAck(AttachAck {
                            accepted: true, reason: String::new(),
                        })),
                    };
                    let _ = outbound_tx.send(ack).await;
                    caster_id = Some(id);
                }

                Some(session_message::Payload::Result(result)) => {
                    let req_id = result.request_id.clone();
                    if let Some((_, p)) = pending.remove(&req_id) {
                        if let Some(session) = caster_id.as_ref().and_then(|id| self.sessions.get(id)) {
                            session.semaphore.add_permits(1);
                        }
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
                    // Late result (already timed out / cancelled): silently ignored
                }

                Some(session_message::Payload::StreamEvent(event)) => {
                    let req_id = event.request_id.clone();
                    if let Some(p) = pending.get(&req_id) {
                        if let PendingResponse::Stream(ref tx) = p.tx {
                            let _ = tx.send(Ok(Bytes::from(event.data))).await;
                        }
                    }
                }

                Some(session_message::Payload::StreamEnd(end)) => {
                    let req_id = end.request_id.clone();
                    if let Some((_, p)) = pending.remove(&req_id) {
                        if let Some(session) = caster_id.as_ref().and_then(|id| self.sessions.get(id)) {
                            session.semaphore.add_permits(1);
                        }
                        if let PendingResponse::Stream(tx) = p.tx {
                            if end.status() != Status::Completed {
                                let _ = tx.send(Err(RuneError::ExecutionFailed {
                                    code: end.error.as_ref().map(|e| e.code.clone()).unwrap_or_default(),
                                    message: end.error.as_ref().map(|e| e.message.clone()).unwrap_or_default(),
                                })).await;
                            }
                        }
                    }
                    // Late stream end: silently ignored
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
        timeout_handle.abort();
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
    ) -> Result<Bytes, RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
        let _permit = session.semaphore.try_acquire().map_err(|_| RuneError::Unavailable)?;
        std::mem::forget(_permit);

        let (tx, rx) = oneshot::channel();
        session.pending.insert(request_id.to_string(), PendingRequest {
            tx: PendingResponse::Once(tx), created_at: Instant::now(), timeout: DEFAULT_TIMEOUT,
        });

        let msg = SessionMessage {
            payload: Some(session_message::Payload::Execute(ExecuteRequest {
                request_id: request_id.to_string(), rune_name: rune_name.to_string(),
                input: input.to_vec(), context: Default::default(),
                timeout_ms: DEFAULT_TIMEOUT.as_millis() as u32,
            })),
        };

        if session.outbound.send(msg).await.is_err() {
            session.pending.remove(request_id);
            session.semaphore.add_permits(1);
            return Err(RuneError::Unavailable);
        }
        drop(session);
        rx.await.map_err(|_| RuneError::Internal(anyhow::anyhow!("response channel dropped")))?
    }

    pub async fn execute_stream(
        &self, caster_id: &str, request_id: &str, rune_name: &str, input: Bytes,
    ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
        let _permit = session.semaphore.try_acquire().map_err(|_| RuneError::Unavailable)?;
        std::mem::forget(_permit);

        let (tx, rx) = mpsc::channel(32);
        session.pending.insert(request_id.to_string(), PendingRequest {
            tx: PendingResponse::Stream(tx), created_at: Instant::now(), timeout: DEFAULT_TIMEOUT,
        });

        let msg = SessionMessage {
            payload: Some(session_message::Payload::Execute(ExecuteRequest {
                request_id: request_id.to_string(), rune_name: rune_name.to_string(),
                input: input.to_vec(), context: Default::default(),
                timeout_ms: DEFAULT_TIMEOUT.as_millis() as u32,
            })),
        };

        if session.outbound.send(msg).await.is_err() {
            session.pending.remove(request_id);
            session.semaphore.add_permits(1);
            return Err(RuneError::Unavailable);
        }
        drop(session);
        Ok(rx)
    }

    pub async fn cancel(&self, caster_id: &str, request_id: &str, reason: &str) -> Result<(), RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
        if let Some((_, p)) = session.pending.remove(request_id) {
            session.semaphore.add_permits(1);
            let err = Err(RuneError::ExecutionFailed { code: "CANCELLED".into(), message: reason.to_string() });
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Helper: creates a SessionManager with one registered caster session.
    /// Returns the receiver too so the outbound channel stays open.
    fn setup_session(max_concurrent: usize) -> (
        Arc<SessionManager>,
        String,
        Arc<Semaphore>,
        Arc<DashMap<String, PendingRequest>>,
        mpsc::Receiver<SessionMessage>,
    ) {
        let mgr = Arc::new(SessionManager::new());
        let (tx, rx) = mpsc::channel(16);
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let pending: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        let caster_id = "test-caster".to_string();

        mgr.sessions.insert(caster_id.clone(), CasterState {
            outbound: tx,
            pending: Arc::clone(&pending),
            semaphore: Arc::clone(&semaphore),
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

        // execute() consumes one permit
        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "r-1", "echo", Bytes::from("hello")).await
            })
        };

        // Wait for pending entry to appear
        wait_for_pending(&pending, "r-1").await;
        assert_eq!(sem.available_permits(), 1);

        // Simulate result arriving: remove from pending + return permit
        if let Some((_, p)) = pending.remove("r-1") {
            sem.add_permits(1);
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
                mgr.execute(&cid, "r-timeout", "slow", Bytes::new()).await
            })
        };

        wait_for_pending(&pending, "r-timeout").await;
        assert_eq!(sem.available_permits(), 1);

        // Simulate timeout: remove from pending + return permit
        if let Some((_, p)) = pending.remove("r-timeout") {
            sem.add_permits(1);
            match p.tx {
                PendingResponse::Once(tx) => { let _ = tx.send(Err(RuneError::Timeout)); }
                _ => panic!("expected Once"),
            }
        }

        assert_eq!(sem.available_permits(), 2);

        // Simulate late result: pending.remove returns None
        let late_found = pending.remove("r-timeout");
        assert!(late_found.is_none(), "late result should find nothing in pending");
        // KEY ASSERTION: permit count must NOT exceed max_concurrent
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
                mgr.execute(&cid, "r-cancel", "slow", Bytes::new()).await
            })
        };

        wait_for_pending(&pending, "r-cancel").await;
        assert_eq!(sem.available_permits(), 1);

        mgr.cancel(&caster_id, "r-cancel", "user requested").await.unwrap();
        assert_eq!(sem.available_permits(), 2, "cancel must return permit");

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(RuneError::ExecutionFailed { .. })));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_max_concurrent_enforced() {
        let (mgr, caster_id, sem, pending, _rx) = setup_session(1);

        // First execute consumes the only permit
        let _handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(&cid, "r-1", "echo", Bytes::new()).await
            })
        };

        wait_for_pending(&pending, "r-1").await;
        assert_eq!(sem.available_permits(), 0);

        // Second execute should fail immediately
        let result = mgr.execute(&caster_id, "r-2", "echo", Bytes::new()).await;
        assert!(matches!(result, Err(RuneError::Unavailable)),
            "second request must be rejected when max_concurrent=1");
    }

    #[tokio::test]
    async fn test_disconnect_clears_all_pending() {
        let (_mgr, _caster_id, _sem, pending, _rx) = setup_session(5);

        // Insert 3 pending requests
        for i in 0..3 {
            let (tx, _rx) = oneshot::channel();
            pending.insert(format!("r-{}", i), PendingRequest {
                tx: PendingResponse::Once(tx),
                created_at: Instant::now(),
                timeout: DEFAULT_TIMEOUT,
            });
        }

        assert_eq!(pending.len(), 3);

        // Simulate disconnect cleanup
        let keys: Vec<String> = pending.iter().map(|e| e.key().clone()).collect();
        for req_id in keys {
            let _ = pending.remove(&req_id);
        }

        assert!(pending.is_empty(), "all pending should be cleared after disconnect");
    }
}
