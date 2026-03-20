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

enum PendingResponse {
    Once(oneshot::Sender<Result<Bytes, RuneError>>),
    Stream(mpsc::Sender<Result<Bytes, RuneError>>),
}

struct PendingRequest {
    tx: PendingResponse,
    created_at: Instant,
    timeout: Duration,
}

pub struct SessionManager {
    sessions: DashMap<String, CasterState>,
}

struct CasterState {
    outbound: mpsc::Sender<SessionMessage>,
    pending: Arc<DashMap<String, PendingRequest>>,
    semaphore: Arc<Semaphore>,
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
