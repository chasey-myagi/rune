use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, Semaphore, TryAcquireError};
use tokio::time;
use tonic::Streaming;
use rune_proto::*;
use crate::rune::{RuneConfig, RuneError};
use crate::relay::Relay;
use crate::invoker::RemoteInvoker;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(35);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

struct PendingRequest {
    tx: oneshot::Sender<Result<Bytes, RuneError>>,
    created_at: Instant,
    timeout: Duration,
}

pub struct SessionManager {
    sessions: DashMap<String, CasterState>,
}

struct CasterState {
    outbound: mpsc::Sender<SessionMessage>,
    pending: Arc<DashMap<String, PendingRequest>>,
    semaphore: Arc<Semaphore>,  // 替代 AtomicU32，原子性容量控制
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
        // 共享 semaphore——初始 0，Attach 时通过 add_permits 设置容量
        // timeout handler 和 execute() 使用同一个实例
        let semaphore = Arc::new(Semaphore::new(0));

        // 后台：心跳发送 + 超时检测
        let hb_tx = outbound_tx.clone();
        let hb_last = Arc::clone(&last_heartbeat_ms);
        let hb_handle = tokio::spawn(async move {
            loop {
                time::sleep(HEARTBEAT_INTERVAL).await;
                let msg = SessionMessage {
                    payload: Some(session_message::Payload::Heartbeat(Heartbeat {
                        timestamp_ms: now_ms(),
                    })),
                };
                if hb_tx.send(msg).await.is_err() { break; }
                let last = hb_last.load(Ordering::Relaxed);
                let elapsed = now_ms().saturating_sub(last);
                if elapsed > HEARTBEAT_TIMEOUT.as_millis() as u64 {
                    tracing::warn!(elapsed_ms = elapsed, "heartbeat timeout");
                    break;
                }
            }
        });

        // 后台：每秒检查超时请求
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
                        timeout_sem.add_permits(1); // 释放许可
                        let _ = p.tx.send(Err(RuneError::Timeout));
                    }
                }
            }
        });

        // 消息处理主循环
        while let Ok(Some(msg)) = inbound.message().await {
            match msg.payload {
                Some(session_message::Payload::Attach(attach)) => {
                    let id = attach.caster_id.clone();
                    let max_conc = attach.max_concurrent;
                    tracing::info!(caster_id = %id, runes = attach.runes.len(), max_concurrent = max_conc, "caster attached");

                    // 设置共享 Semaphore 容量
                    let permits = if max_conc > 0 { max_conc as usize } else { usize::MAX >> 1 };
                    semaphore.add_permits(permits);

                    self.sessions.insert(id.clone(), CasterState {
                        outbound: outbound_tx.clone(),
                        pending: Arc::clone(&pending),
                        semaphore: Arc::clone(&semaphore), // 同一个实例
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
                    if let Some(session) = caster_id.as_ref().and_then(|id| self.sessions.get(id)) {
                        session.semaphore.add_permits(1); // 释放许可
                    }
                    if let Some((_, p)) = pending.remove(&req_id) {
                        let outcome = match result.status() {
                            Status::Completed => Ok(Bytes::from(result.output)),
                            _ => Err(RuneError::ExecutionFailed {
                                code: result.error.as_ref().map(|e| e.code.clone()).unwrap_or_default(),
                                message: result.error.as_ref().map(|e| e.message.clone()).unwrap_or_default(),
                            }),
                        };
                        let _ = p.tx.send(outcome);
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

        // 断线清理
        hb_handle.abort();
        timeout_handle.abort();
        if let Some(id) = &caster_id {
            tracing::info!(caster_id = %id, "cleaning up disconnected caster");
            self.sessions.remove(id);
            relay.remove_caster(id);
            let keys: Vec<String> = pending.iter().map(|e| e.key().clone()).collect();
            for req_id in keys {
                if let Some((_, p)) = pending.remove(&req_id) {
                    let _ = p.tx.send(Err(RuneError::Internal(
                        anyhow::anyhow!("caster '{}' disconnected", id),
                    )));
                }
            }
        }
    }

    /// 发送执行请求（Semaphore 保证原子性容量控制）
    pub async fn execute(
        &self,
        caster_id: &str,
        request_id: &str,
        rune_name: &str,
        input: Bytes,
    ) -> Result<Bytes, RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;

        // 原子性获取许可——无 TOCTOU
        let _permit = session.semaphore.try_acquire()
            .map_err(|_| RuneError::Unavailable)?;

        // permit 会在 _permit drop 时自动释放——但我们需要在 Result 到达时释放，
        // 所以这里 forget permit，手动在 Result handler 里 add_permits(1)
        std::mem::forget(_permit);

        let (tx, rx) = oneshot::channel();
        session.pending.insert(request_id.to_string(), PendingRequest {
            tx,
            created_at: Instant::now(),
            timeout: DEFAULT_TIMEOUT,
        });

        let msg = SessionMessage {
            payload: Some(session_message::Payload::Execute(ExecuteRequest {
                request_id: request_id.to_string(),
                rune_name: rune_name.to_string(),
                input: input.to_vec(),
                context: Default::default(),
                timeout_ms: DEFAULT_TIMEOUT.as_millis() as u32,
            })),
        };

        if session.outbound.send(msg).await.is_err() {
            session.pending.remove(request_id);
            session.semaphore.add_permits(1); // 发送失败，归还许可
            return Err(RuneError::Unavailable);
        }

        drop(session);
        rx.await.map_err(|_| RuneError::Internal(anyhow::anyhow!("response channel dropped")))?
    }

    pub async fn cancel(&self, caster_id: &str, request_id: &str, reason: &str) -> Result<(), RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
        if let Some((_, p)) = session.pending.remove(request_id) {
            session.semaphore.add_permits(1); // 归还许可
            let _ = p.tx.send(Err(RuneError::ExecutionFailed {
                code: "CANCELLED".into(),
                message: reason.to_string(),
            }));
        }
        let msg = SessionMessage {
            payload: Some(session_message::Payload::Cancel(CancelRequest {
                request_id: request_id.to_string(),
                reason: reason.to_string(),
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
