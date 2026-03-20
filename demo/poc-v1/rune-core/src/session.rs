use std::sync::Arc;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tonic::Streaming;
use rune_proto::*;
use crate::rune::{RuneConfig, RuneError};
use crate::relay::Relay;
use crate::invoker::RemoteInvoker;

/// 每个 Caster 的会话状态
struct CasterState {
    outbound: mpsc::Sender<SessionMessage>,
    pending: Arc<DashMap<String, oneshot::Sender<Result<Bytes, RuneError>>>>,
}

/// 管理所有 Caster 的 gRPC 连接
pub struct SessionManager {
    sessions: DashMap<String, CasterState>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self { sessions: DashMap::new() }
    }

    /// 处理一个新 Caster 连接（gRPC 双向流）
    pub async fn handle_session(
        self: &Arc<Self>,
        relay: Arc<Relay>,
        mut inbound: Streaming<SessionMessage>,
        outbound_tx: mpsc::Sender<SessionMessage>,
    ) {
        let mut caster_id: Option<String> = None;
        // pending 是 per-session 的，不是全局的
        let pending: Arc<DashMap<String, oneshot::Sender<Result<Bytes, RuneError>>>> =
            Arc::new(DashMap::new());

        while let Ok(Some(msg)) = inbound.message().await {
            match msg.payload {
                Some(session_message::Payload::Attach(attach)) => {
                    let id = attach.caster_id.clone();
                    tracing::info!(caster_id = %id, runes = attach.runes.len(), "caster attached");

                    self.sessions.insert(id.clone(), CasterState {
                        outbound: outbound_tx.clone(),
                        pending: Arc::clone(&pending),
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
                            accepted: true,
                            reason: String::new(),
                        })),
                    };
                    let _ = outbound_tx.send(ack).await;
                    caster_id = Some(id);
                }

                Some(session_message::Payload::Result(result)) => {
                    let req_id = result.request_id.clone();
                    tracing::debug!(request_id = %req_id, "received result from caster");

                    if let Some((_, sender)) = pending.remove(&req_id) {
                        let outcome = match result.status() {
                            Status::Completed => Ok(Bytes::from(result.output)),
                            _ => Err(RuneError::ExecutionFailed {
                                code: result.error.as_ref().map(|e| e.code.clone()).unwrap_or_default(),
                                message: result.error.as_ref().map(|e| e.message.clone()).unwrap_or_default(),
                            }),
                        };
                        let _ = sender.send(outcome);
                    }
                }

                Some(session_message::Payload::Detach(detach)) => {
                    tracing::info!(reason = %detach.reason, "caster detached");
                    break;
                }

                Some(session_message::Payload::Heartbeat(_)) => {}

                _ => {}
            }
        }

        // 断线清理：只清理本 session 的 pending
        if let Some(id) = &caster_id {
            tracing::info!(caster_id = %id, "caster disconnected, cleaning up");
            self.sessions.remove(id);
            relay.remove_caster(id);
            let keys: Vec<String> = pending.iter().map(|e| e.key().clone()).collect();
            for req_id in keys {
                if let Some((_, sender)) = pending.remove(&req_id) {
                    let _ = sender.send(Err(RuneError::Internal(
                        anyhow::anyhow!("caster '{}' disconnected", id),
                    )));
                }
            }
        }
    }

    /// 向 Caster 发送执行请求并等待结果
    pub async fn execute(
        &self,
        caster_id: &str,
        request_id: &str,
        rune_name: &str,
        input: Bytes,
    ) -> Result<Bytes, RuneError> {
        let session = self.sessions.get(caster_id)
            .ok_or(RuneError::Unavailable)?;

        let (tx, rx) = oneshot::channel();
        session.pending.insert(request_id.to_string(), tx);

        let msg = SessionMessage {
            payload: Some(session_message::Payload::Execute(ExecuteRequest {
                request_id: request_id.to_string(),
                rune_name: rune_name.to_string(),
                input: input.to_vec(),
                context: Default::default(),
            })),
        };

        if session.outbound.send(msg).await.is_err() {
            session.pending.remove(request_id);
            return Err(RuneError::Unavailable);
        }

        drop(session); // 释放 DashMap ref
        rx.await.map_err(|_| RuneError::Internal(anyhow::anyhow!("response channel dropped")))?
    }
}
