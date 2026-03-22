//! Caster — connects to Rune runtime and executes registered handlers.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use rune_proto::rune_service_client::RuneServiceClient;
use rune_proto::{
    session_message::Payload, CasterAttach, ErrorDetail, ExecuteResult,
    GateConfig as ProtoGateConfig, Heartbeat, RuneDeclaration, SessionMessage, StreamEnd,
    StreamEvent,
};

use crate::config::{CasterConfig, FileAttachment, RuneConfig};
use crate::context::RuneContext;
use crate::error::{SdkError, SdkResult};
use crate::handler::{BoxFuture, HandlerKind, RegisteredRune};
use crate::stream::StreamSender;

/// Caster connects to a Rune Runtime and registers Rune handlers.
pub struct Caster {
    config: CasterConfig,
    caster_id: String,
    runes: Arc<RwLock<HashMap<String, RegisteredRune>>>,
}

impl Caster {
    /// Create a new Caster with the given configuration.
    pub fn new(config: CasterConfig) -> Self {
        let caster_id = config
            .caster_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        Self {
            config,
            caster_id,
            runes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the caster ID.
    pub fn caster_id(&self) -> &str {
        &self.caster_id
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &CasterConfig {
        &self.config
    }

    /// Returns the number of registered runes.
    pub fn rune_count(&self) -> usize {
        self.runes.read().unwrap().len()
    }

    /// Returns the config of a registered rune by name.
    pub fn get_rune_config(&self, name: &str) -> Option<RuneConfig> {
        self.runes.read().unwrap().get(name).map(|r| r.config.clone())
    }

    /// Check if a rune is registered as a stream handler.
    pub fn is_stream_rune(&self, name: &str) -> bool {
        self.runes
            .read()
            .unwrap()
            .get(name)
            .map(|r| r.handler.is_stream())
            .unwrap_or(false)
    }

    /// Check if a rune handler accepts file attachments.
    pub fn rune_accepts_files(&self, name: &str) -> bool {
        self.runes
            .read()
            .unwrap()
            .get(name)
            .map(|r| r.handler.accepts_files())
            .unwrap_or(false)
    }

    // -----------------------------------------------------------------------
    // Registration
    // -----------------------------------------------------------------------

    /// Register a unary rune handler.
    ///
    /// The handler receives `(RuneContext, Bytes)` and returns `Result<Bytes>`.
    ///
    /// # Errors
    /// Returns `SdkError::DuplicateRune` if a rune with the same name already exists.
    pub fn rune<F, Fut>(&self, config: RuneConfig, handler: F) -> SdkResult<()>
    where
        F: Fn(RuneContext, Bytes) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = SdkResult<Bytes>> + Send + 'static,
    {
        let handler = Arc::new(move |ctx, input| -> BoxFuture<'static, SdkResult<Bytes>> {
            Box::pin(handler(ctx, input))
        });
        self.register_inner(config, HandlerKind::Unary(handler))
    }

    /// Register a unary rune handler that accepts file attachments.
    pub fn rune_with_files<F, Fut>(&self, config: RuneConfig, handler: F) -> SdkResult<()>
    where
        F: Fn(RuneContext, Bytes, Vec<FileAttachment>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = SdkResult<Bytes>> + Send + 'static,
    {
        let handler =
            Arc::new(
                move |ctx, input, files| -> BoxFuture<'static, SdkResult<Bytes>> {
                    Box::pin(handler(ctx, input, files))
                },
            );
        self.register_inner(config, HandlerKind::UnaryWithFiles(handler))
    }

    /// Register a streaming rune handler.
    ///
    /// The handler receives `(RuneContext, Bytes, StreamSender)` and returns `Result<()>`.
    pub fn stream_rune<F, Fut>(&self, config: RuneConfig, handler: F) -> SdkResult<()>
    where
        F: Fn(RuneContext, Bytes, StreamSender) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = SdkResult<()>> + Send + 'static,
    {
        let handler =
            Arc::new(move |ctx, input, stream| -> BoxFuture<'static, SdkResult<()>> {
                Box::pin(handler(ctx, input, stream))
            });
        let mut cfg = config;
        cfg.supports_stream = true;
        self.register_inner(cfg, HandlerKind::Stream(handler))
    }

    /// Register a streaming rune handler that accepts file attachments.
    pub fn stream_rune_with_files<F, Fut>(&self, config: RuneConfig, handler: F) -> SdkResult<()>
    where
        F: Fn(RuneContext, Bytes, Vec<FileAttachment>, StreamSender) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: std::future::Future<Output = SdkResult<()>> + Send + 'static,
    {
        let handler =
            Arc::new(
                move |ctx, input, files, stream| -> BoxFuture<'static, SdkResult<()>> {
                    Box::pin(handler(ctx, input, files, stream))
                },
            );
        let mut cfg = config;
        cfg.supports_stream = true;
        self.register_inner(cfg, HandlerKind::StreamWithFiles(handler))
    }

    fn register_inner(&self, config: RuneConfig, handler: HandlerKind) -> SdkResult<()> {
        let name = config.name.clone();
        let registered = RegisteredRune { config, handler };
        let mut runes = self.runes.write().unwrap();
        if runes.contains_key(&name) {
            return Err(SdkError::DuplicateRune(name));
        }
        runes.insert(name, registered);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Run
    // -----------------------------------------------------------------------

    /// Start the Caster (blocking async). Connects to Runtime with auto-reconnect.
    pub async fn run(&self) -> SdkResult<()> {
        let mut delay = Duration::from_secs_f64(self.config.reconnect_base_delay_secs);
        let max_delay = Duration::from_secs_f64(self.config.reconnect_max_delay_secs);

        loop {
            match self.connect_and_run().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(
                        "connection error: {}, reconnecting in {:?}",
                        e,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                }
            }
        }
    }

    async fn connect_and_run(&self) -> SdkResult<()> {
        let endpoint = format!("http://{}", self.config.runtime);
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| SdkError::InvalidUri(e.to_string()))?
            .connect()
            .await?;
        let mut client = RuneServiceClient::new(channel);

        // Outbound channel
        let (tx, rx) = mpsc::channel::<SessionMessage>(32);
        let outbound = ReceiverStream::new(rx);
        let response = client.session(outbound).await?;
        let mut inbound = response.into_inner();

        // Send CasterAttach
        let attach_msg = self.build_attach_message();
        tx.send(attach_msg)
            .await
            .map_err(|e| SdkError::ChannelSend(e.to_string()))?;

        // Start heartbeat
        let hb_tx = tx.clone();
        let hb_interval = Duration::from_secs_f64(self.config.heartbeat_interval_secs);
        let hb_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(hb_interval).await;
                let msg = SessionMessage {
                    payload: Some(Payload::Heartbeat(Heartbeat {
                        timestamp_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    })),
                };
                if hb_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Cancellation tokens per request (use tokio::sync for async spawned tasks)
        let cancel_tokens: Arc<tokio::sync::RwLock<HashMap<String, CancellationToken>>> =
            Arc::new(tokio::sync::RwLock::new(HashMap::new()));

        // Message loop
        while let Some(msg) = inbound.message().await? {
            match msg.payload {
                Some(Payload::AttachAck(ack)) => {
                    if ack.accepted {
                        tracing::info!(
                            "attached to {}, caster_id={}",
                            self.config.runtime,
                            self.caster_id
                        );
                    } else {
                        tracing::error!("attach rejected: {}", ack.reason);
                        break;
                    }
                }
                Some(Payload::Execute(req)) => {
                    let registered = self.runes.read().unwrap().get(&req.rune_name).cloned();

                    let token = CancellationToken::new();
                    cancel_tokens
                        .write()
                        .await
                        .insert(req.request_id.clone(), token.clone());

                    let tx_clone = tx.clone();
                    let cancel_tokens_clone = cancel_tokens.clone();
                    let request_id = req.request_id.clone();
                    tokio::spawn(async move {
                        execute_handler(registered, req, tx_clone, token).await;
                        cancel_tokens_clone.write().await.remove(&request_id);
                    });
                }
                Some(Payload::Cancel(cancel)) => {
                    if let Some(token) =
                        cancel_tokens.read().await.get(&cancel.request_id)
                    {
                        token.cancel();
                    }
                    tracing::info!("cancel requested: {}", cancel.request_id);
                }
                Some(Payload::Heartbeat(_)) => {
                    // Server heartbeat — acknowledged silently
                }
                _ => {}
            }
        }

        hb_handle.abort();
        Ok(())
    }

    fn build_attach_message(&self) -> SessionMessage {
        let runes = self.runes.read().unwrap();
        let mut declarations = Vec::new();

        for registered in runes.values() {
            let cfg = &registered.config;
            let gate = cfg.gate.as_ref().map(|g| ProtoGateConfig {
                path: g.path.clone(),
                method: g.method.clone(),
            });
            let input_schema = cfg
                .input_schema
                .as_ref()
                .map(|s| serde_json::to_string(s).unwrap_or_default())
                .unwrap_or_default();
            let output_schema = cfg
                .output_schema
                .as_ref()
                .map(|s| serde_json::to_string(s).unwrap_or_default())
                .unwrap_or_default();

            declarations.push(RuneDeclaration {
                name: cfg.name.clone(),
                version: cfg.version.clone(),
                description: cfg.description.clone(),
                input_schema,
                output_schema,
                supports_stream: cfg.supports_stream,
                gate,
                priority: cfg.priority,
            });
        }

        SessionMessage {
            payload: Some(Payload::Attach(CasterAttach {
                caster_id: self.caster_id.clone(),
                runes: declarations,
                labels: self.config.labels.clone(),
                max_concurrent: self.config.max_concurrent,
            })),
        }
    }
}

// ---------------------------------------------------------------------------
// Execute dispatch (free function)
// ---------------------------------------------------------------------------

async fn execute_handler(
    registered: Option<RegisteredRune>,
    req: rune_proto::ExecuteRequest,
    tx: mpsc::Sender<SessionMessage>,
    cancel_token: CancellationToken,
) {
    let request_id = req.request_id.clone();

    let Some(registered) = registered else {
        let _ = tx
            .send(SessionMessage {
                payload: Some(Payload::Result(ExecuteResult {
                    request_id,
                    status: rune_proto::Status::Failed.into(),
                    output: vec![],
                    error: Some(ErrorDetail {
                        code: "NOT_FOUND".into(),
                        message: format!("rune '{}' not found", req.rune_name),
                        details: vec![],
                    }),
                    attachments: vec![],
                })),
            })
            .await;
        return;
    };

    let ctx = RuneContext {
        rune_name: req.rune_name.clone(),
        request_id: request_id.clone(),
        context: req.context.clone(),
        cancellation: cancel_token,
    };

    let input = Bytes::from(req.input);
    let files: Vec<FileAttachment> = req
        .attachments
        .iter()
        .map(|a| FileAttachment {
            filename: a.filename.clone(),
            data: Bytes::from(a.data.clone()),
            mime_type: a.mime_type.clone(),
        })
        .collect();

    match &registered.handler {
        HandlerKind::Unary(handler) => {
            let result = handler(ctx, input).await;
            let msg = match result {
                Ok(output) => SessionMessage {
                    payload: Some(Payload::Result(ExecuteResult {
                        request_id,
                        status: rune_proto::Status::Completed.into(),
                        output: output.to_vec(),
                        error: None,
                        attachments: vec![],
                    })),
                },
                Err(e) => SessionMessage {
                    payload: Some(Payload::Result(ExecuteResult {
                        request_id,
                        status: rune_proto::Status::Failed.into(),
                        output: vec![],
                        error: Some(ErrorDetail {
                            code: "EXECUTION_FAILED".into(),
                            message: e.to_string(),
                            details: vec![],
                        }),
                        attachments: vec![],
                    })),
                },
            };
            let _ = tx.send(msg).await;
        }
        HandlerKind::UnaryWithFiles(handler) => {
            let result = handler(ctx, input, files).await;
            let msg = match result {
                Ok(output) => SessionMessage {
                    payload: Some(Payload::Result(ExecuteResult {
                        request_id,
                        status: rune_proto::Status::Completed.into(),
                        output: output.to_vec(),
                        error: None,
                        attachments: vec![],
                    })),
                },
                Err(e) => SessionMessage {
                    payload: Some(Payload::Result(ExecuteResult {
                        request_id,
                        status: rune_proto::Status::Failed.into(),
                        output: vec![],
                        error: Some(ErrorDetail {
                            code: "EXECUTION_FAILED".into(),
                            message: e.to_string(),
                            details: vec![],
                        }),
                        attachments: vec![],
                    })),
                },
            };
            let _ = tx.send(msg).await;
        }
        HandlerKind::Stream(handler) => {
            let (stream_tx, mut stream_rx) = mpsc::channel::<Bytes>(32);
            let sender = StreamSender::new(stream_tx);

            // Forward stream events to gRPC
            let tx_clone = tx.clone();
            let rid = request_id.clone();
            let forward_handle = tokio::spawn(async move {
                while let Some(data) = stream_rx.recv().await {
                    let msg = SessionMessage {
                        payload: Some(Payload::StreamEvent(StreamEvent {
                            request_id: rid.clone(),
                            data: data.to_vec(),
                            event_type: String::new(),
                        })),
                    };
                    if tx_clone.send(msg).await.is_err() {
                        break;
                    }
                }
            });

            let result = handler(ctx, input, sender).await;
            forward_handle.await.ok();

            let end_msg = match result {
                Ok(()) => SessionMessage {
                    payload: Some(Payload::StreamEnd(StreamEnd {
                        request_id,
                        status: rune_proto::Status::Completed.into(),
                        error: None,
                    })),
                },
                Err(e) => SessionMessage {
                    payload: Some(Payload::StreamEnd(StreamEnd {
                        request_id,
                        status: rune_proto::Status::Failed.into(),
                        error: Some(ErrorDetail {
                            code: "EXECUTION_FAILED".into(),
                            message: e.to_string(),
                            details: vec![],
                        }),
                    })),
                },
            };
            let _ = tx.send(end_msg).await;
        }
        HandlerKind::StreamWithFiles(handler) => {
            let (stream_tx, mut stream_rx) = mpsc::channel::<Bytes>(32);
            let sender = StreamSender::new(stream_tx);

            let tx_clone = tx.clone();
            let rid = request_id.clone();
            let forward_handle = tokio::spawn(async move {
                while let Some(data) = stream_rx.recv().await {
                    let msg = SessionMessage {
                        payload: Some(Payload::StreamEvent(StreamEvent {
                            request_id: rid.clone(),
                            data: data.to_vec(),
                            event_type: String::new(),
                        })),
                    };
                    if tx_clone.send(msg).await.is_err() {
                        break;
                    }
                }
            });

            let result = handler(ctx, input, files, sender).await;
            forward_handle.await.ok();

            let end_msg = match result {
                Ok(()) => SessionMessage {
                    payload: Some(Payload::StreamEnd(StreamEnd {
                        request_id,
                        status: rune_proto::Status::Completed.into(),
                        error: None,
                    })),
                },
                Err(e) => SessionMessage {
                    payload: Some(Payload::StreamEnd(StreamEnd {
                        request_id,
                        status: rune_proto::Status::Failed.into(),
                        error: Some(ErrorDetail {
                            code: "EXECUTION_FAILED".into(),
                            message: e.to_string(),
                            details: vec![],
                        }),
                    })),
                },
            };
            let _ = tx.send(end_msg).await;
        }
    }
}
