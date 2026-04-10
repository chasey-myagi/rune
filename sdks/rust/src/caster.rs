//! Caster — connects to Rune runtime and executes registered handlers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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
    GateConfig as ProtoGateConfig, HealthReport, HealthStatus, Heartbeat, RuneDeclaration,
    SessionMessage, StreamEnd, StreamEvent,
};

use crate::config::{CasterConfig, FileAttachment, RuneConfig};
use crate::context::RuneContext;
use crate::error::{SdkError, SdkResult};
use crate::handler::{BoxFuture, HandlerKind, RegisteredRune};
use crate::pilot_client::PilotClient;
use crate::stream::StreamSender;

/// Caster connects to a Rune Runtime and registers Rune handlers.
pub struct Caster {
    config: CasterConfig,
    caster_id: String,
    runes: Arc<RwLock<HashMap<String, RegisteredRune>>>,
    shutdown_token: CancellationToken,
    active_requests: Arc<AtomicU32>,
    /// When true, new Execute requests are rejected with SHUTTING_DOWN.
    draining: Arc<AtomicBool>,
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
            shutdown_token: CancellationToken::new(),
            active_requests: Arc::new(AtomicU32::new(0)),
            draining: Arc::new(AtomicBool::new(false)),
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
        self.runes
            .read()
            .unwrap()
            .get(name)
            .map(|r| r.config.clone())
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

    /// Signal the Caster to stop its run loop.
    ///
    /// Safe to call from any thread or task. The [`run()`](Self::run) method
    /// will return shortly after this is called. Idempotent — calling
    /// multiple times is safe.
    pub fn stop(&self) {
        self.shutdown_token.cancel();
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
        let handler = Arc::new(
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
        let handler = Arc::new(
            move |ctx, input, stream| -> BoxFuture<'static, SdkResult<()>> {
                Box::pin(handler(ctx, input, stream))
            },
        );
        let mut cfg = config;
        cfg.supports_stream = true;
        self.register_inner(cfg, HandlerKind::Stream(handler))
    }

    /// Register a streaming rune handler that accepts file attachments.
    pub fn stream_rune_with_files<F, Fut>(&self, config: RuneConfig, handler: F) -> SdkResult<()>
    where
        F: Fn(RuneContext, Bytes, Vec<FileAttachment>, StreamSender) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = SdkResult<()>> + Send + 'static,
    {
        let handler = Arc::new(
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
    ///
    /// Returns when the session ends normally, or when [`stop()`](Self::stop)
    /// is called, or on unrecoverable error.
    pub async fn run(&self) -> SdkResult<()> {
        let mut delay = Duration::from_secs_f64(self.config.reconnect_base_delay_secs);
        let max_delay = Duration::from_secs_f64(self.config.reconnect_max_delay_secs);
        let mut last_pilot: Option<PilotClient> = None;

        let result = loop {
            if self.shutdown_token.is_cancelled() {
                break Ok(());
            }

            // (Re-)establish pilot registration on every connect attempt so
            // that a pilot daemon restart is picked up automatically.
            let pilot_id = if let Some(policy) = self.config.scale_policy.as_ref() {
                match PilotClient::ensure(&self.config.runtime, self.config.key.as_deref()).await {
                    Ok(client) => {
                        if let Err(e) = client.register(&self.caster_id, policy).await {
                            tracing::warn!("pilot registration failed: {e}");
                        }
                        let id = client.pilot_id().to_string();
                        last_pilot = Some(client);
                        Some(id)
                    }
                    Err(e) => {
                        tracing::warn!("pilot ensure failed: {e}");
                        None
                    }
                }
            } else {
                None
            };

            match self.connect_and_run(pilot_id.as_deref()).await {
                Ok(()) => break Ok(()),
                Err(e) => {
                    if self.shutdown_token.is_cancelled() {
                        break Ok(());
                    }
                    tracing::warn!("connection error: {}, reconnecting in {:?}", e, delay);
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = self.shutdown_token.cancelled() => {
                            break Ok(());
                        }
                    }
                    delay = (delay * 2).min(max_delay);
                }
            }
        };

        // Best-effort deregister on shutdown — reuse the cached client to
        // avoid accidentally spawning a new pilot daemon.
        if let Some(client) = last_pilot {
            let _ = client.deregister(&self.caster_id).await;
        }

        result
    }

    async fn connect_and_run(&self, pilot_id: Option<&str>) -> SdkResult<()> {
        // Reset draining state from any previous shutdown cycle.
        self.draining.store(false, Ordering::Relaxed);
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
        let attach_msg = self.build_attach_message(pilot_id);
        tx.send(attach_msg)
            .await
            .map_err(|e| SdkError::ChannelSend(e.to_string()))?;

        // Start heartbeat
        let hb_tx = tx.clone();
        let hb_interval = Duration::from_secs_f64(self.config.heartbeat_interval_secs);
        let config = self.config.clone();
        let active_requests = Arc::clone(&self.active_requests);
        let hb_draining = Arc::clone(&self.draining);
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
                // Always send HealthReport regardless of scale_policy
                let active = active_requests.load(Ordering::Relaxed);
                let is_draining = hb_draining.load(Ordering::Relaxed);
                if hb_tx
                    .send(build_health_report_message(&config, active, is_draining))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        // Cancellation tokens per request (use tokio::sync for async spawned tasks)
        let cancel_tokens: Arc<tokio::sync::RwLock<HashMap<String, CancellationToken>>> =
            Arc::new(tokio::sync::RwLock::new(HashMap::new()));

        // Message loop
        loop {
            let msg = tokio::select! {
                msg = inbound.message() => {
                    match msg? {
                        Some(m) => m,
                        None => break, // stream ended
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    break;
                }
            };
            match msg.payload {
                Some(Payload::AttachAck(ack)) => {
                    if ack.accepted {
                        tracing::info!(
                            "attached to {}, caster_id={}",
                            self.config.runtime,
                            self.caster_id
                        );
                        // Always send initial HealthReport regardless of scale_policy
                        tx.send(build_health_report_message(
                            &self.config,
                            self.active_requests.load(Ordering::Relaxed),
                            self.draining.load(Ordering::Relaxed),
                        ))
                        .await
                        .map_err(|e| SdkError::ChannelSend(e.to_string()))?;
                    } else {
                        tracing::error!("attach rejected: {}", ack.reason);
                        return Err(SdkError::Other(format!("attach rejected: {}", ack.reason)));
                    }
                }
                Some(Payload::Execute(req)) => {
                    // Reject new requests while draining for graceful shutdown.
                    if self.draining.load(Ordering::Relaxed) {
                        let _ = tx
                            .send(SessionMessage {
                                payload: Some(Payload::Result(ExecuteResult {
                                    request_id: req.request_id,
                                    status: rune_proto::Status::Failed.into(),
                                    output: vec![],
                                    error: Some(ErrorDetail {
                                        code: "SHUTTING_DOWN".into(),
                                        message: "caster is draining, no new requests accepted"
                                            .into(),
                                        details: vec![],
                                    }),
                                    attachments: vec![],
                                })),
                            })
                            .await;
                        continue;
                    }
                    let registered = self.runes.read().unwrap().get(&req.rune_name).cloned();
                    self.active_requests.fetch_add(1, Ordering::Relaxed);

                    let token = CancellationToken::new();
                    cancel_tokens
                        .write()
                        .await
                        .insert(req.request_id.clone(), token.clone());

                    let tx_clone = tx.clone();
                    let cancel_tokens_clone = cancel_tokens.clone();
                    let request_id = req.request_id.clone();
                    let active_requests = Arc::clone(&self.active_requests);
                    tokio::spawn(async move {
                        // Decrement active_requests on exit regardless of
                        // whether the handler completes normally or panics.
                        struct Guard(Arc<std::sync::atomic::AtomicU32>);
                        impl Drop for Guard {
                            fn drop(&mut self) {
                                self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        let _guard = Guard(active_requests);

                        execute_handler(registered, req, tx_clone, token).await;
                        cancel_tokens_clone.write().await.remove(&request_id);
                    });
                }
                Some(Payload::Cancel(cancel)) => {
                    if let Some(token) = cancel_tokens.read().await.get(&cancel.request_id) {
                        token.cancel();
                    }
                    tracing::info!("cancel requested: {}", cancel.request_id);
                }
                Some(Payload::Heartbeat(_)) => {
                    // Server heartbeat — acknowledged silently
                }
                Some(Payload::Shutdown(shutdown)) => {
                    tracing::info!(
                        "shutdown requested: {}, grace_period_ms={}",
                        shutdown.reason,
                        shutdown.grace_period_ms
                    );
                    // Mark as draining to reject new Execute requests.
                    self.draining.store(true, Ordering::Relaxed);
                    // Immediately advertise UNHEALTHY so the runtime stops
                    // routing new work to this caster during the grace window.
                    let _ = tx
                        .send(build_health_report_message(
                            &self.config,
                            self.active_requests.load(Ordering::Relaxed),
                            true,
                        ))
                        .await;
                    // Graceful drain: wait for in-flight requests to complete
                    // or until grace_period_ms expires, whichever comes first.
                    let grace = Duration::from_millis(shutdown.grace_period_ms as u64);
                    let drain_start = tokio::time::Instant::now();
                    while self.active_requests.load(Ordering::Relaxed) > 0 {
                        if drain_start.elapsed() >= grace {
                            tracing::warn!(
                                "grace period expired with {} active requests remaining",
                                self.active_requests.load(Ordering::Relaxed)
                            );
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    self.stop();
                    break;
                }
                _ => {}
            }
        }

        hb_handle.abort();
        Ok(())
    }

    fn build_attach_message(&self, pilot_id: Option<&str>) -> SessionMessage {
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
                labels: self.attach_labels(pilot_id),
                max_concurrent: self.config.max_concurrent,
                key: self.config.key.clone().unwrap_or_default(),
                role: "caster".into(),
            })),
        }
    }

    fn attach_labels(&self, pilot_id: Option<&str>) -> HashMap<String, String> {
        let mut labels = self.config.labels.clone();
        if let Some(policy) = self.config.scale_policy.as_ref() {
            labels.insert("group".into(), policy.group.clone());
            labels.insert("_scale_up".into(), policy.scale_up_threshold.to_string());
            labels.insert(
                "_scale_down".into(),
                policy.scale_down_threshold.to_string(),
            );
            labels.insert("_sustained".into(), policy.sustained_secs.to_string());
            labels.insert("_min".into(), policy.min_replicas.to_string());
            labels.insert("_max".into(), policy.max_replicas.to_string());
            labels.insert("_spawn_command".into(), policy.spawn_command.clone());
            labels.insert("_shutdown_signal".into(), policy.shutdown_signal.clone());
            if let Some(pilot_id) = pilot_id {
                labels.insert("_pilot_id".into(), pilot_id.to_string());
            }
        }
        labels
    }
}

fn build_health_report_message(
    config: &CasterConfig,
    active_requests: u32,
    draining: bool,
) -> SessionMessage {
    let mut metrics = config
        .load_report
        .as_ref()
        .map(|report| report.metrics.clone())
        .unwrap_or_default();
    metrics
        .entry("active_requests".into())
        .or_insert(active_requests as f64);
    metrics
        .entry("max_concurrent".into())
        .or_insert(config.max_concurrent as f64);
    metrics
        .entry("available_permits".into())
        .or_insert(config.max_concurrent.saturating_sub(active_requests) as f64);

    let computed_pressure = if config.max_concurrent == 0 {
        0.0
    } else {
        active_requests as f64 / config.max_concurrent as f64
    };
    let pressure = config
        .load_report
        .as_ref()
        .and_then(|lr| lr.pressure)
        .unwrap_or(computed_pressure);

    let status = if draining {
        HealthStatus::Unhealthy
    } else {
        HealthStatus::Healthy
    };
    SessionMessage {
        payload: Some(Payload::HealthReport(HealthReport {
            status: status.into(),
            active_requests,
            error_rate: 0.0,
            custom_info: String::new(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            error_rate_window_secs: 0,
            pressure,
            metrics,
        })),
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
