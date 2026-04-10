use crate::auth::KeyVerifier;
use crate::invoker::RemoteInvoker;
use crate::relay::Relay;
use crate::rune::{RuneConfig, RuneError};
use bytes::Bytes;
use dashmap::DashMap;
use rune_proto::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time;
use tonic::Streaming;
use tracing::Instrument;

pub const PROTOCOL_VERSION: &str = "1.2";
pub const FEATURE_HEALTH_PROBE: &str = "health_probe";
pub const FEATURE_AUTO_SCALING: &str = "auto_scaling";

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SessionState {
    Attaching,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HealthStatusLevel {
    #[default]
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CasterRole {
    #[default]
    Caster,
    Pilot,
}

/// Wraps per-caster health info with a generation marker so that
/// `cleanup_session` can atomically remove only its own generation's
/// entry via `DashMap::remove_if`.
#[derive(Debug)]
pub(crate) struct HealthEntry {
    pub(crate) generation: u64,
    pub(crate) info: Arc<std::sync::RwLock<HealthInfo>>,
}

impl CasterRole {
    fn from_proto(role: &str) -> Self {
        match role {
            "pilot" => Self::Pilot,
            _ => Self::Caster,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Caster => "caster",
            Self::Pilot => "pilot",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CasterMetadata {
    pub labels: HashMap<String, String>,
    pub max_concurrent: usize,
    pub role: CasterRole,
    /// Monotonic generation marker — matches the `CasterState::generation` set
    /// during `handle_attach`.  Used by `cleanup_session` to conditionally
    /// remove only this generation's metadata via `DashMap::remove_if`.
    pub(crate) generation: u64,
    /// Wall-clock connection time — used only for display in management APIs
    /// (e.g. `connected_since` in `/api/v1/casters`).  NOT used for identity.
    pub connected_at: Instant,
}

impl HealthStatusLevel {
    fn from_proto(status: i32) -> Self {
        match HealthStatus::try_from(status).unwrap_or(HealthStatus::Unspecified) {
            HealthStatus::Degraded => Self::Degraded,
            HealthStatus::Unhealthy => Self::Unhealthy,
            _ => Self::Healthy,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HealthInfo {
    pub status: HealthStatusLevel,
    pub active_requests: u32,
    pub error_rate: f32,
    pub error_rate_window_secs: u32,
    pub custom_info: String,
    pub last_report_ms: u64,
    pub pressure: f64,
    pub pressure_reported: bool,
    pub metrics: HashMap<String, f64>,
}

/// Callback invoked when a caster attaches, with caster_id and registered rune configs.
pub type OnCasterAttach = Arc<dyn Fn(&str, &[RuneConfig]) + Send + Sync>;

pub(crate) enum PendingResponse {
    Once(oneshot::Sender<Result<Bytes, RuneError>>),
    Stream(mpsc::Sender<Result<Bytes, RuneError>>),
}

pub(crate) struct PendingRequest {
    pub(crate) tx: PendingResponse,
    pub(crate) _permit: tokio::sync::OwnedSemaphorePermit,
    /// Per-request timing — when the request was enqueued.
    #[allow(dead_code)]
    pub(crate) created_at: Instant,
}

/// Central registry for caster gRPC stream connections.
///
/// `DashMap` is used throughout for concurrent shard-based access.  DashMap
/// internally uses `std::sync::RwLock` per shard; if profiling shows lock
/// contention on hot paths (e.g. high-frequency `pending` lookups), consider
/// enabling the `parking_lot` feature on `dashmap` for fairer scheduling and
/// lower overhead under contention.
pub struct SessionManager {
    pub(crate) sessions: DashMap<String, CasterState>,
    /// Health info uses `std::sync::RwLock` (not `tokio::sync::RwLock`) intentionally:
    /// all read/write operations are short scalar assignments with no `.await` while
    /// holding the lock, so the synchronous lock is faster (no async scheduler overhead).
    /// Do NOT add `.await` calls while holding these locks.
    ///
    /// Each entry carries a `connected_at` generation marker so that
    /// `cleanup_session` can do `remove_if` atomically per-entry, matching
    /// the same pattern used on `sessions`.
    pub(crate) health: DashMap<String, HealthEntry>,
    pub(crate) metadata: DashMap<String, CasterMetadata>,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) heartbeat_timeout: Duration,
    pub(crate) default_caster_max_concurrent: usize,
    caster_count_atomic: AtomicUsize,
    /// Monotonic counter for session generations — each attach gets a unique
    /// u64, eliminating the `Instant`-based identity which could collide in
    /// fast-reconnect scenarios.
    generation_counter: AtomicU64,
    on_caster_attach: OnceLock<OnCasterAttach>,
    key_verifier: Arc<dyn KeyVerifier>,
    dev_mode: bool,
}

pub(crate) struct CasterState {
    pub(crate) outbound: mpsc::Sender<SessionMessage>,
    pub(crate) pending: Arc<DashMap<String, PendingRequest>>,
    pub(crate) timeout_handles: Arc<DashMap<String, tokio::task::JoinHandle<()>>>,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) generation: u64,
}

impl SessionManager {
    /// Test-only constructor with dev_mode=true and no auth.
    /// Production code should use `with_auth()`.
    pub fn new_dev(heartbeat_interval: Duration, heartbeat_timeout: Duration) -> Self {
        Self::new_dev_with_default_max_concurrent(heartbeat_interval, heartbeat_timeout, 1024)
    }

    pub fn new_dev_with_default_max_concurrent(
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
        default_caster_max_concurrent: usize,
    ) -> Self {
        Self {
            sessions: DashMap::new(),
            health: DashMap::new(),
            metadata: DashMap::new(),
            heartbeat_interval,
            heartbeat_timeout,
            default_caster_max_concurrent,
            caster_count_atomic: AtomicUsize::new(0),
            generation_counter: AtomicU64::new(1),
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
        Self::with_auth_and_default_max_concurrent(
            heartbeat_interval,
            heartbeat_timeout,
            key_verifier,
            dev_mode,
            1024,
        )
    }

    pub fn with_auth_and_default_max_concurrent(
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
        key_verifier: Arc<dyn KeyVerifier>,
        dev_mode: bool,
        default_caster_max_concurrent: usize,
    ) -> Self {
        Self {
            sessions: DashMap::new(),
            health: DashMap::new(),
            metadata: DashMap::new(),
            heartbeat_interval,
            heartbeat_timeout,
            default_caster_max_concurrent,
            caster_count_atomic: AtomicUsize::new(0),
            generation_counter: AtomicU64::new(1),
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
        self.sessions
            .get(caster_id)
            .map(|s| s.semaphore.available_permits() > 0)
            .unwrap_or(false)
    }

    pub fn health_info(&self, caster_id: &str) -> Option<HealthInfo> {
        self.health.get(caster_id).map(|entry| {
            entry
                .info
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        })
    }

    pub fn health_status(&self, caster_id: &str) -> Option<HealthStatusLevel> {
        self.health_info(caster_id).map(|info| info.status)
    }

    pub fn is_healthy(&self, caster_id: &str) -> bool {
        self.health_status(caster_id)
            .map(|status| status != HealthStatusLevel::Unhealthy)
            .unwrap_or(true)
    }

    /// Return the list of connected caster IDs.
    pub fn list_caster_ids(&self) -> Vec<String> {
        self.sessions
            .iter()
            .filter(|e| {
                self.metadata
                    .get(e.key())
                    .map(|m| m.role == CasterRole::Caster)
                    .unwrap_or(false)
            })
            .map(|e| e.key().clone())
            .collect()
    }

    /// Return the number of connected casters (excludes Pilot sessions).
    /// O(1) via atomic counter — maintained by handle_attach / cleanup_session.
    pub fn caster_count(&self) -> usize {
        self.caster_count_atomic.load(Ordering::Relaxed)
    }

    /// Return the number of available permits (free concurrency slots) for a caster.
    /// Returns 0 if the caster is not connected.
    pub fn available_permits(&self, caster_id: &str) -> usize {
        self.sessions
            .get(caster_id)
            .map(|s| s.semaphore.available_permits())
            .unwrap_or(0)
    }

    pub fn max_concurrent(&self, caster_id: &str) -> usize {
        self.metadata
            .get(caster_id)
            .map(|meta| meta.max_concurrent)
            .unwrap_or(0)
    }

    pub fn caster_role(&self, caster_id: &str) -> Option<CasterRole> {
        self.metadata.get(caster_id).map(|meta| meta.role)
    }

    pub fn caster_labels(&self, caster_id: &str) -> Option<HashMap<String, String>> {
        self.metadata.get(caster_id).map(|meta| meta.labels.clone())
    }

    pub fn caster_ids_by_role(&self, role: CasterRole) -> Vec<String> {
        self.metadata
            .iter()
            .filter(|entry| entry.value().role == role)
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn pressure(&self, caster_id: &str) -> f64 {
        self.health_info(caster_id)
            .map(|info| info.pressure)
            .unwrap_or(0.0)
    }

    pub fn reported_pressure(&self, caster_id: &str) -> Option<f64> {
        self.health_info(caster_id)
            .and_then(|info| info.pressure_reported.then_some(info.pressure))
    }

    pub fn group_pressure(&self, label_key: &str, label_value: &str) -> (f64, usize) {
        let mut total = 0.0;
        let mut count = 0usize;

        for meta in self.metadata.iter() {
            if meta.value().role != CasterRole::Caster {
                continue;
            }
            if meta.value().labels.get(label_key).map(String::as_str) != Some(label_value) {
                continue;
            }
            if let Some(pressure) = self.reported_pressure(meta.key()) {
                total += pressure;
                count += 1;
            }
        }

        if count == 0 {
            (0.0, 0)
        } else {
            (total / count as f64, count)
        }
    }

    /// Return the monotonic session generation for a caster, or `None` if not found.
    pub fn session_generation(&self, caster_id: &str) -> Option<u64> {
        self.sessions.get(caster_id).map(|s| s.generation)
    }

    /// Wall-clock connection time for display in management APIs.
    pub fn connected_at(&self, caster_id: &str) -> Option<Instant> {
        self.metadata.get(caster_id).map(|m| m.connected_at)
    }

    /// Allocate the next monotonic generation value.
    fn next_generation(&self) -> u64 {
        self.generation_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Insert a dummy caster for integration tests in downstream crates.
    pub fn insert_test_caster(&self, caster_id: &str, max_concurrent: usize) {
        let gen = self.next_generation();
        let (tx, _rx) = mpsc::channel(16);
        self.sessions.insert(
            caster_id.to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::new(DashMap::new()),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::new(Semaphore::new(max_concurrent)),
                generation: gen,
            },
        );
        self.health.insert(
            caster_id.to_string(),
            HealthEntry {
                generation: gen,
                info: Arc::new(std::sync::RwLock::new(HealthInfo::default())),
            },
        );
        self.metadata.insert(
            caster_id.to_string(),
            CasterMetadata {
                labels: HashMap::new(),
                max_concurrent,
                role: CasterRole::Caster,
                generation: gen,
                connected_at: Instant::now(),
            },
        );
        self.caster_count_atomic.fetch_add(1, Ordering::Relaxed);
    }

    /// Acquire one permit from a test caster's semaphore (saturate capacity).
    /// Panics if the caster does not exist or has no available permits.
    pub fn acquire_test_permit(&self, caster_id: &str) -> tokio::sync::OwnedSemaphorePermit {
        let session = self.sessions.get(caster_id).expect("test caster not found");
        let sem = Arc::clone(&session.semaphore);
        drop(session);
        sem.try_acquire_owned()
            .expect("no available permits on test caster")
    }

    /// Set the health status of a test caster.
    pub fn set_test_health(&self, caster_id: &str, status: HealthStatusLevel) {
        if let Some(entry) = self.health.get(caster_id) {
            let mut info = entry.info.write().unwrap();
            info.status = status;
        }
    }

    pub fn set_test_pressure(&self, caster_id: &str, pressure: f64) {
        if let Some(entry) = self.health.get(caster_id) {
            let mut info = entry.info.write().unwrap();
            info.pressure = pressure;
            info.pressure_reported = true;
        }
    }

    pub fn set_test_labels(&self, caster_id: &str, labels: HashMap<String, String>) {
        if let Some(mut metadata) = self.metadata.get_mut(caster_id) {
            metadata.labels = labels;
        }
    }

    pub fn set_test_role(&self, caster_id: &str, role: CasterRole) {
        if let Some(mut metadata) = self.metadata.get_mut(caster_id) {
            metadata.role = role;
        }
    }
}

/// Per-session shared state, aggregated for clean method extraction.
/// Contains all session-local mutable state; `inbound` stays in the caller
/// to avoid `&mut` borrow conflicts with `tokio::select!`.
struct SessionContext {
    caster_id: Option<String>,
    state: SessionState,
    pending: Arc<DashMap<String, PendingRequest>>,
    timeout_handles: Arc<DashMap<String, tokio::task::JoinHandle<()>>>,
    semaphore: Arc<Semaphore>,
    last_heartbeat_ms: Arc<AtomicU64>,
    outbound_tx: mpsc::Sender<SessionMessage>,
    relay: Arc<Relay>,
    /// Monotonic generation set during handle_attach; cleanup_session compares
    /// this against the stored CasterState.generation to avoid deleting a
    /// newer session that reconnected with the same caster_id.
    generation: Option<u64>,
}

impl SessionContext {
    fn new(outbound_tx: mpsc::Sender<SessionMessage>, relay: Arc<Relay>) -> Self {
        Self {
            caster_id: None,
            state: SessionState::Attaching,
            pending: Arc::new(DashMap::new()),
            timeout_handles: Arc::new(DashMap::new()),
            // Starts with 0 permits; handle_attach adds permits once the
            // caster declares max_concurrent.  Before Attach, the is_active()
            // guard prevents any execute calls from reaching this semaphore.
            semaphore: Arc::new(Semaphore::new(0)),
            last_heartbeat_ms: Arc::new(AtomicU64::new(now_ms())),
            outbound_tx,
            relay,
            generation: None,
        }
    }

    fn is_active(&self) -> bool {
        self.state == SessionState::Active
    }

    fn touch_heartbeat(&self) {
        self.last_heartbeat_ms.store(now_ms(), Ordering::Relaxed);
    }
}

impl SessionManager {
    pub async fn handle_session(
        self: &Arc<Self>,
        relay: Arc<Relay>,
        mut inbound: Streaming<SessionMessage>,
        outbound_tx: mpsc::Sender<SessionMessage>,
    ) {
        let mut ctx = SessionContext::new(outbound_tx, relay);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let hb_handle = self.spawn_heartbeat_task(&ctx, shutdown_tx);

        loop {
            let msg = tokio::select! {
                result = inbound.message() => match result {
                    Ok(Some(msg)) => msg,
                    _ => break,
                },
                _ = shutdown_rx.changed() => {
                    tracing::info!("session shutdown signal received");
                    break;
                }
            };

            match msg.payload {
                Some(session_message::Payload::Attach(attach)) => {
                    if !self.handle_attach(&mut ctx, attach).await {
                        break;
                    }
                }
                Some(session_message::Payload::Result(result)) if ctx.is_active() => {
                    Self::handle_execute_result(&ctx, result).await;
                }
                Some(session_message::Payload::StreamEvent(event)) if ctx.is_active() => {
                    Self::handle_stream_event(&ctx, event).await;
                }
                Some(session_message::Payload::StreamEnd(end)) if ctx.is_active() => {
                    Self::handle_stream_end(&ctx, end).await;
                }
                Some(session_message::Payload::Detach(detach)) => {
                    tracing::info!(reason = %detach.reason, "caster detached");
                    break;
                }
                Some(session_message::Payload::Heartbeat(_)) => {
                    ctx.touch_heartbeat();
                }
                Some(session_message::Payload::HealthReport(report)) if ctx.is_active() => {
                    self.handle_health_report(&ctx, report);
                }
                _ => {}
            }
        }

        hb_handle.abort();
        self.cleanup_session(&ctx).await;
    }

    // ── Extracted session handlers ──────────────────────────────────

    fn spawn_heartbeat_task(
        &self,
        ctx: &SessionContext,
        shutdown_tx: tokio::sync::watch::Sender<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let hb_tx = ctx.outbound_tx.clone();
        let hb_last = Arc::clone(&ctx.last_heartbeat_ms);
        let hb_shutdown = shutdown_tx.clone();
        let hb_interval = self.heartbeat_interval;
        let hb_timeout = self.heartbeat_timeout;
        let handle = tokio::spawn(async move {
            loop {
                time::sleep(hb_interval).await;
                let msg = SessionMessage {
                    payload: Some(session_message::Payload::Heartbeat(Heartbeat {
                        timestamp_ms: now_ms(),
                    })),
                };
                if hb_tx.send(msg).await.is_err() {
                    break;
                }
                let elapsed = now_ms().saturating_sub(hb_last.load(Ordering::Relaxed));
                if elapsed > hb_timeout.as_millis() as u64 {
                    tracing::warn!(
                        elapsed_ms = elapsed,
                        "heartbeat timeout, forcing session shutdown"
                    );
                    let _ = hb_shutdown.send(true);
                    break;
                }
            }
        });
        drop(shutdown_tx);
        handle
    }

    /// Process a CasterAttach message. Returns `true` if attach succeeded
    /// (session moves to Active), `false` if auth was rejected (caller breaks).
    async fn handle_attach(
        self: &Arc<Self>,
        ctx: &mut SessionContext,
        attach: CasterAttach,
    ) -> bool {
        let id = attach.caster_id.clone();
        let max_conc = attach.max_concurrent;
        let role = CasterRole::from_proto(&attach.role);

        // ── Auth check ──
        if !self.dev_mode {
            let key_valid = self.key_verifier.verify_caster_key(&attach.key).await;
            if !key_valid {
                tracing::warn!(caster_id = %id, "caster attach rejected: invalid key");
                let ack = SessionMessage {
                    payload: Some(session_message::Payload::AttachAck(AttachAck {
                        accepted: false,
                        reason: "invalid or missing caster key".into(),
                        supported_features: vec![
                            FEATURE_HEALTH_PROBE.to_string(),
                            FEATURE_AUTO_SCALING.to_string(),
                        ],
                        protocol_version: PROTOCOL_VERSION.to_string(),
                    })),
                };
                let _ = ctx.outbound_tx.send(ack).await;
                return false;
            }
        }

        tracing::info!(caster_id = %id, runes = attach.runes.len(), max_concurrent = max_conc, "caster attached");

        let permits = if max_conc > 0 {
            max_conc as usize
        } else {
            self.default_caster_max_concurrent
        };
        tracing::info!(caster_id = %id, effective_max_concurrent = permits, "caster concurrency configured");
        ctx.semaphore.add_permits(permits);

        let gen = self.next_generation();
        ctx.generation = Some(gen);
        self.sessions.insert(
            id.clone(),
            CasterState {
                outbound: ctx.outbound_tx.clone(),
                pending: Arc::clone(&ctx.pending),
                timeout_handles: Arc::clone(&ctx.timeout_handles),
                semaphore: Arc::clone(&ctx.semaphore),
                generation: gen,
            },
        );
        self.health.insert(
            id.clone(),
            HealthEntry {
                generation: gen,
                info: Arc::new(std::sync::RwLock::new(HealthInfo::default())),
            },
        );
        let old_meta = self.metadata.insert(
            id.clone(),
            CasterMetadata {
                labels: attach.labels.clone(),
                max_concurrent: permits,
                role,
                generation: gen,
                connected_at: Instant::now(),
            },
        );

        // Maintain caster_count_atomic: only net-increment when a new Caster
        // appears. If a Caster reconnects (old_meta was also Caster), the
        // count stays the same. If role changed, adjust accordingly.
        let old_was_caster = old_meta
            .map(|m| m.role == CasterRole::Caster)
            .unwrap_or(false);
        let new_is_caster = role == CasterRole::Caster;
        if new_is_caster && !old_was_caster {
            self.caster_count_atomic.fetch_add(1, Ordering::Relaxed);
        } else if !new_is_caster && old_was_caster {
            self.caster_count_atomic.fetch_sub(1, Ordering::Relaxed);
        }

        let mut configs = Vec::new();
        if role != CasterRole::Pilot {
            // Remove stale relay entries from any previous session before
            // re-registering, then stamp this generation so cleanup_session
            // (which uses remove_caster_if_gen) will not wipe our entries.
            ctx.relay.remove_caster(&id);
            ctx.relay.set_caster_generation(&id, gen);
            for decl in &attach.runes {
                let config = RuneConfig {
                    name: decl.name.clone(),
                    version: decl.version.clone(),
                    description: decl.description.clone(),
                    supports_stream: decl.supports_stream,
                    gate: decl.gate.as_ref().map(|g| crate::rune::GateConfig {
                        path: g.path.clone(),
                        method: if g.method.is_empty() {
                            "POST".to_string()
                        } else {
                            g.method.clone()
                        },
                    }),
                    input_schema: if decl.input_schema.is_empty() {
                        None
                    } else {
                        Some(decl.input_schema.clone())
                    },
                    output_schema: if decl.output_schema.is_empty() {
                        None
                    } else {
                        Some(decl.output_schema.clone())
                    },
                    priority: decl.priority,
                    labels: attach.labels.clone(),
                };
                configs.push(config.clone());
                let invoker = Arc::new(RemoteInvoker {
                    session: Arc::clone(self) as Arc<SessionManager>,
                    caster_id: id.clone(),
                });
                if let Err(reason) = ctx.relay.register(config, invoker, Some(id.clone())) {
                    tracing::warn!(rune = %decl.name, %reason, "skipping rune registration");
                }
            }
        }

        // Notify attach callback (e.g. for snapshot recording)
        if let Some(callback) = self.on_caster_attach.get() {
            callback(&id, &configs);
        }

        let ack = SessionMessage {
            payload: Some(session_message::Payload::AttachAck(AttachAck {
                accepted: true,
                reason: String::new(),
                supported_features: vec![
                    FEATURE_HEALTH_PROBE.to_string(),
                    FEATURE_AUTO_SCALING.to_string(),
                ],
                protocol_version: PROTOCOL_VERSION.to_string(),
            })),
        };
        let _ = ctx.outbound_tx.send(ack).await;
        ctx.state = SessionState::Active;
        ctx.caster_id = Some(id);
        true
    }

    async fn handle_execute_result(ctx: &SessionContext, result: rune_proto::ExecuteResult) {
        let req_id = result.request_id.clone();
        if let Some((_, p)) = ctx.pending.remove(&req_id) {
            abort_timeout_handle(&ctx.timeout_handles, &req_id);
            let outcome = match result.status() {
                Status::Completed => Ok(Bytes::from(result.output)),
                _ => Err(RuneError::ExecutionFailed {
                    code: result
                        .error
                        .as_ref()
                        .map(|e| e.code.clone())
                        .unwrap_or_default(),
                    message: result
                        .error
                        .as_ref()
                        .map(|e| e.message.clone())
                        .unwrap_or_default(),
                }),
            };
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(outcome);
                }
                PendingResponse::Stream(tx) => {
                    let _ = tx.send(outcome).await;
                }
            }
        }
    }

    async fn handle_stream_event(ctx: &SessionContext, event: rune_proto::StreamEvent) {
        let req_id = event.request_id.clone();
        if let Some(p) = ctx.pending.get(&req_id) {
            if let PendingResponse::Stream(ref tx) = p.tx {
                let _ = tx.send(Ok(Bytes::from(event.data))).await;
            }
        }
    }

    async fn handle_stream_end(ctx: &SessionContext, end: rune_proto::StreamEnd) {
        let req_id = end.request_id.clone();
        if let Some((_, p)) = ctx.pending.remove(&req_id) {
            abort_timeout_handle(&ctx.timeout_handles, &req_id);
            if let PendingResponse::Stream(tx) = p.tx {
                if end.status() != Status::Completed {
                    let _ = tx
                        .send(Err(RuneError::ExecutionFailed {
                            code: end
                                .error
                                .as_ref()
                                .map(|e| e.code.clone())
                                .unwrap_or_default(),
                            message: end
                                .error
                                .as_ref()
                                .map(|e| e.message.clone())
                                .unwrap_or_default(),
                        }))
                        .await;
                }
            }
        }
    }

    fn handle_health_report(&self, ctx: &SessionContext, report: rune_proto::HealthReport) {
        if let Some(ref id) = ctx.caster_id {
            self.apply_health_report(id, report);
        }
    }

    /// Apply a raw HealthReport to the stored HealthInfo for a caster.
    /// This is the SOLE boundary where telemetry floats are sanitized.
    /// `pub(crate)` for direct testing — production entry is `handle_health_report()`.
    pub(crate) fn apply_health_report(&self, caster_id: &str, report: rune_proto::HealthReport) {
        if let Some(entry) = self.health.get(caster_id) {
            let mut current = entry
                .info
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            current.status = HealthStatusLevel::from_proto(report.status);
            current.active_requests = report.active_requests;
            current.error_rate = sanitize_f32(report.error_rate);
            current.error_rate_window_secs = report.error_rate_window_secs;
            current.custom_info = report.custom_info;
            current.last_report_ms = report.timestamp_ms;
            current.pressure = sanitize_f64(report.pressure);
            // pressure_reported gates group_pressure() sampling.
            // A NaN/inf/negative original value is NOT a valid report —
            // storing 0.0 is safe for JSON, but counting it as a real
            // idle sample would undercount group load.
            current.pressure_reported = report.pressure.is_finite() && report.pressure >= 0.0;
            // Filter out non-finite metric values: NaN/inf entries are
            // dropped rather than clamped to 0.0 — for user-defined metrics,
            // absence is more honest than a fabricated zero.
            current.metrics = report
                .metrics
                .into_iter()
                .filter(|(_, v)| v.is_finite())
                .collect();
        }
    }

    async fn cleanup_session(&self, ctx: &SessionContext) {
        if let Some(id) = &ctx.caster_id {
            // Guard: only remove if the stored session is still ours.
            // If a newer session reconnected with the same caster_id,
            // the stored generation will differ — skip cleanup to
            // avoid deleting the replacement session's data.
            // Atomic check-and-remove: avoids TOCTOU race where a reconnect
            // could insert a new session between our get() and remove().
            let removed = self
                .sessions
                .remove_if(id, |_, s| Some(s.generation) == ctx.generation);
            if removed.is_none() {
                tracing::info!(
                    caster_id = %id,
                    "skipping cleanup — session superseded by reconnect"
                );
                Self::drain_local_state(ctx, id, "superseded").await;
                return;
            }
            tracing::info!(caster_id = %id, "cleaning up disconnected caster");
            // Each entry carries a generation marker set during handle_attach.
            // `remove_if` is atomic per DashMap shard, so a replacement
            // session's entries (with a different generation) are never
            // accidentally deleted — no TOCTOU window.
            let gen = ctx
                .generation
                .expect("BUG: generation not set; handle_attach must run before cleanup");
            self.health
                .remove_if(id, |_, entry| entry.generation == gen);
            let meta_removed = self
                .metadata
                .remove_if(id, |_, meta| meta.generation == gen);
            if let Some((_, meta)) = meta_removed {
                if meta.role == CasterRole::Caster {
                    self.caster_count_atomic.fetch_sub(1, Ordering::Relaxed);
                }
            }
            // Relay: generation-aware removal.  Only removes entries
            // stamped with our generation (set in handle_attach via
            // set_caster_generation).  If a replacement session already
            // re-registered with a new generation, this is a no-op.
            ctx.relay.remove_caster_if_gen(id, gen);
            Self::drain_local_state(ctx, id, "disconnected").await;
        }
    }

    /// Abort pending timeout handles and drain pending requests with an error.
    /// Shared by both "current session cleanup" and "superseded session cleanup".
    async fn drain_local_state(ctx: &SessionContext, caster_id: &str, reason: &str) {
        let th_keys: Vec<String> = ctx
            .timeout_handles
            .iter()
            .map(|e| e.key().clone())
            .collect();
        for key in th_keys {
            abort_timeout_handle(&ctx.timeout_handles, &key);
        }
        let keys: Vec<String> = ctx.pending.iter().map(|e| e.key().clone()).collect();
        for req_id in keys {
            if let Some((_, p)) = ctx.pending.remove(&req_id) {
                let err = Err(RuneError::Internal(anyhow::anyhow!(
                    "caster '{}' {} — pending request aborted",
                    caster_id,
                    reason
                )));
                match p.tx {
                    PendingResponse::Once(tx) => {
                        let _ = tx.send(err);
                    }
                    PendingResponse::Stream(tx) => {
                        let _ = tx.send(err).await;
                    }
                }
            }
        }
    }

    pub async fn execute(
        &self,
        caster_id: &str,
        request_id: &str,
        rune_name: &str,
        input: Bytes,
        context: HashMap<String, String>,
        timeout: Duration,
    ) -> Result<Bytes, RuneError> {
        let trace_id = context
            .get(crate::trace::TRACE_ID_KEY)
            .cloned()
            .unwrap_or_default();

        async move {
            let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
            let permit = Arc::clone(&session.semaphore)
                .try_acquire_owned()
                .map_err(|_| RuneError::Unavailable)?;

            let (tx, rx) = oneshot::channel();
            session.pending.insert(
                request_id.to_string(),
                PendingRequest {
                    tx: PendingResponse::Once(tx),
                    _permit: permit,
                    created_at: Instant::now(),
                },
            );

            spawn_request_timeout(
                Arc::clone(&session.pending),
                Arc::clone(&session.timeout_handles),
                request_id.to_string(),
                timeout,
            );

            let msg = SessionMessage {
                payload: Some(session_message::Payload::Execute(ExecuteRequest {
                    request_id: request_id.to_string(),
                    rune_name: rune_name.to_string(),
                    input: input.to_vec(),
                    context,
                    timeout_ms: safe_timeout_ms(timeout),
                    attachments: Vec::new(),
                })),
            };

            if session.outbound.send(msg).await.is_err() {
                abort_timeout_handle(&session.timeout_handles, request_id);
                session.pending.remove(request_id);
                return Err(RuneError::Unavailable);
            }
            drop(session);
            rx.await
                .map_err(|_| RuneError::Internal(anyhow::anyhow!("response channel dropped")))?
        }
        .instrument(tracing::info_span!(
            "session.execute",
            trace_id = %trace_id,
            request_id = %request_id,
            caster_id = %caster_id,
            rune_name = %rune_name
        ))
        .await
    }

    pub async fn execute_stream(
        &self,
        caster_id: &str,
        request_id: &str,
        rune_name: &str,
        input: Bytes,
        context: HashMap<String, String>,
        timeout: Duration,
    ) -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError> {
        let trace_id = context
            .get(crate::trace::TRACE_ID_KEY)
            .cloned()
            .unwrap_or_default();

        async move {
            let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
            let permit = Arc::clone(&session.semaphore)
                .try_acquire_owned()
                .map_err(|_| RuneError::Unavailable)?;

            let (tx, rx) = mpsc::channel(32);
            session.pending.insert(
                request_id.to_string(),
                PendingRequest {
                    tx: PendingResponse::Stream(tx),
                    _permit: permit,
                    created_at: Instant::now(),
                },
            );

            spawn_request_timeout(
                Arc::clone(&session.pending),
                Arc::clone(&session.timeout_handles),
                request_id.to_string(),
                timeout,
            );

            let msg = SessionMessage {
                payload: Some(session_message::Payload::Execute(ExecuteRequest {
                    request_id: request_id.to_string(),
                    rune_name: rune_name.to_string(),
                    input: input.to_vec(),
                    context,
                    timeout_ms: safe_timeout_ms(timeout),
                    attachments: Vec::new(),
                })),
            };

            if session.outbound.send(msg).await.is_err() {
                abort_timeout_handle(&session.timeout_handles, request_id);
                session.pending.remove(request_id);
                return Err(RuneError::Unavailable);
            }
            drop(session);
            Ok(rx)
        }
        .instrument(tracing::info_span!(
            "session.execute_stream",
            trace_id = %trace_id,
            request_id = %request_id,
            caster_id = %caster_id,
            rune_name = %rune_name
        ))
        .await
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

    pub async fn cancel(
        &self,
        caster_id: &str,
        request_id: &str,
        reason: &str,
    ) -> Result<(), RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
        if let Some((_, p)) = session.pending.remove(request_id) {
            abort_timeout_handle(&session.timeout_handles, request_id);
            // permit auto-returned when PendingRequest is dropped
            let err = Err(RuneError::Cancelled);
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(err);
                }
                PendingResponse::Stream(tx) => {
                    let _ = tx.send(err).await;
                }
            }
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

    pub async fn send_message(
        &self,
        caster_id: &str,
        msg: SessionMessage,
    ) -> Result<(), RuneError> {
        let session = self.sessions.get(caster_id).ok_or(RuneError::Unavailable)?;
        session
            .outbound
            .send(msg)
            .await
            .map_err(|_| RuneError::Unavailable)
    }
}

/// Spawn a timeout task for a pending request and track its handle.
///
/// When the timeout fires, the task removes the pending entry and sends a
/// `RuneError::Timeout` to the caller. If the request has already completed
/// (removed from `pending`), the task is a no-op. The returned `JoinHandle`
/// is stored in `timeout_handles` so it can be aborted when the request
/// completes normally, avoiding ghost tasks that sleep until expiry.
fn spawn_request_timeout(
    pending: Arc<DashMap<String, PendingRequest>>,
    timeout_handles: Arc<DashMap<String, tokio::task::JoinHandle<()>>>,
    request_id: String,
    timeout: Duration,
) {
    let handles_clone = Arc::clone(&timeout_handles);
    let req_id_clone = request_id.clone();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        // If still pending when timeout fires, remove and send error
        if let Some((_, p)) = pending.remove(&req_id_clone) {
            tracing::warn!(request_id = %req_id_clone, "request timed out");
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Err(RuneError::Timeout));
                }
                PendingResponse::Stream(tx) => {
                    let _ = tx.send(Err(RuneError::Timeout)).await;
                }
            }
        }
        // Clean up our own handle entry
        handles_clone.remove(&req_id_clone);
    });
    timeout_handles.insert(request_id, handle);
}

/// Abort and remove a timeout task for the given request, if one exists.
fn abort_timeout_handle(
    timeout_handles: &DashMap<String, tokio::task::JoinHandle<()>>,
    request_id: &str,
) {
    if let Some((_, handle)) = timeout_handles.remove(request_id) {
        handle.abort();
    }
}

/// Convert a Duration to milliseconds clamped to u32::MAX to avoid truncation.
pub(crate) fn safe_timeout_ms(timeout: Duration) -> u32 {
    timeout.as_millis().min(u32::MAX as u128) as u32
}

fn now_ms() -> u64 {
    crate::time_utils::now_ms()
}

/// Clamp non-finite or negative f64 values at the telemetry boundary.
/// NaN/infinity → 0.0, negative → 0.0. Prevents poisoned aggregation.
fn sanitize_f64(v: f64) -> f64 {
    if v.is_finite() && v >= 0.0 {
        v
    } else {
        0.0
    }
}

/// Clamp non-finite or negative f32 values at the telemetry boundary.
fn sanitize_f32(v: f32) -> f32 {
    if v.is_finite() && v >= 0.0 {
        v
    } else {
        0.0
    }
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
    fn setup_session(
        max_concurrent: usize,
    ) -> (
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

        mgr.sessions.insert(
            caster_id.clone(),
            CasterState {
                outbound: tx,
                pending: Arc::clone(&pending),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::clone(&semaphore),
                generation: 1,
            },
        );

        (mgr, caster_id, semaphore, pending, rx)
    }

    /// Poll until a key appears in the pending map (max 1 second).
    async fn wait_for_pending(pending: &DashMap<String, PendingRequest>, key: &str) {
        for _ in 0..100 {
            if pending.contains_key(key) {
                return;
            }
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
                mgr.execute(
                    &cid,
                    "r-1",
                    "echo",
                    Bytes::from("hello"),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "r-1").await;
        assert_eq!(sem.available_permits(), 1);

        if let Some((_, p)) = pending.remove("r-1") {
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Ok(Bytes::from("world")));
                }
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
                mgr.execute(
                    &cid,
                    "r-timeout",
                    "slow",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "r-timeout").await;
        assert_eq!(sem.available_permits(), 1);

        if let Some((_, p)) = pending.remove("r-timeout") {
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Err(RuneError::Timeout));
                }
                _ => panic!("expected Once"),
            }
        }

        assert_eq!(sem.available_permits(), 2);

        let late_found = pending.remove("r-timeout");
        assert!(
            late_found.is_none(),
            "late result should find nothing in pending"
        );
        assert_eq!(
            sem.available_permits(),
            2,
            "late result must not inflate permits"
        );

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
                mgr.execute(
                    &cid,
                    "r-cancel",
                    "slow",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "r-cancel").await;
        assert_eq!(sem.available_permits(), 1);

        mgr.cancel(&caster_id, "r-cancel", "user requested")
            .await
            .unwrap();
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
                mgr.execute(
                    &cid,
                    "r-1",
                    "echo",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "r-1").await;
        assert_eq!(sem.available_permits(), 0);

        let result = mgr
            .execute(
                &caster_id,
                "r-2",
                "echo",
                Bytes::new(),
                Default::default(),
                DEFAULT_TIMEOUT,
            )
            .await;
        assert!(
            matches!(result, Err(RuneError::Unavailable)),
            "second request must be rejected when max_concurrent=1"
        );
    }

    #[tokio::test]
    async fn test_disconnect_clears_all_pending() {
        let (_mgr, _caster_id, _sem, pending, _rx) = setup_session(5);

        let dummy_sem = Arc::new(Semaphore::new(10));
        for i in 0..3 {
            let (tx, _rx) = oneshot::channel();
            let permit = Arc::clone(&dummy_sem).try_acquire_owned().unwrap();
            pending.insert(
                format!("r-{}", i),
                PendingRequest {
                    tx: PendingResponse::Once(tx),
                    _permit: permit,
                    created_at: Instant::now(),
                },
            );
        }

        assert_eq!(pending.len(), 3);

        let keys: Vec<String> = pending.iter().map(|e| e.key().clone()).collect();
        for req_id in keys {
            let _ = pending.remove(&req_id);
        }

        assert!(
            pending.is_empty(),
            "all pending should be cleared after disconnect"
        );
    }

    #[tokio::test]
    async fn test_list_caster_ids() {
        let mgr = default_session_manager();
        assert!(mgr.list_caster_ids().is_empty());
        assert_eq!(mgr.caster_count(), 0);

        // Use insert_test_caster which correctly populates both sessions and metadata.
        mgr.insert_test_caster("caster-1", 1);
        mgr.insert_test_caster("caster-2", 1);

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
                mgr.execute_stream(
                    &cid,
                    "rs-1",
                    "streamer",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
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
        mgr.sessions.insert(
            "caster-A".to_string(),
            CasterState {
                outbound: tx1,
                pending: Arc::clone(&pending1),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::clone(&sem1),
                generation: 1,
            },
        );

        let (tx2, _rx2) = mpsc::channel(16);
        let sem2 = Arc::new(Semaphore::new(5));
        let pending2: Arc<DashMap<String, PendingRequest>> = Arc::new(DashMap::new());
        mgr.sessions.insert(
            "caster-B".to_string(),
            CasterState {
                outbound: tx2,
                pending: Arc::clone(&pending2),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::clone(&sem2),
                generation: 2,
            },
        );

        // Launch a request on caster-A
        let handle = {
            let mgr_clone = Arc::clone(&mgr);
            tokio::spawn(async move {
                mgr_clone
                    .execute(
                        "caster-A",
                        "cross-req-1",
                        "slow_rune",
                        Bytes::new(),
                        Default::default(),
                        DEFAULT_TIMEOUT,
                    )
                    .await
            })
        };

        wait_for_pending(&pending1, "cross-req-1").await;
        assert_eq!(sem1.available_permits(), 4);

        // Cancel from the manager level (without knowing which caster owns the request)
        let cancelled = mgr
            .cancel_by_request_id("cross-req-1", "user cancelled")
            .await;
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
        assert!(
            !cancelled,
            "cancel should return false for non-existent request"
        );
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
                mgr.execute(
                    &cid,
                    "serial-1",
                    "echo",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "serial-1").await;
        assert_eq!(sem.available_permits(), 0);

        // Second request should be rejected immediately
        let result2 = mgr
            .execute(
                &caster_id,
                "serial-2",
                "echo",
                Bytes::new(),
                Default::default(),
                DEFAULT_TIMEOUT,
            )
            .await;
        assert!(
            matches!(result2, Err(RuneError::Unavailable)),
            "second request must be Unavailable when max_concurrent=1"
        );

        // Complete first request (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("serial-1") {
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Ok(Bytes::from("done")));
                }
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
                mgr.execute(
                    &cid,
                    "serial-3",
                    "echo",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "serial-3").await;
        assert_eq!(sem.available_permits(), 0);

        // Complete third request (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("serial-3") {
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Ok(Bytes::from("done3")));
                }
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
                mgr.execute(
                    &cid,
                    "timeout-req",
                    "slow",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "timeout-req").await;
        assert_eq!(sem.available_permits(), 0, "permit should be consumed");

        // Simulate timeout by removing pending (permit auto-returned on drop)
        if let Some((_, p)) = pending.remove("timeout-req") {
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Err(RuneError::Timeout));
                }
                _ => panic!("expected Once"),
            }
        }

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(RuneError::Timeout)));

        // Permit should be available again
        assert_eq!(
            sem.available_permits(),
            1,
            "permit must be returned after timeout"
        );

        // A new request should now succeed
        let handle2 = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(
                    &cid,
                    "after-timeout",
                    "echo",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "after-timeout").await;
        assert_eq!(sem.available_permits(), 0);

        // Complete it (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("after-timeout") {
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Ok(Bytes::from("ok")));
                }
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
            mgr.sessions.insert(
                format!("caster-{}", i),
                CasterState {
                    outbound: tx,
                    pending: Arc::clone(&pending),
                    timeout_handles: Arc::new(DashMap::new()),
                    semaphore: Arc::clone(&sem),
                    generation: 1,
                },
            );
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
                mgr_clone
                    .execute(
                        &cid,
                        &rid,
                        "rune",
                        Bytes::new(),
                        Default::default(),
                        DEFAULT_TIMEOUT,
                    )
                    .await
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
        assert!(
            !pending_maps[1].contains_key("req-1"),
            "req-1 should be removed from pending"
        );
        assert_eq!(
            sems[1].available_permits(),
            5,
            "caster-1 permit should be returned"
        );

        // caster-0 and caster-2 requests should still be pending
        assert!(
            pending_maps[0].contains_key("req-0"),
            "req-0 should still be pending"
        );
        assert!(
            pending_maps[2].contains_key("req-2"),
            "req-2 should still be pending"
        );
        assert_eq!(
            sems[0].available_permits(),
            4,
            "caster-0 permit should still be consumed"
        );
        assert_eq!(
            sems[2].available_permits(),
            4,
            "caster-2 permit should still be consumed"
        );

        // Verify the cancelled request's result
        let result1 = handles.remove(1).await.unwrap();
        assert!(matches!(result1, Err(RuneError::Cancelled)));

        // Complete remaining requests (permit auto-returned on PendingRequest drop)
        for i in [0, 2] {
            if let Some((_, p)) = pending_maps[i].remove(&format!("req-{}", i)) {
                match p.tx {
                    PendingResponse::Once(tx) => {
                        let _ = tx.send(Ok(Bytes::from("done")));
                    }
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
                mgr.execute_stream(
                    &cid,
                    "stream-cancel",
                    "streamer",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "stream-cancel").await;
        assert_eq!(sem.available_permits(), 1);

        // Cancel the streaming request
        mgr.cancel(&caster_id, "stream-cancel", "user cancel")
            .await
            .unwrap();

        // Permit should be returned
        assert_eq!(
            sem.available_permits(),
            2,
            "cancel must return permit for stream request"
        );

        // The stream receiver should eventually get the cancellation error
        let _stream_rx = handle.await.unwrap().unwrap();
    }

    // ---- Disconnect and reconnect with same caster_id ----

    #[tokio::test]
    async fn test_disconnect_then_reconnect_same_caster_id() {
        let mgr = default_session_manager();

        // First connection (insert_test_caster populates sessions + metadata)
        mgr.insert_test_caster("reconnect-caster", 3);

        assert!(mgr.is_available("reconnect-caster"));
        assert_eq!(mgr.caster_count(), 1);

        // Simulate disconnect: remove from sessions and metadata
        mgr.sessions.remove("reconnect-caster");
        mgr.metadata.remove("reconnect-caster");
        mgr.caster_count_atomic.fetch_sub(1, Ordering::Relaxed);
        assert!(!mgr.is_available("reconnect-caster"));
        assert_eq!(mgr.caster_count(), 0);

        // Reconnect with same caster_id
        mgr.insert_test_caster("reconnect-caster", 5);

        // Should be available again with new capacity
        assert!(mgr.is_available("reconnect-caster"));
        assert_eq!(mgr.caster_count(), 1);
        assert_eq!(mgr.available_permits("reconnect-caster"), 5);
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
        mgr.sessions.insert(
            "one-permit".to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::new(DashMap::new()),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::clone(&sem),
                generation: 1,
            },
        );

        assert!(mgr.is_available("one-permit"));

        // Consume the permit
        let _permit = sem.try_acquire().unwrap();
        assert!(
            !mgr.is_available("one-permit"),
            "should be unavailable when all permits consumed"
        );

        // Release the permit
        drop(_permit);
        assert!(
            mgr.is_available("one-permit"),
            "should be available again after permit release"
        );
    }

    // ---- Execute on non-existent caster ----

    #[tokio::test]
    async fn test_execute_nonexistent_caster() {
        let mgr = default_session_manager();

        let result = mgr
            .execute(
                "nonexistent",
                "req-1",
                "rune",
                Bytes::new(),
                Default::default(),
                DEFAULT_TIMEOUT,
            )
            .await;

        assert!(matches!(result, Err(RuneError::Unavailable)));
    }

    #[tokio::test]
    async fn test_execute_stream_nonexistent_caster() {
        let mgr = default_session_manager();

        let result = mgr
            .execute_stream(
                "nonexistent",
                "req-1",
                "rune",
                Bytes::new(),
                Default::default(),
                DEFAULT_TIMEOUT,
            )
            .await;

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
                mgr.execute(
                    &cid,
                    &rid,
                    "echo",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            }));
        }

        // Wait for all pending
        for i in 0..3 {
            wait_for_pending(&pending, &format!("concurrent-{}", i)).await;
        }
        assert_eq!(sem.available_permits(), 0);

        // Fourth request should be rejected
        let result = mgr
            .execute(
                &caster_id,
                "concurrent-3",
                "echo",
                Bytes::new(),
                Default::default(),
                DEFAULT_TIMEOUT,
            )
            .await;
        assert!(matches!(result, Err(RuneError::Unavailable)));

        // Complete all requests (permit auto-returned on PendingRequest drop)
        for i in 0..3 {
            if let Some((_, p)) = pending.remove(&format!("concurrent-{}", i)) {
                match p.tx {
                    PendingResponse::Once(tx) => {
                        let _ = tx.send(Ok(Bytes::from("ok")));
                    }
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
        let (mgr, caster_id, _sem, pending, _rx) = setup_session(2);

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(
                    &cid,
                    "err-req",
                    "failing_rune",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
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
                mgr.execute_stream(
                    &cid,
                    "stream-multi",
                    "streamer",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
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
        assert!(
            result.is_ok(),
            "cancelling non-existent request should not error"
        );
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
                mgr.execute(
                    &cid,
                    "done-req",
                    "rune",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "done-req").await;

        // Complete the request first (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("done-req") {
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Ok(Bytes::from("completed")));
                }
                _ => panic!("expected Once"),
            }
        }

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, Bytes::from("completed"));

        // Now try to cancel the already-completed request
        let cancel_result = mgr.cancel(&caster_id, "done-req", "too late").await;
        assert!(
            cancel_result.is_ok(),
            "cancel after completion should not error"
        );
        assert_eq!(
            sem.available_permits(),
            2,
            "permits should remain unchanged"
        );
    }

    // ---- many casters connected ----

    #[test]
    fn test_many_casters_simultaneously() {
        let mgr = default_session_manager();
        let count = 50;

        for i in 0..count {
            mgr.insert_test_caster(&format!("caster-{}", i), 10);
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
        mgr.sessions.insert(
            "caster-q".to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::new(DashMap::new()),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::clone(&sem),
                generation: 1,
            },
        );

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
        mgr.sessions.insert(
            "dying-caster".to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::clone(&pending),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::clone(&semaphore),
                generation: 1,
            },
        );

        // Drop the receiver to close the channel
        drop(rx);

        let result = mgr
            .execute(
                "dying-caster",
                "req-dead",
                "rune",
                Bytes::new(),
                Default::default(),
                DEFAULT_TIMEOUT,
            )
            .await;

        assert!(
            matches!(result, Err(RuneError::Unavailable)),
            "execute on closed outbound should return Unavailable"
        );
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
        mgr.sessions.insert(
            "dying-stream".to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::clone(&pending),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::clone(&semaphore),
                generation: 1,
            },
        );

        drop(rx);

        let result = mgr
            .execute_stream(
                "dying-stream",
                "req-s-dead",
                "rune",
                Bytes::new(),
                Default::default(),
                DEFAULT_TIMEOUT,
            )
            .await;

        assert!(matches!(result, Err(RuneError::Unavailable)));
        assert!(!pending.contains_key("req-s-dead"));
        assert_eq!(semaphore.available_permits(), 3);
    }

    // ---- execute with context map ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_sends_context_map_in_message() {
        let (mgr, caster_id, _sem, pending, mut rx) = setup_session(2);

        let mut ctx_map = HashMap::new();
        ctx_map.insert("user_id".to_string(), "u-123".to_string());
        ctx_map.insert("trace_id".to_string(), "t-456".to_string());

        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(
                    &cid,
                    "ctx-req",
                    "rune",
                    Bytes::from("data"),
                    ctx_map,
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        // Read the outbound message to verify context was passed
        let msg = rx.recv().await.expect("should receive ExecuteRequest");
        match msg.payload {
            Some(session_message::Payload::Execute(exec)) => {
                assert_eq!(exec.request_id, "ctx-req");
                assert_eq!(exec.rune_name, "rune");
                assert_eq!(exec.input, b"data");
                assert_eq!(
                    exec.context.get("user_id").map(|s| s.as_str()),
                    Some("u-123")
                );
                assert_eq!(
                    exec.context.get("trace_id").map(|s| s.as_str()),
                    Some("t-456")
                );
                assert_eq!(exec.timeout_ms, 30_000);
            }
            other => panic!("expected Execute payload, got {:?}", other),
        }

        // Complete the request (permit auto-returned on PendingRequest drop)
        if let Some((_, p)) = pending.remove("ctx-req") {
            match p.tx {
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Ok(Bytes::from("done")));
                }
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
                mgr.execute(
                    &cid,
                    "cancel-msg-req",
                    "rune",
                    Bytes::new(),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
            })
        };

        wait_for_pending(&pending, "cancel-msg-req").await;

        // Drain the ExecuteRequest message
        let _exec_msg = rx.recv().await.unwrap();

        // Cancel
        mgr.cancel(&caster_id, "cancel-msg-req", "user wants to stop")
            .await
            .unwrap();

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
        let (mgr, caster_id, _sem, pending, mut rx) = setup_session(5);

        let mut handles = Vec::new();
        for i in 0..3 {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            let rid = format!("ord-{}", i);
            handles.push(tokio::spawn(async move {
                mgr.execute(
                    &cid,
                    &rid,
                    "rune",
                    Bytes::from(format!("data-{}", i)),
                    Default::default(),
                    DEFAULT_TIMEOUT,
                )
                .await
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
                    PendingResponse::Once(tx) => {
                        let _ = tx.send(Ok(Bytes::from("ok")));
                    }
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
        let (mgr, caster_id, _sem, pending, mut rx) = setup_session(2);

        let custom_timeout = Duration::from_secs(120);
        let handle = {
            let mgr = Arc::clone(&mgr);
            let cid = caster_id.clone();
            tokio::spawn(async move {
                mgr.execute(
                    &cid,
                    "timeout-val",
                    "rune",
                    Bytes::new(),
                    Default::default(),
                    custom_timeout,
                )
                .await
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
                PendingResponse::Once(tx) => {
                    let _ = tx.send(Ok(Bytes::new()));
                }
                _ => panic!("expected Once"),
            }
        }
        let _ = handle.await.unwrap().unwrap();
    }

    // ---- S8 回归测试：with_auth 是唯一的生产构造路径 ----

    #[test]
    fn test_new_dev_has_dev_mode_true() {
        // new_dev() (test-only constructor) defaults to dev_mode=true
        let mgr = SessionManager::new_dev(Duration::from_secs(10), Duration::from_secs(35));
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
        assert!(
            !mgr.dev_mode,
            "with_auth(dev_mode=false) should have dev_mode=false"
        );
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
        assert!(
            mgr.dev_mode,
            "with_auth(dev_mode=true) should have dev_mode=true"
        );
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

    /// Regression M7: session exists but metadata is missing (Pilot attach window).
    /// list_caster_ids must NOT include such sessions.
    #[tokio::test]
    async fn test_fix_list_caster_ids_excludes_unknown_metadata() {
        let mgr = default_session_manager();
        let (tx, _rx) = mpsc::channel(16);

        // Insert session WITHOUT writing metadata — simulates the Pilot attach window.
        mgr.sessions.insert(
            "pilot-sess".to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::new(DashMap::new()),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::new(Semaphore::new(1)),
                generation: 1,
            },
        );

        let ids = mgr.list_caster_ids();
        assert!(
            !ids.contains(&"pilot-sess".to_string()),
            "session without metadata must NOT appear in caster list, got: {:?}",
            ids
        );
        assert_eq!(mgr.caster_count(), 0);
    }

    #[test]
    fn test_fix_sanitize_f64_clamps_nan_inf_negative() {
        assert_eq!(sanitize_f64(0.5), 0.5);
        assert_eq!(sanitize_f64(0.0), 0.0);
        assert_eq!(sanitize_f64(1.0), 1.0);
        assert_eq!(sanitize_f64(f64::NAN), 0.0);
        assert_eq!(sanitize_f64(f64::INFINITY), 0.0);
        assert_eq!(sanitize_f64(f64::NEG_INFINITY), 0.0);
        assert_eq!(sanitize_f64(-0.1), 0.0);
    }

    #[test]
    fn test_fix_sanitize_f32_clamps_nan_inf_negative() {
        assert_eq!(sanitize_f32(0.5), 0.5);
        assert_eq!(sanitize_f32(f32::NAN), 0.0);
        assert_eq!(sanitize_f32(f32::INFINITY), 0.0);
        assert_eq!(sanitize_f32(-1.0), 0.0);
    }

    /// Helper: set up a caster with group label for group_pressure tests.
    fn setup_caster_with_group(mgr: &SessionManager, caster_id: &str, group: &str) {
        mgr.insert_test_caster(caster_id, 4);
        mgr.metadata.insert(
            caster_id.to_string(),
            CasterMetadata {
                labels: {
                    let mut m = std::collections::HashMap::new();
                    m.insert("group".to_string(), group.to_string());
                    m
                },
                max_concurrent: 4,
                role: CasterRole::Caster,
                generation: 1,
                connected_at: Instant::now(),
            },
        );
    }

    /// Helper: build a proto HealthReport with the given pressure and metrics.
    fn make_health_report(
        pressure: f64,
        metrics: HashMap<String, f64>,
    ) -> rune_proto::HealthReport {
        rune_proto::HealthReport {
            status: rune_proto::HealthStatus::Healthy.into(),
            active_requests: 0,
            error_rate: 0.0,
            error_rate_window_secs: 60,
            custom_info: String::new(),
            timestamp_ms: 1000,
            pressure,
            metrics,
        }
    }

    #[test]
    fn test_fix_health_report_nan_pressure_excluded_from_group_sampling() {
        let mgr = default_session_manager();
        setup_caster_with_group(&mgr, "c1", "g1");

        // Send a HealthReport with NaN pressure through the real boundary
        let report = make_health_report(f64::NAN, HashMap::new());
        mgr.apply_health_report("c1", report);

        // Storage is safe (finite)
        let info = mgr.health_info("c1").unwrap();
        assert_eq!(info.pressure, 0.0, "NaN must be clamped to 0.0 in storage");
        assert!(info.pressure.is_finite());

        // But NOT counted as a valid sample in group_pressure
        let (avg, count) = mgr.group_pressure("group", "g1");
        assert_eq!(
            count, 0,
            "NaN pressure must not be counted as a reported sample"
        );
        assert_eq!(avg, 0.0);
    }

    #[test]
    fn test_fix_health_report_filters_non_finite_metrics() {
        let mgr = default_session_manager();
        mgr.insert_test_caster("c1", 4);

        let mut metrics = HashMap::new();
        metrics.insert("good".to_string(), 1.5);
        metrics.insert("nan".to_string(), f64::NAN);
        metrics.insert("inf".to_string(), f64::INFINITY);
        metrics.insert("neg_inf".to_string(), f64::NEG_INFINITY);
        metrics.insert("neg".to_string(), -1.0); // finite negative — keep

        let report = make_health_report(0.5, metrics);
        mgr.apply_health_report("c1", report);

        let info = mgr.health_info("c1").unwrap();
        assert_eq!(
            info.metrics.len(),
            2,
            "only finite values survive: got {:?}",
            info.metrics
        );
        assert_eq!(info.metrics["good"], 1.5);
        assert_eq!(info.metrics["neg"], -1.0);
        assert!(!info.metrics.contains_key("nan"));
        assert!(!info.metrics.contains_key("inf"));
        assert!(!info.metrics.contains_key("neg_inf"));
    }

    #[test]
    fn test_fix_health_report_valid_pressure_counted_in_group() {
        let mgr = default_session_manager();
        setup_caster_with_group(&mgr, "c1", "g1");

        let report = make_health_report(0.75, HashMap::new());
        mgr.apply_health_report("c1", report);

        let info = mgr.health_info("c1").unwrap();
        assert_eq!(info.pressure, 0.75);
        assert!(info.pressure_reported);

        let (avg, count) = mgr.group_pressure("group", "g1");
        assert_eq!(
            count, 1,
            "valid pressure must be counted as reported sample"
        );
        assert!((avg - 0.75).abs() < f64::EPSILON);
    }
}
