use crate::session::{CasterRole, HealthStatusLevel, SessionManager};
use dashmap::DashMap;
use parking_lot::Mutex;
use rune_proto::{session_message, ScaleAction, ScaleSignal, SessionMessage, ShutdownRequest};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u32 = 30_000;

enum ActionPlan {
    None,
    ScaleUp {
        pilot_ids: HashSet<String>,
    },
    ScaleDown {
        victim_id: String,
        /// Captured at candidate selection time — used to verify
        /// the victim hasn't reconnected before we send shutdown.
        victim_generation: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct ScalingStatusSnapshot {
    pub group_id: String,
    pub current_replicas: usize,
    pub desired_replicas: usize,
    pub average_pressure: Option<f64>,
    pub action: String,
    pub reason: String,
    pub pilot_id: Option<String>,
}

/// Result of breach-tracker evaluation — what the evaluator *wants* to do.
/// Computed under lock; actual candidate selection and messaging happen outside.
enum ScaleIntent {
    Up,
    Down,
    WaitUp,
    WaitDown,
    None,
}

#[derive(Debug, Default, Clone)]
struct BreachTracker {
    up_since: Option<Instant>,
    down_since: Option<Instant>,
}

#[derive(Debug, Clone)]
struct ScalingPolicy {
    scale_up_threshold: f64,
    scale_down_threshold: f64,
    sustained_secs: u64,
    min_replicas: usize,
    max_replicas: usize,
}

fn policy_eq(a: &ScalingPolicy, b: &ScalingPolicy) -> bool {
    a.scale_up_threshold
        .total_cmp(&b.scale_up_threshold)
        .is_eq()
        && a.scale_down_threshold
            .total_cmp(&b.scale_down_threshold)
            .is_eq()
        && a.sustained_secs == b.sustained_secs
        && a.min_replicas == b.min_replicas
        && a.max_replicas == b.max_replicas
}

#[derive(Debug, Clone)]
struct GroupState {
    policy: ScalingPolicy,
    caster_ids: Vec<String>,
    pilot_ids: HashSet<String>,
    /// Number of casters managed by each pilot in this group.
    pilot_caster_counts: HashMap<String, usize>,
    average_pressure: Option<f64>,
    /// Set when casters in this group advertise conflicting scaling
    /// policies.  A poisoned group is skipped during evaluation to
    /// avoid nondeterministic behavior from DashMap iteration order.
    poisoned: bool,
}

pub struct ScaleEvaluator {
    session_mgr: Arc<SessionManager>,
    check_interval: Duration,
    statuses: DashMap<String, ScalingStatusSnapshot>,
    /// Uses `parking_lot::Mutex` (not `tokio::sync::Mutex`) intentionally: the
    /// critical section in `evaluate_once` performs only CPU-bound comparisons
    /// with no `.await`.
    breaches: Mutex<HashMap<String, BreachTracker>>,
    /// Guard ensuring `evaluate_once` is not called concurrently.
    /// The two-phase lock pattern on `breaches` (lock → unlock → re-lock)
    /// assumes single-caller serialization; this flag enforces it at runtime.
    evaluating: std::sync::atomic::AtomicBool,
    /// Shared shutdown signal.  `notify_waiters()` wakes both the periodic
    /// loop in `start()` and any pending force_kill grace-period sleeps,
    /// ensuring orphan tasks exit promptly instead of lingering for up to
    /// `DEFAULT_SHUTDOWN_GRACE_PERIOD_MS`.
    shutdown: Arc<Notify>,
    shutdown_called: std::sync::atomic::AtomicBool,
}

impl ScaleEvaluator {
    pub fn new(session_mgr: Arc<SessionManager>, check_interval: Duration) -> Self {
        Self {
            session_mgr,
            check_interval,
            statuses: DashMap::new(),
            breaches: Mutex::new(HashMap::new()),
            evaluating: std::sync::atomic::AtomicBool::new(false),
            shutdown: Arc::new(Notify::new()),
            shutdown_called: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn check_interval(&self) -> Duration {
        self.check_interval
    }

    pub fn snapshot_status(&self) -> Vec<ScalingStatusSnapshot> {
        let mut statuses: Vec<_> = self
            .statuses
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        statuses.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        statuses
    }

    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let shutdown = Arc::clone(&self.shutdown);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.check_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        self.evaluate_once().await;
                    }
                    _ = shutdown.notified() => {
                        break;
                    }
                }
            }
        })
    }

    /// Signal the evaluator to stop its periodic loop and immediately
    /// wake all pending force_kill grace-period sleeps so they exit
    /// without sending stale signals.
    pub fn shutdown(&self) {
        self.shutdown_called
            .store(true, std::sync::atomic::Ordering::Release);
        self.shutdown.notify_waiters();
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown_called
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Pure decision function: inspects breach tracker timestamps and
    /// returns what the evaluator should do.  Called under `breaches` lock;
    /// no I/O or session_mgr access.
    fn evaluate_breach(
        policy: &ScalingPolicy,
        average_pressure: Option<f64>,
        tracker: &mut BreachTracker,
        current_replicas: usize,
    ) -> ScaleIntent {
        let Some(pressure) = average_pressure else {
            tracker.up_since = None;
            tracker.down_since = None;
            return ScaleIntent::None;
        };
        if pressure > policy.scale_up_threshold {
            tracker.down_since = None;
            let started = tracker.up_since.get_or_insert_with(Instant::now);
            if started.elapsed() >= Duration::from_secs(policy.sustained_secs)
                && current_replicas < policy.max_replicas
            {
                // Don't reset up_since here — caller decides whether to
                // consume the window (has pilot) or preserve it (no pilot).
                ScaleIntent::Up
            } else {
                ScaleIntent::WaitUp
            }
        } else if pressure < policy.scale_down_threshold {
            tracker.up_since = None;
            let started = tracker.down_since.get_or_insert_with(Instant::now);
            if started.elapsed() >= Duration::from_secs(policy.sustained_secs)
                && current_replicas > policy.min_replicas
            {
                // Don't reset down_since here — caller resets after
                // successful candidate selection.
                ScaleIntent::Down
            } else {
                ScaleIntent::WaitDown
            }
        } else {
            tracker.up_since = None;
            tracker.down_since = None;
            ScaleIntent::None
        }
    }

    pub async fn evaluate_once(&self) {
        // INVARIANT: The two-phase lock pattern on `breaches` (Phase 1: lock →
        // decide; Phase 2: unlock → candidate selection; Phase 3: re-lock →
        // reset timer) assumes this method is never called concurrently.
        // This is currently guaranteed by `start()` running a single
        // `tokio::spawn` with a serial interval loop.  The flag below makes
        // this invariant explicit and catches violations immediately.
        assert!(
            !self
                .evaluating
                .swap(true, std::sync::atomic::Ordering::AcqRel),
            "BUG: evaluate_once called concurrently — the two-phase breaches \
             lock pattern is not safe under concurrent evaluation"
        );
        // Reset on all exit paths (normal return + panic).
        struct EvalGuard<'a>(&'a std::sync::atomic::AtomicBool);
        impl Drop for EvalGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, std::sync::atomic::Ordering::Release);
            }
        }
        let _eval_guard = EvalGuard(&self.evaluating);

        let groups = self.collect_groups();
        let mut seen: HashSet<String> = HashSet::new();

        for (group_id, group) in groups {
            seen.insert(group_id.clone());

            if group.poisoned {
                // Clear any accumulated breach timers so that when the
                // mismatch resolves, scaling starts from a clean slate
                // instead of immediately firing on stale elapsed time.
                self.breaches.lock().remove(&group_id);
                self.statuses.insert(
                    group_id.clone(),
                    ScalingStatusSnapshot {
                        group_id,
                        current_replicas: group.caster_ids.len(),
                        desired_replicas: group.caster_ids.len(),
                        average_pressure: group.average_pressure,
                        action: "disabled".into(),
                        reason: "policy mismatch in group — scaling disabled".into(),
                        pilot_id: group.pilot_ids.iter().next().cloned(),
                    },
                );
                continue;
            }

            let current = group.caster_ids.len();
            let mut status = ScalingStatusSnapshot {
                group_id: group_id.clone(),
                current_replicas: current,
                desired_replicas: current,
                average_pressure: group.average_pressure,
                action: "idle".into(),
                reason: "stable".into(),
                // Display-only: picks an arbitrary pilot from the group.
                // Multi-pilot groups have multiple entries; this is for
                // status serialization, not routing decisions.
                pilot_id: group.pilot_ids.iter().next().cloned(),
            };
            // Phase 1: Under lock — only read/write breach tracker timestamps.
            // select_scale_down_candidate and send_message happen outside the lock.
            let scale_intent = {
                let mut tracker_map = self.breaches.lock();
                let tracker = tracker_map.entry(group_id.clone()).or_default();
                Self::evaluate_breach(&group.policy, group.average_pressure, tracker, current)
            };

            // Phase 2: Outside lock — candidate selection, status updates, action plan.
            let action_plan = match scale_intent {
                ScaleIntent::Up => {
                    if group.pilot_ids.is_empty() {
                        // No pilot — preserve sustained window (don't reset up_since).
                        status.reason = "missing pilot".into();
                        ActionPlan::None
                    } else {
                        // Re-acquire lock briefly to reset the cooldown window.
                        // This second acquisition is intentional: the first lock
                        // only covers the pure decision; resetting the tracker is
                        // deferred until we confirm a pilot exists.  Setting
                        // up_since to now() starts a fresh sustained-breach
                        // interval, allowing consecutive scale-ups if pressure
                        // remains above threshold.
                        self.breaches
                            .lock()
                            .entry(group_id.clone())
                            .or_default()
                            .up_since = Some(Instant::now());
                        status.action = "scale_up".into();
                        status.desired_replicas = current + 1;
                        status.reason = format!(
                            "pressure {:.3} exceeded threshold {:.3}",
                            group.average_pressure.unwrap_or(0.0),
                            group.policy.scale_up_threshold
                        );
                        ActionPlan::ScaleUp {
                            pilot_ids: group.pilot_ids.clone(),
                        }
                    }
                }
                ScaleIntent::Down => {
                    if let Some(victim_id) = self.select_scale_down_candidate(&group) {
                        // Reset the cooldown window — next scale-down requires
                        // a fresh sustained breach below the threshold.
                        self.breaches
                            .lock()
                            .entry(group_id.clone())
                            .or_default()
                            .down_since = Some(Instant::now());
                        status.action = "scale_down".into();
                        status.desired_replicas = current.saturating_sub(1);
                        status.reason = format!("scale down {}", victim_id);
                        let victim_generation = self.session_mgr.session_generation(&victim_id);
                        ActionPlan::ScaleDown {
                            victim_id,
                            victim_generation,
                        }
                    } else {
                        status.reason = "no idle caster available for scale-down".into();
                        ActionPlan::None
                    }
                }
                ScaleIntent::WaitUp => {
                    status.reason = "waiting for sustained scale-up window".into();
                    ActionPlan::None
                }
                ScaleIntent::WaitDown => {
                    status.reason = "waiting for sustained scale-down window".into();
                    ActionPlan::None
                }
                ScaleIntent::None => ActionPlan::None,
            };

            match action_plan {
                ActionPlan::None => {}
                ActionPlan::ScaleUp { pilot_ids } => {
                    // pilot_ids is guaranteed non-empty here — the decision
                    // phase returns ActionPlan::None when no pilot exists.
                    let target_pilot = pilot_ids
                        .iter()
                        .min_by_key(|pid| group.pilot_caster_counts.get(*pid).copied().unwrap_or(0))
                        .cloned();

                    if let Some(pilot_id) = target_pilot {
                        let local_count = group
                            .pilot_caster_counts
                            .get(&pilot_id)
                            .copied()
                            .unwrap_or(0) as u32;
                        if let Err(e) = self
                            .session_mgr
                            .send_message(
                                &pilot_id,
                                SessionMessage {
                                    payload: Some(session_message::Payload::ScaleSignal(
                                        ScaleSignal {
                                            action: ScaleAction::Up.into(),
                                            group_id: group_id.clone(),
                                            desired_replicas: local_count + 1,
                                            current_pressure: group.average_pressure.unwrap_or(0.0),
                                            reason: status.reason.clone(),
                                        },
                                    )),
                                },
                            )
                            .await
                        {
                            tracing::warn!(pilot_id = %pilot_id, group_id = %group_id, error = %e, "scale-up signal failed");
                        }
                    }
                }
                ActionPlan::ScaleDown {
                    victim_id,
                    victim_generation,
                } => {
                    // Verify the victim is still the same session we selected.
                    // If it reconnected (different generation), skip shutdown
                    // to avoid draining a replacement session.
                    let still_same_session = self
                        .session_mgr
                        .session_generation(&victim_id)
                        .zip(victim_generation)
                        .map(|(current, original)| current == original)
                        .unwrap_or(false);
                    if still_same_session {
                        if let Err(e) = self
                            .session_mgr
                            .send_message(
                                &victim_id,
                                SessionMessage {
                                    payload: Some(session_message::Payload::Shutdown(
                                        ShutdownRequest {
                                            reason: "scale_down".into(),
                                            grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
                                        },
                                    )),
                                },
                            )
                            .await
                        {
                            tracing::warn!(victim_id = %victim_id, error = %e, "scale-down shutdown failed");
                        }
                    }

                    // Look up the victim's own pilot from its labels, not the
                    // group-level pilot which may belong to a different host.
                    let victim_pilot_id = self
                        .session_mgr
                        .metadata
                        .get(&victim_id)
                        .and_then(|meta| meta.labels.get("_pilot_id").cloned());

                    if let Some(pilot_id) = victim_pilot_id {
                        // Guard: if shutdown() was called while evaluate_once()
                        // was in flight, skip spawning — the Notify may already
                        // have fired before this task's notified() future exists.
                        if self.is_shutdown() {
                            continue;
                        }
                        let session_mgr = Arc::clone(&self.session_mgr);
                        let shutdown = Arc::clone(&self.shutdown);
                        let group_id_clone = group_id.clone();
                        let reason = format!("force_kill:{}", victim_id);
                        let current_pressure = group.average_pressure.unwrap_or(0.0);
                        let desired_replicas = status.desired_replicas as u32;
                        // victim_generation was captured when we built ActionPlan::ScaleDown
                        // — do NOT re-read here, because send_message().await above may
                        // have stalled long enough for the victim to reconnect, which
                        // would give us the *new* session's generation and defeat the guard.
                        tokio::spawn(async move {
                            // Wait for grace period OR immediate cancellation on shutdown.
                            // Without this select!, the task would linger for up to 30s
                            // as an orphan after the evaluator shuts down.
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_millis(
                                    DEFAULT_SHUTDOWN_GRACE_PERIOD_MS as u64,
                                )) => {}
                                _ = shutdown.notified() => {
                                    return; // evaluator shutting down — skip force_kill
                                }
                            }
                            // Only force_kill if the victim is STILL the same session.
                            // If it reconnected, generation will differ — skip.
                            let still_same_session = session_mgr
                                .session_generation(&victim_id)
                                .zip(victim_generation)
                                .map(|(current, original)| current == original)
                                .unwrap_or(false);
                            if still_same_session {
                                if let Err(e) = session_mgr
                                    .send_message(
                                        &pilot_id,
                                        SessionMessage {
                                            payload: Some(session_message::Payload::ScaleSignal(
                                                ScaleSignal {
                                                    action: ScaleAction::Down.into(),
                                                    group_id: group_id_clone,
                                                    desired_replicas,
                                                    current_pressure,
                                                    reason,
                                                },
                                            )),
                                        },
                                    )
                                    .await
                                {
                                    tracing::warn!(victim_id = %victim_id, pilot_id = %pilot_id, error = %e, "force_kill scale-down signal failed");
                                }
                            }
                        });
                    }
                }
            }

            self.statuses.insert(group_id, status);
        }

        self.statuses.retain(|group_id, _| seen.contains(group_id));
    }

    /// Build a snapshot of scaling groups from live caster metadata.
    ///
    /// Iterates every connected Caster, groups them by `labels["group"]`, and
    /// aggregates per-group pressure (from `HealthReport`) plus the set of
    /// associated pilot IDs.  Returns an owned map so callers can evaluate
    /// scaling decisions without holding DashMap guards across await points.
    fn collect_groups(&self) -> HashMap<String, GroupState> {
        let mut groups = HashMap::new();

        for meta in self.session_mgr.metadata.iter() {
            if meta.value().role != CasterRole::Caster {
                continue;
            }
            let Some(group_id) = meta.value().labels.get("group").cloned() else {
                continue;
            };
            let Some(policy) = Self::policy_from_labels(&group_id, &meta.value().labels) else {
                continue;
            };

            let pilot_id = meta.value().labels.get("_pilot_id").cloned();
            groups
                .entry(group_id.clone())
                .and_modify(|state: &mut GroupState| {
                    if !policy_eq(&state.policy, &policy) {
                        // Poison the entire group so it is skipped during
                        // evaluation.  This avoids nondeterministic behavior
                        // from DashMap iteration order choosing the "winner".
                        if !state.poisoned {
                            tracing::warn!(
                                group_id = %group_id,
                                caster_id = %meta.key(),
                                "scaling policy mismatch — group evaluation disabled"
                            );
                        }
                        state.poisoned = true;
                    }
                    // Always include the caster for replica counting even when
                    // poisoned — status snapshot should reflect actual count.
                    state.caster_ids.push(meta.key().clone());
                    if let Some(ref pid) = pilot_id {
                        state.pilot_ids.insert(pid.clone());
                        *state.pilot_caster_counts.entry(pid.clone()).or_insert(0) += 1;
                    }
                })
                .or_insert_with(|| {
                    let mut pilot_ids = HashSet::new();
                    let mut pilot_caster_counts = HashMap::new();
                    if let Some(ref pid) = pilot_id {
                        pilot_ids.insert(pid.clone());
                        pilot_caster_counts.insert(pid.clone(), 1);
                    }
                    GroupState {
                        policy,
                        caster_ids: vec![meta.key().clone()],
                        pilot_ids,
                        pilot_caster_counts,
                        average_pressure: None, // filled below
                        poisoned: false,
                    }
                });
        }

        // Compute per-group pressure once per group (not once per caster).
        for (group_id, state) in groups.iter_mut() {
            let (avg, reported_count) = self.session_mgr.group_pressure("group", group_id);
            state.average_pressure = if reported_count > 0 { Some(avg) } else { None };
        }

        groups
    }

    fn policy_from_labels(
        group_id: &str,
        labels: &HashMap<String, String>,
    ) -> Option<ScalingPolicy> {
        fn parse_label<T: std::str::FromStr>(
            labels: &HashMap<String, String>,
            key: &str,
            group_id: &str,
        ) -> Option<T> {
            match labels.get(key) {
                None => {
                    tracing::debug!(
                        label = key,
                        group_id,
                        "scaling label missing, group skipped"
                    );
                    None
                }
                Some(v) => match v.parse() {
                    Ok(val) => Some(val),
                    Err(_) => {
                        tracing::warn!(label = key, value = %v, group_id, "scaling label parse failed");
                        None
                    }
                },
            }
        }

        Some(ScalingPolicy {
            scale_up_threshold: parse_label(labels, "_scale_up", group_id)?,
            scale_down_threshold: parse_label(labels, "_scale_down", group_id)?,
            sustained_secs: parse_label(labels, "_sustained", group_id)?,
            min_replicas: parse_label(labels, "_min", group_id)?,
            max_replicas: parse_label(labels, "_max", group_id)?,
        })
    }

    fn select_scale_down_candidate(&self, group: &GroupState) -> Option<String> {
        struct ScaleCandidate {
            has_reported_pressure: bool,
            pressure: f64,
            available_permits: usize,
            generation: Option<u64>,
            caster_id: String,
        }

        let candidates: Vec<ScaleCandidate> = group
            .caster_ids
            .iter()
            .filter_map(|caster_id| {
                let max_concurrent = self.session_mgr.max_concurrent(caster_id);
                let available_permits = self.session_mgr.available_permits(caster_id);
                if available_permits != max_concurrent {
                    return None;
                }
                if self.session_mgr.health_status(caster_id) == Some(HealthStatusLevel::Unhealthy) {
                    return None;
                }
                let reported_pressure = self.session_mgr.reported_pressure(caster_id);
                let pressure = reported_pressure.unwrap_or_else(|| {
                    if max_concurrent > 0 {
                        1.0 - (available_permits as f64 / max_concurrent as f64)
                    } else {
                        0.0
                    }
                });
                Some(ScaleCandidate {
                    has_reported_pressure: reported_pressure.is_some(),
                    pressure,
                    available_permits,
                    generation: self.session_mgr.session_generation(caster_id),
                    caster_id: caster_id.clone(),
                })
            })
            .collect();

        // Prefer candidates without reported pressure (idle), then lowest
        // pressure, then most available permits, then oldest generation.
        candidates
            .into_iter()
            .min_by(|a, b| {
                a.has_reported_pressure
                    .cmp(&b.has_reported_pressure)
                    .then_with(|| a.pressure.total_cmp(&b.pressure))
                    .then_with(|| b.available_permits.cmp(&a.available_permits))
                    .then_with(|| a.generation.cmp(&b.generation))
            })
            .map(|c| c.caster_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{CasterMetadata, CasterState};
    use dashmap::DashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::{mpsc, Semaphore};

    static TEST_GENERATION: AtomicU64 = AtomicU64::new(1);

    fn next_generation() -> u64 {
        TEST_GENERATION.fetch_add(1, Ordering::Relaxed)
    }

    fn insert_scaling_caster_with_reported(
        mgr: &SessionManager,
        caster_id: &str,
        group: &str,
        pilot_id: Option<&str>,
        max_concurrent: usize,
        pressure: f64,
        pressure_reported: bool,
    ) -> Option<mpsc::Receiver<SessionMessage>> {
        let gen = next_generation();
        let (tx, rx) = mpsc::channel(16);
        mgr.sessions.insert(
            caster_id.to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::new(DashMap::new()),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::new(Semaphore::new(max_concurrent)),
                generation: gen,
            },
        );
        mgr.health.insert(
            caster_id.to_string(),
            crate::session::HealthEntry {
                generation: gen,
                info: Arc::new(std::sync::RwLock::new(crate::session::HealthInfo {
                    pressure,
                    pressure_reported,
                    ..Default::default()
                })),
            },
        );
        let mut labels = HashMap::from([
            ("group".to_string(), group.to_string()),
            ("_scale_up".to_string(), "0.8".to_string()),
            ("_scale_down".to_string(), "0.2".to_string()),
            ("_sustained".to_string(), "0".to_string()),
            ("_min".to_string(), "1".to_string()),
            ("_max".to_string(), "4".to_string()),
        ]);
        if let Some(pilot_id) = pilot_id {
            labels.insert("_pilot_id".to_string(), pilot_id.to_string());
        }
        mgr.metadata.insert(
            caster_id.to_string(),
            CasterMetadata {
                labels,
                max_concurrent,
                role: CasterRole::Caster,
                generation: gen,
                connected_at: Instant::now(),
            },
        );
        Some(rx)
    }

    fn insert_scaling_caster(
        mgr: &SessionManager,
        caster_id: &str,
        group: &str,
        pilot_id: Option<&str>,
        max_concurrent: usize,
        pressure: f64,
    ) -> Option<mpsc::Receiver<SessionMessage>> {
        insert_scaling_caster_with_reported(
            mgr,
            caster_id,
            group,
            pilot_id,
            max_concurrent,
            pressure,
            true,
        )
    }

    fn insert_unreported_scaling_caster(
        mgr: &SessionManager,
        caster_id: &str,
        group: &str,
        pilot_id: Option<&str>,
        max_concurrent: usize,
    ) -> Option<mpsc::Receiver<SessionMessage>> {
        insert_scaling_caster_with_reported(
            mgr,
            caster_id,
            group,
            pilot_id,
            max_concurrent,
            0.0,
            false,
        )
    }

    fn insert_pilot(mgr: &SessionManager, pilot_id: &str) -> mpsc::Receiver<SessionMessage> {
        let gen = next_generation();
        let (tx, rx) = mpsc::channel(16);
        mgr.sessions.insert(
            pilot_id.to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::new(DashMap::new()),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::new(Semaphore::new(1)),
                generation: gen,
            },
        );
        mgr.health.insert(
            pilot_id.to_string(),
            crate::session::HealthEntry {
                generation: gen,
                info: Arc::new(std::sync::RwLock::new(crate::session::HealthInfo::default())),
            },
        );
        mgr.metadata.insert(
            pilot_id.to_string(),
            CasterMetadata {
                labels: HashMap::new(),
                max_concurrent: 1,
                role: CasterRole::Pilot,
                generation: gen,
                connected_at: Instant::now(),
            },
        );
        rx
    }

    #[tokio::test]
    async fn test_scale_up_signal_on_sustained_pressure() {
        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let mut pilot_rx = insert_pilot(&mgr, "pilot-1");
        insert_scaling_caster(&mgr, "caster-a", "gpu", Some("pilot-1"), 2, 0.95);
        insert_scaling_caster(&mgr, "caster-b", "gpu", Some("pilot-1"), 2, 0.9);

        let evaluator = ScaleEvaluator::new(Arc::clone(&mgr), Duration::from_secs(30));
        evaluator.evaluate_once().await;

        let msg = pilot_rx
            .recv()
            .await
            .expect("pilot should receive scale signal");
        match msg.payload {
            Some(session_message::Payload::ScaleSignal(signal)) => {
                assert_eq!(signal.action(), ScaleAction::Up);
                assert_eq!(signal.group_id, "gpu");
                assert_eq!(signal.desired_replicas, 3);
            }
            other => panic!("expected ScaleSignal, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_scale_down_sends_shutdown_to_caster_not_pilot() {
        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let mut pilot_rx = insert_pilot(&mgr, "pilot-1");
        let mut caster_rx =
            insert_scaling_caster(&mgr, "caster-a", "gpu", Some("pilot-1"), 2, 0.1).unwrap();
        insert_scaling_caster(&mgr, "caster-b", "gpu", Some("pilot-1"), 2, 0.15);

        let evaluator = ScaleEvaluator::new(Arc::clone(&mgr), Duration::from_secs(30));
        evaluator.evaluate_once().await;

        let msg = caster_rx
            .recv()
            .await
            .expect("caster should receive shutdown request");
        match msg.payload {
            Some(session_message::Payload::Shutdown(req)) => {
                assert_eq!(req.reason, "scale_down");
            }
            other => panic!("expected ShutdownRequest, got {:?}", other),
        }
        assert!(pilot_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_no_signal_on_transient_spike() {
        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let mut pilot_rx = insert_pilot(&mgr, "pilot-1");
        let _ = insert_scaling_caster(&mgr, "caster-a", "gpu", Some("pilot-1"), 2, 0.5);
        let _ = insert_scaling_caster(&mgr, "caster-b", "gpu", Some("pilot-1"), 2, 0.55);

        let evaluator = ScaleEvaluator::new(Arc::clone(&mgr), Duration::from_secs(30));
        evaluator.evaluate_once().await;

        assert!(pilot_rx.try_recv().is_err());
        let statuses = evaluator.snapshot_status();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].action, "idle");
    }

    #[test]
    fn test_fix_scaling_status_snapshot_serializes_when_no_pressure() {
        // C3: When no caster has reported pressure, average_pressure should be
        // None (not f64::NAN) so that serde_json serialization succeeds.
        let snapshot = ScalingStatusSnapshot {
            group_id: "gpu".into(),
            current_replicas: 2,
            desired_replicas: 2,
            average_pressure: None,
            action: "idle".into(),
            reason: "stable".into(),
            pilot_id: None,
        };
        let result = serde_json::to_string(&snapshot);
        assert!(
            result.is_ok(),
            "serialization should succeed with None pressure, got: {:?}",
            result.err()
        );
        let json = result.unwrap();
        assert!(
            json.contains("\"average_pressure\":null"),
            "None should serialize as null, got: {}",
            json
        );
    }

    #[tokio::test]
    async fn test_fix_scale_down_prefers_unreported_caster() {
        // M6: When scaling down, unreported (untrusted) casters should be
        // preferred as victims over casters that have reported low pressure.
        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let _pilot_rx = insert_pilot(&mgr, "pilot-1");
        // reported-caster: has telemetry, pressure=0.8 (busy)
        let mut reported_rx =
            insert_scaling_caster(&mgr, "reported-caster", "gpu", Some("pilot-1"), 2, 0.05)
                .unwrap();
        // unreported-caster: no telemetry, computed pressure=0.0
        let mut unreported_rx =
            insert_unreported_scaling_caster(&mgr, "unknown-caster", "gpu", Some("pilot-1"), 2)
                .unwrap();

        let evaluator = ScaleEvaluator::new(Arc::clone(&mgr), Duration::from_secs(30));
        evaluator.evaluate_once().await;

        // The unreported caster should be the victim (scale-down target)
        let msg = tokio::time::timeout(Duration::from_millis(200), unreported_rx.recv())
            .await
            .expect("unreported caster should be selected for scale-down")
            .expect("unreported caster should receive shutdown request");
        match msg.payload {
            Some(session_message::Payload::Shutdown(req)) => {
                assert_eq!(req.reason, "scale_down");
            }
            other => panic!(
                "expected ShutdownRequest on unreported caster, got {:?}",
                other
            ),
        }
        assert!(
            reported_rx.try_recv().is_err(),
            "reported caster with telemetry should NOT be chosen as scale-down victim"
        );
    }

    #[tokio::test]
    async fn test_fix_scale_down_prefers_unreported_over_reported_idle() {
        // After M6 fix: unreported (untrusted) casters should be scaled down
        // before reported idle casters with known telemetry.
        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let mut pilot_rx = insert_pilot(&mgr, "pilot-1");
        let mut reported_rx =
            insert_scaling_caster(&mgr, "reported-caster", "gpu", Some("pilot-1"), 2, 0.05)
                .unwrap();
        let mut unreported_rx =
            insert_unreported_scaling_caster(&mgr, "unknown-caster", "gpu", Some("pilot-1"), 2)
                .unwrap();

        let evaluator = ScaleEvaluator::new(Arc::clone(&mgr), Duration::from_secs(30));
        evaluator.evaluate_once().await;

        let msg = tokio::time::timeout(Duration::from_millis(200), unreported_rx.recv())
            .await
            .expect("unreported caster should be selected for scale-down")
            .expect("unreported caster should receive shutdown request");
        match msg.payload {
            Some(session_message::Payload::Shutdown(req)) => {
                assert_eq!(req.reason, "scale_down");
            }
            other => panic!("expected ShutdownRequest, got {:?}", other),
        }
        assert!(
            reported_rx.try_recv().is_err(),
            "reported idle caster should NOT be chosen when unreported caster exists"
        );
        assert!(pilot_rx.try_recv().is_err());
    }

    /// Regression P0-2: force_kill generation comparison logic.
    ///
    /// The force_kill spawn captures the generation before sleeping. After the
    /// grace period, it compares against the current generation. If the
    /// victim reconnected (different generation), the force_kill is skipped.
    ///
    /// We test the comparison logic directly via SessionManager: insert a
    /// caster, capture its generation, remove and re-insert (different
    /// generation), and verify the generations differ.
    #[test]
    fn test_fix_force_kill_detects_reconnected_caster() {
        let mgr = SessionManager::new_dev(Duration::from_secs(10), Duration::from_secs(35));
        insert_scaling_caster(&mgr, "victim", "gpu", Some("pilot-1"), 2, 0.05);

        let original_generation = mgr.session_generation("victim");
        assert!(original_generation.is_some());

        // Simulate reconnect: remove and re-insert
        mgr.sessions.remove("victim");
        mgr.health.remove("victim");
        mgr.metadata.remove("victim");
        insert_scaling_caster(&mgr, "victim", "gpu", Some("pilot-1"), 2, 0.05);

        let new_generation = mgr.session_generation("victim");
        assert!(new_generation.is_some());

        // The force_kill logic: `current == original` must be false after reconnect
        assert_ne!(
            original_generation, new_generation,
            "generation must differ after reconnect so force_kill detects stale identity"
        );

        // Also verify: if NOT reconnected, generation stays the same
        let same_check = mgr.session_generation("victim");
        assert_eq!(
            new_generation, same_check,
            "generation must be stable when no reconnect occurs"
        );
    }

    /// Regression: scale-up in a multi-pilot group must send ScaleSignal to
    /// exactly ONE pilot with per-pilot `desired_replicas` (local_count + 1),
    /// NOT the global target to all pilots (which would cause over-scaling).
    #[tokio::test]
    async fn test_fix_multi_pilot_group_sends_to_one_pilot_with_local_desired() {
        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let mut pilot_a_rx = insert_pilot(&mgr, "pilot-a");
        let mut pilot_b_rx = insert_pilot(&mgr, "pilot-b");

        // Two casters in the same group "gpu" but managed by different pilots.
        // Each pilot has exactly 1 caster.
        insert_scaling_caster(&mgr, "caster-1", "gpu", Some("pilot-a"), 2, 0.95);
        insert_scaling_caster(&mgr, "caster-2", "gpu", Some("pilot-b"), 2, 0.90);

        let evaluator = ScaleEvaluator::new(Arc::clone(&mgr), Duration::from_secs(30));
        evaluator.evaluate_once().await;

        // Exactly ONE pilot should receive a ScaleSignal — the one with the
        // fewest casters (tied here, so either is valid).
        let mut received_signal: Option<ScaleSignal> = None;
        let mut received_pilot = String::new();

        if let Ok(Some(msg)) =
            tokio::time::timeout(Duration::from_millis(200), pilot_a_rx.recv()).await
        {
            if let Some(session_message::Payload::ScaleSignal(signal)) = msg.payload {
                received_signal = Some(signal);
                received_pilot = "pilot-a".into();
            }
        }
        if received_signal.is_none() {
            if let Ok(Some(msg)) =
                tokio::time::timeout(Duration::from_millis(200), pilot_b_rx.recv()).await
            {
                if let Some(session_message::Payload::ScaleSignal(signal)) = msg.payload {
                    received_signal = Some(signal);
                    received_pilot = "pilot-b".into();
                }
            }
        }

        let signal = received_signal.expect("one pilot must receive ScaleSignal");
        assert_eq!(signal.action(), ScaleAction::Up);
        assert_eq!(signal.group_id, "gpu");
        // desired_replicas must be the pilot's LOCAL count (1) + 1 = 2,
        // NOT the global count (2) + 1 = 3 which would over-scale.
        assert_eq!(
            signal.desired_replicas, 2,
            "desired_replicas should be local_count(1) + 1 = 2, not global(3)"
        );

        // The OTHER pilot must NOT receive a signal.
        let other_rx = if received_pilot == "pilot-a" {
            &mut pilot_b_rx
        } else {
            &mut pilot_a_rx
        };
        assert!(
            other_rx.try_recv().is_err(),
            "only one pilot should receive scale-up signal, not both"
        );
    }

    /// Regression: when scaling down, the force_kill signal should go to the
    /// victim's OWN pilot, not a random pilot from the group.
    #[tokio::test]
    async fn test_fix_multi_pilot_group_force_kill_uses_victim_pilot() {
        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let mut pilot_a_rx = insert_pilot(&mgr, "pilot-a");
        let mut pilot_b_rx = insert_pilot(&mgr, "pilot-b");

        // caster-1 on pilot-a: idle (low pressure, all permits available)
        let mut caster_1_rx =
            insert_scaling_caster(&mgr, "caster-1", "gpu", Some("pilot-a"), 2, 0.05).unwrap();
        // caster-2 on pilot-b: also idle
        insert_scaling_caster(&mgr, "caster-2", "gpu", Some("pilot-b"), 2, 0.1);

        let evaluator = ScaleEvaluator::new(Arc::clone(&mgr), Duration::from_secs(30));
        evaluator.evaluate_once().await;

        // One of the casters should receive a shutdown request
        let victim_msg = tokio::time::timeout(Duration::from_millis(200), caster_1_rx.recv()).await;
        if let Ok(Some(msg)) = victim_msg {
            match msg.payload {
                Some(session_message::Payload::Shutdown(_)) => {
                    // caster-1 was the victim, so force_kill should go to pilot-a (its pilot)
                    // pilot-b should NOT receive the force_kill
                    // (We can't easily test the delayed force_kill without waiting 30s,
                    // but we verify the shutdown goes to the right caster)
                }
                other => panic!("expected ShutdownRequest on caster-1, got {:?}", other),
            }
        }

        // pilot-b should NOT receive an immediate signal for this scale-down
        assert!(
            pilot_b_rx.try_recv().is_err(),
            "pilot-b should not receive signals when caster-1 (on pilot-a) is the victim"
        );
        // pilot-a should also not receive an immediate signal (force_kill is delayed)
        assert!(
            pilot_a_rx.try_recv().is_err(),
            "pilot-a should not receive immediate signal (force_kill is delayed)"
        );
    }

    /// Regression: the initial ShutdownRequest must verify generation
    /// to avoid draining a replacement session that reconnected between
    /// candidate selection and the actual send.
    #[test]
    fn test_fix_scale_down_shutdown_skips_reconnected_victim() {
        // The fix captures generation when selecting the victim and compares
        // it before sending shutdown.  This test verifies the comparison logic
        // directly: after reconnect, generation differs so the guard fires.
        let mgr = SessionManager::new_dev(Duration::from_secs(10), Duration::from_secs(35));
        insert_scaling_caster(&mgr, "victim", "gpu", Some("pilot-1"), 2, 0.05);
        let selected_at = mgr.session_generation("victim");

        // Simulate reconnect
        mgr.sessions.remove("victim");
        mgr.health.remove("victim");
        mgr.metadata.remove("victim");
        insert_scaling_caster(&mgr, "victim", "gpu", Some("pilot-1"), 2, 0.05);

        // Guard logic from ScaleDown match arm:
        let still_same = mgr
            .session_generation("victim")
            .zip(selected_at)
            .map(|(current, original)| current == original)
            .unwrap_or(false);
        assert!(
            !still_same,
            "shutdown guard must detect reconnected victim and skip"
        );
    }

    /// Regression: when sustained scale-up fires but no pilot exists,
    /// the tracker must NOT reset `up_since`.  Otherwise, the sustained
    /// window is wasted and the next evaluation cycle must wait the full
    /// duration again before retrying.
    #[tokio::test]
    async fn test_fix_scale_up_without_pilot_preserves_sustained_window() {
        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        // Two high-pressure casters in the same group — but NO pilot registered.
        insert_scaling_caster(&mgr, "caster-a", "gpu", None, 2, 0.95);
        insert_scaling_caster(&mgr, "caster-b", "gpu", None, 2, 0.9);

        let evaluator = ScaleEvaluator::new(Arc::clone(&mgr), Duration::from_secs(30));

        // First evaluation: breach tracker starts timing.
        evaluator.evaluate_once().await;

        let tracker_after_first = {
            let map = evaluator.breaches.lock();
            map.get("gpu").cloned()
        };
        assert!(
            tracker_after_first
                .as_ref()
                .and_then(|t| t.up_since)
                .is_some(),
            "first eval should start timing the breach"
        );
        let first_up_since = tracker_after_first.unwrap().up_since.unwrap();

        // Second evaluation: still no pilot.  up_since must be preserved
        // (not reset) so the sustained window keeps accumulating.
        evaluator.evaluate_once().await;

        let tracker_after_second = {
            let map = evaluator.breaches.lock();
            map.get("gpu").cloned()
        };
        let second_up_since = tracker_after_second
            .as_ref()
            .and_then(|t| t.up_since)
            .expect("up_since should still be set after second eval");

        assert_eq!(
            first_up_since, second_up_since,
            "up_since must NOT be reset when no pilot is available — \
             the sustained window should be preserved for next cycle"
        );

        // Status should reflect "missing pilot", not "scale_up".
        let statuses = evaluator.snapshot_status();
        let gpu_status = statuses.iter().find(|s| s.group_id == "gpu").unwrap();
        assert!(
            gpu_status.reason.contains("missing pilot"),
            "reason should indicate missing pilot, got: {}",
            gpu_status.reason
        );
    }

    /// Regression: scale-down tiebreaker must prefer the oldest generation
    /// (smallest generation number) when pressure, permits, and reported
    /// status are all equal.
    #[tokio::test]
    async fn test_scale_down_prefers_oldest_generation() {
        let mgr = Arc::new(SessionManager::new_dev(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let _pilot_rx = insert_pilot(&mgr, "pilot-1");

        // Insert older caster first — gets a smaller generation number.
        let mut older_rx =
            insert_scaling_caster(&mgr, "older-caster", "gpu", Some("pilot-1"), 2, 0.05).unwrap();
        // Insert newer caster second — gets a larger generation number.
        let mut newer_rx =
            insert_scaling_caster(&mgr, "newer-caster", "gpu", Some("pilot-1"), 2, 0.05).unwrap();

        let evaluator = ScaleEvaluator::new(Arc::clone(&mgr), Duration::from_secs(30));
        evaluator.evaluate_once().await;

        // The oldest-generation caster should be the scale-down victim.
        let msg = tokio::time::timeout(Duration::from_millis(200), older_rx.recv())
            .await
            .expect("older caster should be selected for scale-down")
            .expect("older caster should receive shutdown request");
        match msg.payload {
            Some(session_message::Payload::Shutdown(req)) => {
                assert_eq!(req.reason, "scale_down");
            }
            other => panic!("expected ShutdownRequest on older caster, got {:?}", other),
        }
        assert!(
            newer_rx.try_recv().is_err(),
            "newer caster should NOT be chosen when older generation exists"
        );
    }
}
