use crate::session::{CasterRole, HealthStatusLevel, SessionManager};
use dashmap::DashMap;
use rune_proto::{session_message, ScaleAction, ScaleSignal, SessionMessage, ShutdownRequest};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u32 = 30_000;

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

#[derive(Debug, Default, Clone)]
struct BreachTracker {
    up_since: Option<Instant>,
    down_since: Option<Instant>,
}

#[derive(Debug, Clone)]
struct ScalingPolicy {
    pilot_id: Option<String>,
    scale_up_threshold: f64,
    scale_down_threshold: f64,
    sustained_secs: u64,
    min_replicas: usize,
    max_replicas: usize,
}

#[derive(Debug, Clone)]
struct GroupState {
    policy: ScalingPolicy,
    caster_ids: Vec<String>,
    average_pressure: Option<f64>,
}

pub struct ScaleEvaluator {
    session_mgr: Arc<SessionManager>,
    check_interval: Duration,
    statuses: DashMap<String, ScalingStatusSnapshot>,
    breaches: Mutex<HashMap<String, BreachTracker>>,
}

impl ScaleEvaluator {
    pub fn new(session_mgr: Arc<SessionManager>, check_interval: Duration) -> Self {
        Self {
            session_mgr,
            check_interval,
            statuses: DashMap::new(),
            breaches: Mutex::new(HashMap::new()),
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
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.check_interval);
            loop {
                interval.tick().await;
                self.evaluate_once().await;
            }
        })
    }

    pub async fn evaluate_once(&self) {
        let groups = self.collect_groups();
        let mut seen = Vec::new();

        for (group_id, group) in groups {
            seen.push(group_id.clone());
            let current = group.caster_ids.len();
            let mut status = ScalingStatusSnapshot {
                group_id: group_id.clone(),
                current_replicas: current,
                desired_replicas: current,
                average_pressure: group.average_pressure,
                action: "idle".into(),
                reason: "stable".into(),
                pilot_id: group.policy.pilot_id.clone(),
            };
            enum ActionPlan {
                None,
                ScaleUp {
                    pilot_id: Option<String>,
                },
                ScaleDown {
                    victim_id: String,
                    pilot_id: Option<String>,
                },
            }

            let action_plan = {
                let mut tracker_map = self
                    .breaches
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let tracker = tracker_map.entry(group_id.clone()).or_default();

                if let Some(pressure) = group.average_pressure {
                    if pressure > group.policy.scale_up_threshold {
                        tracker.down_since = None;
                        let started = tracker.up_since.get_or_insert_with(Instant::now);
                        if started.elapsed() >= Duration::from_secs(group.policy.sustained_secs)
                            && current < group.policy.max_replicas
                        {
                            status.action = "scale_up".into();
                            status.desired_replicas = current + 1;
                            status.reason = format!(
                                "pressure {:.3} exceeded threshold {:.3}",
                                pressure, group.policy.scale_up_threshold
                            );
                            tracker.up_since = Some(Instant::now());
                            ActionPlan::ScaleUp {
                                pilot_id: group.policy.pilot_id.clone(),
                            }
                        } else {
                            status.reason = "waiting for sustained scale-up window".into();
                            ActionPlan::None
                        }
                    } else if pressure < group.policy.scale_down_threshold {
                        tracker.up_since = None;
                        let started = tracker.down_since.get_or_insert_with(Instant::now);
                        if started.elapsed() >= Duration::from_secs(group.policy.sustained_secs)
                            && current > group.policy.min_replicas
                        {
                            if let Some(victim_id) = self.select_scale_down_candidate(&group) {
                                status.action = "scale_down".into();
                                status.desired_replicas = current.saturating_sub(1);
                                status.reason = format!("scale down {}", victim_id);
                                tracker.down_since = Some(Instant::now());
                                ActionPlan::ScaleDown {
                                    victim_id,
                                    pilot_id: group.policy.pilot_id.clone(),
                                }
                            } else {
                                status.reason = "no idle caster available for scale-down".into();
                                ActionPlan::None
                            }
                        } else {
                            status.reason = "waiting for sustained scale-down window".into();
                            ActionPlan::None
                        }
                    } else {
                        tracker.up_since = None;
                        tracker.down_since = None;
                        ActionPlan::None
                    }
                } else {
                    // No pressure data reported — skip scaling decisions
                    tracker.up_since = None;
                    tracker.down_since = None;
                    ActionPlan::None
                }
            };

            match action_plan {
                ActionPlan::None => {}
                ActionPlan::ScaleUp { pilot_id } => {
                    if let Some(pilot_id) = pilot_id {
                        let _ = self
                            .session_mgr
                            .send_message(
                                &pilot_id,
                                SessionMessage {
                                    payload: Some(session_message::Payload::ScaleSignal(
                                        ScaleSignal {
                                            action: ScaleAction::Up.into(),
                                            group_id: group_id.clone(),
                                            desired_replicas: status.desired_replicas as u32,
                                            current_pressure: group.average_pressure.unwrap_or(0.0),
                                            reason: status.reason.clone(),
                                        },
                                    )),
                                },
                            )
                            .await;
                    } else {
                        status.reason = "missing pilot".into();
                    }
                }
                ActionPlan::ScaleDown {
                    victim_id,
                    pilot_id,
                } => {
                    let _ = self
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
                        .await;

                    if let Some(pilot_id) = pilot_id {
                        let session_mgr = Arc::clone(&self.session_mgr);
                        let group_id_clone = group_id.clone();
                        let reason = format!("force_kill:{}", victim_id);
                        let current_pressure = group.average_pressure.unwrap_or(0.0);
                        let desired_replicas = status.desired_replicas as u32;
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(
                                DEFAULT_SHUTDOWN_GRACE_PERIOD_MS as u64,
                            ))
                            .await;
                            if session_mgr.connected_at(&victim_id).is_some() {
                                let _ = session_mgr
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
                                    .await;
                            }
                        });
                    }
                }
            }

            self.statuses.insert(group_id, status);
        }

        self.statuses
            .retain(|group_id, _| seen.iter().any(|seen_id| seen_id == group_id));
    }

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

            let (avg, reported_count) = self.session_mgr.group_pressure("group", &group_id);
            // When no caster in the group has reported pressure yet, treat
            // the group as having unknown load so we skip scaling decisions
            // rather than misinterpreting silence as 0.0.
            let average_pressure = if reported_count > 0 { Some(avg) } else { None };
            groups
                .entry(group_id.clone())
                .and_modify(|state: &mut GroupState| state.caster_ids.push(meta.key().clone()))
                .or_insert_with(|| GroupState {
                    policy,
                    caster_ids: vec![meta.key().clone()],
                    average_pressure,
                });
        }

        groups
    }

    fn policy_from_labels(
        _group_id: &str,
        labels: &HashMap<String, String>,
    ) -> Option<ScalingPolicy> {
        Some(ScalingPolicy {
            pilot_id: labels.get("_pilot_id").cloned(),
            scale_up_threshold: labels.get("_scale_up")?.parse().ok()?,
            scale_down_threshold: labels.get("_scale_down")?.parse().ok()?,
            sustained_secs: labels.get("_sustained")?.parse().ok()?,
            min_replicas: labels.get("_min")?.parse().ok()?,
            max_replicas: labels.get("_max")?.parse().ok()?,
        })
    }

    fn select_scale_down_candidate(&self, group: &GroupState) -> Option<String> {
        let mut candidates = group
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
                Some((
                    reported_pressure.is_some(),
                    pressure,
                    available_permits,
                    self.session_mgr.connected_at(caster_id),
                    caster_id.clone(),
                ))
            })
            .collect::<Vec<_>>();

        candidates.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.total_cmp(&b.1))
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| b.3.cmp(&a.3))
        });

        candidates
            .into_iter()
            .map(|(_, _, _, _, caster_id)| caster_id)
            .next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{CasterMetadata, CasterState};
    use dashmap::DashMap;
    use tokio::sync::{mpsc, Semaphore};

    fn insert_scaling_caster_with_reported(
        mgr: &SessionManager,
        caster_id: &str,
        group: &str,
        pilot_id: Option<&str>,
        max_concurrent: usize,
        pressure: f64,
        pressure_reported: bool,
    ) -> Option<mpsc::Receiver<SessionMessage>> {
        let (tx, rx) = mpsc::channel(16);
        mgr.sessions.insert(
            caster_id.to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::new(DashMap::new()),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::new(Semaphore::new(max_concurrent)),
                connected_at: Instant::now(),
            },
        );
        mgr.health.insert(
            caster_id.to_string(),
            Arc::new(std::sync::RwLock::new(crate::session::HealthInfo {
                pressure,
                pressure_reported,
                ..Default::default()
            })),
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
        let (tx, rx) = mpsc::channel(16);
        mgr.sessions.insert(
            pilot_id.to_string(),
            CasterState {
                outbound: tx,
                pending: Arc::new(DashMap::new()),
                timeout_handles: Arc::new(DashMap::new()),
                semaphore: Arc::new(Semaphore::new(1)),
                connected_at: Instant::now(),
            },
        );
        mgr.health.insert(
            pilot_id.to_string(),
            Arc::new(std::sync::RwLock::new(crate::session::HealthInfo::default())),
        );
        mgr.metadata.insert(
            pilot_id.to_string(),
            CasterMetadata {
                labels: HashMap::new(),
                max_concurrent: 1,
                role: CasterRole::Pilot,
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
}
