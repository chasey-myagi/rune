use crate::relay::RuneEntry;
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Resolver trait — picks one candidate from the provided slice.
///
/// Returns the **index** into `candidates` of the chosen entry, or `None`
/// if `candidates` is empty. Returning an index instead of a reference
/// eliminates the `ptr::eq` fragility that previously existed when
/// delegating resolvers worked with cloned sub-slices.
pub trait Resolver: Send + Sync {
    fn pick(&self, rune_name: &str, candidates: &[RuneEntry]) -> Option<usize>;
}

/// Round-robin resolver
pub struct RoundRobinResolver {
    counters: DashMap<String, AtomicUsize>,
}

impl Default for RoundRobinResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobinResolver {
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }
}

impl Resolver for RoundRobinResolver {
    fn pick(&self, rune_name: &str, candidates: &[RuneEntry]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        let idx = self
            .counters
            .entry(rune_name.to_string())
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);
        Some(idx % candidates.len())
    }
}

// ---------------------------------------------------------------------------
// v0.6.0 — Advanced scheduling strategies
// ---------------------------------------------------------------------------

/// Random resolver — picks a random candidate
pub struct RandomResolver;

impl Default for RandomResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl RandomResolver {
    pub fn new() -> Self {
        Self
    }
}

impl Resolver for RandomResolver {
    fn pick(&self, _rune_name: &str, candidates: &[RuneEntry]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        use rand::Rng;
        Some(rand::thread_rng().gen_range(0..candidates.len()))
    }
}

/// Least-load resolver — picks the candidate with the most available permits
/// (i.e., the lowest current concurrency).
/// Requires a reference to SessionManager to query available permits.
pub struct LeastLoadResolver {
    session_mgr: Arc<crate::session::SessionManager>,
}

impl LeastLoadResolver {
    pub fn new(session_mgr: Arc<crate::session::SessionManager>) -> Self {
        Self { session_mgr }
    }
}

pub struct HealthAwareResolver {
    inner: Arc<dyn Resolver>,
    session_mgr: Arc<crate::session::SessionManager>,
}

impl HealthAwareResolver {
    pub fn new(inner: Arc<dyn Resolver>, session_mgr: Arc<crate::session::SessionManager>) -> Self {
        Self { inner, session_mgr }
    }

    fn rank(&self, entry: &RuneEntry) -> u8 {
        match entry.caster_id.as_deref() {
            None => 2,
            Some(caster_id) => match self.session_mgr.health_status(caster_id) {
                Some(crate::session::HealthStatusLevel::Healthy) | None => 2,
                Some(crate::session::HealthStatusLevel::Degraded) => 1,
                Some(crate::session::HealthStatusLevel::Unhealthy) => 0,
            },
        }
    }
}

impl Resolver for HealthAwareResolver {
    fn pick(&self, rune_name: &str, candidates: &[RuneEntry]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }

        // Compute ranks once, collect indices of best-rank candidates.
        let ranks: Vec<u8> = candidates.iter().map(|e| self.rank(e)).collect();
        let best_rank = *ranks.iter().max().unwrap_or(&0);

        let top_indices: Vec<usize> = ranks
            .iter()
            .enumerate()
            .filter(|(_, &r)| r == best_rank)
            .map(|(i, _)| i)
            .collect();

        if top_indices.len() == candidates.len() {
            // All same rank — delegate directly.
            return self.inner.pick(rune_name, candidates);
        }

        // Build temporary vec for inner resolver, then map back via index.
        let top_tier: Vec<RuneEntry> = top_indices.iter().map(|&i| candidates[i].clone()).collect();
        let inner_idx = self.inner.pick(rune_name, &top_tier)?;

        Some(top_indices[inner_idx])
    }
}

impl Resolver for LeastLoadResolver {
    fn pick(&self, _rune_name: &str, candidates: &[RuneEntry]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }

        let mut best: Option<(f64, usize, usize)> = None;
        for (idx, entry) in candidates.iter().enumerate() {
            let (pressure, permits) = match &entry.caster_id {
                Some(cid) => {
                    let permits = self.session_mgr.available_permits(cid);
                    let raw_pressure = match self.session_mgr.reported_pressure(cid) {
                        Some(p) => p,
                        None => {
                            let max = self.session_mgr.max_concurrent(cid);
                            if max > 0 {
                                1.0 - (permits as f64 / max as f64)
                            } else {
                                0.0
                            }
                        }
                    };
                    let p = if raw_pressure.is_nan() || raw_pressure.is_infinite() {
                        1.0
                    } else if raw_pressure < 0.0 {
                        0.0
                    } else {
                        raw_pressure
                    };
                    (p, permits)
                }
                None => (0.0, usize::MAX),
            };
            let dominated = match &best {
                None => false,
                Some((best_p, best_perm, _)) => match pressure.total_cmp(best_p) {
                    std::cmp::Ordering::Less => false,
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Equal => permits <= *best_perm,
                },
            };
            if !dominated {
                best = Some((pressure, permits, idx));
            }
        }
        best.map(|(_, _, idx)| idx)
    }
}

/// Priority resolver — wraps an inner resolver, filtering candidates to the
/// highest priority tier before delegating.
pub struct PriorityResolver {
    inner: Arc<dyn Resolver>,
}

impl PriorityResolver {
    pub fn new(inner: Arc<dyn Resolver>) -> Self {
        Self { inner }
    }
}

impl Resolver for PriorityResolver {
    fn pick(&self, rune_name: &str, candidates: &[RuneEntry]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }

        let max_priority = candidates.iter().map(|e| e.config.priority).max().unwrap();

        let top_indices: Vec<usize> = candidates
            .iter()
            .enumerate()
            .filter(|(_, e)| e.config.priority == max_priority)
            .map(|(i, _)| i)
            .collect();

        if top_indices.len() == candidates.len() {
            self.inner.pick(rune_name, candidates)
        } else {
            let top_tier: Vec<RuneEntry> =
                top_indices.iter().map(|&i| candidates[i].clone()).collect();
            let inner_idx = self.inner.pick(rune_name, &top_tier)?;
            Some(top_indices[inner_idx])
        }
    }
}

/// Remove stale counter entries for rune names that are no longer registered.
impl RoundRobinResolver {
    pub fn remove_counter(&self, rune_name: &str) {
        self.counters.remove(rune_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invoker::LocalInvoker;
    use crate::rune::{make_handler, RuneConfig};
    use crate::session::{HealthInfo, HealthStatusLevel, SessionManager};

    fn make_entry(name: &str, priority: i32) -> RuneEntry {
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        RuneEntry {
            config: RuneConfig {
                name: name.into(),
                priority,
                ..Default::default()
            },
            invoker: Arc::new(LocalInvoker::new(handler)),
            caster_id: None,
        }
    }

    // MF-6: PriorityResolver should correctly map back to original candidates
    // even when inner resolver picks non-first entry from the filtered set
    #[test]
    fn priority_resolver_maps_back_correctly() {
        // candidates: [low(0), high(10), high(10)]
        // PriorityResolver should filter to indices [1, 2] and round-robin among them
        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        let candidates = vec![
            make_entry("low", 0),
            make_entry("high_a", 10),
            make_entry("high_b", 10),
        ];

        // First pick should be high_a (index 1 in original)
        let idx = resolver.pick("test", &candidates).unwrap();
        assert_eq!(candidates[idx].config.name, "high_a");

        // Second pick should be high_b (index 2 in original)
        let idx = resolver.pick("test", &candidates).unwrap();
        assert_eq!(candidates[idx].config.name, "high_b");

        // Third pick should round back to high_a
        let idx = resolver.pick("test", &candidates).unwrap();
        assert_eq!(candidates[idx].config.name, "high_a");
    }

    #[test]
    fn priority_resolver_all_same_priority_delegates_directly() {
        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        let candidates = vec![make_entry("a", 5), make_entry("b", 5), make_entry("c", 5)];

        let idx = resolver.pick("test", &candidates).unwrap();
        assert_eq!(candidates[idx].config.name, "a");
        let idx = resolver.pick("test", &candidates).unwrap();
        assert_eq!(candidates[idx].config.name, "b");
        let idx = resolver.pick("test", &candidates).unwrap();
        assert_eq!(candidates[idx].config.name, "c");
    }

    #[test]
    fn priority_resolver_single_high_always_picked() {
        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        let candidates = vec![
            make_entry("low_a", 1),
            make_entry("low_b", 1),
            make_entry("high", 10),
        ];

        for _ in 0..5 {
            let idx = resolver.pick("test", &candidates).unwrap();
            assert_eq!(candidates[idx].config.name, "high");
        }
    }

    // NF-7: RoundRobin counter cleanup
    #[test]
    fn round_robin_remove_counter_cleans_up() {
        let resolver = RoundRobinResolver::new();
        let entries = vec![make_entry("test_rune", 0)];

        // Use the resolver to create a counter entry
        resolver.pick("test_rune", &entries);
        assert!(resolver.counters.contains_key("test_rune"));

        // Remove the counter
        resolver.remove_counter("test_rune");
        assert!(!resolver.counters.contains_key("test_rune"));
    }

    #[test]
    fn round_robin_remove_counter_nonexistent_is_noop() {
        let resolver = RoundRobinResolver::new();
        // Should not panic
        resolver.remove_counter("nonexistent");
    }

    #[test]
    fn priority_resolver_many_candidates_stress() {
        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);
        let mut candidates: Vec<RuneEntry> = (0..10)
            .map(|i| make_entry(&format!("low_{}", i), 1))
            .collect();
        candidates.push(make_entry("high_a", 10));
        candidates.push(make_entry("high_b", 10));
        candidates.push(make_entry("high_c", 10));

        let names: Vec<String> = (0..6)
            .map(|_| {
                let idx = resolver.pick("stress", &candidates).unwrap();
                candidates[idx].config.name.clone()
            })
            .collect();
        assert_eq!(names[0], "high_a");
        assert_eq!(names[1], "high_b");
        assert_eq!(names[2], "high_c");
        assert_eq!(names[3], "high_a");
    }

    // I-1 回归测试: ptr::eq 在 clone 后的 top_tier Vec 上会匹配失败，
    // 导致 unwrap_or(0) 静默回退到第一个候选。
    // 需要混合优先级才会触发 top_tier clone 路径。
    #[test]
    fn test_fix_priority_resolver_value_match_not_ptr_match() {
        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        // 混合优先级: low(0), high_a(10), high_b(10)
        // 这会触发 top_tier clone 路径
        let candidates = vec![
            make_entry("low", 0),
            make_entry("high_a", 10),
            make_entry("high_b", 10),
        ];

        // 第一次 pick 应该是 high_a
        let first_idx = resolver.pick("test_i1", &candidates).unwrap();
        assert_eq!(candidates[first_idx].config.name, "high_a");

        // 第二次 pick 应该是 high_b（round-robin 在 top_tier 中前进）
        let second_idx = resolver.pick("test_i1", &candidates).unwrap();
        assert_eq!(
            candidates[second_idx].config.name, "high_b",
            "second pick should be high_b, not high_a; fallback to index 0 is the bug"
        );
    }

    /// Regression: HealthAwareResolver with round-robin inner must rotate
    /// among same-rank candidates and return correct references.
    #[test]
    fn test_fix_health_aware_resolver_round_robins_among_same_rank() {
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("caster_a", 1);
        session_mgr.insert_test_caster("caster_b", 1);

        let resolver = HealthAwareResolver::new(Arc::new(RoundRobinResolver::new()), session_mgr);
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let candidates = vec![
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: Some("caster_a".into()),
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler)),
                caster_id: Some("caster_b".into()),
            },
        ];

        let first_idx = resolver.pick("echo", &candidates).unwrap();
        let second_idx = resolver.pick("echo", &candidates).unwrap();

        assert_ne!(
            candidates[first_idx].caster_id, candidates[second_idx].caster_id,
            "HealthAwareResolver must round-robin among same-rank candidates"
        );
    }

    /// Regression: when candidates contain duplicate (name, caster_id=None)
    /// entries (allowed by local registration), the `seen=0` logic always
    /// returns the first match, breaking load balancing for the second pick.
    #[test]
    fn test_fix_health_aware_resolver_duplicate_local_candidates() {
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));

        let resolver = HealthAwareResolver::new(Arc::new(RoundRobinResolver::new()), session_mgr);
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        // Two local entries with same name and caster_id=None
        let candidates = vec![
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: None,
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler)),
                caster_id: None,
            },
        ];

        let first_idx = resolver.pick("echo", &candidates).unwrap();
        let second_idx = resolver.pick("echo", &candidates).unwrap();

        // Round-robin should pick candidates[0] then candidates[1]
        assert_eq!(first_idx, 0);
        assert_eq!(
            second_idx, 1,
            "second pick must return index 1, not 0 again (seen=0 bug)"
        );
    }

    #[test]
    fn health_aware_resolver_prefers_healthy_over_degraded() {
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("healthy", 1);
        session_mgr.insert_test_caster("degraded", 1);
        if let Some(entry) = session_mgr.health.get("degraded") {
            let mut info = entry.info.write().unwrap();
            *info = HealthInfo {
                status: HealthStatusLevel::Degraded,
                ..HealthInfo::default()
            };
        }

        let resolver = HealthAwareResolver::new(Arc::new(RoundRobinResolver::new()), session_mgr);
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let candidates = vec![
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: Some("degraded".into()),
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler)),
                caster_id: Some("healthy".into()),
            },
        ];

        let idx = resolver.pick("echo", &candidates).unwrap();
        assert_eq!(candidates[idx].caster_id.as_deref(), Some("healthy"));
    }

    #[test]
    fn health_aware_resolver_falls_back_when_all_unhealthy() {
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("unhealthy", 1);
        if let Some(health) = session_mgr.health.get("unhealthy") {
            let mut info = health.info.write().unwrap();
            *info = HealthInfo {
                status: HealthStatusLevel::Unhealthy,
                ..HealthInfo::default()
            };
        }

        let resolver = HealthAwareResolver::new(Arc::new(RoundRobinResolver::new()), session_mgr);
        let candidates = vec![make_entry("echo", 0)];
        assert!(resolver.pick("echo", &candidates).is_some());
    }

    #[test]
    fn least_load_resolver_prefers_lower_pressure_before_permits() {
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("low-pressure", 10);
        session_mgr.insert_test_caster("high-pressure", 10);
        session_mgr.set_test_pressure("low-pressure", 0.10);
        session_mgr.set_test_pressure("high-pressure", 0.85);

        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let candidates = vec![
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: Some("high-pressure".into()),
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler)),
                caster_id: Some("low-pressure".into()),
            },
        ];

        let resolver = LeastLoadResolver::new(session_mgr);
        let idx = resolver.pick("echo", &candidates).unwrap();
        assert_eq!(candidates[idx].caster_id.as_deref(), Some("low-pressure"));
    }

    #[test]
    fn least_load_resolver_treats_zero_pressure_as_reported_idle() {
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("idle", 10);
        session_mgr.insert_test_caster("busy", 10);
        session_mgr.set_test_pressure("idle", 0.0);
        session_mgr.set_test_pressure("busy", 0.6);

        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let candidates = vec![
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: Some("busy".into()),
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler)),
                caster_id: Some("idle".into()),
            },
        ];

        let resolver = LeastLoadResolver::new(session_mgr);
        let idx = resolver.pick("echo", &candidates).unwrap();
        assert_eq!(candidates[idx].caster_id.as_deref(), Some("idle"));
    }

    // ---------------------------------------------------------------
    // C1 回归测试: HealthAwareResolver 应使用 value matching 而非 ptr::eq
    // ---------------------------------------------------------------
    #[test]
    fn test_fix_health_aware_resolver_uses_value_matching_not_ptr_eq() {
        // 构造场景：两个 caster 都健康（同 rank），但其中一个不健康使得
        // 触发 top_tier clone 路径。inner round-robin 在 top_tier 中选第二个时，
        // ptr::eq 无法匹配回原始 candidates 切片。
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("healthy_a", 1);
        session_mgr.insert_test_caster("healthy_b", 1);
        session_mgr.insert_test_caster("unhealthy_c", 1);
        // 把 unhealthy_c 标记为 Unhealthy，使得 top_tier 只包含 healthy_a 和 healthy_b
        if let Some(health) = session_mgr.health.get("unhealthy_c") {
            let mut info = health.info.write().unwrap();
            *info = HealthInfo {
                status: HealthStatusLevel::Unhealthy,
                ..HealthInfo::default()
            };
        }

        let resolver = HealthAwareResolver::new(Arc::new(RoundRobinResolver::new()), session_mgr);
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let candidates = vec![
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: Some("healthy_a".into()),
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: Some("healthy_b".into()),
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler)),
                caster_id: Some("unhealthy_c".into()),
            },
        ];

        // 第一次 pick 应选 healthy_a
        let first_idx = resolver.pick("echo", &candidates).unwrap();
        assert_eq!(
            candidates[first_idx].caster_id.as_deref(),
            Some("healthy_a")
        );

        // 第二次 pick 应选 healthy_b（round-robin 在 top_tier 中前进）
        let second_idx = resolver.pick("echo", &candidates).unwrap();
        assert_eq!(
            candidates[second_idx].caster_id.as_deref(),
            Some("healthy_b"),
            "second pick should be healthy_b; fallback bug would break this"
        );
    }

    // ---------------------------------------------------------------
    // M2 回归测试: LeastLoadResolver 处理 NaN 和负数 pressure
    // ---------------------------------------------------------------
    #[test]
    fn test_fix_least_load_resolver_handles_nan_pressure() {
        // 构造场景：一个 candidate 的 pressure 是 NaN
        // NaN caster 不应被优先选中
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("nan-caster", 10);
        session_mgr.insert_test_caster("normal-caster", 10);
        session_mgr.set_test_pressure("nan-caster", f64::NAN);
        session_mgr.set_test_pressure("normal-caster", 0.5);

        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let candidates = vec![
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: Some("nan-caster".into()),
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler)),
                caster_id: Some("normal-caster".into()),
            },
        ];

        let resolver = LeastLoadResolver::new(session_mgr);
        let idx = resolver.pick("echo", &candidates).unwrap();
        assert_eq!(
            candidates[idx].caster_id.as_deref(),
            Some("normal-caster"),
            "NaN pressure caster should NOT be preferred; it should be sorted to last"
        );
    }

    #[test]
    fn test_fix_least_load_resolver_handles_negative_pressure() {
        // 构造场景：pressure 为负数
        // 验证排序不崩溃，且负 pressure（比 0 更好）caster 被优先选中
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("negative", 10);
        session_mgr.insert_test_caster("positive", 10);
        session_mgr.set_test_pressure("negative", -0.1);
        session_mgr.set_test_pressure("positive", 0.5);

        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let candidates = vec![
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: Some("positive".into()),
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler)),
                caster_id: Some("negative".into()),
            },
        ];

        let resolver = LeastLoadResolver::new(session_mgr);
        let idx = resolver.pick("echo", &candidates).unwrap();
        // 负 pressure 意味着更低负载，应该被优先选中
        assert_eq!(
            candidates[idx].caster_id.as_deref(),
            Some("negative"),
            "negative pressure caster should be preferred over positive pressure"
        );
    }

    #[test]
    fn least_load_resolver_falls_back_to_available_permits_without_pressure() {
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("busy", 10);
        session_mgr.insert_test_caster("idle", 10);
        let _busy_permit_a = session_mgr.acquire_test_permit("busy");
        let _busy_permit_b = session_mgr.acquire_test_permit("busy");

        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let candidates = vec![
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler.clone())),
                caster_id: Some("busy".into()),
            },
            RuneEntry {
                config: RuneConfig {
                    name: "echo".into(),
                    ..Default::default()
                },
                invoker: Arc::new(LocalInvoker::new(handler)),
                caster_id: Some("idle".into()),
            },
        ];

        let resolver = LeastLoadResolver::new(session_mgr);
        let idx = resolver.pick("echo", &candidates).unwrap();
        assert_eq!(candidates[idx].caster_id.as_deref(), Some("idle"));
    }
}
