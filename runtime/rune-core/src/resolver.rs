use crate::relay::RuneEntry;
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Resolver trait — picks one candidate from the registry
pub trait Resolver: Send + Sync {
    fn pick<'a>(&self, rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry>;
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
    fn pick<'a>(&self, rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry> {
        if candidates.is_empty() {
            return None;
        }
        let idx = self
            .counters
            .entry(rune_name.to_string())
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);
        Some(&candidates[idx % candidates.len()])
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
    fn pick<'a>(&self, _rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry> {
        if candidates.is_empty() {
            return None;
        }
        use rand::Rng;
        let idx = rand::thread_rng().gen_range(0..candidates.len());
        Some(&candidates[idx])
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
    fn pick<'a>(&self, rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry> {
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
            // All same rank — delegate directly (no clone needed).
            return self.inner.pick(rune_name, candidates);
        }

        // Build temporary vec for inner resolver, then map back via index.
        let top_tier: Vec<RuneEntry> = top_indices.iter().map(|&i| candidates[i].clone()).collect();
        let picked = self.inner.pick(rune_name, &top_tier)?;

        // Find which position in top_tier was picked, then map back to
        // original candidates via top_indices. Use pointer comparison
        // within top_tier (inner resolver returns a reference into top_tier).
        let inner_idx = top_tier
            .iter()
            .position(|e| std::ptr::eq(e, picked))
            .unwrap_or_else(|| {
                // Fallback: value-based match (if inner resolver clones).
                top_tier
                    .iter()
                    .position(|e| {
                        e.config.name == picked.config.name && e.caster_id == picked.caster_id
                    })
                    .unwrap_or(0)
            });

        Some(&candidates[top_indices[inner_idx]])
    }
}

impl Resolver for LeastLoadResolver {
    fn pick<'a>(&self, _rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry> {
        if candidates.is_empty() {
            return None;
        }

        // Pick the candidate with the most available permits (least load).
        // If a candidate has no caster_id (in-process), treat as max permits.
        let mut best: Option<(usize, &'a RuneEntry)> = None;
        for entry in candidates {
            let permits = match &entry.caster_id {
                Some(cid) => self.session_mgr.available_permits(cid),
                None => usize::MAX, // in-process invokers are always available
            };
            match &best {
                None => best = Some((permits, entry)),
                Some((best_permits, _)) => {
                    if permits > *best_permits {
                        best = Some((permits, entry));
                    }
                }
            }
        }
        best.map(|(_, entry)| entry)
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
    fn pick<'a>(&self, rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry> {
        if candidates.is_empty() {
            return None;
        }

        // Find the maximum priority value
        let max_priority = candidates.iter().map(|e| e.config.priority).max().unwrap();

        // Collect indices of top-tier candidates
        let top_indices: Vec<usize> = candidates
            .iter()
            .enumerate()
            .filter(|(_, e)| e.config.priority == max_priority)
            .map(|(i, _)| i)
            .collect();

        if top_indices.len() == candidates.len() {
            // All same priority — delegate directly to inner
            self.inner.pick(rune_name, candidates)
        } else {
            // Build a temporary vec of top-tier entries for inner resolver
            let top_tier: Vec<RuneEntry> =
                top_indices.iter().map(|&i| candidates[i].clone()).collect();
            let picked = self.inner.pick(rune_name, &top_tier)?;
            // Safe index lookup: match by (name, caster_id) value equality,
            // which is robust even if inner resolver clones or rebinds references.
            // Precondition: (name, caster_id) must be unique within top_tier.
            debug_assert!(
                {
                    let mut pairs: Vec<_> = top_tier
                        .iter()
                        .map(|e| (&e.config.name, &e.caster_id))
                        .collect();
                    pairs.sort();
                    pairs.dedup();
                    pairs.len() == top_tier.len()
                },
                "top_tier contains duplicate (name, caster_id) entries"
            );
            let inner_idx = top_tier
                .iter()
                .position(|e| {
                    e.config.name == picked.config.name && e.caster_id == picked.caster_id
                })
                .unwrap_or(0);
            Some(&candidates[top_indices[inner_idx]])
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
        let picked = resolver.pick("test", &candidates).unwrap();
        assert_eq!(picked.config.name, "high_a");

        // Second pick should be high_b (index 2 in original)
        let picked = resolver.pick("test", &candidates).unwrap();
        assert_eq!(picked.config.name, "high_b");

        // Third pick should round back to high_a
        let picked = resolver.pick("test", &candidates).unwrap();
        assert_eq!(picked.config.name, "high_a");
    }

    #[test]
    fn priority_resolver_all_same_priority_delegates_directly() {
        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        let candidates = vec![make_entry("a", 5), make_entry("b", 5), make_entry("c", 5)];

        let picked = resolver.pick("test", &candidates).unwrap();
        assert_eq!(picked.config.name, "a");
        let picked = resolver.pick("test", &candidates).unwrap();
        assert_eq!(picked.config.name, "b");
        let picked = resolver.pick("test", &candidates).unwrap();
        assert_eq!(picked.config.name, "c");
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
            let picked = resolver.pick("test", &candidates).unwrap();
            assert_eq!(picked.config.name, "high");
        }
    }

    #[test]
    fn priority_resolver_returns_reference_to_original_candidate() {
        // Ensure the returned reference points into the original slice,
        // not a temporary clone
        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        let candidates = vec![make_entry("low", 0), make_entry("high", 10)];

        let picked = resolver.pick("test", &candidates).unwrap();
        let picked_ptr = picked as *const RuneEntry;
        let orig_ptr = &candidates[1] as *const RuneEntry;
        assert_eq!(
            picked_ptr, orig_ptr,
            "picked should point to original candidate, not a clone"
        );
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
                resolver
                    .pick("stress", &candidates)
                    .unwrap()
                    .config
                    .name
                    .clone()
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
        let first = resolver.pick("test_i1", &candidates).unwrap();
        assert_eq!(first.config.name, "high_a");

        // 第二次 pick 应该是 high_b（round-robin 在 top_tier 中前进）
        let second = resolver.pick("test_i1", &candidates).unwrap();
        assert_eq!(
            second.config.name, "high_b",
            "second pick should be high_b, not high_a; ptr::eq fallback to index 0 is the bug"
        );

        // 验证返回的引用指向原始 candidates 切片
        let second_ptr = second as *const RuneEntry;
        let original_ptr = &candidates[2] as *const RuneEntry;
        assert_eq!(
            second_ptr, original_ptr,
            "returned reference must point to original candidate slice, not a clone"
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

        let first = resolver.pick("echo", &candidates).unwrap();
        let second = resolver.pick("echo", &candidates).unwrap();

        assert_ne!(
            first.caster_id, second.caster_id,
            "HealthAwareResolver must round-robin among same-rank candidates"
        );
        // References must point into the original candidates slice
        let first_ptr = first as *const RuneEntry;
        let second_ptr = second as *const RuneEntry;
        assert!(first_ptr == &candidates[0] as *const _ || first_ptr == &candidates[1] as *const _,);
        assert!(
            second_ptr == &candidates[0] as *const _ || second_ptr == &candidates[1] as *const _,
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

        let first = resolver.pick("echo", &candidates).unwrap();
        let second = resolver.pick("echo", &candidates).unwrap();

        // Round-robin should pick candidates[0] then candidates[1]
        let first_ptr = first as *const RuneEntry;
        let second_ptr = second as *const RuneEntry;
        assert_eq!(first_ptr, &candidates[0] as *const _);
        assert_eq!(
            second_ptr, &candidates[1] as *const _,
            "second pick must return candidates[1], not candidates[0] again (seen=0 bug)"
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
        if let Some(health) = session_mgr.health.get("degraded") {
            let mut info = health.write().unwrap();
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

        let picked = resolver.pick("echo", &candidates).unwrap();
        assert_eq!(picked.caster_id.as_deref(), Some("healthy"));
    }

    #[test]
    fn health_aware_resolver_falls_back_when_all_unhealthy() {
        let session_mgr = Arc::new(SessionManager::new_dev(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(30),
        ));
        session_mgr.insert_test_caster("unhealthy", 1);
        if let Some(health) = session_mgr.health.get("unhealthy") {
            let mut info = health.write().unwrap();
            *info = HealthInfo {
                status: HealthStatusLevel::Unhealthy,
                ..HealthInfo::default()
            };
        }

        let resolver = HealthAwareResolver::new(Arc::new(RoundRobinResolver::new()), session_mgr);
        let candidates = vec![make_entry("echo", 0)];
        assert!(resolver.pick("echo", &candidates).is_some());
    }
}
