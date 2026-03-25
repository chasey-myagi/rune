use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use dashmap::DashMap;
use crate::relay::RuneEntry;

/// Resolver trait — picks one candidate from the registry
pub trait Resolver: Send + Sync {
    fn pick<'a>(&self, rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry>;
}

/// Round-robin resolver
pub struct RoundRobinResolver {
    counters: DashMap<String, AtomicUsize>,
}

impl RoundRobinResolver {
    pub fn new() -> Self {
        Self { counters: DashMap::new() }
    }
}

impl Resolver for RoundRobinResolver {
    fn pick<'a>(&self, rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry> {
        if candidates.is_empty() { return None; }
        let idx = self.counters
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

impl RandomResolver {
    pub fn new() -> Self {
        Self
    }
}

impl Resolver for RandomResolver {
    fn pick<'a>(&self, _rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry> {
        if candidates.is_empty() { return None; }
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

impl Resolver for LeastLoadResolver {
    fn pick<'a>(&self, _rune_name: &str, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry> {
        if candidates.is_empty() { return None; }

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
        if candidates.is_empty() { return None; }

        // Find the maximum priority value
        let max_priority = candidates
            .iter()
            .map(|e| e.config.priority)
            .max()
            .unwrap();

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
            let top_tier: Vec<RuneEntry> = top_indices.iter().map(|&i| candidates[i].clone()).collect();
            let picked = self.inner.pick(rune_name, &top_tier)?;
            // Safe index lookup: find which element in top_tier was picked.
            // picked's lifetime is tied to top_tier; ptr::eq works because both
            // references point into the same Vec allocation.
            let inner_idx = top_tier.iter().position(|e| {
                e.config.name == picked.config.name && e.caster_id == picked.caster_id
            }).unwrap_or(0);
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
    use crate::rune::{RuneConfig, make_handler};
    use crate::invoker::LocalInvoker;

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

        let candidates = vec![
            make_entry("a", 5),
            make_entry("b", 5),
            make_entry("c", 5),
        ];

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

        let candidates = vec![
            make_entry("low", 0),
            make_entry("high", 10),
        ];

        let picked = resolver.pick("test", &candidates).unwrap();
        let picked_ptr = picked as *const RuneEntry;
        let orig_ptr = &candidates[1] as *const RuneEntry;
        assert_eq!(picked_ptr, orig_ptr, "picked should point to original candidate, not a clone");
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
            .map(|_| resolver.pick("stress", &candidates).unwrap().config.name.clone())
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
        assert_eq!(second.config.name, "high_b",
            "second pick should be high_b, not high_a; ptr::eq fallback to index 0 is the bug");

        // 验证返回的引用指向原始 candidates 切片
        let second_ptr = second as *const RuneEntry;
        let original_ptr = &candidates[2] as *const RuneEntry;
        assert_eq!(second_ptr, original_ptr,
            "returned reference must point to original candidate slice, not a clone");
    }
}
