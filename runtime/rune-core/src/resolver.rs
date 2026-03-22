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
            // Map back: the inner resolver returns index into top_tier,
            // we need the corresponding index in the original candidates.
            // Find which top_tier entry was picked by comparing Arc pointers.
            for (ti, &orig_idx) in top_indices.iter().enumerate() {
                if Arc::ptr_eq(&top_tier[ti].invoker, &picked.invoker) {
                    return Some(&candidates[orig_idx]);
                }
            }
            // Fallback: shouldn't happen, but just return the first top-tier
            Some(&candidates[top_indices[0]])
        }
    }
}
