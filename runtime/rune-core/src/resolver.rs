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
// v0.6.0 — Advanced scheduling strategies (stubs for TDD)
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
        // TODO: implement random selection
        // For now, always pick first (stub)
        Some(&candidates[0])
    }
}

/// Least-load resolver — picks the candidate with the lowest current concurrency.
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
        // TODO: implement least-load selection based on session_mgr.available_permits()
        // For now, always pick first (stub)
        Some(&candidates[0])
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
        // TODO: filter to highest-priority tier, then delegate to inner
        // For now, delegate directly (stub)
        self.inner.pick(rune_name, candidates)
    }
}
