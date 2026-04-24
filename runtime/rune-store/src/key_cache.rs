use crate::models::{ApiKey, KeyType};
use dashmap::DashMap;
use std::time::{Duration, Instant};

struct CacheEntry {
    key: ApiKey,
    inserted_at: Instant,
}

pub(crate) enum CacheResult {
    Hit(ApiKey),
    NegativeHit,
    Miss,
}

pub(crate) struct KeyCache {
    entries: DashMap<String, CacheEntry>,
    negatives: DashMap<String, Instant>,
    ttl: Duration,
    negative_ttl: Duration,
    max_entries: usize,
}

const DEFAULT_MAX_CACHE_ENTRIES: usize = 10_000;

impl KeyCache {
    pub fn new(ttl: Duration, negative_ttl: Duration) -> Self {
        Self::with_max_entries(ttl, negative_ttl, DEFAULT_MAX_CACHE_ENTRIES)
    }

    pub fn with_max_entries(ttl: Duration, negative_ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: DashMap::new(),
            negatives: DashMap::new(),
            ttl,
            negative_ttl,
            max_entries: max_entries.max(1),
        }
    }

    pub fn get(&self, key_hash: &str, expected_type: KeyType) -> CacheResult {
        let cache_key = cache_key(key_hash, expected_type);

        if let Some(entry) = self.entries.get(&cache_key) {
            if entry.inserted_at.elapsed() <= self.ttl {
                return CacheResult::Hit(entry.key.clone());
            }
            drop(entry);
            self.entries.remove(&cache_key);
        }

        if let Some(inserted_at) = self.negatives.get(&cache_key) {
            if inserted_at.elapsed() <= self.negative_ttl {
                return CacheResult::NegativeHit;
            }
            drop(inserted_at);
            self.negatives.remove(&cache_key);
        }

        CacheResult::Miss
    }

    pub fn insert(&self, key_hash: String, key: ApiKey) {
        self.evict_if_full();
        let cache_key = cache_key(&key_hash, key.key_type);
        self.negatives.remove(&cache_key);
        self.entries.insert(
            cache_key,
            CacheEntry {
                key,
                inserted_at: Instant::now(),
            },
        );
    }

    pub fn insert_negative(&self, key_hash: String, expected_type: KeyType) {
        self.evict_if_full();
        let cache_key = cache_key(&key_hash, expected_type);
        self.entries.remove(&cache_key);
        self.negatives.insert(cache_key, Instant::now());
    }

    fn evict_if_full(&self) {
        let total = self.entries.len() + self.negatives.len();
        if total < self.max_entries {
            return;
        }

        // Phase 1: evict all expired entries
        let ttl = self.ttl;
        let negative_ttl = self.negative_ttl;
        self.entries
            .retain(|_, entry| entry.inserted_at.elapsed() <= ttl);
        self.negatives
            .retain(|_, inserted_at| inserted_at.elapsed() <= negative_ttl);

        // Phase 2: if still over limit, evict arbitrarily.
        // A true LRU (e.g. `lru` crate or a linked-list) would be better,
        // but arbitrary-order eviction avoids the allocation/sort overhead
        // of a "sort all keys" approach. See TODO below.
        let total = self.entries.len() + self.negatives.len();
        if total >= self.max_entries {
            let target = self.max_entries.saturating_sub(1);

            // Arbitrary-order eviction for negatives — no allocations per removal.
            while self.negatives.len() + self.entries.len() > target && !self.negatives.is_empty() {
                if let Some(entry) = self.negatives.iter().next() {
                    let key = entry.key().clone();
                    drop(entry);
                    self.negatives.remove(&key);
                }
            }

            // Arbitrary-order eviction for entries.
            while self.negatives.len() + self.entries.len() > target && !self.entries.is_empty() {
                if let Some(entry) = self.entries.iter().next() {
                    let key = entry.key().clone();
                    drop(entry);
                    self.entries.remove(&key);
                }
            }
        }
        // TODO: replace random eviction with a real LRU (e.g. `lru` crate)
        // if cache hit ratio becomes a concern.
    }

    /// Remove all cache entries for a specific key hash (across all key types).
    pub fn invalidate(&self, key_hash: &str) {
        for key_type in [KeyType::Gate, KeyType::Admin, KeyType::Caster] {
            let ck = cache_key(key_hash, key_type);
            self.entries.remove(&ck);
            self.negatives.remove(&ck);
        }
    }
}

fn cache_key(key_hash: &str, key_type: KeyType) -> String {
    format!("{}:{key_hash}", key_type.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(key_type: KeyType) -> ApiKey {
        ApiKey {
            id: 1,
            key_prefix: "rk_test".into(),
            key_hash: Some("abc".into()),
            key_type,
            label: "test".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            revoked_at: None,
            last_used_at: None,
            last_used_ip: None,
        }
    }

    #[test]
    fn positive_hit_matches_expected_type() {
        let cache = KeyCache::new(Duration::from_secs(60), Duration::from_secs(30));
        cache.insert("hash-1".into(), make_key(KeyType::Gate));

        match cache.get("hash-1", KeyType::Gate) {
            CacheResult::Hit(key) => assert_eq!(key.key_type, KeyType::Gate),
            _ => panic!("expected positive cache hit"),
        }
    }

    #[test]
    fn negative_cache_is_type_specific() {
        let cache = KeyCache::new(Duration::from_secs(60), Duration::from_secs(30));
        cache.insert_negative("hash-2".into(), KeyType::Caster);

        assert!(matches!(
            cache.get("hash-2", KeyType::Caster),
            CacheResult::NegativeHit
        ));
        assert!(matches!(
            cache.get("hash-2", KeyType::Gate),
            CacheResult::Miss
        ));
    }

    #[test]
    fn test_fix_cache_evicts_when_max_entries_exceeded() {
        // Bug: no size limit — brute-force API key scanning fills negatives
        // unbounded, eventually OOM. Cache must cap total entries.
        let cache = KeyCache::with_max_entries(
            Duration::from_secs(60),
            Duration::from_secs(30),
            5, // max 5 entries
        );

        // Insert 6 negative entries
        for i in 0..6 {
            cache.insert_negative(format!("hash-{i}"), KeyType::Gate);
        }

        // After exceeding max, the cache should have been cleared and only
        // the last entry remains.
        let total = cache.entries.len() + cache.negatives.len();
        assert!(
            total <= 5,
            "cache should not exceed max_entries, got {total}"
        );
    }

    #[test]
    fn test_fix_cache_evicts_positive_entries_too() {
        let cache = KeyCache::with_max_entries(Duration::from_secs(60), Duration::from_secs(30), 3);

        for i in 0..4 {
            let mut key = make_key(KeyType::Gate);
            key.id = i;
            cache.insert(format!("hash-{i}"), key);
        }

        let total = cache.entries.len() + cache.negatives.len();
        assert!(
            total <= 3,
            "cache should not exceed max_entries, got {total}"
        );
    }

    #[test]
    fn test_fix_cache_eviction_preserves_valid_entries() {
        // max_entries = 10, insert 10 entries (5 expired + 5 valid), then trigger eviction
        // with the nuclear invalidate_all, ALL entries are wiped — valid ones too.
        // After fix: valid entries should mostly survive.
        let cache =
            KeyCache::with_max_entries(Duration::from_secs(60), Duration::from_secs(60), 10);

        // Insert 5 entries that we'll make "expired" by using a short-TTL cache trick:
        // We can't easily backdate Instant, so we use a two-phase approach:
        // Phase 1: create a short-ttl cache, insert expired entries, then copy them.
        // Actually, since CacheEntry uses Instant::now(), we need to sleep.
        // Simpler: insert 5 negatives with the cache, sleep to expire them, then insert 5 more valid ones + 1 to trigger eviction.

        // Insert 5 negatives (these will expire)
        for i in 0..5 {
            cache.insert_negative(format!("old-{i}"), KeyType::Gate);
        }

        // Temporarily override TTLs won't work, so let's use a different approach:
        // Build cache with very short TTL so old entries expire quickly.
        let cache =
            KeyCache::with_max_entries(Duration::from_millis(10), Duration::from_millis(10), 10);

        // Insert 5 entries that will expire
        for i in 0..5 {
            cache.insert_negative(format!("old-{i}"), KeyType::Gate);
        }
        std::thread::sleep(Duration::from_millis(20)); // let them expire

        // Insert 5 valid entries (these are fresh)
        for i in 0..5 {
            let mut key = make_key(KeyType::Gate);
            key.id = i as i64;
            cache.insert(format!("new-{i}"), key);
        }

        // Now total = 5 expired negatives + 5 valid entries = 10 = max_entries
        // Inserting one more triggers eviction
        let mut key = make_key(KeyType::Gate);
        key.id = 99;
        cache.insert("trigger".into(), key);

        // After eviction, valid entries should mostly survive (not all wiped)
        let mut valid_remaining = 0;
        for i in 0..5 {
            if cache.entries.contains_key(&format!("gate:new-{i}")) {
                valid_remaining += 1;
            }
        }
        // The trigger entry should also be there
        assert!(
            cache.entries.contains_key("gate:trigger"),
            "the just-inserted entry must survive"
        );
        assert!(
            valid_remaining >= 3,
            "at least 3 of 5 valid entries should survive eviction, got {valid_remaining}"
        );
    }

    #[test]
    fn test_fix_cache_eviction_removes_expired_first() {
        // Insert some expired and some fresh entries, trigger eviction.
        // Expired ones should be removed, fresh ones should remain.
        let cache =
            KeyCache::with_max_entries(Duration::from_millis(10), Duration::from_millis(10), 8);

        // Insert 4 entries that will expire
        for i in 0..4 {
            cache.insert_negative(format!("expired-{i}"), KeyType::Gate);
        }
        std::thread::sleep(Duration::from_millis(20));

        // Insert 4 fresh entries
        for i in 0..4 {
            let mut key = make_key(KeyType::Gate);
            key.id = i as i64;
            cache.insert(format!("fresh-{i}"), key);
        }

        // total = 4 expired negatives + 4 fresh entries = 8 = max_entries
        // Trigger eviction by inserting one more
        let mut key = make_key(KeyType::Gate);
        key.id = 100;
        cache.insert("final".into(), key);

        // All expired negatives should be gone
        for i in 0..4 {
            assert!(
                !cache.negatives.contains_key(&format!("gate:expired-{i}")),
                "expired negative {i} should have been evicted"
            );
        }

        // All fresh entries should still be present (since removing expired freed enough space)
        for i in 0..4 {
            assert!(
                cache.entries.contains_key(&format!("gate:fresh-{i}")),
                "fresh entry {i} should survive eviction"
            );
        }

        // The final entry should be present
        assert!(
            cache.entries.contains_key("gate:final"),
            "the just-inserted entry must survive"
        );
    }

    #[test]
    fn expired_entries_miss() {
        let cache = KeyCache::new(Duration::from_millis(1), Duration::from_millis(1));
        cache.insert("hash-3".into(), make_key(KeyType::Gate));
        cache.insert_negative("hash-4".into(), KeyType::Gate);

        std::thread::sleep(Duration::from_millis(5));

        assert!(matches!(
            cache.get("hash-3", KeyType::Gate),
            CacheResult::Miss
        ));
        assert!(matches!(
            cache.get("hash-4", KeyType::Gate),
            CacheResult::Miss
        ));
    }
}
