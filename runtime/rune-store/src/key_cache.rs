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
        if total >= self.max_entries {
            self.invalidate_all();
        }
    }

    pub fn invalidate_all(&self) {
        self.entries.clear();
        self.negatives.clear();
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
