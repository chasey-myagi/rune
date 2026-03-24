use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;

/// Default TTL for uploaded files: 5 minutes
const DEFAULT_FILE_TTL_SECS: u64 = 300;

/// Minimum interval (in seconds) between full eviction scans.
const EVICT_INTERVAL_SECS: u64 = 30;

/// Metadata for a stored file in the broker.
#[derive(Clone)]
pub struct StoredFile {
    pub filename: String,
    pub mime_type: String,
    pub data: Bytes,
    /// The request_id that uploaded this file
    pub request_id: String,
    /// When this file was stored (for TTL-based expiry)
    pub stored_at: Instant,
}

/// FileBroker stores uploaded files in memory, keyed by unique file_id.
/// Files are automatically expired after a TTL to prevent memory leaks.
#[derive(Clone)]
pub struct FileBroker {
    pub(crate) files: Arc<DashMap<String, StoredFile>>,
    ttl_secs: u64,
    /// Epoch-relative timestamp (secs) of last eviction, used to throttle cleanup
    last_eviction_secs: Arc<AtomicU64>,
    /// Fixed epoch for computing elapsed seconds (avoids Instant arithmetic issues)
    epoch: Instant,
    /// Minimum interval between eviction scans
    evict_interval_secs: u64,
}

impl FileBroker {
    pub fn new() -> Self {
        Self {
            files: Arc::new(DashMap::new()),
            ttl_secs: DEFAULT_FILE_TTL_SECS,
            last_eviction_secs: Arc::new(AtomicU64::new(0)),
            epoch: Instant::now(),
            evict_interval_secs: EVICT_INTERVAL_SECS,
        }
    }

    /// Create a FileBroker with custom TTL and eviction interval (for testing).
    #[cfg(test)]
    pub fn with_ttl(ttl_secs: u64, evict_interval_secs: u64) -> Self {
        Self {
            files: Arc::new(DashMap::new()),
            ttl_secs,
            last_eviction_secs: Arc::new(AtomicU64::new(0)),
            epoch: Instant::now(),
            evict_interval_secs,
        }
    }

    /// Store a file and return its unique file_id.
    pub fn store(&self, filename: String, mime_type: String, data: Bytes, request_id: &str) -> String {
        // Throttled eviction: only run full scan if enough time has passed since last eviction
        self.maybe_evict_expired();

        let file_id = uuid::Uuid::new_v4().to_string();
        self.files.insert(
            file_id.clone(),
            StoredFile {
                filename,
                mime_type,
                data,
                request_id: request_id.to_string(),
                stored_at: Instant::now(),
            },
        );
        file_id
    }

    /// Get a stored file by id. Returns None if expired or not found.
    pub fn get(&self, file_id: &str) -> Option<StoredFile> {
        let entry = self.files.get(file_id)?;
        if entry.stored_at.elapsed().as_secs() >= self.ttl_secs {
            drop(entry);
            self.files.remove(file_id);
            return None;
        }
        Some(entry.value().clone())
    }

    /// Remove a stored file by id.
    pub fn remove(&self, file_id: &str) -> Option<StoredFile> {
        self.files.remove(file_id).map(|(_, v)| v)
    }

    /// Mark a request as completed — physically removes all its files from memory.
    pub fn complete_request(&self, request_id: &str) {
        self.files.retain(|_, v| v.request_id != request_id);
    }

    /// Return the number of tracked entries (for testing / diagnostics).
    pub fn entry_count(&self) -> usize {
        self.files.len()
    }

    /// Throttled eviction: CAS check ensures only one thread runs evict_expired
    /// and only if at least `evict_interval_secs` have elapsed since the last run.
    fn maybe_evict_expired(&self) {
        let now_secs = self.epoch.elapsed().as_secs();
        let last = self.last_eviction_secs.load(Ordering::Relaxed);
        if now_secs.saturating_sub(last) >= self.evict_interval_secs {
            // CAS to prevent multiple threads evicting simultaneously
            if self.last_eviction_secs.compare_exchange(
                last, now_secs, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                self.evict_expired();
            }
        }
    }

    /// Remove files older than TTL.
    fn evict_expired(&self) {
        let ttl = self.ttl_secs;
        self.files.retain(|_, v| v.stored_at.elapsed().as_secs() < ttl);
    }
}
