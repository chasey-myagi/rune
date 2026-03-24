use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;

/// Default TTL for uploaded files: 5 minutes
const DEFAULT_FILE_TTL_SECS: u64 = 300;

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
}

impl FileBroker {
    pub fn new() -> Self {
        Self {
            files: Arc::new(DashMap::new()),
            ttl_secs: DEFAULT_FILE_TTL_SECS,
        }
    }

    /// Store a file and return its unique file_id.
    pub fn store(&self, filename: String, mime_type: String, data: Bytes, request_id: &str) -> String {
        // Lazy eviction: clean expired files on each store() call
        self.evict_expired();

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

    /// Remove files older than TTL.
    fn evict_expired(&self) {
        let ttl = self.ttl_secs;
        self.files.retain(|_, v| v.stored_at.elapsed().as_secs() < ttl);
    }
}
