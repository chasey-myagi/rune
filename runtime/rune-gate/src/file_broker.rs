use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;

/// Metadata for a stored file in the broker.
#[derive(Clone)]
pub struct StoredFile {
    pub filename: String,
    pub mime_type: String,
    pub data: Bytes,
    /// The request_id that uploaded this file
    pub request_id: String,
}

/// FileBroker stores uploaded files in memory, keyed by unique file_id.
/// Files are associated with a request_id. After sync execution, the request_id
/// is marked as completed, and files from completed requests are no longer downloadable.
#[derive(Clone)]
pub struct FileBroker {
    pub(crate) files: Arc<DashMap<String, StoredFile>>,
    completed_requests: Arc<DashMap<String, ()>>,
}

impl FileBroker {
    pub fn new() -> Self {
        Self {
            files: Arc::new(DashMap::new()),
            completed_requests: Arc::new(DashMap::new()),
        }
    }

    /// Store a file and return its unique file_id.
    pub fn store(&self, filename: String, mime_type: String, data: Bytes, request_id: &str) -> String {
        let file_id = uuid::Uuid::new_v4().to_string();
        self.files.insert(
            file_id.clone(),
            StoredFile {
                filename,
                mime_type,
                data,
                request_id: request_id.to_string(),
            },
        );
        file_id
    }

    /// Get a stored file by id. Returns None if the file's request has been completed.
    pub fn get(&self, file_id: &str) -> Option<StoredFile> {
        if let Some(entry) = self.files.get(file_id) {
            let file = entry.value().clone();
            if self.completed_requests.contains_key(&file.request_id) {
                // Request completed, file is no longer available
                None
            } else {
                Some(file)
            }
        } else {
            None
        }
    }

    /// Remove a stored file by id.
    pub fn remove(&self, file_id: &str) -> Option<StoredFile> {
        self.files.remove(file_id).map(|(_, v)| v)
    }

    /// Mark a request as completed and physically remove its files from memory.
    pub fn complete_request(&self, request_id: &str) {
        // Physically delete all files belonging to this request (prevents OOM)
        self.files.retain(|_, v| v.request_id != request_id);
        // Also mark as completed for safety checks on stale references
        self.completed_requests.insert(request_id.to_string(), ());
    }
}
