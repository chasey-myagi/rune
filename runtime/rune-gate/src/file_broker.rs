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
/// Files are associated with a request_id and physically removed when
/// the request completes.
#[derive(Clone)]
pub struct FileBroker {
    pub(crate) files: Arc<DashMap<String, StoredFile>>,
}

impl FileBroker {
    pub fn new() -> Self {
        Self {
            files: Arc::new(DashMap::new()),
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

    /// Get a stored file by id.
    pub fn get(&self, file_id: &str) -> Option<StoredFile> {
        self.files.get(file_id).map(|entry| entry.value().clone())
    }

    /// Remove a stored file by id.
    pub fn remove(&self, file_id: &str) -> Option<StoredFile> {
        self.files.remove(file_id).map(|(_, v)| v)
    }

    /// Mark a request as completed — physically removes all its files from memory.
    pub fn complete_request(&self, request_id: &str) {
        self.files.retain(|_, v| v.request_id != request_id);
    }
}
