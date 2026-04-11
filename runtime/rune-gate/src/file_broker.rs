use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;

/// Default TTL for uploaded files: 5 minutes
const DEFAULT_FILE_TTL_SECS: u64 = 300;

/// Minimum interval (in seconds) between full eviction scans.
const EVICT_INTERVAL_SECS: u64 = 30;

/// Files larger than 4 MB are spilled to disk instead of kept in memory.
const DISK_THRESHOLD: usize = 4 * 1024 * 1024;

/// How a file's payload is stored.
#[derive(Clone, Debug)]
pub enum FileStorage {
    /// Small file — kept entirely in memory.
    Memory(Bytes),
    /// Large file — written to a temporary file on disk.
    Disk { path: PathBuf },
}

/// Metadata for a stored file in the broker.
#[derive(Clone)]
pub struct StoredFile {
    pub filename: String,
    pub mime_type: String,
    storage: FileStorage,
    /// The request_id that uploaded this file
    pub request_id: String,
    /// When this file was stored (for TTL-based expiry)
    pub stored_at: Instant,
}

impl StoredFile {
    /// Retrieve file payload (sync). Memory files return a cheap clone; disk
    /// files perform a blocking read. Prefer [`data_async`](Self::data_async)
    /// when called from an async / tokio context.
    pub fn data(&self) -> std::io::Result<Bytes> {
        match &self.storage {
            FileStorage::Memory(data) => Ok(data.clone()),
            FileStorage::Disk { path } => Ok(Bytes::from(std::fs::read(path)?)),
        }
    }

    /// Async version of [`data`](Self::data) that off-loads the blocking disk
    /// read to [`tokio::task::spawn_blocking`] so it never stalls a tokio
    /// worker thread.
    pub async fn data_async(&self) -> std::io::Result<Bytes> {
        match &self.storage {
            FileStorage::Memory(data) => Ok(data.clone()),
            FileStorage::Disk { path } => {
                let path = path.clone();
                tokio::task::spawn_blocking(move || std::fs::read(&path).map(Bytes::from))
                    .await
                    .map_err(std::io::Error::other)?
            }
        }
    }
}

/// FileBroker stores uploaded files keyed by unique file_id.
/// Small files live in memory; large files (> 4 MB) are spilled to disk.
/// Files are automatically expired after a TTL to prevent resource leaks.
#[derive(Clone)]
pub struct FileBroker {
    pub(crate) files: Arc<DashMap<String, StoredFile>>,
    pub(crate) ttl_secs: u64,
    /// Epoch-relative timestamp (secs) of last eviction, used to throttle cleanup
    last_eviction_secs: Arc<AtomicU64>,
    /// Fixed epoch for computing elapsed seconds (avoids Instant arithmetic issues)
    epoch: Instant,
    /// Minimum interval between eviction scans
    pub(crate) evict_interval_secs: u64,
    /// Optional directory for spilling large files to disk.
    /// `None` means pure in-memory mode (backward-compatible default).
    disk_dir: Option<PathBuf>,
}

impl Default for FileBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl FileBroker {
    pub fn new() -> Self {
        Self {
            files: Arc::new(DashMap::new()),
            ttl_secs: DEFAULT_FILE_TTL_SECS,
            last_eviction_secs: Arc::new(AtomicU64::new(0)),
            epoch: Instant::now(),
            evict_interval_secs: EVICT_INTERVAL_SECS,
            disk_dir: None,
        }
    }

    /// Create a FileBroker that spills large files into `dir`.
    pub fn with_disk_dir(dir: PathBuf) -> Self {
        Self {
            files: Arc::new(DashMap::new()),
            ttl_secs: DEFAULT_FILE_TTL_SECS,
            last_eviction_secs: Arc::new(AtomicU64::new(0)),
            epoch: Instant::now(),
            evict_interval_secs: EVICT_INTERVAL_SECS,
            disk_dir: Some(dir),
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
            disk_dir: None,
        }
    }

    /// Store a file and return its unique file_id.
    pub fn store(
        &self,
        filename: String,
        mime_type: String,
        data: Bytes,
        request_id: &str,
    ) -> String {
        // Throttled eviction: only run full scan if enough time has passed since last eviction
        self.maybe_evict_expired();

        let file_id = uuid::Uuid::new_v4().to_string();

        let storage = if data.len() > DISK_THRESHOLD {
            if let Some(dir) = &self.disk_dir {
                let path = dir.join(&file_id);
                // Write to disk. For large files this is blocking I/O, but store()
                // is called synchronously from multipart parsing which already has
                // the full body in memory. The write latency is bounded by file size
                // and is a one-time cost per upload.
                match std::fs::write(&path, &data) {
                    Ok(()) => FileStorage::Disk { path },
                    Err(e) => {
                        tracing::warn!(file_id = %file_id, error = %e, "disk spill failed, keeping in memory");
                        FileStorage::Memory(data)
                    }
                }
            } else {
                FileStorage::Memory(data)
            }
        } else {
            FileStorage::Memory(data)
        };

        self.files.insert(
            file_id.clone(),
            StoredFile {
                filename,
                mime_type,
                storage,
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
            self.remove_with_cleanup(file_id);
            return None;
        }
        Some(entry.value().clone())
    }

    /// Remove a stored file by id.
    pub fn remove(&self, file_id: &str) -> Option<StoredFile> {
        let removed = self.files.remove(file_id).map(|(_, v)| v);
        if let Some(ref stored) = removed {
            Self::cleanup_disk(&stored.storage);
        }
        removed
    }

    /// Mark a request as completed — physically removes all its files.
    pub fn complete_request(&self, request_id: &str) {
        self.files.retain(|_, v| {
            if v.request_id == request_id {
                Self::cleanup_disk(&v.storage);
                false
            } else {
                true
            }
        });
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
            if self
                .last_eviction_secs
                .compare_exchange(last, now_secs, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.evict_expired();
            }
        }
    }

    /// Remove files older than TTL, cleaning up disk files as needed.
    fn evict_expired(&self) {
        let ttl = self.ttl_secs;
        self.files.retain(|_, v| {
            if v.stored_at.elapsed().as_secs() >= ttl {
                Self::cleanup_disk(&v.storage);
                false
            } else {
                true
            }
        });
    }

    /// Remove entry from map and clean up disk (internal helper for get-expired path).
    fn remove_with_cleanup(&self, file_id: &str) {
        if let Some((_, stored)) = self.files.remove(file_id) {
            Self::cleanup_disk(&stored.storage);
        }
    }

    /// If the storage is a disk file, delete it on a blocking thread so we
    /// never stall a tokio worker. The deletion is fire-and-forget; errors are
    /// logged but ignored.
    fn cleanup_disk(storage: &FileStorage) {
        if let FileStorage::Disk { path } = storage {
            let path = path.clone();
            tokio::task::spawn_blocking(move || {
                if let Err(e) = std::fs::remove_file(&path) {
                    // Not found is fine — file may have been cleaned already.
                    if e.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(path = %path.display(), error = %e, "failed to remove spilled file");
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn small_file_stays_in_memory() {
        let dir = TempDir::new().unwrap();
        let broker = FileBroker::with_disk_dir(dir.path().to_path_buf());
        let data = Bytes::from(vec![0u8; 1024]); // 1KB — 小文件
        let file_id = broker.store(
            "small.txt".into(),
            "text/plain".into(),
            data.clone(),
            "req1",
        );

        let stored = broker.get(&file_id).unwrap();
        assert_eq!(stored.data().unwrap(), data);
        assert_eq!(stored.data_async().await.unwrap(), data);
        // 磁盘目录应该没有文件
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn large_file_spills_to_disk() {
        let dir = TempDir::new().unwrap();
        let broker = FileBroker::with_disk_dir(dir.path().to_path_buf());
        let data = Bytes::from(vec![42u8; 5 * 1024 * 1024]); // 5MB — 大文件
        let file_id = broker.store(
            "large.bin".into(),
            "application/octet-stream".into(),
            data.clone(),
            "req2",
        );

        let stored = broker.get(&file_id).unwrap();
        assert_eq!(stored.data().unwrap(), data);
        assert_eq!(stored.data_async().await.unwrap(), data);
        // 磁盘目录应该有 1 个文件
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn expired_disk_file_cleaned_up() {
        let dir = TempDir::new().unwrap();
        let mut broker = FileBroker::with_disk_dir(dir.path().to_path_buf());
        broker.ttl_secs = 0; // 立即过期
        broker.evict_interval_secs = 0; // 不限制 evict 频率

        let data = Bytes::from(vec![42u8; 5 * 1024 * 1024]); // 5MB
        let file_id = broker.store(
            "large.bin".into(),
            "application/octet-stream".into(),
            data,
            "req3",
        );

        // 文件应已过期
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(broker.get(&file_id).is_none());

        // 触发一次 store 来触发 eviction
        let _ = broker.store(
            "trigger.txt".into(),
            "text/plain".into(),
            Bytes::from("x"),
            "req4",
        );

        // spawn_blocking 是 fire-and-forget，需要等待删除完成
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 磁盘上大文件应该被清理
        // large.bin 的磁盘文件被删除了（trigger.txt 是小文件，不会写磁盘）
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn complete_request_removes_disk_files() {
        let dir = TempDir::new().unwrap();
        let broker = FileBroker::with_disk_dir(dir.path().to_path_buf());
        let data = Bytes::from(vec![42u8; 5 * 1024 * 1024]); // 5MB
        let _file_id = broker.store(
            "large.bin".into(),
            "application/octet-stream".into(),
            data,
            "req5",
        );

        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

        broker.complete_request("req5");

        // spawn_blocking 是 fire-and-forget，需要等待删除完成
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 磁盘文件应该被删除
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
