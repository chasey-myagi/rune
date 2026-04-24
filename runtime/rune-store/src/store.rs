use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;

use crate::key_cache::KeyCache;
use crate::pool::ConnectionPool;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid key type: {0}")]
    InvalidKeyType(String),
    #[error("invalid key format: {0}")]
    InvalidKeyFormat(String),
    #[error("invalid task status: {0}")]
    InvalidTaskStatus(String),
    #[error("flow already exists: {0}")]
    DuplicateFlow(String),
    #[error("blocking task failed: {0}")]
    BlockingTask(#[from] tokio::task::JoinError),
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone)]
pub struct StorePoolConfig {
    pub reader_count: usize,
    pub key_cache_ttl: Duration,
    pub key_cache_negative_ttl: Duration,
}

impl Default for StorePoolConfig {
    fn default() -> Self {
        Self {
            reader_count: 4,
            key_cache_ttl: Duration::from_secs(60),
            key_cache_negative_ttl: Duration::from_secs(30),
        }
    }
}

/// SQLite-backed persistence layer for Rune runtime.
#[derive(Clone)]
pub struct RuneStore {
    pub(crate) pool: Arc<ConnectionPool>,
    pub(crate) key_cache: Arc<KeyCache>,
}

impl RuneStore {
    /// Open a database at the given file path.
    pub fn open(path: impl AsRef<Path>) -> StoreResult<Self> {
        Self::open_with_config(path, StorePoolConfig::default())
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: StorePoolConfig) -> StoreResult<Self> {
        let pool = ConnectionPool::new(path.as_ref(), config.reader_count)?;
        let store = Self {
            pool: Arc::new(pool),
            key_cache: Arc::new(KeyCache::new(
                config.key_cache_ttl,
                config.key_cache_negative_ttl,
            )),
        };
        store.run_migrations()?;
        Ok(store)
    }

    /// Open an in-memory database (useful for testing).
    pub fn open_in_memory() -> StoreResult<Self> {
        Self::open_in_memory_with_config(StorePoolConfig::default())
    }

    pub fn open_in_memory_with_config(config: StorePoolConfig) -> StoreResult<Self> {
        let pool = ConnectionPool::new_in_memory()?;
        let store = Self {
            pool: Arc::new(pool),
            key_cache: Arc::new(KeyCache::new(
                config.key_cache_ttl,
                config.key_cache_negative_ttl,
            )),
        };
        store.run_migrations()?;
        Ok(store)
    }

    /// Deliberately poison the internal Mutex (test-only).
    #[cfg(feature = "test-helpers")]
    pub fn poison_mutex(&self) {
        let pool = Arc::clone(&self.pool);
        let handle = std::thread::spawn(move || {
            let _guard = pool.writer();
            panic!("intentional panic to poison the mutex");
        });
        // The thread panicked while holding the lock → Mutex is now poisoned.
        let _ = handle.join();
    }

    /// Query the current journal_mode (test-only).
    #[cfg(feature = "test-helpers")]
    pub fn journal_mode(&self) -> StoreResult<String> {
        let conn = self.pool.writer();
        let mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        Ok(mode)
    }

    /// Query the current busy_timeout (test-only).
    #[cfg(feature = "test-helpers")]
    pub fn busy_timeout(&self) -> StoreResult<i64> {
        let conn = self.pool.writer();
        let timeout: i64 = conn.query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
        Ok(timeout)
    }

    fn run_migrations(&self) -> StoreResult<()> {
        let conn = self.pool.writer();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS api_keys (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                key_prefix  TEXT NOT NULL,
                key_hash    TEXT NOT NULL UNIQUE,
                key_type    TEXT NOT NULL CHECK(key_type IN ('caster', 'gate', 'admin')),
                label       TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                revoked_at  TEXT
            );

            CREATE TABLE IF NOT EXISTS tasks (
                task_id      TEXT PRIMARY KEY,
                rune_name    TEXT NOT NULL,
                status       TEXT NOT NULL CHECK(status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
                input        TEXT,
                output       TEXT,
                error        TEXT,
                created_at   TEXT NOT NULL,
                started_at   TEXT,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS call_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id  TEXT NOT NULL,
                rune_name   TEXT NOT NULL,
                mode        TEXT NOT NULL CHECK(mode IN ('sync', 'stream', 'async')),
                caster_id   TEXT,
                latency_ms  INTEGER NOT NULL,
                status_code INTEGER NOT NULL,
                input_size  INTEGER NOT NULL,
                output_size INTEGER NOT NULL,
                timestamp   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rune_snapshots (
                rune_name       TEXT PRIMARY KEY,
                version         TEXT NOT NULL,
                description     TEXT NOT NULL,
                supports_stream INTEGER NOT NULL DEFAULT 0,
                gate_path       TEXT NOT NULL,
                gate_method     TEXT NOT NULL,
                last_seen       TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS flows (
                name            TEXT PRIMARY KEY,
                definition_json TEXT NOT NULL,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_call_logs_rune ON call_logs(rune_name);
            CREATE INDEX IF NOT EXISTS idx_call_logs_ts ON call_logs(timestamp);
            CREATE INDEX IF NOT EXISTS idx_call_logs_caster ON call_logs(caster_id);
            CREATE INDEX IF NOT EXISTS idx_call_logs_request ON call_logs(request_id);
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_rune ON tasks(rune_name);
            CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
            CREATE INDEX IF NOT EXISTS idx_flows_updated_at ON flows(updated_at);
            CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
            CREATE INDEX IF NOT EXISTS idx_api_keys_type_active ON api_keys(key_type, revoked_at);
            ",
        )?;

        // v1.3.1 migration: add audit columns to api_keys.
        // We use a transaction + PRAGMA table_info + conditional ALTER.
        // Within a single SQLite connection (single writer) this is atomic:
        // no TOCTOU race because the connection serialises all access.
        // We do NOT use "ADD COLUMN IF NOT EXISTS" — that requires SQLite
        // 3.37.0+ and fails silently on older versions bundled with distros
        // such as Ubuntu 20.04 LTS (SQLite 3.31.1).
        conn.execute("BEGIN IMMEDIATE", [])?;
        let existing_cols: std::collections::HashSet<String> = conn
            .prepare("PRAGMA table_info(api_keys)")?
            .query_map([], |row| row.get::<_, String>("name"))?
            .filter_map(|r| r.ok())
            .collect();
        if !existing_cols.contains("last_used_at") {
            conn.execute("ALTER TABLE api_keys ADD COLUMN last_used_at TEXT", [])?;
        }
        if !existing_cols.contains("last_used_ip") {
            conn.execute("ALTER TABLE api_keys ADD COLUMN last_used_ip  TEXT", [])?;
        }
        conn.execute("COMMIT", [])?;

        Ok(())
    }
}
