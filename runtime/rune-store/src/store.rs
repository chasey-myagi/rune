use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("invalid key type: {0}")]
    InvalidKeyType(String),
    #[error("invalid task status: {0}")]
    InvalidTaskStatus(String),
    #[error("blocking task failed: {0}")]
    BlockingTask(#[from] tokio::task::JoinError),
}

pub type StoreResult<T> = Result<T, StoreError>;

/// SQLite-backed persistence layer for Rune runtime.
#[derive(Clone)]
pub struct RuneStore {
    pub(crate) conn: Arc<Mutex<Connection>>,
}

impl RuneStore {
    /// Open a database at the given file path.
    pub fn open(path: impl AsRef<Path>) -> StoreResult<Self> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.run_migrations()?;
        Ok(store)
    }

    /// Open an in-memory database (useful for testing).
    pub fn open_in_memory() -> StoreResult<Self> {
        let conn = Connection::open_in_memory()?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.run_migrations()?;
        Ok(store)
    }

    /// Deliberately poison the internal Mutex (test-only).
    #[cfg(feature = "test-helpers")]
    pub fn poison_mutex(&self) {
        let mutex = self.conn.clone();
        let handle = std::thread::spawn(move || {
            let _guard = mutex.lock().unwrap();
            panic!("intentional panic to poison the mutex");
        });
        // The thread panicked while holding the lock → Mutex is now poisoned.
        let _ = handle.join();
    }

    fn run_migrations(&self) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
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

            CREATE INDEX IF NOT EXISTS idx_call_logs_rune ON call_logs(rune_name);
            CREATE INDEX IF NOT EXISTS idx_call_logs_ts ON call_logs(timestamp);
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_rune ON tasks(rune_name);
            ",
        )?;
        Ok(())
    }
}
