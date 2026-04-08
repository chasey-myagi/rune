use crate::store::{StoreError, StoreResult};
use rusqlite::Connection;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

const BUSY_TIMEOUT_MS: i64 = 5_000;

pub(crate) struct ConnectionPool {
    writer: Mutex<Connection>,
    readers: Vec<Mutex<Connection>>,
    next_reader: AtomicUsize,
    use_writer_for_reads: bool,
}

impl ConnectionPool {
    pub fn new(path: &Path, reader_count: usize) -> StoreResult<Self> {
        let writer = open_writer(path)?;
        let readers = open_file_readers(path, reader_count)?;
        Ok(Self {
            writer: Mutex::new(writer),
            readers,
            next_reader: AtomicUsize::new(0),
            use_writer_for_reads: false,
        })
    }

    /// In-memory databases cannot share connections across threads, so readers
    /// always fall back to the writer. No reader_count parameter is accepted.
    pub fn new_in_memory() -> StoreResult<Self> {
        let writer = Connection::open_in_memory()?;
        configure_connection(&writer, false, false)?;
        Ok(Self {
            writer: Mutex::new(writer),
            readers: Vec::new(),
            next_reader: AtomicUsize::new(0),
            use_writer_for_reads: true,
        })
    }

    pub fn writer(&self) -> MutexGuard<'_, Connection> {
        self.writer.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn reader(&self) -> MutexGuard<'_, Connection> {
        if self.use_writer_for_reads || self.readers.is_empty() {
            return self.writer();
        }

        let len = self.readers.len();
        let idx = self.next_reader.fetch_add(1, Ordering::Relaxed) % len;
        self.readers[idx].lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn normalize_reader_count(reader_count: usize) -> usize {
    reader_count.max(1)
}

fn open_writer(path: &Path) -> StoreResult<Connection> {
    let conn = Connection::open(path)?;
    configure_connection(&conn, true, false)?;
    Ok(conn)
}

fn open_reader(path: &Path) -> StoreResult<Connection> {
    let conn = Connection::open(path)?;
    // WAL is per-database (set by writer); readers only need busy_timeout + query_only.
    configure_connection(&conn, false, true)?;
    Ok(conn)
}

fn open_file_readers(path: &Path, reader_count: usize) -> StoreResult<Vec<Mutex<Connection>>> {
    (0..normalize_reader_count(reader_count))
        .map(|_| open_reader(path).map(Mutex::new))
        .collect()
}

fn configure_connection(
    conn: &Connection,
    enable_wal: bool,
    query_only: bool,
) -> Result<(), StoreError> {
    conn.execute_batch(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS};"))?;
    if enable_wal {
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
    }
    if query_only {
        conn.execute_batch("PRAGMA query_only=ON;")?;
    }
    Ok(())
}
