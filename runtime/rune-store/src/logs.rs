use crate::models::CallLog;
use crate::store::{RuneStore, StoreResult};

impl RuneStore {
    /// Insert a call log entry.
    pub fn insert_log(&self, log: &CallLog) -> StoreResult<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO call_logs (request_id, rune_name, mode, caster_id, latency_ms, status_code, input_size, output_size, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                log.request_id,
                log.rune_name,
                log.mode,
                log.caster_id,
                log.latency_ms,
                log.status_code,
                log.input_size,
                log.output_size,
                log.timestamp,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Query call logs with optional rune_name filter, ordered by timestamp descending.
    pub fn query_logs(
        &self,
        rune_name: Option<&str>,
        limit: i64,
    ) -> StoreResult<Vec<CallLog>> {
        let conn = self.conn.lock().unwrap();

        let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(rn) = rune_name {
            (
                "SELECT id, request_id, rune_name, mode, caster_id, latency_ms, status_code, input_size, output_size, timestamp \
                 FROM call_logs WHERE rune_name = ?1 ORDER BY timestamp DESC LIMIT ?2".to_string(),
                vec![Box::new(rn.to_string()), Box::new(limit)],
            )
        } else {
            (
                "SELECT id, request_id, rune_name, mode, caster_id, latency_ms, status_code, input_size, output_size, timestamp \
                 FROM call_logs ORDER BY timestamp DESC LIMIT ?1".to_string(),
                vec![Box::new(limit)],
            )
        };

        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let mut stmt = conn.prepare(&sql)?;
        let logs = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(CallLog {
                    id: row.get(0)?,
                    request_id: row.get(1)?,
                    rune_name: row.get(2)?,
                    mode: row.get(3)?,
                    caster_id: row.get(4)?,
                    latency_ms: row.get(5)?,
                    status_code: row.get(6)?,
                    input_size: row.get(7)?,
                    output_size: row.get(8)?,
                    timestamp: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(logs)
    }

    /// Delete call logs older than the given ISO 8601 timestamp.
    pub fn cleanup_logs_before(&self, before: &str) -> StoreResult<u64> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM call_logs WHERE timestamp < ?1",
            rusqlite::params![before],
        )?;
        Ok(deleted as u64)
    }

    /// Get aggregate call statistics.
    pub fn call_stats(&self) -> StoreResult<(i64, Vec<(String, i64, i64)>)> {
        let conn = self.conn.lock().unwrap();
        let total: i64 =
            conn.query_row("SELECT COUNT(*) FROM call_logs", [], |row| row.get(0))?;
        let mut stmt = conn.prepare(
            "SELECT rune_name, COUNT(*), CAST(COALESCE(AVG(latency_ms), 0) AS INTEGER) FROM call_logs GROUP BY rune_name ORDER BY COUNT(*) DESC",
        )?;
        let by_rune = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok((total, by_rune))
    }
}
