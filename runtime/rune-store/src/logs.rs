use crate::models::CallLog;
use crate::store::{RuneStore, StoreResult};
impl RuneStore {
    pub async fn insert_log(&self, log: &CallLog) -> StoreResult<i64> {
        let conn = self.conn.clone();
        let log = log.clone();
        tokio::task::spawn_blocking(move || { let conn = conn.lock().unwrap(); conn.execute("INSERT INTO call_logs (request_id, rune_name, mode, caster_id, latency_ms, status_code, input_size, output_size, timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)", rusqlite::params![log.request_id, log.rune_name, log.mode, log.caster_id, log.latency_ms, log.status_code, log.input_size, log.output_size, log.timestamp])?; Ok(conn.last_insert_rowid()) }).await?
    }
    pub async fn query_logs(
        &self,
        rune_name: Option<&str>,
        limit: i64,
    ) -> StoreResult<Vec<CallLog>> {
        let conn = self.conn.clone();
        let rune_name = rune_name.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || { let conn = conn.lock().unwrap(); let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(rn) = rune_name { ("SELECT id, request_id, rune_name, mode, caster_id, latency_ms, status_code, input_size, output_size, timestamp FROM call_logs WHERE rune_name = ?1 ORDER BY timestamp DESC LIMIT ?2".to_string(), vec![Box::new(rn), Box::new(limit)]) } else { ("SELECT id, request_id, rune_name, mode, caster_id, latency_ms, status_code, input_size, output_size, timestamp FROM call_logs ORDER BY timestamp DESC LIMIT ?1".to_string(), vec![Box::new(limit)]) }; let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect(); let mut stmt = conn.prepare(&sql)?; let logs = stmt.query_map(param_refs.as_slice(), |row| Ok(CallLog { id: row.get(0)?, request_id: row.get(1)?, rune_name: row.get(2)?, mode: row.get(3)?, caster_id: row.get(4)?, latency_ms: row.get(5)?, status_code: row.get(6)?, input_size: row.get(7)?, output_size: row.get(8)?, timestamp: row.get(9)? }))?.collect::<Result<Vec<_>, _>>()?; Ok(logs) }).await?
    }
    pub async fn cleanup_logs_before(&self, before: &str) -> StoreResult<u64> {
        let conn = self.conn.clone();
        let before = before.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();
            let deleted = conn.execute(
                "DELETE FROM call_logs WHERE timestamp < ?1",
                rusqlite::params![before],
            )?;
            Ok(deleted as u64)
        })
        .await?
    }
    pub async fn call_stats(&self) -> StoreResult<(i64, Vec<(String, i64, i64)>)> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || { let conn = conn.lock().unwrap(); let total: i64 = conn.query_row("SELECT COUNT(*) FROM call_logs", [], |row| row.get(0))?; let mut stmt = conn.prepare("SELECT rune_name, COUNT(*), CAST(COALESCE(AVG(latency_ms), 0) AS INTEGER) FROM call_logs GROUP BY rune_name ORDER BY COUNT(*) DESC")?; let by_rune = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)))?.collect::<Result<Vec<_>, _>>()?; Ok((total, by_rune)) }).await?
    }
    pub async fn call_stats_enhanced(
        &self,
    ) -> StoreResult<(i64, Vec<(String, i64, i64, f64, f64)>)> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || { let conn = conn.lock().unwrap(); let total: i64 = conn.query_row("SELECT COUNT(*) FROM call_logs", [], |row| row.get(0))?; let mut stmt = conn.prepare("SELECT DISTINCT rune_name FROM call_logs ORDER BY rune_name")?; let rune_names: Vec<String> = stmt.query_map([], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?; let mut results = Vec::new(); for rune_name in rune_names { let count: i64 = conn.query_row("SELECT COUNT(*) FROM call_logs WHERE rune_name = ?1", rusqlite::params![rune_name], |row| row.get(0))?; let success_count: i64 = conn.query_row("SELECT COUNT(*) FROM call_logs WHERE rune_name = ?1 AND status_code >= 200 AND status_code < 300", rusqlite::params![rune_name], |row| row.get(0))?; let avg_latency: i64 = conn.query_row("SELECT CAST(COALESCE(AVG(latency_ms), 0) AS INTEGER) FROM call_logs WHERE rune_name = ?1", rusqlite::params![rune_name], |row| row.get(0))?; let success_rate = if count > 0 { success_count as f64 / count as f64 } else { 0.0 }; let mut lat_stmt = conn.prepare("SELECT latency_ms FROM call_logs WHERE rune_name = ?1 ORDER BY latency_ms ASC")?; let latencies: Vec<i64> = lat_stmt.query_map(rusqlite::params![rune_name], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()?; let p95 = if latencies.is_empty() { 0.0 } else { let idx = ((latencies.len() as f64 * 0.95).ceil() as usize).min(latencies.len()) - 1; latencies[idx] as f64 }; results.push((rune_name, count, avg_latency, success_rate, p95)); } Ok((total, results)) }).await?
    }
}
