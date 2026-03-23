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
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap();

            // Query 1: Single aggregation for count, avg, success per rune
            let mut agg_stmt = conn.prepare(
                "SELECT \
                     rune_name, \
                     COUNT(*) as total, \
                     CAST(COALESCE(AVG(latency_ms), 0) AS INTEGER) as avg_latency, \
                     SUM(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 ELSE 0 END) as success_count \
                 FROM call_logs \
                 GROUP BY rune_name \
                 ORDER BY rune_name"
            )?;

            struct RuneAgg {
                name: String,
                count: i64,
                avg_latency: i64,
                success_rate: f64,
            }

            let mut grand_total: i64 = 0;
            let aggs: Vec<RuneAgg> = agg_stmt.query_map([], |row| {
                let count: i64 = row.get(1)?;
                let success_count: i64 = row.get(3)?;
                Ok(RuneAgg {
                    name: row.get(0)?,
                    count,
                    avg_latency: row.get(2)?,
                    success_rate: if count > 0 { success_count as f64 / count as f64 } else { 0.0 },
                })
            })?.collect::<Result<Vec<_>, _>>()?;

            for a in &aggs {
                grand_total += a.count;
            }

            // Query 2: All latencies sorted by rune_name + latency for P95
            let mut lat_stmt = conn.prepare(
                "SELECT rune_name, latency_ms \
                 FROM call_logs \
                 ORDER BY rune_name, latency_ms ASC"
            )?;

            // Stream through sorted results, grouping by rune_name sequentially.
            // Since results are sorted by rune_name, we can compute P95 per group
            // without a HashMap by collecting latencies for the current group.
            let mut p95_map: Vec<(String, f64)> = Vec::with_capacity(aggs.len());
            let mut current_name = String::new();
            let mut current_lats: Vec<i64> = Vec::new();

            let rows = lat_stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;

            for r in rows {
                let (name, lat) = r?;
                if name != current_name {
                    if !current_name.is_empty() {
                        let idx = ((current_lats.len() as f64 * 0.95).ceil() as usize)
                            .min(current_lats.len()) - 1;
                        p95_map.push((std::mem::take(&mut current_name), current_lats[idx] as f64));
                        current_lats.clear();
                    }
                    current_name = name;
                }
                current_lats.push(lat);
            }
            if !current_name.is_empty() {
                let idx = ((current_lats.len() as f64 * 0.95).ceil() as usize)
                    .min(current_lats.len()) - 1;
                p95_map.push((current_name, current_lats[idx] as f64));
            }

            // Combine aggregation + P95 (both are sorted by rune_name)
            let mut p95_iter = p95_map.into_iter().peekable();
            let results: Vec<(String, i64, i64, f64, f64)> = aggs
                .into_iter()
                .map(|a| {
                    let p95 = if let Some(entry) = p95_iter.peek() {
                        if entry.0 == a.name {
                            p95_iter.next().unwrap().1
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    };
                    (a.name, a.count, a.avg_latency, a.success_rate, p95)
                })
                .collect();

            Ok((grand_total, results))
        }).await?
    }
}
