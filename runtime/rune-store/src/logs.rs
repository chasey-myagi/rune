use crate::models::CallLog;
use crate::store::{RuneStore, StoreResult};

impl RuneStore {
    pub async fn insert_log(&self, log: &CallLog) -> StoreResult<i64> {
        let conn = self.conn.clone();
        let log = log.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "INSERT INTO call_logs \
                 (request_id, rune_name, mode, caster_id, latency_ms, \
                  status_code, input_size, output_size, timestamp) \
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
                    log.timestamp
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
        .await?
    }

    pub async fn query_logs(
        &self,
        rune_name: Option<&str>,
        limit: i64,
    ) -> StoreResult<Vec<CallLog>> {
        let conn = self.conn.clone();
        let rune_name = rune_name.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
                if let Some(rn) = rune_name {
                    (
                        "SELECT id, request_id, rune_name, mode, caster_id, \
                         latency_ms, status_code, input_size, output_size, timestamp \
                         FROM call_logs WHERE rune_name = ?1 \
                         ORDER BY timestamp DESC LIMIT ?2"
                            .to_string(),
                        vec![Box::new(rn), Box::new(limit)],
                    )
                } else {
                    (
                        "SELECT id, request_id, rune_name, mode, caster_id, \
                         latency_ms, status_code, input_size, output_size, timestamp \
                         FROM call_logs \
                         ORDER BY timestamp DESC LIMIT ?1"
                            .to_string(),
                        vec![Box::new(limit)],
                    )
                };
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
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
        })
        .await?
    }

    pub async fn cleanup_logs_before(&self, before: &str) -> StoreResult<u64> {
        let conn = self.conn.clone();
        let before = before.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
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
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let total: i64 =
                conn.query_row("SELECT COUNT(*) FROM call_logs", [], |row| row.get(0))?;
            let mut stmt = conn.prepare(
                "SELECT rune_name, COUNT(*), \
                 CAST(COALESCE(AVG(latency_ms), 0) AS INTEGER) \
                 FROM call_logs GROUP BY rune_name ORDER BY COUNT(*) DESC",
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
        })
        .await?
    }

    pub async fn call_stats_enhanced(
        &self,
    ) -> StoreResult<(i64, Vec<(String, i64, i64, f64, f64)>)> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());

            // Query 1: Single aggregation for count, avg, success per rune
            let mut agg_stmt = conn.prepare(
                "SELECT \
                     rune_name, \
                     COUNT(*) as total, \
                     CAST(COALESCE(AVG(latency_ms), 0) AS INTEGER) as avg_latency, \
                     SUM(CASE WHEN status_code >= 200 AND status_code < 300 \
                         THEN 1 ELSE 0 END) as success_count \
                 FROM call_logs \
                 GROUP BY rune_name \
                 ORDER BY rune_name",
            )?;

            struct RuneAgg {
                name: String,
                count: i64,
                avg_latency: i64,
                success_rate: f64,
            }

            let mut grand_total: i64 = 0;
            let aggs: Vec<RuneAgg> = agg_stmt
                .query_map([], |row| {
                    let count: i64 = row.get(1)?;
                    let success_count: i64 = row.get(3)?;
                    Ok(RuneAgg {
                        name: row.get(0)?,
                        count,
                        avg_latency: row.get(2)?,
                        success_rate: if count > 0 {
                            success_count as f64 / count as f64
                        } else {
                            0.0
                        },
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;

            for a in &aggs {
                grand_total += a.count;
            }

            // Query 2: P95 via SQL window function (no full-table scan into memory)
            let mut p95_stmt = conn.prepare(
                "WITH ranked AS (
                    SELECT rune_name, latency_ms,
                           ROW_NUMBER() OVER (PARTITION BY rune_name ORDER BY latency_ms ASC) as rn,
                           COUNT(*) OVER (PARTITION BY rune_name) as cnt
                    FROM call_logs
                )
                SELECT rune_name, latency_ms
                FROM ranked
                WHERE rn = CAST((cnt * 95 + 99) / 100 AS INTEGER)
                ORDER BY rune_name",
            )?;
            let p95_map: std::collections::HashMap<String, f64> = p95_stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as f64))
                })?
                .collect::<Result<std::collections::HashMap<_, _>, _>>()?;

            // Combine aggregation + P95 by name lookup (not positional)
            let results: Vec<(String, i64, i64, f64, f64)> = aggs
                .into_iter()
                .map(|a| {
                    let p95 = p95_map.get(&a.name).copied().unwrap_or(0.0);
                    (a.name, a.count, a.avg_latency, a.success_rate, p95)
                })
                .collect();

            Ok((grand_total, results))
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use crate::store::RuneStore;

    #[tokio::test]
    async fn test_call_stats_enhanced_empty_no_panic() {
        // NF-11: call_stats_enhanced must not panic on empty data
        let store = RuneStore::open_in_memory().unwrap();
        let (total, results) = store.call_stats_enhanced().await.unwrap();
        assert_eq!(total, 0);
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_call_stats_enhanced_with_data() {
        use crate::models::CallLog;

        let store = RuneStore::open_in_memory().unwrap();
        let log = CallLog {
            id: 0,
            request_id: "r1".into(),
            rune_name: "echo".into(),
            mode: "sync".into(),
            caster_id: None,
            latency_ms: 100,
            status_code: 200,
            input_size: 10,
            output_size: 20,
            timestamp: "2025-01-01T00:00:00Z".into(),
        };
        store.insert_log(&log).await.unwrap();

        let (total, results) = store.call_stats_enhanced().await.unwrap();
        assert_eq!(total, 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "echo");
        assert_eq!(results[0].1, 1); // count
                                     // P95 of a single value should be that value
        assert!(
            (results[0].4 - 100.0).abs() < 0.01,
            "p95 should be 100.0, got {}",
            results[0].4
        );
    }

    #[tokio::test]
    async fn test_p95_calculation_accuracy_100_entries() {
        use crate::models::CallLog;
        let store = RuneStore::open_in_memory().unwrap();
        // 插入 100 条日志, latency_ms = 1..=100
        for i in 1..=100i64 {
            let log = CallLog {
                id: 0,
                request_id: format!("r{}", i),
                rune_name: "echo".into(),
                mode: "sync".into(),
                caster_id: None,
                latency_ms: i,
                status_code: 200,
                input_size: 10,
                output_size: 20,
                timestamp: "2025-01-01T00:00:00Z".into(),
            };
            store.insert_log(&log).await.unwrap();
        }
        let (total, results) = store.call_stats_enhanced().await.unwrap();
        assert_eq!(total, 100);
        assert_eq!(results.len(), 1);
        // P95 of 1..=100 should be 95
        assert!(
            (results[0].4 - 95.0).abs() < 1.01,
            "p95 should be ~95, got {}",
            results[0].4
        );
    }

    #[tokio::test]
    async fn test_p95_multiple_runes() {
        use crate::models::CallLog;
        let store = RuneStore::open_in_memory().unwrap();
        // rune "fast": latencies 1..=20 (P95 ≈ 19)
        for i in 1..=20i64 {
            let log = CallLog {
                id: 0,
                request_id: format!("rf{}", i),
                rune_name: "fast".into(),
                mode: "sync".into(),
                caster_id: None,
                latency_ms: i,
                status_code: 200,
                input_size: 10,
                output_size: 20,
                timestamp: "2025-01-01T00:00:00Z".into(),
            };
            store.insert_log(&log).await.unwrap();
        }
        // rune "slow": latencies 100..=200 (P95 ≈ 196)
        for i in 100..=200i64 {
            let log = CallLog {
                id: 0,
                request_id: format!("rs{}", i),
                rune_name: "slow".into(),
                mode: "sync".into(),
                caster_id: None,
                latency_ms: i,
                status_code: 200,
                input_size: 10,
                output_size: 20,
                timestamp: "2025-01-01T00:00:00Z".into(),
            };
            store.insert_log(&log).await.unwrap();
        }
        let (total, results) = store.call_stats_enhanced().await.unwrap();
        assert_eq!(total, 121); // 20 + 101
        assert_eq!(results.len(), 2);
        // Results are sorted by rune_name
        let fast = results.iter().find(|r| r.0 == "fast").unwrap();
        let slow = results.iter().find(|r| r.0 == "slow").unwrap();
        assert!(
            (fast.4 - 19.0).abs() < 2.0,
            "fast p95 should be ~19, got {}",
            fast.4
        );
        assert!(
            (slow.4 - 196.0).abs() < 6.0,
            "slow p95 should be ~196, got {}",
            slow.4
        );
    }

    /// I-2 回归测试: P95 合并应按名称匹配，不依赖隐式排序位置
    #[tokio::test]
    async fn test_fix_p95_join_by_name_not_position() {
        use crate::models::CallLog;
        let store = RuneStore::open_in_memory().unwrap();

        // 插入 3 个不同 rune 的日志，名字故意选择不同排序特征
        for (name, latencies) in [
            ("zeta_rune", vec![100, 200, 300]),
            ("alpha_rune", vec![10, 20, 30]),
            ("middle_rune", vec![50, 60, 70]),
        ] {
            for (i, lat) in latencies.iter().enumerate() {
                let log = CallLog {
                    id: 0,
                    request_id: format!("r_{}_{}", name, i),
                    rune_name: name.into(),
                    mode: "sync".into(),
                    caster_id: None,
                    latency_ms: *lat,
                    status_code: 200,
                    input_size: 10,
                    output_size: 20,
                    timestamp: "2025-01-01T00:00:00Z".into(),
                };
                store.insert_log(&log).await.unwrap();
            }
        }

        let (total, results) = store.call_stats_enhanced().await.unwrap();
        assert_eq!(total, 9);
        assert_eq!(results.len(), 3);

        // 验证每个 rune 的 P95 对应正确（不依赖位置）
        let alpha = results.iter().find(|r| r.0 == "alpha_rune").unwrap();
        let middle = results.iter().find(|r| r.0 == "middle_rune").unwrap();
        let zeta = results.iter().find(|r| r.0 == "zeta_rune").unwrap();

        // P95 of 3 values: ceil(3*0.95) = 3, so P95 = max value
        assert!(
            (alpha.4 - 30.0).abs() < 1.0,
            "alpha P95 should be ~30, got {}",
            alpha.4
        );
        assert!(
            (middle.4 - 70.0).abs() < 1.0,
            "middle P95 should be ~70, got {}",
            middle.4
        );
        assert!(
            (zeta.4 - 300.0).abs() < 1.0,
            "zeta P95 should be ~300, got {}",
            zeta.4
        );
    }
}
