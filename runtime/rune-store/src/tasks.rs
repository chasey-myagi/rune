use crate::keys::timestamp_now;
use crate::models::{TaskRecord, TaskStatus};
use crate::store::{RuneStore, StoreResult};

impl RuneStore {
    pub async fn insert_task(
        &self,
        task_id: &str,
        rune_name: &str,
        input: Option<&str>,
    ) -> StoreResult<TaskRecord> {
        let pool = self.pool.clone();
        let task_id = task_id.to_string();
        let rune_name = rune_name.to_string();
        let input = input.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || {
            let now = timestamp_now();
            let conn = pool.writer();
            conn.execute(
                "INSERT INTO tasks (task_id, rune_name, status, input, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![task_id, rune_name, "pending", input, now],
            )?;
            Ok(TaskRecord {
                task_id,
                rune_name,
                status: TaskStatus::Pending,
                input,
                output: None,
                error: None,
                created_at: now,
                started_at: None,
                completed_at: None,
            })
        })
        .await?
    }

    /// Insert a task and immediately mark it as Running in a single transaction.
    ///
    /// The caller is responsible for dispatching the actual execution. Using a
    /// single atomic write prevents a crash between insert+update from leaving
    /// a task permanently stuck in the `pending` state.
    pub async fn insert_task_and_start(
        &self,
        task_id: &str,
        rune_name: &str,
        input: Option<&str>,
    ) -> StoreResult<TaskRecord> {
        let pool = self.pool.clone();
        let task_id = task_id.to_string();
        let rune_name = rune_name.to_string();
        let input = input.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || {
            let now = timestamp_now();
            let mut conn = pool.writer();
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO tasks \
                 (task_id, rune_name, status, input, created_at, started_at) \
                 VALUES (?1, ?2, 'running', ?3, ?4, ?4)",
                rusqlite::params![task_id, rune_name, input, now],
            )?;
            tx.commit()?;
            Ok(TaskRecord {
                task_id,
                rune_name,
                status: TaskStatus::Running,
                input,
                output: None,
                error: None,
                created_at: now.clone(),
                started_at: Some(now),
                completed_at: None,
            })
        })
        .await?
    }

    pub async fn get_task(&self, task_id: &str) -> StoreResult<Option<TaskRecord>> {
        let pool = self.pool.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.reader();
            let mut stmt = conn.prepare(
                "SELECT task_id, rune_name, status, input, output, error, \
                 created_at, started_at, completed_at \
                 FROM tasks WHERE task_id = ?1",
            )?;
            let result = stmt.query_row(rusqlite::params![task_id], |row| {
                Ok(TaskRecord {
                    task_id: row.get(0)?,
                    rune_name: row.get(1)?,
                    status: {
                        let s: String = row.get(2)?;
                        TaskStatus::parse(&s).unwrap_or_else(|| {
                            tracing::warn!(status = %s, "unknown task status in db — treating as pending");
                            TaskStatus::Pending
                        })
                    },
                    input: row.get(3)?,
                    output: row.get(4)?,
                    error: row.get(5)?,
                    created_at: row.get(6)?,
                    started_at: row.get(7)?,
                    completed_at: row.get(8)?,
                })
            });
            match result {
                Ok(task) => Ok(Some(task)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await?
    }

    pub async fn update_task_status(
        &self,
        task_id: &str,
        status: TaskStatus,
        output: Option<&str>,
        error: Option<&str>,
    ) -> StoreResult<()> {
        let pool = self.pool.clone();
        let task_id = task_id.to_string();
        let output = output.map(|s| s.to_string());
        let error = error.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || {
            let now = timestamp_now();
            let conn = pool.writer();
            match status {
                TaskStatus::Running => {
                    conn.execute(
                        "UPDATE tasks SET status = ?1, started_at = ?2 WHERE task_id = ?3",
                        rusqlite::params![status.as_str(), now, task_id],
                    )?;
                }
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled => {
                    conn.execute(
                        "UPDATE tasks SET status = ?1, output = ?2, error = ?3, \
                         completed_at = ?4 WHERE task_id = ?5",
                        rusqlite::params![status.as_str(), output, error, now, task_id],
                    )?;
                }
                TaskStatus::Pending => {
                    conn.execute(
                        "UPDATE tasks SET status = ?1 WHERE task_id = ?2",
                        rusqlite::params![status.as_str(), task_id],
                    )?;
                }
            }
            Ok(())
        })
        .await?
    }

    /// Atomically complete a task only if it has not been cancelled.
    /// Returns `true` if the row was updated, `false` if the task was already cancelled.
    pub async fn complete_task_if_not_cancelled(
        &self,
        task_id: &str,
        status: TaskStatus,
        output: Option<&str>,
        error: Option<&str>,
    ) -> StoreResult<bool> {
        let pool = self.pool.clone();
        let task_id = task_id.to_string();
        let output = output.map(|s| s.to_string());
        let error = error.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || {
            let now = timestamp_now();
            let conn = pool.writer();
            let rows = conn.execute(
                "UPDATE tasks SET status = ?1, output = ?2, error = ?3, \
                 completed_at = ?4 WHERE task_id = ?5 AND status != 'cancelled'",
                rusqlite::params![status.as_str(), output, error, now, task_id],
            )?;
            Ok(rows > 0)
        })
        .await?
    }

    pub async fn list_tasks(
        &self,
        status: Option<TaskStatus>,
        rune_name: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> StoreResult<Vec<TaskRecord>> {
        let limit = limit.clamp(0, 500);
        let offset = offset.max(0);
        let pool = self.pool.clone();
        let rune_name = rune_name.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = pool.reader();
            let mut sql = String::from(
                "SELECT task_id, rune_name, status, input, output, error, \
                 created_at, started_at, completed_at FROM tasks WHERE 1=1",
            );
            let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
            if let Some(s) = status {
                sql.push_str(" AND status = ?");
                params.push(Box::new(s.as_str().to_string()));
            }
            if let Some(rn) = rune_name {
                sql.push_str(" AND rune_name = ?");
                params.push(Box::new(rn));
            }
            sql.push_str(" ORDER BY created_at DESC LIMIT ? OFFSET ?");
            params.push(Box::new(limit));
            params.push(Box::new(offset));
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
            let mut stmt = conn.prepare(&sql)?;
            let tasks = stmt
                .query_map(param_refs.as_slice(), |row| {
                    Ok(TaskRecord {
                        task_id: row.get(0)?,
                        rune_name: row.get(1)?,
                        status: {
                            let s: String = row.get(2)?;
                            TaskStatus::parse(&s).unwrap_or_else(|| {
                                tracing::warn!(
                                    status = %s,
                                    "unknown task status in db (list_tasks) — treating as pending"
                                );
                                TaskStatus::Pending
                            })
                        },
                        input: row.get(3)?,
                        output: row.get(4)?,
                        error: row.get(5)?,
                        created_at: row.get(6)?,
                        started_at: row.get(7)?,
                        completed_at: row.get(8)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(tasks)
        })
        .await?
    }

    /// Delete completed/failed tasks whose `created_at` is older than `before` (ISO-8601).
    /// Returns the number of rows deleted.
    pub async fn cleanup_tasks_before(&self, before: &str) -> StoreResult<u64> {
        let pool = self.pool.clone();
        let before = before.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.writer();
            let deleted = conn.execute(
                "DELETE FROM tasks WHERE created_at < ?1 AND status IN ('completed', 'failed')",
                rusqlite::params![before],
            )?;
            Ok(deleted as u64)
        })
        .await?
    }
}
