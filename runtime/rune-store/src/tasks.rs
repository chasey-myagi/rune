use crate::keys::timestamp_now;
use crate::models::{TaskRecord, TaskStatus};
use crate::store::{RuneStore, StoreResult};
impl RuneStore {
    pub async fn insert_task(&self, task_id: &str, rune_name: &str, input: Option<&str>) -> StoreResult<TaskRecord> {
        let conn = self.conn.clone(); let task_id = task_id.to_string(); let rune_name = rune_name.to_string(); let input = input.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || { let now = timestamp_now(); let conn = conn.lock().unwrap(); conn.execute("INSERT INTO tasks (task_id, rune_name, status, input, created_at) VALUES (?1, ?2, ?3, ?4, ?5)", rusqlite::params![task_id, rune_name, "pending", input, now])?; Ok(TaskRecord { task_id, rune_name, status: TaskStatus::Pending, input, output: None, error: None, created_at: now, started_at: None, completed_at: None }) }).await?
    }
    pub async fn get_task(&self, task_id: &str) -> StoreResult<Option<TaskRecord>> {
        let conn = self.conn.clone(); let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || { let conn = conn.lock().unwrap(); let mut stmt = conn.prepare("SELECT task_id, rune_name, status, input, output, error, created_at, started_at, completed_at FROM tasks WHERE task_id = ?1")?; let result = stmt.query_row(rusqlite::params![task_id], |row| Ok(TaskRecord { task_id: row.get(0)?, rune_name: row.get(1)?, status: TaskStatus::from_str(&row.get::<_, String>(2)?).unwrap_or(TaskStatus::Pending), input: row.get(3)?, output: row.get(4)?, error: row.get(5)?, created_at: row.get(6)?, started_at: row.get(7)?, completed_at: row.get(8)? })); match result { Ok(task) => Ok(Some(task)), Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None), Err(e) => Err(e.into()) } }).await?
    }
    pub async fn update_task_status(&self, task_id: &str, status: TaskStatus, output: Option<&str>, error: Option<&str>) -> StoreResult<()> {
        let conn = self.conn.clone(); let task_id = task_id.to_string(); let output = output.map(|s| s.to_string()); let error = error.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || { let now = timestamp_now(); let conn = conn.lock().unwrap(); match status { TaskStatus::Running => { conn.execute("UPDATE tasks SET status = ?1, started_at = ?2 WHERE task_id = ?3", rusqlite::params![status.as_str(), now, task_id])?; } TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled => { conn.execute("UPDATE tasks SET status = ?1, output = ?2, error = ?3, completed_at = ?4 WHERE task_id = ?5", rusqlite::params![status.as_str(), output, error, now, task_id])?; } TaskStatus::Pending => { conn.execute("UPDATE tasks SET status = ?1 WHERE task_id = ?2", rusqlite::params![status.as_str(), task_id])?; } } Ok(()) }).await?
    }
    pub async fn list_tasks(&self, status: Option<TaskStatus>, rune_name: Option<&str>, limit: i64, offset: i64) -> StoreResult<Vec<TaskRecord>> {
        let conn = self.conn.clone(); let rune_name = rune_name.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || { let conn = conn.lock().unwrap(); let mut sql = String::from("SELECT task_id, rune_name, status, input, output, error, created_at, started_at, completed_at FROM tasks WHERE 1=1"); let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new(); if let Some(s) = status { sql.push_str(" AND status = ?"); params.push(Box::new(s.as_str().to_string())); } if let Some(rn) = rune_name { sql.push_str(" AND rune_name = ?"); params.push(Box::new(rn)); } sql.push_str(" ORDER BY created_at DESC LIMIT ? OFFSET ?"); params.push(Box::new(limit)); params.push(Box::new(offset)); let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect(); let mut stmt = conn.prepare(&sql)?; let tasks = stmt.query_map(param_refs.as_slice(), |row| Ok(TaskRecord { task_id: row.get(0)?, rune_name: row.get(1)?, status: TaskStatus::from_str(&row.get::<_, String>(2)?).unwrap_or(TaskStatus::Pending), input: row.get(3)?, output: row.get(4)?, error: row.get(5)?, created_at: row.get(6)?, started_at: row.get(7)?, completed_at: row.get(8)? }))?.collect::<Result<Vec<_>, _>>()?; Ok(tasks) }).await?
    }
}
