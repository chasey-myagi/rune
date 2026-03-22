use crate::keys::timestamp_now;
use crate::models::RuneSnapshot;
use crate::store::{RuneStore, StoreResult};

impl RuneStore {
    /// Upsert a rune snapshot. Inserts if new, updates all fields if exists.
    pub fn upsert_snapshot(&self, snapshot: &RuneSnapshot) -> StoreResult<()> {
        let now = timestamp_now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO rune_snapshots (rune_name, version, description, supports_stream, gate_path, gate_method, last_seen)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(rune_name) DO UPDATE SET
                version = excluded.version,
                description = excluded.description,
                supports_stream = excluded.supports_stream,
                gate_path = excluded.gate_path,
                gate_method = excluded.gate_method,
                last_seen = excluded.last_seen",
            rusqlite::params![
                snapshot.rune_name,
                snapshot.version,
                snapshot.description,
                snapshot.supports_stream as i32,
                snapshot.gate_path,
                snapshot.gate_method,
                now,
            ],
        )?;
        Ok(())
    }

    /// List all rune snapshots.
    pub fn list_snapshots(&self) -> StoreResult<Vec<RuneSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT rune_name, version, description, supports_stream, gate_path, gate_method, last_seen FROM rune_snapshots ORDER BY rune_name",
        )?;
        let snapshots = stmt
            .query_map([], |row| {
                Ok(RuneSnapshot {
                    rune_name: row.get(0)?,
                    version: row.get(1)?,
                    description: row.get(2)?,
                    supports_stream: row.get::<_, i32>(3)? != 0,
                    gate_path: row.get(4)?,
                    gate_method: row.get(5)?,
                    last_seen: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(snapshots)
    }
}
