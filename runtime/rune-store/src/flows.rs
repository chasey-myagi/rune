use crate::keys::timestamp_now;
use crate::store::{RuneStore, StoreError, StoreResult};
use rune_flow::dag::FlowDefinition;

impl RuneStore {
    pub async fn create_flow(&self, flow: &FlowDefinition) -> StoreResult<()> {
        let pool = self.pool.clone();
        let flow = flow.clone();
        tokio::task::spawn_blocking(move || {
            let now = timestamp_now();
            let definition_json = serde_json::to_string(&flow)?;
            let conn = pool.writer();
            match conn.execute(
                "INSERT INTO flows (name, definition_json, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![flow.name, definition_json, now, now],
            ) {
                Ok(_) => Ok(()),
                Err(err) if is_unique_constraint(&err) => Err(StoreError::DuplicateFlow(flow.name)),
                Err(err) => Err(err.into()),
            }
        })
        .await?
    }

    pub async fn upsert_flow(&self, flow: &FlowDefinition) -> StoreResult<()> {
        let pool = self.pool.clone();
        let flow = flow.clone();
        tokio::task::spawn_blocking(move || {
            let now = timestamp_now();
            let definition_json = serde_json::to_string(&flow)?;
            let conn = pool.writer();
            conn.execute(
                "INSERT INTO flows (name, definition_json, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(name) DO UPDATE SET \
                 definition_json = excluded.definition_json, \
                 updated_at = excluded.updated_at",
                rusqlite::params![flow.name, definition_json, now, now],
            )?;
            Ok(())
        })
        .await?
    }

    pub async fn get_flow(&self, name: &str) -> StoreResult<Option<FlowDefinition>> {
        let pool = self.pool.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.reader();
            let mut stmt = conn.prepare("SELECT definition_json FROM flows WHERE name = ?1")?;
            let result = stmt.query_row(rusqlite::params![name], |row| row.get::<_, String>(0));
            match result {
                Ok(definition_json) => Ok(Some(serde_json::from_str(&definition_json)?)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(err) => Err(err.into()),
            }
        })
        .await?
    }

    pub async fn list_flows(&self) -> StoreResult<Vec<FlowDefinition>> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.reader();
            let mut stmt = conn.prepare("SELECT definition_json FROM flows ORDER BY name ASC")?;
            let definitions = stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;

            let mut flows = Vec::with_capacity(definitions.len());
            for definition_json in definitions {
                flows.push(serde_json::from_str(&definition_json)?);
            }
            Ok(flows)
        })
        .await?
    }

    pub async fn delete_flow(&self, name: &str) -> StoreResult<bool> {
        let pool = self.pool.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.writer();
            let deleted =
                conn.execute("DELETE FROM flows WHERE name = ?1", rusqlite::params![name])?;
            Ok(deleted > 0)
        })
        .await?
    }
}

fn is_unique_constraint(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::ConstraintViolation,
                ..
            },
            _
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rune_flow::dag::StepDefinition;

    fn sample_flow(name: &str) -> FlowDefinition {
        FlowDefinition {
            name: name.to_string(),
            steps: vec![StepDefinition {
                name: "step-a".to_string(),
                rune: "echo".to_string(),
                depends_on: Vec::new(),
                condition: None,
                input_mapping: None,
                timeout_ms: None,
            }],
            gate_path: Some(format!("/{name}")),
        }
    }

    #[tokio::test]
    async fn create_and_get_flow_round_trip() {
        let store = RuneStore::open_in_memory().unwrap();
        let flow = sample_flow("pipeline");

        store.create_flow(&flow).await.unwrap();
        let loaded = store.get_flow("pipeline").await.unwrap();

        assert_eq!(loaded.unwrap().name, flow.name);
    }

    #[tokio::test]
    async fn create_flow_rejects_duplicates() {
        let store = RuneStore::open_in_memory().unwrap();
        let flow = sample_flow("pipeline");

        store.create_flow(&flow).await.unwrap();
        let err = store.create_flow(&flow).await.unwrap_err();

        assert!(matches!(err, StoreError::DuplicateFlow(name) if name == "pipeline"));
    }

    #[tokio::test]
    async fn upsert_flow_updates_existing_definition() {
        let store = RuneStore::open_in_memory().unwrap();
        let mut flow = sample_flow("pipeline");
        store.create_flow(&flow).await.unwrap();

        flow.gate_path = Some("/updated".to_string());
        store.upsert_flow(&flow).await.unwrap();

        let loaded = store.get_flow("pipeline").await.unwrap().unwrap();
        assert_eq!(loaded.gate_path.as_deref(), Some("/updated"));
    }

    #[tokio::test]
    async fn list_and_delete_flows() {
        let store = RuneStore::open_in_memory().unwrap();
        store.create_flow(&sample_flow("a")).await.unwrap();
        store.create_flow(&sample_flow("b")).await.unwrap();

        let flows = store.list_flows().await.unwrap();
        assert_eq!(flows.len(), 2);
        assert_eq!(flows[0].name, "a");
        assert_eq!(flows[1].name, "b");

        assert!(store.delete_flow("a").await.unwrap());
        assert!(!store.delete_flow("missing").await.unwrap());
    }
}
