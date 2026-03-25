use crate::models::{ApiKey, KeyType};
use crate::store::{RuneStore, StoreResult};
use rand::Rng;
use sha2::{Digest, Sha256};

pub struct CreateKeyResult {
    pub raw_key: String,
    pub api_key: ApiKey,
}

impl RuneStore {
    pub async fn create_key(&self, key_type: KeyType, label: &str) -> StoreResult<CreateKeyResult> {
        let conn = self.conn.clone();
        let label = label.to_string();
        tokio::task::spawn_blocking(move || {
            let raw_key = generate_raw_key();
            let key_prefix = format!("rk_{}", &raw_key[3..19]);
            let key_hash = hash_key(&raw_key);
            let now = now_iso8601();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "INSERT INTO api_keys (key_prefix, key_hash, key_type, label, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![key_prefix, key_hash, key_type.as_str(), label, now],
            )?;
            let id = conn.last_insert_rowid();
            Ok(CreateKeyResult {
                raw_key: raw_key.clone(),
                api_key: ApiKey {
                    id,
                    key_prefix,
                    key_hash: Some(key_hash),
                    key_type,
                    label,
                    created_at: now,
                    revoked_at: None,
                },
            })
        })
        .await?
    }

    pub async fn verify_key(
        &self,
        raw_key: &str,
        expected_type: KeyType,
    ) -> StoreResult<Option<ApiKey>> {
        if raw_key.is_empty() || !raw_key.starts_with("rk_") {
            return Ok(None);
        }
        let conn = self.conn.clone();
        let key_hash = hash_key(raw_key);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = conn.prepare(
                "SELECT id, key_prefix, key_hash, key_type, label, created_at, revoked_at \
                 FROM api_keys WHERE key_hash = ?1",
            )?;
            let result = stmt.query_row(rusqlite::params![key_hash], |row| {
                let key_type_str = row.get::<_, String>(3)?;
                let key_type = KeyType::from_str(&key_type_str).ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        format!("unknown key_type: '{}'", key_type_str).into(),
                    )
                })?;
                Ok(ApiKey {
                    id: row.get(0)?,
                    key_prefix: row.get(1)?,
                    key_hash: row.get::<_, String>(2).ok(),
                    key_type,
                    label: row.get(4)?,
                    created_at: row.get(5)?,
                    revoked_at: row.get(6)?,
                })
            });
            match result {
                Ok(key) => {
                    if key.revoked_at.is_some() {
                        return Ok(None);
                    }
                    if key.key_type != expected_type {
                        return Ok(None);
                    }
                    Ok(Some(key))
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await?
    }

    pub async fn list_keys(&self) -> StoreResult<Vec<ApiKey>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = conn.prepare(
                "SELECT id, key_prefix, key_type, label, created_at, revoked_at \
                 FROM api_keys ORDER BY id",
            )?;
            let keys = stmt
                .query_map([], |row| {
                    let key_type_str = row.get::<_, String>(2)?;
                    let key_type = KeyType::from_str(&key_type_str).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            format!("unknown key_type: '{}'", key_type_str).into(),
                        )
                    })?;
                    Ok(ApiKey {
                        id: row.get(0)?,
                        key_prefix: row.get(1)?,
                        key_hash: None,
                        key_type,
                        label: row.get(3)?,
                        created_at: row.get(4)?,
                        revoked_at: row.get(5)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(keys)
        })
        .await?
    }

    pub async fn revoke_key(&self, key_id: i64) -> StoreResult<()> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let now = now_iso8601();
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "UPDATE api_keys SET revoked_at = ?1 WHERE id = ?2 AND revoked_at IS NULL",
                rusqlite::params![now, key_id],
            )?;
            Ok(())
        })
        .await?
    }
}

fn generate_raw_key() -> String {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    format!("rk_{}", hex::encode(bytes))
}

fn hash_key(raw_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    hex::encode(hasher.finalize())
}

/// Delegates to the canonical implementation in `rune_core::time_utils`.
pub fn now_iso8601() -> String {
    rune_core::time_utils::now_iso8601()
}

pub(crate) use now_iso8601 as timestamp_now;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::KeyType;

    #[tokio::test]
    async fn test_key_prefix_has_sufficient_entropy() {
        // key_prefix should be "rk_" + 16 hex chars (8 bytes = 64 bit)
        let store = crate::store::RuneStore::open_in_memory().unwrap();
        let result = store.create_key(KeyType::Gate, "test").await.unwrap();

        let prefix = &result.api_key.key_prefix;
        assert!(prefix.starts_with("rk_"), "prefix should start with rk_, got: {}", prefix);
        // "rk_" (3 chars) + 16 hex chars = 19 chars total
        assert_eq!(prefix.len(), 19, "prefix should be 19 chars (rk_ + 16 hex), got {} ({} chars)", prefix, prefix.len());

        // The hex part after "rk_" should be valid hex
        let hex_part = &prefix[3..];
        assert!(hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "prefix hex part should be valid hex, got: {}", hex_part);
    }

    #[tokio::test]
    async fn test_key_prefix_uniqueness() {
        // With only 32-bit prefix, collision probability is high for moderate key counts.
        // With 64-bit prefix, collisions should be extremely rare.
        let store = crate::store::RuneStore::open_in_memory().unwrap();
        let mut prefixes = std::collections::HashSet::new();
        for _ in 0..100 {
            let result = store.create_key(KeyType::Gate, "test").await.unwrap();
            prefixes.insert(result.api_key.key_prefix);
        }
        // All 100 prefixes should be unique with 64-bit entropy
        assert_eq!(prefixes.len(), 100, "all key prefixes should be unique");
    }
}
