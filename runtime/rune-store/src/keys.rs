use crate::key_cache::CacheResult;
use crate::models::{ApiKey, KeyType};
use crate::pool::ConnectionPool;
use crate::store::{RuneStore, StoreError, StoreResult};
use hmac::{Hmac, Mac};
use rand::TryRngCore;
use rusqlite::{Connection, Error as SqlError, OptionalExtension, Row};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub struct CreateKeyResult {
    pub raw_key: String,
    pub api_key: ApiKey,
}

impl RuneStore {
    pub async fn create_key(&self, key_type: KeyType, label: &str) -> StoreResult<CreateKeyResult> {
        let pool = self.pool.clone();
        let label = label.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.writer();
            insert_generated_key(&conn, key_type, &label)
        })
        .await?
    }

    pub async fn has_admin_key(&self) -> StoreResult<bool> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.reader();
            has_active_admin_key(&conn)
        })
        .await?
    }

    pub fn has_admin_key_sync(&self) -> StoreResult<bool> {
        let conn = self.pool.reader();
        has_active_admin_key(&conn)
    }

    pub fn bootstrap_admin_key(&self, label: &str) -> StoreResult<CreateKeyResult> {
        let conn = self.pool.writer();
        insert_generated_key(&conn, KeyType::Admin, label)
    }

    pub async fn import_admin_key(&self, raw_key: &str, label: &str) -> StoreResult<ApiKey> {
        let pool = self.pool.clone();
        let raw_key = raw_key.to_string();
        let label = label.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.writer();
            insert_raw_key(&conn, &raw_key, KeyType::Admin, &label)
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
        let pool = self.pool.clone();
        let key_cache = self.key_cache.clone();
        let key_hash = hash_key(raw_key);
        tokio::task::spawn_blocking(move || {
            load_key_by_hashes(
                pool.as_ref(),
                key_cache.as_ref(),
                &[key_hash.as_str()],
                expected_type,
            )
        })
        .await?
    }

    pub async fn list_keys(&self) -> StoreResult<Vec<ApiKey>> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.reader();
            let mut stmt = conn.prepare(
                "SELECT id, key_prefix, key_type, label, created_at, revoked_at, \
                        last_used_at, last_used_ip \
                 FROM api_keys ORDER BY id",
            )?;
            let keys = stmt
                .query_map([], |row| {
                    let key_type_str = row.get::<_, String>(2)?;
                    let key_type = KeyType::parse(&key_type_str).ok_or_else(|| {
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
                        last_used_at: row.get(6)?,
                        last_used_ip: row.get(7)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(keys)
        })
        .await?
    }

    /// Create an API key hashed with HMAC-SHA256 instead of plain SHA-256.
    pub async fn create_key_hmac(
        &self,
        key_type: KeyType,
        label: &str,
        secret: &[u8],
    ) -> StoreResult<CreateKeyResult> {
        let pool = self.pool.clone();
        let label = label.to_string();
        let secret = secret.to_vec();
        tokio::task::spawn_blocking(move || {
            let raw_key = generate_raw_key();
            let key_prefix = format!("rk_{}", &raw_key[3..19]);
            let key_hash = hmac_key(&raw_key, &secret);
            let now = now_iso8601();
            let conn = pool.writer();
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
                    last_used_at: None,
                    last_used_ip: None,
                },
            })
        })
        .await?
    }

    /// Verify an API key using HMAC-SHA256, falling back to legacy SHA-256.
    ///
    /// This enables zero-downtime migration: keys created with the old
    /// `hash_key` (plain SHA-256) are still accepted.
    pub async fn verify_key_hmac(
        &self,
        raw_key: &str,
        expected_type: KeyType,
        secret: &[u8],
    ) -> StoreResult<Option<ApiKey>> {
        if raw_key.is_empty() || !raw_key.starts_with("rk_") {
            return Ok(None);
        }
        let pool = self.pool.clone();
        let key_cache = self.key_cache.clone();
        let hmac_hash = hmac_key(raw_key, secret);
        let sha_hash = hash_key(raw_key);
        tokio::task::spawn_blocking(move || {
            load_key_by_hashes(
                pool.as_ref(),
                key_cache.as_ref(),
                &[hmac_hash.as_str(), sha_hash.as_str()],
                expected_type,
            )
        })
        .await?
    }

    pub async fn revoke_key(&self, key_id: i64) -> StoreResult<()> {
        let pool = self.pool.clone();
        let key_cache = self.key_cache.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.writer();
            // Look up key_hash first so we can do a targeted cache invalidation.
            let key_hash: Option<String> = conn
                .query_row(
                    "SELECT key_hash FROM api_keys WHERE id = ?1 AND revoked_at IS NULL",
                    rusqlite::params![key_id],
                    |row| row.get(0),
                )
                .optional()?;
            let now = now_iso8601();
            conn.execute(
                "UPDATE api_keys SET revoked_at = ?1 WHERE id = ?2 AND revoked_at IS NULL",
                rusqlite::params![now, key_id],
            )?;
            // Invalidate cache entry so the revoked key is not served from cache.
            if let Some(hash) = key_hash {
                key_cache.invalidate(&hash);
            }
            Ok(())
        })
        .await?
    }

    /// Update the last_used_at and last_used_ip audit fields identified by key_prefix.
    ///
    /// Uses `key_prefix` (the first 8 bytes / 16 hex chars of the key, always derived the
    /// same way) rather than `key_hash` so that both SHA-256 and HMAC-SHA256 keys are
    /// correctly matched — callers do not need to know which hash algorithm is in use.
    pub async fn update_key_last_used(
        &self,
        key_prefix: &str,
        used_at: &str,
        used_ip: &str,
    ) -> StoreResult<()> {
        let pool = self.pool.clone();
        let key_prefix = key_prefix.to_string();
        let used_at = used_at.to_string();
        let used_ip = used_ip.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = pool.writer();
            let rows = conn.execute(
                "UPDATE api_keys SET last_used_at = ?1, last_used_ip = ?2 \
                 WHERE key_prefix = ?3 AND revoked_at IS NULL",
                rusqlite::params![used_at, used_ip, key_prefix],
            )?;
            if rows == 0 {
                tracing::debug!(key_prefix = %key_prefix, "update_key_last_used: no matching row");
            }
            Ok(())
        })
        .await?
    }
}

fn generate_raw_key() -> String {
    let mut rng = rand::rngs::OsRng;
    let mut bytes = [0u8; 16];
    rng.try_fill_bytes(&mut bytes)
        .expect("OsRng should always be available");
    format!("rk_{}", hex::encode(bytes))
}

fn hash_key(raw_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    hex::encode(hasher.finalize())
}

fn hmac_key(raw_key: &str, secret: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts any key size");
    mac.update(raw_key.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn insert_generated_key(
    conn: &Connection,
    key_type: KeyType,
    label: &str,
) -> StoreResult<CreateKeyResult> {
    let raw_key = generate_raw_key();
    let api_key = insert_raw_key(conn, &raw_key, key_type, label)?;
    Ok(CreateKeyResult { raw_key, api_key })
}

fn insert_raw_key(
    conn: &Connection,
    raw_key: &str,
    key_type: KeyType,
    label: &str,
) -> StoreResult<ApiKey> {
    if !raw_key.starts_with("rk_") || raw_key.len() < 35 {
        return Err(StoreError::InvalidKeyFormat(
            "expected a key starting with 'rk_' and at least 35 characters (rk_ + 32 hex)"
                .to_string(),
        ));
    }
    let key_prefix = format!("rk_{}", &raw_key[3..19]);
    let key_hash = hash_key(raw_key);
    let now = now_iso8601();
    conn.execute(
        "INSERT INTO api_keys (key_prefix, key_hash, key_type, label, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![key_prefix, key_hash, key_type.as_str(), label, now],
    )?;
    let id = conn.last_insert_rowid();
    Ok(ApiKey {
        id,
        key_prefix,
        key_hash: Some(key_hash),
        key_type,
        label: label.to_string(),
        created_at: now,
        revoked_at: None,
        last_used_at: None,
        last_used_ip: None,
    })
}

fn has_active_admin_key(conn: &Connection) -> StoreResult<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM api_keys WHERE key_type = 'admin' AND revoked_at IS NULL",
        [],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn load_key_by_hashes(
    pool: &ConnectionPool,
    key_cache: &crate::key_cache::KeyCache,
    candidate_hashes: &[&str],
    expected_type: KeyType,
) -> StoreResult<Option<ApiKey>> {
    for candidate_hash in candidate_hashes {
        match key_cache.get(candidate_hash, expected_type) {
            CacheResult::Hit(key) => return Ok(Some(key)),
            CacheResult::NegativeHit => continue,
            CacheResult::Miss => {}
        }

        let key = {
            let conn = pool.reader();
            query_key_by_hash(&conn, candidate_hash)?
        };

        match key {
            Some(key) if key.revoked_at.is_none() && key.key_type == expected_type => {
                key_cache.insert((*candidate_hash).to_string(), key.clone());
                return Ok(Some(key));
            }
            Some(_) => {
                key_cache.insert_negative((*candidate_hash).to_string(), expected_type);
                return Ok(None);
            }
            None => {
                key_cache.insert_negative((*candidate_hash).to_string(), expected_type);
            }
        }
    }

    Ok(None)
}

fn query_key_by_hash(conn: &Connection, key_hash: &str) -> rusqlite::Result<Option<ApiKey>> {
    let mut stmt = conn.prepare(
        "SELECT id, key_prefix, key_hash, key_type, label, created_at, revoked_at, \
                last_used_at, last_used_ip \
         FROM api_keys WHERE key_hash = ?1",
    )?;
    let result = stmt.query_row(rusqlite::params![key_hash], parse_api_key_row);
    match result {
        Ok(key) => Ok(Some(key)),
        Err(SqlError::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

fn parse_api_key_row(row: &Row<'_>) -> rusqlite::Result<ApiKey> {
    let key_type_str = row.get::<_, String>(3)?;
    let key_type = KeyType::parse(&key_type_str).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            format!("unknown key_type: '{key_type_str}'").into(),
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
        last_used_at: row.get(7)?,
        last_used_ip: row.get(8)?,
    })
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
        assert!(
            prefix.starts_with("rk_"),
            "prefix should start with rk_, got: {}",
            prefix
        );
        // "rk_" (3 chars) + 16 hex chars = 19 chars total
        assert_eq!(
            prefix.len(),
            19,
            "prefix should be 19 chars (rk_ + 16 hex), got {} ({} chars)",
            prefix,
            prefix.len()
        );

        // The hex part after "rk_" should be valid hex
        let hex_part = &prefix[3..];
        assert!(
            hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "prefix hex part should be valid hex, got: {}",
            hex_part
        );
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

    #[tokio::test]
    async fn test_hmac_key_create_and_verify() {
        let store = crate::store::RuneStore::open_in_memory().unwrap();
        let secret = b"test-hmac-secret-key";

        // 创建 key 时用 HMAC
        let result = store
            .create_key_hmac(KeyType::Gate, "test-key", secret)
            .await
            .unwrap();
        assert!(result.raw_key.starts_with("rk_"));

        // 用相同 secret 验证应该成功
        let verified = store
            .verify_key_hmac(&result.raw_key, KeyType::Gate, secret)
            .await
            .unwrap();
        assert!(
            verified.is_some(),
            "HMAC verify with correct secret should succeed"
        );

        // 用错误 secret 验证应该失败
        let verified = store
            .verify_key_hmac(&result.raw_key, KeyType::Gate, b"wrong-secret")
            .await
            .unwrap();
        assert!(
            verified.is_none(),
            "HMAC verify with wrong secret should fail"
        );
    }

    #[tokio::test]
    async fn test_legacy_sha256_key_still_verifiable() {
        let store = crate::store::RuneStore::open_in_memory().unwrap();
        let secret = b"test-hmac-secret-key";

        // 用旧方式（SHA-256）创建一个 key
        let result = store.create_key(KeyType::Gate, "legacy-key").await.unwrap();

        // 用 HMAC 验证应该回退到 SHA-256 并成功
        let verified = store
            .verify_key_hmac(&result.raw_key, KeyType::Gate, secret)
            .await
            .unwrap();
        assert!(
            verified.is_some(),
            "legacy SHA-256 key should still be verifiable via HMAC fallback"
        );
    }

    #[tokio::test]
    async fn test_hmac_different_secrets_produce_different_hashes() {
        let secret_a = b"secret-alpha";
        let secret_b = b"secret-beta";
        let raw_key = "rk_deadbeef12345678deadbeef12345678";

        let hash_a = hmac_key(raw_key, secret_a);
        let hash_b = hmac_key(raw_key, secret_b);
        assert_ne!(
            hash_a, hash_b,
            "different secrets must produce different hashes"
        );
    }

    #[tokio::test]
    async fn verify_key_cache_hit_survives_table_drop() {
        let store = crate::store::RuneStore::open_in_memory().unwrap();
        let result = store.create_key(KeyType::Gate, "cached").await.unwrap();

        let first = store
            .verify_key(&result.raw_key, KeyType::Gate)
            .await
            .unwrap();
        assert!(first.is_some(), "first lookup should hit the database");

        {
            let conn = store.pool.writer();
            conn.execute_batch("DROP TABLE api_keys").unwrap();
        }

        let second = store
            .verify_key(&result.raw_key, KeyType::Gate)
            .await
            .unwrap();
        assert!(second.is_some(), "second lookup should come from cache");
    }

    #[tokio::test]
    async fn negative_cache_does_not_poison_other_key_type() {
        let store = crate::store::RuneStore::open_in_memory().unwrap();
        let result = store.create_key(KeyType::Gate, "typed").await.unwrap();

        let wrong_type = store
            .verify_key(&result.raw_key, KeyType::Caster)
            .await
            .unwrap();
        assert!(
            wrong_type.is_none(),
            "wrong key type should store a typed negative cache entry"
        );

        let correct_type = store
            .verify_key(&result.raw_key, KeyType::Gate)
            .await
            .unwrap();
        assert!(
            correct_type.is_some(),
            "typed negative cache entry must not affect the correct key type"
        );
    }

    #[tokio::test]
    async fn admin_key_presence_checks_ignore_revoked_keys() {
        let store = crate::store::RuneStore::open_in_memory().unwrap();
        assert!(!store.has_admin_key().await.unwrap());
        assert!(!store.has_admin_key_sync().unwrap());

        let result = store.bootstrap_admin_key("bootstrap-admin").unwrap();
        assert!(store.has_admin_key().await.unwrap());
        assert!(store.has_admin_key_sync().unwrap());

        store.revoke_key(result.api_key.id).await.unwrap();
        assert!(!store.has_admin_key().await.unwrap());
        assert!(!store.has_admin_key_sync().unwrap());
    }

    #[tokio::test]
    async fn bootstrap_admin_key_creates_verifiable_admin_key() {
        let store = crate::store::RuneStore::open_in_memory().unwrap();

        let result = store.bootstrap_admin_key("bootstrap-admin").unwrap();

        let verified = store
            .verify_key(&result.raw_key, KeyType::Admin)
            .await
            .unwrap();
        assert!(verified.is_some());
        assert_eq!(verified.unwrap().label, "bootstrap-admin");
    }

    #[tokio::test]
    async fn import_admin_key_round_trips() {
        let store = crate::store::RuneStore::open_in_memory().unwrap();
        let raw_key = "rk_1234567890abcdef1234567890abcdef";

        let imported = store
            .import_admin_key(raw_key, "initial-admin-from-env")
            .await
            .unwrap();
        assert_eq!(imported.key_type, KeyType::Admin);

        let verified = store.verify_key(raw_key, KeyType::Admin).await.unwrap();
        assert!(verified.is_some());
    }
}
