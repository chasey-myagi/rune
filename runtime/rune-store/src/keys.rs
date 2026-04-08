use crate::models::{ApiKey, KeyType};
use crate::store::{RuneStore, StoreResult};
use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

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
                let key_type = KeyType::parse(&key_type_str).ok_or_else(|| {
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
        let conn = self.conn.clone();
        let label = label.to_string();
        let secret = secret.to_vec();
        tokio::task::spawn_blocking(move || {
            let raw_key = generate_raw_key();
            let key_prefix = format!("rk_{}", &raw_key[3..19]);
            let key_hash = hmac_key(&raw_key, &secret);
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
        let conn = self.conn.clone();
        let hmac_hash = hmac_key(raw_key, secret);
        let sha_hash = hash_key(raw_key);
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            // Try HMAC hash first, then fall back to legacy SHA-256 hash
            for candidate_hash in [&hmac_hash, &sha_hash] {
                let mut stmt = conn.prepare(
                    "SELECT id, key_prefix, key_hash, key_type, label, created_at, revoked_at \
                     FROM api_keys WHERE key_hash = ?1",
                )?;
                let result = stmt.query_row(rusqlite::params![candidate_hash], |row| {
                    let key_type_str = row.get::<_, String>(3)?;
                    let key_type = KeyType::parse(&key_type_str).ok_or_else(|| {
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
                        return Ok(Some(key));
                    }
                    Err(rusqlite::Error::QueryReturnedNoRows) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(None)
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

fn hmac_key(raw_key: &str, secret: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts any key size");
    mac.update(raw_key.as_bytes());
    hex::encode(mac.finalize().into_bytes())
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
}
