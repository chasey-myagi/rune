use rand::Rng;
use sha2::{Digest, Sha256};

use crate::models::{ApiKey, KeyType};
use crate::store::{RuneStore, StoreResult};

/// Result of creating a new API key — includes the raw key (shown once).
pub struct CreateKeyResult {
    pub raw_key: String,
    pub api_key: ApiKey,
}

impl RuneStore {
    /// Generate a new API key of the given type with a label.
    /// Returns the raw key (only chance to see it) and the stored record.
    pub fn create_key(&self, key_type: KeyType, label: &str) -> StoreResult<CreateKeyResult> {
        let raw_key = generate_raw_key();
        let key_prefix = format!("rk_{}", &raw_key[3..11]); // first 8 hex chars after "rk_"
        let key_hash = hash_key(&raw_key);
        let now = now_iso8601();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO api_keys (key_prefix, key_hash, key_type, label, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
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
                label: label.to_string(),
                created_at: now,
                revoked_at: None,
            },
        })
    }

    /// Verify a raw key against stored keys.
    /// Returns the matching ApiKey if valid and not revoked, with the expected key_type.
    pub fn verify_key(&self, raw_key: &str, expected_type: KeyType) -> StoreResult<Option<ApiKey>> {
        if raw_key.is_empty() || !raw_key.starts_with("rk_") {
            return Ok(None);
        }
        let key_hash = hash_key(raw_key);
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, key_prefix, key_hash, key_type, label, created_at, revoked_at FROM api_keys WHERE key_hash = ?1",
        )?;
        let result = stmt.query_row(rusqlite::params![key_hash], |row| {
            Ok(ApiKey {
                id: row.get(0)?,
                key_prefix: row.get(1)?,
                key_hash: row.get::<_, String>(2).ok(),
                key_type: KeyType::from_str(&row.get::<_, String>(3)?).unwrap_or(KeyType::Gate),
                label: row.get(4)?,
                created_at: row.get(5)?,
                revoked_at: row.get(6)?,
            })
        });

        match result {
            Ok(key) => {
                // Check not revoked
                if key.revoked_at.is_some() {
                    return Ok(None);
                }
                // Check type matches
                if key.key_type != expected_type {
                    return Ok(None);
                }
                Ok(Some(key))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// List all API keys. Key hashes are NOT included in the result.
    pub fn list_keys(&self) -> StoreResult<Vec<ApiKey>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, key_prefix, key_type, label, created_at, revoked_at FROM api_keys ORDER BY id",
        )?;
        let keys = stmt
            .query_map([], |row| {
                Ok(ApiKey {
                    id: row.get(0)?,
                    key_prefix: row.get(1)?,
                    key_hash: None, // intentionally hidden
                    key_type: KeyType::from_str(&row.get::<_, String>(2)?).unwrap_or(KeyType::Gate),
                    label: row.get(3)?,
                    created_at: row.get(4)?,
                    revoked_at: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(keys)
    }

    /// Revoke an API key by id. No-op if the key doesn't exist.
    pub fn revoke_key(&self, key_id: i64) -> StoreResult<()> {
        let now = now_iso8601();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE api_keys SET revoked_at = ?1 WHERE id = ?2 AND revoked_at IS NULL",
            rusqlite::params![now, key_id],
        )?;
        Ok(())
    }
}

/// Generate a raw key in the format `rk_{32_hex_chars}`.
fn generate_raw_key() -> String {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    format!("rk_{}", hex::encode(bytes))
}

/// SHA-256 hash of a raw key, returned as hex string.
fn hash_key(raw_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn now_iso8601() -> String {
    // Simple ISO 8601 timestamp without external chrono dependency
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = dur.as_secs();
    // Format as a basic ISO 8601 timestamp
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    // Calculate year/month/day from days since epoch (1970-01-01)
    let (year, month, day) = days_to_ymd(days);

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

pub(crate) fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    days += 719468;
    let era = days / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// Re-export for use in other modules
pub(crate) use now_iso8601 as timestamp_now;

