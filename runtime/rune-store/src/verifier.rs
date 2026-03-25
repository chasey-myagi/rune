use crate::models::KeyType;
use crate::store::RuneStore;
use async_trait::async_trait;
use rune_core::auth::KeyVerifier;
use std::sync::Arc;

pub struct StoreKeyVerifier {
    store: Arc<RuneStore>,
    /// HMAC secret for key verification. When `Some`, uses HMAC-SHA256 with
    /// fallback to legacy SHA-256. When `None`, uses plain SHA-256 only.
    hmac_secret: Option<Vec<u8>>,
}

impl StoreKeyVerifier {
    /// Create a verifier that uses legacy SHA-256 only (backward compatible).
    pub fn new(store: Arc<RuneStore>) -> Self {
        Self {
            store,
            hmac_secret: None,
        }
    }

    /// Create a verifier that uses HMAC-SHA256 with fallback to legacy SHA-256.
    pub fn with_hmac_secret(store: Arc<RuneStore>, secret: Vec<u8>) -> Self {
        Self {
            store,
            hmac_secret: Some(secret),
        }
    }

    async fn verify(&self, raw_key: &str, key_type: KeyType) -> bool {
        let result = match &self.hmac_secret {
            Some(secret) => self.store.verify_key_hmac(raw_key, key_type, secret).await,
            None => self.store.verify_key(raw_key, key_type).await,
        };
        result
            .inspect_err(|e| {
                tracing::error!(
                    key_type = key_type.as_str(),
                    error = %e,
                    "verify_key failed due to store error"
                )
            })
            .ok()
            .flatten()
            .is_some()
    }
}

#[async_trait]
impl KeyVerifier for StoreKeyVerifier {
    async fn verify_gate_key(&self, raw_key: &str) -> bool {
        self.verify(raw_key, KeyType::Gate).await
    }
    async fn verify_caster_key(&self, raw_key: &str) -> bool {
        self.verify(raw_key, KeyType::Caster).await
    }
    async fn verify_admin_key(&self, raw_key: &str) -> bool {
        self.verify(raw_key, KeyType::Admin).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_verify_gate_key_valid() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let result = store
            .create_key(KeyType::Gate, "test gate key")
            .await
            .unwrap();
        let verifier = StoreKeyVerifier::new(store);
        assert!(verifier.verify_gate_key(&result.raw_key).await);
    }
    #[tokio::test]
    async fn test_verify_gate_key_invalid() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let verifier = StoreKeyVerifier::new(store);
        assert!(!verifier.verify_gate_key("rk_invalid").await);
        assert!(!verifier.verify_gate_key("").await);
        assert!(!verifier.verify_gate_key("not_a_key").await);
    }
    #[tokio::test]
    async fn test_verify_caster_key_valid() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let result = store
            .create_key(KeyType::Caster, "test caster key")
            .await
            .unwrap();
        let verifier = StoreKeyVerifier::new(store);
        assert!(verifier.verify_caster_key(&result.raw_key).await);
    }
    #[tokio::test]
    async fn test_verify_wrong_key_type() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let gate_key = store.create_key(KeyType::Gate, "gate").await.unwrap();
        let caster_key = store.create_key(KeyType::Caster, "caster").await.unwrap();
        let verifier = StoreKeyVerifier::new(store);
        assert!(!verifier.verify_caster_key(&gate_key.raw_key).await);
        assert!(!verifier.verify_gate_key(&caster_key.raw_key).await);
    }
    #[tokio::test]
    async fn test_verify_revoked_key() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let result = store.create_key(KeyType::Gate, "revocable").await.unwrap();
        store.revoke_key(result.api_key.id).await.unwrap();
        let verifier = StoreKeyVerifier::new(store);
        assert!(!verifier.verify_gate_key(&result.raw_key).await);
    }

    #[tokio::test]
    async fn test_verify_db_error_returns_false_not_panic() {
        // NF-18: DB errors should not silently succeed; they should return false
        // and log the error (logging tested via inspect_err in implementation).
        // Simulate a "broken" store by dropping the table.
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let result = store.create_key(KeyType::Gate, "test").await.unwrap();

        // Drop the api_keys table to simulate a DB error
        {
            let conn = store.conn.lock().unwrap();
            conn.execute_batch("DROP TABLE api_keys").unwrap();
        }

        let verifier = StoreKeyVerifier::new(store);
        // Should return false (not panic), and the error is logged via inspect_err
        assert!(!verifier.verify_gate_key(&result.raw_key).await);
    }

    #[tokio::test]
    async fn test_hmac_verifier_with_hmac_key() {
        let secret = b"test-hmac-secret".to_vec();
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let result = store
            .create_key_hmac(KeyType::Gate, "hmac gate key", &secret)
            .await
            .unwrap();
        let verifier = StoreKeyVerifier::with_hmac_secret(store, secret);
        assert!(verifier.verify_gate_key(&result.raw_key).await);
    }

    #[tokio::test]
    async fn test_hmac_verifier_legacy_fallback() {
        let secret = b"test-hmac-secret".to_vec();
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        // Create key with legacy SHA-256
        let result = store
            .create_key(KeyType::Gate, "legacy gate key")
            .await
            .unwrap();
        // Verify with HMAC verifier — should fall back to SHA-256
        let verifier = StoreKeyVerifier::with_hmac_secret(store, secret);
        assert!(verifier.verify_gate_key(&result.raw_key).await);
    }
}
