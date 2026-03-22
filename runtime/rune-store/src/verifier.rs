use std::sync::Arc;

use async_trait::async_trait;
use rune_core::auth::KeyVerifier;

use crate::models::KeyType;
use crate::store::RuneStore;

/// Bridges RuneStore to the KeyVerifier trait.
/// Verifies API keys by looking them up in the SQLite database.
pub struct StoreKeyVerifier {
    store: Arc<RuneStore>,
}

impl StoreKeyVerifier {
    pub fn new(store: Arc<RuneStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl KeyVerifier for StoreKeyVerifier {
    async fn verify_gate_key(&self, raw_key: &str) -> bool {
        self.store
            .verify_key(raw_key, KeyType::Gate)
            .ok()
            .flatten()
            .is_some()
    }

    async fn verify_caster_key(&self, raw_key: &str) -> bool {
        self.store
            .verify_key(raw_key, KeyType::Caster)
            .ok()
            .flatten()
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_verify_gate_key_valid() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let result = store.create_key(KeyType::Gate, "test gate key").unwrap();
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
        let result = store.create_key(KeyType::Caster, "test caster key").unwrap();
        let verifier = StoreKeyVerifier::new(store);

        assert!(verifier.verify_caster_key(&result.raw_key).await);
    }

    #[tokio::test]
    async fn test_verify_wrong_key_type() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let gate_key = store.create_key(KeyType::Gate, "gate").unwrap();
        let caster_key = store.create_key(KeyType::Caster, "caster").unwrap();
        let verifier = StoreKeyVerifier::new(store);

        // Gate key should not pass caster verification
        assert!(!verifier.verify_caster_key(&gate_key.raw_key).await);
        // Caster key should not pass gate verification
        assert!(!verifier.verify_gate_key(&caster_key.raw_key).await);
    }

    #[tokio::test]
    async fn test_verify_revoked_key() {
        let store = Arc::new(RuneStore::open_in_memory().unwrap());
        let result = store.create_key(KeyType::Gate, "revocable").unwrap();
        store.revoke_key(result.api_key.id).unwrap();
        let verifier = StoreKeyVerifier::new(store);

        assert!(!verifier.verify_gate_key(&result.raw_key).await);
    }
}
