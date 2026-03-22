use rune_core::auth::{KeyVerifier, NoopVerifier};

/// Core behavior: NoopVerifier accepts any string for both gate and caster keys.
#[tokio::test]
async fn noop_verifier_accepts_any_key() {
    let verifier = NoopVerifier;

    // Normal keys
    assert!(verifier.verify_gate_key("some-api-key-123").await);
    assert!(verifier.verify_caster_key("caster-secret-key").await);

    // Empty keys
    assert!(verifier.verify_gate_key("").await);
    assert!(verifier.verify_caster_key("").await);

    // Special characters, unicode, null bytes
    assert!(verifier.verify_gate_key("key-with-spëcial-chars!@#$%").await);
    assert!(verifier.verify_caster_key("key\0with\0nulls").await);
    assert!(verifier.verify_gate_key("key-with-emoji-\u{1F600}").await);

    // rk_ prefix variants (NoopVerifier doesn't care about format)
    assert!(verifier.verify_gate_key("rk_abc123").await);
    assert!(verifier.verify_caster_key("rk_def456").await);

    // Bearer-style prefixes are just opaque strings to KeyVerifier
    assert!(verifier.verify_gate_key("Bearer my-token-123").await);
}

/// Edge case: very long keys are accepted.
#[tokio::test]
async fn noop_verifier_accepts_long_keys() {
    let verifier = NoopVerifier;
    let long_key = "x".repeat(10_000);
    assert!(verifier.verify_gate_key(&long_key).await);
    assert!(verifier.verify_caster_key(&long_key).await);
}

/// Concurrency: multiple tasks can verify simultaneously.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn noop_verifier_concurrent_verification() {
    use std::sync::Arc;

    let verifier = Arc::new(NoopVerifier);
    let mut handles = Vec::new();

    for i in 0..50 {
        let v = Arc::clone(&verifier);
        let key = format!("key-{}", i);
        handles.push(tokio::spawn(async move {
            assert!(v.verify_gate_key(&key).await);
            assert!(v.verify_caster_key(&key).await);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
