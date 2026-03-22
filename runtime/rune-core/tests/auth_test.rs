use rune_core::auth::{KeyVerifier, NoopVerifier};

#[tokio::test]
async fn test_noop_verifier_always_passes() {
    let verifier = NoopVerifier;

    assert!(verifier.verify_gate_key("some-api-key-123").await);
    assert!(verifier.verify_caster_key("caster-secret-key").await);
    assert!(verifier.verify_gate_key("another-key").await);
    assert!(verifier.verify_caster_key("another-caster-key").await);
}

#[tokio::test]
async fn test_noop_verifier_empty_key() {
    let verifier = NoopVerifier;

    assert!(verifier.verify_gate_key("").await);
    assert!(verifier.verify_caster_key("").await);
}

#[tokio::test]
async fn test_noop_verifier_special_characters() {
    let verifier = NoopVerifier;

    assert!(verifier.verify_gate_key("key-with-spëcial-chars!@#$%").await);
    assert!(verifier.verify_caster_key("  spaces  ").await);
    assert!(verifier.verify_gate_key("中文key").await);
    assert!(verifier.verify_caster_key("\n\t\r").await);
}

#[tokio::test]
async fn test_noop_verifier_very_long_key() {
    let verifier = NoopVerifier;

    let long_key = "a".repeat(10_000);
    assert!(verifier.verify_gate_key(&long_key).await);
    assert!(verifier.verify_caster_key(&long_key).await);
}
