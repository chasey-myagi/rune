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

// ---- Scenario 6: Bearer prefix variant ----
// The KeyVerifier trait receives the raw key string, not the full
// "Authorization: Bearer <token>" header.  Stripping the "Bearer " prefix
// is the responsibility of the HTTP middleware layer (rune-gate), not
// rune-core's KeyVerifier.  Therefore we only test that NoopVerifier
// accepts keys that happen to contain the "Bearer " prefix as-is.
//
// A proper "Bearer prefix stripping" test belongs in the rune-gate crate
// where the HTTP middleware lives.
// TODO: add Bearer-prefix-stripping test to rune-gate's middleware tests.

#[tokio::test]
async fn test_noop_verifier_bearer_prefix_variants() {
    let verifier = NoopVerifier;

    // Keys with Bearer-style prefixes are just opaque strings to KeyVerifier
    assert!(verifier.verify_gate_key("Bearer my-token-123").await);
    assert!(verifier.verify_gate_key("bearer my-token-123").await);
    assert!(verifier.verify_gate_key("BEARER my-token-123").await);
    assert!(verifier.verify_gate_key("Bearer").await);
    assert!(verifier.verify_gate_key("Bearer ").await);
}

// ---- Concurrent verification calls ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_noop_verifier_concurrent_gate_and_caster() {
    use std::sync::Arc;

    let verifier = Arc::new(NoopVerifier);
    let mut handles = Vec::new();

    // Spawn 50 gate key verifications and 50 caster key verifications concurrently
    for i in 0..50 {
        let v = Arc::clone(&verifier);
        let key = format!("gate-key-{}", i);
        handles.push(tokio::spawn(async move {
            assert!(v.verify_gate_key(&key).await, "gate key {} should pass", key);
        }));
    }
    for i in 0..50 {
        let v = Arc::clone(&verifier);
        let key = format!("caster-key-{}", i);
        handles.push(tokio::spawn(async move {
            assert!(v.verify_caster_key(&key).await, "caster key {} should pass", key);
        }));
    }

    // All tasks should complete successfully
    for handle in handles {
        handle.await.unwrap();
    }
}

// ---- Keys not starting with rk_ prefix ----

#[tokio::test]
async fn test_noop_verifier_non_standard_prefixes() {
    let verifier = NoopVerifier;

    // Keys without standard prefixes (NoopVerifier doesn't care about format)
    assert!(verifier.verify_gate_key("no-prefix-at-all").await);
    assert!(verifier.verify_gate_key("sk_live_1234").await);
    assert!(verifier.verify_gate_key("pk_test_abcd").await);
    assert!(verifier.verify_caster_key("random-string").await);
}

// ---- rk_ prefix variants (NoopVerifier accepts all) ----

#[tokio::test]
async fn test_noop_verifier_rk_prefix_variants() {
    let verifier = NoopVerifier;

    // Standard rk_ prefixed keys
    assert!(verifier.verify_gate_key("rk_abc123").await);
    assert!(verifier.verify_caster_key("rk_def456").await);

    // Just the prefix with nothing after
    assert!(verifier.verify_gate_key("rk_").await);

    // Prefix with non-hex characters (NoopVerifier doesn't validate format)
    assert!(verifier.verify_gate_key("rk_zzz-not-hex!@#").await);
    assert!(verifier.verify_caster_key("rk_spaces and stuff").await);
}

// ---- Extreme key lengths ----

#[tokio::test]
async fn test_noop_verifier_10k_character_key() {
    let verifier = NoopVerifier;

    let long_key = "x".repeat(10_000);
    assert!(verifier.verify_gate_key(&long_key).await);
    assert!(verifier.verify_caster_key(&long_key).await);
}

// ---- Null bytes and binary-like content ----

#[tokio::test]
async fn test_noop_verifier_null_bytes_and_binary() {
    let verifier = NoopVerifier;

    // Key containing null bytes
    let key_with_null = "key\0with\0nulls";
    assert!(verifier.verify_gate_key(key_with_null).await);
    assert!(verifier.verify_caster_key(key_with_null).await);

    // Key that is just whitespace
    assert!(verifier.verify_gate_key("   ").await);
    assert!(verifier.verify_caster_key("\t\t\t").await);
}

// ---- Unicode edge cases ----

#[tokio::test]
async fn test_noop_verifier_unicode_edge_cases() {
    let verifier = NoopVerifier;

    // Emoji key
    assert!(verifier.verify_gate_key("key-with-emoji-\u{1F600}").await);
    // Zero-width characters
    assert!(verifier.verify_gate_key("key\u{200B}with\u{200B}zwsp").await);
    // Right-to-left text
    assert!(verifier.verify_caster_key("\u{202E}backwards").await);
    // Very long unicode
    let unicode_key = "\u{1F600}".repeat(1000);
    assert!(verifier.verify_gate_key(&unicode_key).await);
}
