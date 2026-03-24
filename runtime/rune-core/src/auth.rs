/// Trait for verifying API keys on both Gate (HTTP) and Caster (gRPC) paths.
///
/// Implementors can back this with a database lookup, an in-memory set,
/// or any async source.  The runtime calls these methods in middleware
/// before dispatching requests.
#[async_trait::async_trait]
pub trait KeyVerifier: Send + Sync {
    /// Verify a key presented by an HTTP Gate request.
    async fn verify_gate_key(&self, raw_key: &str) -> bool;

    /// Verify a key presented by a Caster during gRPC session attachment.
    async fn verify_caster_key(&self, raw_key: &str) -> bool;

    /// Verify a key has admin privileges (required for key management operations).
    async fn verify_admin_key(&self, raw_key: &str) -> bool;
}

/// A no-op verifier that accepts every key.
///
/// Used in development mode or when auth is disabled.
pub struct NoopVerifier;

#[async_trait::async_trait]
impl KeyVerifier for NoopVerifier {
    async fn verify_gate_key(&self, _raw_key: &str) -> bool {
        true
    }

    async fn verify_caster_key(&self, _raw_key: &str) -> bool {
        true
    }

    async fn verify_admin_key(&self, _raw_key: &str) -> bool {
        true
    }
}
