use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};
use tonic::Request;

use rune_proto::rune_service_client::RuneServiceClient;
use rune_proto::rune_service_server::RuneServiceServer;
use rune_proto::*;

use rune_core::auth::{KeyVerifier, NoopVerifier};
use rune_core::grpc_service::RuneGrpcService;
use rune_core::relay::Relay;
use rune_core::session::SessionManager;

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

// ============================================================================
// Test helpers for auth integration tests
// ============================================================================

/// A verifier that only accepts a specific key.
struct StaticKeyVerifier {
    valid_key: String,
}

#[async_trait::async_trait]
impl KeyVerifier for StaticKeyVerifier {
    async fn verify_gate_key(&self, raw_key: &str) -> bool {
        raw_key == self.valid_key
    }
    async fn verify_caster_key(&self, raw_key: &str) -> bool {
        raw_key == self.valid_key
    }
    async fn verify_admin_key(&self, raw_key: &str) -> bool {
        raw_key == self.valid_key
    }
}

async fn start_auth_server(
    relay: Arc<Relay>,
    session_mgr: Arc<SessionManager>,
) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let grpc_service = RuneGrpcService {
        relay,
        session_mgr,
    };

    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        Server::builder()
            .add_service(RuneServiceServer::new(grpc_service))
            .serve_with_incoming_shutdown(incoming, async { shutdown_rx.await.ok(); })
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, shutdown_tx)
}

async fn connect_client(addr: SocketAddr) -> RuneServiceClient<Channel> {
    RuneServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap()
}

fn make_rune_decl(name: &str) -> RuneDeclaration {
    RuneDeclaration {
        name: name.into(),
        version: "1.0".into(),
        description: format!("{} rune", name),
        input_schema: String::new(),
        output_schema: String::new(),
        supports_stream: false,
        gate: None,
        priority: 0,
    }
}

fn make_attach_msg_with_key(caster_id: &str, key: &str) -> SessionMessage {
    SessionMessage {
        payload: Some(session_message::Payload::Attach(CasterAttach {
            caster_id: caster_id.into(),
            runes: vec![make_rune_decl("test_rune")],
            labels: Default::default(),
            max_concurrent: 5,
            key: key.into(),
        })),
    }
}

fn make_detach_msg(reason: &str) -> SessionMessage {
    SessionMessage {
        payload: Some(session_message::Payload::Detach(CasterDetach {
            reason: reason.into(),
        })),
    }
}

// ============================================================================
// Auth integration tests
// ============================================================================

/// When auth is enabled and no key is provided, CasterAttach should be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unauthenticated_caster_rejected_when_auth_enabled() {
    let relay = Arc::new(Relay::new());
    let verifier: Arc<dyn KeyVerifier> = Arc::new(StaticKeyVerifier {
        valid_key: "rk_secret_123".into(),
    });
    let session_mgr = Arc::new(SessionManager::with_auth(
        Duration::from_secs(60),
        Duration::from_secs(120),
        verifier,
        false, // auth enabled (not dev mode)
    ));

    let (addr, _shutdown) = start_auth_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);
    let mut response_stream = client.session(Request::new(stream)).await.unwrap().into_inner();

    // Send attach with empty key
    tx.send(make_attach_msg_with_key("caster-no-key", ""))
        .await
        .unwrap();

    // Should receive rejected AttachAck
    let msg = response_stream.message().await.unwrap().unwrap();
    match msg.payload {
        Some(session_message::Payload::AttachAck(ack)) => {
            assert!(!ack.accepted, "attach should be rejected without key");
            assert!(
                ack.reason.contains("invalid"),
                "reason should mention invalid key, got: {}",
                ack.reason
            );
        }
        other => panic!("expected AttachAck, got {:?}", other),
    }

    // Session should NOT be registered
    assert_eq!(session_mgr.caster_count(), 0);

    // Rune should NOT be registered
    assert!(relay.list().is_empty());
}

/// When auth is enabled and an invalid key is provided, CasterAttach should be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_invalid_caster_key_rejected() {
    let relay = Arc::new(Relay::new());
    let verifier: Arc<dyn KeyVerifier> = Arc::new(StaticKeyVerifier {
        valid_key: "rk_secret_123".into(),
    });
    let session_mgr = Arc::new(SessionManager::with_auth(
        Duration::from_secs(60),
        Duration::from_secs(120),
        verifier,
        false,
    ));

    let (addr, _shutdown) = start_auth_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);
    let mut response_stream = client.session(Request::new(stream)).await.unwrap().into_inner();

    // Send attach with wrong key
    tx.send(make_attach_msg_with_key("caster-bad-key", "rk_wrong_key"))
        .await
        .unwrap();

    // Should receive rejected AttachAck
    let msg = response_stream.message().await.unwrap().unwrap();
    match msg.payload {
        Some(session_message::Payload::AttachAck(ack)) => {
            assert!(!ack.accepted, "attach should be rejected with invalid key");
            assert!(
                ack.reason.contains("invalid"),
                "reason should mention invalid key, got: {}",
                ack.reason
            );
        }
        other => panic!("expected AttachAck, got {:?}", other),
    }

    // Session should NOT be registered
    assert_eq!(session_mgr.caster_count(), 0);
}

/// When auth is enabled and a valid key is provided, CasterAttach should succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_valid_caster_key_accepted() {
    let relay = Arc::new(Relay::new());
    let verifier: Arc<dyn KeyVerifier> = Arc::new(StaticKeyVerifier {
        valid_key: "rk_secret_123".into(),
    });
    let session_mgr = Arc::new(SessionManager::with_auth(
        Duration::from_secs(60),
        Duration::from_secs(120),
        verifier,
        false,
    ));

    let (addr, _shutdown) = start_auth_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);
    let mut response_stream = client.session(Request::new(stream)).await.unwrap().into_inner();

    // Send attach with correct key
    tx.send(make_attach_msg_with_key("caster-good-key", "rk_secret_123"))
        .await
        .unwrap();

    // Should receive accepted AttachAck
    let msg = response_stream.message().await.unwrap().unwrap();
    match msg.payload {
        Some(session_message::Payload::AttachAck(ack)) => {
            assert!(ack.accepted, "attach should be accepted with valid key");
            assert!(ack.reason.is_empty());
        }
        other => panic!("expected AttachAck, got {:?}", other),
    }

    // Session should be registered
    assert_eq!(session_mgr.caster_count(), 1);
    assert!(session_mgr.list_caster_ids().contains(&"caster-good-key".to_string()));

    // Rune should be registered
    assert!(relay.list().iter().any(|(name, _)| name == "test_rune"));

    // Cleanup
    tx.send(make_detach_msg("test done")).await.unwrap();
}

/// In dev mode, auth is skipped — even empty key should be accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dev_mode_skips_auth() {
    let relay = Arc::new(Relay::new());
    // Use a strict verifier that rejects everything except a specific key
    let verifier: Arc<dyn KeyVerifier> = Arc::new(StaticKeyVerifier {
        valid_key: "rk_secret_123".into(),
    });
    let session_mgr = Arc::new(SessionManager::with_auth(
        Duration::from_secs(60),
        Duration::from_secs(120),
        verifier,
        true, // dev mode — should skip auth
    ));

    let (addr, _shutdown) = start_auth_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);
    let mut response_stream = client.session(Request::new(stream)).await.unwrap().into_inner();

    // Send attach with NO key — should still succeed in dev mode
    tx.send(make_attach_msg_with_key("caster-dev", ""))
        .await
        .unwrap();

    // Should receive accepted AttachAck
    let msg = response_stream.message().await.unwrap().unwrap();
    match msg.payload {
        Some(session_message::Payload::AttachAck(ack)) => {
            assert!(ack.accepted, "dev mode should accept attach without key");
            assert!(ack.reason.is_empty());
        }
        other => panic!("expected AttachAck, got {:?}", other),
    }

    // Session should be registered
    assert_eq!(session_mgr.caster_count(), 1);

    // Cleanup
    tx.send(make_detach_msg("test done")).await.unwrap();
}
