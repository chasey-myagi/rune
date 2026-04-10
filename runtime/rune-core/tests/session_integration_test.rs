//! Integration tests for `handle_session()` — the 200-line state machine in session.rs.
//!
//! Strategy: spin up a real gRPC RuneService on localhost, connect via generated client,
//! and drive the bidirectional session stream to test every message type.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};
use tonic::Request;

use rune_proto::rune_service_client::RuneServiceClient;
use rune_proto::rune_service_server::RuneServiceServer;
use rune_proto::*;

use rune_core::circuit_breaker::CBState;
use rune_core::config::{AppConfig, CircuitBreakerConfig, RetryConfig};
use rune_core::grpc_service::RuneGrpcService;
use rune_core::relay::Relay;
use rune_core::resolver::RoundRobinResolver;
use rune_core::rune::{RuneContext, RuneError};
use rune_core::session::SessionManager;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Start a gRPC server on an ephemeral port and return (addr, shutdown_sender).
async fn start_server(
    relay: Arc<Relay>,
    session_mgr: Arc<SessionManager>,
) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let grpc_service = RuneGrpcService { relay, session_mgr };

    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        Server::builder()
            .add_service(RuneServiceServer::new(grpc_service))
            .serve_with_incoming_shutdown(incoming, async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // Wait briefly for server to be ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, shutdown_tx)
}

async fn connect_client(addr: SocketAddr) -> RuneServiceClient<Channel> {
    RuneServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap()
}

fn make_attach_msg(
    caster_id: &str,
    runes: Vec<RuneDeclaration>,
    max_concurrent: u32,
) -> SessionMessage {
    make_attach_msg_with_key(caster_id, runes, max_concurrent, "")
}

fn make_attach_msg_with_key(
    caster_id: &str,
    runes: Vec<RuneDeclaration>,
    max_concurrent: u32,
    key: &str,
) -> SessionMessage {
    SessionMessage {
        payload: Some(session_message::Payload::Attach(CasterAttach {
            caster_id: caster_id.into(),
            runes,
            labels: Default::default(),
            max_concurrent,
            key: key.into(),
            role: "caster".into(),
        })),
    }
}

fn make_echo_rune_decl(name: &str) -> RuneDeclaration {
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

fn make_stream_rune_decl(name: &str) -> RuneDeclaration {
    RuneDeclaration {
        name: name.into(),
        version: "1.0".into(),
        description: format!("{} stream rune", name),
        input_schema: String::new(),
        output_schema: String::new(),
        supports_stream: true,
        gate: None,
        priority: 0,
    }
}

fn make_result_msg(request_id: &str, output: &[u8]) -> SessionMessage {
    SessionMessage {
        payload: Some(session_message::Payload::Result(ExecuteResult {
            request_id: request_id.into(),
            status: Status::Completed as i32,
            output: output.to_vec(),
            error: None,
            attachments: Vec::new(),
        })),
    }
}

fn make_stream_event_msg(request_id: &str, data: &[u8]) -> SessionMessage {
    SessionMessage {
        payload: Some(session_message::Payload::StreamEvent(StreamEvent {
            request_id: request_id.into(),
            data: data.to_vec(),
            event_type: String::new(),
        })),
    }
}

fn make_stream_end_msg(request_id: &str) -> SessionMessage {
    SessionMessage {
        payload: Some(session_message::Payload::StreamEnd(StreamEnd {
            request_id: request_id.into(),
            status: Status::Completed as i32,
            error: None,
        })),
    }
}

fn make_heartbeat_msg() -> SessionMessage {
    SessionMessage {
        payload: Some(session_message::Payload::Heartbeat(Heartbeat {
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
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

fn default_session_config() -> AppConfig {
    let mut config = AppConfig::default();
    // Use short intervals for tests
    config.session.heartbeat_interval_secs = 60;
    config.session.heartbeat_timeout_secs = 120;
    config
}

fn fast_heartbeat_config() -> AppConfig {
    let mut config = AppConfig::default();
    // Very short heartbeat for timeout testing
    config.session.heartbeat_interval_secs = 1;
    config.session.heartbeat_timeout_secs = 2;
    config
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test 1: Attach success → AttachAck accepted
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_success_returns_ack_accepted() {
    let config = default_session_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Send attach
    tx.send(make_attach_msg(
        "caster-1",
        vec![make_echo_rune_decl("echo")],
        5,
    ))
    .await
    .unwrap();

    // Read AttachAck
    let msg = response_stream.message().await.unwrap().unwrap();
    match msg.payload {
        Some(session_message::Payload::AttachAck(ack)) => {
            assert!(ack.accepted, "attach should be accepted");
            assert!(ack.reason.is_empty());
        }
        other => panic!("expected AttachAck, got {:?}", other),
    }

    // Session should be registered
    assert_eq!(session_mgr.caster_count(), 1);
    assert!(session_mgr
        .list_caster_ids()
        .contains(&"caster-1".to_string()));

    // Rune should be registered in relay
    let list = relay.list();
    assert!(list.iter().any(|(name, _)| name == "echo"));

    // Cleanup
    tx.send(make_detach_msg("test done")).await.unwrap();
}

/// Test 2: Attach → ExecuteRequest → caster sends Result → caller receives
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_then_execute_unary_returns_result() {
    let config = default_session_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach with echo rune
    tx.send(make_attach_msg(
        "caster-exec",
        vec![make_echo_rune_decl("exec_echo")],
        5,
    ))
    .await
    .unwrap();

    // Wait for AttachAck
    let ack = response_stream.message().await.unwrap().unwrap();
    assert!(matches!(
        ack.payload,
        Some(session_message::Payload::AttachAck(_))
    ));

    // Now invoke the rune through relay — this sends ExecuteRequest to caster
    let session_mgr_clone = Arc::clone(&session_mgr);
    let invoke_handle = tokio::spawn(async move {
        session_mgr_clone
            .execute(
                "caster-exec",
                "req-1",
                "exec_echo",
                Bytes::from("hello"),
                Default::default(),
                Duration::from_secs(10),
            )
            .await
    });

    // Caster receives ExecuteRequest
    let execute_msg = response_stream.message().await.unwrap().unwrap();
    match execute_msg.payload {
        Some(session_message::Payload::Execute(req)) => {
            assert_eq!(req.request_id, "req-1");
            assert_eq!(req.rune_name, "exec_echo");
            assert_eq!(req.input, b"hello");
        }
        other => panic!("expected ExecuteRequest, got {:?}", other),
    }

    // Caster sends back result
    tx.send(make_result_msg("req-1", b"world")).await.unwrap();

    // Caller should receive the result
    let result = invoke_handle.await.unwrap().unwrap();
    assert_eq!(result, Bytes::from("world"));

    tx.send(make_detach_msg("done")).await.unwrap();
}

/// Test 3: Stream ExecuteRequest → caster sends StreamEvents + StreamEnd
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_then_stream_execute_returns_chunks() {
    let config = default_session_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach
    tx.send(make_attach_msg(
        "caster-stream",
        vec![make_stream_rune_decl("streamer")],
        5,
    ))
    .await
    .unwrap();

    let _ = response_stream.message().await.unwrap().unwrap(); // AttachAck

    // Invoke stream
    let session_mgr_clone = Arc::clone(&session_mgr);
    let invoke_handle = tokio::spawn(async move {
        session_mgr_clone
            .execute_stream(
                "caster-stream",
                "req-stream-1",
                "streamer",
                Bytes::from("input"),
                Default::default(),
                Duration::from_secs(10),
            )
            .await
    });

    // Caster receives ExecuteRequest
    let exec_msg = response_stream.message().await.unwrap().unwrap();
    assert!(matches!(
        exec_msg.payload,
        Some(session_message::Payload::Execute(_))
    ));

    // Caster sends stream events
    tx.send(make_stream_event_msg("req-stream-1", b"chunk-1"))
        .await
        .unwrap();
    tx.send(make_stream_event_msg("req-stream-1", b"chunk-2"))
        .await
        .unwrap();
    tx.send(make_stream_end_msg("req-stream-1")).await.unwrap();

    // Caller receives stream chunks
    let mut rx = invoke_handle.await.unwrap().unwrap();
    let c1 = rx.recv().await.unwrap().unwrap();
    assert_eq!(c1, Bytes::from("chunk-1"));
    let c2 = rx.recv().await.unwrap().unwrap();
    assert_eq!(c2, Bytes::from("chunk-2"));
    // Channel should close after StreamEnd
    // (StreamEnd with Completed status doesn't push an error, just removes pending)
    assert!(rx.recv().await.is_none());

    tx.send(make_detach_msg("done")).await.unwrap();
}

/// Test 4: Heartbeat exchange — server sends heartbeat, client responds
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_exchange() {
    let mut config = AppConfig::default();
    config.session.heartbeat_interval_secs = 1;
    config.session.heartbeat_timeout_secs = 60;
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach
    tx.send(make_attach_msg("caster-hb", vec![], 5))
        .await
        .unwrap();
    let _ = response_stream.message().await.unwrap().unwrap(); // AttachAck

    // Wait for heartbeat from server (interval is 1s)
    let hb_msg = tokio::time::timeout(Duration::from_secs(3), response_stream.message())
        .await
        .expect("should receive heartbeat within 3s")
        .unwrap()
        .unwrap();

    match hb_msg.payload {
        Some(session_message::Payload::Heartbeat(hb)) => {
            assert!(hb.timestamp_ms > 0, "heartbeat should have a timestamp");
        }
        other => panic!("expected Heartbeat, got {:?}", other),
    }

    // Send heartbeat response from client
    tx.send(make_heartbeat_msg()).await.unwrap();

    tx.send(make_detach_msg("done")).await.unwrap();
}

/// Test 5: Detach → session cleanup
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detach_cleans_up_session() {
    let config = default_session_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach
    tx.send(make_attach_msg(
        "caster-detach",
        vec![make_echo_rune_decl("detach_rune")],
        5,
    ))
    .await
    .unwrap();
    let _ = response_stream.message().await.unwrap().unwrap(); // AttachAck

    assert_eq!(session_mgr.caster_count(), 1);
    assert!(relay.list().iter().any(|(n, _)| n == "detach_rune"));

    // Send detach
    tx.send(make_detach_msg("graceful shutdown")).await.unwrap();

    // Wait for cleanup
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(session_mgr.caster_count(), 0);
    // Rune should be removed from relay
    assert!(!relay.list().iter().any(|(n, _)| n == "detach_rune"));
}

/// Test 6: Heartbeat timeout → disconnection
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_timeout_disconnects() {
    let config = fast_heartbeat_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach but never respond to heartbeats
    tx.send(make_attach_msg("caster-timeout", vec![], 5))
        .await
        .unwrap();
    let _ = response_stream.message().await.unwrap().unwrap(); // AttachAck

    assert_eq!(session_mgr.caster_count(), 1);

    // Wait for timeout (2s timeout + 1s interval + buffer)
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Session should be cleaned up
    assert_eq!(session_mgr.caster_count(), 0);
}

/// Test 7: CancelRequest → cancels in-progress request
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_request_cancels_pending() {
    let config = default_session_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach
    tx.send(make_attach_msg(
        "caster-cancel",
        vec![make_echo_rune_decl("cancel_rune")],
        5,
    ))
    .await
    .unwrap();
    let _ = response_stream.message().await.unwrap().unwrap(); // AttachAck

    // Start a request that will hang
    let session_mgr_clone = Arc::clone(&session_mgr);
    let invoke_handle = tokio::spawn(async move {
        session_mgr_clone
            .execute(
                "caster-cancel",
                "req-cancel-1",
                "cancel_rune",
                Bytes::from("data"),
                Default::default(),
                Duration::from_secs(30),
            )
            .await
    });

    // Wait for execute request to arrive
    let exec_msg = response_stream.message().await.unwrap().unwrap();
    assert!(matches!(
        exec_msg.payload,
        Some(session_message::Payload::Execute(_))
    ));

    // Cancel the request
    session_mgr
        .cancel("caster-cancel", "req-cancel-1", "test cancel")
        .await
        .unwrap();

    // Caster should receive CancelRequest
    let cancel_msg = response_stream.message().await.unwrap().unwrap();
    match cancel_msg.payload {
        Some(session_message::Payload::Cancel(c)) => {
            assert_eq!(c.request_id, "req-cancel-1");
            assert_eq!(c.reason, "test cancel");
        }
        other => panic!("expected CancelRequest, got {:?}", other),
    }

    // The invoke should fail with Cancelled
    let result = invoke_handle.await.unwrap();
    assert!(
        matches!(result, Err(RuneError::Cancelled)),
        "expected Cancelled, got {:?}",
        result
    );

    tx.send(make_detach_msg("done")).await.unwrap();
}

/// Test 8: Connection drop (tx dropped) → session cleanup
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_drop_cleans_up_session() {
    let config = default_session_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach
    tx.send(make_attach_msg(
        "caster-drop",
        vec![make_echo_rune_decl("drop_rune")],
        5,
    ))
    .await
    .unwrap();
    let _ = response_stream.message().await.unwrap().unwrap(); // AttachAck

    assert_eq!(session_mgr.caster_count(), 1);

    // Drop the sender to simulate connection drop
    drop(tx);
    drop(response_stream);

    // Wait for cleanup
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(session_mgr.caster_count(), 0);
    assert!(!relay.list().iter().any(|(n, _)| n == "drop_rune"));
}

/// Test 9: Disconnect with pending requests → all pending get error
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_with_pending_requests_sends_errors() {
    let config = default_session_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach
    tx.send(make_attach_msg(
        "caster-pending",
        vec![make_echo_rune_decl("pending_rune")],
        5,
    ))
    .await
    .unwrap();
    let _ = response_stream.message().await.unwrap().unwrap(); // AttachAck

    // Start a request
    let session_mgr_clone = Arc::clone(&session_mgr);
    let invoke_handle = tokio::spawn(async move {
        session_mgr_clone
            .execute(
                "caster-pending",
                "req-pending-1",
                "pending_rune",
                Bytes::new(),
                Default::default(),
                Duration::from_secs(30),
            )
            .await
    });

    // Wait for execute to arrive
    let _ = response_stream.message().await.unwrap().unwrap();

    // Drop connection
    drop(tx);
    drop(response_stream);

    // Pending request should receive an error
    let result = tokio::time::timeout(Duration::from_secs(3), invoke_handle)
        .await
        .expect("invoke should complete")
        .unwrap();

    assert!(
        result.is_err(),
        "pending request should error on disconnect"
    );
}

/// Test 10: Multiple rune registration via single attach
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_registers_multiple_runes() {
    let config = default_session_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach with multiple runes
    tx.send(make_attach_msg(
        "caster-multi",
        vec![
            make_echo_rune_decl("rune_a"),
            make_echo_rune_decl("rune_b"),
            make_stream_rune_decl("rune_c"),
        ],
        10,
    ))
    .await
    .unwrap();

    let _ = response_stream.message().await.unwrap().unwrap(); // AttachAck

    // All runes should be registered
    let list = relay.list();
    let names: Vec<&str> = list.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"rune_a"));
    assert!(names.contains(&"rune_b"));
    assert!(names.contains(&"rune_c"));
    assert_eq!(list.len(), 3);

    tx.send(make_detach_msg("done")).await.unwrap();
}

// ---------------------------------------------------------------------------
// Retry + Circuit Breaker integration test (real remote invoker path)
// ---------------------------------------------------------------------------

/// Integration test: consecutive timeouts through real remote invoker path
/// open the circuit breaker, but timeout itself is never retried.
///
/// Verifies the full chain: Relay::resolve → RetryInvoker → RemoteInvoker →
/// SessionManager → gRPC → (Caster never responds) → Timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fix_timeout_trips_cb_but_never_retries_integration() {
    let retry_config = RetryConfig {
        enabled: true,
        max_retries: 3,
        base_delay_ms: 10,
        max_delay_ms: 50,
        backoff_multiplier: 1.0,
        retryable_errors: vec!["unavailable".into(), "internal".into()],
        circuit_breaker: CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 2,
            success_threshold: 1,
            reset_timeout_ms: 60_000,
            half_open_max_permits: 1,
        },
    };

    let config = default_session_config();
    let relay = Arc::new(Relay::with_retry(retry_config));
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;
    let mut client = connect_client(addr).await;

    let (tx, rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(rx);

    let mut response_stream = client
        .session(Request::new(stream))
        .await
        .unwrap()
        .into_inner();

    // Attach a caster that declares a rune but will NEVER respond to ExecuteRequest.
    tx.send(make_attach_msg(
        "caster-slow",
        vec![make_echo_rune_decl("slow_rune")],
        5,
    ))
    .await
    .unwrap();

    let ack = response_stream.message().await.unwrap().unwrap();
    assert!(matches!(
        ack.payload,
        Some(session_message::Payload::AttachAck(_))
    ));

    let resolver = RoundRobinResolver::new();

    // --- Call 1: timeout (not retried), CB failure count → 1 ---
    let invoker = relay
        .resolve("slow_rune", &resolver)
        .expect("rune should be registered");
    let ctx1 = RuneContext {
        rune_name: "slow_rune".into(),
        request_id: "timeout-1".into(),
        context: Default::default(),
        timeout: Duration::from_millis(200),
    };
    let result1 = invoker.invoke_once(ctx1, Bytes::from("ping")).await;
    assert!(
        matches!(result1, Err(RuneError::Timeout)),
        "call 1 should timeout, got {:?}",
        result1
    );

    // Drain the ExecuteRequest the caster received (we never respond).
    let exec_msg1 = tokio::time::timeout(Duration::from_secs(2), response_stream.message())
        .await
        .expect("should receive execute request 1")
        .unwrap()
        .unwrap();
    assert!(matches!(
        exec_msg1.payload,
        Some(session_message::Payload::Execute(_))
    ));

    // CB still Closed (1 < threshold 2)
    let states = relay.circuit_breaker_states();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].state, CBState::Closed);

    // --- Call 2: another timeout → CB failure count → 2 → Opens ---
    let invoker = relay.resolve("slow_rune", &resolver).unwrap();
    let ctx2 = RuneContext {
        rune_name: "slow_rune".into(),
        request_id: "timeout-2".into(),
        context: Default::default(),
        timeout: Duration::from_millis(200),
    };
    let result2 = invoker.invoke_once(ctx2, Bytes::from("ping")).await;
    assert!(matches!(result2, Err(RuneError::Timeout)));

    // Drain ExecuteRequest 2
    let exec_msg2 = tokio::time::timeout(Duration::from_secs(2), response_stream.message())
        .await
        .expect("should receive execute request 2")
        .unwrap()
        .unwrap();
    assert!(matches!(
        exec_msg2.payload,
        Some(session_message::Payload::Execute(_))
    ));

    // CB should now be Open
    let states = relay.circuit_breaker_states();
    assert_eq!(
        states[0].state,
        CBState::Open,
        "CB must be Open after 2 consecutive timeouts"
    );

    // --- Call 3: CB open → fast-fail Unavailable, no ExecuteRequest sent ---
    let invoker = relay.resolve("slow_rune", &resolver).unwrap();
    let ctx3 = RuneContext {
        rune_name: "slow_rune".into(),
        request_id: "fast-fail-3".into(),
        context: Default::default(),
        timeout: Duration::from_secs(5),
    };
    let start = std::time::Instant::now();
    let result3 = invoker.invoke_once(ctx3, Bytes::from("ping")).await;
    let elapsed = start.elapsed();

    assert!(
        matches!(result3, Err(RuneError::CircuitOpen { .. })),
        "call 3 should fast-fail, got {:?}",
        result3
    );
    assert!(
        elapsed < Duration::from_millis(100),
        "fast-fail should be instant, took {:?}",
        elapsed
    );

    // Verify NO ExecuteRequest reached the caster for call 3.
    let maybe_msg =
        tokio::time::timeout(Duration::from_millis(100), response_stream.message()).await;
    assert!(
        maybe_msg.is_err(),
        "no ExecuteRequest should reach caster when CB is open"
    );

    tx.send(make_detach_msg("done")).await.unwrap();
}

/// Regression P0-1: cleanup_session of old session must NOT delete new session's data.
///
/// Scenario: caster-A connects → session 1 active → caster-A reconnects (session 2)
/// → session 1 loop breaks and cleanup runs. Before the fix, cleanup_session
/// unconditionally removed all data for the caster_id, deleting session 2's entries.
#[tokio::test]
async fn test_fix_reconnect_cleanup_does_not_delete_new_session() {
    let config = default_session_config();
    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new_dev(
        config.heartbeat_interval(),
        config.heartbeat_timeout(),
    ));

    let (addr, _shutdown) = start_server(Arc::clone(&relay), Arc::clone(&session_mgr)).await;

    // Session 1: connect and attach
    let mut client1 = connect_client(addr).await;
    let (tx1, rx1) = mpsc::channel(32);
    let stream1 = ReceiverStream::new(rx1);
    let mut resp1 = client1
        .session(Request::new(stream1))
        .await
        .unwrap()
        .into_inner();

    tx1.send(make_attach_msg(
        "reconnect-caster",
        vec![make_echo_rune_decl("echo")],
        2,
    ))
    .await
    .unwrap();
    let ack1 = resp1.message().await.unwrap().unwrap();
    assert!(matches!(ack1.payload, Some(session_message::Payload::AttachAck(ref a)) if a.accepted));
    assert_eq!(session_mgr.caster_count(), 1);

    // Session 2: connect and attach with SAME caster_id (reconnect)
    let mut client2 = connect_client(addr).await;
    let (tx2, rx2) = mpsc::channel(32);
    let stream2 = ReceiverStream::new(rx2);
    let mut resp2 = client2
        .session(Request::new(stream2))
        .await
        .unwrap()
        .into_inner();

    tx2.send(make_attach_msg(
        "reconnect-caster",
        vec![make_echo_rune_decl("echo")],
        4,
    ))
    .await
    .unwrap();
    let ack2 = resp2.message().await.unwrap().unwrap();
    assert!(matches!(ack2.payload, Some(session_message::Payload::AttachAck(ref a)) if a.accepted));

    // Session 2 is now the active session. max_concurrent should be 4.
    assert_eq!(session_mgr.max_concurrent("reconnect-caster"), 4);

    // Now disconnect session 1 (triggers cleanup of old session)
    tx1.send(make_detach_msg("reconnect")).await.unwrap();
    // Wait for cleanup to complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Session 2's data must still be intact
    assert_eq!(
        session_mgr.caster_count(),
        1,
        "new session must survive old session's cleanup"
    );
    assert_eq!(
        session_mgr.max_concurrent("reconnect-caster"),
        4,
        "new session's metadata must not be deleted by old cleanup"
    );
    assert!(
        session_mgr.connected_at("reconnect-caster").is_some(),
        "new session entry must still exist"
    );
    // Relay entries from session 2 must survive old cleanup
    assert!(
        relay.find("echo").is_some(),
        "new session's relay entries must not be wiped by old cleanup"
    );

    // Clean up session 2
    tx2.send(make_detach_msg("done")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        session_mgr.caster_count(),
        0,
        "session 2 cleanup should work normally"
    );
    // After session 2 disconnects normally, relay should be clean
    assert!(
        relay.find("echo").is_none(),
        "relay should be clean after final session disconnects"
    );
}
