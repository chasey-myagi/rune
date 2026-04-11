//! Cross-module integration tests: App register → Relay → resolve → invoke full chain.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use rune_core::app::App;
use rune_core::invoker::LocalInvoker;
use rune_core::relay::Relay;
use rune_core::resolver::RoundRobinResolver;
use rune_core::rune::{
    make_handler, RuneConfig, RuneContext, RuneError, StreamRuneHandler, StreamSender,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn echo_config(name: &str) -> RuneConfig {
    RuneConfig {
        name: name.into(),
        version: "1.0".into(),
        description: format!("{} rune", name),
        supports_stream: false,
        gate: None,
        input_schema: None,
        output_schema: None,
        priority: 0,
        labels: Default::default(),
    }
}

fn stream_config(name: &str) -> RuneConfig {
    RuneConfig {
        supports_stream: true,
        ..echo_config(name)
    }
}

fn test_ctx(rune_name: &str, request_id: &str) -> RuneContext {
    RuneContext {
        rune_name: rune_name.into(),
        request_id: request_id.into(),
        context: Default::default(),
        timeout: Duration::from_secs(30),
    }
}

struct CountingStreamHandler {
    count: usize,
}

#[async_trait::async_trait]
impl StreamRuneHandler for CountingStreamHandler {
    async fn execute(
        &self,
        _ctx: RuneContext,
        _input: Bytes,
        tx: StreamSender,
    ) -> Result<(), RuneError> {
        for i in 0..self.count {
            tx.emit(Bytes::from(format!("chunk-{}", i))).await?;
        }
        tx.end().await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test 1: App register echo rune → relay.resolve → invoker.invoke_once → correct result
#[tokio::test]
async fn app_register_echo_rune_full_chain() {
    let mut app = App::new();
    let handler = make_handler(|_ctx, input| async move {
        let mut out = b"echo:".to_vec();
        out.extend_from_slice(&input);
        Ok(Bytes::from(out))
    });
    app.rune(echo_config("echo"), handler);

    let resolver = RoundRobinResolver::new();
    let invoker = app
        .relay
        .resolve("echo", &resolver)
        .expect("echo rune should be resolvable");

    let result = invoker
        .invoke_once(test_ctx("echo", "r-1"), Bytes::from("hello"))
        .await
        .unwrap();

    assert_eq!(result, Bytes::from("echo:hello"));
}

/// Test 2: App register stream rune → invoke_stream → receive multiple chunks
#[tokio::test]
async fn app_register_stream_rune_invoke_stream() {
    let mut app = App::new();
    app.stream_rune(
        stream_config("streamer"),
        CountingStreamHandler { count: 3 },
    );

    let resolver = RoundRobinResolver::new();
    let invoker = app
        .relay
        .resolve("streamer", &resolver)
        .expect("stream rune should be resolvable");

    let mut rx = invoker
        .invoke_stream(test_ctx("streamer", "r-s1"), Bytes::new())
        .await
        .unwrap();

    let c0 = rx.recv().await.unwrap().unwrap();
    assert_eq!(c0, Bytes::from("chunk-0"));
    let c1 = rx.recv().await.unwrap().unwrap();
    assert_eq!(c1, Bytes::from("chunk-1"));
    let c2 = rx.recv().await.unwrap().unwrap();
    assert_eq!(c2, Bytes::from("chunk-2"));
    assert!(rx.recv().await.is_none());
}

/// Test 3: Multiple rune registration → round-robin verification
#[tokio::test]
async fn multiple_rune_round_robin_selection() {
    let mut app = App::new();
    let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("v1")) });
    let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("v2")) });

    app.rune(echo_config("rr"), h1);
    app.rune(echo_config("rr"), h2);

    let resolver = RoundRobinResolver::new();

    // Round-robin should alternate
    let r1 = app
        .relay
        .resolve("rr", &resolver)
        .unwrap()
        .invoke_once(test_ctx("rr", "r1"), Bytes::new())
        .await
        .unwrap();
    let r2 = app
        .relay
        .resolve("rr", &resolver)
        .unwrap()
        .invoke_once(test_ctx("rr", "r2"), Bytes::new())
        .await
        .unwrap();

    assert_ne!(r1, r2, "round-robin should alternate between v1 and v2");

    // Third call should wrap around
    let r3 = app
        .relay
        .resolve("rr", &resolver)
        .unwrap()
        .invoke_once(test_ctx("rr", "r3"), Bytes::new())
        .await
        .unwrap();
    assert_eq!(r1, r3, "round-robin should wrap around");
}

/// Test 4: Session disconnect → relay remove_caster integration
#[tokio::test]
async fn session_disconnect_removes_caster_from_relay() {
    let relay = Arc::new(Relay::new());
    let resolver = RoundRobinResolver::new();

    // Manually register a rune with a caster_id to simulate remote registration
    let handler = make_handler(|_ctx, input| async move { Ok(input) });
    relay
        .register(
            echo_config("remote_echo"),
            Arc::new(LocalInvoker::new(handler)),
            Some("caster-99".to_string()),
        )
        .unwrap();

    // Rune should be resolvable
    assert!(relay.resolve("remote_echo", &resolver).is_some());

    // Simulate disconnect: remove_caster
    relay.remove_caster("caster-99");

    // Rune should no longer be resolvable
    assert!(relay.resolve("remote_echo", &resolver).is_none());
    assert!(relay.list().is_empty());
}

/// Test 5: Full chain with context propagation
#[tokio::test]
async fn full_chain_with_context_propagation() {
    let mut app = App::new();
    let handler = make_handler(|ctx, input| async move {
        let user = ctx.context.get("user").cloned().unwrap_or_default();
        let output = format!(
            "rune={} user={} input={}",
            ctx.rune_name,
            user,
            String::from_utf8_lossy(&input)
        );
        Ok(Bytes::from(output))
    });
    app.rune(echo_config("ctx_rune"), handler);

    let resolver = RoundRobinResolver::new();
    let invoker = app.relay.resolve("ctx_rune", &resolver).unwrap();

    let ctx = RuneContext {
        rune_name: "ctx_rune".into(),
        request_id: "r-ctx".into(),
        context: [("user".to_string(), "alice".to_string())]
            .into_iter()
            .collect(),
        timeout: Duration::from_secs(10),
    };

    let result = invoker.invoke_once(ctx, Bytes::from("data")).await.unwrap();
    assert_eq!(result, Bytes::from("rune=ctx_rune user=alice input=data"));
}

/// Test 6: Build → components still functional
#[tokio::test]
async fn app_build_returns_functional_components() {
    let mut app = App::new();
    app.rune(
        echo_config("built"),
        make_handler(|_ctx, input| async move { Ok(input) }),
    );
    let running = app.build();

    let resolver = RoundRobinResolver::new();
    let invoker = running.relay.resolve("built", &resolver).unwrap();
    let result = invoker
        .invoke_once(test_ctx("built", "r-build"), Bytes::from("test"))
        .await
        .unwrap();
    assert_eq!(result, Bytes::from("test"));
}

// ---------------------------------------------------------------------------
// Relay concurrent safety tests
// ---------------------------------------------------------------------------

/// Concurrent test 1: 10 tasks simultaneously register different runes → all succeed
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_concurrent_register_different_runes() {
    let relay = Arc::new(Relay::new());
    let mut handles = Vec::new();

    for i in 0..10 {
        let relay = Arc::clone(&relay);
        handles.push(tokio::spawn(async move {
            let handler = make_handler(move |_ctx, _input| async move {
                Ok(Bytes::from(format!("handler-{}", i)))
            });
            let config = echo_config(&format!("concurrent_rune_{}", i));
            relay
                .register(config, Arc::new(LocalInvoker::new(handler)), None)
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let list = relay.list();
    assert_eq!(list.len(), 10, "all 10 runes should be registered");
    for i in 0..10 {
        let name = format!("concurrent_rune_{}", i);
        assert!(
            list.iter().any(|(n, _)| n == &name),
            "rune {} should be in the list",
            name
        );
    }
}

/// Concurrent test 2: Simultaneous register and remove → final state is consistent
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_concurrent_register_and_remove() {
    let relay = Arc::new(Relay::new());

    // Pre-register some runes with caster IDs
    for i in 0..5 {
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                echo_config(&format!("rune_{}", i)),
                Arc::new(LocalInvoker::new(handler)),
                Some(format!("caster-{}", i)),
            )
            .unwrap();
    }

    let mut handles = Vec::new();

    // Remove casters 0-4 concurrently
    for i in 0..5 {
        let relay = Arc::clone(&relay);
        handles.push(tokio::spawn(async move {
            relay.remove_caster(&format!("caster-{}", i));
        }));
    }

    // Register new runes 5-9 concurrently
    for i in 5..10 {
        let relay = Arc::clone(&relay);
        handles.push(tokio::spawn(async move {
            let handler = make_handler(|_ctx, input| async move { Ok(input) });
            relay
                .register(
                    echo_config(&format!("rune_{}", i)),
                    Arc::new(LocalInvoker::new(handler)),
                    None,
                )
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Old runes (caster-based) should be gone, new runes should be present
    let list = relay.list();
    let names: Vec<&str> = list.iter().map(|(n, _)| n.as_str()).collect();

    // All old caster runes should be removed
    for i in 0..5 {
        assert!(
            !names.contains(&format!("rune_{}", i).as_str()),
            "rune_{} should have been removed",
            i
        );
    }

    // All new runes should be present
    for i in 5..10 {
        assert!(
            names.contains(&format!("rune_{}", i).as_str()),
            "rune_{} should be registered",
            i
        );
    }
}

/// Concurrent test 3: Simultaneous resolve and remove → no panic
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_concurrent_resolve_and_remove_no_panic() {
    let relay = Arc::new(Relay::new());
    let resolver = Arc::new(RoundRobinResolver::new());

    // Register runes with caster IDs
    for i in 0..10 {
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        relay
            .register(
                echo_config(&format!("race_rune_{}", i)),
                Arc::new(LocalInvoker::new(handler)),
                Some(format!("race-caster-{}", i)),
            )
            .unwrap();
    }

    let mut handles = Vec::new();

    // Resolve in parallel
    for i in 0..10 {
        let relay = Arc::clone(&relay);
        let resolver = Arc::clone(&resolver);
        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let _ = relay.resolve(&format!("race_rune_{}", i), resolver.as_ref());
                tokio::task::yield_now().await;
            }
        }));
    }

    // Remove casters in parallel
    for i in 0..10 {
        let relay = Arc::clone(&relay);
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            relay.remove_caster(&format!("race-caster-{}", i));
        }));
    }

    // All tasks should complete without panic
    for h in handles {
        h.await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Regression: PR #18 code review fixes
// ---------------------------------------------------------------------------

/// Fix 4: App::new() must wire up retry/circuit-breaker via build_relay(),
/// not bypass it with bare Relay::new(). RetryConfig::default().enabled is
/// true, so the default constructor should produce a Relay with a
/// CircuitBreakerRegistry.
#[test]
fn test_fix_app_new_has_retry_enabled() {
    let app = App::new();
    assert!(
        app.relay.circuit_breaker_registry().is_some(),
        "App::new() must wire retry/circuit-breaker — got None, \
         which means Relay::new() was used instead of build_relay()"
    );
}
