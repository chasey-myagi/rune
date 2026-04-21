use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// Relay benchmarks
// ============================================================================

fn relay_benchmarks(c: &mut Criterion) {
    use rune_core::invoker::LocalInvoker;
    use rune_core::relay::Relay;
    use rune_core::resolver::RoundRobinResolver;
    use rune_core::rune::{make_handler, GateConfig, RuneConfig};

    let mut group = c.benchmark_group("relay");

    // --- relay_register ---
    group.bench_function("register", |b| {
        b.iter(|| {
            let relay = Relay::new();
            let handler = make_handler(|_ctx, input| async move { Ok(input) });
            let config = RuneConfig {
                name: "bench_rune".into(),
                version: "1.0.0".into(),
                description: "benchmark rune".into(),
                supports_stream: false,
                gate: Some(GateConfig {
                    path: "/bench".into(),
                    method: "POST".into(),
                }),
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels: Default::default(),
            };
            relay
                .register(config, Arc::new(LocalInvoker::new(handler)), None)
                .unwrap();
        });
    });

    // --- relay_resolve with varying candidate counts ---
    for count in [1, 10, 100] {
        group.bench_with_input(BenchmarkId::new("resolve", count), &count, |b, &count| {
            let relay = Relay::new();
            let resolver = RoundRobinResolver::new();
            for i in 0..count {
                let handler = make_handler(|_ctx, input| async move { Ok(input) });
                let config = RuneConfig {
                    name: "bench_rune".into(),
                    version: "1.0.0".into(),
                    description: format!("handler {}", i),
                    supports_stream: false,
                    gate: None,
                    input_schema: None,
                    output_schema: None,
                    priority: 0,
                    labels: Default::default(),
                };
                relay
                    .register(
                        config,
                        Arc::new(LocalInvoker::new(handler)),
                        Some(format!("caster-{}", i)),
                    )
                    .unwrap();
            }
            b.iter(|| {
                relay.resolve("bench_rune", &resolver);
            });
        });
    }

    // --- relay_resolve_with_labels ---
    group.bench_function("resolve_with_labels", |b| {
        let relay = Relay::new();
        let resolver = RoundRobinResolver::new();
        for i in 0..10 {
            let handler = make_handler(|_ctx, input| async move { Ok(input) });
            let mut labels = HashMap::new();
            labels.insert(
                "region".to_string(),
                if i % 2 == 0 {
                    "us".to_string()
                } else {
                    "eu".to_string()
                },
            );
            labels.insert("tier".to_string(), format!("t{}", i % 3));
            let config = RuneConfig {
                name: "labeled_rune".into(),
                version: "1.0.0".into(),
                description: format!("handler {}", i),
                supports_stream: false,
                gate: None,
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels,
            };
            relay
                .register(
                    config,
                    Arc::new(LocalInvoker::new(handler)),
                    Some(format!("caster-{}", i)),
                )
                .unwrap();
        }
        let mut required = HashMap::new();
        required.insert("region".to_string(), "us".to_string());
        b.iter(|| {
            relay.resolve_with_labels("labeled_rune", &required, &resolver);
        });
    });

    // --- relay_list with 100 runes ---
    group.bench_function("list_100", |b| {
        let relay = Relay::new();
        for i in 0..100 {
            let handler = make_handler(|_ctx, input| async move { Ok(input) });
            let config = RuneConfig {
                name: format!("rune_{}", i),
                version: "1.0.0".into(),
                description: format!("rune {}", i),
                supports_stream: false,
                gate: Some(GateConfig {
                    path: format!("/rune/{}", i),
                    method: "POST".into(),
                }),
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels: Default::default(),
            };
            relay
                .register(config, Arc::new(LocalInvoker::new(handler)), None)
                .unwrap();
        }
        b.iter(|| {
            relay.list();
        });
    });

    // --- relay_remove_caster with varying total caster counts ---
    // Verifies that remove_caster for a single-rune caster is ~constant
    // regardless of how many other casters are registered (O(R_this_caster)
    // rather than O(R_total * C_max)).
    for count in [100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("remove_caster", count),
            &count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let relay = Relay::new();
                        for i in 0..count {
                            let handler = make_handler(|_ctx, input| async move { Ok(input) });
                            let config = RuneConfig {
                                name: "bench_rune".into(),
                                version: "1.0.0".into(),
                                description: format!("handler {}", i),
                                supports_stream: false,
                                gate: None,
                                input_schema: None,
                                output_schema: None,
                                priority: 0,
                                labels: Default::default(),
                            };
                            relay
                                .register(
                                    config,
                                    Arc::new(LocalInvoker::new(handler)),
                                    Some(format!("caster-{}", i)),
                                )
                                .unwrap();
                        }
                        relay
                    },
                    |relay| relay.remove_caster("caster-0"),
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Invoker benchmarks
// ============================================================================

fn invoker_benchmarks(c: &mut Criterion) {
    use rune_core::invoker::{LocalInvoker, LocalStreamInvoker, RuneInvoker};
    use rune_core::rune::{make_handler, RuneContext, RuneError, StreamRuneHandler, StreamSender};

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("invoker");

    // --- local_invoke_once ---
    group.bench_function("local_invoke_once", |b| {
        let handler = make_handler(|_ctx, input| async move {
            let mut out = b"echo:".to_vec();
            out.extend_from_slice(&input);
            Ok(Bytes::from(out))
        });
        let invoker = LocalInvoker::new(handler);
        b.iter(|| {
            let ctx = RuneContext {
                rune_name: "echo".into(),
                request_id: "bench-1".into(),
                context: Default::default(),
                timeout: Duration::from_secs(30),
            };
            rt.block_on(invoker.invoke_once(ctx, Bytes::from("hello")))
                .unwrap();
        });
    });

    // --- local_invoke_stream (10 chunks) ---
    struct TenChunkHandler;

    #[async_trait::async_trait]
    impl StreamRuneHandler for TenChunkHandler {
        async fn execute(
            &self,
            _ctx: RuneContext,
            _input: Bytes,
            tx: StreamSender,
        ) -> Result<(), RuneError> {
            for i in 0..10 {
                tx.emit(Bytes::from(format!("chunk-{}", i))).await?;
            }
            tx.end().await?;
            Ok(())
        }
    }

    group.bench_function("local_invoke_stream_10chunks", |b| {
        let invoker = LocalStreamInvoker::new(Arc::new(TenChunkHandler));
        b.iter(|| {
            let ctx = RuneContext {
                rune_name: "stream".into(),
                request_id: "bench-s".into(),
                context: Default::default(),
                timeout: Duration::from_secs(30),
            };
            rt.block_on(async {
                let mut rx = invoker
                    .invoke_stream(ctx, Bytes::from("data"))
                    .await
                    .unwrap();
                while let Some(chunk) = rx.recv().await {
                    let _ = chunk.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ============================================================================
// DAG benchmarks
// ============================================================================

fn dag_benchmarks(c: &mut Criterion) {
    use rune_core::invoker::LocalInvoker;
    use rune_core::relay::Relay;
    use rune_core::resolver::RoundRobinResolver;
    use rune_core::rune::{make_handler, RuneConfig};
    use rune_flow::dag::{topological_layers, validate_dag, FlowDefinition, StepDefinition};
    use rune_flow::engine::FlowEngine;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("dag");

    // --- dag_validate (10 steps) ---
    let flow_10 = FlowDefinition {
        name: "bench_flow".into(),
        steps: (0..10)
            .map(|i| StepDefinition {
                name: format!("step_{}", i),
                rune: format!("rune_{}", i),
                depends_on: if i == 0 {
                    vec![]
                } else {
                    vec![format!("step_{}", i - 1)]
                },
                condition: None,
                input_mapping: None,
                timeout_ms: None,
            })
            .collect(),
        gate_path: None,
    };

    group.bench_function("validate_10_steps", |b| {
        b.iter(|| {
            validate_dag(&flow_10).unwrap();
        });
    });

    // --- dag_topological_layers ---
    group.bench_function("topological_layers", |b| {
        b.iter(|| {
            topological_layers(&flow_10).unwrap();
        });
    });

    // --- flow_execute_linear_3 ---
    group.bench_function("flow_execute_linear_3", |b| {
        let relay = Arc::new(Relay::new());
        let resolver: Arc<dyn rune_core::resolver::Resolver> = Arc::new(RoundRobinResolver::new());

        // Register 3 echo runes
        for i in 0..3 {
            let handler = make_handler(|_ctx, input| async move { Ok(input) });
            let config = RuneConfig {
                name: format!("step_{}", i),
                version: "1.0.0".into(),
                description: format!("step {}", i),
                supports_stream: false,
                gate: None,
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels: Default::default(),
            };
            relay
                .register(config, Arc::new(LocalInvoker::new(handler)), None)
                .unwrap();
        }

        let mut engine = FlowEngine::new(Arc::clone(&relay), Arc::clone(&resolver));
        let flow = FlowDefinition {
            name: "linear_3".into(),
            steps: (0..3)
                .map(|i| StepDefinition {
                    name: format!("step_{}", i),
                    rune: format!("step_{}", i),
                    depends_on: if i == 0 {
                        vec![]
                    } else {
                        vec![format!("step_{}", i - 1)]
                    },
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                })
                .collect(),
            gate_path: None,
        };
        engine.register(flow).unwrap();

        b.iter(|| {
            rt.block_on(engine.execute("linear_3", Bytes::from(r#"{"msg":"hello"}"#)))
                .unwrap();
        });
    });

    // --- flow_execute_diamond ---
    group.bench_function("flow_execute_diamond", |b| {
        let relay = Arc::new(Relay::new());
        let resolver: Arc<dyn rune_core::resolver::Resolver> = Arc::new(RoundRobinResolver::new());

        for name in &["start", "left", "right", "join"] {
            let handler = make_handler(|_ctx, input| async move { Ok(input) });
            let config = RuneConfig {
                name: name.to_string(),
                version: "1.0.0".into(),
                description: name.to_string(),
                supports_stream: false,
                gate: None,
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels: Default::default(),
            };
            relay
                .register(config, Arc::new(LocalInvoker::new(handler)), None)
                .unwrap();
        }

        let mut engine = FlowEngine::new(Arc::clone(&relay), Arc::clone(&resolver));
        let flow = FlowDefinition {
            name: "diamond".into(),
            steps: vec![
                StepDefinition {
                    name: "start".into(),
                    rune: "start".into(),
                    depends_on: vec![],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                },
                StepDefinition {
                    name: "left".into(),
                    rune: "left".into(),
                    depends_on: vec!["start".into()],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                },
                StepDefinition {
                    name: "right".into(),
                    rune: "right".into(),
                    depends_on: vec!["start".into()],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                },
                StepDefinition {
                    name: "join".into(),
                    rune: "join".into(),
                    depends_on: vec!["left".into(), "right".into()],
                    condition: None,
                    input_mapping: Some({
                        let mut m = HashMap::new();
                        m.insert("left".to_string(), "left.output".to_string());
                        m.insert("right".to_string(), "right.output".to_string());
                        m
                    }),
                    timeout_ms: None,
                },
            ],
            gate_path: None,
        };
        engine.register(flow).unwrap();

        b.iter(|| {
            rt.block_on(engine.execute("diamond", Bytes::from(r#"{"msg":"test"}"#)))
                .unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Schema benchmarks
// ============================================================================

fn schema_benchmarks(c: &mut Criterion) {
    use rune_schema::openapi::{generate_openapi, RuneInfo};
    use rune_schema::validator::{clear_validator_cache, validate_input};

    let mut group = c.benchmark_group("schema");

    let schema = r#"{
        "type": "object",
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "integer", "minimum": 0 },
            "email": { "type": "string", "format": "email" }
        },
        "required": ["name", "age"]
    }"#;

    let valid_input = r#"{"name": "Alice", "age": 30, "email": "alice@example.com"}"#;
    let invalid_input = r#"{"name": 123, "age": -5}"#;

    // --- schema_validate_input (valid, includes cache warmup) ---
    group.bench_function("validate_input_valid", |b| {
        // Pre-warm the cache so benchmark measures cached path
        validate_input(Some(schema), valid_input.as_bytes()).unwrap();
        b.iter(|| {
            validate_input(Some(schema), valid_input.as_bytes()).unwrap();
        });
    });

    // --- schema_validate_input (cold, no cache) ---
    group.bench_function("validate_input_cold", |b| {
        b.iter(|| {
            clear_validator_cache();
            validate_input(Some(schema), valid_input.as_bytes()).unwrap();
        });
    });

    // --- schema_validate_invalid ---
    group.bench_function("validate_input_invalid", |b| {
        b.iter(|| {
            let _ = validate_input(Some(schema), invalid_input.as_bytes());
        });
    });

    // --- openapi_generate (10 runes) ---
    group.bench_function("openapi_generate_10", |b| {
        let runes: Vec<RuneInfo> = (0..10)
            .map(|i| RuneInfo {
                name: format!("rune_{}", i),
                gate_path: Some(format!("/api/rune/{}", i)),
                gate_method: "POST".to_string(),
                input_schema: Some(schema.to_string()),
                output_schema: Some(
                    r#"{"type":"object","properties":{"result":{"type":"string"}}}"#.to_string(),
                ),
                description: format!("Rune {} description", i),
            })
            .collect();
        b.iter(|| {
            generate_openapi(&runes);
        });
    });

    group.finish();
}

// ============================================================================
// Store benchmarks
// ============================================================================

fn store_benchmarks(c: &mut Criterion) {
    use rune_store::{CallLog, RuneStore};

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("store");

    // --- store_insert_log ---
    group.bench_function("insert_log", |b| {
        let store = RuneStore::open_in_memory().unwrap();
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            let log = CallLog {
                id: 0,
                request_id: format!("req-{}", counter),
                rune_name: "echo".into(),
                mode: "sync".into(),
                caster_id: None,
                latency_ms: 42,
                status_code: 200,
                input_size: 100,
                output_size: 50,
                timestamp: "2026-03-23T00:00:00Z".into(),
            };
            rt.block_on(store.insert_log(&log)).unwrap();
        });
    });

    // --- store_query_logs (50 entries) ---
    group.bench_function("query_logs_50", |b| {
        let store = RuneStore::open_in_memory().unwrap();
        // Pre-populate 100 logs
        for i in 0..100 {
            let log = CallLog {
                id: 0,
                request_id: format!("req-{}", i),
                rune_name: if i % 2 == 0 {
                    "echo".into()
                } else {
                    "transform".into()
                },
                mode: "sync".into(),
                caster_id: None,
                latency_ms: (i * 10) as i64,
                status_code: 200,
                input_size: 100,
                output_size: 50,
                timestamp: format!("2026-03-23T00:{:02}:00Z", i % 60),
            };
            rt.block_on(store.insert_log(&log)).unwrap();
        }
        b.iter(|| {
            rt.block_on(store.query_logs(None, 50)).unwrap();
        });
    });

    // --- store_stats_enhanced ---
    group.bench_function("stats_enhanced", |b| {
        let store = RuneStore::open_in_memory().unwrap();
        // Pre-populate 200 logs across 5 runes
        for i in 0..200 {
            let log = CallLog {
                id: 0,
                request_id: format!("req-{}", i),
                rune_name: format!("rune_{}", i % 5),
                mode: "sync".into(),
                caster_id: None,
                latency_ms: (i % 100 + 1) as i64,
                status_code: if i % 20 == 0 { 500 } else { 200 },
                input_size: 100,
                output_size: 50,
                timestamp: format!("2026-03-23T00:{:02}:{:02}Z", (i / 60) % 60, i % 60),
            };
            rt.block_on(store.insert_log(&log)).unwrap();
        }
        b.iter(|| {
            rt.block_on(store.call_stats_enhanced()).unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Rate limit benchmarks
// ============================================================================

fn rate_limit_benchmarks(c: &mut Criterion) {
    use rune_gate::rate_limit::RateLimitState;

    let mut group = c.benchmark_group("rate_limit");

    group.bench_function("check", |b| {
        let limiter = RateLimitState::new(1000, 60);
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            // Use rotating keys to avoid always hitting the limit
            let key = format!("client-{}", counter % 100);
            let _ = limiter.check(&key);
        });
    });

    group.finish();
}

// ============================================================================
// Main
// ============================================================================

criterion_group!(
    benches,
    relay_benchmarks,
    invoker_benchmarks,
    dag_benchmarks,
    schema_benchmarks,
    store_benchmarks,
    rate_limit_benchmarks
);
criterion_main!(benches);
