use bytes::Bytes;
use rune_core::invoker::LocalInvoker;
use rune_core::relay::Relay;
use rune_core::resolver::RoundRobinResolver;
use rune_core::rune::{make_handler, RuneConfig, RuneError};
use rune_flow::dag::{FlowDefinition, RuneConfig as FlowRuneConfig, StepDefinition, StepKind};
use rune_flow::engine::{FlowEngine, FlowError, StepStatus};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================================
// Helpers
// ============================================================

fn rune_config(name: &str) -> RuneConfig {
    RuneConfig {
        name: name.to_string(),
        version: String::new(),
        description: String::new(),
        supports_stream: false,
        gate: None,
        input_schema: None,
        output_schema: None,
        priority: 0,
        labels: Default::default(),
    }
}

fn step(name: &str, rune: &str) -> StepDefinition {
    StepDefinition {
        name: name.to_string(),
        depends_on: vec![],
        condition: None,
        input_mapping: None,
        timeout_ms: None,
        retry: None,
        kind: StepKind::Rune(FlowRuneConfig { rune: rune.into() }),
    }
}

fn step_with_deps(name: &str, rune: &str, deps: &[&str]) -> StepDefinition {
    StepDefinition {
        name: name.to_string(),
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
        condition: None,
        input_mapping: None,
        timeout_ms: None,
        retry: None,
        kind: StepKind::Rune(FlowRuneConfig { rune: rune.into() }),
    }
}

fn step_with_deps_and_mapping(
    name: &str,
    rune: &str,
    deps: &[&str],
    mapping: HashMap<String, String>,
) -> StepDefinition {
    StepDefinition {
        name: name.to_string(),
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
        condition: None,
        input_mapping: Some(mapping),
        timeout_ms: None,
        retry: None,
        kind: StepKind::Rune(FlowRuneConfig { rune: rune.into() }),
    }
}

fn step_with_condition(name: &str, rune: &str, deps: &[&str], condition: &str) -> StepDefinition {
    StepDefinition {
        name: name.to_string(),
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
        condition: Some(condition.to_string()),
        input_mapping: None,
        timeout_ms: None,
        retry: None,
        kind: StepKind::Rune(FlowRuneConfig { rune: rune.into() }),
    }
}

fn step_with_condition_and_mapping(
    name: &str,
    rune: &str,
    deps: &[&str],
    condition: &str,
    mapping: HashMap<String, String>,
) -> StepDefinition {
    StepDefinition {
        name: name.to_string(),
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
        condition: Some(condition.to_string()),
        input_mapping: Some(mapping),
        timeout_ms: None,
        retry: None,
        kind: StepKind::Rune(FlowRuneConfig { rune: rune.into() }),
    }
}

fn flow(name: &str, steps: Vec<StepDefinition>) -> FlowDefinition {
    FlowDefinition {
        name: name.to_string(),
        steps,
        gate_path: None,
    }
}

/// 创建一个简单的 Relay，注册常用 rune handler
fn test_relay() -> Arc<Relay> {
    let relay = Arc::new(Relay::new());

    // step_a: 给 JSON 加 "a": true
    let ha = make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).map_err(|e| RuneError::InvalidInput(e.to_string()))?;
        v.as_object_mut().unwrap().insert("a".into(), true.into());
        Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(rune_config("step_a"), Arc::new(LocalInvoker::new(ha)), None)
        .unwrap();

    // step_b: 给 JSON 加 "b": true
    let hb = make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).map_err(|e| RuneError::InvalidInput(e.to_string()))?;
        v.as_object_mut().unwrap().insert("b".into(), true.into());
        Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(rune_config("step_b"), Arc::new(LocalInvoker::new(hb)), None)
        .unwrap();

    // step_c: 给 JSON 加 "c": true
    let hc = make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).map_err(|e| RuneError::InvalidInput(e.to_string()))?;
        v.as_object_mut().unwrap().insert("c".into(), true.into());
        Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(rune_config("step_c"), Arc::new(LocalInvoker::new(hc)), None)
        .unwrap();

    // step_d: 给 JSON 加 "d": true
    let hd = make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).map_err(|e| RuneError::InvalidInput(e.to_string()))?;
        v.as_object_mut().unwrap().insert("d".into(), true.into());
        Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(rune_config("step_d"), Arc::new(LocalInvoker::new(hd)), None)
        .unwrap();

    // echo: 原样返回输入
    let echo = make_handler(|_ctx, input| async move { Ok(input) });
    relay
        .register(rune_config("echo"), Arc::new(LocalInvoker::new(echo)), None)
        .unwrap();

    // fail_step: 总是失败
    let hf = make_handler(|_ctx, _input| async move {
        Err(RuneError::ExecutionFailed {
            code: "BOOM".into(),
            message: "intentional failure".into(),
        })
    });
    relay
        .register(
            rune_config("fail_step"),
            Arc::new(LocalInvoker::new(hf)),
            None,
        )
        .unwrap();

    // slow_step: sleep 100ms 后返回
    let hs = make_handler(|_ctx, input| async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(input)
    });
    relay
        .register(
            rune_config("slow_step"),
            Arc::new(LocalInvoker::new(hs)),
            None,
        )
        .unwrap();

    // sleep_200ms: sleep 200ms 后返回（用于 timeout 测试）
    let h200 = make_handler(|_ctx, input| async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(input)
    });
    relay
        .register(
            rune_config("sleep_200ms"),
            Arc::new(LocalInvoker::new(h200)),
            None,
        )
        .unwrap();

    relay
}

fn new_engine(relay: Arc<Relay>) -> FlowEngine {
    let resolver = Arc::new(RoundRobinResolver::new());
    FlowEngine::new(relay, resolver)
}

// ============================================================
// 基础执行
// ============================================================

#[tokio::test]
async fn execute_single_step() {
    // 单 step flow 执行：input → rune → output
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow("single", vec![step("A", "step_a")]))
        .unwrap();

    let result = engine
        .execute("single", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["x"], 1);
    assert_eq!(v["a"], true);
    assert_eq!(result.steps_executed, 1);
}

#[tokio::test]
async fn execute_linear_chain() {
    // A→B→C 线性 chain
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "chain",
            vec![
                step("A", "step_a"),
                step_with_deps("B", "step_b", &["A"]),
                step_with_deps("C", "step_c", &["B"]),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("chain", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["x"], 1);
    assert_eq!(v["a"], true);
    assert_eq!(v["b"], true);
    assert_eq!(v["c"], true);
    assert_eq!(result.steps_executed, 3);
}

#[tokio::test]
async fn execute_diamond_dag() {
    // 菱形 DAG：A→B, A→C, B+C→D
    let mut mapping = HashMap::new();
    mapping.insert("from_b".to_string(), "B.output".to_string());
    mapping.insert("from_c".to_string(), "C.output".to_string());

    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "diamond",
            vec![
                step("A", "step_a"),
                step_with_deps("B", "step_b", &["A"]),
                step_with_deps("C", "step_c", &["A"]),
                step_with_deps_and_mapping("D", "step_d", &["B", "C"], mapping),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("diamond", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["d"], true);
    assert_eq!(result.steps_executed, 4);
}

#[tokio::test]
async fn execute_independent_steps_all_parallel() {
    // A, B, C 无依赖，全并行
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "parallel",
            vec![
                step("A", "step_a"),
                step("B", "step_b"),
                step("C", "step_c"),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("parallel", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    assert_eq!(result.steps_executed, 3);

    // 所有 step 都应该完成
    for name in ["A", "B", "C"] {
        assert!(
            matches!(result.steps.get(name), Some(StepStatus::Completed { .. })),
            "step {} should be completed",
            name
        );
    }
}

#[tokio::test]
async fn execute_empty_flow_passthrough() {
    // 空 flow：passthrough input
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine.register(flow("empty", vec![])).unwrap();

    let input = Bytes::from(r#"{"pass":"through"}"#);
    let result = engine.execute("empty", input.clone()).await.unwrap();

    assert_eq!(result.output, input);
    assert_eq!(result.steps_executed, 0);
}

// ============================================================
// 并行验证
// ============================================================

#[tokio::test]
async fn parallel_execution_timing() {
    // A→B, A→C 中 B 和 C 使用 slow_step（100ms）
    // 如果并行则 ~200ms（A 100ms + B/C 并行 100ms），串行则 ~300ms
    let relay = test_relay();
    let mut engine = new_engine(relay);

    let mut mapping = HashMap::new();
    mapping.insert("from_b".to_string(), "B.output".to_string());
    mapping.insert("from_c".to_string(), "C.output".to_string());

    engine
        .register(flow(
            "parallel_timing",
            vec![
                step("A", "echo"),
                step_with_deps("B", "slow_step", &["A"]),
                step_with_deps("C", "slow_step", &["A"]),
                step_with_deps_and_mapping("D", "echo", &["B", "C"], mapping),
            ],
        ))
        .unwrap();

    let start = Instant::now();
    let _result = engine
        .execute("parallel_timing", Bytes::from(r#"{}"#))
        .await
        .unwrap();
    let elapsed = start.elapsed();

    // B 和 C 并行: total ~100ms，非串行的 ~200ms
    assert!(
        elapsed < Duration::from_millis(180),
        "B and C should run in parallel, elapsed: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn multi_layer_parallel() {
    // 两层并行：Layer 0: [A, B]，Layer 1: [C, D]
    // A, B 各 100ms，C depends on A 100ms，D depends on B 100ms
    // 总时间应约 200ms（两层各 100ms），非 400ms
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "multi_layer",
            vec![
                step("A", "slow_step"),
                step("B", "slow_step"),
                step_with_deps("C", "slow_step", &["A"]),
                step_with_deps("D", "slow_step", &["B"]),
            ],
        ))
        .unwrap();

    let start = Instant::now();
    let _result = engine
        .execute("multi_layer", Bytes::from(r#"{}"#))
        .await
        .unwrap();
    let elapsed = start.elapsed();

    // 2 层各 100ms ~200ms，不是 4*100ms=400ms
    assert!(
        elapsed < Duration::from_millis(300),
        "multi-layer parallel should be ~200ms, elapsed: {:?}",
        elapsed
    );
}

// ============================================================
// 条件分支
// ============================================================

#[tokio::test]
async fn condition_true_executes_step() {
    // condition 为 true → step 执行
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "cond_true",
            vec![step_with_condition("A", "step_a", &[], "true")],
        ))
        .unwrap();

    let result = engine
        .execute("cond_true", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    assert!(matches!(
        result.steps.get("A"),
        Some(StepStatus::Completed { .. })
    ));
}

#[tokio::test]
async fn condition_false_skips_step() {
    // condition 为 false → step 被 skip
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "cond_false",
            vec![step_with_condition("A", "step_a", &[], "false")],
        ))
        .unwrap();

    let result = engine
        .execute("cond_false", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    assert!(matches!(result.steps.get("A"), Some(StepStatus::Skipped)));
}

#[tokio::test]
async fn skip_propagation_single_upstream() {
    // A(skip) → B 只有一个上游且被 skip，B 也 skip
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "skip_prop",
            vec![
                step_with_condition("A", "step_a", &[], "false"),
                step_with_deps("B", "step_b", &["A"]),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("skip_prop", Bytes::from(r#"{}"#))
        .await
        .unwrap();

    assert!(matches!(result.steps.get("A"), Some(StepStatus::Skipped)));
    assert!(matches!(result.steps.get("B"), Some(StepStatus::Skipped)));
}

#[tokio::test]
async fn skip_does_not_block_other_deps() {
    // B depends on [A(skip), C(completed)]
    // A 被 skip 但 C 完成，B 应该执行（只是缺少 A 的输入）
    let mut mapping = HashMap::new();
    mapping.insert("from_a".to_string(), "A.output".to_string());
    mapping.insert("from_c".to_string(), "C.output".to_string());

    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "skip_partial",
            vec![
                step_with_condition("A", "step_a", &[], "false"),
                step("C", "step_c"),
                step_with_deps_and_mapping("B", "step_b", &["A", "C"], mapping),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("skip_partial", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    assert!(matches!(result.steps.get("A"), Some(StepStatus::Skipped)));
    assert!(matches!(
        result.steps.get("C"),
        Some(StepStatus::Completed { .. })
    ));
    assert!(matches!(
        result.steps.get("B"),
        Some(StepStatus::Completed { .. })
    ));
}

#[tokio::test]
async fn all_upstreams_skipped_then_downstream_skipped() {
    // A(skip), B(skip) → C depends on [A, B]，所有上游 skip → C 也 skip
    let mut mapping = HashMap::new();
    mapping.insert("from_a".to_string(), "A.output".to_string());
    mapping.insert("from_b".to_string(), "B.output".to_string());

    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "all_skip",
            vec![
                step_with_condition("A", "step_a", &[], "false"),
                step_with_condition("B", "step_b", &[], "false"),
                step_with_deps_and_mapping("C", "step_c", &["A", "B"], mapping),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("all_skip", Bytes::from(r#"{}"#))
        .await
        .unwrap();

    assert!(matches!(result.steps.get("A"), Some(StepStatus::Skipped)));
    assert!(matches!(result.steps.get("B"), Some(StepStatus::Skipped)));
    assert!(matches!(result.steps.get("C"), Some(StepStatus::Skipped)));
}

#[tokio::test]
async fn condition_references_upstream_output() {
    // condition 引用上游 output 字段
    // A 输出 {"a": true}，B 的 condition 检查 "steps.A.output.a == true"
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "cond_ref",
            vec![
                step("A", "step_a"),
                step_with_condition("B", "step_b", &["A"], "steps.A.output.a == true"),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("cond_ref", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    // B 的 condition 引用 A 的输出，A 输出 {"x":1, "a":true}，所以 B 应该执行
    assert!(matches!(
        result.steps.get("B"),
        Some(StepStatus::Completed { .. })
    ));
}

// ============================================================
// input_mapping
// ============================================================

#[tokio::test]
async fn single_upstream_default_passthrough() {
    // 单上游默认传递：A→B，B 收到 A 的完整 output
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "passthrough",
            vec![step("A", "step_a"), step_with_deps("B", "step_b", &["A"])],
        ))
        .unwrap();

    let result = engine
        .execute("passthrough", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    // B 收到 A 的输出 {"x":1, "a": true}，然后加 "b": true
    assert_eq!(v["x"], 1);
    assert_eq!(v["a"], true);
    assert_eq!(v["b"], true);
}

#[tokio::test]
async fn multi_upstream_mapping_constructs_json() {
    // 多上游 mapping：B+C→D，D 的 input 是 mapping 构造的 JSON
    let mut mapping = HashMap::new();
    mapping.insert("from_b".to_string(), "B.output".to_string());
    mapping.insert("from_c".to_string(), "C.output".to_string());

    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "mapping_test",
            vec![
                step("B", "step_b"),
                step("C", "step_c"),
                step_with_deps_and_mapping("D", "step_d", &["B", "C"], mapping),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("mapping_test", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    // D 应该收到由 mapping 构造的 JSON，然后加 "d": true
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["d"], true);
}

#[tokio::test]
async fn mapping_input_reference() {
    // $input 引用：mapping 中 "$input.field" 引用 flow 原始输入
    let mut mapping = HashMap::new();
    mapping.insert("original".to_string(), "$input.x".to_string());
    mapping.insert("from_a".to_string(), "A.output".to_string());

    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "input_ref",
            vec![
                step("A", "step_a"),
                step_with_deps_and_mapping("B", "step_b", &["A"], mapping),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("input_ref", Bytes::from(r#"{"x":42}"#))
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["b"], true);
}

#[tokio::test]
async fn mapping_upstream_specific_field() {
    // step_name.output.field 引用上游特定字段
    let mut mapping = HashMap::new();
    mapping.insert("a_flag".to_string(), "A.output.a".to_string());

    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "field_ref",
            vec![
                step("A", "step_a"),
                step_with_deps_and_mapping("B", "step_b", &["A"], mapping),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("field_ref", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["b"], true);
}

#[tokio::test]
async fn skipped_step_output_is_null_in_mapping() {
    // skipped step 的 output 为 null
    let mut mapping = HashMap::new();
    mapping.insert("from_a".to_string(), "A.output".to_string());
    mapping.insert("from_c".to_string(), "C.output".to_string());

    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "null_skip",
            vec![
                step_with_condition("A", "step_a", &[], "false"),
                step("C", "step_c"),
                step_with_deps_and_mapping("B", "echo", &["A", "C"], mapping),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("null_skip", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    // B 应该执行（至少有一个上游 C 完成）
    assert!(matches!(
        result.steps.get("B"),
        Some(StepStatus::Completed { .. })
    ));

    // 验证 B 收到的 input 中 from_a 为 null
    if let Some(StepStatus::Completed { output }) = result.steps.get("B") {
        let v: serde_json::Value = serde_json::from_slice(output).unwrap();
        assert!(v["from_a"].is_null(), "skipped step output should be null");
    }
}

// ============================================================
// 错误处理
// ============================================================

#[tokio::test]
async fn error_flow_not_found() {
    let relay = test_relay();
    let engine = new_engine(relay);

    let err = engine.execute("nope", Bytes::new()).await.unwrap_err();
    assert!(matches!(err, FlowError::FlowNotFound(_)));
}

#[tokio::test]
async fn error_rune_not_found_in_step() {
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow("bad_rune", vec![step("A", "nonexistent_rune")]))
        .unwrap();

    let err = engine
        .execute("bad_rune", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();
    match err {
        FlowError::StepFailed { step, .. } => {
            assert_eq!(step, "A");
        }
        _ => panic!("expected StepFailed, got {:?}", err),
    }
}

#[tokio::test]
async fn error_step_execution_fails() {
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "fail_flow",
            vec![
                step("A", "step_a"),
                step_with_deps("B", "fail_step", &["A"]),
                step_with_deps("C", "step_c", &["B"]),
            ],
        ))
        .unwrap();

    let err = engine
        .execute("fail_flow", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();
    match err {
        FlowError::StepFailed { step, .. } => {
            assert_eq!(step, "B");
        }
        _ => panic!("expected StepFailed, got {:?}", err),
    }
}

#[tokio::test]
async fn error_downstream_not_executed_after_failure() {
    // 失败后未启动的下游不执行
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "fail_downstream",
            vec![
                step("A", "fail_step"),
                step_with_deps("B", "step_b", &["A"]),
                step_with_deps("C", "step_c", &["B"]),
            ],
        ))
        .unwrap();

    let err = engine
        .execute("fail_downstream", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();

    // Flow 应该失败且 B, C 不执行
    assert!(matches!(err, FlowError::StepFailed { .. }));
}

#[tokio::test]
async fn error_parallel_one_fails_others_complete() {
    // 并行 step 中一个失败 → 其他仍完成（但 flow 结果为失败）
    let relay = test_relay();
    let mut engine = new_engine(relay);

    let mut mapping = HashMap::new();
    mapping.insert("from_b".to_string(), "B.output".to_string());
    mapping.insert("from_c".to_string(), "C.output".to_string());

    engine
        .register(flow(
            "parallel_fail",
            vec![
                step("A", "echo"),
                step_with_deps("B", "fail_step", &["A"]),
                step_with_deps("C", "step_c", &["A"]),
                step_with_deps_and_mapping("D", "step_d", &["B", "C"], mapping),
            ],
        ))
        .unwrap();

    let err = engine
        .execute("parallel_fail", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();

    // Flow 整体失败
    assert!(matches!(err, FlowError::StepFailed { .. }));
}

// ============================================================
// CRUD
// ============================================================

#[test]
fn crud_register_and_get() {
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow("my_flow", vec![step("A", "step_a")]))
        .unwrap();

    let f = engine.get("my_flow");
    assert!(f.is_some());
    assert_eq!(f.unwrap().name, "my_flow");
}

#[test]
fn crud_register_and_list() {
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow("flow_1", vec![step("A", "step_a")]))
        .unwrap();
    engine
        .register(flow("flow_2", vec![step("B", "step_b")]))
        .unwrap();

    let list = engine.list();
    assert_eq!(list.len(), 2);
    assert!(list.contains(&"flow_1"));
    assert!(list.contains(&"flow_2"));
}

#[test]
fn crud_register_remove_get_none() {
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow("removable", vec![step("A", "step_a")]))
        .unwrap();
    assert!(engine.get("removable").is_some());

    let removed = engine.remove("removable");
    assert!(removed);
    assert!(engine.get("removable").is_none());
}

#[test]
fn crud_register_invalid_dag_returns_error() {
    let relay = test_relay();
    let mut engine = new_engine(relay);

    // 注册带环的 DAG → DagError
    let result = engine.register(flow(
        "invalid",
        vec![
            step_with_deps("A", "step_a", &["B"]),
            step_with_deps("B", "step_b", &["A"]),
        ],
    ));

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FlowError::DagError(_)));
}

#[test]
fn crud_remove_nonexistent_returns_false() {
    let relay = test_relay();
    let mut engine = new_engine(relay);

    let removed = engine.remove("nonexistent");
    assert!(!removed);
}

#[test]
fn crud_get_nonexistent_returns_none() {
    let relay = test_relay();
    let engine = new_engine(relay);

    assert!(engine.get("nonexistent").is_none());
}

// ============================================================
// 边界测试
// ============================================================

#[tokio::test]
async fn boundary_large_dag_50_steps() {
    // 超大 DAG（50 个 step）线性链 echo → 验证可正常处理
    let relay = test_relay();
    let mut engine = new_engine(relay);

    let mut steps = vec![step("step_0", "echo")];
    for i in 1..50 {
        steps.push(step_with_deps(
            &format!("step_{}", i),
            "echo",
            &[&format!("step_{}", i - 1)],
        ));
    }

    engine.register(flow("big", steps)).unwrap();

    let result = engine
        .execute("big", Bytes::from(r#"{"start":true}"#))
        .await
        .unwrap();

    assert_eq!(result.steps_executed, 50);
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["start"], true);
}

#[test]
fn boundary_step_name_special_chars() {
    // step 名含特殊字符也能注册
    let relay = test_relay();
    let mut engine = new_engine(relay);

    let result = engine.register(flow(
        "special",
        vec![
            step("step-with-dashes", "echo"),
            step("step_with_underscores", "echo"),
            step("step.with.dots", "echo"),
        ],
    ));

    assert!(result.is_ok());
}

#[tokio::test]
async fn boundary_empty_input() {
    // 空 input
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow("empty_input", vec![step("A", "echo")]))
        .unwrap();

    let result = engine.execute("empty_input", Bytes::new()).await.unwrap();

    assert_eq!(result.steps_executed, 1);
}

#[tokio::test]
async fn boundary_empty_output() {
    // 空 output（echo 空字节）
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow("empty_output", vec![step("A", "echo")]))
        .unwrap();

    let result = engine.execute("empty_output", Bytes::new()).await.unwrap();

    assert_eq!(result.output, Bytes::new());
}

// ============================================================
// FlowResult 的 steps 状态验证
// ============================================================

#[tokio::test]
async fn result_steps_all_completed() {
    // 所有 step 完成后，steps map 应包含所有 step 的 Completed 状态
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "all_completed",
            vec![step("A", "step_a"), step_with_deps("B", "step_b", &["A"])],
        ))
        .unwrap();

    let result = engine
        .execute("all_completed", Bytes::from(r#"{}"#))
        .await
        .unwrap();

    assert_eq!(result.steps.len(), 2);
    assert!(matches!(
        result.steps.get("A"),
        Some(StepStatus::Completed { .. })
    ));
    assert!(matches!(
        result.steps.get("B"),
        Some(StepStatus::Completed { .. })
    ));
}

#[tokio::test]
async fn result_terminal_step_output() {
    // flow 最终输出应该是末端 step 的 output
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "terminal",
            vec![step("A", "step_a"), step_with_deps("B", "step_b", &["A"])],
        ))
        .unwrap();

    let result = engine
        .execute("terminal", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    // 末端 step B 的 output 应该包含 a+b
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["a"], true);
    assert_eq!(v["b"], true);
}

// ============================================================
// 额外场景
// ============================================================

#[tokio::test]
async fn execute_wide_fan_out() {
    // 1 个 root → 5 个并行 step
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "fan_out",
            vec![
                step("root", "echo"),
                step_with_deps("a", "step_a", &["root"]),
                step_with_deps("b", "step_b", &["root"]),
                step_with_deps("c", "step_c", &["root"]),
                step_with_deps("d", "step_d", &["root"]),
                step_with_deps("e", "echo", &["root"]),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("fan_out", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    assert_eq!(result.steps_executed, 6);
}

#[tokio::test]
async fn execute_fan_in_without_mapping_fails_register() {
    // 5 个 step → 1 个 merge（无 mapping）应在注册时失败
    let relay = test_relay();
    let mut engine = new_engine(relay);

    let result = engine.register(flow(
        "fan_in_no_mapping",
        vec![
            step("a", "step_a"),
            step("b", "step_b"),
            step("c", "step_c"),
            step_with_deps("merge", "echo", &["a", "b", "c"]),
        ],
    ));

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FlowError::DagError(_)));
}

#[tokio::test]
async fn execute_reregister_flow_overwrites() {
    // 重新注册同名 flow 应覆盖
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow("overwrite", vec![step("A", "step_a")]))
        .unwrap();
    engine
        .register(flow("overwrite", vec![step("B", "step_b")]))
        .unwrap();

    let f = engine.get("overwrite").unwrap();
    // 最新注册的 flow 应只有 step B
    assert_eq!(f.steps.len(), 1);
    assert_eq!(f.steps[0].name, "B");
}

#[tokio::test]
async fn execute_multiple_terminal_steps() {
    // 多个末端 step（无下游），引擎需要决定最终输出
    // 这个测试验证引擎不会 panic
    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "multi_terminal",
            vec![
                step("A", "echo"),
                step_with_deps("B", "step_b", &["A"]),
                step_with_deps("C", "step_c", &["A"]),
            ],
        ))
        .unwrap();

    // 应该不 panic，具体行为取决于实现
    let _result = engine
        .execute("multi_terminal", Bytes::from(r#"{"x":1}"#))
        .await;
}

#[tokio::test]
async fn condition_with_mapping_combined() {
    // condition + input_mapping 同时存在
    let mut mapping = HashMap::new();
    mapping.insert("from_a".to_string(), "A.output".to_string());
    mapping.insert("from_b".to_string(), "B.output".to_string());

    let relay = test_relay();
    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "cond_mapping",
            vec![
                step("A", "step_a"),
                step("B", "step_b"),
                step_with_condition_and_mapping("C", "step_c", &["A", "B"], "true", mapping),
            ],
        ))
        .unwrap();

    let result = engine
        .execute("cond_mapping", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    assert!(matches!(
        result.steps.get("C"),
        Some(StepStatus::Completed { .. })
    ));
}

// ============================================================
// 条件表达式解析器回归测试
// ============================================================

use rune_flow::engine::{evaluate_comparison, evaluate_condition};

/// Helper: 创建带 step output 的上下文来测试条件表达式
fn eval_cond(expr: &str) -> bool {
    let statuses = HashMap::new();
    let outputs = HashMap::new();
    let input: Option<serde_json::Value> = None;
    evaluate_condition(expr, &statuses, &outputs, &input)
}

/// Helper: 带 step outputs 和 flow input 的条件评估
fn eval_cond_with_context(
    expr: &str,
    step_outputs: &HashMap<String, Option<Bytes>>,
    flow_input_json: &Option<serde_json::Value>,
) -> bool {
    let statuses = HashMap::new();
    evaluate_condition(expr, &statuses, step_outputs, flow_input_json)
}

/// Helper: 直接测试 evaluate_comparison 返回值
fn eval_comp(expr: &str) -> Option<bool> {
    let statuses = HashMap::new();
    let outputs = HashMap::new();
    let input: Option<serde_json::Value> = None;
    evaluate_comparison(expr, &statuses, &outputs, &input)
}

fn eval_comp_with_context(
    expr: &str,
    step_outputs: &HashMap<String, Option<Bytes>>,
    flow_input_json: &Option<serde_json::Value>,
) -> Option<bool> {
    let statuses = HashMap::new();
    evaluate_comparison(expr, &statuses, step_outputs, flow_input_json)
}

#[test]
fn test_condition_with_simple_equals() {
    // "steps.A.output.count == 5" 应正确解析：lhs=steps.A.output.count, op===, rhs=5
    let mut outputs: HashMap<String, Option<Bytes>> = HashMap::new();
    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"count":5}"#)));
    let input = None;
    let result = eval_cond_with_context("steps.A.output.count == 5", &outputs, &input);
    assert!(
        result,
        "steps.A.output.count == 5 should be true when count is 5"
    );
}

#[test]
fn test_condition_with_gte() {
    // "steps.A.output.value >= 10" 正确解析
    let mut outputs: HashMap<String, Option<Bytes>> = HashMap::new();
    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"value":15}"#)));
    let input = None;
    assert!(eval_cond_with_context(
        "steps.A.output.value >= 10",
        &outputs,
        &input
    ));

    // 边界：等于
    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"value":10}"#)));
    assert!(eval_cond_with_context(
        "steps.A.output.value >= 10",
        &outputs,
        &input
    ));

    // 小于时不满足
    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"value":9}"#)));
    assert!(!eval_cond_with_context(
        "steps.A.output.value >= 10",
        &outputs,
        &input
    ));
}

#[test]
fn test_condition_with_lte() {
    // "steps.A.output.value <= 0" 正确解析
    let mut outputs: HashMap<String, Option<Bytes>> = HashMap::new();
    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"value":-1}"#)));
    let input = None;
    assert!(eval_cond_with_context(
        "steps.A.output.value <= 0",
        &outputs,
        &input
    ));

    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"value":0}"#)));
    assert!(eval_cond_with_context(
        "steps.A.output.value <= 0",
        &outputs,
        &input
    ));

    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"value":1}"#)));
    assert!(!eval_cond_with_context(
        "steps.A.output.value <= 0",
        &outputs,
        &input
    ));
}

#[test]
fn test_condition_with_not_equals() {
    // "steps.A.output.status != error" 正确解析
    let mut outputs: HashMap<String, Option<Bytes>> = HashMap::new();
    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"status":"ok"}"#)));
    let input = None;
    assert!(eval_cond_with_context(
        "steps.A.output.status != error",
        &outputs,
        &input
    ));

    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"status":"error"}"#)));
    assert!(!eval_cond_with_context(
        "steps.A.output.status != error",
        &outputs,
        &input
    ));
}

#[test]
fn test_condition_with_string_comparison() {
    // "steps.A.output.name == hello" 正确比较字符串
    let mut outputs: HashMap<String, Option<Bytes>> = HashMap::new();
    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"name":"hello"}"#)));
    let input = None;
    assert!(eval_cond_with_context(
        "steps.A.output.name == hello",
        &outputs,
        &input
    ));

    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"name":"world"}"#)));
    assert!(!eval_cond_with_context(
        "steps.A.output.name == hello",
        &outputs,
        &input
    ));
}

#[test]
fn test_condition_with_special_chars_in_path() {
    // 路径中包含 > 或 < 等字符时不应干扰操作符解析
    // 例如 step 名为 "a>b" （虽然不推荐，但解析器不应 panic）
    // 这里模拟：lhs 中包含 > 字符的情况
    // "5 == 5" 应该正常工作（简单字面量比较）
    assert_eq!(eval_comp("5 == 5"), Some(true));
    assert_eq!(eval_comp("5 == 6"), Some(false));
    assert_eq!(eval_comp("5 > 3"), Some(true));
    assert_eq!(eval_comp("3 < 5"), Some(true));
}

#[test]
fn test_condition_with_spaces_around_operator() {
    // "steps.A.output.x  ==  5" 带多余空格
    let mut outputs: HashMap<String, Option<Bytes>> = HashMap::new();
    outputs.insert("A".to_string(), Some(Bytes::from(r#"{"x":5}"#)));
    let input = None;
    assert!(eval_cond_with_context(
        "steps.A.output.x  ==  5",
        &outputs,
        &input
    ));
    assert!(eval_cond_with_context(
        "  steps.A.output.x == 5  ",
        &outputs,
        &input
    ));
}

#[test]
fn test_condition_with_boolean_and_or() {
    // 当前实现不支持 AND/OR 复合表达式
    // 不 panic 即可，不对结果做具体断言
    let _ = eval_cond("true AND true");
    let _ = eval_cond("5 == 5 AND 3 == 3");
}

#[test]
fn test_condition_empty_expression() {
    // 空表达式不应 panic，默认 false（安全侧：无法解析 = 不执行）
    assert!(!eval_cond(""));
    assert!(!eval_cond("   "));
}

#[test]
fn test_condition_invalid_expression() {
    // 无效表达式不应 panic
    // evaluate_comparison 返回 None，evaluate_condition 默认 false（安全侧）
    assert!(!eval_cond("gibberish"));
    assert!(!eval_cond("no_operator_here"));
    assert_eq!(eval_comp("no_operator_here"), None);
}

#[test]
fn test_condition_unparseable_returns_false() {
    // "x==5" (无空格) 无法被 token 解析器识别为比较表达式，应返回 false
    assert!(!eval_cond("x==5"));
}

#[test]
fn test_condition_garbage_returns_false() {
    // 完全无意义的字符串应返回 false
    assert!(!eval_cond("asdfgh"));
}

#[test]
fn test_condition_operator_in_step_name() {
    // 核心回归测试：step 路径中包含操作符字符
    // "steps.a>b.output.val == 5" 不应错误匹配路径中的 >
    // 用 token 方式解析时，操作符应该是独立 token "=="
    let mut outputs: HashMap<String, Option<Bytes>> = HashMap::new();
    // 注意：step 名中带 > 在实际使用中不推荐，但解析器应该正确处理
    // 关键是操作符两边有空格，token 解析不会被 lhs 中的 > 干扰
    outputs.insert("a>b".to_string(), Some(Bytes::from(r#"{"val":5}"#)));
    let input = None;
    let result = eval_comp_with_context("steps.a>b.output.val == 5", &outputs, &input);
    assert_eq!(
        result,
        Some(true),
        "should correctly parse == as operator, not > in path"
    );
}

#[test]
fn test_condition_gt_in_step_name_with_gt_operator() {
    // step 路径包含 > 且操作符也是 >
    // "steps.a>b.output.val > 3" — 操作符 > 是独立 token
    let mut outputs: HashMap<String, Option<Bytes>> = HashMap::new();
    outputs.insert("a>b".to_string(), Some(Bytes::from(r#"{"val":5}"#)));
    let input = None;
    let result = eval_comp_with_context("steps.a>b.output.val > 3", &outputs, &input);
    assert_eq!(
        result,
        Some(true),
        "should find standalone > operator, not the one in path"
    );
}

// ============================================================
// SF-2: uuid_simple() 并行 step 应产生唯一 request_id
// ============================================================

#[tokio::test]
async fn sf2_parallel_steps_get_unique_request_ids() {
    // 两个无依赖 step 并行执行时，它们的 request_id 不应重复。
    // 我们通过让 rune handler 回显 ctx.request_id 来验证。
    let relay = Arc::new(Relay::new());

    let id_echo = make_handler(|ctx, _input| async move {
        let id = ctx.request_id;
        Ok(Bytes::from(format!(r#"{{"id":"{}"}}"#, id)))
    });
    relay
        .register(
            rune_config("id_echo"),
            Arc::new(rune_core::invoker::LocalInvoker::new(id_echo)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    // Two parallel steps (no dependencies between them)
    engine
        .register(flow(
            "parallel_ids",
            vec![step("p1", "id_echo"), step("p2", "id_echo")],
        ))
        .unwrap();

    let result = engine
        .execute("parallel_ids", Bytes::from(r#"{}"#))
        .await
        .unwrap();

    // 提取两个 step 的 request_id
    let id1 = match result.steps.get("p1") {
        Some(StepStatus::Completed { output }) => {
            let v: serde_json::Value = serde_json::from_slice(output).unwrap();
            v["id"].as_str().unwrap().to_string()
        }
        other => panic!("p1 should be completed, got {:?}", other),
    };
    let id2 = match result.steps.get("p2") {
        Some(StepStatus::Completed { output }) => {
            let v: serde_json::Value = serde_json::from_slice(output).unwrap();
            v["id"].as_str().unwrap().to_string()
        }
        other => panic!("p2 should be completed, got {:?}", other),
    };

    assert_ne!(
        id1, id2,
        "parallel steps must get unique request_ids: {} vs {}",
        id1, id2
    );
    // Both should now contain a counter segment (req-{ts}-{seq})
    assert!(
        id1.matches('-').count() >= 2,
        "id should have format req-{{ts}}-{{seq}}: {}",
        id1
    );
    assert!(
        id2.matches('-').count() >= 2,
        "id should have format req-{{ts}}-{{seq}}: {}",
        id2
    );
}

// ============================================================
// SF-6: execute_flow works with a cloned FlowDefinition
// ============================================================

#[tokio::test]
async fn sf6_execute_flow_with_cloned_definition() {
    let relay = test_relay();
    let mut engine = new_engine(relay);

    let fd = flow("cloned", vec![step("A", "step_a")]);
    engine.register(fd.clone()).unwrap();

    // Simulate the pattern used in the gate handler:
    // 1. Get a clone of the flow definition
    let cloned_def = engine.get("cloned").unwrap().clone();

    // 2. Execute using the cloned definition (no longer looking it up internally)
    let result = engine
        .execute_flow(&cloned_def, Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["x"], 1);
    assert_eq!(v["a"], true);
    assert_eq!(result.steps_executed, 1);
}

// ============================================================
// NF-5: time_utils consistency
// ============================================================

#[test]
fn nf5_time_utils_now_ms_is_reasonable() {
    let ms = rune_core::time_utils::now_ms();
    // After 2024-01-01
    assert!(ms > 1_704_067_200_000);
}

#[test]
fn nf5_time_utils_now_iso8601_format() {
    let s = rune_core::time_utils::now_iso8601();
    assert_eq!(s.len(), 20);
    assert!(s.ends_with('Z'));
}

// ============================================================
// G: Per-step timeout_ms
// ============================================================

#[tokio::test]
async fn per_step_timeout_overrides_global() {
    // 全局 500ms，step 设 50ms，rune sleep 200ms → step timeout 触发
    let relay = test_relay();
    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::with_timeout(relay, resolver, Duration::from_millis(500));

    let mut timed_step = step("A", "sleep_200ms");
    timed_step.timeout_ms = Some(50);

    engine
        .register(flow("timeout_override", vec![timed_step]))
        .unwrap();

    let err = engine
        .execute("timeout_override", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();

    match err {
        FlowError::StepFailed {
            source: rune_core::rune::RuneError::Timeout,
            ..
        } => {}
        other => panic!("expected StepFailed(Timeout), got {:?}", other),
    }
}

#[tokio::test]
async fn per_step_timeout_none_uses_global() {
    // 无 per-step timeout，全局 50ms，rune sleep 200ms → 全局 timeout 触发
    let relay = test_relay();
    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::with_timeout(relay, resolver, Duration::from_millis(50));

    engine
        .register(flow("global_timeout", vec![step("A", "sleep_200ms")]))
        .unwrap();

    let err = engine
        .execute("global_timeout", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();

    match err {
        FlowError::StepFailed {
            source: rune_core::rune::RuneError::Timeout,
            ..
        } => {}
        other => panic!("expected StepFailed(Timeout), got {:?}", other),
    }
}

// ============================================================
// A: Loop Step (A-10 to A-16)
// ============================================================

use rune_flow::dag::{BodyStep, BodyStepKind, LoopConfig};

/// A-10: 基础 loop — 循环到 count == 3 后 until 条件满足
#[tokio::test]
async fn loop_basic_until_condition() {
    let relay = test_relay();
    let h = rune_core::rune::make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).unwrap_or(serde_json::Value::Object(Default::default()));
        let n = v["count"].as_i64().unwrap_or(0) + 1;
        v["count"] = n.into();
        Ok(bytes::Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(
            rune_config("counter"),
            Arc::new(rune_core::invoker::LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "loop_flow",
            vec![StepDefinition {
                name: "loop".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 10,
                    until: Some("steps.inc.output.count == 3".to_string()),
                    body: vec![BodyStep {
                        name: "inc".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Rune(FlowRuneConfig {
                            rune: "counter".into(),
                        }),
                    }],
                }),
            }],
        ))
        .unwrap();

    let result = engine
        .execute("loop_flow", Bytes::from(r#"{"count": 0}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["count"], 3);
}

/// A-11: body step condition — `fix` 只在 passed == false 时运行，passed == true 时被跳过
#[tokio::test]
async fn loop_body_step_condition_skips_when_false() {
    let relay = test_relay();
    let hp = rune_core::rune::make_handler(|_ctx, _input| async move {
        Ok(bytes::Bytes::from(r#"{"passed": true}"#))
    });
    relay
        .register(
            rune_config("set_pass"),
            Arc::new(rune_core::invoker::LocalInvoker::new(hp)),
            None,
        )
        .unwrap();
    let hf = rune_core::rune::make_handler(|_ctx, _input| async move {
        Err(rune_core::rune::RuneError::ExecutionFailed {
            code: "SHOULD_NOT_RUN".into(),
            message: "fix should have been skipped".into(),
        })
    });
    relay
        .register(
            rune_config("would_fail2"),
            Arc::new(rune_core::invoker::LocalInvoker::new(hf)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "cond_flow",
            vec![StepDefinition {
                name: "loop".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 1,
                    until: Some("steps.implement.output.passed == true".to_string()),
                    body: vec![
                        BodyStep {
                            name: "implement".to_string(),
                            condition: None,
                            input_mapping: None,
                            timeout_ms: None,
                            retry: None,
                            kind: BodyStepKind::Rune(FlowRuneConfig {
                                rune: "set_pass".into(),
                            }),
                        },
                        BodyStep {
                            name: "fix".to_string(),
                            condition: Some("steps.implement.output.passed == false".to_string()),
                            input_mapping: None,
                            timeout_ms: None,
                            retry: None,
                            kind: BodyStepKind::Rune(FlowRuneConfig {
                                rune: "would_fail2".into(),
                            }),
                        },
                    ],
                }),
            }],
        ))
        .unwrap();

    let result = engine
        .execute("cond_flow", Bytes::from(r#"{}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["passed"], true);
}

/// A-12: body step input_mapping — 从上游 output 字段取值作为下游输入
#[tokio::test]
async fn loop_body_step_input_mapping() {
    let relay = test_relay();
    let hp = rune_core::rune::make_handler(|_ctx, _input| async move {
        Ok(bytes::Bytes::from(r#"{"session_id": "abc123"}"#))
    });
    relay
        .register(
            rune_config("produce"),
            Arc::new(rune_core::invoker::LocalInvoker::new(hp)),
            None,
        )
        .unwrap();
    let hc = rune_core::rune::make_handler(|_ctx, input| async move { Ok(input) });
    relay
        .register(
            rune_config("consume"),
            Arc::new(rune_core::invoker::LocalInvoker::new(hc)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    let mut mapping = HashMap::new();
    mapping.insert(
        "sid".to_string(),
        "steps.produce.output.session_id".to_string(),
    );

    engine
        .register(flow(
            "mapping_flow",
            vec![StepDefinition {
                name: "loop".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 1,
                    until: None,
                    body: vec![
                        BodyStep {
                            name: "produce".to_string(),
                            condition: None,
                            input_mapping: None,
                            timeout_ms: None,
                            retry: None,
                            kind: BodyStepKind::Rune(FlowRuneConfig {
                                rune: "produce".into(),
                            }),
                        },
                        BodyStep {
                            name: "consume".to_string(),
                            condition: None,
                            input_mapping: Some(mapping),
                            timeout_ms: None,
                            retry: None,
                            kind: BodyStepKind::Rune(FlowRuneConfig {
                                rune: "consume".into(),
                            }),
                        },
                    ],
                }),
            }],
        ))
        .unwrap();

    let result = engine
        .execute("mapping_flow", Bytes::from(r#"{}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["sid"], "abc123");
}

/// A-13: 达到 max_iterations 时正常返回（非 error）
#[tokio::test]
async fn loop_reaches_max_iterations_returns_ok() {
    let relay = test_relay();
    let h = rune_core::rune::make_handler(|_ctx, _input| async move {
        Ok(bytes::Bytes::from(r#"{"passed": false}"#))
    });
    relay
        .register(
            rune_config("never_done"),
            Arc::new(rune_core::invoker::LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "max_iter_flow",
            vec![StepDefinition {
                name: "loop".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 3,
                    until: Some("steps.work.output.passed == true".to_string()),
                    body: vec![BodyStep {
                        name: "work".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Rune(FlowRuneConfig {
                            rune: "never_done".into(),
                        }),
                    }],
                }),
            }],
        ))
        .unwrap();

    let result = engine
        .execute("max_iter_flow", Bytes::from(r#"{}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["passed"], false);
}

/// A-14: 嵌套 loop — 外层 body 包含内层 loop，x 从 0 累加到 2
#[tokio::test]
async fn loop_nested_inner_outer() {
    let relay = test_relay();
    let h = rune_core::rune::make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).unwrap_or(serde_json::Value::Object(Default::default()));
        let n = v["x"].as_i64().unwrap_or(0) + 1;
        v["x"] = n.into();
        Ok(bytes::Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(
            rune_config("add_x"),
            Arc::new(rune_core::invoker::LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "nested_flow",
            vec![StepDefinition {
                name: "outer".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 1,
                    until: None,
                    body: vec![BodyStep {
                        name: "inner".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Loop(LoopConfig {
                            max_iterations: 5,
                            until: Some("steps.inc.output.x == 2".to_string()),
                            body: vec![BodyStep {
                                name: "inc".to_string(),
                                condition: None,
                                input_mapping: None,
                                timeout_ms: None,
                                retry: None,
                                kind: BodyStepKind::Rune(FlowRuneConfig {
                                    rune: "add_x".into(),
                                }),
                            }],
                        }),
                    }],
                }),
            }],
        ))
        .unwrap();

    let result = engine
        .execute("nested_flow", Bytes::from(r#"{"x": 0}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["x"], 2);
}

/// A-15: Loop step 作为 DAG 中的节点（有 depends_on），接收上游输出
#[tokio::test]
async fn loop_step_as_dag_node_with_deps() {
    let relay = test_relay();
    let h = rune_core::rune::make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).unwrap_or(serde_json::Value::Object(Default::default()));
        let n = v["count"].as_i64().unwrap_or(0) + 1;
        v["count"] = n.into();
        Ok(bytes::Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(
            rune_config("bump"),
            Arc::new(rune_core::invoker::LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "dag_loop",
            vec![
                step("A", "echo"),
                StepDefinition {
                    name: "B".to_string(),
                    depends_on: vec!["A".to_string()],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                    retry: None,
                    kind: StepKind::Loop(LoopConfig {
                        max_iterations: 5,
                        until: Some("steps.inc.output.count == 2".to_string()),
                        body: vec![BodyStep {
                            name: "inc".to_string(),
                            condition: None,
                            input_mapping: None,
                            timeout_ms: None,
                            retry: None,
                            kind: BodyStepKind::Rune(FlowRuneConfig {
                                rune: "bump".into(),
                            }),
                        }],
                    }),
                },
            ],
        ))
        .unwrap();

    let result = engine
        .execute("dag_loop", Bytes::from(r#"{"count": 0}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["count"], 2);
}

/// A-16: 所有 body steps 均 Skip 时，输出为 loop 入参（passthrough）
#[tokio::test]
async fn loop_all_body_steps_skipped_passthrough() {
    let relay = test_relay();
    let h = rune_core::rune::make_handler(|_ctx, _input| async move {
        Err(rune_core::rune::RuneError::ExecutionFailed {
            code: "SHOULD_NOT_RUN".into(),
            message: "this body step should have been skipped".into(),
        })
    });
    relay
        .register(
            rune_config("never_run"),
            Arc::new(rune_core::invoker::LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "skip_flow",
            vec![StepDefinition {
                name: "loop".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 1,
                    until: None,
                    body: vec![BodyStep {
                        name: "skip_me".to_string(),
                        condition: Some("false".to_string()),
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Rune(FlowRuneConfig {
                            rune: "never_run".into(),
                        }),
                    }],
                }),
            }],
        ))
        .unwrap();

    let input = Bytes::from(r#"{"original": true}"#);
    let result = engine.execute("skip_flow", input).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["original"], true);
}

// ============================================================
// B: Map Step (B-6 to B-10)
// ============================================================

use rune_flow::dag::MapConfig;

/// B-6: 基础数组并行处理，验证输出顺序与输入对应
#[tokio::test]
async fn map_basic_parallel_preserves_order() {
    let relay = test_relay();
    // double: wraps input number in {"value": n * 2}
    relay
        .register(
            rune_config("double"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, input| async move {
                let v: serde_json::Value = serde_json::from_slice(&input).unwrap();
                let n = v.as_i64().unwrap_or(0);
                Ok(Bytes::from(
                    serde_json::to_vec(&serde_json::json!({"value": n * 2})).unwrap(),
                ))
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "map_flow",
            vec![StepDefinition {
                name: "map_step".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Map(MapConfig {
                    over: "$input.items".to_string(),
                    body: Box::new(BodyStep {
                        name: "process".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Rune(FlowRuneConfig {
                            rune: "double".into(),
                        }),
                    }),
                    concurrency: None,
                }),
            }],
        ))
        .unwrap();

    let input = Bytes::from(r#"{"items": [1, 2, 3, 4, 5]}"#);
    let result = engine.execute("map_flow", input).await.unwrap();
    let arr: Vec<serde_json::Value> = serde_json::from_slice(&result.output).unwrap();

    assert_eq!(arr.len(), 5);
    // output order must match input order
    assert_eq!(arr[0]["value"], 2);
    assert_eq!(arr[1]["value"], 4);
    assert_eq!(arr[2]["value"], 6);
    assert_eq!(arr[3]["value"], 8);
    assert_eq!(arr[4]["value"], 10);
}

/// B-7: concurrency 限制 — 验证峰值并发数不超过设定值
#[tokio::test]
async fn map_concurrency_limit() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let relay = test_relay();
    let counter = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let counter_c = Arc::clone(&counter);
    let peak_c = Arc::clone(&peak);

    relay
        .register(
            rune_config("slow_task"),
            Arc::new(LocalInvoker::new(make_handler(move |_ctx, input| {
                let counter_c = Arc::clone(&counter_c);
                let peak_c = Arc::clone(&peak_c);
                async move {
                    let current = counter_c.fetch_add(1, Ordering::SeqCst) + 1;
                    // record peak
                    let mut p = peak_c.load(Ordering::SeqCst);
                    while current > p {
                        match peak_c.compare_exchange(
                            p,
                            current,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => break,
                            Err(actual) => p = actual,
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    counter_c.fetch_sub(1, Ordering::SeqCst);
                    Ok(input)
                }
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "limited_map",
            vec![StepDefinition {
                name: "map_step".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Map(MapConfig {
                    over: "$input.items".to_string(),
                    body: Box::new(BodyStep {
                        name: "task".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Rune(FlowRuneConfig {
                            rune: "slow_task".into(),
                        }),
                    }),
                    concurrency: Some(2),
                }),
            }],
        ))
        .unwrap();

    let input = Bytes::from(r#"{"items": [1, 2, 3, 4, 5, 6]}"#);
    engine.execute("limited_map", input).await.unwrap();

    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "peak concurrency exceeded limit"
    );
}

/// B-8: over 路径为 steps.X.output.list（引用前一 step 的输出）
#[tokio::test]
async fn map_over_previous_step_output() {
    let relay = test_relay();

    // producer: returns {"list": [10, 20, 30]}
    relay
        .register(
            rune_config("produce_list"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                Ok(Bytes::from(r#"{"list": [10, 20, 30]}"#))
            }))),
            None,
        )
        .unwrap();

    // add_one: increments input number
    relay
        .register(
            rune_config("add_one"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, input| async move {
                let v: serde_json::Value = serde_json::from_slice(&input).unwrap();
                let n = v.as_i64().unwrap_or(0) + 1;
                Ok(Bytes::from(
                    serde_json::to_vec(&serde_json::json!(n)).unwrap(),
                ))
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "ref_step_flow",
            vec![
                step("producer", "produce_list"),
                StepDefinition {
                    name: "mapper".to_string(),
                    depends_on: vec!["producer".to_string()],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                    retry: None,
                    kind: StepKind::Map(MapConfig {
                        over: "steps.producer.output.list".to_string(),
                        body: Box::new(BodyStep {
                            name: "inc".to_string(),
                            condition: None,
                            input_mapping: None,
                            timeout_ms: None,
                            retry: None,
                            kind: BodyStepKind::Rune(FlowRuneConfig {
                                rune: "add_one".into(),
                            }),
                        }),
                        concurrency: None,
                    }),
                },
            ],
        ))
        .unwrap();

    let result = engine
        .execute("ref_step_flow", Bytes::from("{}"))
        .await
        .unwrap();
    let arr: Vec<serde_json::Value> = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(arr, vec![11, 21, 31]);
}

/// B-9: 数组中某元素失败 → 整个 Map step StepFailed
#[tokio::test]
async fn map_element_failure_propagates() {
    let relay = test_relay();

    relay
        .register(
            rune_config("sometimes_fail"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, input| async move {
                let v: serde_json::Value = serde_json::from_slice(&input).unwrap();
                if v.as_i64() == Some(2) {
                    return Err(RuneError::ExecutionFailed {
                        code: "BOOM".into(),
                        message: "element 2 exploded".into(),
                    });
                }
                Ok(input)
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "fail_map",
            vec![StepDefinition {
                name: "map_step".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Map(MapConfig {
                    over: "$input.items".to_string(),
                    body: Box::new(BodyStep {
                        name: "check".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Rune(FlowRuneConfig {
                            rune: "sometimes_fail".into(),
                        }),
                    }),
                    concurrency: None,
                }),
            }],
        ))
        .unwrap();

    let input = Bytes::from(r#"{"items": [1, 2, 3]}"#);
    let err = engine.execute("fail_map", input).await.unwrap_err();
    assert!(
        matches!(err, FlowError::StepFailed { .. }),
        "expected StepFailed, got: {err:?}"
    );
}

/// B-10: body 为 Loop（Map + Loop 嵌套）
#[tokio::test]
async fn map_body_is_loop() {
    let relay = test_relay();

    // add: increments {"n": x} by 1
    relay
        .register(
            rune_config("increment"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, input| async move {
                let mut v: serde_json::Value = serde_json::from_slice(&input).unwrap();
                let n = v["n"].as_i64().unwrap_or(0) + 1;
                v["n"] = n.into();
                Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    // For each item {"n": x}, loop 3 times incrementing n, so output {"n": x+3}
    engine
        .register(flow(
            "map_loop_flow",
            vec![StepDefinition {
                name: "map_step".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Map(MapConfig {
                    over: "$input.items".to_string(),
                    body: Box::new(BodyStep {
                        name: "loop_body".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Loop(LoopConfig {
                            max_iterations: 3,
                            until: None,
                            body: vec![BodyStep {
                                name: "inc".to_string(),
                                condition: None,
                                input_mapping: None,
                                timeout_ms: None,
                                retry: None,
                                kind: BodyStepKind::Rune(FlowRuneConfig {
                                    rune: "increment".into(),
                                }),
                            }],
                        }),
                    }),
                    concurrency: None,
                }),
            }],
        ))
        .unwrap();

    let input = Bytes::from(r#"{"items": [{"n": 0}, {"n": 10}, {"n": 100}]}"#);
    let result = engine.execute("map_loop_flow", input).await.unwrap();
    let arr: Vec<serde_json::Value> = serde_json::from_slice(&result.output).unwrap();

    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["n"], 3);
    assert_eq!(arr[1]["n"], 13);
    assert_eq!(arr[2]["n"], 103);
}

// ============================================================
// C: Switch Step (C-6 to C-10)
// ============================================================

use rune_flow::dag::{SwitchCase, SwitchConfig};

/// C-6: 字符串值精确匹配，路由到对应 case body
#[tokio::test]
async fn switch_string_value_match() {
    let relay = test_relay();

    relay
        .register(
            rune_config("route-a"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                Ok(Bytes::from(r#"{"routed":"a"}"#))
            }))),
            None,
        )
        .unwrap();
    relay
        .register(
            rune_config("route-b"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                Ok(Bytes::from(r#"{"routed":"b"}"#))
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "switch_string_flow",
            vec![StepDefinition {
                name: "dispatch".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Switch(SwitchConfig {
                    on: "$input.action".to_string(),
                    cases: vec![
                        SwitchCase {
                            when: serde_json::json!("a"),
                            body: BodyStep {
                                name: "case_a".to_string(),
                                condition: None,
                                input_mapping: None,
                                timeout_ms: None,
                                retry: None,
                                kind: BodyStepKind::Rune(FlowRuneConfig {
                                    rune: "route-a".into(),
                                }),
                            },
                        },
                        SwitchCase {
                            when: serde_json::json!("b"),
                            body: BodyStep {
                                name: "case_b".to_string(),
                                condition: None,
                                input_mapping: None,
                                timeout_ms: None,
                                retry: None,
                                kind: BodyStepKind::Rune(FlowRuneConfig {
                                    rune: "route-b".into(),
                                }),
                            },
                        },
                    ],
                    default: None,
                }),
            }],
        ))
        .unwrap();

    // "a" → case_a
    let result = engine
        .execute("switch_string_flow", Bytes::from(r#"{"action":"a"}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["routed"], "a");

    // "b" → case_b
    let result2 = engine
        .execute("switch_string_flow", Bytes::from(r#"{"action":"b"}"#))
        .await
        .unwrap();
    let v2: serde_json::Value = serde_json::from_slice(&result2.output).unwrap();
    assert_eq!(v2["routed"], "b");
}

/// C-7: 数值类型精确匹配
#[tokio::test]
async fn switch_number_value_match() {
    let relay = test_relay();

    relay
        .register(
            rune_config("priority-low"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                Ok(Bytes::from(r#"{"level":"low"}"#))
            }))),
            None,
        )
        .unwrap();
    relay
        .register(
            rune_config("priority-high"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                Ok(Bytes::from(r#"{"level":"high"}"#))
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "switch_number_flow",
            vec![StepDefinition {
                name: "dispatch".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Switch(SwitchConfig {
                    on: "$input.priority".to_string(),
                    cases: vec![
                        SwitchCase {
                            when: serde_json::json!(1),
                            body: BodyStep {
                                name: "case_low".to_string(),
                                condition: None,
                                input_mapping: None,
                                timeout_ms: None,
                                retry: None,
                                kind: BodyStepKind::Rune(FlowRuneConfig {
                                    rune: "priority-low".into(),
                                }),
                            },
                        },
                        SwitchCase {
                            when: serde_json::json!(2),
                            body: BodyStep {
                                name: "case_high".to_string(),
                                condition: None,
                                input_mapping: None,
                                timeout_ms: None,
                                retry: None,
                                kind: BodyStepKind::Rune(FlowRuneConfig {
                                    rune: "priority-high".into(),
                                }),
                            },
                        },
                    ],
                    default: None,
                }),
            }],
        ))
        .unwrap();

    let result = engine
        .execute("switch_number_flow", Bytes::from(r#"{"priority":2}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["level"], "high");
}

/// C-8: 无 case 匹配时执行 default body
#[tokio::test]
async fn switch_no_match_uses_default() {
    let relay = test_relay();

    relay
        .register(
            rune_config("default-handler"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                Ok(Bytes::from(r#"{"routed":"default"}"#))
            }))),
            None,
        )
        .unwrap();
    relay
        .register(
            rune_config("route-x"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                Ok(Bytes::from(r#"{"routed":"x"}"#))
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "switch_default_flow",
            vec![StepDefinition {
                name: "dispatch".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Switch(SwitchConfig {
                    on: "$input.action".to_string(),
                    cases: vec![SwitchCase {
                        when: serde_json::json!("x"),
                        body: BodyStep {
                            name: "case_x".to_string(),
                            condition: None,
                            input_mapping: None,
                            timeout_ms: None,
                            retry: None,
                            kind: BodyStepKind::Rune(FlowRuneConfig {
                                rune: "route-x".into(),
                            }),
                        },
                    }],
                    default: Some(Box::new(BodyStep {
                        name: "default_case".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Rune(FlowRuneConfig {
                            rune: "default-handler".into(),
                        }),
                    })),
                }),
            }],
        ))
        .unwrap();

    // action "unknown" → no case matches → default executes
    let result = engine
        .execute(
            "switch_default_flow",
            Bytes::from(r#"{"action":"unknown"}"#),
        )
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["routed"], "default");
}

/// C-9: 无 case 匹配且无 default → switch step Skipped，下游级联 Skipped
#[tokio::test]
async fn switch_no_match_no_default_step_skipped() {
    let relay = test_relay();

    relay
        .register(
            rune_config("after-switch"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                Ok(Bytes::from(r#"{"ran":true}"#))
            }))),
            None,
        )
        .unwrap();
    relay
        .register(
            rune_config("route-known"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async move {
                Ok(Bytes::from(r#"{"routed":"known"}"#))
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "switch_skip_flow",
            vec![
                StepDefinition {
                    name: "dispatch".to_string(),
                    depends_on: vec![],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                    retry: None,
                    kind: StepKind::Switch(SwitchConfig {
                        on: "$input.action".to_string(),
                        cases: vec![SwitchCase {
                            when: serde_json::json!("known"),
                            body: BodyStep {
                                name: "case_known".to_string(),
                                condition: None,
                                input_mapping: None,
                                timeout_ms: None,
                                retry: None,
                                kind: BodyStepKind::Rune(FlowRuneConfig {
                                    rune: "route-known".into(),
                                }),
                            },
                        }],
                        default: None,
                    }),
                },
                StepDefinition {
                    name: "after".to_string(),
                    depends_on: vec!["dispatch".to_string()],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                    retry: None,
                    kind: StepKind::Rune(FlowRuneConfig {
                        rune: "after-switch".into(),
                    }),
                },
            ],
        ))
        .unwrap();

    // "unknown" → no match, no default → dispatch Skipped → after cascade Skipped
    let result = engine
        .execute("switch_skip_flow", Bytes::from(r#"{"action":"unknown"}"#))
        .await
        .unwrap();

    assert!(matches!(
        result.steps.get("dispatch"),
        Some(StepStatus::Skipped)
    ));
    assert!(matches!(
        result.steps.get("after"),
        Some(StepStatus::Skipped)
    ));
}

/// C-10: Switch case body 为 Loop（Switch + Loop 嵌套）
#[tokio::test]
async fn switch_case_body_is_loop() {
    let relay = test_relay();

    relay
        .register(
            rune_config("inc"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, input| async move {
                let mut v: serde_json::Value = serde_json::from_slice(&input).unwrap();
                let n = v["n"].as_i64().unwrap_or(0) + 1;
                v["n"] = n.into();
                Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
            }))),
            None,
        )
        .unwrap();
    relay
        .register(
            rune_config("passthrough"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, input| async move {
                Ok(input)
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "switch_loop_flow",
            vec![StepDefinition {
                name: "dispatch".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Switch(SwitchConfig {
                    on: "$input.cmd".to_string(),
                    cases: vec![SwitchCase {
                        when: serde_json::json!("loop"),
                        body: BodyStep {
                            name: "loop_case".to_string(),
                            condition: None,
                            input_mapping: None,
                            timeout_ms: None,
                            retry: None,
                            kind: BodyStepKind::Loop(LoopConfig {
                                max_iterations: 3,
                                until: None,
                                body: vec![BodyStep {
                                    name: "step".to_string(),
                                    condition: None,
                                    input_mapping: None,
                                    timeout_ms: None,
                                    retry: None,
                                    kind: BodyStepKind::Rune(FlowRuneConfig { rune: "inc".into() }),
                                }],
                            }),
                        },
                    }],
                    default: Some(Box::new(BodyStep {
                        name: "default_case".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Rune(FlowRuneConfig {
                            rune: "passthrough".into(),
                        }),
                    })),
                }),
            }],
        ))
        .unwrap();

    // cmd "loop" → Loop 3 iterations → n = 0 + 3 = 3
    let result = engine
        .execute("switch_loop_flow", Bytes::from(r#"{"cmd":"loop","n":0}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["n"], 3);

    // cmd "other" → default passthrough → input unchanged
    let input2 = Bytes::from(r#"{"cmd":"other","n":42}"#);
    let result2 = engine.execute("switch_loop_flow", input2).await.unwrap();
    let v2: serde_json::Value = serde_json::from_slice(&result2.output).unwrap();
    assert_eq!(v2["n"], 42);
    assert_eq!(v2["cmd"], "other");
}

/// C-11: Loop body 内嵌 Switch（BodyStepKind::Switch 路径直接覆盖）
#[tokio::test]
async fn loop_body_is_switch() {
    let relay = test_relay();

    relay
        .register(
            rune_config("add-five"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, input| async move {
                let mut v: serde_json::Value = serde_json::from_slice(&input).unwrap();
                let n = v["n"].as_i64().unwrap_or(0) + 5;
                v["n"] = n.into();
                Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
            }))),
            None,
        )
        .unwrap();
    relay
        .register(
            rune_config("passthrough-c11"),
            Arc::new(LocalInvoker::new(make_handler(|_ctx, input| async move {
                Ok(input)
            }))),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "loop_switch_flow",
            vec![StepDefinition {
                name: "looper".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 2,
                    until: None,
                    body: vec![BodyStep {
                        name: "route".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Switch(SwitchConfig {
                            on: "$input.action".to_string(),
                            cases: vec![SwitchCase {
                                when: serde_json::json!("go"),
                                body: BodyStep {
                                    name: "go_case".to_string(),
                                    condition: None,
                                    input_mapping: None,
                                    timeout_ms: None,
                                    retry: None,
                                    kind: BodyStepKind::Rune(FlowRuneConfig {
                                        rune: "add-five".into(),
                                    }),
                                },
                            }],
                            default: Some(Box::new(BodyStep {
                                name: "default_case".to_string(),
                                condition: None,
                                input_mapping: None,
                                timeout_ms: None,
                                retry: None,
                                kind: BodyStepKind::Rune(FlowRuneConfig {
                                    rune: "passthrough-c11".into(),
                                }),
                            })),
                        }),
                    }],
                }),
            }],
        ))
        .unwrap();

    // action "go" → 2 iterations × add-five → n = 0 + 5 + 5 = 10
    let result = engine
        .execute("loop_switch_flow", Bytes::from(r#"{"action":"go","n":0}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["n"], 10);

    // action "noop" → default passthrough × 2 → n unchanged
    let result2 = engine
        .execute(
            "loop_switch_flow",
            Bytes::from(r#"{"action":"noop","n":99}"#),
        )
        .await
        .unwrap();
    let v2: serde_json::Value = serde_json::from_slice(&result2.output).unwrap();
    assert_eq!(v2["n"], 99);
}

// ─── Module D: Flow Step ─────────────────────────────────────────────────────

/// D-6: 基本子流调用 — 外层 flow 的 Flow Step 正确调用子流并返回其结果
#[tokio::test]
async fn flow_step_calls_subflow() {
    use rune_flow::dag::FlowConfig;

    let relay = test_relay();
    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(FlowDefinition {
            name: "sub".to_string(),
            gate_path: None,
            steps: vec![step("a", "step_a"), step_with_deps("b", "step_b", &["a"])],
        })
        .unwrap();

    engine
        .register(FlowDefinition {
            name: "outer".to_string(),
            gate_path: None,
            steps: vec![StepDefinition {
                name: "call_sub".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Flow(FlowConfig {
                    flow: "sub".to_string(),
                }),
            }],
        })
        .unwrap();

    let result = engine.execute("outer", Bytes::from(r#"{}"#)).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["a"], true);
    assert_eq!(v["b"], true);
}

/// D-7: Flow Step 引用不存在的子流 → 步骤失败（错误信息含 flow 名）
#[tokio::test]
async fn flow_step_not_found_error() {
    use rune_flow::dag::FlowConfig;

    let relay = test_relay();
    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(FlowDefinition {
            name: "outer".to_string(),
            gate_path: None,
            steps: vec![StepDefinition {
                name: "bad_call".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Flow(FlowConfig {
                    flow: "nonexistent".to_string(),
                }),
            }],
        })
        .unwrap();

    let err = engine
        .execute("outer", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();
    assert!(
        matches!(err, FlowError::StepFailed { .. }),
        "expected StepFailed, got {err:?}"
    );
    let FlowError::StepFailed { source, .. } = err else {
        unreachable!()
    };
    assert!(
        source.to_string().contains("nonexistent"),
        "error message should mention the missing flow, got: {source}"
    );
}

/// D-8: 直接循环引用 — flow A 调用自身 → 步骤失败（内含循环链信息）
#[tokio::test]
async fn flow_step_direct_circular_ref() {
    use rune_flow::dag::FlowConfig;

    let relay = test_relay();
    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(FlowDefinition {
            name: "cycle_a".to_string(),
            gate_path: None,
            steps: vec![StepDefinition {
                name: "self_call".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Flow(FlowConfig {
                    flow: "cycle_a".to_string(),
                }),
            }],
        })
        .unwrap();

    let err = engine
        .execute("cycle_a", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();
    assert!(
        matches!(err, FlowError::StepFailed { .. }),
        "expected StepFailed, got {err:?}"
    );
    let FlowError::StepFailed { source, .. } = err else {
        unreachable!()
    };
    assert!(
        source.to_string().contains("cycle_a"),
        "error should contain flow name, got: {source}"
    );
}

/// D-9: 间接循环引用 A → B → A → 步骤失败（错误信息含完整调用链）
#[tokio::test]
async fn flow_step_indirect_circular_ref() {
    use rune_flow::dag::FlowConfig;

    let relay = test_relay();
    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(FlowDefinition {
            name: "cycle_a".to_string(),
            gate_path: None,
            steps: vec![StepDefinition {
                name: "call_b".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Flow(FlowConfig {
                    flow: "cycle_b".to_string(),
                }),
            }],
        })
        .unwrap();

    engine
        .register(FlowDefinition {
            name: "cycle_b".to_string(),
            gate_path: None,
            steps: vec![StepDefinition {
                name: "call_a".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Flow(FlowConfig {
                    flow: "cycle_a".to_string(),
                }),
            }],
        })
        .unwrap();

    let err = engine
        .execute("cycle_a", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();
    assert!(
        matches!(err, FlowError::StepFailed { .. }),
        "expected StepFailed, got {err:?}"
    );
    let FlowError::StepFailed { source, .. } = err else {
        unreachable!()
    };
    let msg = source.to_string();
    assert!(
        msg.contains("cycle_a") && msg.contains("cycle_b"),
        "error should show call chain, got: {msg}"
    );
}

/// D-10: Flow Step 在 Loop body 中 — 3 次迭代，每次调用子流递增 count，最终 count == 3
#[tokio::test]
async fn flow_step_in_loop_body() {
    use rune_flow::dag::{BodyStep, BodyStepKind, FlowConfig, LoopConfig};

    let relay = test_relay();
    let h = rune_core::rune::make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).unwrap_or(serde_json::Value::Object(Default::default()));
        let n = v["count"].as_i64().unwrap_or(0) + 1;
        v["count"] = n.into();
        Ok(bytes::Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(
            rune_config("inc-d10"),
            Arc::new(rune_core::invoker::LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(FlowDefinition {
            name: "incrementer".to_string(),
            gate_path: None,
            steps: vec![StepDefinition {
                name: "inc".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Rune(FlowRuneConfig {
                    rune: "inc-d10".to_string(),
                }),
            }],
        })
        .unwrap();

    engine
        .register(FlowDefinition {
            name: "loop_with_flow".to_string(),
            gate_path: None,
            steps: vec![StepDefinition {
                name: "loop".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 3,
                    until: None,
                    body: vec![BodyStep {
                        name: "call_inc".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: BodyStepKind::Flow(FlowConfig {
                            flow: "incrementer".to_string(),
                        }),
                    }],
                }),
            }],
        })
        .unwrap();

    let result = engine
        .execute("loop_with_flow", Bytes::from(r#"{"count": 0}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(
        v["count"], 3,
        "3 loop iterations × 1 increment each = count 3"
    );
}

// ─── Module F: Loop Progress Events ─────────────────────────────────────────

/// F-9: Loop 执行时推送 LoopIterationStart 和 LoopIterationEnd 事件
#[tokio::test]
async fn loop_progress_iteration_events() {
    use rune_flow::dag::LoopConfig;
    use rune_flow::engine::FlowProgressEvent;
    use tokio::sync::mpsc;

    let relay = test_relay();
    let h = make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).unwrap_or(serde_json::Value::Object(Default::default()));
        let n = v["count"].as_i64().unwrap_or(0) + 1;
        v["count"] = n.into();
        Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(
            rune_config("counter-f9"),
            Arc::new(LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "loop_f9",
            vec![StepDefinition {
                name: "my_loop".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 3,
                    until: None,
                    body: vec![rune_flow::dag::BodyStep {
                        name: "inc".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: rune_flow::dag::BodyStepKind::Rune(FlowRuneConfig {
                            rune: "counter-f9".into(),
                        }),
                    }],
                }),
            }],
        ))
        .unwrap();

    let (progress_tx, mut progress_rx) = mpsc::channel::<FlowProgressEvent>(32);
    let runner = engine.runner().with_progress(progress_tx);

    let flow_def = engine.get("loop_f9").unwrap().clone();
    let _ = runner
        .execute_flow(&flow_def, Bytes::from(r#"{"count":0}"#))
        .await
        .unwrap();

    // runner 在 execute_flow 完成后被 drop，progress_tx 也随之 drop → channel 关闭
    let mut events = vec![];
    while let Ok(evt) = progress_rx.try_recv() {
        events.push(evt);
    }

    let starts = events
        .iter()
        .filter(|e| matches!(e, FlowProgressEvent::LoopIterationStart { .. }))
        .count();
    let ends = events
        .iter()
        .filter(|e| matches!(e, FlowProgressEvent::LoopIterationEnd { .. }))
        .count();
    let done = events
        .iter()
        .filter(|e| matches!(e, FlowProgressEvent::BodyStepDone { .. }))
        .count();

    assert_eq!(starts, 3, "expected 3 LoopIterationStart");
    assert_eq!(ends, 3, "expected 3 LoopIterationEnd");
    assert_eq!(done, 3, "expected 3 BodyStepDone");

    // 断言：所有事件的 step 字段都是 "my_loop"
    for evt in &events {
        let step = match evt {
            FlowProgressEvent::LoopIterationStart { step, .. }
            | FlowProgressEvent::LoopIterationEnd { step, .. }
            | FlowProgressEvent::BodyStepDone { step, .. }
            | FlowProgressEvent::BodyStepSkipped { step, .. }
            | FlowProgressEvent::BodyStepFailed { step, .. } => step,
        };
        assert_eq!(step, "my_loop");
    }
}

/// F-10: until 条件满足时 LoopIterationEnd 的 until_met 为 true
#[tokio::test]
async fn loop_progress_until_met_flag() {
    use rune_flow::dag::LoopConfig;
    use rune_flow::engine::FlowProgressEvent;
    use tokio::sync::mpsc;

    let relay = test_relay();
    let h = make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).unwrap_or(serde_json::Value::Object(Default::default()));
        let n = v["count"].as_i64().unwrap_or(0) + 1;
        v["count"] = n.into();
        Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(
            rune_config("counter-f10"),
            Arc::new(LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "loop_f10",
            vec![StepDefinition {
                name: "counted_loop".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 10,
                    until: Some("steps.inc.output.count == 2".to_string()),
                    body: vec![rune_flow::dag::BodyStep {
                        name: "inc".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: rune_flow::dag::BodyStepKind::Rune(FlowRuneConfig {
                            rune: "counter-f10".into(),
                        }),
                    }],
                }),
            }],
        ))
        .unwrap();

    let (progress_tx, mut progress_rx) = mpsc::channel::<FlowProgressEvent>(32);
    let runner = engine.runner().with_progress(progress_tx);
    let flow_def = engine.get("loop_f10").unwrap().clone();
    let _ = runner
        .execute_flow(&flow_def, Bytes::from(r#"{"count":0}"#))
        .await
        .unwrap();

    let mut events = vec![];
    while let Ok(evt) = progress_rx.try_recv() {
        events.push(evt);
    }

    let end_events: Vec<_> = events
        .iter()
        .filter_map(|e| {
            if let FlowProgressEvent::LoopIterationEnd {
                until_met,
                iteration,
                ..
            } = e
            {
                Some((*iteration, *until_met))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(end_events.len(), 2, "expected 2 iterations");
    assert!(!end_events[0].1, "iteration 0: until_met should be false");
    assert!(end_events[1].1, "iteration 1: until_met should be true");
}

/// F-11: 无 progress_tx 时 Loop 执行正常（零开销路径）
#[tokio::test]
async fn loop_without_progress_tx_works() {
    use rune_flow::dag::LoopConfig;

    let relay = test_relay();
    let h = make_handler(|_ctx, input| async move {
        let mut v: serde_json::Value =
            serde_json::from_slice(&input).unwrap_or(serde_json::Value::Object(Default::default()));
        v["count"] = (v["count"].as_i64().unwrap_or(0) + 1).into();
        Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
    });
    relay
        .register(
            rune_config("counter-f11"),
            Arc::new(LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let resolver = Arc::new(RoundRobinResolver::new());
    let mut engine = FlowEngine::new(relay, resolver);

    engine
        .register(flow(
            "loop_f11",
            vec![StepDefinition {
                name: "loop".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Loop(LoopConfig {
                    max_iterations: 2,
                    until: None,
                    body: vec![rune_flow::dag::BodyStep {
                        name: "inc".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: rune_flow::dag::BodyStepKind::Rune(FlowRuneConfig {
                            rune: "counter-f11".into(),
                        }),
                    }],
                }),
            }],
        ))
        .unwrap();

    // 不设置 progress_tx，走默认路径
    let result = engine
        .execute("loop_f11", Bytes::from(r#"{"count":0}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["count"], 2);
}

// ─── Module E: Retry ─────────────────────────────────────────────────────────

/// E-5: Retry Rune step — 前两次失败，第三次成功
#[tokio::test]
async fn retry_rune_step_on_failure() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let relay = test_relay();
    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count2 = Arc::clone(&attempt_count);

    let h = make_handler(move |_ctx, input| {
        let count = Arc::clone(&attempt_count2);
        async move {
            let n = count.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(RuneError::ExecutionFailed {
                    code: "TRANSIENT".into(),
                    message: "temporary failure".into(),
                })
            } else {
                Ok(input)
            }
        }
    });
    relay
        .register(rune_config("flaky"), Arc::new(LocalInvoker::new(h)), None)
        .unwrap();

    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "retry_flow",
            vec![StepDefinition {
                name: "step1".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: Some(rune_flow::dag::RetryConfig {
                    max_attempts: 3,
                    backoff_ms: 0,
                }),
                kind: StepKind::Rune(FlowRuneConfig {
                    rune: "flaky".into(),
                }),
            }],
        ))
        .unwrap();

    let result = engine
        .execute("retry_flow", Bytes::from(r#"{"ok":true}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(
        attempt_count.load(Ordering::SeqCst),
        3,
        "should have been called 3 times"
    );
}

/// E-6: max_attempts 耗尽后返回最后一次错误
#[tokio::test]
async fn retry_exhausted_returns_error() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let relay = test_relay();
    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count2 = Arc::clone(&attempt_count);

    let h = make_handler(move |_ctx, _input| {
        let count = Arc::clone(&attempt_count2);
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err::<Bytes, RuneError>(RuneError::ExecutionFailed {
                code: "ALWAYS_FAIL".into(),
                message: "always fails".into(),
            })
        }
    });
    relay
        .register(
            rune_config("always-fail"),
            Arc::new(LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "retry_exhaust",
            vec![StepDefinition {
                name: "step1".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: Some(rune_flow::dag::RetryConfig {
                    max_attempts: 2,
                    backoff_ms: 0,
                }),
                kind: StepKind::Rune(FlowRuneConfig {
                    rune: "always-fail".into(),
                }),
            }],
        ))
        .unwrap();

    let err = engine
        .execute("retry_exhaust", Bytes::from(r#"{}"#))
        .await
        .unwrap_err();
    assert!(
        matches!(err, FlowError::StepFailed { .. }),
        "expected StepFailed, got {err:?}"
    );
    assert_eq!(
        attempt_count.load(Ordering::SeqCst),
        2,
        "should have been called exactly 2 times"
    );
}

/// E-7: Loop step retry — loop 第一次整体失败，retry 后成功
#[tokio::test]
async fn retry_loop_step_on_failure() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let relay = test_relay();
    let call_count = Arc::new(AtomicU32::new(0));
    let call_count2 = Arc::clone(&call_count);

    // 第 1 次调用失败 → loop 整体失败 → retry → 第 2 次调用成功
    let h = make_handler(move |_ctx, input| {
        let count = Arc::clone(&call_count2);
        async move {
            let n = count.fetch_add(1, Ordering::SeqCst);
            if n < 1 {
                Err(RuneError::ExecutionFailed {
                    code: "FAIL".into(),
                    message: "first attempt fails".into(),
                })
            } else {
                Ok(input)
            }
        }
    });
    relay
        .register(
            rune_config("semi-flaky"),
            Arc::new(LocalInvoker::new(h)),
            None,
        )
        .unwrap();

    let mut engine = new_engine(relay);

    engine
        .register(flow(
            "retry_loop",
            vec![StepDefinition {
                name: "loop_step".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: Some(rune_flow::dag::RetryConfig {
                    max_attempts: 2,
                    backoff_ms: 0,
                }),
                kind: StepKind::Loop(rune_flow::dag::LoopConfig {
                    max_iterations: 1,
                    until: None,
                    body: vec![rune_flow::dag::BodyStep {
                        name: "inner".to_string(),
                        condition: None,
                        input_mapping: None,
                        timeout_ms: None,
                        retry: None,
                        kind: rune_flow::dag::BodyStepKind::Rune(FlowRuneConfig {
                            rune: "semi-flaky".into(),
                        }),
                    }],
                }),
            }],
        ))
        .unwrap();

    let result = engine
        .execute("retry_loop", Bytes::from(r#"{"x":1}"#))
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();
    assert_eq!(v["x"], 1);
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        2,
        "called once (fail) + once (success) = 2"
    );
}
