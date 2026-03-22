use bytes::Bytes;
use rune_core::invoker::LocalInvoker;
use rune_core::relay::Relay;
use rune_core::resolver::RoundRobinResolver;
use rune_core::rune::{make_handler, RuneConfig, RuneError};
use rune_flow::dag::{FlowDefinition, StepDefinition};
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
        priority: 0, labels: Default::default(),
    }
}

fn step(name: &str, rune: &str) -> StepDefinition {
    StepDefinition {
        name: name.to_string(),
        rune: rune.to_string(),
        depends_on: vec![],
        condition: None,
        input_mapping: None,
    }
}

fn step_with_deps(name: &str, rune: &str, deps: &[&str]) -> StepDefinition {
    StepDefinition {
        name: name.to_string(),
        rune: rune.to_string(),
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
        condition: None,
        input_mapping: None,
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
        rune: rune.to_string(),
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
        condition: None,
        input_mapping: Some(mapping),
    }
}

fn step_with_condition(name: &str, rune: &str, deps: &[&str], condition: &str) -> StepDefinition {
    StepDefinition {
        name: name.to_string(),
        rune: rune.to_string(),
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
        condition: Some(condition.to_string()),
        input_mapping: None,
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
        rune: rune.to_string(),
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
        condition: Some(condition.to_string()),
        input_mapping: Some(mapping),
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
                step_with_condition(
                    "B",
                    "step_b",
                    &["A"],
                    "steps.A.output.a == true",
                ),
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
            vec![
                step("A", "step_a"),
                step_with_deps("B", "step_b", &["A"]),
            ],
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
        .register(flow(
            "bad_rune",
            vec![step("A", "nonexistent_rune")],
        ))
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

    let result = engine
        .execute("empty_input", Bytes::new())
        .await
        .unwrap();

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

    let result = engine
        .execute("empty_output", Bytes::new())
        .await
        .unwrap();

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
            vec![
                step("A", "step_a"),
                step_with_deps("B", "step_b", &["A"]),
            ],
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
            vec![
                step("A", "step_a"),
                step_with_deps("B", "step_b", &["A"]),
            ],
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
                step_with_condition_and_mapping(
                    "C",
                    "step_c",
                    &["A", "B"],
                    "true",
                    mapping,
                ),
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
