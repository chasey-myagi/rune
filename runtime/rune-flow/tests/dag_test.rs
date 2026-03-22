use rune_flow::dag::*;
use std::collections::HashMap;

// ============================================================
// Helper: 快速构建 FlowDefinition
// ============================================================

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

fn flow(name: &str, steps: Vec<StepDefinition>) -> FlowDefinition {
    FlowDefinition {
        name: name.to_string(),
        steps,
        gate_path: None,
    }
}

// ============================================================
// DAG 验证测试
// ============================================================

#[test]
fn validate_linear_flow() {
    // A→B→C: 线性 DAG 应验证通过
    let f = flow("linear", vec![
        step("A", "rune_a"),
        step_with_deps("B", "rune_b", &["A"]),
        step_with_deps("C", "rune_c", &["B"]),
    ]);
    assert!(validate_dag(&f).is_ok());
}

#[test]
fn validate_diamond_dag() {
    // A→B, A→C, B+C→D（菱形 DAG）
    let mut mapping = HashMap::new();
    mapping.insert("from_b".to_string(), "B.output".to_string());
    mapping.insert("from_c".to_string(), "C.output".to_string());

    let f = flow("diamond", vec![
        step("A", "rune_a"),
        step_with_deps("B", "rune_b", &["A"]),
        step_with_deps("C", "rune_c", &["A"]),
        step_with_deps_and_mapping("D", "rune_d", &["B", "C"], mapping),
    ]);
    assert!(validate_dag(&f).is_ok());
}

#[test]
fn validate_cycle_detected() {
    // A→B→C→A: 应检测到环
    let f = flow("cyclic", vec![
        step_with_deps("A", "rune_a", &["C"]),
        step_with_deps("B", "rune_b", &["A"]),
        step_with_deps("C", "rune_c", &["B"]),
    ]);
    let err = validate_dag(&f).unwrap_err();
    assert!(matches!(err, DagError::CycleDetected(_)));
}

#[test]
fn validate_self_loop_detected() {
    // A depends on A: 自环
    let f = flow("self_loop", vec![
        step_with_deps("A", "rune_a", &["A"]),
    ]);
    let err = validate_dag(&f).unwrap_err();
    assert!(matches!(err, DagError::CycleDetected(_)));
}

#[test]
fn validate_unknown_dependency() {
    // A depends on X（不存在）
    let f = flow("unknown_dep", vec![
        step_with_deps("A", "rune_a", &["X"]),
    ]);
    let err = validate_dag(&f).unwrap_err();
    match err {
        DagError::UnknownDependency { step, dep } => {
            assert_eq!(step, "A");
            assert_eq!(dep, "X");
        }
        _ => panic!("expected UnknownDependency, got {:?}", err),
    }
}

#[test]
fn validate_duplicate_step_name() {
    // 两个 step 都叫 "extract"
    let f = flow("dup", vec![
        step("extract", "rune_a"),
        step("extract", "rune_b"),
    ]);
    let err = validate_dag(&f).unwrap_err();
    match err {
        DagError::DuplicateStep(name) => assert_eq!(name, "extract"),
        _ => panic!("expected DuplicateStep, got {:?}", err),
    }
}

#[test]
fn validate_multi_upstream_missing_input_mapping() {
    // B+C→D 无 input_mapping
    let f = flow("no_mapping", vec![
        step("B", "rune_b"),
        step("C", "rune_c"),
        step_with_deps("D", "rune_d", &["B", "C"]),
    ]);
    let err = validate_dag(&f).unwrap_err();
    match err {
        DagError::MissingInputMapping { step } => assert_eq!(step, "D"),
        _ => panic!("expected MissingInputMapping, got {:?}", err),
    }
}

#[test]
fn validate_multi_upstream_with_input_mapping() {
    // B+C→D 有 input_mapping: 验证通过
    let mut mapping = HashMap::new();
    mapping.insert("b_out".to_string(), "B.output".to_string());
    mapping.insert("c_out".to_string(), "C.output".to_string());

    let f = flow("with_mapping", vec![
        step("B", "rune_b"),
        step("C", "rune_c"),
        step_with_deps_and_mapping("D", "rune_d", &["B", "C"], mapping),
    ]);
    assert!(validate_dag(&f).is_ok());
}

#[test]
fn validate_single_upstream_no_mapping() {
    // A→B, 单上游不需要 mapping
    let f = flow("single_upstream", vec![
        step("A", "rune_a"),
        step_with_deps("B", "rune_b", &["A"]),
    ]);
    assert!(validate_dag(&f).is_ok());
}

#[test]
fn validate_empty_flow() {
    // 空 flow（无 steps）：验证通过
    let f = flow("empty", vec![]);
    assert!(validate_dag(&f).is_ok());
}

#[test]
fn validate_single_step_no_deps() {
    // 单个 step 无依赖：验证通过
    let f = flow("single", vec![step("A", "rune_a")]);
    assert!(validate_dag(&f).is_ok());
}

// ============================================================
// 拓扑排序测试
// ============================================================

#[test]
fn topo_linear() {
    // A→B→C → 3 层，每层 1 个
    let f = flow("linear", vec![
        step("A", "rune_a"),
        step_with_deps("B", "rune_b", &["A"]),
        step_with_deps("C", "rune_c", &["B"]),
    ]);
    let layers = topological_layers(&f).unwrap();
    assert_eq!(layers.len(), 3);
    assert_eq!(layers[0].len(), 1); // [A]
    assert_eq!(layers[1].len(), 1); // [B]
    assert_eq!(layers[2].len(), 1); // [C]
}

#[test]
fn topo_diamond() {
    // A→B, A→C, B+C→D → 3 层 [[A], [B,C], [D]]
    let mut mapping = HashMap::new();
    mapping.insert("from_b".to_string(), "B.output".to_string());
    mapping.insert("from_c".to_string(), "C.output".to_string());

    let f = flow("diamond", vec![
        step("A", "rune_a"),
        step_with_deps("B", "rune_b", &["A"]),
        step_with_deps("C", "rune_c", &["A"]),
        step_with_deps_and_mapping("D", "rune_d", &["B", "C"], mapping),
    ]);
    let layers = topological_layers(&f).unwrap();
    assert_eq!(layers.len(), 3);
    assert_eq!(layers[0].len(), 1); // [A]
    assert_eq!(layers[1].len(), 2); // [B, C]
    assert_eq!(layers[2].len(), 1); // [D]
}

#[test]
fn topo_all_independent() {
    // A, B, C 无依赖 → 1 层 [[A,B,C]]
    let f = flow("independent", vec![
        step("A", "rune_a"),
        step("B", "rune_b"),
        step("C", "rune_c"),
    ]);
    let layers = topological_layers(&f).unwrap();
    assert_eq!(layers.len(), 1);
    assert_eq!(layers[0].len(), 3);
}

#[test]
fn topo_complex_multi_layer() {
    // 复杂 DAG：
    // A, B（独立）→ C depends on [A], D depends on [A, B]（需要 mapping），E depends on [C, D]（需要 mapping）
    let mut cd_mapping = HashMap::new();
    cd_mapping.insert("from_a".to_string(), "A.output".to_string());
    cd_mapping.insert("from_b".to_string(), "B.output".to_string());

    let mut e_mapping = HashMap::new();
    e_mapping.insert("from_c".to_string(), "C.output".to_string());
    e_mapping.insert("from_d".to_string(), "D.output".to_string());

    let f = flow("complex", vec![
        step("A", "rune_a"),
        step("B", "rune_b"),
        step_with_deps("C", "rune_c", &["A"]),
        step_with_deps_and_mapping("D", "rune_d", &["A", "B"], cd_mapping),
        step_with_deps_and_mapping("E", "rune_e", &["C", "D"], e_mapping),
    ]);
    let layers = topological_layers(&f).unwrap();
    assert_eq!(layers.len(), 3);
    // Layer 0: [A, B]
    assert_eq!(layers[0].len(), 2);
    // Layer 1: [C, D]
    assert_eq!(layers[1].len(), 2);
    // Layer 2: [E]
    assert_eq!(layers[2].len(), 1);
}

#[test]
fn topo_step_with_condition_still_participates() {
    // 有 condition 的 step 仍然参与拓扑排序
    let f = flow("conditional_topo", vec![
        step("A", "rune_a"),
        step_with_condition("B", "rune_b", &["A"], "input.x > 10"),
        step_with_deps("C", "rune_c", &["B"]),
    ]);
    let layers = topological_layers(&f).unwrap();
    assert_eq!(layers.len(), 3);
    assert_eq!(layers[0].len(), 1); // [A]
    assert_eq!(layers[1].len(), 1); // [B] (有 condition 但仍在排序中)
    assert_eq!(layers[2].len(), 1); // [C]
}

// ============================================================
// FlowDefinition 序列化测试
// ============================================================

#[test]
fn serde_round_trip() {
    let mut mapping = HashMap::new();
    mapping.insert("from_b".to_string(), "B.output.value".to_string());

    let original = FlowDefinition {
        name: "test_flow".to_string(),
        steps: vec![
            StepDefinition {
                name: "A".to_string(),
                rune: "rune_a".to_string(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
            },
            StepDefinition {
                name: "B".to_string(),
                rune: "rune_b".to_string(),
                depends_on: vec!["A".to_string()],
                condition: Some("input.ready == true".to_string()),
                input_mapping: Some(mapping),
            },
        ],
        gate_path: Some("/api/test".to_string()),
    };

    let json = serde_json::to_string(&original).unwrap();
    let deserialized: FlowDefinition = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.name, original.name);
    assert_eq!(deserialized.steps.len(), original.steps.len());
    assert_eq!(deserialized.gate_path, original.gate_path);

    // 验证 step 细节
    assert_eq!(deserialized.steps[0].name, "A");
    assert!(deserialized.steps[0].depends_on.is_empty());
    assert!(deserialized.steps[0].condition.is_none());
    assert!(deserialized.steps[0].input_mapping.is_none());

    assert_eq!(deserialized.steps[1].name, "B");
    assert_eq!(deserialized.steps[1].depends_on, vec!["A".to_string()]);
    assert_eq!(deserialized.steps[1].condition.as_deref(), Some("input.ready == true"));
    assert!(deserialized.steps[1].input_mapping.is_some());
}

#[test]
fn serde_missing_depends_on_defaults_empty() {
    // depends_on 缺失时默认为空 Vec
    let json = r#"{
        "name": "minimal",
        "steps": [{
            "name": "A",
            "rune": "rune_a"
        }],
        "gate_path": null
    }"#;
    let f: FlowDefinition = serde_json::from_str(json).unwrap();
    assert!(f.steps[0].depends_on.is_empty());
}

#[test]
fn serde_missing_condition_defaults_none() {
    // condition 缺失时默认为 None
    let json = r#"{
        "name": "no_cond",
        "steps": [{
            "name": "A",
            "rune": "rune_a",
            "depends_on": ["B"]
        }],
        "gate_path": null
    }"#;
    let f: FlowDefinition = serde_json::from_str(json).unwrap();
    assert!(f.steps[0].condition.is_none());
}

#[test]
fn serde_missing_input_mapping_defaults_none() {
    // input_mapping 缺失时默认为 None
    let json = r#"{
        "name": "no_mapping",
        "steps": [{
            "name": "A",
            "rune": "rune_a",
            "depends_on": []
        }],
        "gate_path": null
    }"#;
    let f: FlowDefinition = serde_json::from_str(json).unwrap();
    assert!(f.steps[0].input_mapping.is_none());
}

// ============================================================
// 额外边界场景
// ============================================================

#[test]
fn validate_longer_cycle_detected() {
    // A→B→C→D→B: 更长的环
    let f = flow("long_cycle", vec![
        step("A", "rune_a"),
        step_with_deps("B", "rune_b", &["A", "D"]),
        step_with_deps("C", "rune_c", &["B"]),
        step_with_deps("D", "rune_d", &["C"]),
    ]);
    let err = validate_dag(&f).unwrap_err();
    assert!(matches!(err, DagError::CycleDetected(_)));
}

#[test]
fn validate_multiple_roots() {
    // 多个没有依赖的根 step 应验证通过
    let f = flow("multi_root", vec![
        step("root1", "rune_a"),
        step("root2", "rune_b"),
        step_with_deps("merge", "rune_c", &["root1"]),
    ]);
    assert!(validate_dag(&f).is_ok());
}

#[test]
fn topo_empty_flow() {
    // 空 flow 的拓扑排序应返回空层
    let f = flow("empty", vec![]);
    let layers = topological_layers(&f).unwrap();
    assert!(layers.is_empty());
}

#[test]
fn topo_single_step() {
    // 单 step 的拓扑排序
    let f = flow("single", vec![step("A", "rune_a")]);
    let layers = topological_layers(&f).unwrap();
    assert_eq!(layers.len(), 1);
    assert_eq!(layers[0].len(), 1);
}

#[test]
fn validate_complex_diamond_with_conditions_and_mappings() {
    // 复杂场景：多条件、多映射的菱形 DAG
    let mut mapping = HashMap::new();
    mapping.insert("summary".to_string(), "summarize.output.text".to_string());
    mapping.insert("keywords".to_string(), "extract.output.keywords".to_string());

    let f = flow("complex_diamond", vec![
        step("parse", "rune_parse"),
        step_with_condition("summarize", "rune_summarize", &["parse"], "input.length > 100"),
        step_with_condition("extract", "rune_extract", &["parse"], "input.type == 'article'"),
        step_with_deps_and_mapping("combine", "rune_combine", &["summarize", "extract"], mapping),
    ]);
    assert!(validate_dag(&f).is_ok());
}
