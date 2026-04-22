use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, HashSet, VecDeque};

/// DAG Flow 定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDefinition {
    pub name: String,
    pub steps: Vec<StepDefinition>,
    pub gate_path: Option<String>,
}

/// DAG 中的一个步骤
#[derive(Debug, Clone)]
pub struct StepDefinition {
    pub name: String,
    #[allow(dead_code)]
    pub depends_on: Vec<String>,
    pub condition: Option<String>,
    pub input_mapping: Option<HashMap<String, String>>,
    /// 覆盖全局 step_timeout（单位毫秒）
    pub timeout_ms: Option<u64>,
    pub retry: Option<RetryConfig>,
    pub kind: StepKind,
}

/// Step 类型判别符
#[derive(Debug, Clone)]
pub enum StepKind {
    Rune(RuneConfig),
    Loop(LoopConfig),
}

/// Rune step 配置
#[derive(Debug, Clone)]
pub struct RuneConfig {
    pub rune: String,
}

/// Loop Step 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopConfig {
    pub body: Vec<BodyStep>,
    pub max_iterations: u32,
    pub until: Option<String>,
}

/// Loop body 中的步骤（有名字、条件、映射）
#[derive(Debug, Clone)]
pub struct BodyStep {
    pub name: String,
    pub condition: Option<String>,
    pub input_mapping: Option<HashMap<String, String>>,
    pub timeout_ms: Option<u64>,
    pub retry: Option<RetryConfig>,
    pub kind: BodyStepKind,
}

/// BodyStep 的类型判别（支持递归嵌套）
#[derive(Debug, Clone)]
pub enum BodyStepKind {
    Rune(RuneConfig),
    Loop(LoopConfig),
}

/// Step 失败后的重试策略（PR-7 实现执行逻辑）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// 含首次执行，必须 >= 1
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

// ─── Serde for StepDefinition ────────────────────────────────────────────────
//
// Backward compat: 无 "type" 字段 + 有 "rune" 字段 → StepKind::Rune
// New: "type":"loop" → StepKind::Loop
// Serialize: Rune 步骤只写 "rune" 字段（不写 "type"），Loop 步骤写 "type":"loop"

impl Serialize for StepDefinition {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = serde_json::Map::new();
        map.insert("name".into(), self.name.clone().into());
        map.insert(
            "depends_on".into(),
            serde_json::to_value(&self.depends_on).map_err(serde::ser::Error::custom)?,
        );
        if let Some(c) = &self.condition {
            map.insert("condition".into(), c.clone().into());
        }
        if let Some(m) = &self.input_mapping {
            map.insert(
                "input_mapping".into(),
                serde_json::to_value(m).map_err(serde::ser::Error::custom)?,
            );
        }
        if let Some(t) = self.timeout_ms {
            map.insert("timeout_ms".into(), t.into());
        }
        if let Some(r) = &self.retry {
            map.insert(
                "retry".into(),
                serde_json::to_value(r).map_err(serde::ser::Error::custom)?,
            );
        }
        match &self.kind {
            StepKind::Rune(rc) => {
                map.insert("rune".into(), rc.rune.clone().into());
            }
            StepKind::Loop(lc) => {
                map.insert("type".into(), "loop".into());
                let lv = serde_json::to_value(lc).map_err(serde::ser::Error::custom)?;
                if let serde_json::Value::Object(lm) = lv {
                    map.extend(lm);
                }
            }
        }
        serde_json::Value::Object(map).serialize(s)
    }
}

impl<'de> Deserialize<'de> for StepDefinition {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(d)?;
        StepDefinition::try_from(v).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<serde_json::Value> for StepDefinition {
    type Error = String;

    fn try_from(v: serde_json::Value) -> Result<Self, Self::Error> {
        let name = v["name"]
            .as_str()
            .ok_or("StepDefinition missing 'name'")?
            .to_string();
        let depends_on: Vec<String> = v
            .get("depends_on")
            .and_then(|d| serde_json::from_value(d.clone()).ok())
            .unwrap_or_default();
        let condition = v["condition"].as_str().map(|s| s.to_string());
        let input_mapping: Option<HashMap<String, String>> = v
            .get("input_mapping")
            .and_then(|m| serde_json::from_value(m.clone()).ok());
        let timeout_ms: Option<u64> = v.get("timeout_ms").and_then(|t| t.as_u64());
        let retry: Option<RetryConfig> = v
            .get("retry")
            .and_then(|r| serde_json::from_value(r.clone()).ok());

        // 类型推断：先看 "type" 字段，缺失时默认 "rune"（向后兼容——旧格式无 type 字段）
        let step_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("rune");

        let kind = match step_type {
            "rune" => {
                let rune = v["rune"]
                    .as_str()
                    .ok_or("StepDefinition type=rune missing 'rune' field")?
                    .to_string();
                StepKind::Rune(RuneConfig { rune })
            }
            "loop" => {
                let lc: LoopConfig =
                    serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
                StepKind::Loop(lc)
            }
            other => return Err(format!("unknown step type: '{other}'")),
        };

        Ok(StepDefinition {
            name,
            depends_on,
            condition,
            input_mapping,
            timeout_ms,
            retry,
            kind,
        })
    }
}

// ─── Serde for BodyStep ───────────────────────────────────────────────────────

impl Serialize for BodyStep {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = serde_json::Map::new();
        map.insert("name".into(), self.name.clone().into());
        if let Some(c) = &self.condition {
            map.insert("condition".into(), c.clone().into());
        }
        if let Some(m) = &self.input_mapping {
            map.insert(
                "input_mapping".into(),
                serde_json::to_value(m).map_err(serde::ser::Error::custom)?,
            );
        }
        if let Some(t) = self.timeout_ms {
            map.insert("timeout_ms".into(), t.into());
        }
        if let Some(r) = &self.retry {
            map.insert(
                "retry".into(),
                serde_json::to_value(r).map_err(serde::ser::Error::custom)?,
            );
        }
        match &self.kind {
            BodyStepKind::Rune(rc) => {
                map.insert("type".into(), "rune".into());
                map.insert("rune".into(), rc.rune.clone().into());
            }
            BodyStepKind::Loop(lc) => {
                map.insert("type".into(), "loop".into());
                let lv = serde_json::to_value(lc).map_err(serde::ser::Error::custom)?;
                if let serde_json::Value::Object(lm) = lv {
                    map.extend(lm);
                }
            }
        }
        serde_json::Value::Object(map).serialize(s)
    }
}

impl<'de> Deserialize<'de> for BodyStep {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(d)?;
        BodyStep::try_from(v).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<serde_json::Value> for BodyStep {
    type Error = String;

    fn try_from(v: serde_json::Value) -> Result<Self, Self::Error> {
        let name = v["name"]
            .as_str()
            .ok_or("BodyStep missing 'name'")?
            .to_string();
        let condition = v["condition"].as_str().map(|s| s.to_string());
        let input_mapping: Option<HashMap<String, String>> = v
            .get("input_mapping")
            .and_then(|m| serde_json::from_value(m.clone()).ok());
        let timeout_ms: Option<u64> = v.get("timeout_ms").and_then(|t| t.as_u64());
        let retry: Option<RetryConfig> = v
            .get("retry")
            .and_then(|r| serde_json::from_value(r.clone()).ok());

        let step_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("rune");

        let kind = match step_type {
            "rune" => {
                let rune = v["rune"]
                    .as_str()
                    .ok_or("BodyStep type=rune missing 'rune' field")?
                    .to_string();
                BodyStepKind::Rune(RuneConfig { rune })
            }
            "loop" => {
                let lc: LoopConfig =
                    serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
                BodyStepKind::Loop(lc)
            }
            other => return Err(format!("unknown body step type: '{other}'")),
        };

        Ok(BodyStep {
            name,
            condition,
            input_mapping,
            timeout_ms,
            retry,
            kind,
        })
    }
}

// ─── DAG 验证错误 ─────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("cycle detected: {0}")]
    CycleDetected(String),
    #[error("unknown dependency: step '{step}' depends on '{dep}' which doesn't exist")]
    UnknownDependency { step: String, dep: String },
    #[error("duplicate step name: {0}")]
    DuplicateStep(String),
    #[error("multi-upstream step '{step}' must declare input_mapping")]
    MissingInputMapping { step: String },
    #[error("step '{step}' has unparseable condition: '{condition}' — use 'true', 'false', or a comparison with spaces around the operator (e.g. 'x == 5')")]
    InvalidCondition { step: String, condition: String },
    #[error("step '{step}' has invalid loop config: {reason}")]
    InvalidLoopConfig { step: String, reason: String },
}

// ─── 验证 ─────────────────────────────────────────────────────────────────────

/// 验证 FlowDefinition，检查 DAG 有效性
pub fn validate_dag(flow: &FlowDefinition) -> Result<(), DagError> {
    let steps = &flow.steps;

    if steps.is_empty() {
        return Ok(());
    }

    // 1. 检查重复 step 名
    let mut seen = HashSet::new();
    for s in steps {
        if !seen.insert(&s.name) {
            return Err(DagError::DuplicateStep(s.name.clone()));
        }
    }

    let name_to_idx: HashMap<&str, usize> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();

    // 2. 检查不存在的依赖
    for s in steps {
        for dep in &s.depends_on {
            if !name_to_idx.contains_key(dep.as_str()) {
                return Err(DagError::UnknownDependency {
                    step: s.name.clone(),
                    dep: dep.clone(),
                });
            }
        }
    }

    // 3. Kahn 算法检测环
    let n = steps.len();
    let mut in_degree = vec![0usize; n];
    let mut adj: Vec<Vec<usize>> = vec![vec![]; n];

    for (i, s) in steps.iter().enumerate() {
        for dep in &s.depends_on {
            let dep_idx = name_to_idx[dep.as_str()];
            adj[dep_idx].push(i);
            in_degree[i] += 1;
        }
    }

    let mut queue: VecDeque<usize> = VecDeque::new();
    for (i, deg) in in_degree.iter().enumerate() {
        if *deg == 0 {
            queue.push_back(i);
        }
    }

    let mut visited = 0usize;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        for &next in &adj[node] {
            in_degree[next] -= 1;
            if in_degree[next] == 0 {
                queue.push_back(next);
            }
        }
    }

    if visited != n {
        let cycle_steps: Vec<String> = (0..n)
            .filter(|&i| in_degree[i] > 0)
            .map(|i| steps[i].name.clone())
            .collect();
        return Err(DagError::CycleDetected(cycle_steps.join(" -> ")));
    }

    // 4. 多上游无 input_mapping 检查
    for s in steps {
        if s.depends_on.len() > 1 && s.input_mapping.is_none() {
            return Err(DagError::MissingInputMapping {
                step: s.name.clone(),
            });
        }
    }

    // 5. condition 语法预检 + loop config 验证
    let operators = ["==", "!=", ">=", "<=", ">", "<"];
    for s in steps {
        validate_condition_syntax(&s.name, &s.condition, &operators)?;
        if let StepKind::Loop(lc) = &s.kind {
            validate_loop_config(&s.name, lc, &operators)?;
        }
    }

    Ok(())
}

fn validate_condition_syntax(
    step_name: &str,
    condition: &Option<String>,
    operators: &[&str],
) -> Result<(), DagError> {
    if let Some(condition) = condition {
        let trimmed = condition.trim();
        if trimmed.is_empty() || trimmed == "true" || trimmed == "false" {
            return Ok(());
        }
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        let has_operator = operators.iter().any(|op| tokens.contains(op));
        if !has_operator {
            return Err(DagError::InvalidCondition {
                step: step_name.to_string(),
                condition: condition.clone(),
            });
        }
    }
    Ok(())
}

fn validate_loop_config(
    step_name: &str,
    lc: &LoopConfig,
    operators: &[&str],
) -> Result<(), DagError> {
    if lc.max_iterations == 0 {
        return Err(DagError::InvalidLoopConfig {
            step: step_name.to_string(),
            reason: "max_iterations must be >= 1".to_string(),
        });
    }
    if lc.body.is_empty() {
        return Err(DagError::InvalidLoopConfig {
            step: step_name.to_string(),
            reason: "body must not be empty".to_string(),
        });
    }

    // body step 名唯一性
    let mut seen = HashSet::new();
    for bs in &lc.body {
        if !seen.insert(&bs.name) {
            return Err(DagError::InvalidLoopConfig {
                step: step_name.to_string(),
                reason: format!("duplicate body step name: '{}'", bs.name),
            });
        }
    }

    // body step condition 预检 + 递归验证嵌套 loop
    let qualified = |bs_name: &str| format!("{step_name}/{bs_name}");
    for bs in &lc.body {
        validate_condition_syntax(&qualified(&bs.name), &bs.condition, operators)?;
        if let BodyStepKind::Loop(nested) = &bs.kind {
            validate_loop_config(&qualified(&bs.name), nested, operators)?;
        }
    }

    // until condition 预检
    if lc.until.is_some() {
        validate_condition_syntax(step_name, &lc.until, operators)?;
    }

    Ok(())
}

// ─── 拓扑排序 ─────────────────────────────────────────────────────────────────

/// 拓扑排序，返回执行层级（同一层可并行）
pub fn topological_layers(flow: &FlowDefinition) -> Result<Vec<Vec<usize>>, DagError> {
    validate_dag(flow)?;

    let steps = &flow.steps;
    if steps.is_empty() {
        return Ok(vec![]);
    }

    let n = steps.len();
    let name_to_idx: HashMap<&str, usize> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();

    let mut in_degree = vec![0usize; n];
    let mut adj: Vec<Vec<usize>> = vec![vec![]; n];

    for (i, s) in steps.iter().enumerate() {
        for dep in &s.depends_on {
            let dep_idx = name_to_idx[dep.as_str()];
            adj[dep_idx].push(i);
            in_degree[i] += 1;
        }
    }

    let mut layers: Vec<Vec<usize>> = Vec::new();
    let mut current_layer: Vec<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();

    while !current_layer.is_empty() {
        let mut next_layer = Vec::new();
        for &node in &current_layer {
            for &next in &adj[node] {
                in_degree[next] -= 1;
                if in_degree[next] == 0 {
                    next_layer.push(next);
                }
            }
        }
        layers.push(current_layer);
        current_layer = next_layer;
    }

    Ok(layers)
}
