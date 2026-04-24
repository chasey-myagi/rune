use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, HashSet, VecDeque};

// Re-export RuneError so RetryConfig::should_retry can reference it without
// forcing every caller to import it.
use rune_core::rune::RuneError;

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
    Map(MapConfig),
    Switch(SwitchConfig),
    Flow(FlowConfig),
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

/// Map Step 配置：对数组中每个元素并发执行 body
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapConfig {
    /// 路径表达式，解析为输入数组（如 "$input.items" 或 "steps.X.output.list"）
    pub over: String,
    /// 对每个元素执行的步骤
    pub body: Box<BodyStep>,
    /// 最大并发数，None = 全并行
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<usize>,
}

/// Flow Step 配置：将另一个已注册的 Flow 作为子步骤调用
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowConfig {
    /// 子 Flow 名称（注册时不验证是否存在，执行时检测）
    pub flow: String,
}

/// Switch Step 配置：对表达式求值，按结果路由到对应的 case body
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwitchConfig {
    /// 路径表达式，求值结果与 cases[*].when 精确匹配
    pub on: String,
    pub cases: Vec<SwitchCase>,
    /// 无任何 case 匹配时执行的 body；None 时 step 状态为 Skipped
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Box<BodyStep>>,
}

/// Switch 中的一个匹配分支
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwitchCase {
    /// 精确匹配值（支持任意 JSON 类型）
    pub when: serde_json::Value,
    pub body: BodyStep,
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
    Map(MapConfig),
    Switch(SwitchConfig),
    Flow(FlowConfig),
}

/// 退避策略
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BackoffStrategy {
    #[default]
    Fixed,
    Exponential,
    ExponentialJitter,
}

/// 重试触发条件
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RetryCondition {
    Any,
    Timeout,
}

/// Retry configuration for a **single flow step**.
///
/// Controls how many times a step is retried within the flow engine
/// before the step is marked as failed. This is distinct from
/// `rune_core::config::RetryConfig` which governs relay-level retries
/// for cross-caster RPC calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// 含首次执行，必须 >= 1
    pub max_attempts: u32,
    pub backoff_ms: u64,
    #[serde(default)]
    pub backoff_strategy: BackoffStrategy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_delay_ms: Option<u64>,
    /// 重试触发条件列表。省略该字段、显式指定 ["any"] 或空列表 []
    /// 都表示重试所有错误（空列表保留为向后兼容）。
    #[serde(default = "retry_all")]
    pub retry_on: Vec<RetryCondition>,
}

fn retry_all() -> Vec<RetryCondition> {
    vec![RetryCondition::Any]
}

impl RetryConfig {
    /// Returns true if the given error should trigger a retry according to
    /// `retry_on`. Containing `RetryCondition::Any` means "retry all errors".
    /// An empty `retry_on` is treated as "retry all" for backward compatibility.
    pub fn should_retry(&self, e: &RuneError) -> bool {
        // Backward compat: empty retry_on means "retry all" (same as [Any]).
        if self.retry_on.is_empty() || self.retry_on.contains(&RetryCondition::Any) {
            return true;
        }
        self.retry_on
            .iter()
            .any(|cond| matches!((cond, e), (RetryCondition::Timeout, RuneError::Timeout)))
    }
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
            StepKind::Map(mc) => {
                map.insert("type".into(), "map".into());
                let mv = serde_json::to_value(mc).map_err(serde::ser::Error::custom)?;
                if let serde_json::Value::Object(mm) = mv {
                    map.extend(mm);
                }
            }
            StepKind::Switch(sc) => {
                map.insert("type".into(), "switch".into());
                let sv = serde_json::to_value(sc).map_err(serde::ser::Error::custom)?;
                if let serde_json::Value::Object(sm) = sv {
                    map.extend(sm);
                }
            }
            StepKind::Flow(fc) => {
                map.insert("type".into(), "flow".into());
                map.insert("flow".into(), fc.flow.clone().into());
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
            "map" => {
                let mc: MapConfig = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
                StepKind::Map(mc)
            }
            "switch" => {
                let sc: SwitchConfig =
                    serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
                StepKind::Switch(sc)
            }
            "flow" => {
                let flow_name = v["flow"]
                    .as_str()
                    .ok_or("StepDefinition type=flow missing 'flow' field")?
                    .to_string();
                StepKind::Flow(FlowConfig { flow: flow_name })
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
            BodyStepKind::Map(mc) => {
                map.insert("type".into(), "map".into());
                let mv = serde_json::to_value(mc).map_err(serde::ser::Error::custom)?;
                if let serde_json::Value::Object(mm) = mv {
                    map.extend(mm);
                }
            }
            BodyStepKind::Switch(sc) => {
                map.insert("type".into(), "switch".into());
                let sv = serde_json::to_value(sc).map_err(serde::ser::Error::custom)?;
                if let serde_json::Value::Object(sm) = sv {
                    map.extend(sm);
                }
            }
            BodyStepKind::Flow(fc) => {
                map.insert("type".into(), "flow".into());
                map.insert("flow".into(), fc.flow.clone().into());
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
            "map" => {
                let mc: MapConfig = serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
                BodyStepKind::Map(mc)
            }
            "switch" => {
                let sc: SwitchConfig =
                    serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
                BodyStepKind::Switch(sc)
            }
            "flow" => {
                let flow_name = v["flow"]
                    .as_str()
                    .ok_or("BodyStep type=flow missing 'flow' field")?
                    .to_string();
                BodyStepKind::Flow(FlowConfig { flow: flow_name })
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
    #[error("reserved step name: '{0}' is reserved for flow input references")]
    ReservedStepName(String),
    #[error("multi-upstream step '{step}' must declare input_mapping")]
    MissingInputMapping { step: String },
    #[error("step '{step}' has unparseable condition: '{condition}' — use 'true', 'false', or a comparison with spaces around the operator (e.g. 'x == 5')")]
    InvalidCondition { step: String, condition: String },
    #[error("step '{step}' has invalid loop config: {reason}")]
    InvalidLoopConfig { step: String, reason: String },
    #[error("step '{step}' has invalid map config: {reason}")]
    InvalidMapConfig { step: String, reason: String },
    #[error("step '{step}' has invalid switch config: {reason}")]
    InvalidSwitchConfig { step: String, reason: String },
    #[error("step '{step}' has invalid flow config: {reason}")]
    InvalidFlowConfig { step: String, reason: String },
    #[error("step '{step}' has invalid retry config: {reason}")]
    InvalidRetryConfig { step: String, reason: String },
}

// ─── 验证 ─────────────────────────────────────────────────────────────────────

/// 验证 FlowDefinition，检查 DAG 有效性
pub fn validate_dag(flow: &FlowDefinition) -> Result<(), DagError> {
    let steps = &flow.steps;

    if steps.is_empty() {
        return Ok(());
    }

    // 1. 检查重复 step 名，以及保留名（input / $input 在 resolve_path 中有特殊语义）
    let mut seen = HashSet::new();
    for s in steps {
        if !seen.insert(&s.name) {
            return Err(DagError::DuplicateStep(s.name.clone()));
        }
        if s.name == "input" || s.name == "$input" {
            return Err(DagError::ReservedStepName(s.name.clone()));
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

    // 5. condition 语法预检 + loop/map/switch/flow config 验证 + retry 验证
    let operators = ["==", "!=", ">=", "<=", ">", "<"];
    for s in steps {
        validate_condition_syntax(&s.name, &s.condition, &operators)?;
        validate_retry_config(&s.name, &s.retry)?;
        match &s.kind {
            StepKind::Loop(lc) => validate_loop_config(&s.name, lc, &operators)?,
            StepKind::Map(mc) => validate_map_config(&s.name, mc, &operators)?,
            StepKind::Switch(sc) => validate_switch_config(&s.name, sc, &operators)?,
            StepKind::Flow(fc) => validate_flow_config(&s.name, fc)?,
            StepKind::Rune(_) => {}
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

fn validate_retry_config(step_name: &str, retry: &Option<RetryConfig>) -> Result<(), DagError> {
    if let Some(r) = retry {
        if r.max_attempts == 0 {
            return Err(DagError::InvalidRetryConfig {
                step: step_name.to_string(),
                reason: "max_attempts must be >= 1 (includes the initial attempt)".into(),
            });
        }
        // Empty retry_on is accepted for backward compatibility
        // (it is treated as [Any] by should_retry).
        if let Some(max_delay) = r.max_delay_ms {
            if max_delay < r.backoff_ms {
                return Err(DagError::InvalidRetryConfig {
                    step: step_name.to_string(),
                    reason: "max_delay_ms must be >= backoff_ms".into(),
                });
            }
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

    // body step condition 预检 + retry 验证 + 递归验证嵌套 loop/map/switch/flow
    let qualified = |bs_name: &str| format!("{step_name}/{bs_name}");
    for bs in &lc.body {
        validate_condition_syntax(&qualified(&bs.name), &bs.condition, operators)?;
        validate_retry_config(&qualified(&bs.name), &bs.retry)?;
        match &bs.kind {
            BodyStepKind::Loop(nested) => {
                validate_loop_config(&qualified(&bs.name), nested, operators)?
            }
            BodyStepKind::Map(nested) => {
                validate_map_config(&qualified(&bs.name), nested, operators)?
            }
            BodyStepKind::Switch(nested) => {
                validate_switch_config(&qualified(&bs.name), nested, operators)?
            }
            BodyStepKind::Flow(fc) => validate_flow_config(&qualified(&bs.name), fc)?,
            BodyStepKind::Rune(_) => {}
        }
    }

    // until condition 预检
    if lc.until.is_some() {
        validate_condition_syntax(step_name, &lc.until, operators)?;
    }

    Ok(())
}

fn validate_map_config(
    step_name: &str,
    mc: &MapConfig,
    operators: &[&str],
) -> Result<(), DagError> {
    if mc.over.trim().is_empty() {
        return Err(DagError::InvalidMapConfig {
            step: step_name.to_string(),
            reason: "'over' must not be empty".to_string(),
        });
    }
    if mc.concurrency == Some(0) {
        return Err(DagError::InvalidMapConfig {
            step: step_name.to_string(),
            reason: "'concurrency' must be >= 1".to_string(),
        });
    }
    // 递归验证 body
    let body_name = format!("{step_name}/{}", mc.body.name);
    validate_condition_syntax(&body_name, &mc.body.condition, operators)?;
    validate_retry_config(&body_name, &mc.body.retry)?;
    match &mc.body.kind {
        BodyStepKind::Loop(nested) => validate_loop_config(&body_name, nested, operators)?,
        BodyStepKind::Map(nested) => validate_map_config(&body_name, nested, operators)?,
        BodyStepKind::Switch(nested) => validate_switch_config(&body_name, nested, operators)?,
        BodyStepKind::Flow(fc) => validate_flow_config(&body_name, fc)?,
        BodyStepKind::Rune(_) => {}
    }
    Ok(())
}

fn validate_switch_config(
    step_name: &str,
    sc: &SwitchConfig,
    operators: &[&str],
) -> Result<(), DagError> {
    if sc.on.trim().is_empty() {
        return Err(DagError::InvalidSwitchConfig {
            step: step_name.to_string(),
            reason: "'on' must not be empty".to_string(),
        });
    }
    if sc.cases.is_empty() {
        return Err(DagError::InvalidSwitchConfig {
            step: step_name.to_string(),
            reason: "'cases' must not be empty".to_string(),
        });
    }
    // serde_json::Value 未实现 Hash/Ord，无法用 HashSet/BTreeSet；
    // switch cases 在实践中数量极少（通常 < 10），O(n²) 可接受。
    let mut seen_whens: Vec<&serde_json::Value> = Vec::new();
    for case in &sc.cases {
        if seen_whens.contains(&&case.when) {
            return Err(DagError::InvalidSwitchConfig {
                step: step_name.to_string(),
                reason: format!("duplicate 'when' value: {}", case.when),
            });
        }
        seen_whens.push(&case.when);
    }
    // 递归验证每个 case body 和 default
    let validate_body = |b: &BodyStep| -> Result<(), DagError> {
        let body_name = format!("{step_name}/{}", b.name);
        validate_condition_syntax(&body_name, &b.condition, operators)?;
        validate_retry_config(&body_name, &b.retry)?;
        match &b.kind {
            BodyStepKind::Loop(nested) => validate_loop_config(&body_name, nested, operators)?,
            BodyStepKind::Map(nested) => validate_map_config(&body_name, nested, operators)?,
            BodyStepKind::Switch(nested) => validate_switch_config(&body_name, nested, operators)?,
            BodyStepKind::Flow(fc) => validate_flow_config(&body_name, fc)?,
            BodyStepKind::Rune(_) => {}
        }
        Ok(())
    };
    for case in &sc.cases {
        validate_body(&case.body)?;
    }
    if let Some(default_body) = &sc.default {
        validate_body(default_body)?;
    }
    Ok(())
}

fn validate_flow_config(step_name: &str, fc: &FlowConfig) -> Result<(), DagError> {
    if fc.flow.trim().is_empty() {
        return Err(DagError::InvalidFlowConfig {
            step: step_name.to_string(),
            reason: "'flow' must not be empty".to_string(),
        });
    }
    // 拒绝带前后空格的名字：通过验证但运行时找不到 flow，会产生误导性错误。
    if fc.flow != fc.flow.trim() {
        return Err(DagError::InvalidFlowConfig {
            step: step_name.to_string(),
            reason: format!(
                "'flow' must not have leading/trailing whitespace: {:?}",
                fc.flow
            ),
        });
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_flow_with_retry(retry: Option<RetryConfig>) -> FlowDefinition {
        FlowDefinition {
            name: "test".into(),
            gate_path: None,
            steps: vec![StepDefinition {
                name: "s1".into(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry,
                kind: StepKind::Rune(RuneConfig {
                    rune: "my_rune".into(),
                }),
            }],
        }
    }

    /// max_delay_ms < backoff_ms 时 validate_dag 报 InvalidRetryConfig
    #[test]
    fn validate_max_delay_lt_backoff_fails() {
        let flow = make_flow_with_retry(Some(RetryConfig {
            max_attempts: 3,
            backoff_ms: 500,
            backoff_strategy: BackoffStrategy::Exponential,
            max_delay_ms: Some(200), // 200 < 500，非法
            retry_on: vec![RetryCondition::Any],
        }));
        let result = validate_dag(&flow);
        assert!(
            matches!(result, Err(DagError::InvalidRetryConfig { .. })),
            "expected InvalidRetryConfig, got {:?}",
            result
        );
    }

    /// max_delay_ms >= backoff_ms 时验证通过
    #[test]
    fn validate_max_delay_ge_backoff_ok() {
        let flow = make_flow_with_retry(Some(RetryConfig {
            max_attempts: 3,
            backoff_ms: 100,
            backoff_strategy: BackoffStrategy::Exponential,
            max_delay_ms: Some(1000),
            retry_on: vec![RetryCondition::Any],
        }));
        assert!(validate_dag(&flow).is_ok());
    }

    /// 旧格式 JSON（只有 max_attempts + backoff_ms）反序列化向后兼容
    #[test]
    fn retry_config_backward_compat_deserialize() {
        let json = r#"{"max_attempts": 3, "backoff_ms": 100}"#;
        let cfg: RetryConfig = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(cfg.max_attempts, 3);
        assert_eq!(cfg.backoff_ms, 100);
        assert_eq!(cfg.backoff_strategy, BackoffStrategy::Fixed);
        assert_eq!(cfg.max_delay_ms, None);
        assert_eq!(cfg.retry_on, vec![RetryCondition::Any]);
    }
}
