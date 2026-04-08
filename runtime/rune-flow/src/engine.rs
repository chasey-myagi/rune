use crate::dag::{topological_layers, validate_dag, DagError, FlowDefinition};
use bytes::Bytes;
use rune_core::relay::Relay;
use rune_core::resolver::Resolver;
use rune_core::rune::{RuneContext, RuneError};
use std::collections::HashMap;
use std::sync::Arc;

/// Step 执行状态
#[derive(Debug, Clone)]
pub enum StepStatus {
    Pending,
    Running,
    Completed { output: Bytes },
    Failed { error: String },
    Skipped,
}

/// Flow 执行结果
#[derive(Debug)]
pub struct FlowResult {
    pub output: Bytes,
    pub steps: HashMap<String, StepStatus>,
    pub steps_executed: usize,
}

/// Flow 执行错误
#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    #[error("flow not found: {0}")]
    FlowNotFound(String),
    #[error("step '{step}' failed: {source}")]
    StepFailed { step: String, source: RuneError },
    #[error("dag error: {0}")]
    DagError(#[from] DagError),
    #[error("no terminal step found")]
    NoTerminalStep,
    #[error("serialization failed: {0}")]
    SerializationFailed(String),
}

/// Default step timeout when not configured.
const DEFAULT_STEP_TIMEOUT_SECS: u64 = 30;

/// Lightweight handle for executing flows without holding the engine lock.
///
/// Created via [`FlowEngine::runner()`]. Clone the necessary `Arc` fields from
/// `FlowEngine`, then release the lock and run the (potentially long)
/// execution on this handle instead.
pub struct FlowRunner {
    relay: Arc<Relay>,
    resolver: Arc<dyn Resolver>,
    step_timeout: std::time::Duration,
}

#[allow(dead_code)]
pub struct FlowEngine {
    flows: HashMap<String, FlowDefinition>,
    relay: Arc<Relay>,
    resolver: Arc<dyn Resolver>,
    step_timeout: std::time::Duration,
}

impl FlowEngine {
    pub fn new(relay: Arc<Relay>, resolver: Arc<dyn Resolver>) -> Self {
        Self {
            flows: HashMap::new(),
            relay,
            resolver,
            step_timeout: std::time::Duration::from_secs(DEFAULT_STEP_TIMEOUT_SECS),
        }
    }

    /// Create a FlowEngine with a custom step timeout.
    pub fn with_timeout(
        relay: Arc<Relay>,
        resolver: Arc<dyn Resolver>,
        step_timeout: std::time::Duration,
    ) -> Self {
        Self {
            flows: HashMap::new(),
            relay,
            resolver,
            step_timeout,
        }
    }

    /// Create a lightweight runner that can execute flows without the engine lock.
    pub fn runner(&self) -> FlowRunner {
        FlowRunner {
            relay: Arc::clone(&self.relay),
            resolver: Arc::clone(&self.resolver),
            step_timeout: self.step_timeout,
        }
    }

    pub fn register(&mut self, flow: FlowDefinition) -> Result<(), FlowError> {
        validate_dag(&flow)?;
        self.flows.insert(flow.name.clone(), flow);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&FlowDefinition> {
        self.flows.get(name)
    }

    pub fn list(&self) -> Vec<&str> {
        self.flows.keys().map(|s| s.as_str()).collect()
    }

    pub fn remove(&mut self, name: &str) -> bool {
        self.flows.remove(name).is_some()
    }

    pub async fn execute(&self, flow_name: &str, input: Bytes) -> Result<FlowResult, FlowError> {
        let flow = self
            .flows
            .get(flow_name)
            .ok_or_else(|| FlowError::FlowNotFound(flow_name.to_string()))?
            .clone();
        self.execute_flow(&flow, input).await
    }

    /// Execute a pre-fetched FlowDefinition without looking it up from the
    /// internal registry.  Delegates to [`FlowRunner::execute_flow`].
    pub async fn execute_flow(
        &self,
        flow: &FlowDefinition,
        input: Bytes,
    ) -> Result<FlowResult, FlowError> {
        self.runner().execute_flow(flow, input).await
    }
}

impl FlowRunner {
    /// Execute a pre-fetched FlowDefinition.
    ///
    /// This is the core execution logic.  Because `FlowRunner` only holds
    /// cheap `Arc` clones, callers can drop the engine lock before calling
    /// this method.
    pub async fn execute_flow(
        &self,
        flow: &FlowDefinition,
        input: Bytes,
    ) -> Result<FlowResult, FlowError> {
        // 空 flow: passthrough
        if flow.steps.is_empty() {
            return Ok(FlowResult {
                output: input,
                steps: HashMap::new(),
                steps_executed: 0,
            });
        }

        let layers = topological_layers(flow)?;

        // step name → output bytes (完成后)
        let mut step_outputs: HashMap<String, Option<Bytes>> = HashMap::new();
        let mut step_statuses: HashMap<String, StepStatus> = HashMap::new();
        let mut steps_executed: usize = 0;

        // 解析 flow 原始输入为 JSON（用于 $input 引用和 condition 上下文）
        let flow_input_json: Option<serde_json::Value> = serde_json::from_slice(&input).ok();

        for layer in &layers {
            // 检查该层是否有上游失败
            let mut has_failed_upstream = false;
            for &step_idx in layer {
                let step_def = &flow.steps[step_idx];
                for dep in &step_def.depends_on {
                    if matches!(
                        step_statuses.get(dep.as_str()),
                        Some(StepStatus::Failed { .. })
                    ) {
                        has_failed_upstream = true;
                        break;
                    }
                }
                if has_failed_upstream {
                    break;
                }
            }

            if has_failed_upstream {
                // 上游失败，这层不执行
                break;
            }

            // 确定每个 step 是否 skip
            let mut step_tasks: Vec<(usize, bool)> = Vec::new(); // (step_idx, should_skip)

            for &step_idx in layer {
                let step_def = &flow.steps[step_idx];

                // 检查是否所有上游都被 skip
                let all_upstreams_skipped = if step_def.depends_on.is_empty() {
                    false
                } else {
                    step_def.depends_on.iter().all(|dep| {
                        matches!(step_statuses.get(dep.as_str()), Some(StepStatus::Skipped))
                    })
                };

                if all_upstreams_skipped {
                    step_tasks.push((step_idx, true));
                    continue;
                }

                // 评估 condition
                if let Some(condition) = &step_def.condition {
                    let cond_result = evaluate_condition(
                        condition,
                        &step_statuses,
                        &step_outputs,
                        &flow_input_json,
                    );
                    if !cond_result {
                        step_tasks.push((step_idx, true));
                        continue;
                    }
                }

                step_tasks.push((step_idx, false));
            }

            // 执行该层——使用 JoinSet 并行
            use tokio::task::JoinSet;
            let mut join_set: JoinSet<Result<(usize, Bytes), (usize, RuneError)>> = JoinSet::new();

            let mut skip_indices = Vec::new();

            for &(step_idx, should_skip) in &step_tasks {
                if should_skip {
                    skip_indices.push(step_idx);
                    continue;
                }

                let step_def = &flow.steps[step_idx];

                // 构造 step input
                let step_input =
                    Self::build_step_input(step_def, &input, &step_outputs, &flow_input_json)?;

                // Resolve rune invoker
                let invoker = self.relay.resolve(&step_def.rune, self.resolver.as_ref());

                let rune_name = step_def.rune.clone();
                let si = step_input;

                match invoker {
                    Some(inv) => {
                        let timeout = self.step_timeout;
                        join_set.spawn(async move {
                            let ctx = RuneContext {
                                rune_name: rune_name.clone(),
                                request_id: uuid_simple(),
                                context: Default::default(),
                                timeout,
                            };
                            match inv.invoke_once(ctx, si).await {
                                Ok(output) => Ok((step_idx, output)),
                                Err(e) => Err((step_idx, e)),
                            }
                        });
                    }
                    None => {
                        // Rune not found
                        join_set
                            .spawn(async move { Err((step_idx, RuneError::NotFound(rune_name))) });
                    }
                }
            }

            // 记录 skip 的 step
            for idx in &skip_indices {
                let step_def = &flow.steps[*idx];
                step_statuses.insert(step_def.name.clone(), StepStatus::Skipped);
                step_outputs.insert(step_def.name.clone(), None);
            }

            // 收集并行结果
            let mut layer_error: Option<(String, RuneError)> = None;

            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok((idx, output))) => {
                        let step_def = &flow.steps[idx];
                        step_statuses.insert(
                            step_def.name.clone(),
                            StepStatus::Completed {
                                output: output.clone(),
                            },
                        );
                        step_outputs.insert(step_def.name.clone(), Some(output));
                        steps_executed += 1;
                    }
                    Ok(Err((idx, rune_err))) => {
                        let step_def = &flow.steps[idx];
                        step_statuses.insert(
                            step_def.name.clone(),
                            StepStatus::Failed {
                                error: rune_err.to_string(),
                            },
                        );
                        if layer_error.is_none() {
                            layer_error = Some((step_def.name.clone(), rune_err));
                        }
                    }
                    Err(join_err) => {
                        // JoinError（不太可能发生）
                        tracing::error!("join error: {}", join_err);
                    }
                }
            }

            // 如果该层有失败 step，终止执行并返回错误
            if let Some((step_name, rune_err)) = layer_error {
                return Err(FlowError::StepFailed {
                    step: step_name,
                    source: rune_err,
                });
            }
        }

        // 确定最终输出：取最后一层中最后一个完成的 step 的 output
        let output = Self::determine_output(flow, &step_outputs, &input);

        Ok(FlowResult {
            output,
            steps: step_statuses,
            steps_executed,
        })
    }

    fn build_step_input(
        step_def: &crate::dag::StepDefinition,
        flow_input: &Bytes,
        step_outputs: &HashMap<String, Option<Bytes>>,
        flow_input_json: &Option<serde_json::Value>,
    ) -> Result<Bytes, FlowError> {
        if let Some(mapping) = &step_def.input_mapping {
            // 有 input_mapping，构造 JSON
            let mut result = serde_json::Map::new();
            for (key, path) in mapping {
                let value = resolve_path(path, step_outputs, flow_input_json);
                result.insert(key.clone(), value);
            }
            let bytes = serde_json::to_vec(&serde_json::Value::Object(result))
                .map_err(|e| FlowError::SerializationFailed(e.to_string()))?;
            Ok(Bytes::from(bytes))
        } else if step_def.depends_on.len() == 1 {
            // 单上游，传递上游 output
            let dep = &step_def.depends_on[0];
            Ok(match step_outputs.get(dep.as_str()) {
                Some(Some(output)) => output.clone(),
                Some(None) => Bytes::from("null"),
                None => flow_input.clone(),
            })
        } else {
            // 无依赖，传递 flow 原始输入
            Ok(flow_input.clone())
        }
    }

    fn determine_output(
        flow: &FlowDefinition,
        step_outputs: &HashMap<String, Option<Bytes>>,
        flow_input: &Bytes,
    ) -> Bytes {
        // 找到末端 step（没有下游的 step）
        let mut has_downstream: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for s in &flow.steps {
            for dep in &s.depends_on {
                has_downstream.insert(dep.as_str());
            }
        }

        let terminal_steps: Vec<&str> = flow
            .steps
            .iter()
            .filter(|s| !has_downstream.contains(s.name.as_str()))
            .map(|s| s.name.as_str())
            .collect();

        // 找最后一个有输出的末端 step
        for name in terminal_steps.iter().rev() {
            if let Some(Some(output)) = step_outputs.get(*name) {
                return output.clone();
            }
        }

        // 所有末端 step 都被 skip 或没有输出，返回 flow 输入
        flow_input.clone()
    }
}

/// 解析路径引用，返回 JSON Value
fn resolve_path(
    path: &str,
    step_outputs: &HashMap<String, Option<Bytes>>,
    flow_input_json: &Option<serde_json::Value>,
) -> serde_json::Value {
    if path.starts_with("$input") {
        // $input.field.subfield...
        let parts: Vec<&str> = path.splitn(2, '.').collect();
        if parts.len() < 2 {
            // just "$input"
            return flow_input_json.clone().unwrap_or(serde_json::Value::Null);
        }
        let field_path = parts[1]; // "field" or "field.subfield"
        if let Some(input_val) = flow_input_json {
            resolve_json_path(input_val, field_path)
        } else {
            serde_json::Value::Null
        }
    } else {
        // step_name.output or step_name.output.field
        let parts: Vec<&str> = path.splitn(3, '.').collect();
        if parts.is_empty() {
            return serde_json::Value::Null;
        }

        let step_name = parts[0];

        // 获取 step output
        let step_output = match step_outputs.get(step_name) {
            Some(Some(bytes)) => serde_json::from_slice::<serde_json::Value>(bytes)
                .unwrap_or(serde_json::Value::Null),
            Some(None) => serde_json::Value::Null, // skipped step
            None => serde_json::Value::Null,
        };

        if parts.len() == 1 {
            // just step_name
            return step_output;
        }

        if parts.len() == 2 && parts[1] == "output" {
            // step_name.output → 完整 output
            return step_output;
        }

        if parts.len() == 3 && parts[1] == "output" {
            // step_name.output.field
            return resolve_json_path(&step_output, parts[2]);
        }

        serde_json::Value::Null
    }
}

/// 在 JSON 值中按 dot-separated 路径取值
fn resolve_json_path(value: &serde_json::Value, path: &str) -> serde_json::Value {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in parts {
        match current.get(part) {
            Some(v) => current = v,
            None => return serde_json::Value::Null,
        }
    }
    current.clone()
}

/// 评估条件表达式
/// Evaluate a condition expression. Exported for testing; not part of stable API.
pub fn evaluate_condition(
    condition: &str,
    step_statuses: &HashMap<String, StepStatus>,
    step_outputs: &HashMap<String, Option<Bytes>>,
    flow_input_json: &Option<serde_json::Value>,
) -> bool {
    let trimmed = condition.trim();

    // 简单的字面量
    if trimmed == "true" {
        return true;
    }
    if trimmed == "false" {
        return false;
    }

    // 解析简单的比较表达式: lhs operator rhs
    // 支持: steps.X.output.field == value, steps.X.output.field > value 等
    if let Some(result) = evaluate_comparison(trimmed, step_statuses, step_outputs, flow_input_json)
    {
        return result;
    }

    // 无法解析时默认 false（不执行 step）——安全侧：条件未生效时不应静默执行
    tracing::warn!(condition = trimmed, "condition expression could not be parsed, defaulting to false (skip step) — operators must be separated by spaces (e.g. 'x == 5')");
    false
}

/// Evaluate a comparison expression. Exported for testing; not part of stable API.
pub fn evaluate_comparison(
    expr: &str,
    _step_statuses: &HashMap<String, StepStatus>,
    step_outputs: &HashMap<String, Option<Bytes>>,
    flow_input_json: &Option<serde_json::Value>,
) -> Option<bool> {
    // Token-based 解析：按空白 split 成 tokens，查找操作符 token
    // 这避免了 lhs/rhs 中包含操作符字符（如 steps.a>b.output.val）时的误匹配
    let operators = ["==", "!=", ">=", "<=", ">", "<"];

    let tokens: Vec<&str> = expr.split_whitespace().collect();

    // 在 tokens 中查找操作符 token
    for op in &operators {
        if let Some(op_idx) = tokens.iter().position(|t| *t == *op) {
            if op_idx == 0 || op_idx >= tokens.len() - 1 {
                // 操作符在开头或末尾，无效
                continue;
            }
            let lhs = tokens[..op_idx].join(" ");
            let rhs = tokens[op_idx + 1..].join(" ");

            if lhs.is_empty() || rhs.is_empty() {
                continue;
            }

            let lhs_val = resolve_condition_value(&lhs, step_outputs, flow_input_json);
            let rhs_val = parse_literal(&rhs);

            return Some(compare_values(&lhs_val, &rhs_val, op));
        }
    }

    None
}

fn resolve_condition_value(
    path: &str,
    step_outputs: &HashMap<String, Option<Bytes>>,
    flow_input_json: &Option<serde_json::Value>,
) -> serde_json::Value {
    if path.starts_with("$input.") || path == "$input" {
        return resolve_path(path, step_outputs, flow_input_json);
    }

    if path.starts_with("input.") {
        // "input.x" → flow 原始输入的 x 字段
        let field_path = &path["input.".len()..];
        if let Some(input_val) = flow_input_json {
            return resolve_json_path(input_val, field_path);
        }
        return serde_json::Value::Null;
    }

    if path.starts_with("steps.") {
        // "steps.A.output.field" → step A 的 output 的 field
        let rest = &path["steps.".len()..];
        // rest = "A.output.field"
        let parts: Vec<&str> = rest.splitn(3, '.').collect();
        if parts.is_empty() {
            return serde_json::Value::Null;
        }
        let step_name = parts[0];
        let step_output = match step_outputs.get(step_name) {
            Some(Some(bytes)) => serde_json::from_slice::<serde_json::Value>(bytes)
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        };

        if parts.len() == 1 {
            return step_output;
        }
        if parts.len() == 2 && parts[1] == "output" {
            return step_output;
        }
        if parts.len() == 3 && parts[1] == "output" {
            return resolve_json_path(&step_output, parts[2]);
        }
        return serde_json::Value::Null;
    }

    // 可能是字面量
    parse_literal(path)
}

fn parse_literal(s: &str) -> serde_json::Value {
    let s = s.trim();
    if s == "true" {
        return serde_json::Value::Bool(true);
    }
    if s == "false" {
        return serde_json::Value::Bool(false);
    }
    if s == "null" {
        return serde_json::Value::Null;
    }
    // 尝试数字
    if let Ok(n) = s.parse::<i64>() {
        return serde_json::Value::Number(n.into());
    }
    if let Ok(n) = s.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(n) {
            return serde_json::Value::Number(num);
        }
    }
    // 字符串（去除引号）
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        return serde_json::Value::String(s[1..s.len() - 1].to_string());
    }
    serde_json::Value::String(s.to_string())
}

fn compare_values(lhs: &serde_json::Value, rhs: &serde_json::Value, op: &str) -> bool {
    match op {
        "==" => lhs == rhs,
        "!=" => lhs != rhs,
        ">" => compare_numeric(lhs, rhs).map_or(false, |ord| ord == std::cmp::Ordering::Greater),
        "<" => compare_numeric(lhs, rhs).map_or(false, |ord| ord == std::cmp::Ordering::Less),
        ">=" => compare_numeric(lhs, rhs).map_or(false, |ord| ord != std::cmp::Ordering::Less),
        "<=" => compare_numeric(lhs, rhs).map_or(false, |ord| ord != std::cmp::Ordering::Greater),
        _ => false,
    }
}

fn compare_numeric(lhs: &serde_json::Value, rhs: &serde_json::Value) -> Option<std::cmp::Ordering> {
    let l = as_f64(lhs)?;
    let r = as_f64(rhs)?;
    l.partial_cmp(&r)
}

fn as_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

/// 生成简单的请求 ID（时间戳 + 单调计数器，保证并行 step 不重复）
fn uuid_simple() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = rune_core::time_utils::now_ms();
    format!("req-{:x}-{:x}", ts, seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    /// I-4 回归测试: build_step_input 带 input_mapping 时序列化不 panic，返回 Result
    #[test]
    fn test_fix_flow_output_serialization_no_panic() {
        let mut mapping = HashMap::new();
        mapping.insert("key1".to_string(), "$input.field1".to_string());
        mapping.insert("key2".to_string(), "$input.field2".to_string());

        let step_def = crate::dag::StepDefinition {
            name: "test_step".to_string(),
            rune: "test_rune".to_string(),
            depends_on: vec![],
            condition: None,
            input_mapping: Some(mapping),
        };

        let flow_input = Bytes::from(r#"{"field1": "hello", "field2": 42}"#);
        let step_outputs: HashMap<String, Option<Bytes>> = HashMap::new();
        let flow_input_json: Option<serde_json::Value> = serde_json::from_slice(&flow_input).ok();

        // 修复前这里用 unwrap()，修复后返回 Result
        let result =
            FlowRunner::build_step_input(&step_def, &flow_input, &step_outputs, &flow_input_json);
        assert!(result.is_ok(), "build_step_input should return Ok");

        let output = result.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(parsed["key1"], "hello");
        assert_eq!(parsed["key2"], 42);
    }

    /// I-4 回归测试: build_step_input 无 mapping 时仍正常返回
    #[test]
    fn test_fix_build_step_input_no_mapping() {
        let step_def = crate::dag::StepDefinition {
            name: "passthrough".to_string(),
            rune: "test_rune".to_string(),
            depends_on: vec![],
            condition: None,
            input_mapping: None,
        };

        let flow_input = Bytes::from(r#"{"data": "test"}"#);
        let step_outputs: HashMap<String, Option<Bytes>> = HashMap::new();
        let flow_input_json: Option<serde_json::Value> = serde_json::from_slice(&flow_input).ok();

        let result =
            FlowRunner::build_step_input(&step_def, &flow_input, &step_outputs, &flow_input_json);
        assert!(result.is_ok());
        // 无依赖无 mapping，应该 passthrough flow input
        assert_eq!(result.unwrap(), flow_input);
    }
}
