use crate::dag::{
    topological_layers, validate_dag, BodyStep, BodyStepKind, DagError, FlowConfig, FlowDefinition,
    LoopConfig, MapConfig, StepKind,
};
use bytes::Bytes;
use rune_core::relay::Relay;
use rune_core::resolver::Resolver;
use rune_core::rune::{RuneContext, RuneError};
use rune_core::trace;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

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
    #[error("map step: 'over' path '{0}' did not resolve to a JSON array")]
    MapItemsNotArray(String),
    #[error("flow step references unknown flow: '{0}'")]
    FlowStepNotFound(String),
    #[error("circular flow reference detected: {0}")]
    CircularFlowRef(String),
}

/// Default step timeout when not configured.
const DEFAULT_STEP_TIMEOUT_SECS: u64 = 30;

/// Lightweight handle for executing flows without holding the engine lock.
///
/// Created via [`FlowEngine::runner()`]. Clone the necessary `Arc` fields from
/// `FlowEngine`, then release the lock and run the (potentially long)
/// execution on this handle instead.
#[derive(Clone)]
pub struct FlowRunner {
    relay: Arc<Relay>,
    resolver: Arc<dyn Resolver>,
    step_timeout: Duration,
    /// 创建 runner 时的 Flow 快照，供 Flow Step 子流查找（无锁）
    flows_snapshot: Arc<HashMap<String, FlowDefinition>>,
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
            flows_snapshot: Arc::new(self.flows.clone()),
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

    /// Check if any existing flow (other than `exclude_name`) uses the given gate_path.
    /// Returns the name of the conflicting flow, if any.
    pub fn find_by_gate_path(&self, gate_path: &str, exclude_name: &str) -> Option<&str> {
        self.flows.values().find_map(|f| {
            if f.name != exclude_name {
                if let Some(ref gp) = f.gate_path {
                    if gp == gate_path {
                        return Some(f.name.as_str());
                    }
                }
            }
            None
        })
    }

    pub fn remove(&mut self, name: &str) -> bool {
        self.flows.remove(name).is_some()
    }

    pub async fn execute(&self, flow_name: &str, input: Bytes) -> Result<FlowResult, FlowError> {
        self.execute_with_context(flow_name, input, HashMap::new(), None)
            .await
    }

    pub async fn execute_with_context(
        &self,
        flow_name: &str,
        input: Bytes,
        parent_context: HashMap<String, String>,
        flow_request_id: Option<String>,
    ) -> Result<FlowResult, FlowError> {
        let flow = self
            .flows
            .get(flow_name)
            .ok_or_else(|| FlowError::FlowNotFound(flow_name.to_string()))?
            .clone();
        self.execute_flow_with_context(&flow, input, parent_context, flow_request_id)
            .await
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

    pub async fn execute_flow_with_context(
        &self,
        flow: &FlowDefinition,
        input: Bytes,
        parent_context: HashMap<String, String>,
        flow_request_id: Option<String>,
    ) -> Result<FlowResult, FlowError> {
        self.runner()
            .execute_flow_with_context(flow, input, parent_context, flow_request_id)
            .await
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
        self.execute_flow_with_context(flow, input, HashMap::new(), None)
            .await
    }

    pub async fn execute_flow_with_context(
        &self,
        flow: &FlowDefinition,
        input: Bytes,
        parent_context: HashMap<String, String>,
        flow_request_id: Option<String>,
    ) -> Result<FlowResult, FlowError> {
        self.execute_flow_internal(
            flow,
            input,
            vec![flow.name.clone()],
            parent_context,
            flow_request_id,
        )
        .await
    }

    async fn execute_flow_internal(
        &self,
        flow: &FlowDefinition,
        input: Bytes,
        call_stack: Vec<String>,
        mut parent_context: HashMap<String, String>,
        flow_request_id: Option<String>,
    ) -> Result<FlowResult, FlowError> {
        // 空 flow: passthrough
        if flow.steps.is_empty() {
            return Ok(FlowResult {
                output: input,
                steps: HashMap::new(),
                steps_executed: 0,
            });
        }

        trace::ensure_trace_defaults(&mut parent_context);
        let flow_request_id =
            flow_request_id.unwrap_or_else(rune_core::time_utils::unique_request_id);
        let flow_span_id = parent_context
            .get(trace::SPAN_ID_KEY)
            .cloned()
            .unwrap_or_else(rune_core::time_utils::generate_span_id);

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

                let timeout = step_def
                    .timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(self.step_timeout);
                let si = step_input;

                match &step_def.kind {
                    StepKind::Rune(rc) => {
                        let invoker = self.relay.resolve(&rc.rune, self.resolver.as_ref());
                        let rune_name = rc.rune.clone();
                        match invoker {
                            Some(inv) => {
                                let parent_context = parent_context.clone();
                                let flow_request_id = flow_request_id.clone();
                                let flow_span_id = flow_span_id.clone();
                                join_set.spawn(async move {
                                    let mut step_context = parent_context;
                                    step_context.insert(
                                        trace::PARENT_REQUEST_ID_KEY.to_string(),
                                        flow_request_id,
                                    );
                                    step_context.insert(
                                        trace::PARENT_SPAN_ID_KEY.to_string(),
                                        flow_span_id,
                                    );
                                    step_context.insert(
                                        trace::SPAN_ID_KEY.to_string(),
                                        rune_core::time_utils::generate_span_id(),
                                    );
                                    let ctx = RuneContext {
                                        rune_name: rune_name.clone(),
                                        request_id: uuid_simple(),
                                        context: step_context,
                                        timeout,
                                    };
                                    // engine 层强制 hard deadline（LocalInvoker 无内置 timeout）。
                                    // RemoteInvoker 经由 RuneContext.timeout 在 session 层同时
                                    // 发起 cancel，两者使用相同值，互补不冲突。
                                    match tokio::time::timeout(timeout, inv.invoke_once(ctx, si))
                                        .await
                                    {
                                        Ok(Ok(output)) => Ok((step_idx, output)),
                                        Ok(Err(e)) => Err((step_idx, e)),
                                        Err(_) => Err((step_idx, RuneError::Timeout)),
                                    }
                                });
                            }
                            None => {
                                join_set.spawn(async move {
                                    Err((step_idx, RuneError::NotFound(rune_name)))
                                });
                            }
                        }
                    }
                    StepKind::Loop(lc) => {
                        let runner = self.clone();
                        let lc = lc.clone();
                        let parent_context = parent_context.clone();
                        let flow_request_id = flow_request_id.clone();
                        let call_stack = call_stack.clone();
                        join_set.spawn(async move {
                            let result = tokio::time::timeout(
                                timeout,
                                runner.execute_loop(
                                    &lc,
                                    si,
                                    parent_context,
                                    flow_request_id,
                                    call_stack,
                                ),
                            )
                            .await;
                            match result {
                                Ok(Ok(out)) => Ok((step_idx, out)),
                                Ok(Err(e)) => Err((
                                    step_idx,
                                    RuneError::ExecutionFailed {
                                        code: "LOOP_FAILED".into(),
                                        message: e.to_string(),
                                    },
                                )),
                                Err(_) => Err((step_idx, RuneError::Timeout)),
                            }
                        });
                    }
                    StepKind::Map(mc) => {
                        let runner = self.clone();
                        let mc = mc.clone();
                        let step_outputs_snapshot = step_outputs.clone();
                        let parent_context = parent_context.clone();
                        let flow_request_id = flow_request_id.clone();
                        let call_stack = call_stack.clone();
                        join_set.spawn(async move {
                            let result = tokio::time::timeout(
                                timeout,
                                runner.execute_map(
                                    &mc,
                                    si,
                                    &step_outputs_snapshot,
                                    parent_context,
                                    flow_request_id,
                                    call_stack,
                                ),
                            )
                            .await;
                            match result {
                                Ok(Ok(out)) => Ok((step_idx, out)),
                                Ok(Err(e)) => Err((
                                    step_idx,
                                    RuneError::ExecutionFailed {
                                        code: "MAP_FAILED".into(),
                                        message: e.to_string(),
                                    },
                                )),
                                Err(_) => Err((step_idx, RuneError::Timeout)),
                            }
                        });
                    }
                    StepKind::Switch(sc) => {
                        // on 表达式在当前 step_outputs 和 flow_input_json 上下文中求值（同步）
                        let on_val =
                            resolve_condition_value(&sc.on, &step_outputs, &flow_input_json);
                        let matched: Option<BodyStep> = sc
                            .cases
                            .iter()
                            .find(|c| c.when == on_val)
                            .map(|c| c.body.clone())
                            .or_else(|| sc.default.as_deref().cloned());

                        match matched {
                            None => {
                                // 无匹配且无 default → Skipped
                                skip_indices.push(step_idx);
                            }
                            Some(body) => {
                                let si_json: Option<serde_json::Value> =
                                    serde_json::from_slice(&si).ok();
                                let body_input = Self::build_body_step_input(
                                    &body,
                                    &si,
                                    &HashMap::new(),
                                    &si_json,
                                )?;
                                let runner = self.clone();
                                let parent_context = parent_context.clone();
                                let flow_request_id = flow_request_id.clone();
                                let call_stack = call_stack.clone();
                                join_set.spawn(async move {
                                    // body.timeout_ms 覆盖 step 级别 timeout；
                                    // 对内（execute_body_step_kind）和外（hard deadline）使用同一值。
                                    let effective_timeout = body
                                        .timeout_ms
                                        .map(Duration::from_millis)
                                        .unwrap_or(timeout);
                                    let body_future = runner.execute_body_step_kind(
                                        body.kind,
                                        body_input,
                                        effective_timeout,
                                        parent_context,
                                        flow_request_id,
                                        call_stack,
                                    );
                                    match tokio::time::timeout(effective_timeout, body_future).await
                                    {
                                        Ok(Ok(out)) => Ok((step_idx, out)),
                                        Ok(Err(e)) => Err((
                                            step_idx,
                                            RuneError::ExecutionFailed {
                                                code: "SWITCH_CASE_FAILED".into(),
                                                message: e.to_string(),
                                            },
                                        )),
                                        Err(_) => Err((step_idx, RuneError::Timeout)),
                                    }
                                });
                            }
                        }
                    }
                    StepKind::Flow(fc) => {
                        let runner = self.clone();
                        let fc = fc.clone();
                        let parent_context = parent_context.clone();
                        let flow_request_id = flow_request_id.clone();
                        let call_stack = call_stack.clone();
                        join_set.spawn(async move {
                            match tokio::time::timeout(
                                timeout,
                                runner.execute_flow_step(
                                    fc,
                                    si,
                                    call_stack,
                                    parent_context,
                                    flow_request_id,
                                ),
                            )
                            .await
                            {
                                Ok(Ok(out)) => Ok((step_idx, out)),
                                Ok(Err(e)) => Err((
                                    step_idx,
                                    RuneError::ExecutionFailed {
                                        code: "FLOW_STEP_FAILED".into(),
                                        message: e.to_string(),
                                    },
                                )),
                                Err(_) => Err((step_idx, RuneError::Timeout)),
                            }
                        });
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

    // ─── Loop 执行 ───────────────────────────────────────────────────────────

    async fn execute_loop(
        &self,
        config: &LoopConfig,
        input: Bytes,
        parent_context: HashMap<String, String>,
        flow_request_id: String,
        call_stack: Vec<String>,
    ) -> Result<Bytes, FlowError> {
        let mut current = input;

        for _iter in 0..config.max_iterations {
            let mut body_outputs: HashMap<String, Option<Bytes>> = HashMap::new();
            let iter_input_json: Option<serde_json::Value> = serde_json::from_slice(&current).ok();

            for body_step in &config.body {
                // condition 评估（引用本轮 body_outputs）
                if let Some(cond) = &body_step.condition {
                    if !evaluate_condition(cond, &HashMap::new(), &body_outputs, &iter_input_json) {
                        body_outputs.insert(body_step.name.clone(), None); // skipped
                        continue;
                    }
                }

                let step_input = Self::build_body_step_input(
                    body_step,
                    &current,
                    &body_outputs,
                    &iter_input_json,
                )?;

                let effective_timeout = body_step
                    .timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(self.step_timeout);

                let body_future = self.execute_body_step_kind(
                    body_step.kind.clone(),
                    step_input,
                    effective_timeout,
                    parent_context.clone(),
                    flow_request_id.clone(),
                    call_stack.clone(),
                );

                let out = match tokio::time::timeout(effective_timeout, body_future).await {
                    Ok(Ok(o)) => o,
                    Ok(Err(e)) => {
                        return Err(FlowError::StepFailed {
                            step: body_step.name.clone(),
                            source: e,
                        })
                    }
                    Err(_) => {
                        return Err(FlowError::StepFailed {
                            step: body_step.name.clone(),
                            source: RuneError::Timeout,
                        })
                    }
                };

                body_outputs.insert(body_step.name.clone(), Some(out));
            }

            // 取本轮最后一个非 skip 的 body step 输出，作为下一轮 current
            // 必须按 body 顺序（而非 HashMap 顺序）反向查找，以保证语义正确
            let last = config
                .body
                .iter()
                .rev()
                .find_map(|s| body_outputs.get(&s.name).and_then(|v| v.as_ref()))
                .cloned()
                .unwrap_or_else(|| current.clone());

            // until 条件检查
            if let Some(until) = &config.until {
                let last_json: Option<serde_json::Value> = serde_json::from_slice(&last).ok();
                if evaluate_condition(until, &HashMap::new(), &body_outputs, &last_json) {
                    return Ok(last);
                }
            }

            current = last;
        }

        Ok(current)
    }

    // ─── Map 执行 ────────────────────────────────────────────────────────────

    async fn execute_map(
        &self,
        config: &MapConfig,
        input: Bytes,
        step_outputs: &HashMap<String, Option<Bytes>>,
        parent_context: HashMap<String, String>,
        flow_request_id: String,
        call_stack: Vec<String>,
    ) -> Result<Bytes, FlowError> {
        use std::sync::Arc;
        use tokio::sync::Semaphore;
        use tokio::task::JoinSet;

        // 解析 over 路径。$input.* 引用 step 自身入参；steps.X.* 引用前序步骤输出。
        let input_json: Option<serde_json::Value> = serde_json::from_slice(&input).ok();
        let items_val = resolve_condition_value(&config.over, step_outputs, &input_json);

        // move out of items_val — 避免 .as_array().clone() 的不必要拷贝
        let items = match items_val {
            serde_json::Value::Array(arr) => arr,
            _ => return Err(FlowError::MapItemsNotArray(config.over.clone())),
        };

        if items.is_empty() {
            let empty: &[serde_json::Value] = &[];
            return Ok(Bytes::from(
                serde_json::to_vec(empty)
                    .map_err(|e| FlowError::SerializationFailed(e.to_string()))?,
            ));
        }

        let n_items = items.len();
        let concurrency = config.concurrency.unwrap_or(n_items).max(1);
        let sem = Arc::new(Semaphore::new(concurrency));

        let mut join_set: JoinSet<Result<(usize, Bytes), (usize, FlowError)>> = JoinSet::new();

        for (i, item) in items.into_iter().enumerate() {
            let runner = self.clone();
            let body = *config.body.clone();
            let item_bytes = Bytes::from(
                serde_json::to_vec(&item)
                    .map_err(|e| FlowError::SerializationFailed(e.to_string()))?,
            );
            let sem = Arc::clone(&sem);
            let parent_context = parent_context.clone();
            let flow_request_id = flow_request_id.clone();
            let call_stack = call_stack.clone();

            join_set.spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();

                let effective_timeout = body
                    .timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(runner.step_timeout);

                let body_future = runner.execute_body_step_kind(
                    body.kind,
                    item_bytes,
                    effective_timeout,
                    parent_context,
                    flow_request_id,
                    call_stack,
                );

                let result = tokio::time::timeout(effective_timeout, body_future).await;
                match result {
                    Ok(Ok(out)) => Ok((i, out)),
                    Ok(Err(e)) => Err((
                        i,
                        FlowError::StepFailed {
                            step: format!("map[{i}]"),
                            source: e,
                        },
                    )),
                    Err(_) => Err((
                        i,
                        FlowError::StepFailed {
                            step: format!("map[{i}]"),
                            source: RuneError::Timeout,
                        },
                    )),
                }
            });
        }

        // 收集结果，fail-fast：任意子任务失败或 panic 均中止整批
        let mut results: Vec<Option<serde_json::Value>> = vec![None; n_items];
        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok(Ok((i, out))) => {
                    let val: serde_json::Value =
                        serde_json::from_slice(&out).unwrap_or(serde_json::Value::Null);
                    results[i] = Some(val);
                }
                Ok(Err((_, e))) => {
                    join_set.abort_all();
                    return Err(e);
                }
                Err(join_err) => {
                    // task panic 或被 abort — 不能静默变 null，必须 fail-fast
                    join_set.abort_all();
                    return Err(FlowError::SerializationFailed(format!(
                        "map task panicked or aborted: {join_err}"
                    )));
                }
            }
        }

        let output: Vec<serde_json::Value> = results
            .into_iter()
            .map(|v| v.unwrap_or(serde_json::Value::Null))
            .collect();
        let bytes = serde_json::to_vec(&output)
            .map_err(|e| FlowError::SerializationFailed(e.to_string()))?;
        Ok(Bytes::from(bytes))
    }

    /// 执行单个 BodyStep 的 kind（不含 timeout，调用方负责包裹）。
    /// 返回 Pin<Box<...>> 以支持 BodyStepKind::Loop 的异步递归。
    fn execute_body_step_kind(
        &self,
        kind: BodyStepKind,
        input: Bytes,
        effective_timeout: Duration,
        parent_context: HashMap<String, String>,
        flow_request_id: String,
        call_stack: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, RuneError>> + Send>> {
        let runner = self.clone();
        Box::pin(async move {
            match kind {
                BodyStepKind::Rune(rc) => {
                    let invoker = runner.relay.resolve(&rc.rune, runner.resolver.as_ref());
                    match invoker {
                        Some(inv) => {
                            let mut step_context = parent_context;
                            step_context
                                .insert(trace::PARENT_REQUEST_ID_KEY.to_string(), flow_request_id);
                            step_context.insert(
                                trace::SPAN_ID_KEY.to_string(),
                                rune_core::time_utils::generate_span_id(),
                            );
                            let ctx = RuneContext {
                                rune_name: rc.rune.clone(),
                                request_id: uuid_simple(),
                                context: step_context,
                                // RemoteInvoker 经由 RuneContext.timeout 在 session 层发起
                                // cancel；外层 execute_loop 的 tokio::time::timeout 是
                                // LocalInvoker 的 hard deadline。两者使用相同值，互补不冲突。
                                timeout: effective_timeout,
                            };
                            inv.invoke_once(ctx, input).await
                        }
                        None => Err(RuneError::NotFound(rc.rune)),
                    }
                }
                BodyStepKind::Loop(lc) => runner
                    .execute_loop(&lc, input, parent_context, flow_request_id, call_stack)
                    .await
                    .map_err(|e| RuneError::ExecutionFailed {
                        code: "LOOP_FAILED".into(),
                        message: e.to_string(),
                    }),
                BodyStepKind::Map(mc) => runner
                    // body step 内 Map 的 over 只能引用 $input.*（当前元素），
                    // 不能引用外层 flow 的步骤输出，故传空 step_outputs。
                    .execute_map(
                        &mc,
                        input,
                        &HashMap::new(),
                        parent_context,
                        flow_request_id,
                        call_stack,
                    )
                    .await
                    .map_err(|e| RuneError::ExecutionFailed {
                        code: "MAP_FAILED".into(),
                        message: e.to_string(),
                    }),
                BodyStepKind::Switch(sc) => {
                    // body step 内 Switch 同理：on 只能引用 $input.*
                    let input_json: Option<serde_json::Value> = serde_json::from_slice(&input).ok();
                    let on_val = resolve_condition_value(&sc.on, &HashMap::new(), &input_json);
                    let matched: Option<BodyStep> = sc
                        .cases
                        .into_iter()
                        .find(|c| c.when == on_val)
                        .map(|c| c.body)
                        .or_else(|| sc.default.map(|b| *b));

                    match matched {
                        None => Ok(Bytes::copy_from_slice(b"null")),
                        Some(body) => {
                            // Switch body 是孤立执行单元，没有前序 body 输出可引用
                            let body_input = FlowRunner::build_body_step_input(
                                &body,
                                &input,
                                &HashMap::new(),
                                &input_json,
                            )
                            .map_err(|e| {
                                RuneError::ExecutionFailed {
                                    code: "SWITCH_INPUT_MAPPING_FAILED".into(),
                                    message: e.to_string(),
                                }
                            })?;
                            runner
                                .execute_body_step_kind(
                                    body.kind,
                                    body_input,
                                    effective_timeout,
                                    parent_context,
                                    flow_request_id,
                                    call_stack,
                                )
                                .await
                        }
                    }
                }
                BodyStepKind::Flow(fc) => runner
                    .execute_flow_step(fc, input, call_stack, parent_context, flow_request_id)
                    .await
                    .map_err(|e| RuneError::ExecutionFailed {
                        code: "FLOW_STEP_FAILED".into(),
                        message: e.to_string(),
                    }),
            }
        })
    }

    /// 执行 Flow Step（调用子流）。
    ///
    /// 返回 `Pin<Box<dyn Future + Send>>` 而非 `async fn`：这是打破 execute_flow_step →
    /// execute_flow_internal → execute_flow_step 间接递归循环的必要手段——否则编译器无法
    /// 确定 future 类型大小，JoinSet::spawn 会因 not-Send 报错。
    fn execute_flow_step(
        &self,
        config: FlowConfig,
        input: Bytes,
        call_stack: Vec<String>,
        parent_context: HashMap<String, String>,
        flow_request_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes, FlowError>> + Send>> {
        let runner = self.clone();
        Box::pin(async move {
            // 用 HashSet 做 O(1) 成员检测；call_stack Vec 保留用于生成错误信息里的调用链。
            let visited: std::collections::HashSet<&str> =
                call_stack.iter().map(String::as_str).collect();
            if visited.contains(config.flow.as_str()) {
                let mut chain = call_stack;
                chain.push(config.flow.clone());
                return Err(FlowError::CircularFlowRef(chain.join(" -> ")));
            }
            let flow = runner
                .flows_snapshot
                .get(&config.flow)
                .ok_or_else(|| FlowError::FlowStepNotFound(config.flow.clone()))?
                .clone();
            let mut new_stack = call_stack;
            new_stack.push(config.flow.clone());
            let result = runner
                .execute_flow_internal(
                    &flow,
                    input,
                    new_stack,
                    parent_context,
                    Some(flow_request_id),
                )
                .await?;
            Ok(result.output)
        })
    }

    fn build_body_step_input(
        body_step: &BodyStep,
        // `iter_input` = 本轮迭代的起始输入（每轮迭代后更新）
        iter_input: &Bytes,
        body_outputs: &HashMap<String, Option<Bytes>>,
        iter_input_json: &Option<serde_json::Value>,
    ) -> Result<Bytes, FlowError> {
        if let Some(mapping) = &body_step.input_mapping {
            let mut result = serde_json::Map::new();
            for (key, path) in mapping {
                let value = resolve_condition_value(path, body_outputs, iter_input_json);
                result.insert(key.clone(), value);
            }
            let bytes = serde_json::to_vec(&serde_json::Value::Object(result))
                .map_err(|e| FlowError::SerializationFailed(e.to_string()))?;
            Ok(Bytes::from(bytes))
        } else {
            Ok(iter_input.clone())
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

    if let Some(field_path) = path.strip_prefix("input.") {
        // "input.x" → flow 原始输入的 x 字段
        if let Some(input_val) = flow_input_json {
            return resolve_json_path(input_val, field_path);
        }
        return serde_json::Value::Null;
    }

    if let Some(rest) = path.strip_prefix("steps.") {
        // "steps.A.output.field" → step A 的 output 的 field
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
        ">" => compare_numeric(lhs, rhs).is_some_and(|ord| ord == std::cmp::Ordering::Greater),
        "<" => compare_numeric(lhs, rhs).is_some_and(|ord| ord == std::cmp::Ordering::Less),
        ">=" => compare_numeric(lhs, rhs).is_some_and(|ord| ord != std::cmp::Ordering::Less),
        "<=" => compare_numeric(lhs, rhs).is_some_and(|ord| ord != std::cmp::Ordering::Greater),
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
    rune_core::time_utils::unique_request_id()
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
            depends_on: vec![],
            condition: None,
            input_mapping: Some(mapping),
            timeout_ms: None,
            retry: None,
            kind: crate::dag::StepKind::Rune(crate::dag::RuneConfig {
                rune: "test_rune".to_string(),
            }),
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
            depends_on: vec![],
            condition: None,
            input_mapping: None,
            timeout_ms: None,
            retry: None,
            kind: crate::dag::StepKind::Rune(crate::dag::RuneConfig {
                rune: "test_rune".to_string(),
            }),
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
