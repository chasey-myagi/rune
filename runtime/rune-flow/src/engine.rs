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

/// Loop 执行进度事件，通过可选的 progress_tx channel 推送（非 SSE 路径零开销）
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum FlowProgressEvent {
    LoopIterationStart {
        step: String,
        iteration: u32,
        max_iterations: u32,
    },
    BodyStepDone {
        step: String,
        body_step: String,
        duration_ms: u64,
    },
    BodyStepSkipped {
        step: String,
        body_step: String,
    },
    BodyStepFailed {
        step: String,
        body_step: String,
        error: String,
        duration_ms: u64,
    },
    LoopIterationEnd {
        step: String,
        iteration: u32,
        duration_ms: u64,
        until_met: bool,
    },
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
    pub progress_tx: Option<tokio::sync::mpsc::Sender<FlowProgressEvent>>,
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
            progress_tx: None,
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
    /// 设置 progress channel；Loop 执行时会向该 channel 推送迭代事件。
    /// 失败（满/关闭）时静默丢弃，确保主执行路径不受影响。
    pub fn with_progress(mut self, tx: tokio::sync::mpsc::Sender<FlowProgressEvent>) -> Self {
        self.progress_tx = Some(tx);
        self
    }

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
        let mut first_error: Option<FlowError> = None;
        let flow_start = std::time::Instant::now();

        tracing::debug!(
            flow = %flow.name,
            request_id = %flow_request_id,
            steps = flow.steps.len(),
            layers = layers.len(),
            "flow execution started"
        );

        for layer in &layers {
            // 确定每个 step 是否 skip
            let mut step_tasks: Vec<(usize, bool)> = Vec::new(); // (step_idx, should_skip)

            for &step_idx in layer {
                let step_def = &flow.steps[step_idx];

                // 检查是否有失败的上游：失败向下游传播，该 step 标记为 Failed 并跳过；
                // 仅跳过有失败依赖的 step，不影响同层其他独立 step（修正层级粒度 fail-fast bug）
                let failed_dep = step_def.depends_on.iter().find_map(|dep| {
                    match step_statuses.get(dep.as_str()) {
                        Some(StepStatus::Failed { error }) => Some((dep.clone(), error.clone())),
                        _ => None,
                    }
                });
                if let Some((dep_name, dep_err)) = failed_dep {
                    step_statuses.insert(
                        step_def.name.clone(),
                        StepStatus::Failed {
                            error: format!("upstream '{}' failed: {}", dep_name, dep_err),
                        },
                    );
                    continue;
                }

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
                                let retry = step_def.retry.clone();
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
                                    // timeout 包裹整个 retry 序列：单次调用 + backoff + 重试，
                                    // 超出预算时整体失败，避免 retry 把 timeout 变相放大。
                                    match tokio::time::timeout(
                                        timeout,
                                        invoke_with_retry(inv, ctx, si, retry.as_ref()),
                                    )
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
                        let loop_step_name = step_def.name.clone();
                        let retry = step_def.retry.clone();
                        join_set.spawn(async move {
                            use crate::dag::RetryCondition;
                            let default_config = crate::dag::RetryConfig {
                                max_attempts: 1,
                                backoff_ms: 0,
                                backoff_strategy: crate::dag::BackoffStrategy::Fixed,
                                max_delay_ms: None,
                                retry_on: vec![],
                            };
                            let config = retry.as_ref().unwrap_or(&default_config);
                            let should_retry = |e: &RuneError| -> bool {
                                if config.retry_on.is_empty()
                                    || config.retry_on.contains(&RetryCondition::Any)
                                {
                                    return true;
                                }
                                config.retry_on.iter().any(|cond| {
                                    matches!(
                                        (cond, e),
                                        (RetryCondition::Timeout, RuneError::Timeout)
                                    )
                                })
                            };
                            retry_op(config, should_retry, || {
                                let runner = runner.clone();
                                let lc = lc.clone();
                                let si = si.clone();
                                let parent_context = parent_context.clone();
                                let flow_request_id = flow_request_id.clone();
                                let call_stack = call_stack.clone();
                                let loop_step_name = loop_step_name.clone();
                                async move {
                                    match tokio::time::timeout(
                                        timeout,
                                        runner.execute_loop(
                                            &lc,
                                            si,
                                            parent_context,
                                            flow_request_id,
                                            call_stack,
                                            loop_step_name,
                                        ),
                                    )
                                    .await
                                    {
                                        Ok(Ok(out)) => Ok(out),
                                        Ok(Err(e)) => Err(RuneError::ExecutionFailed {
                                            code: "LOOP_FAILED".into(),
                                            message: e.to_string(),
                                        }),
                                        Err(_) => Err(RuneError::Timeout),
                                    }
                                }
                            })
                            .await
                            .map(|out| (step_idx, out))
                            .map_err(|e| (step_idx, e))
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
                                        body.name,
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
                        tracing::debug!(
                            flow = %flow.name,
                            step = %step_def.name,
                            output_bytes = output.len(),
                            "step completed"
                        );
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
                        tracing::warn!(
                            flow = %flow.name,
                            step = %step_def.name,
                            error = %rune_err,
                            "step failed"
                        );
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

            // 记录首个失败（不立即返回，允许独立分支继续执行）
            if let Some((step_name, rune_err)) = layer_error {
                if first_error.is_none() {
                    first_error = Some(FlowError::StepFailed {
                        step: step_name,
                        source: rune_err,
                    });
                }
            }
        }

        // 确定最终输出：取最后一层中最后一个完成的 step 的 output
        let output = Self::determine_output(flow, &step_outputs, &input);

        // 如果任意 terminal step（无后继的 step）成功完成，视为整体成功（部分执行）；
        // 否则以第一个实际失败的 step 为错误返回。
        // 使用 step_statuses 而非字节比较，避免 passthrough/echo flow 被误判为失败。
        let all_deps: std::collections::HashSet<&str> = flow
            .steps
            .iter()
            .flat_map(|s| s.depends_on.iter().map(String::as_str))
            .collect();
        let terminal_succeeded = flow
            .steps
            .iter()
            .filter(|s| !all_deps.contains(s.name.as_str()))
            .any(|s| {
                matches!(
                    step_statuses.get(&s.name),
                    Some(StepStatus::Completed { .. })
                )
            });

        let duration_ms = flow_start.elapsed().as_millis();
        if terminal_succeeded {
            tracing::debug!(
                flow = %flow.name,
                duration_ms,
                steps_executed,
                "flow completed"
            );
            metrics::counter!("flow_executions_total", "flow" => flow.name.clone(), "status" => "ok")
                .increment(1);
            metrics::histogram!("flow_duration_seconds", "flow" => flow.name.clone())
                .record(duration_ms as f64 / 1000.0);
            Ok(FlowResult {
                output,
                steps: step_statuses,
                steps_executed,
            })
        } else if let Some(err) = first_error {
            tracing::warn!(
                flow = %flow.name,
                duration_ms,
                steps_executed,
                error = %err,
                "flow failed"
            );
            metrics::counter!("flow_executions_total", "flow" => flow.name.clone(), "status" => "error")
                .increment(1);
            metrics::histogram!("flow_duration_seconds", "flow" => flow.name.clone())
                .record(duration_ms as f64 / 1000.0);
            Err(err)
        } else {
            tracing::debug!(
                flow = %flow.name,
                duration_ms,
                steps_executed,
                "flow completed (no terminal step output)"
            );
            metrics::counter!("flow_executions_total", "flow" => flow.name.clone(), "status" => "ok")
                .increment(1);
            metrics::histogram!("flow_duration_seconds", "flow" => flow.name.clone())
                .record(duration_ms as f64 / 1000.0);
            Ok(FlowResult {
                output,
                steps: step_statuses,
                steps_executed,
            })
        }
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
        loop_step_name: String,
    ) -> Result<Bytes, FlowError> {
        let mut current = input;

        let emit = |evt: FlowProgressEvent| {
            if let Some(tx) = &self.progress_tx {
                let _ = tx.try_send(evt);
            }
        };

        for iter in 0..config.max_iterations {
            let iter_start = std::time::Instant::now();

            emit(FlowProgressEvent::LoopIterationStart {
                step: loop_step_name.clone(),
                iteration: iter,
                max_iterations: config.max_iterations,
            });

            let mut body_outputs: HashMap<String, Option<Bytes>> = HashMap::new();
            let iter_input_json: Option<serde_json::Value> = serde_json::from_slice(&current).ok();

            for body_step in &config.body {
                // condition 评估（引用本轮 body_outputs）
                if let Some(cond) = &body_step.condition {
                    if !evaluate_condition(cond, &HashMap::new(), &body_outputs, &iter_input_json) {
                        body_outputs.insert(body_step.name.clone(), None); // skipped
                        emit(FlowProgressEvent::BodyStepSkipped {
                            step: loop_step_name.clone(),
                            body_step: body_step.name.clone(),
                        });
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
                    body_step.name.clone(),
                    step_input,
                    effective_timeout,
                    parent_context.clone(),
                    flow_request_id.clone(),
                    call_stack.clone(),
                );

                let body_start = std::time::Instant::now();
                let out = match tokio::time::timeout(effective_timeout, body_future).await {
                    Ok(Ok(o)) => {
                        let duration_ms = body_start.elapsed().as_millis() as u64;
                        emit(FlowProgressEvent::BodyStepDone {
                            step: loop_step_name.clone(),
                            body_step: body_step.name.clone(),
                            duration_ms,
                        });
                        o
                    }
                    Ok(Err(e)) => {
                        let duration_ms = body_start.elapsed().as_millis() as u64;
                        emit(FlowProgressEvent::BodyStepFailed {
                            step: loop_step_name.clone(),
                            body_step: body_step.name.clone(),
                            error: e.to_string(),
                            duration_ms,
                        });
                        return Err(FlowError::StepFailed {
                            step: format!("{loop_step_name}[iter={iter}]/{}", body_step.name),
                            source: e,
                        });
                    }
                    Err(_) => {
                        let duration_ms = body_start.elapsed().as_millis() as u64;
                        emit(FlowProgressEvent::BodyStepFailed {
                            step: loop_step_name.clone(),
                            body_step: body_step.name.clone(),
                            error: "timeout".to_string(),
                            duration_ms,
                        });
                        return Err(FlowError::StepFailed {
                            step: format!("{loop_step_name}[iter={iter}]/{}", body_step.name),
                            source: RuneError::Timeout,
                        });
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
                let met = evaluate_condition(until, &HashMap::new(), &body_outputs, &last_json);
                emit(FlowProgressEvent::LoopIterationEnd {
                    step: loop_step_name.clone(),
                    iteration: iter,
                    duration_ms: iter_start.elapsed().as_millis() as u64,
                    until_met: met,
                });
                if met {
                    return Ok(last);
                }
            } else {
                emit(FlowProgressEvent::LoopIterationEnd {
                    step: loop_step_name.clone(),
                    iteration: iter,
                    duration_ms: iter_start.elapsed().as_millis() as u64,
                    until_met: false,
                });
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
                let _permit = sem
                    .acquire_owned()
                    .await
                    .expect("map semaphore closed unexpectedly");

                let effective_timeout = body
                    .timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(runner.step_timeout);

                let body_future = runner.execute_body_step_kind(
                    body.kind,
                    body.name,
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
    #[allow(clippy::too_many_arguments)]
    fn execute_body_step_kind(
        &self,
        kind: BodyStepKind,
        step_name: String,
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
                    .execute_loop(
                        &lc,
                        input,
                        parent_context,
                        flow_request_id,
                        call_stack,
                        step_name.clone(),
                    )
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
                                    body.name,
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
    // Support both "$input.x" and "input.x" for consistency with condition expressions.
    if path.starts_with("$input") || path == "input" || path.starts_with("input.") {
        let field_path = path
            .strip_prefix("$input.")
            .or_else(|| path.strip_prefix("input."));
        return match field_path {
            None => flow_input_json.clone().unwrap_or(serde_json::Value::Null),
            Some(fp) => flow_input_json
                .as_ref()
                .map(|v| resolve_json_path(v, fp))
                .unwrap_or(serde_json::Value::Null),
        };
    }
    {
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

/// 根据 RetryConfig 和当前 attempt（0-based）计算退避延迟毫秒数。
fn compute_backoff_ms(config: &crate::dag::RetryConfig, attempt: u32) -> u64 {
    use crate::dag::BackoffStrategy;
    let base = config.backoff_ms;
    if base == 0 {
        return 0;
    }
    let delay = match config.backoff_strategy {
        BackoffStrategy::Fixed => base,
        BackoffStrategy::Exponential => base.saturating_mul(1u64 << attempt.min(62)),
        BackoffStrategy::ExponentialJitter => {
            let exp = base.saturating_mul(1u64 << attempt.min(62));
            // ±25% jitter：确定性伪随机，基于 attempt，无需 rand crate
            let jitter_range = exp / 4;
            let jitter = if attempt.is_multiple_of(2) {
                jitter_range / 2
            } else {
                jitter_range.wrapping_mul(3) / 4
            };
            if (attempt / 2).is_multiple_of(2) {
                exp.saturating_add(jitter)
            } else {
                exp.saturating_sub(jitter)
            }
        }
    };
    if let Some(max_delay) = config.max_delay_ms {
        delay.min(max_delay)
    } else {
        delay
    }
}

/// Generic retry helper used by both Rune steps and Loop steps.
/// Calls `op` up to `max_attempts` times (min 1). On failure, waits
/// computed backoff before retrying. `should_retry` predicate controls
/// whether a given error triggers a retry; returning false exits immediately.
///
/// Callers spawning this inside `tokio::spawn` must ensure `F: Send` and
/// `Fut: Send`; the generic bounds here do not enforce it so the helper can
/// also be used in non-Send async contexts.
async fn retry_op<F, Fut, T, E, P>(
    config: &crate::dag::RetryConfig,
    should_retry: P,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: Fn(&E) -> bool,
{
    let max = config.max_attempts.max(1);
    for attempt in 0..max {
        match op().await {
            Ok(out) => return Ok(out),
            Err(e) => {
                if attempt + 1 < max && should_retry(&e) {
                    let delay = compute_backoff_ms(config, attempt);
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                } else {
                    return Err(e);
                }
            }
        }
    }
    // 不可达，但需要满足编译器
    unreachable!("retry_op loop exhausted without returning")
}

/// 按 RetryConfig 执行 invoker.invoke_once，失败时 backoff 后重试。
/// max_attempts 含首次执行；None 等价于 max_attempts=1（不重试）。
async fn invoke_with_retry(
    invoker: Arc<dyn rune_core::invoker::RuneInvoker>,
    ctx: RuneContext,
    input: Bytes,
    retry: Option<&crate::dag::RetryConfig>,
) -> Result<Bytes, RuneError> {
    use crate::dag::RetryCondition;
    match retry {
        None => invoker.invoke_once(ctx, input).await,
        Some(config) => {
            let should_retry = |e: &RuneError| -> bool {
                if config.retry_on.is_empty() || config.retry_on.contains(&RetryCondition::Any) {
                    return true;
                }
                config
                    .retry_on
                    .iter()
                    .any(|cond| matches!((cond, e), (RetryCondition::Timeout, RuneError::Timeout)))
            };
            retry_op(config, should_retry, || {
                invoker.invoke_once(ctx.clone(), input.clone())
            })
            .await
        }
    }
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

    // ─── RetryConfig / compute_backoff_ms 测试 ────────────────────────────────

    fn make_retry(
        max_attempts: u32,
        backoff_ms: u64,
        strategy: crate::dag::BackoffStrategy,
        max_delay_ms: Option<u64>,
        retry_on: Vec<crate::dag::RetryCondition>,
    ) -> crate::dag::RetryConfig {
        crate::dag::RetryConfig {
            max_attempts,
            backoff_ms,
            backoff_strategy: strategy,
            max_delay_ms,
            retry_on,
        }
    }

    /// Fixed 策略下每次延迟相同
    #[test]
    fn retry_fixed_backoff_unchanged() {
        use crate::dag::BackoffStrategy;
        let cfg = make_retry(3, 100, BackoffStrategy::Fixed, None, vec![]);
        assert_eq!(compute_backoff_ms(&cfg, 0), 100);
        assert_eq!(compute_backoff_ms(&cfg, 1), 100);
        assert_eq!(compute_backoff_ms(&cfg, 2), 100);
    }

    /// Exponential 策略下延迟翻倍
    #[test]
    fn retry_exponential_doubles() {
        use crate::dag::BackoffStrategy;
        let cfg = make_retry(4, 100, BackoffStrategy::Exponential, None, vec![]);
        assert_eq!(compute_backoff_ms(&cfg, 0), 100);
        assert_eq!(compute_backoff_ms(&cfg, 1), 200);
        assert_eq!(compute_backoff_ms(&cfg, 2), 400);
        assert_eq!(compute_backoff_ms(&cfg, 3), 800);
    }

    /// max_delay_ms 正确截断退避值
    #[test]
    fn retry_max_delay_caps_backoff() {
        use crate::dag::BackoffStrategy;
        let cfg = make_retry(5, 100, BackoffStrategy::Exponential, Some(300), vec![]);
        assert_eq!(compute_backoff_ms(&cfg, 0), 100);
        assert_eq!(compute_backoff_ms(&cfg, 1), 200);
        assert_eq!(compute_backoff_ms(&cfg, 2), 300); // 400 capped → 300
        assert_eq!(compute_backoff_ms(&cfg, 3), 300); // 800 capped → 300
    }

    /// retry_on=[Timeout] 时非 Timeout 错误不重试（直接返回错误，尝试次数 < max）
    #[tokio::test]
    async fn retry_on_timeout_skips_other_errors() {
        use crate::dag::BackoffStrategy;
        use rune_core::rune::RuneError;
        let cfg = make_retry(
            3,
            0,
            BackoffStrategy::Fixed,
            None,
            vec![crate::dag::RetryCondition::Timeout],
        );
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();
        let result = retry_op(
            &cfg,
            |e: &RuneError| matches!(e, RuneError::Timeout),
            || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err::<bytes::Bytes, RuneError>(RuneError::Unavailable)
                }
            },
        )
        .await;
        // 非 Timeout 错误不应重试，只调用 1 次
        assert!(result.is_err());
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// retry_on=[] 时所有错误都重试，应达到 max_attempts 次调用
    #[tokio::test]
    async fn retry_on_any_retries_all_errors() {
        use crate::dag::BackoffStrategy;
        use rune_core::rune::RuneError;
        let cfg = make_retry(3, 0, BackoffStrategy::Fixed, None, vec![]);
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();
        let result = retry_op(
            &cfg,
            |_: &RuneError| true,
            || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err::<bytes::Bytes, RuneError>(RuneError::Unavailable)
                }
            },
        )
        .await;
        assert!(result.is_err());
        // 应调用 3 次（max_attempts=3）
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
