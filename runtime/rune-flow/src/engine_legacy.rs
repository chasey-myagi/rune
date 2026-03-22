use std::sync::Arc;
use bytes::Bytes;
use rune_core::rune::{RuneContext, RuneError};
use rune_core::relay::Relay;
use rune_core::resolver::Resolver;
use crate::dsl::Flow;

/// Flow 执行结果
#[derive(Debug)]
pub struct FlowResult {
    pub output: Bytes,
    pub steps_executed: usize,
}

/// Flow 执行错误
#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    #[error("flow '{0}' not found")]
    FlowNotFound(String),

    #[error("step {step_index} '{rune_name}' failed: {source}")]
    StepFailed {
        step_index: usize,
        rune_name: String,
        source: RuneError,
    },
}

/// Flow 引擎：按顺序执行 Chain
pub struct FlowEngine {
    flows: std::collections::HashMap<String, Flow>,
    relay: Arc<Relay>,
    resolver: Arc<dyn Resolver>,
}

impl FlowEngine {
    pub fn new(relay: Arc<Relay>, resolver: Arc<dyn Resolver>) -> Self {
        Self {
            flows: std::collections::HashMap::new(),
            relay,
            resolver,
        }
    }

    /// 注册一个 Flow
    pub fn register(&mut self, flow: Flow) {
        self.flows.insert(flow.name.clone(), flow);
    }

    /// 列出所有已注册 Flow
    pub fn list(&self) -> Vec<&str> {
        self.flows.keys().map(|s| s.as_str()).collect()
    }

    /// 执行一个 Flow（Chain 顺序执行）
    pub async fn execute(&self, flow_name: &str, input: Bytes) -> Result<FlowResult, FlowError> {
        let flow = self.flows.get(flow_name)
            .ok_or_else(|| FlowError::FlowNotFound(flow_name.to_string()))?;

        // 空 Flow → 直接返回 input
        if flow.steps.is_empty() {
            return Ok(FlowResult { output: input, steps_executed: 0 });
        }

        let mut current = input;

        for (i, rune_name) in flow.steps.iter().enumerate() {
            tracing::info!(flow = flow_name, step = i, rune = %rune_name, "executing step");

            let invoker = self.relay.resolve(rune_name, &*self.resolver)
                .ok_or_else(|| FlowError::StepFailed {
                    step_index: i,
                    rune_name: rune_name.clone(),
                    source: RuneError::NotFound(rune_name.clone()),
                })?;

            let ctx = RuneContext {
                rune_name: rune_name.clone(),
                request_id: unique_flow_request_id(flow_name, i),
                context: Default::default(),
                timeout: std::time::Duration::from_secs(30),
            };

            current = invoker.invoke_once(ctx, current).await
                .map_err(|e| FlowError::StepFailed {
                    step_index: i,
                    rune_name: rune_name.clone(),
                    source: e,
                })?;

            tracing::info!(flow = flow_name, step = i, rune = %rune_name, "step completed");
        }

        Ok(FlowResult {
            output: current,
            steps_executed: flow.steps.len(),
        })
    }
}

fn unique_flow_request_id(flow_name: &str, step: usize) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("f-{}-{}-{:x}", flow_name, step, seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rune_core::rune::{make_handler, RuneConfig};
    use rune_core::invoker::LocalInvoker;
    use rune_core::resolver::RoundRobinResolver;

    fn test_relay() -> Arc<Relay> {
        let relay = Arc::new(Relay::new());

        // step_a: 给 JSON 加 "a": true
        let ha = make_handler(|_ctx, input| async move {
            let mut v: serde_json::Value = serde_json::from_slice(&input)
                .map_err(|e| RuneError::InvalidInput(e.to_string()))?;
            v.as_object_mut().unwrap().insert("a".into(), true.into());
            Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
        });
        relay.register(
            RuneConfig { name: "step_a".into(), version: String::new(), description: "".into(), supports_stream: false, gate: None, input_schema: None, output_schema: None, priority: 0, labels: Default::default() },
            Arc::new(LocalInvoker::new(ha)), None,
        ).unwrap();

        // step_b: 给 JSON 加 "b": true
        let hb = make_handler(|_ctx, input| async move {
            let mut v: serde_json::Value = serde_json::from_slice(&input)
                .map_err(|e| RuneError::InvalidInput(e.to_string()))?;
            v.as_object_mut().unwrap().insert("b".into(), true.into());
            Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
        });
        relay.register(
            RuneConfig { name: "step_b".into(), version: String::new(), description: "".into(), supports_stream: false, gate: None, input_schema: None, output_schema: None, priority: 0, labels: Default::default() },
            Arc::new(LocalInvoker::new(hb)), None,
        ).unwrap();

        // step_c: 给 JSON 加 "c": true
        let hc = make_handler(|_ctx, input| async move {
            let mut v: serde_json::Value = serde_json::from_slice(&input)
                .map_err(|e| RuneError::InvalidInput(e.to_string()))?;
            v.as_object_mut().unwrap().insert("c".into(), true.into());
            Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
        });
        relay.register(
            RuneConfig { name: "step_c".into(), version: String::new(), description: "".into(), supports_stream: false, gate: None, input_schema: None, output_schema: None, priority: 0, labels: Default::default() },
            Arc::new(LocalInvoker::new(hc)), None,
        ).unwrap();

        // fail_step: 总是失败
        let hf = make_handler(|_ctx, _input| async move {
            Err(RuneError::ExecutionFailed {
                code: "BOOM".into(),
                message: "intentional failure".into(),
            })
        });
        relay.register(
            RuneConfig { name: "fail_step".into(), version: String::new(), description: "".into(), supports_stream: false, gate: None, input_schema: None, output_schema: None, priority: 0, labels: Default::default() },
            Arc::new(LocalInvoker::new(hf)), None,
        ).unwrap();

        relay
    }

    #[tokio::test]
    async fn test_engine_execute_chain() {
        let relay = test_relay();
        let resolver = Arc::new(RoundRobinResolver::new());
        let mut engine = FlowEngine::new(relay, resolver);
        engine.register(
            crate::dsl::Flow::new("test").chain(vec!["step_a", "step_b", "step_c"]).build()
        );

        let result = engine.execute("test", Bytes::from(r#"{"x":1}"#)).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&result.output).unwrap();

        assert_eq!(v["x"], 1);
        assert_eq!(v["a"], true);
        assert_eq!(v["b"], true);
        assert_eq!(v["c"], true);
        assert_eq!(result.steps_executed, 3);
    }

    #[tokio::test]
    async fn test_engine_step_fail() {
        let relay = test_relay();
        let resolver = Arc::new(RoundRobinResolver::new());
        let mut engine = FlowEngine::new(relay, resolver);
        engine.register(
            crate::dsl::Flow::new("fail").chain(vec!["step_a", "fail_step", "step_c"]).build()
        );

        let err = engine.execute("fail", Bytes::from(r#"{}"#)).await.unwrap_err();
        match err {
            FlowError::StepFailed { step_index, rune_name, .. } => {
                assert_eq!(step_index, 1);
                assert_eq!(rune_name, "fail_step");
            }
            _ => panic!("expected StepFailed"),
        }
    }

    #[tokio::test]
    async fn test_engine_empty_chain() {
        let relay = test_relay();
        let resolver = Arc::new(RoundRobinResolver::new());
        let mut engine = FlowEngine::new(relay, resolver);
        engine.register(crate::dsl::Flow::new("empty").build());

        let result = engine.execute("empty", Bytes::from(r#"{"pass":"through"}"#)).await.unwrap();
        assert_eq!(result.output, Bytes::from(r#"{"pass":"through"}"#));
        assert_eq!(result.steps_executed, 0);
    }

    #[tokio::test]
    async fn test_engine_flow_not_found() {
        let relay = test_relay();
        let resolver = Arc::new(RoundRobinResolver::new());
        let engine = FlowEngine::new(relay, resolver);
        let err = engine.execute("nope", Bytes::new()).await.unwrap_err();
        assert!(matches!(err, FlowError::FlowNotFound(_)));
    }

    #[tokio::test]
    async fn test_engine_rune_not_found_in_step() {
        let relay = test_relay();
        let resolver = Arc::new(RoundRobinResolver::new());
        let mut engine = FlowEngine::new(relay, resolver);
        engine.register(
            crate::dsl::Flow::new("bad").chain(vec!["step_a", "nonexistent"]).build()
        );

        let err = engine.execute("bad", Bytes::from(r#"{}"#)).await.unwrap_err();
        match err {
            FlowError::StepFailed { step_index, rune_name, .. } => {
                assert_eq!(step_index, 1);
                assert_eq!(rune_name, "nonexistent");
            }
            _ => panic!("expected StepFailed"),
        }
    }
}
