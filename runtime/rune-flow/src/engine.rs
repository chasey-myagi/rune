use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use rune_core::rune::RuneError;
use rune_core::relay::Relay;
use rune_core::resolver::Resolver;
use crate::dag::{FlowDefinition, DagError};

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
}

#[allow(dead_code)]
pub struct FlowEngine {
    flows: HashMap<String, FlowDefinition>,
    relay: Arc<Relay>,
    resolver: Arc<dyn Resolver>,
}

impl FlowEngine {
    pub fn new(_relay: Arc<Relay>, _resolver: Arc<dyn Resolver>) -> Self {
        todo!()
    }

    pub fn register(&mut self, _flow: FlowDefinition) -> Result<(), FlowError> {
        todo!()
    }

    pub fn get(&self, _name: &str) -> Option<&FlowDefinition> {
        todo!()
    }

    pub fn list(&self) -> Vec<&str> {
        todo!()
    }

    pub fn remove(&mut self, _name: &str) -> bool {
        todo!()
    }

    pub async fn execute(&self, _flow_name: &str, _input: Bytes) -> Result<FlowResult, FlowError> {
        todo!()
    }
}
