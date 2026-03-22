use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// DAG Flow 定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDefinition {
    pub name: String,
    pub steps: Vec<StepDefinition>,
    pub gate_path: Option<String>,
}

/// DAG 中的一个步骤
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDefinition {
    pub name: String,
    pub rune: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub condition: Option<String>,
    pub input_mapping: Option<HashMap<String, String>>,
}

/// DAG 验证错误
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
}

/// 验证 FlowDefinition，检查 DAG 有效性
pub fn validate_dag(_flow: &FlowDefinition) -> Result<(), DagError> {
    todo!()
}

/// 拓扑排序，返回执行层级（同一层可并行）
pub fn topological_layers(_flow: &FlowDefinition) -> Result<Vec<Vec<usize>>, DagError> {
    todo!()
}
