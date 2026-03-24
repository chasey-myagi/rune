use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

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
    #[error("step '{step}' has unparseable condition: '{condition}' — use 'true', 'false', or a comparison with spaces around the operator (e.g. 'x == 5')")]
    InvalidCondition { step: String, condition: String },
}

/// 验证 FlowDefinition，检查 DAG 有效性
pub fn validate_dag(flow: &FlowDefinition) -> Result<(), DagError> {
    let steps = &flow.steps;

    // 空 flow 直接通过
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

    // 构建 name → index 映射
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

    // 3. 用 Kahn 算法检测环（先于 input_mapping 检查，因为环更严重）
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
    for i in 0..n {
        if in_degree[i] == 0 {
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
        // 收集环中的 step 名
        let cycle_steps: Vec<String> = (0..n)
            .filter(|&i| in_degree[i] > 0)
            .map(|i| steps[i].name.clone())
            .collect();
        return Err(DagError::CycleDetected(cycle_steps.join(" -> ")));
    }

    // 4. 检查多上游无 input_mapping（环检测之后）
    for s in steps {
        if s.depends_on.len() > 1 && s.input_mapping.is_none() {
            return Err(DagError::MissingInputMapping {
                step: s.name.clone(),
            });
        }
    }

    // 5. 条件表达式预检：如果 condition 不是 "true"/"false" 且无法解析出操作符，注册时就报错
    let operators = ["==", "!=", ">=", "<=", ">", "<"];
    for s in steps {
        if let Some(condition) = &s.condition {
            let trimmed = condition.trim();
            if trimmed.is_empty() || trimmed == "true" || trimmed == "false" {
                continue;
            }
            // 检查是否能找到独立的操作符 token
            let tokens: Vec<&str> = trimmed.split_whitespace().collect();
            let has_operator = operators.iter().any(|op| tokens.contains(op));
            if !has_operator {
                return Err(DagError::InvalidCondition {
                    step: s.name.clone(),
                    condition: condition.clone(),
                });
            }
        }
    }

    Ok(())
}

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
