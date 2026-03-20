/// Flow 定义：一组按顺序执行的 Rune 名称
#[derive(Debug, Clone)]
pub struct Flow {
    pub name: String,
    pub steps: Vec<String>,
}

impl Flow {
    pub fn new(name: &str) -> FlowBuilder {
        FlowBuilder {
            name: name.to_string(),
            steps: Vec::new(),
        }
    }
}

pub struct FlowBuilder {
    name: String,
    steps: Vec<String>,
}

impl FlowBuilder {
    /// 添加 Chain 步骤（顺序执行）
    pub fn chain(mut self, runes: Vec<&str>) -> Self {
        self.steps.extend(runes.into_iter().map(|s| s.to_string()));
        self
    }

    /// 添加单个步骤
    pub fn step(mut self, rune: &str) -> Self {
        self.steps.push(rune.to_string());
        self
    }

    pub fn build(self) -> Flow {
        Flow {
            name: self.name,
            steps: self.steps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_build() {
        let flow = Flow::new("pipeline")
            .chain(vec!["a", "b", "c"])
            .build();
        assert_eq!(flow.name, "pipeline");
        assert_eq!(flow.steps, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_step_build() {
        let flow = Flow::new("seq")
            .step("x")
            .step("y")
            .build();
        assert_eq!(flow.steps, vec!["x", "y"]);
    }

    #[test]
    fn test_empty_flow() {
        let flow = Flow::new("empty").build();
        assert!(flow.steps.is_empty());
    }
}
