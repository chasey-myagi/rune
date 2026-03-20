use std::sync::Arc;
use dashmap::DashMap;
use crate::rune::RuneConfig;
use crate::invoker::RuneInvoker;
use crate::resolver::Resolver;

/// 一个 Rune 的注册条目
pub struct RuneEntry {
    pub config: RuneConfig,
    pub invoker: Arc<dyn RuneInvoker>,
    pub caster_id: Option<String>,  // None = 进程内
}

/// Relay = 注册表
pub struct Relay {
    entries: DashMap<String, Vec<RuneEntry>>,
}

impl Relay {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// 注册一个 Rune（进程内或远程）
    pub fn register(&self, config: RuneConfig, invoker: Arc<dyn RuneInvoker>, caster_id: Option<String>) {
        let name = config.name.clone();
        self.entries
            .entry(name)
            .or_default()
            .push(RuneEntry { config, invoker, caster_id });
    }

    /// 移除某个 Caster 的所有 Rune
    pub fn remove_caster(&self, caster_id: &str) {
        for mut entry in self.entries.iter_mut() {
            entry.value_mut().retain(|e| {
                e.caster_id.as_deref() != Some(caster_id)
            });
        }
        // 清理空条目
        self.entries.retain(|_, v| !v.is_empty());
    }

    /// Find all candidates for a rune name
    pub fn find(&self, rune_name: &str) -> Option<dashmap::mapref::one::Ref<'_, String, Vec<RuneEntry>>> {
        let entry = self.entries.get(rune_name)?;
        if entry.value().is_empty() { return None; }
        Some(entry)
    }

    /// Convenience: find + pick using a resolver
    pub fn resolve(&self, rune_name: &str, resolver: &dyn Resolver) -> Option<Arc<dyn RuneInvoker>> {
        let entries = self.find(rune_name)?;
        let picked = resolver.pick(rune_name, entries.value())?;
        Some(Arc::clone(&picked.invoker))
    }

    /// 列出所有已注册 Rune 的名称和 gate path
    pub fn list(&self) -> Vec<(String, Option<String>)> {
        let mut result = Vec::new();
        for entry in self.entries.iter() {
            for e in entry.value() {
                let gate_path = e.config.gate.as_ref().map(|g| g.path.clone());
                result.push((e.config.name.clone(), gate_path));
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rune::{RuneContext, make_handler};
    use crate::resolver::RoundRobinResolver;
    use bytes::Bytes;
    use std::time::Duration;

    #[tokio::test]
    async fn test_register_and_resolve() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let config = RuneConfig {
            name: "echo".into(),
            version: String::new(),
            description: "echo test".into(),
            supports_stream: false,
            gate: None,
        };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None);

        let resolver = RoundRobinResolver::new();
        let invoker = relay.resolve("echo", &resolver).expect("should resolve");
        let ctx = RuneContext {
            rune_name: "echo".into(),
            request_id: "r1".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };
        let result = invoker.invoke_once(ctx, Bytes::from("hello")).await.unwrap();
        assert_eq!(result, Bytes::from("hello"));
    }

    #[test]
    fn test_resolve_not_found() {
        let relay = Relay::new();
        let resolver = RoundRobinResolver::new();
        assert!(relay.resolve("nonexistent", &resolver).is_none());
    }

    #[tokio::test]
    async fn test_round_robin() {
        let relay = Relay::new();

        // 注册两个 handler，返回不同内容
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("a")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("b")) });

        let cfg = |n: &str| RuneConfig {
            name: n.into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
        };
        relay.register(cfg("rr"), Arc::new(crate::invoker::LocalInvoker::new(h1)), None);
        relay.register(cfg("rr"), Arc::new(crate::invoker::LocalInvoker::new(h2)), None);

        let resolver = RoundRobinResolver::new();
        let ctx = || RuneContext {
            rune_name: "rr".into(),
            request_id: "r".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };
        let r1 = relay.resolve("rr", &resolver).unwrap().invoke_once(ctx(), Bytes::new()).await.unwrap();
        let r2 = relay.resolve("rr", &resolver).unwrap().invoke_once(ctx(), Bytes::new()).await.unwrap();

        // 轮询应该交替
        assert_ne!(r1, r2);
    }

    #[test]
    fn test_remove_caster() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let config = RuneConfig {
            name: "x".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
        };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), Some("c1".into()));

        let resolver = RoundRobinResolver::new();
        assert!(relay.resolve("x", &resolver).is_some());
        relay.remove_caster("c1");
        assert!(relay.resolve("x", &resolver).is_none());
    }
}
