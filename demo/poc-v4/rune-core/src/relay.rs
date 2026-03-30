use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use dashmap::DashMap;
use crate::rune::RuneConfig;
use crate::invoker::RuneInvoker;

/// 一个 Rune 的注册条目
pub struct RuneEntry {
    pub config: RuneConfig,
    pub invoker: Arc<dyn RuneInvoker>,
    pub caster_id: Option<String>,  // None = 进程内
}

/// Relay = 注册表 + 轮询 Resolver（POC 合一，正式版拆开）
pub struct Relay {
    entries: DashMap<String, Vec<RuneEntry>>,
    counters: DashMap<String, AtomicUsize>,
}

impl Relay {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            counters: DashMap::new(),
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

    /// 选一个 Invoker（轮询）
    pub fn resolve(&self, rune_name: &str) -> Option<Arc<dyn RuneInvoker>> {
        let entries = self.entries.get(rune_name)?;
        let candidates = entries.value();
        if candidates.is_empty() {
            return None;
        }
        let idx = self.counters
            .entry(rune_name.to_string())
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);
        Some(Arc::clone(&candidates[idx % candidates.len()].invoker))
    }

    /// 列出所有已注册 Rune 的名称和 gate_path
    pub fn list(&self) -> Vec<(String, Option<String>)> {
        let mut result = Vec::new();
        for entry in self.entries.iter() {
            for e in entry.value() {
                result.push((e.config.name.clone(), e.config.gate_path.clone()));
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rune::{RuneContext, RuneError, make_handler};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_register_and_resolve() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let config = RuneConfig {
            name: "echo".into(),
            description: "echo test".into(),
            gate_path: None,
        };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None);

        let invoker = relay.resolve("echo").expect("should resolve");
        let ctx = RuneContext { rune_name: "echo".into(), request_id: "r1".into() };
        let result = invoker.invoke(ctx, Bytes::from("hello")).await.unwrap();
        assert_eq!(result, Bytes::from("hello"));
    }

    #[test]
    fn test_resolve_not_found() {
        let relay = Relay::new();
        assert!(relay.resolve("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_round_robin() {
        let relay = Relay::new();

        // 注册两个 handler，返回不同内容
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("a")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("b")) });

        let cfg = |n: &str| RuneConfig { name: n.into(), description: "".into(), gate_path: None };
        relay.register(cfg("rr"), Arc::new(crate::invoker::LocalInvoker::new(h1)), None);
        relay.register(cfg("rr"), Arc::new(crate::invoker::LocalInvoker::new(h2)), None);

        let ctx = || RuneContext { rune_name: "rr".into(), request_id: "r".into() };
        let r1 = relay.resolve("rr").unwrap().invoke(ctx(), Bytes::new()).await.unwrap();
        let r2 = relay.resolve("rr").unwrap().invoke(ctx(), Bytes::new()).await.unwrap();

        // 轮询应该交替
        assert_ne!(r1, r2);
    }

    #[test]
    fn test_remove_caster() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let config = RuneConfig { name: "x".into(), description: "".into(), gate_path: None };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), Some("c1".into()));

        assert!(relay.resolve("x").is_some());
        relay.remove_caster("c1");
        assert!(relay.resolve("x").is_none());
    }
}
