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
    /// 反向索引：gate_path → rune_name，O(1) 查找
    gate_paths: DashMap<String, String>,
}

impl Relay {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            counters: DashMap::new(),
            gate_paths: DashMap::new(),
        }
    }

    /// 注册一个 Rune（进程内或远程）
    pub fn register(&self, config: RuneConfig, invoker: Arc<dyn RuneInvoker>, caster_id: Option<String>) {
        let name = config.name.clone();
        // 维护 gate_path 反向索引
        if let Some(ref path) = config.gate_path {
            if let Some(existing) = self.gate_paths.get(path) {
                if existing.value() != &name {
                    tracing::warn!(
                        gate_path = %path,
                        existing_rune = %existing.value(),
                        new_rune = %name,
                        "gate_path conflict: overwriting with new rune (last-wins)"
                    );
                }
            }
            self.gate_paths.insert(path.clone(), name.clone());
        }
        self.entries
            .entry(name)
            .or_default()
            .push(RuneEntry { config, invoker, caster_id });
    }

    /// 移除某个 Caster 的所有 Rune
    // TODO: remove_caster 的三步操作（收集 paths → 删条目 → 清反向索引）不是原子的。
    // 并发 register() 可能导致短暂的不一致状态。正式版应用锁或事务保证原子性。
    pub fn remove_caster(&self, caster_id: &str) {
        // 收集被移除条目的 gate_path，用于清理反向索引
        let mut removed_paths: Vec<String> = Vec::new();
        for entry in self.entries.iter() {
            for e in entry.value() {
                if e.caster_id.as_deref() == Some(caster_id) {
                    if let Some(ref path) = e.config.gate_path {
                        removed_paths.push(path.clone());
                    }
                }
            }
        }
        for mut entry in self.entries.iter_mut() {
            entry.value_mut().retain(|e| {
                e.caster_id.as_deref() != Some(caster_id)
            });
        }
        // 清理空条目
        self.entries.retain(|_, v| !v.is_empty());
        // 清理反向索引（仅当该 path 对应的 rune 已无实例时才移除）
        for path in removed_paths {
            if let Some(rune_name) = self.gate_paths.get(&path) {
                if self.entries.get(rune_name.value()).is_none() {
                    drop(rune_name);
                    self.gate_paths.remove(&path);
                }
            }
        }
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

    /// 按 gate_path 精确匹配查找 Rune，返回 (rune_name, invoker)
    /// 通过 gate_paths 反向索引 O(1) 查找，再复用 resolve() 的 round-robin 逻辑
    pub fn resolve_by_gate_path(&self, path: &str) -> Option<(String, Arc<dyn RuneInvoker>)> {
        let rune_name = self.gate_paths.get(path)?.value().clone();
        let invoker = self.resolve(&rune_name)?;
        Some((rune_name, invoker))
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
    use crate::rune::{RuneContext, make_handler};
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

    // ========== Issue #5 regression tests ==========

    fn make_echo_handler() -> crate::rune::RuneHandler {
        make_handler(|_ctx, input| async move { Ok(input) })
    }

    fn register_with_gate_path(relay: &Relay, name: &str, gate_path: Option<&str>) {
        let handler = make_echo_handler();
        let config = RuneConfig {
            name: name.into(),
            description: "".into(),
            gate_path: gate_path.map(|s| s.to_string()),
        };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None);
    }

    #[test]
    fn test_fix_resolve_by_gate_path_match() {
        let relay = Relay::new();
        register_with_gate_path(&relay, "echo", Some("/echo"));
        let result = relay.resolve_by_gate_path("/echo");
        assert!(result.is_some());
        let (name, _invoker) = result.unwrap();
        assert_eq!(name, "echo");
    }

    #[test]
    fn test_fix_resolve_by_gate_path_no_match() {
        let relay = Relay::new();
        register_with_gate_path(&relay, "echo", Some("/echo"));
        assert!(relay.resolve_by_gate_path("/not-exist").is_none());
    }

    #[test]
    fn test_fix_resolve_by_gate_path_multiple_runes() {
        let relay = Relay::new();
        register_with_gate_path(&relay, "echo", Some("/echo"));
        register_with_gate_path(&relay, "greet", Some("/greet"));

        let (name1, _) = relay.resolve_by_gate_path("/echo").unwrap();
        let (name2, _) = relay.resolve_by_gate_path("/greet").unwrap();
        assert_eq!(name1, "echo");
        assert_eq!(name2, "greet");
    }

    #[test]
    fn test_fix_resolve_by_gate_path_none_not_matched() {
        let relay = Relay::new();
        register_with_gate_path(&relay, "echo", None); // no gate_path
        assert!(relay.resolve_by_gate_path("/echo").is_none());
    }

    #[test]
    fn test_fix_resolve_by_gate_path_after_remove_caster() {
        let relay = Relay::new();
        let handler = make_echo_handler();
        let config = RuneConfig {
            name: "echo".into(),
            description: "".into(),
            gate_path: Some("/echo".into()),
        };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), Some("c1".into()));

        assert!(relay.resolve_by_gate_path("/echo").is_some());
        relay.remove_caster("c1");
        assert!(relay.resolve_by_gate_path("/echo").is_none());
    }

    #[tokio::test]
    async fn test_fix_resolve_by_gate_path_duplicate_path() {
        let relay = Relay::new();
        register_with_gate_path(&relay, "echo_a", Some("/echo"));
        register_with_gate_path(&relay, "echo_b", Some("/echo"));
        // Should return some match and the invoker should be callable
        let (name, invoker) = relay.resolve_by_gate_path("/echo").expect("should resolve");
        assert!(name == "echo_a" || name == "echo_b");
        let ctx = RuneContext { rune_name: name, request_id: "r1".into() };
        let result = invoker.invoke(ctx, Bytes::from("test")).await.unwrap();
        assert_eq!(result, Bytes::from("test"));
    }

    #[test]
    fn test_fix_resolve_by_gate_path_trailing_slash() {
        let relay = Relay::new();
        register_with_gate_path(&relay, "echo", Some("/echo"));
        // /echo/ is a different path than /echo
        assert!(relay.resolve_by_gate_path("/echo/").is_none());
    }

    #[test]
    fn test_fix_resolve_by_gate_path_root() {
        let relay = Relay::new();
        register_with_gate_path(&relay, "root", Some("/"));
        let result = relay.resolve_by_gate_path("/");
        assert!(result.is_some());
        let (name, _) = result.unwrap();
        assert_eq!(name, "root");
    }

    #[test]
    fn test_fix_resolve_by_gate_path_empty() {
        let relay = Relay::new();
        register_with_gate_path(&relay, "echo", Some("/echo"));
        assert!(relay.resolve_by_gate_path("").is_none());
    }

    #[tokio::test]
    async fn test_fix_resolve_by_gate_path_round_robin() {
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("a")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("b")) });
        let cfg = RuneConfig { name: "rr".into(), description: "".into(), gate_path: Some("/rr".into()) };
        relay.register(cfg, Arc::new(crate::invoker::LocalInvoker::new(h1)), None);
        let cfg2 = RuneConfig { name: "rr".into(), description: "".into(), gate_path: Some("/rr".into()) };
        relay.register(cfg2, Arc::new(crate::invoker::LocalInvoker::new(h2)), None);

        let ctx = || RuneContext { rune_name: "rr".into(), request_id: "r".into() };
        let (_, inv1) = relay.resolve_by_gate_path("/rr").unwrap();
        let (_, inv2) = relay.resolve_by_gate_path("/rr").unwrap();
        let r1 = inv1.invoke(ctx(), Bytes::new()).await.unwrap();
        let r2 = inv2.invoke(ctx(), Bytes::new()).await.unwrap();
        assert_ne!(r1, r2, "round-robin should alternate between invokers");
    }

    #[test]
    fn test_fix_resolve_by_gate_path_reregister() {
        let relay = Relay::new();
        let handler = make_echo_handler();
        let config = RuneConfig {
            name: "echo".into(),
            description: "".into(),
            gate_path: Some("/echo".into()),
        };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), Some("c1".into()));
        assert!(relay.resolve_by_gate_path("/echo").is_some());

        relay.remove_caster("c1");
        assert!(relay.resolve_by_gate_path("/echo").is_none());

        // Re-register
        let handler2 = make_echo_handler();
        let config2 = RuneConfig {
            name: "echo".into(),
            description: "".into(),
            gate_path: Some("/echo".into()),
        };
        relay.register(config2, Arc::new(crate::invoker::LocalInvoker::new(handler2)), Some("c2".into()));
        assert!(relay.resolve_by_gate_path("/echo").is_some());
    }
}
