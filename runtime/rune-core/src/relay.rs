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

    /// 注册一个 Rune（进程内或远程），检查 gate.path 冲突
    pub fn register(&self, config: RuneConfig, invoker: Arc<dyn RuneInvoker>, caster_id: Option<String>) -> Result<(), String> {
        // Check gate.path conflict: different rune_name with same path+method is a hard error
        if let Some(ref gate) = config.gate {
            for entry in self.entries.iter() {
                for e in entry.value() {
                    if let Some(ref existing_gate) = e.config.gate {
                        if existing_gate.path == gate.path
                            && existing_gate.method == gate.method
                            && e.config.name != config.name
                        {
                            return Err(format!(
                                "route conflict: '{}' and '{}' both declare gate path '{}' method '{}'",
                                e.config.name, config.name, gate.path, gate.method
                            ));
                        }
                    }
                }
            }
            // Also check conflict with reserved management routes
            let reserved = ["/health", "/api/v1/runes", "/api/v1/tasks", "/api/v1/flows"];
            for r in reserved {
                if gate.path == r || gate.path.starts_with(&format!("{}/", r)) {
                    return Err(format!(
                        "route conflict: gate path '{}' conflicts with management route '{}'",
                        gate.path, r
                    ));
                }
            }
        }

        let name = config.name.clone();
        self.entries.entry(name).or_default().push(RuneEntry { config, invoker, caster_id });
        Ok(())
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
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None).unwrap();

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
        relay.register(cfg("rr"), Arc::new(crate::invoker::LocalInvoker::new(h1)), None).unwrap();
        relay.register(cfg("rr"), Arc::new(crate::invoker::LocalInvoker::new(h2)), None).unwrap();

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
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), Some("c1".into())).unwrap();

        let resolver = RoundRobinResolver::new();
        assert!(relay.resolve("x", &resolver).is_some());
        relay.remove_caster("c1");
        assert!(relay.resolve("x", &resolver).is_none());
    }

    // ---- Scenario 7: gate.path route conflict between different runes ----

    #[test]
    fn test_gate_path_conflict_different_runes() {
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, input| async move { Ok(input) });
        let h2 = make_handler(|_ctx, input| async move { Ok(input) });

        let config1 = RuneConfig {
            name: "rune_a".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/do-something".into(),
                method: "POST".into(),
            }),
        };
        let config2 = RuneConfig {
            name: "rune_b".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/do-something".into(),
                method: "POST".into(),
            }),
        };

        relay.register(config1, Arc::new(crate::invoker::LocalInvoker::new(h1)), None).unwrap();
        let result = relay.register(config2, Arc::new(crate::invoker::LocalInvoker::new(h2)), None);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("route conflict"), "error should mention route conflict, got: {}", err);
        assert!(err.contains("rune_a"), "error should mention first rune name, got: {}", err);
        assert!(err.contains("rune_b"), "error should mention second rune name, got: {}", err);
    }

    #[test]
    fn test_gate_path_no_conflict_different_methods() {
        // Same path but different HTTP methods should NOT conflict
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, input| async move { Ok(input) });
        let h2 = make_handler(|_ctx, input| async move { Ok(input) });

        let config1 = RuneConfig {
            name: "rune_a".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/resource".into(),
                method: "POST".into(),
            }),
        };
        let config2 = RuneConfig {
            name: "rune_b".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/resource".into(),
                method: "GET".into(),
            }),
        };

        relay.register(config1, Arc::new(crate::invoker::LocalInvoker::new(h1)), None).unwrap();
        relay.register(config2, Arc::new(crate::invoker::LocalInvoker::new(h2)), None).unwrap();
    }

    #[test]
    fn test_gate_path_no_conflict_same_rune_name() {
        // Same rune name registering same path+method (e.g. scaling) should be allowed
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, input| async move { Ok(input) });
        let h2 = make_handler(|_ctx, input| async move { Ok(input) });

        let config = RuneConfig {
            name: "same_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/shared".into(),
                method: "POST".into(),
            }),
        };

        relay.register(config.clone(), Arc::new(crate::invoker::LocalInvoker::new(h1)), Some("c1".into())).unwrap();
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(h2)), Some("c2".into())).unwrap();
    }

    // ---- Scenario 8: reserved route conflict ----

    #[test]
    fn test_reserved_route_health() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let config = RuneConfig {
            name: "my_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/health".into(),
                method: "GET".into(),
            }),
        };

        let result = relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("/health"), "error should mention /health, got: {}", err);
        assert!(err.contains("management route"), "error should mention management route, got: {}", err);
    }

    #[test]
    fn test_reserved_route_runes_api() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let config = RuneConfig {
            name: "my_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/v1/runes".into(),
                method: "POST".into(),
            }),
        };

        let result = relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("/api/v1/runes"), "error should mention /api/v1/runes, got: {}", err);
    }

    #[test]
    fn test_reserved_route_sub_path() {
        // Paths that start with a reserved prefix should also be rejected
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let config = RuneConfig {
            name: "my_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/v1/runes/custom".into(),
                method: "POST".into(),
            }),
        };

        let result = relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("/api/v1/runes"), "error should mention the reserved prefix, got: {}", err);
    }

    // ---- Scenario 9: list() method ----

    #[test]
    fn test_list_empty_relay() {
        let relay = Relay::new();
        let result = relay.list();
        assert!(result.is_empty());
    }

    #[test]
    fn test_list_returns_all_registered_runes() {
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, input| async move { Ok(input) });
        let h2 = make_handler(|_ctx, input| async move { Ok(input) });
        let h3 = make_handler(|_ctx, input| async move { Ok(input) });

        relay.register(RuneConfig {
            name: "echo".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/echo".into(),
                method: "POST".into(),
            }),
        }, Arc::new(crate::invoker::LocalInvoker::new(h1)), None).unwrap();

        relay.register(RuneConfig {
            name: "translate".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
        }, Arc::new(crate::invoker::LocalInvoker::new(h2)), None).unwrap();

        // Same name "echo" from another caster
        relay.register(RuneConfig {
            name: "echo".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/echo".into(),
                method: "POST".into(),
            }),
        }, Arc::new(crate::invoker::LocalInvoker::new(h3)), Some("c2".into())).unwrap();

        let list = relay.list();
        assert_eq!(list.len(), 3);

        // Verify the entries contain the expected names and gate paths
        let echo_entries: Vec<_> = list.iter().filter(|(n, _)| n == "echo").collect();
        assert_eq!(echo_entries.len(), 2);
        for (_, gate_path) in &echo_entries {
            assert_eq!(gate_path.as_deref(), Some("/api/echo"));
        }

        let translate_entries: Vec<_> = list.iter().filter(|(n, _)| n == "translate").collect();
        assert_eq!(translate_entries.len(), 1);
        assert_eq!(translate_entries[0].1, None);
    }

    // ---- Scenario 10: remove_caster then re-register same caster_id ----

    #[test]
    fn test_remove_caster_then_reregister() {
        let relay = Relay::new();
        let resolver = RoundRobinResolver::new();

        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("first")) });
        let config = RuneConfig {
            name: "my_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
        };

        // Register with caster_id "c1"
        relay.register(config.clone(), Arc::new(crate::invoker::LocalInvoker::new(h1)), Some("c1".into())).unwrap();
        assert!(relay.resolve("my_rune", &resolver).is_some());

        // Remove caster "c1"
        relay.remove_caster("c1");
        assert!(relay.resolve("my_rune", &resolver).is_none());

        // Re-register with same caster_id "c1"
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("second")) });
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(h2)), Some("c1".into())).unwrap();
        assert!(relay.resolve("my_rune", &resolver).is_some());
    }

    // ---- Scenario 11: remove_caster on empty relay does not panic ----

    #[test]
    fn test_remove_caster_empty_relay_no_panic() {
        let relay = Relay::new();
        // Should not panic when removing from empty registry
        relay.remove_caster("nonexistent_caster");
        assert!(relay.list().is_empty());
    }

    #[test]
    fn test_remove_caster_unknown_id_no_panic() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let config = RuneConfig {
            name: "rune_a".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
        };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), Some("c1".into())).unwrap();

        // Removing a non-existent caster should not affect existing entries
        relay.remove_caster("c99");
        assert_eq!(relay.list().len(), 1);
        assert_eq!(relay.list()[0].0, "rune_a");
    }
}
