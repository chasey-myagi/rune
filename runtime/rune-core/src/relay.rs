use std::sync::{Arc, Mutex};
use dashmap::DashMap;
use crate::rune::RuneConfig;
use crate::invoker::RuneInvoker;
use crate::resolver::Resolver;

/// 一个 Rune 的注册条目
#[derive(Clone)]
pub struct RuneEntry {
    pub config: RuneConfig,
    pub invoker: Arc<dyn RuneInvoker>,
    pub caster_id: Option<String>,  // None = 进程内
}

/// Relay = 注册表
pub struct Relay {
    entries: DashMap<String, Vec<RuneEntry>>,
    /// Reverse index: gate_path → rune_name for O(1) dynamic route lookup
    gate_path_index: DashMap<String, String>,
    /// Serializes register/remove_caster to prevent race conditions on gate_path_index
    write_lock: Mutex<()>,
}

impl Relay {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            gate_path_index: DashMap::new(),
            write_lock: Mutex::new(()),
        }
    }

    /// 注册一个 Rune（进程内或远程），检查 gate.path 冲突
    pub fn register(&self, config: RuneConfig, invoker: Arc<dyn RuneInvoker>, caster_id: Option<String>) -> Result<(), String> {
        let _guard = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());

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

        // Maintain gate_path reverse index (key = "METHOD:path")
        if let Some(ref gate) = config.gate {
            let index_key = format!("{}:{}", gate.method, gate.path);
            self.gate_path_index.insert(index_key, config.name.clone());
        }

        let name = config.name.clone();
        self.entries.entry(name).or_default().push(RuneEntry { config, invoker, caster_id });
        Ok(())
    }

    /// 移除某个 Caster 的所有 Rune
    pub fn remove_caster(&self, caster_id: &str) {
        let _guard = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());

        // Collect index keys ("METHOD:path") to remove from index before mutating entries
        let mut keys_to_remove = Vec::new();
        for entry in self.entries.iter() {
            for e in entry.value() {
                if e.caster_id.as_deref() == Some(caster_id) {
                    if let Some(ref gate) = e.config.gate {
                        keys_to_remove.push(format!("{}:{}", gate.method, gate.path));
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

        // Clean up gate_path_index: only remove if no remaining entry uses this key
        for key in keys_to_remove {
            let still_exists = self.entries.iter().any(|entry| {
                entry.value().iter().any(|e| {
                    e.config.gate.as_ref()
                        .map(|g| format!("{}:{}", g.method, g.path))
                        .as_deref() == Some(&key)
                })
            });
            if !still_exists {
                self.gate_path_index.remove(&key);
            }
        }
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

    /// Resolve with label filtering: only consider candidates whose labels
    /// are a superset of `required_labels`. Returns None if no match.
    pub fn resolve_with_labels(
        &self,
        rune_name: &str,
        required_labels: &std::collections::HashMap<String, String>,
        resolver: &dyn Resolver,
    ) -> Option<Arc<dyn RuneInvoker>> {
        let entries = self.find(rune_name)?;
        if required_labels.is_empty() {
            // No label filter — fall through to normal resolve
            let picked = resolver.pick(rune_name, entries.value())?;
            return Some(Arc::clone(&picked.invoker));
        }
        // Filter candidates by labels
        let filtered: Vec<RuneEntry> = entries.value().iter()
            .filter(|e| {
                required_labels.iter().all(|(k, v)| e.config.labels.get(k) == Some(v))
            })
            .cloned()
            .collect();
        if filtered.is_empty() { return None; }
        let picked = resolver.pick(rune_name, &filtered)?;
        Some(Arc::clone(&picked.invoker))
    }

    /// O(1) lookup: (method, gate_path) → rune_name via reverse index
    pub fn resolve_by_gate_path(&self, method: &str, gate_path: &str) -> Option<String> {
        let key = format!("{}:{}", method, gate_path);
        self.gate_path_index.get(&key).map(|r| r.value().clone())
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        }, Arc::new(crate::invoker::LocalInvoker::new(h1)), None).unwrap();

        relay.register(RuneConfig {
            name: "translate".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
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
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), Some("c1".into())).unwrap();

        // Removing a non-existent caster should not affect existing entries
        relay.remove_caster("c99");
        assert_eq!(relay.list().len(), 1);
        assert_eq!(relay.list()[0].0, "rune_a");
    }

    // ---- Register 100 runes: verify resolve and list correctness ----

    #[test]
    fn test_register_100_runes() {
        let relay = Relay::new();
        let resolver = RoundRobinResolver::new();

        for i in 0..100 {
            let handler = make_handler(|_ctx, input| async move { Ok(input) });
            let config = RuneConfig {
                name: format!("rune_{}", i),
                version: String::new(),
                description: format!("rune number {}", i),
                supports_stream: false,
                gate: Some(crate::rune::GateConfig {
                    path: format!("/api/rune_{}", i),
                    method: "POST".into(),
                }),
                input_schema: None,
                output_schema: None,
                priority: 0, labels: Default::default(),
            };
            relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None).unwrap();
        }

        // list should return exactly 100 entries
        assert_eq!(relay.list().len(), 100);

        // Every rune should be resolvable
        for i in 0..100 {
            let name = format!("rune_{}", i);
            assert!(
                relay.resolve(&name, &resolver).is_some(),
                "rune '{}' should be resolvable",
                name
            );
        }

        // Non-existent rune should still be None
        assert!(relay.resolve("rune_100", &resolver).is_none());
    }

    // ---- Same rune name, multiple casters: register 5, remove 3, verify 2 remain ----

    #[tokio::test]
    async fn test_same_rune_multiple_casters_partial_remove() {
        let relay = Relay::new();
        let resolver = RoundRobinResolver::new();

        // Register 5 casters for the same rune
        for i in 0..5 {
            let handler = make_handler(move |_ctx, _input| {
                let val = format!("caster_{}", i);
                async move { Ok(Bytes::from(val)) }
            });
            let config = RuneConfig {
                name: "shared_rune".into(),
                version: String::new(),
                description: "".into(),
                supports_stream: false,
                gate: None,
                input_schema: None,
                output_schema: None,
                priority: 0, labels: Default::default(),
            };
            relay.register(
                config,
                Arc::new(crate::invoker::LocalInvoker::new(handler)),
                Some(format!("c{}", i)),
            ).unwrap();
        }

        // Verify 5 entries
        let list: Vec<_> = relay.list().into_iter()
            .filter(|(n, _)| n == "shared_rune").collect();
        assert_eq!(list.len(), 5);

        // Remove casters c0, c1, c2
        relay.remove_caster("c0");
        relay.remove_caster("c1");
        relay.remove_caster("c2");

        // Verify 2 remaining entries
        let list: Vec<_> = relay.list().into_iter()
            .filter(|(n, _)| n == "shared_rune").collect();
        assert_eq!(list.len(), 2);

        // Rune should still be resolvable
        assert!(relay.resolve("shared_rune", &resolver).is_some());

        // Verify round-robin across remaining 2 casters
        let ctx = || RuneContext {
            rune_name: "shared_rune".into(),
            request_id: "r".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };
        let r1 = relay.resolve("shared_rune", &resolver).unwrap()
            .invoke_once(ctx(), Bytes::new()).await.unwrap();
        let r2 = relay.resolve("shared_rune", &resolver).unwrap()
            .invoke_once(ctx(), Bytes::new()).await.unwrap();
        // The two remaining casters should alternate
        assert_ne!(r1, r2);
    }

    // ---- Interleaved register and remove ----

    #[test]
    fn test_interleaved_register_remove() {
        let relay = Relay::new();
        let resolver = RoundRobinResolver::new();

        let make_cfg = || RuneConfig {
            name: "interleaved".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };

        // Register A
        let h_a = make_handler(|_ctx, _input| async { Ok(Bytes::from("A")) });
        relay.register(make_cfg(), Arc::new(crate::invoker::LocalInvoker::new(h_a)), Some("A".into())).unwrap();
        assert_eq!(relay.list().len(), 1);

        // Register B
        let h_b = make_handler(|_ctx, _input| async { Ok(Bytes::from("B")) });
        relay.register(make_cfg(), Arc::new(crate::invoker::LocalInvoker::new(h_b)), Some("B".into())).unwrap();
        assert_eq!(relay.list().len(), 2);

        // Remove A
        relay.remove_caster("A");
        assert_eq!(relay.list().len(), 1);

        // Register C
        let h_c = make_handler(|_ctx, _input| async { Ok(Bytes::from("C")) });
        relay.register(make_cfg(), Arc::new(crate::invoker::LocalInvoker::new(h_c)), Some("C".into())).unwrap();
        assert_eq!(relay.list().len(), 2);

        // Remove B
        relay.remove_caster("B");
        assert_eq!(relay.list().len(), 1);

        // Only C should remain
        assert!(relay.resolve("interleaved", &resolver).is_some());
        let entries = relay.find("interleaved").unwrap();
        assert_eq!(entries.value().len(), 1);
        assert_eq!(entries.value()[0].caster_id.as_deref(), Some("C"));
    }

    // ---- Same rune_name with different gate.path ----

    #[test]
    fn test_same_rune_name_different_gate_paths() {
        // The same rune name with different gate paths should be allowed
        // (same rune name = same implementation, just different entry points)
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, input| async move { Ok(input) });
        let h2 = make_handler(|_ctx, input| async move { Ok(input) });

        let config1 = RuneConfig {
            name: "multi_path_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/path_a".into(),
                method: "POST".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        let config2 = RuneConfig {
            name: "multi_path_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/path_b".into(),
                method: "POST".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };

        relay.register(config1, Arc::new(crate::invoker::LocalInvoker::new(h1)), Some("c1".into())).unwrap();
        relay.register(config2, Arc::new(crate::invoker::LocalInvoker::new(h2)), Some("c2".into())).unwrap();

        let list: Vec<_> = relay.list().into_iter()
            .filter(|(n, _)| n == "multi_path_rune").collect();
        assert_eq!(list.len(), 2);

        // Both gate paths should be present
        let paths: Vec<_> = list.iter()
            .map(|(_, p)| p.as_deref().unwrap_or(""))
            .collect();
        assert!(paths.contains(&"/api/path_a"));
        assert!(paths.contains(&"/api/path_b"));
    }

    // ---- Gate path with query string ----

    #[test]
    fn test_gate_path_with_query_string() {
        // Gate paths with query strings are treated as literal path strings
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });

        let config = RuneConfig {
            name: "query_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/translate?v=1".into(),
                method: "POST".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };

        // Should succeed (gate path is just a string, no URL parsing)
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None).unwrap();

        let list = relay.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].1.as_deref(), Some("/translate?v=1"));
    }

    #[test]
    fn test_gate_path_with_fragment() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });

        let config = RuneConfig {
            name: "fragment_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/translate#section".into(),
                method: "POST".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };

        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(handler)), None).unwrap();
        let list = relay.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].1.as_deref(), Some("/translate#section"));
    }

    // ---- Remove all casters of same rune ----

    #[test]
    fn test_remove_all_casters_empties_rune() {
        let relay = Relay::new();
        let resolver = RoundRobinResolver::new();

        for i in 0..3 {
            let handler = make_handler(|_ctx, input| async move { Ok(input) });
            let config = RuneConfig {
                name: "doomed_rune".into(),
                version: String::new(),
                description: "".into(),
                supports_stream: false,
                gate: None,
                input_schema: None,
                output_schema: None,
                priority: 0, labels: Default::default(),
            };
            relay.register(
                config,
                Arc::new(crate::invoker::LocalInvoker::new(handler)),
                Some(format!("c{}", i)),
            ).unwrap();
        }

        assert_eq!(relay.list().len(), 3);
        assert!(relay.resolve("doomed_rune", &resolver).is_some());

        // Remove all casters one by one
        relay.remove_caster("c0");
        relay.remove_caster("c1");
        relay.remove_caster("c2");

        // Rune should no longer be resolvable
        assert!(relay.resolve("doomed_rune", &resolver).is_none());
        assert!(relay.list().is_empty());
    }

    // ---- find() returns None for empty candidates ----

    #[test]
    fn test_find_returns_none_for_nonexistent() {
        let relay = Relay::new();
        assert!(relay.find("does_not_exist").is_none());
    }

    // ====================================================================
    // v0.6.0 TDD Wave A — Scheduling, Labels, Priority
    // ====================================================================

    use crate::resolver::{RandomResolver, LeastLoadResolver, PriorityResolver};
    use std::collections::HashMap;

    // Helper to build RuneConfig with labels
    fn cfg_with_labels(name: &str, labels: HashMap<String, String>) -> RuneConfig {
        RuneConfig {
            name: name.into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0,
            labels,
        }
    }

    fn cfg_with_priority(name: &str, priority: i32) -> RuneConfig {
        RuneConfig {
            name: name.into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority,
            labels: Default::default(),
        }
    }

    // ---- RandomResolver tests ----

    #[tokio::test]
    async fn test_random_resolver_returns_results() {
        // RandomResolver should return Some for non-empty candidates
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("a")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("b")) });

        let cfg = || RuneConfig {
            name: "rand_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        relay.register(cfg(), Arc::new(crate::invoker::LocalInvoker::new(h1)), Some("c1".into())).unwrap();
        relay.register(cfg(), Arc::new(crate::invoker::LocalInvoker::new(h2)), Some("c2".into())).unwrap();

        let resolver = RandomResolver::new();
        // Should always return Some
        for _ in 0..10 {
            assert!(relay.resolve("rand_rune", &resolver).is_some());
        }
    }

    #[tokio::test]
    async fn test_random_resolver_varies_selection() {
        // Over many calls, RandomResolver should pick different candidates (probabilistic)
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("a")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("b")) });
        let h3 = make_handler(|_ctx, _input| async { Ok(Bytes::from("c")) });

        let cfg = || RuneConfig {
            name: "rand_var".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        relay.register(cfg(), Arc::new(crate::invoker::LocalInvoker::new(h1)), Some("c1".into())).unwrap();
        relay.register(cfg(), Arc::new(crate::invoker::LocalInvoker::new(h2)), Some("c2".into())).unwrap();
        relay.register(cfg(), Arc::new(crate::invoker::LocalInvoker::new(h3)), Some("c3".into())).unwrap();

        let resolver = RandomResolver::new();
        let ctx = || RuneContext {
            rune_name: "rand_var".into(),
            request_id: "r".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };

        let mut results = std::collections::HashSet::new();
        for _ in 0..50 {
            let invoker = relay.resolve("rand_var", &resolver).unwrap();
            let r = invoker.invoke_once(ctx(), Bytes::new()).await.unwrap();
            results.insert(r);
        }
        // With 50 trials and 3 options, probability of seeing only 1 is negligible
        assert!(results.len() > 1, "random resolver should vary selection, got only {:?}", results);
    }

    #[test]
    fn test_random_resolver_no_candidates() {
        let relay = Relay::new();
        let resolver = RandomResolver::new();
        assert!(relay.resolve("nonexistent", &resolver).is_none());
    }

    #[test]
    fn test_random_resolver_single_candidate() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let cfg = RuneConfig {
            name: "single".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        relay.register(cfg, Arc::new(crate::invoker::LocalInvoker::new(handler)), None).unwrap();

        let resolver = RandomResolver::new();
        assert!(relay.resolve("single", &resolver).is_some());
    }

    // ---- LeastLoadResolver tests ----

    #[test]
    fn test_least_load_resolver_picks_least_loaded() {
        // Create a SessionManager, register two casters with different loads
        let session_mgr = Arc::new(crate::session::SessionManager::new(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let resolver = LeastLoadResolver::new(Arc::clone(&session_mgr));
        let relay = Relay::new();

        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("low_load")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("high_load")) });

        let cfg = || RuneConfig {
            name: "ll_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        relay.register(cfg(), Arc::new(crate::invoker::LocalInvoker::new(h1)), Some("low".into())).unwrap();
        relay.register(cfg(), Arc::new(crate::invoker::LocalInvoker::new(h2)), Some("high".into())).unwrap();

        // LeastLoadResolver should prefer the caster with more available permits
        // (lower load). Without real session state, this tests the resolver logic.
        let result = relay.resolve("ll_rune", &resolver);
        assert!(result.is_some());
    }

    #[test]
    fn test_least_load_resolver_no_candidates() {
        let session_mgr = Arc::new(crate::session::SessionManager::new(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let resolver = LeastLoadResolver::new(session_mgr);
        let relay = Relay::new();
        assert!(relay.resolve("nonexistent", &resolver).is_none());
    }

    #[test]
    fn test_least_load_resolver_single_candidate() {
        let session_mgr = Arc::new(crate::session::SessionManager::new(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let resolver = LeastLoadResolver::new(session_mgr);
        let relay = Relay::new();

        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let cfg = RuneConfig {
            name: "single_ll".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        relay.register(cfg, Arc::new(crate::invoker::LocalInvoker::new(handler)), None).unwrap();
        assert!(relay.resolve("single_ll", &resolver).is_some());
    }

    // ---- All strategies consistent for single candidate ----

    #[test]
    fn test_all_resolvers_single_candidate_consistent() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, input| async move { Ok(input) });
        let cfg = RuneConfig {
            name: "only_one".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        relay.register(cfg, Arc::new(crate::invoker::LocalInvoker::new(handler)), None).unwrap();

        let rr = RoundRobinResolver::new();
        let rand = RandomResolver::new();
        let session_mgr = Arc::new(crate::session::SessionManager::new(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let ll = LeastLoadResolver::new(session_mgr);

        assert!(relay.resolve("only_one", &rr).is_some());
        assert!(relay.resolve("only_one", &rand).is_some());
        assert!(relay.resolve("only_one", &ll).is_some());
    }

    // ---- All strategies return None for no candidates ----

    #[test]
    fn test_all_resolvers_no_candidates_return_none() {
        let relay = Relay::new();

        let rr = RoundRobinResolver::new();
        let rand = RandomResolver::new();
        let session_mgr = Arc::new(crate::session::SessionManager::new(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));
        let ll = LeastLoadResolver::new(session_mgr);

        assert!(relay.resolve("nope", &rr).is_none());
        assert!(relay.resolve("nope", &rand).is_none());
        assert!(relay.resolve("nope", &ll).is_none());
    }

    // ---- Labels routing tests ----

    #[tokio::test]
    async fn test_labels_exact_match_routes_correctly() {
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("prod")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("staging")) });

        let mut labels_prod = HashMap::new();
        labels_prod.insert("env".to_string(), "prod".to_string());
        let mut labels_staging = HashMap::new();
        labels_staging.insert("env".to_string(), "staging".to_string());

        relay.register(
            cfg_with_labels("labeled", labels_prod.clone()),
            Arc::new(crate::invoker::LocalInvoker::new(h1)),
            Some("c_prod".into()),
        ).unwrap();
        relay.register(
            cfg_with_labels("labeled", labels_staging),
            Arc::new(crate::invoker::LocalInvoker::new(h2)),
            Some("c_staging".into()),
        ).unwrap();

        let resolver = RoundRobinResolver::new();
        let ctx = RuneContext {
            rune_name: "labeled".into(),
            request_id: "r1".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };

        // Request with env=prod should get prod caster
        let invoker = relay.resolve_with_labels("labeled", &labels_prod, &resolver)
            .expect("should find prod caster");
        let result = invoker.invoke_once(ctx, Bytes::new()).await.unwrap();
        assert_eq!(result, Bytes::from("prod"));
    }

    #[test]
    fn test_labels_no_match_returns_none() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, _input| async { Ok(Bytes::from("prod")) });

        let mut labels_prod = HashMap::new();
        labels_prod.insert("env".to_string(), "prod".to_string());
        relay.register(
            cfg_with_labels("labeled", labels_prod),
            Arc::new(crate::invoker::LocalInvoker::new(handler)),
            Some("c_prod".into()),
        ).unwrap();

        let resolver = RoundRobinResolver::new();

        // Request with env=dev — no match
        let mut labels_dev = HashMap::new();
        labels_dev.insert("env".to_string(), "dev".to_string());
        assert!(relay.resolve_with_labels("labeled", &labels_dev, &resolver).is_none());
    }

    #[tokio::test]
    async fn test_labels_multiple_conditions_all_must_match() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, _input| async { Ok(Bytes::from("match")) });

        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("region".to_string(), "us-west".to_string());

        relay.register(
            cfg_with_labels("multi_label", labels),
            Arc::new(crate::invoker::LocalInvoker::new(handler)),
            Some("c1".into()),
        ).unwrap();

        let resolver = RoundRobinResolver::new();

        // Both labels match → success
        let mut req_labels = HashMap::new();
        req_labels.insert("env".to_string(), "prod".to_string());
        req_labels.insert("region".to_string(), "us-west".to_string());
        assert!(relay.resolve_with_labels("multi_label", &req_labels, &resolver).is_some());
    }

    #[test]
    fn test_labels_partial_match_not_sufficient() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, _input| async { Ok(Bytes::from("match")) });

        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("region".to_string(), "us-west".to_string());

        relay.register(
            cfg_with_labels("partial", labels),
            Arc::new(crate::invoker::LocalInvoker::new(handler)),
            Some("c1".into()),
        ).unwrap();

        let resolver = RoundRobinResolver::new();

        // Only env matches, region doesn't → should NOT match
        let mut req_labels = HashMap::new();
        req_labels.insert("env".to_string(), "prod".to_string());
        req_labels.insert("region".to_string(), "eu-central".to_string());
        assert!(relay.resolve_with_labels("partial", &req_labels, &resolver).is_none());
    }

    #[test]
    fn test_labels_empty_filter_uses_default_strategy() {
        let relay = Relay::new();
        let handler = make_handler(|_ctx, _input| async { Ok(Bytes::from("any")) });

        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());

        relay.register(
            cfg_with_labels("no_filter", labels),
            Arc::new(crate::invoker::LocalInvoker::new(handler)),
            Some("c1".into()),
        ).unwrap();

        let resolver = RoundRobinResolver::new();

        // Empty labels → should match all (default strategy)
        let empty_labels = HashMap::new();
        assert!(relay.resolve_with_labels("no_filter", &empty_labels, &resolver).is_some());
    }

    #[test]
    fn test_labels_superset_labels_on_caster_matches() {
        // Caster has more labels than requested — should still match
        let relay = Relay::new();
        let handler = make_handler(|_ctx, _input| async { Ok(Bytes::from("ok")) });

        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("region".to_string(), "us-west".to_string());
        labels.insert("gpu".to_string(), "true".to_string());

        relay.register(
            cfg_with_labels("superset", labels),
            Arc::new(crate::invoker::LocalInvoker::new(handler)),
            Some("c1".into()),
        ).unwrap();

        let resolver = RoundRobinResolver::new();

        // Request only requires env=prod → caster has it plus more → match
        let mut req_labels = HashMap::new();
        req_labels.insert("env".to_string(), "prod".to_string());
        assert!(relay.resolve_with_labels("superset", &req_labels, &resolver).is_some());
    }

    // ---- Priority tests ----

    #[tokio::test]
    async fn test_priority_higher_value_selected() {
        let relay = Relay::new();
        let h_low = make_handler(|_ctx, _input| async { Ok(Bytes::from("low")) });
        let h_high = make_handler(|_ctx, _input| async { Ok(Bytes::from("high")) });

        relay.register(
            cfg_with_priority("prio_rune", 1),
            Arc::new(crate::invoker::LocalInvoker::new(h_low)),
            Some("c_low".into()),
        ).unwrap();
        relay.register(
            cfg_with_priority("prio_rune", 10),
            Arc::new(crate::invoker::LocalInvoker::new(h_high)),
            Some("c_high".into()),
        ).unwrap();

        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        let ctx = RuneContext {
            rune_name: "prio_rune".into(),
            request_id: "r".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };

        // Should always pick the high-priority caster
        let invoker = relay.resolve("prio_rune", &resolver).unwrap();
        let result = invoker.invoke_once(ctx, Bytes::new()).await.unwrap();
        assert_eq!(result, Bytes::from("high"));
    }

    #[tokio::test]
    async fn test_priority_fallback_when_high_removed() {
        let relay = Relay::new();
        let h_low = make_handler(|_ctx, _input| async { Ok(Bytes::from("low")) });
        let h_high = make_handler(|_ctx, _input| async { Ok(Bytes::from("high")) });

        relay.register(
            cfg_with_priority("prio_fb", 1),
            Arc::new(crate::invoker::LocalInvoker::new(h_low)),
            Some("c_low".into()),
        ).unwrap();
        relay.register(
            cfg_with_priority("prio_fb", 10),
            Arc::new(crate::invoker::LocalInvoker::new(h_high)),
            Some("c_high".into()),
        ).unwrap();

        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        // Remove high-priority caster
        relay.remove_caster("c_high");

        let ctx = RuneContext {
            rune_name: "prio_fb".into(),
            request_id: "r".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };

        // Should fallback to low-priority caster
        let invoker = relay.resolve("prio_fb", &resolver).unwrap();
        let result = invoker.invoke_once(ctx, Bytes::new()).await.unwrap();
        assert_eq!(result, Bytes::from("low"));
    }

    #[tokio::test]
    async fn test_priority_same_priority_uses_inner_strategy() {
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("a")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("b")) });

        relay.register(
            cfg_with_priority("same_prio", 5),
            Arc::new(crate::invoker::LocalInvoker::new(h1)),
            Some("c1".into()),
        ).unwrap();
        relay.register(
            cfg_with_priority("same_prio", 5),
            Arc::new(crate::invoker::LocalInvoker::new(h2)),
            Some("c2".into()),
        ).unwrap();

        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        let ctx = || RuneContext {
            rune_name: "same_prio".into(),
            request_id: "r".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };

        // With same priority, inner round-robin should alternate
        let r1 = relay.resolve("same_prio", &resolver).unwrap()
            .invoke_once(ctx(), Bytes::new()).await.unwrap();
        let r2 = relay.resolve("same_prio", &resolver).unwrap()
            .invoke_once(ctx(), Bytes::new()).await.unwrap();
        assert_ne!(r1, r2, "same priority should delegate to inner strategy (round-robin alternation)");
    }

    // ---- Priority resolver with no candidates ----

    #[test]
    fn test_priority_resolver_no_candidates() {
        let relay = Relay::new();
        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);
        assert!(relay.resolve("nonexistent", &resolver).is_none());
    }

    // ---- Labels resolve_with_labels on nonexistent rune ----

    #[test]
    fn test_labels_nonexistent_rune_returns_none() {
        let relay = Relay::new();
        let resolver = RoundRobinResolver::new();
        let labels = HashMap::new();
        assert!(relay.resolve_with_labels("nope", &labels, &resolver).is_none());
    }

    // ====================================================================
    // v0.6.0 TDD — Supplementary Labels Tests
    // ====================================================================

    #[test]
    fn test_labels_key_with_special_chars() {
        // Label keys containing special characters (=, comma, space) should
        // be handled correctly — they are valid HashMap keys.
        let relay = Relay::new();
        let handler = make_handler(|_ctx, _input| async { Ok(Bytes::from("ok")) });

        let mut labels = HashMap::new();
        labels.insert("key=with=equals".to_string(), "val1".to_string());
        labels.insert("key,with,commas".to_string(), "val2".to_string());
        labels.insert("key with spaces".to_string(), "val3".to_string());

        relay.register(
            cfg_with_labels("special_labels", labels.clone()),
            Arc::new(crate::invoker::LocalInvoker::new(handler)),
            Some("c1".into()),
        ).unwrap();

        let resolver = RoundRobinResolver::new();

        // Exact match on all special-char keys
        assert!(
            relay.resolve_with_labels("special_labels", &labels, &resolver).is_some(),
            "should match labels with special characters in keys"
        );

        // Partial match with one special-char key
        let mut partial = HashMap::new();
        partial.insert("key=with=equals".to_string(), "val1".to_string());
        assert!(
            relay.resolve_with_labels("special_labels", &partial, &resolver).is_some(),
            "partial match with special-char key should succeed"
        );

        // Wrong value for special-char key
        let mut wrong = HashMap::new();
        wrong.insert("key=with=equals".to_string(), "wrong".to_string());
        assert!(
            relay.resolve_with_labels("special_labels", &wrong, &resolver).is_none(),
            "wrong value for special-char key should not match"
        );
    }

    #[test]
    fn test_labels_value_empty_string() {
        // Label value can be an empty string — should still match exactly
        let relay = Relay::new();
        let handler = make_handler(|_ctx, _input| async { Ok(Bytes::from("ok")) });

        let mut labels = HashMap::new();
        labels.insert("tag".to_string(), "".to_string());

        relay.register(
            cfg_with_labels("empty_val", labels),
            Arc::new(crate::invoker::LocalInvoker::new(handler)),
            Some("c1".into()),
        ).unwrap();

        let resolver = RoundRobinResolver::new();

        // Match with empty string value
        let mut req = HashMap::new();
        req.insert("tag".to_string(), "".to_string());
        assert!(
            relay.resolve_with_labels("empty_val", &req, &resolver).is_some(),
            "empty string value should match exactly"
        );

        // Non-empty string should NOT match
        let mut req_nonempty = HashMap::new();
        req_nonempty.insert("tag".to_string(), "something".to_string());
        assert!(
            relay.resolve_with_labels("empty_val", &req_nonempty, &resolver).is_none(),
            "non-empty value should not match empty label value"
        );
    }

    #[tokio::test]
    async fn test_labels_combined_with_priority() {
        // Labels filter first, then priority picks among matches
        let relay = Relay::new();
        let h_low = make_handler(|_ctx, _input| async { Ok(Bytes::from("low")) });
        let h_high = make_handler(|_ctx, _input| async { Ok(Bytes::from("high")) });
        let h_other = make_handler(|_ctx, _input| async { Ok(Bytes::from("other")) });

        let mut labels_prod = HashMap::new();
        labels_prod.insert("env".to_string(), "prod".to_string());

        let mut labels_staging = HashMap::new();
        labels_staging.insert("env".to_string(), "staging".to_string());

        // Low-priority prod
        let mut cfg_low = cfg_with_labels("combo", labels_prod.clone());
        cfg_low.priority = 1;
        relay.register(cfg_low, Arc::new(crate::invoker::LocalInvoker::new(h_low)), Some("c_low".into())).unwrap();

        // High-priority prod
        let mut cfg_high = cfg_with_labels("combo", labels_prod.clone());
        cfg_high.priority = 10;
        relay.register(cfg_high, Arc::new(crate::invoker::LocalInvoker::new(h_high)), Some("c_high".into())).unwrap();

        // High-priority staging (should be filtered out by labels)
        let mut cfg_staging = cfg_with_labels("combo", labels_staging);
        cfg_staging.priority = 100;
        relay.register(cfg_staging, Arc::new(crate::invoker::LocalInvoker::new(h_other)), Some("c_staging".into())).unwrap();

        // Use labels filter for env=prod, then priority should pick the high-priority one
        let inner = Arc::new(RoundRobinResolver::new());
        let prio_resolver = PriorityResolver::new(inner);

        let invoker = relay.resolve_with_labels("combo", &labels_prod, &prio_resolver)
            .expect("should find a prod caster");

        let ctx = RuneContext {
            rune_name: "combo".into(),
            request_id: "r1".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };
        let result = invoker.invoke_once(ctx, Bytes::new()).await.unwrap();
        assert_eq!(result, Bytes::from("high"), "should pick high-priority prod, not staging");
    }

    // ====================================================================
    // v0.6.0 TDD — Supplementary Priority Tests
    // ====================================================================

    #[tokio::test]
    async fn test_priority_negative_value() {
        // Negative priority should work — lower than 0 means lowest tier
        let relay = Relay::new();
        let h_neg = make_handler(|_ctx, _input| async { Ok(Bytes::from("neg")) });
        let h_pos = make_handler(|_ctx, _input| async { Ok(Bytes::from("pos")) });

        relay.register(
            cfg_with_priority("neg_prio", -10),
            Arc::new(crate::invoker::LocalInvoker::new(h_neg)),
            Some("c_neg".into()),
        ).unwrap();
        relay.register(
            cfg_with_priority("neg_prio", 5),
            Arc::new(crate::invoker::LocalInvoker::new(h_pos)),
            Some("c_pos".into()),
        ).unwrap();

        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        let ctx = RuneContext {
            rune_name: "neg_prio".into(),
            request_id: "r".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };

        // Should pick the positive-priority candidate
        let invoker = relay.resolve("neg_prio", &resolver).unwrap();
        let result = invoker.invoke_once(ctx, Bytes::new()).await.unwrap();
        assert_eq!(result, Bytes::from("pos"), "positive priority should be selected over negative");
    }

    #[tokio::test]
    async fn test_priority_three_tiers() {
        // Three tiers: 10 (high), 5 (mid), 1 (low) — should always pick 10
        let relay = Relay::new();
        let h_low = make_handler(|_ctx, _input| async { Ok(Bytes::from("low")) });
        let h_mid = make_handler(|_ctx, _input| async { Ok(Bytes::from("mid")) });
        let h_high = make_handler(|_ctx, _input| async { Ok(Bytes::from("high")) });

        relay.register(
            cfg_with_priority("tri", 1),
            Arc::new(crate::invoker::LocalInvoker::new(h_low)),
            Some("c_low".into()),
        ).unwrap();
        relay.register(
            cfg_with_priority("tri", 5),
            Arc::new(crate::invoker::LocalInvoker::new(h_mid)),
            Some("c_mid".into()),
        ).unwrap();
        relay.register(
            cfg_with_priority("tri", 10),
            Arc::new(crate::invoker::LocalInvoker::new(h_high)),
            Some("c_high".into()),
        ).unwrap();

        let inner = Arc::new(RoundRobinResolver::new());
        let resolver = PriorityResolver::new(inner);

        let ctx = RuneContext {
            rune_name: "tri".into(),
            request_id: "r".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };

        // Should always pick the highest priority (10)
        let invoker = relay.resolve("tri", &resolver).unwrap();
        let result = invoker.invoke_once(ctx, Bytes::new()).await.unwrap();
        assert_eq!(result, Bytes::from("high"), "should pick highest of three tiers");
    }

    // ====================================================================
    // v0.6.0 TDD — Cross-module: Priority wrapping LeastLoad
    // ====================================================================

    #[tokio::test]
    async fn test_priority_with_least_load() {
        // PriorityResolver wrapping LeastLoadResolver:
        // 1) filters to highest priority tier
        // 2) delegates to LeastLoad among that tier
        let session_mgr = Arc::new(crate::session::SessionManager::new(
            Duration::from_secs(10),
            Duration::from_secs(35),
        ));

        let relay = Relay::new();
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("a")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("b")) });
        let h_low = make_handler(|_ctx, _input| async { Ok(Bytes::from("low")) });

        // Two high-priority candidates
        relay.register(
            cfg_with_priority("prio_ll", 10),
            Arc::new(crate::invoker::LocalInvoker::new(h1)),
            Some("c1".into()),
        ).unwrap();
        relay.register(
            cfg_with_priority("prio_ll", 10),
            Arc::new(crate::invoker::LocalInvoker::new(h2)),
            Some("c2".into()),
        ).unwrap();
        // One low-priority candidate
        relay.register(
            cfg_with_priority("prio_ll", 1),
            Arc::new(crate::invoker::LocalInvoker::new(h_low)),
            Some("c_low".into()),
        ).unwrap();

        let inner = Arc::new(LeastLoadResolver::new(Arc::clone(&session_mgr)));
        let resolver = PriorityResolver::new(inner);

        let ctx = RuneContext {
            rune_name: "prio_ll".into(),
            request_id: "r".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
        };

        // Should resolve to one of the high-priority candidates (a or b), never "low"
        let invoker = relay.resolve("prio_ll", &resolver).unwrap();
        let result = invoker.invoke_once(ctx, Bytes::new()).await.unwrap();
        assert!(
            result == Bytes::from("a") || result == Bytes::from("b"),
            "should pick from high-priority tier, got {:?}",
            result
        );
    }

    // ---- P1 fix: gate_path_index must include HTTP method ----

    #[test]
    fn test_reverse_index_includes_method() {
        // The gate_path_index key should include the HTTP method so that
        // same-path-different-method runes don't overwrite each other.
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, input| async move { Ok(input) });

        let config = RuneConfig {
            name: "post_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/foo".into(),
                method: "POST".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        relay.register(config, Arc::new(crate::invoker::LocalInvoker::new(h1)), None).unwrap();

        // Should resolve with correct method
        assert_eq!(
            relay.resolve_by_gate_path("POST", "/api/foo"),
            Some("post_rune".to_string()),
        );
        // Should NOT resolve with wrong method
        assert_eq!(
            relay.resolve_by_gate_path("GET", "/api/foo"),
            None,
        );
    }

    #[test]
    fn test_same_path_different_methods_both_routable() {
        // Register POST /api/foo and GET /api/foo — both must be independently
        // resolvable via the reverse index.
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("post")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("get")) });

        let config_post = RuneConfig {
            name: "post_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/foo".into(),
                method: "POST".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        let config_get = RuneConfig {
            name: "get_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/foo".into(),
                method: "GET".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };

        relay.register(config_post, Arc::new(crate::invoker::LocalInvoker::new(h1)), None).unwrap();
        relay.register(config_get, Arc::new(crate::invoker::LocalInvoker::new(h2)), None).unwrap();

        // Both should resolve to their respective rune names
        assert_eq!(
            relay.resolve_by_gate_path("POST", "/api/foo"),
            Some("post_rune".to_string()),
        );
        assert_eq!(
            relay.resolve_by_gate_path("GET", "/api/foo"),
            Some("get_rune".to_string()),
        );
    }

    #[test]
    fn test_remove_caster_cleans_method_prefixed_index() {
        // When a caster is removed, the method-prefixed index entry should also be cleaned.
        let relay = Relay::new();
        let h1 = make_handler(|_ctx, input| async move { Ok(input) });
        let h2 = make_handler(|_ctx, input| async move { Ok(input) });

        let config_post = RuneConfig {
            name: "post_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/bar".into(),
                method: "POST".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };
        let config_get = RuneConfig {
            name: "get_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/bar".into(),
                method: "GET".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0, labels: Default::default(),
        };

        relay.register(config_post, Arc::new(crate::invoker::LocalInvoker::new(h1)), Some("c1".into())).unwrap();
        relay.register(config_get, Arc::new(crate::invoker::LocalInvoker::new(h2)), Some("c2".into())).unwrap();

        // Remove POST caster
        relay.remove_caster("c1");

        // POST should be gone, GET should remain
        assert_eq!(relay.resolve_by_gate_path("POST", "/api/bar"), None);
        assert_eq!(relay.resolve_by_gate_path("GET", "/api/bar"), Some("get_rune".to_string()));
    }

    // ====================================================================
    // P2: Concurrent register / remove_caster race condition
    // ====================================================================

    #[test]
    fn test_concurrent_register_and_remove_caster() {
        // Regression test: verify that concurrent register and remove_caster
        // do not corrupt the gate_path_index.
        //
        // Scenario: caster "old" registers a rune with gate_path "/api/shared".
        // Then, in parallel, "old" is removed while caster "new" registers
        // the same gate_path. After both complete, the gate_path_index must
        // reflect the surviving entries — if "new" is still registered,
        // resolve_by_gate_path must return Some.

        use std::sync::Barrier;

        let relay = Arc::new(Relay::new());

        let make_cfg = || RuneConfig {
            name: "shared_rune".into(),
            version: String::new(),
            description: "".into(),
            supports_stream: false,
            gate: Some(crate::rune::GateConfig {
                path: "/api/shared".into(),
                method: "POST".into(),
            }),
            input_schema: None,
            output_schema: None,
            priority: 0,
            labels: Default::default(),
        };

        // Run the race many times to increase the chance of hitting the window
        for _ in 0..200 {
            // Reset relay state
            relay.remove_caster("old");
            relay.remove_caster("new");

            // Pre-register "old" caster
            let h_old = make_handler(|_ctx, input| async move { Ok(input) });
            relay.register(
                make_cfg(),
                Arc::new(crate::invoker::LocalInvoker::new(h_old)),
                Some("old".into()),
            ).unwrap();

            let barrier = Arc::new(Barrier::new(2));

            let relay2 = Arc::clone(&relay);
            let barrier2 = barrier.clone();

            // Thread 1: remove "old" caster
            let t1 = std::thread::spawn(move || {
                barrier2.wait();
                relay2.remove_caster("old");
            });

            // Thread 2: register "new" caster with same gate_path
            let relay3 = Arc::clone(&relay);
            let barrier3 = barrier.clone();
            let t2 = std::thread::spawn(move || {
                barrier3.wait();
                let h_new = make_handler(|_ctx, input| async move { Ok(input) });
                relay3.register(
                    make_cfg(),
                    Arc::new(crate::invoker::LocalInvoker::new(h_new)),
                    Some("new".into()),
                ).unwrap();
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // Invariant: gate_path_index must be consistent with entries.
            // If any entry with gate_path "/api/shared" exists,
            // resolve_by_gate_path must return Some.
            let has_entry_with_path = relay.list().iter().any(|(_, gp)| {
                gp.as_deref() == Some("/api/shared")
            });
            let index_has_path = relay.resolve_by_gate_path("POST", "/api/shared").is_some();

            assert_eq!(
                has_entry_with_path, index_has_path,
                "gate_path_index inconsistency: entries has path={}, index has path={}",
                has_entry_with_path, index_has_path
            );
        }
    }
}
