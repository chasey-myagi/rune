use crate::auth::KeyVerifier;
use crate::config::{AppConfig, RetryConfig};
use crate::grpc_service::RuneGrpcService;
use crate::invoker::{LocalInvoker, LocalStreamInvoker};
use crate::relay::Relay;
use crate::resolver::{
    HealthAwareResolver, LeastLoadResolver, PriorityResolver, RandomResolver, Resolver,
    RoundRobinResolver,
};
use crate::rune::{RuneConfig, RuneHandler, StreamRuneHandler};
use crate::session::SessionManager;
use rune_proto::rune_service_server::RuneServiceServer;
use std::sync::Arc;

fn resolver_from_strategy(strategy: &str, session_mgr: &Arc<SessionManager>) -> Arc<dyn Resolver> {
    let inner: Arc<dyn Resolver> = match strategy {
        "random" => Arc::new(RandomResolver),
        "least_load" => Arc::new(LeastLoadResolver::new(Arc::clone(session_mgr))),
        "priority" => Arc::new(PriorityResolver::new(Arc::new(RoundRobinResolver::new()))),
        _ => Arc::new(RoundRobinResolver::new()), // "round_robin" and fallback
    };

    Arc::new(HealthAwareResolver::new(inner, Arc::clone(session_mgr)))
}

fn build_relay(retry: &RetryConfig) -> Arc<Relay> {
    if retry.enabled {
        Arc::new(Relay::with_retry(retry.clone()))
    } else {
        Arc::new(Relay::new())
    }
}

pub struct App {
    pub relay: Arc<Relay>,
    pub resolver: Arc<dyn Resolver>,
    pub session_mgr: Arc<SessionManager>,
    pub config: AppConfig,
}

/// Type alias for backward compatibility — `RunningApp` is identical to `App`.
pub type RunningApp = App;

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl App {
    pub fn new() -> Self {
        let config = AppConfig::default();
        let session_mgr = Arc::new(SessionManager::new_dev_with_default_max_concurrent(
            config.heartbeat_interval(),
            config.heartbeat_timeout(),
            config.rate_limit.default_caster_max_concurrent as usize,
        ));
        let resolver = resolver_from_strategy(&config.resolver.strategy, &session_mgr);
        Self {
            relay: build_relay(&config.retry),
            resolver,
            session_mgr,
            config,
        }
    }

    pub fn with_config(config: AppConfig) -> Self {
        let session_mgr = Arc::new(SessionManager::new_dev_with_default_max_concurrent(
            config.heartbeat_interval(),
            config.heartbeat_timeout(),
            config.rate_limit.default_caster_max_concurrent as usize,
        ));
        let resolver = resolver_from_strategy(&config.resolver.strategy, &session_mgr);
        Self {
            relay: build_relay(&config.retry),
            resolver,
            session_mgr,
            config,
        }
    }

    pub fn with_config_and_auth(config: AppConfig, key_verifier: Arc<dyn KeyVerifier>) -> Self {
        let dev_mode = config.server.dev_mode;
        let session_mgr = Arc::new(SessionManager::with_auth_and_default_max_concurrent(
            config.heartbeat_interval(),
            config.heartbeat_timeout(),
            key_verifier,
            dev_mode,
            config.rate_limit.default_caster_max_concurrent as usize,
        ));
        let resolver = resolver_from_strategy(&config.resolver.strategy, &session_mgr);
        Self {
            relay: build_relay(&config.retry),
            resolver,
            session_mgr,
            config,
        }
    }

    pub fn rune(
        &mut self,
        config: RuneConfig,
        handler: RuneHandler,
    ) -> Result<&mut Self, anyhow::Error> {
        self.relay
            .register(config, Arc::new(LocalInvoker::new(handler)), None)
            .map_err(|e| anyhow::anyhow!("route conflict in local rune registration: {e}"))?;
        Ok(self)
    }

    pub fn stream_rune(
        &mut self,
        config: RuneConfig,
        handler: impl StreamRuneHandler,
    ) -> Result<&mut Self, anyhow::Error> {
        self.relay
            .register(
                config,
                Arc::new(LocalStreamInvoker::new(Arc::new(handler))),
                None,
            )
            .map_err(|e| {
                anyhow::anyhow!("route conflict in local stream rune registration: {e}")
            })?;
        Ok(self)
    }

    pub fn set_resolver(&mut self, r: impl Resolver + 'static) -> &mut Self {
        self.resolver = Arc::new(r);
        self
    }

    /// Start the gRPC server and block until shutdown signal (SIGINT / ctrl-c).
    pub async fn run(self) -> anyhow::Result<()> {
        let grpc_addr = self.config.grpc_addr();

        // Periodic cleanup of idle circuit breakers to prevent unbounded
        // memory growth from transient caster IDs.
        let (cb_shutdown_tx, mut cb_shutdown_rx) = tokio::sync::watch::channel(false);
        let cb_handle = if let Some(cb_registry) = self.relay.circuit_breaker_registry() {
            let cleanup_interval = std::time::Duration::from_secs(
                self.config.retry.circuit_breaker.cleanup_interval_secs,
            );
            let cleanup_idle = std::time::Duration::from_secs(
                self.config.retry.circuit_breaker.cleanup_idle_timeout_secs,
            );
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(cleanup_interval);
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            cb_registry.cleanup_stale(cleanup_idle);
                            tracing::debug!("circuit breaker registry stale cleanup completed");
                        }
                        _ = cb_shutdown_rx.changed() => break,
                    }
                }
                tracing::debug!("circuit breaker cleanup task exited");
            }))
        } else {
            None
        };

        let grpc_service = RuneGrpcService {
            relay: Arc::clone(&self.relay),
            session_mgr: Arc::clone(&self.session_mgr),
        };

        tracing::info!("gRPC listening on {}", grpc_addr);

        tonic::transport::Server::builder()
            .add_service(RuneServiceServer::new(grpc_service))
            .serve_with_shutdown(grpc_addr, async {
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{signal, SignalKind};
                    let mut sigterm = match signal(SignalKind::terminate()) {
                        Ok(s) => Some(s),
                        Err(e) => {
                            tracing::warn!("failed to register SIGTERM handler: {}", e);
                            None
                        }
                    };
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {}
                        _ = async {
                            if let Some(ref mut s) = sigterm {
                                s.recv().await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        } => {}
                    }
                }
                #[cfg(not(unix))]
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("shutdown signal received");
                let _ = cb_shutdown_tx.send(true);
            })
            .await?;

        // Gracefully wait for the circuit breaker cleanup task to exit.
        if let Some(h) = cb_handle {
            if tokio::time::timeout(std::time::Duration::from_secs(5), h)
                .await
                .is_err()
            {
                tracing::warn!("circuit breaker cleanup task did not shut down within 5s");
            }
        }

        Ok(())
    }

    /// Finalize configuration and return components for custom server setup.
    /// Returns `Self` (aliased as `RunningApp` for backward compatibility).
    pub fn build(self) -> App {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::RoundRobinResolver;
    use crate::rune::{
        make_handler, GateConfig, RuneConfig, RuneContext, RuneError, StreamRuneHandler,
        StreamSender,
    };
    use crate::session::HealthStatusLevel;
    use bytes::Bytes;
    use std::time::Duration;

    fn echo_config(name: &str) -> RuneConfig {
        RuneConfig {
            name: name.into(),
            version: "1.0".into(),
            description: format!("{} description", name),
            supports_stream: false,
            gate: None,
            input_schema: None,
            output_schema: None,
            priority: 0,
            labels: Default::default(),
        }
    }

    fn echo_handler() -> RuneHandler {
        make_handler(|_ctx, input| async move { Ok(input) })
    }

    fn test_ctx(rune_name: &str) -> RuneContext {
        RuneContext {
            rune_name: rune_name.into(),
            request_id: "r-1".into(),
            context: Default::default(),
            timeout: Duration::from_secs(30),
            disable_runtime_retry: false,
        }
    }

    // ========================================================================
    // App::new() defaults
    // ========================================================================

    #[test]
    fn app_new_has_empty_relay() {
        let app = App::new();
        let list = app.relay.list();
        assert!(list.is_empty(), "fresh App should have no runes registered");
    }

    #[test]
    fn app_new_has_default_config() {
        let app = App::new();
        assert_eq!(app.config.server.grpc_port, 50070);
        assert_eq!(app.config.session.heartbeat_interval_secs, 10);
        assert_eq!(app.config.session.heartbeat_timeout_secs, 35);
    }

    #[test]
    fn app_new_session_manager_has_zero_casters() {
        let app = App::new();
        assert_eq!(app.session_mgr.caster_count(), 0);
    }

    #[test]
    fn test_fix_app_new_default_resolver_prefers_healthy_caster() {
        let app = App::new();
        app.session_mgr.insert_test_caster("degraded", 1);
        app.session_mgr.insert_test_caster("healthy", 1);
        app.session_mgr
            .set_test_health("degraded", HealthStatusLevel::Degraded);

        app.relay
            .register(
                echo_config("echo"),
                Arc::new(LocalInvoker::new(echo_handler())),
                Some("degraded".into()),
            )
            .unwrap();
        app.relay
            .register(
                echo_config("echo"),
                Arc::new(LocalInvoker::new(echo_handler())),
                Some("healthy".into()),
            )
            .unwrap();

        let picked = app
            .relay
            .select_entry("echo", &*app.resolver)
            .expect("resolver should pick a candidate");
        assert_eq!(
            picked.caster_id.as_deref(),
            Some("healthy"),
            "default resolver should filter out degraded candidates when a healthy caster exists"
        );
    }

    // ========================================================================
    // App::with_config()
    // ========================================================================

    #[test]
    fn app_with_config_uses_custom_values() {
        let mut config = AppConfig::default();
        config.server.grpc_port = 9999;
        config.session.heartbeat_interval_secs = 5;
        config.session.heartbeat_timeout_secs = 20;
        let app = App::with_config(config);
        assert_eq!(app.config.server.grpc_port, 9999);
        assert_eq!(app.session_mgr.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(app.session_mgr.heartbeat_timeout, Duration::from_secs(20));
    }

    // ========================================================================
    // register_rune — single
    // ========================================================================

    #[tokio::test]
    async fn app_register_rune_resolvable() {
        let mut app = App::new();
        app.rune(echo_config("echo"), echo_handler()).unwrap();

        let resolver = RoundRobinResolver::new();
        let invoker = app
            .relay
            .resolve("echo", &resolver)
            .expect("rune should be resolvable");
        let result = invoker
            .invoke_once(test_ctx("echo"), Bytes::from("hi"))
            .await
            .unwrap();
        assert_eq!(result, Bytes::from("hi"));
    }

    #[test]
    fn app_register_rune_appears_in_list() {
        let mut app = App::new();
        app.rune(echo_config("my_rune"), echo_handler()).unwrap();
        let list = app.relay.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "my_rune");
        assert!(list[0].1.is_none(), "no gate configured");
    }

    // ========================================================================
    // register_rune — multiple
    // ========================================================================

    #[test]
    fn app_register_multiple_runes_all_listed() {
        let mut app = App::new();
        app.rune(echo_config("rune_a"), echo_handler()).unwrap();
        app.rune(echo_config("rune_b"), echo_handler()).unwrap();
        app.rune(echo_config("rune_c"), echo_handler()).unwrap();
        let list = app.relay.list();
        assert_eq!(list.len(), 3);
        let names: Vec<&str> = list.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"rune_a"));
        assert!(names.contains(&"rune_b"));
        assert!(names.contains(&"rune_c"));
    }

    // ========================================================================
    // register_stream_rune
    // ========================================================================

    struct TestStreamHandler;

    #[async_trait::async_trait]
    impl StreamRuneHandler for TestStreamHandler {
        async fn execute(
            &self,
            _ctx: RuneContext,
            _input: Bytes,
            tx: StreamSender,
        ) -> Result<(), RuneError> {
            tx.emit(Bytes::from("stream-data")).await?;
            tx.end().await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn app_register_stream_rune_invocable() {
        let mut app = App::new();
        let cfg = RuneConfig {
            name: "streamer".into(),
            supports_stream: true,
            ..echo_config("streamer")
        };
        app.stream_rune(cfg, TestStreamHandler).unwrap();

        let resolver = RoundRobinResolver::new();
        let invoker = app
            .relay
            .resolve("streamer", &resolver)
            .expect("stream rune resolvable");
        let mut rx = invoker
            .invoke_stream(test_ctx("streamer"), Bytes::new())
            .await
            .unwrap();
        let chunk = rx.recv().await.unwrap().unwrap();
        assert_eq!(chunk, Bytes::from("stream-data"));
        assert!(rx.recv().await.is_none());
    }

    // ========================================================================
    // gate_path registration
    // ========================================================================

    #[test]
    fn app_register_rune_with_gate_path() {
        let mut app = App::new();
        let cfg = RuneConfig {
            gate: Some(GateConfig {
                path: "/api/echo".into(),
                method: "POST".into(),
            }),
            ..echo_config("gated")
        };
        app.rune(cfg, echo_handler()).unwrap();
        let list = app.relay.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "gated");
        assert_eq!(list[0].1.as_deref(), Some("/api/echo"));
    }

    // ========================================================================
    // schema registration
    // ========================================================================

    #[tokio::test]
    async fn app_register_rune_with_schema() {
        let mut app = App::new();
        let cfg = RuneConfig {
            input_schema: Some(r#"{"type":"object"}"#.into()),
            output_schema: Some(r#"{"type":"string"}"#.into()),
            ..echo_config("schema_rune")
        };
        app.rune(cfg, echo_handler()).unwrap();

        // Verify the rune is registered and works
        let resolver = RoundRobinResolver::new();
        let invoker = app.relay.resolve("schema_rune", &resolver).unwrap();
        let result = invoker
            .invoke_once(test_ctx("schema_rune"), Bytes::from("test"))
            .await
            .unwrap();
        assert_eq!(result, Bytes::from("test"));
    }

    // ========================================================================
    // duplicate rune name — adds another candidate (not error)
    // ========================================================================

    #[tokio::test]
    async fn app_register_duplicate_name_adds_candidate() {
        let mut app = App::new();
        let h1 = make_handler(|_ctx, _input| async { Ok(Bytes::from("v1")) });
        let h2 = make_handler(|_ctx, _input| async { Ok(Bytes::from("v2")) });
        app.rune(echo_config("dup"), h1).unwrap();
        app.rune(echo_config("dup"), h2).unwrap();

        // Both should be registered as candidates
        let entries = app.relay.find("dup").unwrap();
        assert_eq!(entries.len(), 2);
    }

    // ========================================================================
    // App::build() returns RunningApp with all components
    // ========================================================================

    #[test]
    fn app_build_returns_complete_running_app() {
        let mut app = App::new();
        app.rune(echo_config("built_rune"), echo_handler()).unwrap();
        let running = app.build();

        // Verify all components are present
        let list = running.relay.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "built_rune");
        assert_eq!(running.session_mgr.caster_count(), 0);
        assert_eq!(running.config.server.grpc_port, 50070);
    }

    // ========================================================================
    // set_resolver
    // ========================================================================

    #[tokio::test]
    async fn app_set_resolver_replaces_default() {
        let mut app = App::new();
        app.rune(echo_config("res_rune"), echo_handler()).unwrap();

        // Use a custom resolver that always picks the first
        struct FirstResolver;
        impl Resolver for FirstResolver {
            fn pick(
                &self,
                _rune_name: &str,
                candidates: &[crate::relay::RuneEntry],
            ) -> Option<usize> {
                if candidates.is_empty() {
                    None
                } else {
                    Some(0)
                }
            }
        }
        app.set_resolver(FirstResolver);

        let invoker = app
            .relay
            .resolve("res_rune", app.resolver.as_ref())
            .unwrap();
        let result = invoker
            .invoke_once(test_ctx("res_rune"), Bytes::from("data"))
            .await
            .unwrap();
        assert_eq!(result, Bytes::from("data"));
    }

    // ========================================================================
    // fluent builder pattern (chaining)
    // ========================================================================

    #[test]
    fn app_builder_chaining() {
        let mut app = App::new();
        app.rune(echo_config("a"), echo_handler())
            .unwrap()
            .rune(echo_config("b"), echo_handler())
            .unwrap();

        let list = app.relay.list();
        assert_eq!(list.len(), 2);
    }

    // ========================================================================
    // gate conflict is logged, not panicked — second rune is silently dropped
    // ========================================================================

    #[test]
    fn app_register_conflicting_gate_paths_panics() {
        let mut app = App::new();
        let cfg1 = RuneConfig {
            gate: Some(GateConfig {
                path: "/api/do".into(),
                method: "POST".into(),
            }),
            ..echo_config("rune_x")
        };
        let cfg2 = RuneConfig {
            gate: Some(GateConfig {
                path: "/api/do".into(),
                method: "POST".into(),
            }),
            ..echo_config("rune_y")
        };
        app.rune(cfg1, echo_handler()).unwrap();

        // A route conflict is a programming error — the process should panic
        // so the mistake is caught at startup, not silently ignored.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            app.rune(cfg2, echo_handler()).unwrap();
        }));
        assert!(
            result.is_err(),
            "expected panic on route conflict, but it did not panic"
        );

        // Only rune_x was registered (rune_y failed with conflict before panic)
        let list = app.relay.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "rune_x");
    }
}
