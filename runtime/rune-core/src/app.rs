use std::sync::Arc;
use crate::rune::{RuneConfig, RuneHandler, StreamRuneHandler};
use crate::relay::Relay;
use crate::resolver::{Resolver, RoundRobinResolver};
use crate::session::SessionManager;
use crate::invoker::{LocalInvoker, LocalStreamInvoker};
use crate::config::AppConfig;
use crate::grpc_service::RuneGrpcService;
use rune_proto::rune_service_server::RuneServiceServer;

pub struct App {
    pub relay: Arc<Relay>,
    pub resolver: Arc<dyn Resolver>,
    pub session_mgr: Arc<SessionManager>,
    pub config: AppConfig,
}

/// Components returned by `App::build()` for custom server setup.
pub struct RunningApp {
    pub relay: Arc<Relay>,
    pub resolver: Arc<dyn Resolver>,
    pub session_mgr: Arc<SessionManager>,
    pub config: AppConfig,
}

impl App {
    pub fn new() -> Self {
        Self {
            relay: Arc::new(Relay::new()),
            resolver: Arc::new(RoundRobinResolver::new()),
            session_mgr: Arc::new(SessionManager::new()),
            config: AppConfig::default(),
        }
    }

    pub fn with_config(config: AppConfig) -> Self {
        Self {
            relay: Arc::new(Relay::new()),
            resolver: Arc::new(RoundRobinResolver::new()),
            session_mgr: Arc::new(SessionManager::new()),
            config,
        }
    }

    pub fn rune(&mut self, config: RuneConfig, handler: RuneHandler) -> &mut Self {
        self.relay.register(config, Arc::new(LocalInvoker::new(handler)), None)
            .expect("route conflict in local rune registration");
        self
    }

    pub fn stream_rune(&mut self, config: RuneConfig, handler: impl StreamRuneHandler) -> &mut Self {
        self.relay.register(config, Arc::new(LocalStreamInvoker::new(Arc::new(handler))), None)
            .expect("route conflict in local stream rune registration");
        self
    }

    pub fn set_resolver(&mut self, r: impl Resolver + 'static) -> &mut Self {
        self.resolver = Arc::new(r);
        self
    }

    /// Start the gRPC server and block until shutdown signal (SIGINT / ctrl-c).
    ///
    /// This is the simplest way to run an App — it starts the gRPC server for
    /// Caster connections and waits for a termination signal. The caller does
    /// NOT get access to components for building an HTTP server; use `build()`
    /// instead if you need that.
    pub async fn run(self) -> anyhow::Result<()> {
        let grpc_addr = self.config.grpc_addr;

        let grpc_service = RuneGrpcService {
            relay: Arc::clone(&self.relay),
            session_mgr: Arc::clone(&self.session_mgr),
        };

        tracing::info!("gRPC listening on {}", grpc_addr);

        tonic::transport::Server::builder()
            .add_service(RuneServiceServer::new(grpc_service))
            .serve_with_shutdown(grpc_addr, async {
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("shutdown signal received");
            })
            .await?;

        Ok(())
    }

    /// Finalize configuration and return components for custom server setup.
    ///
    /// The caller is responsible for starting gRPC and HTTP servers using the
    /// returned `RunningApp`. This is the preferred approach when you need to
    /// compose additional routes (e.g., Gate HTTP + Flow routes).
    pub fn build(self) -> RunningApp {
        RunningApp {
            relay: self.relay,
            resolver: self.resolver,
            session_mgr: self.session_mgr,
            config: self.config,
        }
    }
}
