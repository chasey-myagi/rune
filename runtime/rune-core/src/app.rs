use std::sync::Arc;
use crate::rune::{RuneConfig, RuneHandler, StreamRuneHandler};
use crate::relay::Relay;
use crate::resolver::{Resolver, RoundRobinResolver};
use crate::session::SessionManager;
use crate::invoker::{LocalInvoker, LocalStreamInvoker};
use crate::config::AppConfig;

pub struct App {
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
}
