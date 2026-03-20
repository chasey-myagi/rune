use std::net::SocketAddr;
use std::time::Duration;

pub struct AppConfig {
    pub grpc_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub default_timeout: Duration,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            grpc_addr: "0.0.0.0:50070".parse().unwrap(),
            http_addr: "0.0.0.0:50060".parse().unwrap(),
            heartbeat_interval: Duration::from_secs(10),
            heartbeat_timeout: Duration::from_secs(35),
            default_timeout: Duration::from_secs(30),
        }
    }
}
