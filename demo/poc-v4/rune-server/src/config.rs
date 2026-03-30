use std::fmt;

#[derive(Debug)]
pub struct Config {
    pub host: String,
    pub http_port: u16,
    pub grpc_port: u16,
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let host = std::env::var("RUNE_HOST").unwrap_or_else(|_| "0.0.0.0".into());
        let http_port: u16 = std::env::var("RUNE_HTTP_PORT")
            .unwrap_or_else(|_| "50060".into())
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid RUNE_HTTP_PORT: {}", e))?;
        let grpc_port: u16 = std::env::var("RUNE_GRPC_PORT")
            .unwrap_or_else(|_| "50070".into())
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid RUNE_GRPC_PORT: {}", e))?;
        let log_level = std::env::var("RUNE_LOG_LEVEL").unwrap_or_else(|_| "info".into());

        Ok(Self { host, http_port, grpc_port, log_level })
    }

    pub fn http_addr(&self) -> String {
        format!("{}:{}", self.host, self.http_port)
    }

    pub fn grpc_addr(&self) -> String {
        format!("{}:{}", self.host, self.grpc_port)
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "host={} http_port={} grpc_port={} log_level={}",
            self.host, self.http_port, self.grpc_port, self.log_level
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // env vars are process-global, so tests must run sequentially
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        let _guard = ENV_LOCK.lock().unwrap();
        let original: Vec<(&str, Option<String>)> = vars
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();

        for (k, v) in vars {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }

        f();

        for (k, orig) in &original {
            match orig {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
    }

    #[test]
    fn test_defaults() {
        with_env(
            &[
                ("RUNE_HOST", None),
                ("RUNE_HTTP_PORT", None),
                ("RUNE_GRPC_PORT", None),
                ("RUNE_LOG_LEVEL", None),
            ],
            || {
                let cfg = Config::from_env().unwrap();
                assert_eq!(cfg.host, "0.0.0.0");
                assert_eq!(cfg.http_port, 50060);
                assert_eq!(cfg.grpc_port, 50070);
                assert_eq!(cfg.log_level, "info");
            },
        );
    }

    #[test]
    fn test_custom_values() {
        with_env(
            &[
                ("RUNE_HOST", Some("127.0.0.1")),
                ("RUNE_HTTP_PORT", Some("8080")),
                ("RUNE_GRPC_PORT", Some("9090")),
                ("RUNE_LOG_LEVEL", Some("debug")),
            ],
            || {
                let cfg = Config::from_env().unwrap();
                assert_eq!(cfg.host, "127.0.0.1");
                assert_eq!(cfg.http_port, 8080);
                assert_eq!(cfg.grpc_port, 9090);
                assert_eq!(cfg.log_level, "debug");
            },
        );
    }

    #[test]
    fn test_invalid_http_port() {
        with_env(
            &[("RUNE_HTTP_PORT", Some("not_a_number"))],
            || {
                let err = Config::from_env().unwrap_err();
                assert!(err.to_string().contains("invalid RUNE_HTTP_PORT"));
            },
        );
    }

    #[test]
    fn test_invalid_grpc_port() {
        with_env(
            &[("RUNE_GRPC_PORT", Some("abc"))],
            || {
                let err = Config::from_env().unwrap_err();
                assert!(err.to_string().contains("invalid RUNE_GRPC_PORT"));
            },
        );
    }

    #[test]
    fn test_port_out_of_range() {
        with_env(
            &[("RUNE_HTTP_PORT", Some("99999"))],
            || {
                let err = Config::from_env().unwrap_err();
                assert!(err.to_string().contains("invalid RUNE_HTTP_PORT"));
            },
        );
    }

    #[test]
    fn test_addr_formatting() {
        with_env(
            &[
                ("RUNE_HOST", Some("10.0.0.1")),
                ("RUNE_HTTP_PORT", Some("3000")),
                ("RUNE_GRPC_PORT", Some("4000")),
            ],
            || {
                let cfg = Config::from_env().unwrap();
                assert_eq!(cfg.http_addr(), "10.0.0.1:3000");
                assert_eq!(cfg.grpc_addr(), "10.0.0.1:4000");
            },
        );
    }

    #[test]
    fn test_garbage_log_level_still_accepted() {
        // Config::from_env accepts any string; EnvFilter validates at init time
        with_env(
            &[("RUNE_LOG_LEVEL", Some("garbage"))],
            || {
                let cfg = Config::from_env().unwrap();
                assert_eq!(cfg.log_level, "garbage");
            },
        );
    }
}
