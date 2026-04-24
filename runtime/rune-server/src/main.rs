use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use rune_core::app::App;
use rune_core::auth::{KeyVerifier, NoopVerifier};
use rune_core::config::{AppConfig, LogFormat};
use rune_core::grpc_service::RuneGrpcService;
use rune_core::rune::{make_handler, GateConfig, RuneConfig, RuneError};
use rune_core::scaling::ScaleEvaluator;
use rune_core::telemetry::TelemetryConfig;
use rune_flow::dag::{FlowDefinition, RuneConfig as FlowRuneConfig, StepDefinition, StepKind};
use rune_flow::engine::FlowEngine;
use rune_gate::gate;
use rune_proto::rune_service_server::RuneServiceServer;
use rune_store::{RuneSnapshot, RuneStore, StoreKeyVerifier, StorePoolConfig};

#[derive(Parser)]
#[command(name = "rune-server", about = "Rune runtime server")]
struct Cli {
    /// Path to configuration file (TOML)
    #[arg(long, short)]
    config: Option<String>,

    /// Enable development mode (localhost binding, auth disabled)
    #[arg(long)]
    dev: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── CLI + Config ──
    let cli = Cli::parse();
    let mut config = if cli.dev && cli.config.is_none() {
        // dev mode with no explicit config is the only path that may
        // start with built-in defaults. Production usage must supply a file.
        AppConfig::default()
    } else {
        AppConfig::load(cli.config.as_deref())?
    };
    if cli.dev {
        config.apply_dev_mode();
    }
    config.apply_env_overrides();

    // prevent env vars from partially overriding dev mode invariants.
    // If dev mode is active (set by --dev flag or RUNE_SERVER__DEV_MODE=true), auth must
    // remain disabled regardless of RUNE_AUTH__ENABLED being set in the environment.
    if config.server.dev_mode {
        config.auth.enabled = false;
    }

    config.validate()?;

    if config.server.dev_mode {
        // Safety guard: dev mode disables auth and rate limiting.
        // Refuse to bind to a non-loopback address to prevent accidental
        // production exposure via RUNE_SERVER__HTTP_HOST=0.0.0.0.
        if !config.server.http_host.is_loopback() {
            anyhow::bail!(
                "dev mode cannot bind to non-loopback address '{}' — \
                 dev mode disables authentication and rate limiting.\n\
                 Remove --dev or set RUNE_SERVER__HTTP_HOST=127.0.0.1.",
                config.server.http_host
            );
        }
        // eprintln! intentional: init_telemetry is not called until after this block,
        // so tracing is not yet initialized here — tracing::warn! would be silently dropped.
        eprintln!(
            "\n\
             ⚠️  DEV MODE ACTIVE — auth disabled, rate limiting disabled\n\
             \x20   DO NOT USE IN PRODUCTION\n"
        );
    }

    // ── Telemetry (tracing + metrics) ──
    let _tracer_provider = init_telemetry(&config.telemetry, &config.log.format);

    if let Some(ref path) = config.log.file {
        tracing::error!(
            path = %path,
            "log.file is configured but file-based logging is not yet implemented; \
             remove log.file from rune.toml to start the server"
        );
        anyhow::bail!(
            "log.file '{}' is not supported yet; remove it from configuration",
            path
        );
    }

    tracing::info!(dev_mode = config.server.dev_mode, "loading configuration");

    // ── Store ──
    let store_pool_config = StorePoolConfig {
        reader_count: config.store.reader_pool_size,
        key_cache_ttl: std::time::Duration::from_secs(config.store.key_cache_ttl_secs),
        key_cache_negative_ttl: std::time::Duration::from_secs(
            config.store.key_cache_negative_ttl_secs,
        ),
    };
    let store = if config.server.dev_mode && config.store.db_path == "rune.db" {
        // In dev mode with default path, use in-memory database
        Arc::new(RuneStore::open_in_memory_with_config(
            store_pool_config.clone(),
        )?)
    } else {
        Arc::new(RuneStore::open_with_config(
            &config.store.db_path,
            store_pool_config,
        )?)
    };
    tracing::info!(db_path = %config.store.db_path, "store initialized");

    if config.auth.enabled && !config.server.dev_mode {
        if let Some(initial_admin_key) = config.auth.initial_admin_key.as_deref() {
            match store.has_admin_key().await {
                Ok(true) => {
                    tracing::debug!(
                        "admin key already exists; skipping RUNE_AUTH__INITIAL_ADMIN_KEY bootstrap"
                    );
                }
                Ok(false) => match store
                    .import_admin_key(initial_admin_key, "initial-admin-from-env")
                    .await
                {
                    Ok(api_key) => {
                        tracing::info!(
                            key_prefix = %api_key.key_prefix,
                            "bootstrapped admin key from RUNE_AUTH__INITIAL_ADMIN_KEY"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "failed to bootstrap admin key from RUNE_AUTH__INITIAL_ADMIN_KEY — aborting"
                        );
                        std::process::exit(1);
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "failed to check existing admin keys before bootstrap; skipping env bootstrap"
                    );
                }
            }
        }
    }

    // ── Key Verifier ──
    let key_verifier: Arc<dyn KeyVerifier> = if config.auth.enabled {
        let hmac_secret = match config.auth.hmac_secret.clone() {
            Some(s) => s,
            None if config.server.dev_mode => {
                tracing::info!("dev mode: using fixed HMAC secret for key hashing");
                "dev-hmac-secret".to_string()
            }
            None => {
                anyhow::bail!(
                    "auth is enabled but auth.hmac_secret is not configured.\n\
                     API keys use HMAC-SHA256 and will NOT survive a restart without a \
                     stable secret.\n\
                     Set RUNE_AUTH__HMAC_SECRET or auth.hmac_secret in rune.toml.\n\
                     To disable auth (development only), use --dev."
                );
            }
        };
        Arc::new(StoreKeyVerifier::with_hmac_secret(
            store.clone(),
            hmac_secret.into_bytes(),
        ))
    } else {
        Arc::new(NoopVerifier)
    };

    // ── App + Runes ──
    let mut app = App::with_config_and_auth(config.clone(), key_verifier.clone());

    // Demo runes — only registered in dev mode
    if config.server.dev_mode {
        // hello: static response
        app.rune(
            RuneConfig {
                name: "hello".into(),
                version: String::new(),
                description: "local hello".into(),
                supports_stream: false,
                gate: Some(GateConfig {
                    path: "/hello".into(),
                    method: "POST".into(),
                }),
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels: Default::default(),
            },
            make_handler(|_ctx, _input| async {
                Ok(Bytes::from(r#"{"message":"hello from local rune!"}"#))
            }),
        )
        .map_err(|e| anyhow::anyhow!("failed to register demo rune 'hello': {e}"))?;

        // step_b: local Rust step (adds step_b field to JSON)
        app.rune(
            RuneConfig {
                name: "step_b".into(),
                version: String::new(),
                description: "local step_b".into(),
                supports_stream: false,
                gate: None,
                input_schema: None,
                output_schema: None,
                priority: 0,
                labels: Default::default(),
            },
            make_handler(|_ctx, input| async move {
                let mut v: serde_json::Value = serde_json::from_slice(&input)
                    .map_err(|e| RuneError::InvalidInput(e.to_string()))?;
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("step_b".into(), true.into());
                } else {
                    return Err(RuneError::InvalidInput(
                        "step_b expects a JSON object as input".to_string(),
                    ));
                }
                Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
            }),
        )
        .map_err(|e| anyhow::anyhow!("failed to register demo rune 'step_b': {e}"))?;

        tracing::info!("registered demo runes: hello, step_b");
    }

    // ── Build app components ──
    let running = app.build();

    // Set up snapshot recording on caster attach
    // Issue #7 fix: use tokio::spawn instead of block_in_place + block_on
    // to avoid panics on current-thread runtime and improve scheduling efficiency.
    let store_for_attach = store.clone();
    running
        .session_mgr
        .set_on_caster_attach(Arc::new(move |_caster_id, configs| {
            for config in configs {
                let store = store_for_attach.clone();
                let snapshot = RuneSnapshot {
                    rune_name: config.name.clone(),
                    version: config.version.clone(),
                    description: config.description.clone(),
                    supports_stream: config.supports_stream,
                    gate_path: config
                        .gate
                        .as_ref()
                        .map(|g| g.path.clone())
                        .unwrap_or_default(),
                    gate_method: config
                        .gate
                        .as_ref()
                        .map(|g| g.method.clone())
                        .unwrap_or("POST".into()),
                    last_seen: String::new(), // filled by upsert_snapshot
                };
                let rune_name = config.name.clone();
                tokio::spawn(async move {
                    if let Err(e) = store.upsert_snapshot(&snapshot).await {
                        tracing::warn!(rune = %rune_name, error = %e, "failed to record snapshot");
                    }
                });
            }
        }));

    // ── Flow Engine ──
    let mut flow_engine = FlowEngine::with_timeout(
        Arc::clone(&running.relay),
        Arc::clone(&running.resolver),
        config.default_timeout(),
    );

    match store.list_flows().await {
        Ok(flows) => {
            let flow_count = flows.len();
            for flow in flows {
                let flow_name = flow.name.clone();
                if let Err(e) = flow_engine.register(flow) {
                    tracing::warn!(
                        flow = %flow_name,
                        error = %e,
                        "failed to restore persisted flow"
                    );
                }
            }
            tracing::info!(flow_count, "loaded persisted flows");
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to load persisted flows");
        }
    }

    // Demo flows — only registered in dev mode
    if config.server.dev_mode {
        let _ = flow_engine.register(FlowDefinition {
            name: "pipeline".to_string(),
            steps: vec![
                StepDefinition {
                    name: "s_a".into(),
                    depends_on: vec![],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                    retry: None,
                    kind: StepKind::Rune(FlowRuneConfig {
                        rune: "step_a".into(),
                    }),
                },
                StepDefinition {
                    name: "s_b".into(),
                    depends_on: vec!["s_a".into()],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                    retry: None,
                    kind: StepKind::Rune(FlowRuneConfig {
                        rune: "step_b".into(),
                    }),
                },
                StepDefinition {
                    name: "s_c".into(),
                    depends_on: vec!["s_b".into()],
                    condition: None,
                    input_mapping: None,
                    timeout_ms: None,
                    retry: None,
                    kind: StepKind::Rune(FlowRuneConfig {
                        rune: "step_c".into(),
                    }),
                },
            ],
            gate_path: None,
        });

        let _ = flow_engine.register(FlowDefinition {
            name: "single".to_string(),
            steps: vec![StepDefinition {
                name: "s_a".into(),
                depends_on: vec![],
                condition: None,
                input_mapping: None,
                timeout_ms: None,
                retry: None,
                kind: StepKind::Rune(FlowRuneConfig {
                    rune: "step_a".into(),
                }),
            }],
            gate_path: None,
        });
        let _ = flow_engine.register(FlowDefinition {
            name: "empty".to_string(),
            steps: vec![],
            gate_path: None,
        });

        tracing::info!("registered demo flows: pipeline, single, empty");
    }

    // ── HTTP Gate ──
    let flow_engine = Arc::new(tokio::sync::RwLock::new(flow_engine));
    let shutdown = gate::ShutdownCoordinator::new();
    let drain_timeout_secs = config.server.drain_timeout_secs;
    let scaling = if config.scaling.enabled {
        let evaluator = Arc::new(ScaleEvaluator::new(
            Arc::clone(&running.session_mgr),
            Duration::from_secs(config.scaling.eval_interval_secs),
        ));
        Arc::clone(&evaluator).start();
        Some(evaluator)
    } else {
        None
    };

    let gate_state = gate::GateState {
        auth: gate::AuthState {
            key_verifier,
            auth_enabled: config.auth.enabled,
            exempt_routes: Arc::new(config.auth.exempt_routes.clone()),
            trust_proxy: config.gate.trust_proxy.as_ref().map(|v| {
                let parsed: Result<Vec<_>, _> =
                    v.iter().map(|s| gate::TrustedCidr::parse(s)).collect();
                match parsed {
                    Ok(cidrs) => Arc::new(cidrs),
                    Err(e) => {
                        tracing::error!("failed to parse trust_proxy CIDR: {}", e);
                        std::process::exit(1);
                    }
                }
            }),
            audit_semaphore: Arc::new(tokio::sync::Semaphore::new(64)),
        },
        rune: gate::RuneState {
            relay: Arc::clone(&running.relay),
            resolver: Arc::clone(&running.resolver),
            session_mgr: Arc::clone(&running.session_mgr),
            file_broker: Arc::new(init_file_broker(&config.gate.disk_spill_dir).await),
            max_upload_size_mb: config.gate.max_upload_size_mb,
            request_timeout: config.default_timeout(),
        },
        flow: gate::FlowState {
            flow_engine: Arc::clone(&flow_engine),
        },
        admin: gate::AdminState {
            store: store.clone(),
            started_at: Instant::now(),
            dev_mode: config.server.dev_mode,
            scaling: scaling.clone(),
        },
        cors_origins: Arc::new(config.gate.cors_origins.clone()),
        rate_limiter: if config.server.dev_mode {
            None
        } else {
            Some(gate::RateLimitState::with_per_rune_limits(
                config.rate_limit.max_requests,
                config.rate_limit.window_secs,
                config.rate_limit.per_rune.clone(),
            ))
        },
        shutdown: shutdown.clone(),
    };

    let http_router = gate::build_router(gate_state, None);

    // Determine whether TLS is active (disabled in dev mode even if configured)
    let tls_enabled =
        !config.server.dev_mode && config.tls.cert_path.is_some() && config.tls.key_path.is_some();

    if tls_enabled {
        tracing::info!("TLS enabled for HTTP and gRPC servers");
    } else if !config.server.dev_mode
        && (config.tls.cert_path.is_some() || config.tls.key_path.is_some())
    {
        tracing::warn!(
            "TLS partially configured (need both tls.cert_path and tls.key_path); \
             falling back to plaintext"
        );
    }

    // Issue #4 fix: use watch channel to coordinate graceful shutdown signals
    // for both HTTP and gRPC servers.
    let (shutdown_tx, mut http_shutdown_rx) = tokio::sync::watch::channel(false);
    let mut grpc_shutdown_rx = shutdown_tx.subscribe();

    // ── Background cleanup task ──
    {
        let store_cleanup = store.clone();
        let log_days = config.store.log_retention_days;
        let task_days = config.store.task_retention_days;
        let mut cleanup_shutdown_rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let period = Duration::from_secs(24 * 3600);
            let mut interval =
                tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    result = cleanup_shutdown_rx.changed() => {
                        if result.is_err() {
                            tracing::warn!("cleanup task: shutdown channel closed unexpectedly");
                        }
                        break;
                    }
                }
                if *cleanup_shutdown_rx.borrow() {
                    break;
                }
                let cutoff_logs = chrono_days_ago(log_days);
                let cutoff_tasks = chrono_days_ago(task_days);
                match store_cleanup.cleanup_logs_before(&cutoff_logs).await {
                    Ok(n) => tracing::info!(deleted = n, "cleaned up call_logs"),
                    Err(e) => tracing::warn!(error = %e, "call_logs cleanup failed"),
                }
                match store_cleanup.cleanup_tasks_before(&cutoff_tasks).await {
                    Ok(n) => tracing::info!(deleted = n, "cleaned up tasks"),
                    Err(e) => tracing::warn!(error = %e, "tasks cleanup failed"),
                }
            }
        });
    }

    let http_addr = running.config.http_addr();
    // Handle for axum-server graceful shutdown (only used in TLS mode)
    let axum_server_handle = axum_server::Handle::new();
    let axum_server_handle_clone = axum_server_handle.clone();

    let http_handle = if tls_enabled {
        let cert_path = config.tls.cert_path.clone().unwrap();
        let key_path = config.tls.key_path.clone().unwrap();
        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path).await?;
        tracing::info!("HTTPS gate listening on {}", http_addr);
        // Spawn a task that watches the shutdown channel and triggers graceful shutdown
        let handle_for_shutdown = axum_server_handle_clone;
        tokio::spawn({
            let mut rx = http_shutdown_rx;
            async move {
                while !*rx.borrow_and_update() {
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
                handle_for_shutdown.graceful_shutdown(None);
            }
        });
        tokio::spawn(async move {
            if let Err(e) = axum_server::bind_rustls(http_addr, rustls_config)
                .handle(axum_server_handle)
                .serve(http_router.into_make_service())
                .await
            {
                tracing::error!(error = %e, "HTTPS server error");
            }
        })
    } else {
        let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
        tracing::info!("HTTP gate listening on {}", http_addr);
        tokio::spawn(async move {
            if let Err(e) = axum::serve(http_listener, http_router)
                .with_graceful_shutdown(async move {
                    while !*http_shutdown_rx.borrow_and_update() {
                        if http_shutdown_rx.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await
            {
                tracing::error!(error = %e, "HTTP server error");
            }
        })
    };

    // ── gRPC ──
    let grpc_service = RuneGrpcService {
        relay: Arc::clone(&running.relay),
        session_mgr: Arc::clone(&running.session_mgr),
    };

    let grpc_addr = running.config.grpc_addr();
    let grpc_handle = if tls_enabled {
        let cert_path = config.tls.cert_path.clone().unwrap();
        let key_path = config.tls.key_path.clone().unwrap();
        let cert = tokio::fs::read(&cert_path).await?;
        let key = tokio::fs::read(&key_path).await?;
        let identity = tonic::transport::Identity::from_pem(cert, key);
        tracing::info!("gRPCS listening on {}", grpc_addr);
        tokio::spawn(async move {
            let mut server = match tonic::transport::Server::builder()
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "invalid TLS configuration for gRPC");
                    return;
                }
            };
            if let Err(e) = server
                .add_service(RuneServiceServer::new(grpc_service))
                .serve_with_shutdown(grpc_addr, async move {
                    while !*grpc_shutdown_rx.borrow_and_update() {
                        if grpc_shutdown_rx.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await
            {
                tracing::error!(error = %e, "gRPCS server error");
            }
        })
    } else {
        tracing::info!("gRPC listening on {}", grpc_addr);
        tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(RuneServiceServer::new(grpc_service))
                .serve_with_shutdown(grpc_addr, async move {
                    while !*grpc_shutdown_rx.borrow_and_update() {
                        if grpc_shutdown_rx.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await
            {
                tracing::error!(error = %e, "gRPC server error");
            }
        })
    };

    // ── Wait for shutdown signal ──
    // Listen for both SIGINT (Ctrl-C) and SIGTERM (docker stop / systemd).
    // On non-Unix platforms only SIGINT is available.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received SIGINT, starting graceful shutdown");
            }
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, starting graceful shutdown");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        tracing::info!("received SIGINT, starting graceful shutdown");
    }

    // 0. Stop the scale evaluator so no new grace-period force_kill spawns fire
    if let Some(ref evaluator) = scaling {
        evaluator.shutdown();
    }

    // 1. Signal drain mode — new requests will be rejected with 503
    shutdown.start_drain();
    tracing::info!(drain_timeout_secs, "draining in-flight requests");

    // 2. Signal both servers to stop accepting new connections
    //    and finish in-flight requests gracefully
    let _ = shutdown_tx.send(true);

    // 3. Wait for both servers to finish (with drain timeout as deadline)
    let deadline = std::time::Duration::from_secs(drain_timeout_secs);
    let _ = tokio::time::timeout(deadline, async {
        let _ = http_handle.await;
        let _ = grpc_handle.await;
    })
    .await;

    tracing::info!("drain complete, shutting down");

    // Flush pending OpenTelemetry spans before exit
    if let Some(provider) = _tracer_provider {
        if let Err(e) = provider.shutdown() {
            eprintln!("OpenTelemetry tracer shutdown error: {e:?}");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return an ISO-8601 timestamp for `days` days ago, used as a DELETE cutoff.
/// Initialise the FileBroker with an optional disk spill directory.
/// Defaults to `$TMPDIR/rune-uploads`; an empty string forces in-memory mode.
async fn init_file_broker(disk_spill_dir: &Option<String>) -> gate::FileBroker {
    let dir = if let Some(ref d) = disk_spill_dir {
        if d.is_empty() {
            // empty string = force in-memory mode
            return gate::FileBroker::new();
        } else {
            Some(std::path::PathBuf::from(d))
        }
    } else {
        Some(std::env::temp_dir().join("rune-uploads"))
    };

    let Some(d) = dir else {
        return gate::FileBroker::new();
    };

    if let Err(e) = std::fs::create_dir_all(&d) {
        tracing::warn!(path = %d.display(), error = %e, "failed to create disk_spill_dir");
        return gate::FileBroker::new();
    }

    // clean up orphaned files from previous runs (process crash).
    // Only delete files with the `rune-` prefix to avoid
    // removing unrelated data if disk_spill_dir is shared.
    let d_clean = d.clone();
    tokio::task::spawn_blocking(move || {
        if let Ok(rd) = std::fs::read_dir(&d_clean) {
            for entry in rd.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("rune-") {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
    })
    .await
    .ok();

    gate::FileBroker::with_disk_dir(d)
}

fn chrono_days_ago(days: u32) -> String {
    (chrono::Utc::now() - chrono::Duration::days(i64::from(days)))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

// ---------------------------------------------------------------------------
// Telemetry initialization
// ---------------------------------------------------------------------------

/// Initialize tracing subscriber and optional metrics exporter.
///
/// - Without any configuration: behaves identically to `tracing_subscriber::fmt::init()`.
/// - With `otlp_endpoint`: adds an OpenTelemetry OTLP tracing layer on top of fmt.
/// - With `prometheus_port`: starts a Prometheus `/metrics` HTTP listener.
///
/// Returns the `SdkTracerProvider` (if created) so the caller can flush spans
/// on shutdown via `provider.shutdown()`.
fn init_telemetry(
    config: &TelemetryConfig,
    log_format: &LogFormat,
) -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig as _;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer: Box<dyn tracing_subscriber::Layer<_> + Send + Sync> = match log_format {
        LogFormat::Json => Box::new(tracing_subscriber::fmt::layer().json()),
        LogFormat::Text => Box::new(tracing_subscriber::fmt::layer()),
    };

    let provider = if let Some(ref endpoint) = config.otlp_endpoint {
        // OTLP gRPC exporter -> OpenTelemetry tracing layer
        match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
        {
            Ok(exporter) => {
                let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                    .with_batch_exporter(exporter)
                    .build();

                let tracer = tracer_provider.tracer("rune-server");
                let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt_layer)
                    .with(otel_layer)
                    .init();

                tracing::info!(otlp_endpoint = endpoint, "OpenTelemetry tracing enabled");
                Some(tracer_provider)
            }
            Err(e) => {
                // Graceful fallback: initialise fmt-only subscriber and continue
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt_layer)
                    .init();

                eprintln!(
                    "WARNING: failed to initialize OTLP exporter: {e}, \
                     continuing without tracing"
                );
                None
            }
        }
    } else {
        // No OTLP — plain fmt subscriber (same as original behavior)
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
        None
    };

    // Prometheus metrics exporter (independent of tracing)
    if let Some(port) = config.prometheus_port {
        match metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(([0, 0, 0, 0], port))
            .install()
        {
            Ok(_) => {
                tracing::info!(port, "Prometheus metrics exporter started");
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to start Prometheus exporter, continuing without metrics"
                );
            }
        }
    }

    provider
}
