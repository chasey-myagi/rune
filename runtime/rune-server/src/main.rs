use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use rune_proto::rune_service_server::RuneServiceServer;
use rune_core::app::App;
use rune_core::auth::{KeyVerifier, NoopVerifier};
use rune_core::config::AppConfig;
use rune_core::grpc_service::RuneGrpcService;
use rune_core::rune::{RuneConfig, RuneError, GateConfig, make_handler};
use rune_flow::dag::{FlowDefinition, StepDefinition};
use rune_flow::engine::FlowEngine;
use rune_gate::gate;
use rune_store::{RuneSnapshot, RuneStore, StoreKeyVerifier};
use bytes::Bytes;

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
    tracing_subscriber::fmt::init();

    // ── CLI + Config ──
    let cli = Cli::parse();
    let mut config = AppConfig::load(cli.config.as_deref())?;
    if cli.dev {
        config.apply_dev_mode();
    }
    config.apply_env_overrides();

    tracing::info!(dev_mode = config.server.dev_mode, "loading configuration");

    // ── Store ──
    let store = if config.server.dev_mode && config.store.db_path == "rune.db" {
        // In dev mode with default path, use in-memory database
        Arc::new(RuneStore::open_in_memory()?)
    } else {
        Arc::new(RuneStore::open(&config.store.db_path)?)
    };
    tracing::info!(db_path = %config.store.db_path, "store initialized");

    // ── Key Verifier ──
    let key_verifier: Arc<dyn KeyVerifier> = if config.auth.enabled {
        let hmac_secret = config.auth.hmac_secret.clone().unwrap_or_else(|| {
            if config.server.dev_mode {
                tracing::info!("dev mode: using fixed HMAC secret for key hashing");
                "dev-hmac-secret".to_string()
            } else {
                let secret = hex::encode(rand::random::<[u8; 32]>());
                tracing::warn!(
                    "no HMAC secret configured (auth.hmac_secret); generated a random \
                     ephemeral secret — keys created now will NOT survive a restart. \
                     Set RUNE_AUTH__HMAC_SECRET or auth.hmac_secret in config."
                );
                secret
            }
        });
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
                priority: 0, labels: Default::default(),
            },
            make_handler(|_ctx, _input| async {
                Ok(Bytes::from(r#"{"message":"hello from local rune!"}"#))
            }),
        );

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
                priority: 0, labels: Default::default(),
            },
            make_handler(|_ctx, input| async move {
                let mut v: serde_json::Value =
                    serde_json::from_slice(&input).map_err(|e| RuneError::InvalidInput(e.to_string()))?;
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("step_b".into(), true.into());
                } else {
                    return Err(RuneError::InvalidInput(
                        "step_b expects a JSON object as input".to_string(),
                    ));
                }
                Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
            }),
        );

        tracing::info!("registered demo runes: hello, step_b");
    }

    // ── Build app components ──
    let running = app.build();

    // Set up snapshot recording on caster attach
    // Issue #7 fix: use tokio::spawn instead of block_in_place + block_on
    // to avoid panics on current-thread runtime and improve scheduling efficiency.
    let store_for_attach = store.clone();
    running.session_mgr.set_on_caster_attach(Arc::new(move |_caster_id, configs| {
        for config in configs {
            let store = store_for_attach.clone();
            let snapshot = RuneSnapshot {
                rune_name: config.name.clone(),
                version: config.version.clone(),
                description: config.description.clone(),
                supports_stream: config.supports_stream,
                gate_path: config.gate.as_ref().map(|g| g.path.clone()).unwrap_or_default(),
                gate_method: config.gate.as_ref().map(|g| g.method.clone()).unwrap_or("POST".into()),
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

    // Demo flows — only registered in dev mode
    if config.server.dev_mode {
        let _ = flow_engine.register(FlowDefinition {
            name: "pipeline".to_string(),
            steps: vec![
                StepDefinition { name: "s_a".into(), rune: "step_a".into(), depends_on: vec![], condition: None, input_mapping: None },
                StepDefinition { name: "s_b".into(), rune: "step_b".into(), depends_on: vec!["s_a".into()], condition: None, input_mapping: None },
                StepDefinition { name: "s_c".into(), rune: "step_c".into(), depends_on: vec!["s_b".into()], condition: None, input_mapping: None },
            ],
            gate_path: None,
        });

        let _ = flow_engine.register(FlowDefinition {
            name: "single".to_string(),
            steps: vec![
                StepDefinition { name: "s_a".into(), rune: "step_a".into(), depends_on: vec![], condition: None, input_mapping: None },
            ],
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

    let gate_state = gate::GateState {
        auth: gate::AuthState {
            key_verifier,
            auth_enabled: config.auth.enabled,
            exempt_routes: Arc::new(config.auth.exempt_routes.clone()),
        },
        rune: gate::RuneState {
            relay: Arc::clone(&running.relay),
            resolver: Arc::clone(&running.resolver),
            session_mgr: Arc::clone(&running.session_mgr),
            file_broker: Arc::new(gate::FileBroker::new()),
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
        },
        cors_origins: Arc::new(config.gate.cors_origins.clone()),
        rate_limiter: if config.server.dev_mode {
            None
        } else {
            Some(gate::RateLimitState::new(config.rate_limit.requests_per_minute, 60))
        },
        shutdown: shutdown.clone(),
    };

    let http_router = gate::build_router(gate_state, None);

    let http_listener = tokio::net::TcpListener::bind(running.config.http_addr()).await?;
    tracing::info!("gate listening on {}", running.config.http_addr());

    // Issue #4 fix: use watch channel to coordinate graceful shutdown signals
    // for both HTTP and gRPC servers.
    let (shutdown_tx, mut http_shutdown_rx) = tokio::sync::watch::channel(false);
    let mut grpc_shutdown_rx = shutdown_tx.subscribe();

    let http_handle = tokio::spawn(async move {
        axum::serve(http_listener, http_router)
            .with_graceful_shutdown(async move {
                while !*http_shutdown_rx.borrow_and_update() {
                    if http_shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await
            .unwrap();
    });

    // ── gRPC ──
    let grpc_service = RuneGrpcService {
        relay: Arc::clone(&running.relay),
        session_mgr: Arc::clone(&running.session_mgr),
    };

    tracing::info!("grpc listening on {}", running.config.grpc_addr());

    let grpc_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RuneServiceServer::new(grpc_service))
            .serve_with_shutdown(running.config.grpc_addr(), async move {
                while !*grpc_shutdown_rx.borrow_and_update() {
                    if grpc_shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await
            .unwrap();
    });

    // ── Wait for shutdown signal ──
    tokio::signal::ctrl_c().await?;
    tracing::info!("received SIGINT, starting graceful shutdown");

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
    }).await;

    tracing::info!("drain complete, shutting down");

    Ok(())
}
