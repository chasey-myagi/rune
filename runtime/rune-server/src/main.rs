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
        Arc::new(StoreKeyVerifier::new(store.clone()))
    } else {
        Arc::new(NoopVerifier)
    };

    // ── App + Runes ──
    let mut app = App::with_config(config.clone());

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
            v.as_object_mut()
                .unwrap()
                .insert("step_b".into(), true.into());
            Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
        }),
    );

    tracing::info!("registered local runes: hello, step_b");

    // ── Build app components ──
    let running = app.build();

    // Set up snapshot recording on caster attach
    let store_for_attach = store.clone();
    running.session_mgr.set_on_caster_attach(Arc::new(move |_caster_id, configs| {
        for config in configs {
            let snapshot = RuneSnapshot {
                rune_name: config.name.clone(),
                version: config.version.clone(),
                description: config.description.clone(),
                supports_stream: config.supports_stream,
                gate_path: config.gate.as_ref().map(|g| g.path.clone()).unwrap_or_default(),
                gate_method: config.gate.as_ref().map(|g| g.method.clone()).unwrap_or("POST".into()),
                last_seen: String::new(), // filled by upsert_snapshot
            };
            if let Err(e) = store_for_attach.upsert_snapshot(&snapshot) {
                tracing::warn!(rune = %config.name, error = %e, "failed to record snapshot");
            }
        }
    }));

    // ── Flow Engine ──
    let mut flow_engine = FlowEngine::new(Arc::clone(&running.relay), Arc::clone(&running.resolver));

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

    tracing::info!("registered flows: pipeline, single, empty");

    // ── HTTP Gate ──
    let flow_engine = Arc::new(tokio::sync::RwLock::new(flow_engine));
    let gate_state = gate::GateState {
        relay: Arc::clone(&running.relay),
        resolver: Arc::clone(&running.resolver),
        store: store.clone(),
        key_verifier,
        session_mgr: Arc::clone(&running.session_mgr),
        auth_enabled: config.auth.enabled,
        exempt_routes: config.auth.exempt_routes.clone(),
        cors_origins: config.gate.cors_origins.clone(),
        dev_mode: config.server.dev_mode,
        started_at: Instant::now(),
        file_broker: Arc::new(gate::FileBroker::new()),
        max_upload_size_mb: 100,
        flow_engine: Arc::clone(&flow_engine),
    };

    let http_router = gate::build_router(gate_state, None);

    let http_listener = tokio::net::TcpListener::bind(running.config.http_addr()).await?;
    tracing::info!("gate listening on {}", running.config.http_addr());

    let http_handle = tokio::spawn(async move {
        axum::serve(http_listener, http_router).await.unwrap();
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
            .serve(running.config.grpc_addr())
            .await
            .unwrap();
    });

    tokio::select! {
        _ = http_handle => tracing::info!("http server stopped"),
        _ = grpc_handle => tracing::info!("grpc server stopped"),
        _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
    }

    Ok(())
}
