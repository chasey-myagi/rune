use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use rune_proto::rune_service_server::RuneServiceServer;
use rune_core::app::App;
use rune_core::auth::{KeyVerifier, NoopVerifier};
use rune_core::config::AppConfig;
use rune_core::grpc_service::RuneGrpcService;
use rune_core::rune::{RuneConfig, RuneError, GateConfig, make_handler};
use rune_flow::dsl::Flow;
use rune_flow::engine::FlowEngine;
use rune_gate::gate;
use rune_store::{RuneSnapshot, RuneStore, StoreKeyVerifier, TaskStatus};
use axum::{
    Extension,
    extract::{Path, State, Query},
    Json, Router,
    routing::{get, post},
    http::StatusCode,
    response::IntoResponse,
};
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

// ── Flow handlers (hosted in rune-server) ──

async fn list_flows(Extension(engine): Extension<Arc<FlowEngine>>) -> impl IntoResponse {
    let flows: Vec<&str> = engine.list();
    Json(serde_json::json!({"flows": flows}))
}

async fn run_flow(
    State(state): State<gate::GateState>,
    Extension(engine): Extension<Arc<FlowEngine>>,
    Path(name): Path<String>,
    Query(params): Query<gate::RunParams>,
    body: Bytes,
) -> axum::response::Response {
    // async mode
    if params.async_mode.unwrap_or(false) {
        let task_id = format!(
            "f-{:x}-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            0u64,
        );

        let input_str = String::from_utf8_lossy(&body).to_string();
        if let Err(e) = state.store.insert_task(&task_id, &format!("flow:{}", name), Some(&input_str)) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": {"code": "INTERNAL", "message": e.to_string()}})),
            )
                .into_response();
        }
        let _ = state.store.update_task_status(&task_id, TaskStatus::Running, None, None);

        let engine = Arc::clone(&engine);
        let store = Arc::clone(&state.store);
        let flow_name = name.clone();
        let body_clone = body.clone();
        let tid = task_id.clone();
        tokio::spawn(async move {
            match engine.execute(&flow_name, body_clone).await {
                Ok(result) => {
                    let output_str = String::from_utf8_lossy(&result.output).to_string();
                    let val = serde_json::from_str::<serde_json::Value>(&output_str)
                        .unwrap_or(serde_json::Value::Null);
                    let combined = serde_json::json!({
                        "output": val,
                        "steps_executed": result.steps_executed,
                    });
                    let _ = store.update_task_status(
                        &tid,
                        TaskStatus::Completed,
                        Some(&combined.to_string()),
                        None,
                    );
                }
                Err(e) => {
                    let _ = store.update_task_status(
                        &tid,
                        TaskStatus::Failed,
                        None,
                        Some(&e.to_string()),
                    );
                }
            }
        });

        return (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "task_id": task_id,
                "status": "running",
            })),
        )
            .into_response();
    }

    // sync mode
    match engine.execute(&name, body).await {
        Ok(result) => match serde_json::from_slice::<serde_json::Value>(&result.output) {
            Ok(json) => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "output": json,
                    "steps_executed": result.steps_executed,
                })),
            )
                .into_response(),
            Err(_) => (StatusCode::OK, result.output).into_response(),
        },
        Err(e) => {
            let status = match &e {
                rune_flow::engine::FlowError::FlowNotFound(_) => StatusCode::NOT_FOUND,
                rune_flow::engine::FlowError::StepFailed { .. } => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };
            (
                status,
                Json(serde_json::json!({
                    "error": {"code": "FLOW_ERROR", "message": e.to_string()}
                })),
            )
                .into_response()
        }
    }
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
            priority: 0,
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
            priority: 0,
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

    flow_engine.register(
        Flow::new("pipeline")
            .chain(vec!["step_a", "step_b", "step_c"])
            .build(),
    );

    flow_engine.register(Flow::new("single").step("step_a").build());
    flow_engine.register(Flow::new("empty").build());

    tracing::info!("registered flows: pipeline, single, empty");

    // ── HTTP Gate ──
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
    };

    let flow_engine = Arc::new(flow_engine);
    let flow_routes: Router<gate::GateState> = Router::new()
        .route("/api/v1/flows", get(list_flows))
        .route("/api/v1/flows/{name}/run", post(run_flow))
        .layer(Extension(Arc::clone(&flow_engine)));

    let http_router = gate::build_router(gate_state, Some(flow_routes));

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
