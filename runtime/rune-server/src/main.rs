use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use rune_proto::rune_service_server::{RuneService, RuneServiceServer};
use rune_proto::SessionMessage;
use rune_core::relay::Relay;
use rune_core::rune::{RuneConfig, RuneError, GateConfig, make_handler};
use rune_core::invoker::LocalInvoker;
use rune_core::resolver::RoundRobinResolver;
use rune_core::session::SessionManager;
use rune_flow::dsl::Flow;
use rune_flow::engine::FlowEngine;
use rune_gate::gate;
use axum::{
    Extension,
    extract::{Path, State, Query},
    Json, Router,
    routing::{get, post},
    http::StatusCode,
    response::IntoResponse,
};
use bytes::Bytes;

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
        state.tasks.insert(
            task_id.clone(),
            gate::TaskInfo {
                task_id: task_id.clone(),
                status: "running".into(),
                result: None,
                error: None,
            },
        );

        let engine = Arc::clone(&engine);
        let tasks = Arc::clone(&state.tasks);
        let flow_name = name.clone();
        let body_clone = body.clone();
        let tid = task_id.clone();
        tokio::spawn(async move {
            match engine.execute(&flow_name, body_clone).await {
                Ok(result) => {
                    let val = serde_json::from_slice(&result.output)
                        .unwrap_or(serde_json::Value::Null);
                    tasks.insert(
                        tid.clone(),
                        gate::TaskInfo {
                            task_id: tid,
                            status: "completed".into(),
                            result: Some(serde_json::json!({
                                "output": val,
                                "steps_executed": result.steps_executed,
                            })),
                            error: None,
                        },
                    );
                }
                Err(e) => {
                    tasks.insert(
                        tid.clone(),
                        gate::TaskInfo {
                            task_id: tid,
                            status: "failed".into(),
                            result: None,
                            error: Some(serde_json::json!({"message": e.to_string()})),
                        },
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

// ── gRPC service ──

struct RuneGrpcService {
    relay: Arc<Relay>,
    session_mgr: Arc<SessionManager>,
}

#[tonic::async_trait]
impl RuneService for RuneGrpcService {
    type SessionStream = tokio_stream::wrappers::ReceiverStream<Result<SessionMessage, Status>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<SessionMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<SessionMessage>(32);

        let tx_clone = tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if tx_clone.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
        });

        let relay = Arc::clone(&self.relay);
        let session_mgr = Arc::clone(&self.session_mgr);
        tokio::spawn(async move {
            session_mgr
                .handle_session(relay, inbound, outbound_tx)
                .await;
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new());

    // ── 本地 Rune ──

    // hello: 静态响应
    relay.register(
        RuneConfig {
            name: "hello".into(),
            version: String::new(),
            description: "local hello".into(),
            supports_stream: false,
            gate: Some(GateConfig {
                path: "/hello".into(),
                method: "POST".into(),
            }),
        },
        Arc::new(LocalInvoker::new(make_handler(|_ctx, _input| async {
            Ok(Bytes::from(r#"{"message":"hello from local rune!"}"#))
        }))),
        None,
    );

    // step_b: 本地 Rust 步骤（给 JSON 加 step_b 字段）
    relay.register(
        RuneConfig {
            name: "step_b".into(),
            version: String::new(),
            description: "local step_b".into(),
            supports_stream: false,
            gate: None,
        },
        Arc::new(LocalInvoker::new(make_handler(|_ctx, input| async move {
            let mut v: serde_json::Value =
                serde_json::from_slice(&input).map_err(|e| RuneError::InvalidInput(e.to_string()))?;
            v.as_object_mut()
                .unwrap()
                .insert("step_b".into(), true.into());
            Ok(Bytes::from(serde_json::to_vec(&v).unwrap()))
        }))),
        None,
    );

    tracing::info!("registered local runes: hello, step_b");

    // ── Flow 引擎 ──

    let resolver: Arc<dyn rune_core::resolver::Resolver> = Arc::new(RoundRobinResolver::new());

    let mut flow_engine = FlowEngine::new(Arc::clone(&relay), Arc::clone(&resolver));

    // pipeline: step_a (Python) → step_b (Rust) → step_c (Python)
    flow_engine.register(
        Flow::new("pipeline")
            .chain(vec!["step_a", "step_b", "step_c"])
            .build(),
    );

    // single: 单步 Flow
    flow_engine.register(Flow::new("single").step("step_a").build());

    // empty: 空 Flow
    flow_engine.register(Flow::new("empty").build());

    tracing::info!("registered flows: pipeline, single, empty");

    // ── 启动 HTTP ──

    // Gate state (no flow_engine — flow routes are hosted in rune-server)
    let gate_state = gate::GateState {
        relay: Arc::clone(&relay),
        resolver,
        tasks: Arc::new(dashmap::DashMap::new()),
    };

    // Flow routes as extra router, injected via Extension
    let flow_engine = Arc::new(flow_engine);
    let flow_routes: Router<gate::GateState> = Router::new()
        .route("/api/v1/flows", get(list_flows))
        .route("/api/v1/flows/{name}/run", post(run_flow))
        .layer(Extension(Arc::clone(&flow_engine)));

    // Build final router with gate + flow routes
    let http_router = gate::build_router(gate_state, Some(flow_routes));
    let http_addr = "0.0.0.0:50060";

    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    tracing::info!("gate listening on {}", http_addr);

    let http_handle = tokio::spawn(async move {
        axum::serve(http_listener, http_router).await.unwrap();
    });

    // ── 启动 gRPC ──

    let grpc_addr = "0.0.0.0:50070".parse()?;
    let grpc_service = RuneGrpcService {
        relay: Arc::clone(&relay),
        session_mgr: Arc::clone(&session_mgr),
    };

    tracing::info!("grpc listening on {}", grpc_addr);

    let grpc_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RuneServiceServer::new(grpc_service))
            .serve(grpc_addr)
            .await
            .unwrap();
    });

    tokio::select! {
        _ = http_handle => tracing::info!("http server stopped"),
        _ = grpc_handle => tracing::info!("grpc server stopped"),
    }

    Ok(())
}
