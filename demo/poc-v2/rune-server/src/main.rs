use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use rune_proto::rune_service_server::{RuneService, RuneServiceServer};
use rune_proto::SessionMessage;
use rune_core::relay::Relay;
use rune_core::rune::{RuneConfig, make_handler};
use rune_core::invoker::LocalInvoker;
use rune_core::session::SessionManager;
use rune_gate::gate::{self, GateState};
use bytes::Bytes;

/// gRPC 服务实现
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

        // 转发 outbound 消息到 gRPC stream
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if tx_clone.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
        });

        // 处理 session
        let relay = Arc::clone(&self.relay);
        let session_mgr = Arc::clone(&self.session_mgr);
        tokio::spawn(async move {
            session_mgr.handle_session(relay, inbound, outbound_tx).await;
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let relay = Arc::new(Relay::new());
    let session_mgr = Arc::new(SessionManager::new());

    // 注册进程内 "hello" Rune
    let handler = make_handler(|_ctx, _input| async {
        Ok(Bytes::from(r#"{"message":"hello from local rune!"}"#))
    });
    relay.register(
        RuneConfig {
            name: "hello".into(),
            description: "local hello rune".into(),
            gate_path: Some("/hello".into()),
        },
        Arc::new(LocalInvoker::new(handler)),
        None,
    );
    tracing::info!("registered local rune: hello");

    // 启动 HTTP (Gate)
    let gate_state = GateState { relay: Arc::clone(&relay) };
    let http_router = gate::build_router(gate_state);
    let http_addr = "0.0.0.0:50060";

    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    tracing::info!("gate listening on {}", http_addr);

    let http_handle = tokio::spawn(async move {
        axum::serve(http_listener, http_router).await.unwrap();
    });

    // 启动 gRPC
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

    // 等待两个 server
    tokio::select! {
        _ = http_handle => tracing::info!("http server stopped"),
        _ = grpc_handle => tracing::info!("grpc server stopped"),
    }

    Ok(())
}
