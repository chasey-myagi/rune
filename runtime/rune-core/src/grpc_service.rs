use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use rune_proto::rune_service_server::RuneService;
use rune_proto::SessionMessage;
use crate::relay::Relay;
use crate::session::SessionManager;

pub struct RuneGrpcService {
    pub relay: Arc<Relay>,
    pub session_mgr: Arc<SessionManager>,
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
