use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use crate::rune::{RuneContext, RuneError, RuneHandler, StreamRuneHandler, StreamSender};

#[async_trait::async_trait]
pub trait RuneInvoker: Send + Sync {
    async fn invoke_once(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError>;
    async fn invoke_stream(&self, ctx: RuneContext, input: Bytes)
        -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError>;
}

/// 进程内调用——直接调 handler
pub struct LocalInvoker {
    handler: RuneHandler,
}

impl LocalInvoker {
    pub fn new(handler: RuneHandler) -> Self { Self { handler } }
}

#[async_trait::async_trait]
impl RuneInvoker for LocalInvoker {
    async fn invoke_once(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        (self.handler)(ctx, input).await
    }

    async fn invoke_stream(&self, ctx: RuneContext, input: Bytes)
        -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError>
    {
        // Fallback: call unary handler, wrap result as single-item stream
        let result = (self.handler)(ctx, input).await;
        let (tx, rx) = mpsc::channel(1);
        let _ = tx.send(result).await;
        Ok(rx)
    }
}

/// Local stream invoker — wraps a StreamRuneHandler
pub struct LocalStreamInvoker {
    handler: Arc<dyn StreamRuneHandler>,
}

impl LocalStreamInvoker {
    pub fn new(handler: Arc<dyn StreamRuneHandler>) -> Self { Self { handler } }
}

#[async_trait::async_trait]
impl RuneInvoker for LocalStreamInvoker {
    async fn invoke_once(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        // Collect stream into single response
        let mut rx = self.invoke_stream(ctx, input).await?;
        let mut collected = Vec::new();
        while let Some(chunk) = rx.recv().await {
            collected.push(chunk?);
        }
        // Return last chunk or empty
        Ok(collected.into_iter().last().unwrap_or_default())
    }

    async fn invoke_stream(&self, ctx: RuneContext, input: Bytes)
        -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError>
    {
        let (tx, rx) = mpsc::channel(32);
        let sender = StreamSender::new(tx);
        let handler = Arc::clone(&self.handler);
        tokio::spawn(async move {
            if let Err(e) = handler.execute(ctx, input, sender).await {
                // Error is dropped since channel may be closed
                tracing::error!("stream handler error: {}", e);
            }
        });
        Ok(rx)
    }
}

/// 远程调用——通过 SessionManager 发 gRPC
pub struct RemoteInvoker {
    pub(crate) session: Arc<crate::session::SessionManager>,
    pub(crate) caster_id: String,
}

#[async_trait::async_trait]
impl RuneInvoker for RemoteInvoker {
    async fn invoke_once(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        self.session.execute(
            &self.caster_id, &ctx.request_id, &ctx.rune_name, input,
            ctx.context.clone(), ctx.timeout,
        ).await
    }

    async fn invoke_stream(&self, ctx: RuneContext, input: Bytes)
        -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError>
    {
        self.session.execute_stream(
            &self.caster_id, &ctx.request_id, &ctx.rune_name, input,
            ctx.context.clone(), ctx.timeout,
        ).await
    }
}
