use bytes::Bytes;
use crate::rune::{RuneContext, RuneError, RuneHandler};

/// 统一调用接口。上层不关心 Rune 在本地还是远程。
#[async_trait::async_trait]
pub trait RuneInvoker: Send + Sync {
    async fn invoke(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError>;
}

/// 进程内调用——直接调 handler
pub struct LocalInvoker {
    handler: RuneHandler,
}

impl LocalInvoker {
    pub fn new(handler: RuneHandler) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl RuneInvoker for LocalInvoker {
    async fn invoke(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        (self.handler)(ctx, input).await
    }
}

/// 远程调用——通过 SessionManager 发 gRPC
pub struct RemoteInvoker {
    pub(crate) session: std::sync::Arc<crate::session::SessionManager>,
    pub(crate) caster_id: String,
}

#[async_trait::async_trait]
impl RuneInvoker for RemoteInvoker {
    async fn invoke(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        self.session
            .execute(&self.caster_id, &ctx.request_id, &ctx.rune_name, input)
            .await
    }
}
