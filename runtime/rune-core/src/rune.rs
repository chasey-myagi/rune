use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Rune 的执行上下文
#[derive(Debug, Clone)]
pub struct RuneContext {
    pub rune_name: String,
    pub request_id: String,
    pub context: std::collections::HashMap<String, String>,
    pub timeout: std::time::Duration,
}

/// Rune 注册配置
#[derive(Debug, Clone)]
pub struct RuneConfig {
    pub name: String,
    pub version: String,
    pub description: String,
    pub supports_stream: bool,
    pub gate: Option<GateConfig>,
}

#[derive(Debug, Clone)]
pub struct GateConfig {
    pub path: String,
    pub method: String, // default "POST"
}

impl Default for GateConfig {
    fn default() -> Self {
        Self { path: String::new(), method: "POST".to_string() }
    }
}

/// Rune 执行错误
#[derive(Debug, thiserror::Error)]
pub enum RuneError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("execution failed: {code} - {message}")]
    ExecutionFailed { code: String, message: String },
    #[error("rune not found: {0}")]
    NotFound(String),
    #[error("no available caster")]
    Unavailable,
    #[error("timeout")]
    Timeout,
    #[error("cancelled")]
    Cancelled,
    #[error("internal: {0}")]
    Internal(#[from] anyhow::Error),
}

/// Rune handler：一个异步函数 (context, input) → output
pub type RuneHandler = Arc<
    dyn Fn(RuneContext, Bytes) -> Pin<Box<dyn Future<Output = Result<Bytes, RuneError>> + Send>>
        + Send
        + Sync,
>;

/// 便捷宏：把 async fn 包装成 RuneHandler
pub fn make_handler<F, Fut>(f: F) -> RuneHandler
where
    F: Fn(RuneContext, Bytes) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, RuneError>> + Send + 'static,
{
    Arc::new(move |ctx, input| Box::pin(f(ctx, input)))
}

/// 流式 Rune handler
#[async_trait::async_trait]
pub trait StreamRuneHandler: Send + Sync + 'static {
    async fn execute(&self, ctx: RuneContext, input: Bytes, tx: StreamSender) -> Result<(), RuneError>;
}

/// StreamSender — handler pushes stream events through this
pub struct StreamSender {
    tx: mpsc::Sender<Result<Bytes, RuneError>>,
}

impl StreamSender {
    pub fn new(tx: mpsc::Sender<Result<Bytes, RuneError>>) -> Self {
        Self { tx }
    }

    pub async fn emit(&self, data: Bytes) -> Result<(), RuneError> {
        self.tx.send(Ok(data)).await
            .map_err(|_| RuneError::Internal(anyhow::anyhow!("stream receiver dropped")))
    }

    pub async fn end(self) -> Result<(), RuneError> {
        // Drop self, closing the channel
        Ok(())
    }
}
