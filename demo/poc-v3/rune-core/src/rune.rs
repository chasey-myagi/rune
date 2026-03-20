use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Rune 的执行上下文
#[derive(Debug, Clone)]
pub struct RuneContext {
    pub rune_name: String,
    pub request_id: String,
}

/// Rune 注册配置
#[derive(Debug, Clone)]
pub struct RuneConfig {
    pub name: String,
    pub description: String,
    pub gate_path: Option<String>,
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
