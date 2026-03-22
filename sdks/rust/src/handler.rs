//! Handler traits and registration types.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use crate::config::{FileAttachment, RuneConfig};
use crate::context::RuneContext;
use crate::error::SdkResult;
use crate::stream::StreamSender;

// ---------------------------------------------------------------------------
// Handler type aliases
// ---------------------------------------------------------------------------

/// Boxed future returned by handlers.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A unary handler function: `(ctx, input) -> Result<Bytes>`.
pub type RuneHandlerFn =
    Arc<dyn Fn(RuneContext, Bytes) -> BoxFuture<'static, SdkResult<Bytes>> + Send + Sync>;

/// A unary handler that also accepts files: `(ctx, input, files) -> Result<Bytes>`.
pub type RuneHandlerWithFilesFn = Arc<
    dyn Fn(RuneContext, Bytes, Vec<FileAttachment>) -> BoxFuture<'static, SdkResult<Bytes>>
        + Send
        + Sync,
>;

/// A stream handler function: `(ctx, input, stream) -> Result<()>`.
pub type StreamRuneHandlerFn =
    Arc<dyn Fn(RuneContext, Bytes, StreamSender) -> BoxFuture<'static, SdkResult<()>> + Send + Sync>;

/// A stream handler that also accepts files: `(ctx, input, files, stream) -> Result<()>`.
pub type StreamRuneHandlerWithFilesFn = Arc<
    dyn Fn(RuneContext, Bytes, Vec<FileAttachment>, StreamSender) -> BoxFuture<'static, SdkResult<()>>
        + Send
        + Sync,
>;

// ---------------------------------------------------------------------------
// Handler enum
// ---------------------------------------------------------------------------

/// An erased handler, either unary or stream, with or without files.
#[derive(Clone)]
pub enum HandlerKind {
    Unary(RuneHandlerFn),
    UnaryWithFiles(RuneHandlerWithFilesFn),
    Stream(StreamRuneHandlerFn),
    StreamWithFiles(StreamRuneHandlerWithFilesFn),
}

impl std::fmt::Debug for HandlerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unary(_) => write!(f, "HandlerKind::Unary(...)"),
            Self::UnaryWithFiles(_) => write!(f, "HandlerKind::UnaryWithFiles(...)"),
            Self::Stream(_) => write!(f, "HandlerKind::Stream(...)"),
            Self::StreamWithFiles(_) => write!(f, "HandlerKind::StreamWithFiles(...)"),
        }
    }
}

impl HandlerKind {
    /// Whether the handler is a stream handler.
    pub fn is_stream(&self) -> bool {
        matches!(self, Self::Stream(_) | Self::StreamWithFiles(_))
    }

    /// Whether the handler accepts file attachments.
    pub fn accepts_files(&self) -> bool {
        matches!(self, Self::UnaryWithFiles(_) | Self::StreamWithFiles(_))
    }
}

// ---------------------------------------------------------------------------
// Registered rune
// ---------------------------------------------------------------------------

/// A registered rune with its config and handler.
#[derive(Debug, Clone)]
pub struct RegisteredRune {
    /// Rune configuration.
    pub config: RuneConfig,
    /// The handler to execute.
    pub handler: HandlerKind,
}
