//! SDK error types.

use thiserror::Error;

/// Errors that can occur in the Rune SDK.
#[derive(Debug, Error)]
pub enum SdkError {
    /// A rune with this name is already registered.
    #[error("rune '{0}' is already registered")]
    DuplicateRune(String),

    /// Stream has already ended; cannot emit more data.
    #[error("stream already ended")]
    StreamEnded,

    /// gRPC transport error.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// gRPC status error.
    #[error("grpc error: {0}")]
    Grpc(#[from] tonic::Status),

    /// Channel send error.
    #[error("channel send error: {0}")]
    ChannelSend(String),

    /// Handler execution error.
    #[error("handler error: {0}")]
    HandlerError(String),

    /// Invalid URI.
    #[error("invalid uri: {0}")]
    InvalidUri(String),

    /// Attach was rejected by the runtime — permanent, do not retry.
    #[error("attach rejected: {0}")]
    AttachRejected(String),

    /// Other errors.
    #[error("{0}")]
    Other(String),
}

/// Result alias for SDK operations.
pub type SdkResult<T> = Result<T, SdkError>;
