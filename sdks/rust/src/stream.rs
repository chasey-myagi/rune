//! StreamSender for streaming Rune handlers.

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::{SdkError, SdkResult};

/// Sends stream events to the runtime.
///
/// Accepts `Bytes`, `&str`, `String`, or any `serde::Serialize` value
/// (via [`emit_json`]).
#[derive(Debug)]
pub struct StreamSender {
    tx: mpsc::Sender<Bytes>,
    ended: std::sync::atomic::AtomicBool,
}

impl StreamSender {
    /// Create a new StreamSender backed by the given channel sender.
    ///
    /// In typical usage this is called internally by the Caster.
    /// Exposed publicly for testing and advanced use cases.
    pub fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self {
            tx,
            ended: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Whether the stream has been ended.
    pub fn is_ended(&self) -> bool {
        self.ended.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Emit raw bytes.
    pub async fn emit(&self, data: Bytes) -> SdkResult<()> {
        if self.is_ended() {
            return Err(SdkError::StreamEnded);
        }
        self.tx
            .send(data)
            .await
            .map_err(|e| SdkError::ChannelSend(e.to_string()))
    }

    /// Emit a string (auto-encoded to UTF-8 bytes).
    pub async fn emit_str(&self, data: &str) -> SdkResult<()> {
        self.emit(Bytes::from(data.to_owned())).await
    }

    /// Emit a serializable value as JSON bytes.
    pub async fn emit_json<T: serde::Serialize>(&self, data: &T) -> SdkResult<()> {
        let json = serde_json::to_vec(data)
            .map_err(|e| SdkError::Other(format!("json serialization error: {e}")))?;
        self.emit(Bytes::from(json)).await
    }

    /// Signal end of stream. Idempotent — calling multiple times is safe.
    pub fn end(&self) {
        self.ended.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
