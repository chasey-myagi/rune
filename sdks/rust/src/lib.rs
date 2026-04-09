//! Rune SDK for Rust — Caster client library.
//!
//! Connect to a Rune Runtime, register handlers, and serve requests.
//!
//! # Example
//! ```rust,no_run
//! use rune_framework::{Caster, CasterConfig, RuneConfig, RuneContext};
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() {
//!     let caster = Caster::new(CasterConfig::default());
//!
//!     caster.rune(
//!         RuneConfig::new("echo"),
//!         |_ctx: RuneContext, input: Bytes| async move { Ok(input) },
//!     ).unwrap();
//!
//!     caster.run().await.unwrap();
//! }
//! ```

pub mod caster;
pub mod config;
pub mod context;
pub mod error;
pub mod handler;
mod pilot_client;
pub mod stream;

// Re-exports for convenience
pub use caster::Caster;
pub use config::{CasterConfig, FileAttachment, GateConfig, LoadReport, RuneConfig, ScalePolicy};
pub use context::RuneContext;
pub use error::{SdkError, SdkResult};
pub use handler::{HandlerKind, RegisteredRune};
pub use pilot_client::PilotClient;
pub use stream::StreamSender;
