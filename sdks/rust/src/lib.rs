//! Rune SDK for Rust — Caster client library.
//!
//! Connect to a Rune Runtime, register handlers, and serve requests.
//!
//! # Example
//! ```rust,no_run
//! use rune_sdk::{Caster, CasterConfig, RuneConfig, RuneContext};
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
pub mod stream;

// Re-exports for convenience
pub use caster::Caster;
pub use config::{CasterConfig, FileAttachment, GateConfig, RuneConfig};
pub use context::RuneContext;
pub use error::{SdkError, SdkResult};
pub use handler::{HandlerKind, RegisteredRune};
pub use stream::StreamSender;
