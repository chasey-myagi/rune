//! Example Rust Caster — registers runes via the Rust SDK and connects to runtime.
//!
//! Usage:
//!   1. Start the Rune server: cargo run -p rune-server
//!   2. Run this caster: cargo run -p rust-caster-example

use bytes::Bytes;
use rune_framework::{Caster, CasterConfig, RuneConfig, RuneContext, StreamSender};
use rune_framework::config::GateConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let caster = Caster::new(CasterConfig {
        runtime: std::env::var("RUNE_ADDR").unwrap_or_else(|_| "localhost:50070".into()),
        caster_id: Some("example-rust-caster".into()),
        max_concurrent: 10,
        labels: [("lang".into(), "rust".into())].into_iter().collect(),
        ..Default::default()
    });

    // -----------------------------------------------------------------------
    // 1. Echo rune (unary) — simplest handler
    // -----------------------------------------------------------------------

    caster.rune(
        RuneConfig {
            name: "rs-echo".into(),
            version: "1.0.0".into(),
            description: "Echo input back".into(),
            gate: Some(GateConfig::new("/rs/echo")),
            ..Default::default()
        },
        |_ctx: RuneContext, input: Bytes| async move { Ok(input) },
    )?;

    // -----------------------------------------------------------------------
    // 2. Stream rune — emits numbered chunks
    // -----------------------------------------------------------------------

    caster.stream_rune(
        RuneConfig {
            name: "rs-stream".into(),
            version: "1.0.0".into(),
            description: "Stream numbered chunks with delay".into(),
            gate: Some(GateConfig::new("/rs/stream")),
            ..Default::default()
        },
        |_ctx: RuneContext, input: Bytes, stream: StreamSender| async move {
            let data: serde_json::Value =
                serde_json::from_slice(&input).unwrap_or(serde_json::json!({}));
            let count = data.get("count").and_then(|v| v.as_u64()).unwrap_or(5);

            for i in 0..count {
                let chunk = serde_json::json!({ "chunk": i, "total": count });
                stream.emit_json(&chunk).await?;
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Ok(())
        },
    )?;

    // -----------------------------------------------------------------------
    // 3. Rune with schema declaration
    // -----------------------------------------------------------------------

    caster.rune(
        RuneConfig {
            name: "rs-upper".into(),
            version: "1.0.0".into(),
            description: "Convert text to uppercase".into(),
            gate: Some(GateConfig::new("/rs/upper")),
            input_schema: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "text": { "type": "string" }
                },
                "required": ["text"]
            })),
            output_schema: Some(serde_json::json!({
                "type": "object",
                "properties": {
                    "result": { "type": "string" }
                }
            })),
            priority: 10,
            ..Default::default()
        },
        |_ctx: RuneContext, input: Bytes| async move {
            let data: serde_json::Value =
                serde_json::from_slice(&input).map_err(|e| rune_framework::SdkError::Other(e.to_string()))?;
            let text = data.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let result = serde_json::json!({ "result": text.to_uppercase() });
            Ok(Bytes::from(serde_json::to_vec(&result).unwrap()))
        },
    )?;

    println!("Starting Rust Caster — connecting to {}", caster.config().runtime);
    println!("Registered runes: rs-echo, rs-stream, rs-upper");

    caster.run().await?;
    Ok(())
}
