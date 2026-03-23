//! Minimal example: register local Runes using the App builder.
//!
//! This example shows the App API but does NOT start a server.
//! For a full server example, see rune-server.

use rune_core::app::App;
use rune_core::rune::{GateConfig, RuneConfig, make_handler};
use bytes::Bytes;

fn main() {
    // Build an App with local Runes
    let mut app = App::new();

    // 1. Simple hello rune — minimal config
    app.rune(
        RuneConfig {
            name: "hello".into(),
            version: "1.0.0".into(),
            description: "Returns a greeting".into(),
            supports_stream: false,
            gate: Some(GateConfig { path: "/hello".into(), method: "POST".into() }),
            input_schema: None,
            output_schema: None,
            priority: 0,
            labels: Default::default(),
        },
        make_handler(|_ctx, _input| async {
            Ok(Bytes::from(r#"{"message": "hello from local rune!"}"#))
        }),
    );

    // 2. Echo rune — with JSON Schema validation + priority
    app.rune(
        RuneConfig {
            name: "echo".into(),
            version: "1.0.0".into(),
            description: "Echoes input back with schema validation".into(),
            supports_stream: false,
            gate: Some(GateConfig { path: "/echo".into(), method: "POST".into() }),
            input_schema: Some(r#"{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}"#.into()),
            output_schema: Some(r#"{"type":"object"}"#.into()),
            priority: 10,
            labels: [("env".into(), "dev".into())].into_iter().collect(),
        },
        make_handler(|_ctx, input| async move { Ok(input) }),
    );

    println!("App created with {} runes registered (hello, echo).", 2);
    println!("In a real application, call app.run().await to start the server.");
}
