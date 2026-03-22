//! Minimal example: register a local Rune using the App builder.
//!
//! This example shows the App API but does NOT start a server.
//! For a full server example, see rune-server.

use rune_core::app::App;
use rune_core::rune::{RuneConfig, GateConfig, make_handler};
use bytes::Bytes;

fn main() {
    // Build an App with local Runes
    let mut app = App::new();

    // Register a simple hello rune
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
        },
        make_handler(|_ctx, _input| async {
            Ok(Bytes::from(r#"{"message": "hello from local rune!"}"#))
        }),
    );

    // Register an echo rune
    app.rune(
        RuneConfig {
            name: "echo".into(),
            version: "1.0.0".into(),
            description: "Echoes input back".into(),
            supports_stream: false,
            gate: Some(GateConfig { path: "/echo".into(), method: "POST".into() }),
            input_schema: None,
            output_schema: None,
            priority: 0,
        },
        make_handler(|_ctx, input| async move { Ok(input) }),
    );

    println!("App created with {} runes registered.", 2);
    println!("In a real application, call app.run().await to start the server.");
}
