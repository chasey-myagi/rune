//! Unit tests for rune-framework, following test-matrix Part 1.
//!
//! Naming: test_uXX_description

use bytes::Bytes;
use rune_framework::{
    Caster, CasterConfig, FileAttachment, GateConfig, RuneConfig, RuneContext, SdkError,
    StreamSender,
};
use std::collections::HashMap;

// ============================================================================
// 1.1 Types and Configuration (U-01 ~ U-10)
// ============================================================================

#[test]
fn test_u01_rune_config_defaults() {
    let cfg = RuneConfig::new("echo");
    assert_eq!(cfg.name, "echo");
    assert_eq!(cfg.version, "0.0.0");
    assert!(!cfg.supports_stream);
    assert!(cfg.gate.is_none());
    assert_eq!(cfg.priority, 0);
    assert!(cfg.input_schema.is_none());
    assert!(cfg.output_schema.is_none());
    assert_eq!(cfg.description, "");
}

#[test]
fn test_u02_rune_config_all_fields() {
    let cfg = RuneConfig {
        name: "translate".into(),
        version: "1.2.3".into(),
        description: "Translate text".into(),
        input_schema: Some(serde_json::json!({"type": "object"})),
        output_schema: Some(serde_json::json!({"type": "string"})),
        supports_stream: true,
        gate: Some(GateConfig::with_method("/translate", "PUT")),
        priority: 5,
    };
    assert_eq!(cfg.name, "translate");
    assert_eq!(cfg.version, "1.2.3");
    assert_eq!(cfg.description, "Translate text");
    assert!(cfg.input_schema.is_some());
    assert!(cfg.output_schema.is_some());
    assert!(cfg.supports_stream);
    let gate = cfg.gate.as_ref().unwrap();
    assert_eq!(gate.path, "/translate");
    assert_eq!(gate.method, "PUT");
    assert_eq!(cfg.priority, 5);
}

#[test]
fn test_u03_rune_config_input_schema() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "text": {"type": "string"}
        },
        "required": ["text"]
    });
    let cfg = RuneConfig {
        name: "test".into(),
        input_schema: Some(schema.clone()),
        ..Default::default()
    };
    assert_eq!(cfg.input_schema.unwrap(), schema);
}

#[test]
fn test_u04_rune_config_output_schema() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "result": {"type": "number"}
        }
    });
    let cfg = RuneConfig {
        name: "test".into(),
        output_schema: Some(schema.clone()),
        ..Default::default()
    };
    assert_eq!(cfg.output_schema.unwrap(), schema);
}

#[test]
fn test_u05_gate_config_default_method() {
    let gate = GateConfig::new("/echo");
    assert_eq!(gate.path, "/echo");
    assert_eq!(gate.method, "POST");
}

#[tokio::test]
async fn test_u06_rune_context_fields() {
    let ctx = RuneContext {
        rune_name: "echo".into(),
        request_id: "req-123".into(),
        context: {
            let mut m = HashMap::new();
            m.insert("user".into(), "alice".into());
            m
        },
        cancellation: tokio_util::sync::CancellationToken::new(),
    };
    assert_eq!(ctx.rune_name, "echo");
    assert_eq!(ctx.request_id, "req-123");
    assert_eq!(ctx.context.get("user").unwrap(), "alice");
}

#[test]
fn test_u07_caster_default_runtime() {
    let cfg = CasterConfig::default();
    assert_eq!(cfg.runtime, "localhost:50070");
}

#[test]
fn test_u08_file_attachment_fields() {
    let fa = FileAttachment {
        filename: "test.txt".into(),
        data: Bytes::from("hello"),
        mime_type: "text/plain".into(),
    };
    assert_eq!(fa.filename, "test.txt");
    assert_eq!(fa.data, Bytes::from("hello"));
    assert_eq!(fa.mime_type, "text/plain");
}

#[test]
fn test_u09_caster_id_auto_generated() {
    let caster = Caster::new(CasterConfig {
        key: Some("rk_xxx".into()),
        ..Default::default()
    });
    let caster_id = caster.caster_id();
    // caster_id should be auto-generated UUID, not equal to key
    assert_ne!(caster_id, "rk_xxx");
    assert!(!caster_id.is_empty());
    // Should be a valid UUID (36 chars with hyphens)
    assert_eq!(caster_id.len(), 36);
}

#[test]
fn test_u10_caster_id_custom() {
    let caster = Caster::new(CasterConfig {
        caster_id: Some("my-caster-42".into()),
        ..Default::default()
    });
    assert_eq!(caster.caster_id(), "my-caster-42");
}

// ============================================================================
// 1.2 Rune Registration (U-11 ~ U-20)
// ============================================================================

#[tokio::test]
async fn test_u11_register_unary_handler() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .rune(RuneConfig::new("echo"), |_ctx: RuneContext, input: Bytes| async move {
            Ok(input)
        })
        .unwrap();
    assert_eq!(caster.rune_count(), 1);
    assert!(caster.get_rune_config("echo").is_some());
}

#[tokio::test]
async fn test_u12_register_stream_handler() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .stream_rune(
            RuneConfig::new("chat"),
            |_ctx: RuneContext, _input: Bytes, stream: StreamSender| async move {
                stream.emit(Bytes::from("hello")).await?;
                Ok(())
            },
        )
        .unwrap();
    assert!(caster.is_stream_rune("chat"));
    // stream_rune should set supports_stream = true
    let cfg = caster.get_rune_config("chat").unwrap();
    assert!(cfg.supports_stream);
}

#[tokio::test]
async fn test_u13_register_multiple_runes() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .rune(RuneConfig::new("a"), |_ctx: RuneContext, input: Bytes| async move {
            Ok(input)
        })
        .unwrap();
    caster
        .rune(RuneConfig::new("b"), |_ctx: RuneContext, input: Bytes| async move {
            Ok(input)
        })
        .unwrap();
    caster
        .stream_rune(
            RuneConfig::new("c"),
            |_ctx: RuneContext, _input: Bytes, _stream: StreamSender| async move { Ok(()) },
        )
        .unwrap();
    assert_eq!(caster.rune_count(), 3);
    assert!(caster.get_rune_config("a").is_some());
    assert!(caster.get_rune_config("b").is_some());
    assert!(caster.get_rune_config("c").is_some());
}

#[tokio::test]
async fn test_u14_duplicate_rune_error() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .rune(RuneConfig::new("dup"), |_ctx: RuneContext, input: Bytes| async move {
            Ok(input)
        })
        .unwrap();
    let err = caster
        .rune(RuneConfig::new("dup"), |_ctx: RuneContext, input: Bytes| async move {
            Ok(input)
        })
        .unwrap_err();
    assert!(matches!(err, SdkError::DuplicateRune(_)));
}

#[tokio::test]
async fn test_u15_register_with_gate_path() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .rune(
            RuneConfig {
                name: "gated".into(),
                gate: Some(GateConfig::new("/gated")),
                ..Default::default()
            },
            |_ctx: RuneContext, input: Bytes| async move { Ok(input) },
        )
        .unwrap();
    let cfg = caster.get_rune_config("gated").unwrap();
    let gate = cfg.gate.unwrap();
    assert_eq!(gate.path, "/gated");
    assert_eq!(gate.method, "POST");
}

#[tokio::test]
async fn test_u16_register_with_input_schema() {
    let schema = serde_json::json!({"type": "object", "properties": {"text": {"type": "string"}}});
    let caster = Caster::new(CasterConfig::default());
    caster
        .rune(
            RuneConfig {
                name: "with_schema".into(),
                input_schema: Some(schema.clone()),
                ..Default::default()
            },
            |_ctx: RuneContext, input: Bytes| async move { Ok(input) },
        )
        .unwrap();
    let cfg = caster.get_rune_config("with_schema").unwrap();
    assert_eq!(cfg.input_schema.unwrap(), schema);
}

#[tokio::test]
async fn test_u17_register_with_output_schema() {
    let schema = serde_json::json!({"type": "object"});
    let caster = Caster::new(CasterConfig::default());
    caster
        .rune(
            RuneConfig {
                name: "out".into(),
                output_schema: Some(schema.clone()),
                ..Default::default()
            },
            |_ctx: RuneContext, input: Bytes| async move { Ok(input) },
        )
        .unwrap();
    let cfg = caster.get_rune_config("out").unwrap();
    assert_eq!(cfg.output_schema.unwrap(), schema);
}

#[tokio::test]
async fn test_u18_register_with_priority() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .rune(
            RuneConfig {
                name: "prio".into(),
                priority: 42,
                ..Default::default()
            },
            |_ctx: RuneContext, input: Bytes| async move { Ok(input) },
        )
        .unwrap();
    let cfg = caster.get_rune_config("prio").unwrap();
    assert_eq!(cfg.priority, 42);
}

#[tokio::test]
async fn test_u19_handler_can_be_called_directly() {
    // Verify handler can be invoked without gRPC by calling the closure directly
    let handler =
        |_ctx: RuneContext, input: Bytes| async move { Ok::<Bytes, SdkError>(input) };
    let ctx = RuneContext {
        rune_name: "echo".into(),
        request_id: "test".into(),
        context: HashMap::new(),
        cancellation: tokio_util::sync::CancellationToken::new(),
    };
    let result = handler(ctx, Bytes::from("hello")).await.unwrap();
    assert_eq!(result, Bytes::from("hello"));
}

#[tokio::test]
async fn test_u20_stream_handler_can_be_called_directly() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let sender = StreamSender::new(tx);

    let handler = |_ctx: RuneContext, _input: Bytes, stream: StreamSender| async move {
        stream.emit(Bytes::from("chunk1")).await?;
        stream.emit(Bytes::from("chunk2")).await?;
        Ok::<(), SdkError>(())
    };

    let ctx = RuneContext {
        rune_name: "stream".into(),
        request_id: "test".into(),
        context: HashMap::new(),
        cancellation: tokio_util::sync::CancellationToken::new(),
    };

    handler(ctx, Bytes::new(), sender).await.unwrap();

    let c1 = rx.recv().await.unwrap();
    assert_eq!(c1, Bytes::from("chunk1"));
    let c2 = rx.recv().await.unwrap();
    assert_eq!(c2, Bytes::from("chunk2"));
}

// ============================================================================
// 1.3 StreamSender (U-21 ~ U-27)
// ============================================================================

#[tokio::test]
async fn test_u21_emit_bytes() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let sender = StreamSender::new(tx);
    sender.emit(Bytes::from("hello")).await.unwrap();
    let received = rx.recv().await.unwrap();
    assert_eq!(received, Bytes::from("hello"));
}

#[tokio::test]
async fn test_u22_emit_string() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let sender = StreamSender::new(tx);
    sender.emit_str("hello string").await.unwrap();
    let received = rx.recv().await.unwrap();
    assert_eq!(received, Bytes::from("hello string"));
}

#[tokio::test]
async fn test_u23_emit_json() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let sender = StreamSender::new(tx);
    let data = serde_json::json!({"key": "value", "num": 42});
    sender.emit_json(&data).await.unwrap();
    let received = rx.recv().await.unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&received).unwrap();
    assert_eq!(parsed["key"], "value");
    assert_eq!(parsed["num"], 42);
}

#[tokio::test]
async fn test_u24_end_marks_stream_ended() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<Bytes>(32);
    let sender = StreamSender::new(tx);
    assert!(!sender.is_ended());
    sender.end();
    assert!(sender.is_ended());
}

#[tokio::test]
async fn test_u25_emit_after_end_errors() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<Bytes>(32);
    let sender = StreamSender::new(tx);
    sender.end();
    let err = sender.emit(Bytes::from("data")).await.unwrap_err();
    assert!(matches!(err, SdkError::StreamEnded));
}

#[tokio::test]
async fn test_u26_multiple_emit() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let sender = StreamSender::new(tx);
    for i in 0..5 {
        sender
            .emit(Bytes::from(format!("chunk-{i}")))
            .await
            .unwrap();
    }
    for i in 0..5 {
        let received = rx.recv().await.unwrap();
        assert_eq!(received, Bytes::from(format!("chunk-{i}")));
    }
}

#[tokio::test]
async fn test_u27_end_idempotent() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<Bytes>(32);
    let sender = StreamSender::new(tx);
    sender.end();
    sender.end(); // second call should not panic
    sender.end(); // third call
    assert!(sender.is_ended());
}

// ============================================================================
// 1.4 Connection Configuration (U-28 ~ U-33)
// ============================================================================

#[test]
fn test_u28_reconnect_base_delay_configurable() {
    let cfg = CasterConfig {
        reconnect_base_delay_secs: 2.5,
        ..Default::default()
    };
    assert!((cfg.reconnect_base_delay_secs - 2.5).abs() < f64::EPSILON);
}

#[test]
fn test_u29_reconnect_max_delay_configurable() {
    let cfg = CasterConfig {
        reconnect_max_delay_secs: 60.0,
        ..Default::default()
    };
    assert!((cfg.reconnect_max_delay_secs - 60.0).abs() < f64::EPSILON);
}

#[test]
fn test_u30_heartbeat_interval_configurable() {
    let cfg = CasterConfig {
        heartbeat_interval_secs: 5.0,
        ..Default::default()
    };
    assert!((cfg.heartbeat_interval_secs - 5.0).abs() < f64::EPSILON);
}

#[test]
fn test_u31_max_concurrent_configurable() {
    let cfg = CasterConfig {
        max_concurrent: 20,
        ..Default::default()
    };
    assert_eq!(cfg.max_concurrent, 20);
}

#[test]
fn test_u32_labels_configurable() {
    let mut labels = HashMap::new();
    labels.insert("env".into(), "prod".into());
    labels.insert("region".into(), "us-west".into());
    let cfg = CasterConfig {
        labels: labels.clone(),
        ..Default::default()
    };
    assert_eq!(cfg.labels, labels);
}

#[test]
fn test_u33_api_key() {
    let cfg = CasterConfig {
        key: Some("rk_secret_key".into()),
        ..Default::default()
    };
    assert_eq!(cfg.key.unwrap(), "rk_secret_key");
}

// ============================================================================
// 1.5 FileAttachment Detection (U-34 ~ U-37)
// ============================================================================

#[tokio::test]
async fn test_u34_unary_handler_no_files() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .rune(
            RuneConfig::new("no_files"),
            |_ctx: RuneContext, input: Bytes| async move { Ok(input) },
        )
        .unwrap();
    assert!(!caster.rune_accepts_files("no_files"));
}

#[tokio::test]
async fn test_u35_unary_handler_with_files() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .rune_with_files(
            RuneConfig::new("with_files"),
            |_ctx: RuneContext, input: Bytes, _files: Vec<FileAttachment>| async move {
                Ok(input)
            },
        )
        .unwrap();
    assert!(caster.rune_accepts_files("with_files"));
}

#[tokio::test]
async fn test_u36_stream_handler_no_files() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .stream_rune(
            RuneConfig::new("stream_no_files"),
            |_ctx: RuneContext, _input: Bytes, _stream: StreamSender| async move { Ok(()) },
        )
        .unwrap();
    assert!(!caster.rune_accepts_files("stream_no_files"));
}

#[tokio::test]
async fn test_u37_stream_handler_with_files() {
    let caster = Caster::new(CasterConfig::default());
    caster
        .stream_rune_with_files(
            RuneConfig::new("stream_with_files"),
            |_ctx: RuneContext,
             _input: Bytes,
             _files: Vec<FileAttachment>,
             _stream: StreamSender| async move { Ok(()) },
        )
        .unwrap();
    assert!(caster.rune_accepts_files("stream_with_files"));
}

// ============================================================================
// Rust-specific: Type Safety, Send + Sync, Clone/Debug
// ============================================================================

#[test]
fn test_rust_send_sync_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    // Caster should be Send + Sync so it can be shared across tasks
    assert_send::<Caster>();
    assert_sync::<Caster>();

    // StreamSender should be Send + Sync
    assert_send::<StreamSender>();
    assert_sync::<StreamSender>();

    // Config types should be Send + Sync
    assert_send::<CasterConfig>();
    assert_sync::<CasterConfig>();
    assert_send::<RuneConfig>();
    assert_sync::<RuneConfig>();
    assert_send::<RuneContext>();
    assert_sync::<RuneContext>();
    assert_send::<FileAttachment>();
    assert_sync::<FileAttachment>();
}

#[test]
fn test_rust_clone_derive() {
    let cfg = RuneConfig::new("test");
    let _cloned = cfg.clone();

    let caster_cfg = CasterConfig::default();
    let _cloned = caster_cfg.clone();

    let fa = FileAttachment {
        filename: "a.txt".into(),
        data: Bytes::from("data"),
        mime_type: "text/plain".into(),
    };
    let _cloned = fa.clone();

    let gate = GateConfig::new("/path");
    let _cloned = gate.clone();
}

#[test]
fn test_rust_debug_derive() {
    let cfg = RuneConfig::new("test");
    let debug = format!("{:?}", cfg);
    assert!(debug.contains("test"));

    let caster_cfg = CasterConfig::default();
    let debug = format!("{:?}", caster_cfg);
    assert!(debug.contains("localhost:50070"));

    let fa = FileAttachment {
        filename: "a.txt".into(),
        data: Bytes::from("data"),
        mime_type: "text/plain".into(),
    };
    let debug = format!("{:?}", fa);
    assert!(debug.contains("a.txt"));
}

#[test]
fn test_rust_sdk_error_display() {
    let err = SdkError::DuplicateRune("echo".into());
    assert_eq!(err.to_string(), "rune 'echo' is already registered");

    let err = SdkError::StreamEnded;
    assert_eq!(err.to_string(), "stream already ended");
}

// StreamSender should be constructable only via new(tx) — test the pub(crate) pattern
// by verifying that the public API works as expected
#[tokio::test]
async fn test_rust_stream_sender_new_requires_channel() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<Bytes>(1);
    let sender = StreamSender::new(tx);
    // Should be usable
    assert!(!sender.is_ended());
}

// ============================================================================
// NF-9: Caster should have a shutdown/stop mechanism
// ============================================================================

#[test]
fn test_nf9_caster_has_stop_method() {
    let caster = Caster::new(CasterConfig::default());
    // stop() should exist and be callable without panicking
    caster.stop();
}

#[test]
fn test_nf9_stop_is_idempotent() {
    let caster = Caster::new(CasterConfig::default());
    caster.stop();
    caster.stop();
    caster.stop();
    // No panic
}

#[tokio::test]
async fn test_nf9_run_exits_after_stop() {
    let caster = std::sync::Arc::new(Caster::new(CasterConfig {
        runtime: "localhost:59999".into(), // unreachable port
        reconnect_base_delay_secs: 0.1,
        reconnect_max_delay_secs: 0.2,
        ..Default::default()
    }));

    let caster_clone = caster.clone();
    // Call stop() after a short delay — run() should exit
    let stop_handle = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        caster_clone.stop();
    });

    // run() should not hang forever; it should exit after stop() is called
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        caster.run(),
    )
    .await;

    stop_handle.await.unwrap();

    // Should complete (not timeout), and the result should be Ok or an error
    assert!(result.is_ok(), "run() should have exited after stop()");
}

// ============================================================================
// NF-16: attach rejected should return Err, not Ok(())
// ============================================================================

// Note: NF-16 is tested structurally. The fix changes the attach rejected path
// from `break` (which returns Ok(())) to `return Err(...)`.
// A full integration test would require a mock gRPC server.
// We verify via code inspection that the error path is correct.
// The following test verifies SdkError::Other can hold the reject message.
#[test]
fn test_nf16_sdk_error_other_for_attach_rejected() {
    let err = SdkError::Other("attach rejected: unauthorized".into());
    assert_eq!(err.to_string(), "attach rejected: unauthorized");
    // Verify it's the right variant
    assert!(matches!(err, SdkError::Other(_)));
}
