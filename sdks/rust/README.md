# Rune Rust SDK

Official Rust Caster SDK for the Rune framework.

## 依赖

```toml
[dependencies]
rune-sdk = { path = "sdks/rust" }
tokio = { version = "1", features = ["full"] }
bytes = "1"
```

## 快速开始

```rust
use rune_sdk::{Caster, CasterConfig, RuneConfig, RuneContext, GateConfig};
use bytes::Bytes;

#[tokio::main]
async fn main() {
    let caster = Caster::new(CasterConfig::default());

    caster.rune(
        RuneConfig {
            name: "echo".into(),
            gate: Some(GateConfig::new("/echo")),
            ..Default::default()
        },
        |_ctx: RuneContext, input: Bytes| async move { Ok(input) },
    ).unwrap();

    caster.run().await.unwrap();
}
```

启动后 Runtime 自动暴露：
```
POST /echo              -- sync
POST /echo?stream=true  -- SSE streaming
POST /echo?async=true   -- async task
```

## Caster 配置

```rust
use std::collections::HashMap;

let config = CasterConfig {
    runtime: "localhost:50070".into(),      // Runtime gRPC 地址
    key: Some("rk_xxx".into()),            // API Key
    caster_id: Some("my-rust-caster".into()), // Caster ID（默认自动生成 UUID）
    max_concurrent: 10,                     // 最大并发处理数
    labels: HashMap::from([                 // Caster 标签
        ("env".into(), "prod".into()),
        ("gpu".into(), "true".into()),
    ]),
    heartbeat_interval_secs: 10.0,         // 心跳间隔（秒）
    reconnect_base_delay_secs: 1.0,        // 重连初始延迟（秒）
    reconnect_max_delay_secs: 30.0,        // 重连最大延迟（秒）
};

let caster = Caster::new(config);
```

## 注册 Rune

### Unary Handler

```rust
use rune_sdk::{RuneConfig, GateConfig};

caster.rune(
    RuneConfig {
        name: "translate".into(),
        version: "1.0.0".into(),
        description: "Translate text".into(),
        gate: Some(GateConfig::new("/translate")),
        priority: 10,
        input_schema: Some(serde_json::json!({
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "lang": {"type": "string"}
            },
            "required": ["text", "lang"]
        })),
        output_schema: Some(serde_json::json!({
            "type": "object",
            "properties": {
                "translated": {"type": "string"}
            }
        })),
        ..Default::default()
    },
    |_ctx, input| async move {
        let data: serde_json::Value = serde_json::from_slice(&input)?;
        let result = serde_json::json!({"translated": "hello"});
        Ok(Bytes::from(serde_json::to_vec(&result)?))
    },
).unwrap();
```

### Unary Handler with Files

```rust
use rune_sdk::FileAttachment;

caster.rune_with_files(
    RuneConfig {
        name: "process-doc".into(),
        gate: Some(GateConfig::new("/process")),
        ..Default::default()
    },
    |_ctx, input, files: Vec<FileAttachment>| async move {
        for f in &files {
            println!("{} {} {}", f.filename, f.mime_type, f.data.len());
        }
        let result = serde_json::json!({"processed": files.len()});
        Ok(Bytes::from(serde_json::to_vec(&result)?))
    },
).unwrap();
```

### Streaming Handler

```rust
use rune_sdk::StreamSender;

caster.stream_rune(
    RuneConfig {
        name: "generate".into(),
        gate: Some(GateConfig::new("/generate")),
        ..Default::default()
    },
    |_ctx, _input, sender: StreamSender| async move {
        for i in 0..10 {
            sender.send(Bytes::from(format!("chunk {}", i))).await?;
        }
        Ok(())
    },
).unwrap();
```

### Streaming Handler with Files

```rust
caster.stream_rune_with_files(
    RuneConfig {
        name: "stream-process".into(),
        gate: Some(GateConfig::new("/stream-process")),
        ..Default::default()
    },
    |_ctx, _input, files: Vec<FileAttachment>, sender: StreamSender| async move {
        for f in &files {
            sender.send(Bytes::from(format!("processing {}", f.filename))).await?;
        }
        sender.send(Bytes::from("done")).await?;
        Ok(())
    },
).unwrap();
```

## Context

每次调用都会传入 `RuneContext`：

```rust
caster.rune(
    RuneConfig::new("example"),
    |ctx: RuneContext, input: Bytes| async move {
        println!("rune: {}", ctx.rune_name);
        println!("request: {}", ctx.request_id);
        println!("context: {:?}", ctx.context);

        // 取消感知
        if ctx.cancellation.is_cancelled() {
            return Err(rune_sdk::SdkError::Cancelled);
        }

        Ok(input)
    },
).unwrap();
```

`ctx.cancellation` 是 `tokio_util::sync::CancellationToken`，可用于检测取消：

```rust
tokio::select! {
    result = do_long_work() => { Ok(result?) }
    _ = ctx.cancellation.cancelled() => {
        Err(rune_sdk::SdkError::Cancelled)
    }
}
```

## 错误处理

SDK 使用 `SdkResult<T>` 类型（`Result<T, SdkError>`）：

```rust
use rune_sdk::{SdkError, SdkResult};

// SdkError variants:
// - DuplicateRune(String)   -- 重复注册
// - InvalidUri(String)      -- 无效 URI
// - ChannelSend(String)     -- 通道发送失败
// - Cancelled               -- 请求被取消
// - Transport(...)          -- gRPC 传输错误
```

## 运行与查询

```rust
// 阻塞运行（自动重连）
caster.run().await?;

// 查询状态
println!("Rune count: {}", caster.rune_count());
println!("Caster ID: {}", caster.caster_id());
println!("Is stream: {}", caster.is_stream_rune("generate"));
println!("Accepts files: {}", caster.rune_accepts_files("process-doc"));
```

## 开发

```bash
cd sdks/rust
cargo test                        # 单元测试
cargo test --test e2e_test        # E2E 测试（需要运行中的 Runtime）
```
