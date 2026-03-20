# Rune

**协议优先的多语言函数执行框架。定义函数，获得路由 + 流式 + 异步 + 编排。**

## 30 秒理解

### 方式 1：Rust 库模式

```rust
let mut app = App::new();
app.rune(
    RuneConfig::new("translate")
        .gate("/translate"),
    translate_handler,
);
app.run().await;
// POST /translate 已就绪
```

### 方式 2：Python Caster 模式

```python
from rune_sdk import Caster

caster = Caster("localhost:50070")

@caster.rune("translate", gate="/translate")
async def translate(ctx, input):
    result = do_translation(input)
    return result

caster.run()
```

启动后自动获得：

```
POST /translate              ← sync call
POST /translate?stream=true  ← real SSE streaming
POST /translate?async=true   ← async task
GET  /health                 ← health check
```

## 核心概念

| 名称 | 是什么 |
|------|--------|
| **Rune** | 一个函数。输入 bytes, 输出 bytes。任何语言实现。 |
| **Caster** | 远程执行者。通过 gRPC 连接 Runtime，注册并执行 Rune。 |
| **Gate** | HTTP 入口。根据 `gate.path` 声明自动生成真实业务路由。 |
| **Relay** | 注册表 + 路由。记录所有 Rune 在哪，把请求中继过去。 |
| **Flow** | 顺序链式编排。多个 Rune 组成的处理流水线。 |

## 它替代了什么

| 能力 | 传统方案 | Rune |
|------|---------|------|
| HTTP API | Gin / FastAPI / Express | 声明 `gate.path`，自动路由 |
| 流式 SSE | 手写 SSE 逻辑 | `?stream=true`，真流式 |
| 异步任务 | Celery / Asynq + Redis | `?async=true`，内存 task store |
| 链式编排 | Temporal / Airflow | `rune-flow` 顺序 chain |
| 分布式执行 | 自建 gRPC 服务 | Caster 协议，本地/远程统一 |
| 多语言 | 各工具各自的 SDK | 统一 Wire Protocol，先从 Python 开始 |

## 开始使用

### Rust Runtime

```bash
# 克隆仓库
git clone https://github.com/aspect-build/rune.git
cd rune

# 构建
cargo build

# 运行示例
cargo run --bin rune-server
```

### Python SDK

```bash
pip install rune-sdk
```

```python
from rune_sdk import Caster

caster = Caster("localhost:50070")

@caster.rune("echo", gate="/echo")
async def echo(ctx, input):
    return input

@caster.stream_rune("count", gate="/count")
async def count(ctx, input, sender):
    for i in range(10):
        await sender.emit(str(i))
    await sender.end()

caster.run()
```

## 状态

**v0.1.0** — 首个正式版本。

已交付：
- Rust 参考 Runtime（rune-core / rune-gate / rune-flow / rune-server）
- 官方 Python Caster SDK（rune-sdk）
- sync / real stream / async 三种调用模式
- `gate.path` 真实业务路由
- 最小顺序 Flow chain（支持 local + remote 混合）
- 完整协议保证（见 [Protocol Guarantees](docs/protocol-guarantees.md)）

未纳入 v0.1.0：
- Schema 校验 / OpenAPI 生成
- 完整 DAG 工作流
- Durable execution
- Go / TypeScript / Rust SDK

## 文档

| 文档 | 说明 |
|------|------|
| [Protocol Guarantees](docs/protocol-guarantees.md) | v0.1.0 行为保证契约 |
| [v0.1.0 Scope](docs/scope/v0.1.0.md) | 版本范围与交付计划 |
