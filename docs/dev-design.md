# Rune — 技术设计文档

> 定义函数，获得 API + 工作流 + 分布式执行。

---

## 1. 命名

| 概念 | 名称 | 含义 |
|------|------|------|
| 框架 | **Rune** | 古北欧符文——一个符文就是一段力量 |
| 最小执行单元 | **Rune** | 一个函数 = 一段可调用的力量 |
| 协议 | **Rune Wire Protocol (RWP)** | 线上通信协议 |
| 远程执行者 | **Caster** | 施法者——执行 Rune 的远程进程 |
| HTTP 入口 | **Gate** | 门——HTTP API 入口 |
| 模块 | **Kit** | 工具包——一组相关 Rune 的封装 |
| 注册表 | **Relay** | 中继——记录所有 Rune 在哪 |
| 调度器 | **Resolver** | 从候选实例中选择目标 |
| 统一调用 | **Invoker** | 统一本地/远程调用，对上层透明 |
| 工作流 | **Flow** | 流——多个 Rune 编排成的 DAG |

**命名哲学：** 框架名（Rune）和核心角色（Caster/Gate）有辨识度，功能性概念（Kit/Relay/Flow）自解释、不需要学新词。

**Runtime 核心四对象：**

```
Gate / Flow
  → Relay.find(rune)           // 查注册表：有哪些实例
  → Resolver.pick(candidates)  // 选一个目标
  → Invoker.invoke(target)     // 统一调用（本地/远程透明）
  → SessionManager (if remote) // 管理 Caster gRPC 连接
```

---

## 2. 架构分层

```
Layer 0: 协议 (Rune Wire Protocol)
         proto 文件，语言无关
         ↓
Layer 1: Runtime (rune-core crate, ~3000 行)
         Relay (注册表) + Resolver (调度)
         Invoker (统一本地/远程调用)
         SessionManager (Caster gRPC 连接管理)
         生命周期 (启动 / 关闭 / 信号)
         ↓
Layer 2: Gate (rune-gate crate, ~1500 行，可选)
         HTTP 端点自动映射
         sync / stream(SSE) / async
         Schema 校验
         OpenAPI 生成
         ↓
Layer 3: Extensions (各自独立 crate)
         rune-flow   — Flow DAG 引擎
         rune-persist  — 状态持久化
         rune-auth     — 认证
         rune-metrics  — 可观测性
```

**Layer 1 和 Layer 2 分开。** 不是所有场景都需要 HTTP。有的场景只用 gRPC 直连。Gate 是一种"皮肤"，不是核心能力。

---

## 3. Rune Wire Protocol (完整 proto)

```protobuf
syntax = "proto3";
package rune.wire.v1;

// ─────────────────────────────────
// 核心服务：Caster 与 Runtime 的双向流
// ─────────────────────────────────

service RuneService {
    rpc Session(stream SessionMessage) returns (stream SessionMessage);
}

// ─────────────────────────────────
// 顶层消息：用 oneof 区分消息类型
// ─────────────────────────────────

message SessionMessage {
    oneof payload {
        // Caster → Runtime
        CasterAttach attach = 10;
        CasterDetach detach = 11;
        ExecuteResult result = 12;
        StreamEvent stream_event = 13;
        StreamEnd stream_end = 14;

        // Runtime → Caster
        AttachAck attach_ack = 20;
        ExecuteRequest execute = 21;
        CancelRequest cancel = 22;

        // 双向
        Heartbeat heartbeat = 30;
    }
}

// 注意：request_id 只在需要关联请求/响应的消息里，不在顶层。
// Heartbeat、CasterAttach、CasterDetach 不需要 request_id。

// ─────────────────────────────────
// Caster 注册
// ─────────────────────────────────

message CasterAttach {
    string caster_id = 1;
    repeated RuneDeclaration runes = 2;
    map<string, string> labels = 3;     // 调度标签 (gpu=true, region=us 等)
    uint32 max_concurrent = 4;          // 并发容量声明，0 = 无限制
}

message AttachAck {
    bool accepted = 1;
    string reason = 2;                  // 拒绝原因
}

message CasterDetach {
    string reason = 1;                  // 优雅断开
}

// ─────────────────────────────────
// Rune 声明（注册时一次性传递）
// ─────────────────────────────────

message RuneDeclaration {
    string name = 1;
    string version = 2;
    string description = 3;
    bytes input_schema = 4;             // JSON Schema, bytes 不做 UTF-8 校验
    bytes output_schema = 5;
    bool supports_stream = 6;
    GateConfig gate = 7;                // HTTP 暴露配置（可选）
}

message GateConfig {
    string path = 1;                    // 自定义路径，如 /api/v1/translate
    string method = 2;                  // HTTP 方法，默认 POST
}

// ─────────────────────────────────
// 执行请求 / 结果
// ─────────────────────────────────

message ExecuteRequest {
    string request_id = 1;              // 关联响应
    string rune_name = 2;
    bytes input = 3;                    // bytes! 不是 string
    map<string, string> context = 4;    // trace_id, tenant_id 等透传上下文
    ExecutionOptions options = 5;
}

message ExecutionOptions {
    uint32 timeout_ms = 1;
    RetryPolicy retry = 2;
}

message RetryPolicy {
    uint32 max_retries = 1;
    uint32 initial_backoff_ms = 2;
    float backoff_multiplier = 3;       // 退避倍数，默认 2.0
}

message ExecuteResult {
    string request_id = 1;              // 关联请求
    Status status = 2;
    bytes output = 3;                   // bytes!
    ErrorDetail error = 4;
    map<string, string> metadata = 5;   // 执行耗时等元信息
}

// ─────────────────────────────────
// 流式
// ─────────────────────────────────

message StreamEvent {
    string request_id = 1;             // 关联请求
    bytes data = 2;
    string event_type = 3;             // 用户自定义事件类型（string 是对的）
}

message StreamEnd {
    string request_id = 1;             // 关联请求
    Status status = 2;
    ErrorDetail error = 3;
}

// ─────────────────────────────────
// 取消
// ─────────────────────────────────

message CancelRequest {
    string request_id = 1;             // 要取消的请求
    string reason = 2;
}

// ─────────────────────────────────
// 心跳
// ─────────────────────────────────

message Heartbeat {
    uint64 timestamp_ms = 1;
}

// ─────────────────────────────────
// 公共类型
// ─────────────────────────────────

enum Status {
    STATUS_UNSPECIFIED = 0;
    STATUS_COMPLETED = 1;
    STATUS_FAILED = 2;
    STATUS_CANCELLED = 3;
}

message ErrorDetail {
    string code = 1;                    // 机器可读错误码
    string message = 2;                 // 人类可读描述
    bytes details = 3;                  // 结构化错误详情（可选）
}
```

**设计要点：**

| 决策 | 原因 |
|------|------|
| `oneof` 替代 God Message | 每种消息类型有自己的字段，编译期类型安全 |
| `bytes` 替代 `string`（数据字段） | 热路径省 UTF-8 校验，不绑定 JSON，将来可以传 MessagePack/CBOR |
| `request_id` 只在需要关联的消息里 | ExecuteRequest/Result/StreamEvent/StreamEnd/CancelRequest 有，Heartbeat/Attach/Detach 没有——无冗余字段 |
| `Heartbeat` | 检测 Caster 存活，不靠 TCP keepalive（默认 2h 太久） |
| `CasterDetach` | 区分优雅断开和崩溃 |
| `CancelRequest` | 支持取消正在执行的 Rune |
| `ExecutionOptions` 在请求级 | 同一个 Caster 不同 Rune 有不同的超时和重试策略 |
| `labels` 在 CasterAttach | 调度标签，支持未来按 GPU/region 等路由 |
| `max_concurrent` 在 CasterAttach | Caster 声明并发容量，Runtime 做背压控制 |

---

## 4. Rust 项目结构

```
rune/
├── Cargo.toml                    # workspace
├── proto/
│   └── rune/wire/v1/
│       └── rune.proto            # Layer 0: 协议定义
│
├── rune-proto/                   # 生成的 proto 代码
│   ├── Cargo.toml
│   ├── build.rs                  # tonic-build
│   └── src/lib.rs
│
├── rune-core/                    # Layer 1: Runtime
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── rune.rs               # Rune 定义 + RuneHandler trait
│       ├── invoker.rs            # RuneInvoker trait + Local/Remote 实现
│       ├── relay.rs              # Relay 注册表（谁在哪）
│       ├── resolver.rs           # Resolver 调度器（选谁执行）
│       ├── session.rs            # SessionManager + Caster 连接状态机
│       ├── codec.rs              # PayloadCodec trait + JsonCodec
│       ├── kit.rs                # Kit (Module) trait（极简）
│       └── app.rs                # App 生命周期
│
├── rune-gate/                    # Layer 2: HTTP Gateway
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── gate.rs               # axum 路由生成
│       ├── sync_handler.rs       # sync 模式
│       ├── stream_handler.rs     # SSE 模式
│       ├── async_handler.rs      # async 模式
│       ├── validate.rs           # JSON Schema 校验
│       └── openapi.rs            # OpenAPI 生成
│
├── rune-flow/                  # Layer 3: Flow DAG 引擎
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── dag.rs                # DAG 定义 + 拓扑排序
│       ├── engine.rs             # 执行引擎
│       ├── task.rs               # Task 状态机
│       └── dsl.rs                # DSL builder
│
├── rune-server/                  # 独立二进制
│   ├── Cargo.toml
│   └── src/main.rs               # workitem-server → rune-server
│
├── rune-sdk-rust/                # Rust Caster SDK
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── caster.rs             # Caster 客户端
│       └── handler.rs            # handler 宏 / trait
│
└── examples/
    ├── hello/                    # 最小示例（进程内 Rune）
    ├── echo-caster/              # Rust Caster 示例
    └── python-caster/            # Python Caster 示例
```

**Crate 依赖关系：**

```
rune-proto       ← tonic-build 生成
    ↑
rune-core        ← 依赖 rune-proto, tokio, tonic
    ↑
rune-gate        ← 依赖 rune-core, axum, jsonschema
rune-flow      ← 依赖 rune-core
rune-server      ← 依赖 rune-core, rune-gate, rune-flow (全部 optional)
rune-sdk-rust    ← 依赖 rune-proto, tonic
```

---

## 5. 核心 Trait 定义

### 5.1 Rune（函数接口）

```rust
// ─────────────── rune-core/src/rune.rs ───────────────

use bytes::Bytes;

/// Rune 的执行上下文
pub struct RuneContext {
    pub rune_name: String,
    pub request_id: String,
    pub context: HashMap<String, String>,   // trace_id, tenant_id 等
}

/// 一次性 Rune handler
#[async_trait]
pub trait RuneHandler: Send + Sync + 'static {
    async fn execute(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError>;
}

/// 流式 Rune handler
#[async_trait]
pub trait StreamRuneHandler: Send + Sync + 'static {
    async fn execute(
        &self,
        ctx: RuneContext,
        input: Bytes,
        tx: StreamSender,
    ) -> Result<(), RuneError>;
}

/// 流式发送器（背压控制）
pub struct StreamSender { /* mpsc::Sender<StreamEvent> */ }

impl StreamSender {
    pub async fn emit(&self, data: Bytes, event_type: &str) -> Result<(), RuneError>;
    pub async fn end(self) -> Result<(), RuneError>;
}

/// Rune 注册配置
pub struct RuneConfig {
    pub name: String,
    pub version: String,
    pub description: String,
    pub input_schema: Option<Bytes>,
    pub output_schema: Option<Bytes>,
    pub supports_stream: bool,
    pub gate: Option<GateConfig>,       // HTTP 暴露（可选）
}

pub struct GateConfig {
    pub path: String,
    pub method: String,                 // 默认 "POST"
}

/// 错误类型
pub enum RuneError {
    InvalidInput(String),
    ExecutionFailed { code: String, message: String },
    Timeout,
    Cancelled,
    Internal(anyhow::Error),
}
```

### 5.2 Invoker（统一本地/远程调用）

```rust
// ─────────────── rune-core/src/invoker.rs ───────────────

/// 统一调用接口。上层（Gate / Flow）不关心 Rune 在哪。
#[async_trait]
pub trait RuneInvoker: Send + Sync {
    async fn invoke(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError>;
}

/// 进程内调用——直接调 handler
pub struct LocalInvoker {
    handler: Arc<dyn RuneHandler>,
}

#[async_trait]
impl RuneInvoker for LocalInvoker {
    async fn invoke(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        self.handler.execute(ctx, input).await
    }
}

/// 远程调用——通过 Session 发 gRPC
pub struct RemoteInvoker {
    session: Arc<SessionManager>,
    caster_id: String,
}

#[async_trait]
impl RuneInvoker for RemoteInvoker {
    async fn invoke(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError> {
        let request_id = ctx.request_id.clone();
        self.session.execute(&self.caster_id, request_id, ctx, input).await
    }
}
```

**价值：** Gate 和 Flow 只需调 `invoker.invoke(ctx, input)`，不 match Local/Remote。后续加重试、超时、metrics 都是在 Invoker 上包装。

**流式 Invoker（POC v3 时加）：** 当前 invoke() 返回 `Result<Bytes>`，只支持一次性结果。POC v3 流式阶段需要扩展：

```rust
/// POC v3 时加入——流式 Invoker
pub enum InvokeResult {
    Once(Bytes),
    Stream(tokio::sync::mpsc::Receiver<Result<Bytes, RuneError>>),
}

// 或者单独的 StreamInvoker trait，POC v1/v2 不需要
```

### 5.3 Relay（注册表）+ Resolver（调度器）

```rust
// ─────────────── rune-core/src/relay.rs ───────────────

/// Relay = 纯注册表。只管"有哪些 Rune、在哪些实例上"。
pub struct Relay {
    entries: DashMap<String, Vec<RuneEntry>>,
}

pub struct RuneEntry {
    pub config: RuneConfig,
    pub invoker: Arc<dyn RuneInvoker>,
}

impl Relay {
    /// 注册（进程内或远程都通过 Invoker 统一）
    pub fn register(&self, config: RuneConfig, invoker: Arc<dyn RuneInvoker>);

    /// 移除 Caster 的所有 Rune
    pub fn remove_caster(&self, caster_id: &str);

    /// 查找某个 Rune 的所有候选实例
    pub fn find(&self, rune_name: &str) -> Vec<&RuneEntry>;

    /// 列出所有已注册 Rune
    pub fn list(&self) -> Vec<(String, RuneConfig)>;
}
```

```rust
// ─────────────── rune-core/src/resolver.rs ───────────────

/// Resolver = 调度器。从候选里选一个。
/// 与 Relay 分离——Relay 管数据，Resolver 管策略。
pub trait Resolver: Send + Sync {
    fn pick<'a>(&self, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry>;
}

/// 默认：轮询
pub struct RoundRobinResolver {
    counters: DashMap<String, AtomicUsize>,
}

impl Resolver for RoundRobinResolver {
    fn pick<'a>(&self, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry> {
        if candidates.is_empty() { return None; }
        let idx = self.counters
            .entry(candidates[0].config.name.clone())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
        Some(&candidates[idx % candidates.len()])
    }
}

// 未来可扩展：WeightedResolver, LabelResolver, ConsistentHashResolver...
// 注意：pick() 是 sync by design。如果未来需要查外部服务（etcd 读权重），
// 改为 async fn pick()。当前同步足够且避免不必要的 async 开销。
```

### 5.4 SessionManager（Caster 连接管理 + 状态机）

```rust
// ─────────────── rune-core/src/session.rs ───────────────

/// 管理所有 Caster 的 gRPC 双向流连接
pub struct SessionManager {
    sessions: DashMap<String, CasterSession>,
}

pub struct CasterSession {
    caster_id: String,
    state: SessionState,
    tx: mpsc::Sender<SessionMessage>,       // 发给 Caster
    pending: DashMap<String, PendingRequest>, // request_id → 等待中的请求
    max_concurrent: u32,
    active_count: AtomicU32,
}

enum SessionState {
    Attaching,          // 收到 CasterAttach，等待确认
    Active,             // 正常工作中
    Draining,           // 优雅断开中，不接新请求，等待进行中的完成
    Disconnected,       // 已断开
}

struct PendingRequest {
    created_at: Instant,
    timeout: Duration,
    response_tx: oneshot::Sender<Result<Bytes, RuneError>>,
}
```

**Session 状态机：**

```
              CasterAttach
    ┌────────────────────────────┐
    ▼                            │
Attaching ──AttachAck──→ Active ──Heartbeat──→ Active
                           │                     │
                    CasterDetach           Heartbeat timeout
                           │                     │
                           ▼                     ▼
                       Draining ──────→ Disconnected
                      (等待 pending      (清理所有 pending
                       请求完成)          → RuneError::Internal)
```

**Request 状态机：**

```
Created ──dispatch──→ Dispatched ──result──→ Completed
                         │
                    ┌────┼────────┐
                    │    │        │
                 cancel timeout  disconnect
                    │    │        │
                    ▼    ▼        ▼
              Cancelled TimedOut  Failed
                                (orphaned)
```

**关键问题的处理方式：**

| 场景 | 处理 |
|------|------|
| Caster 断线，pending 请求怎么办？ | 全部标记 Failed，response_tx 发 `RuneError::Internal("caster disconnected")` |
| 已取消的 request_id 收到迟到的 result | 忽略（PendingRequest 已被移除，无人接收） |
| Caster 超过 max_concurrent | Resolver 不选该 Caster（通过 active_count 判断） |
| Heartbeat 超时阈值 | 可配置，默认 30s。超过 3 次未收到 → 视为断线 |
| Draining 超时 | Draining 状态有 drain_timeout（默认 30s），超时后强制 Disconnected，剩余 pending 全部 Failed |

### 5.5 Kit（模块，极简 + 依赖排序）

```rust
// ─────────────── rune-core/src/kit.rs ───────────────

/// Kit = Module。只有 init、shutdown 和依赖声明。
#[async_trait]
pub trait Kit: Send + Sync + 'static {
    fn name(&self) -> &str;

    /// 声明依赖的其他 Kit 名称。App::run() 自动拓扑排序。
    fn depends_on(&self) -> &[&str] { &[] }

    async fn init(&self, app: &mut App) -> Result<(), anyhow::Error>;
    async fn shutdown(&self) -> Result<(), anyhow::Error>;
}
```

Kit 在 `init()` 中通过 `app` 显式注册能力：

```rust
// 示例：auth Kit 的 init
async fn init(&self, app: &mut App) -> Result<()> {
    // 注册请求拦截器（core 层，不涉及 HTTP）
    app.add_interceptor(Arc::new(self.auth_interceptor()));

    // 注册 Rune 变更回调
    app.on_rune_registered(|name, config| { /* 自动配 auth 规则 */ });

    Ok(())
}

// auth Kit 依赖 database Kit
fn depends_on(&self) -> &[&str] { &["database"] }
```

**依赖排序：** App::run() 在调用 init 前，根据 `depends_on()` 对所有 Kit 做拓扑排序。循环依赖直接 panic。用户不需要手动保证 `app.kit()` 的顺序。

**路由注册不在 core 里。** Kit 如果需要 HTTP 路由，通过 Gate Kit 提供的扩展方法注册（见 5.8），axum 不出现在 rune-core 的任何类型中。

### 5.6 PayloadCodec（编解码抽象）

```rust
// ─────────────── rune-gate/src/codec.rs ───────────────
// 注意：Codec 在 rune-gate 里，不在 rune-core 里。
// rune-core 只传 Bytes，不关心编解码格式。

/// 编解码器。Bytes ↔ Bytes 转换，不用泛型，保持 object-safe。
pub trait PayloadCodec: Send + Sync {
    fn content_type(&self) -> &str;
    fn encode(&self, data: &[u8]) -> Result<Bytes, CodecError>;
    fn decode(&self, data: &[u8]) -> Result<Bytes, CodecError>;
}

/// 默认实现：JSON（实际上是 identity——输入已经是 JSON bytes）
/// 真正的序列化/反序列化在 SDK 和 Gate handler 中做，Codec 负责的是
/// "收到的 bytes 是不是合法的该格式"的校验 + 必要的格式转换。
pub struct JsonCodec;

impl PayloadCodec for JsonCodec {
    fn content_type(&self) -> &str { "application/json" }
    fn encode(&self, data: &[u8]) -> Result<Bytes, CodecError> {
        // 校验是合法 JSON
        serde_json::from_slice::<serde_json::Value>(data)?;
        Ok(Bytes::copy_from_slice(data))
    }
    fn decode(&self, data: &[u8]) -> Result<Bytes, CodecError> {
        serde_json::from_slice::<serde_json::Value>(data)?;
        Ok(Bytes::copy_from_slice(data))
    }
}
```

**关键：`PayloadCodec` 是 object-safe（无泛型方法），可以 `Arc<dyn PayloadCodec>`。** 泛型的序列化/反序列化由调用方（Gate handler / SDK）自己用 serde 做，Codec 只管 bytes 级别的校验和格式转换。

### 5.7 错误映射（Standard Error Codes）

```rust
// ─────────────── rune-gate/src/errors.rs ───────────────

/// 标准错误码 → HTTP Status 映射。
/// Caster 返回 ErrorDetail.code 时，Gate 按此表映射。
/// 未知 code → 500 + 原始 ErrorDetail 透传。

const ERROR_MAP: &[(&str, StatusCode)] = &[
    ("INVALID_INPUT",     StatusCode::BAD_REQUEST),          // 400
    ("NOT_FOUND",         StatusCode::NOT_FOUND),            // 404
    ("UNAUTHORIZED",      StatusCode::UNAUTHORIZED),         // 401
    ("FORBIDDEN",         StatusCode::FORBIDDEN),            // 403
    ("TIMEOUT",           StatusCode::GATEWAY_TIMEOUT),      // 504
    ("RATE_LIMITED",      StatusCode::TOO_MANY_REQUESTS),    // 429
    ("UNAVAILABLE",       StatusCode::SERVICE_UNAVAILABLE),  // 503
    ("CANCELLED",         StatusCode::from_u16(499).unwrap()), // 499 (Client Closed)
    ("INTERNAL",          StatusCode::INTERNAL_SERVER_ERROR), // 500
];

/// Gate 层的错误响应格式（JSON）
/// {
///   "error": {
///     "code": "MODEL_OVERLOADED",
///     "message": "Backend is overloaded, try again later",
///     "details": { ... }   // 可选
///   }
/// }
```

**规则：**
- 标准 code（上表）→ 精确映射
- 自定义 code（如 `MODEL_OVERLOADED`）→ 500 + body 携带完整 ErrorDetail
- Rune 不存在 → 404
- Schema 校验失败 → 422 (Unprocessable Entity)
- Caster 全部不可用 → 503

### 5.8 App（core 层，不碰 axum）

```rust
// ─────────────── rune-core/src/app.rs ───────────────

/// App = 框架入口。rune-core 层，不依赖 axum。
pub struct App {
    relay: Arc<Relay>,
    resolver: Arc<dyn Resolver>,
    session_mgr: Arc<SessionManager>,
    kits: Vec<Box<dyn Kit>>,
    interceptors: Vec<Arc<dyn Interceptor>>,
    rune_callbacks: Vec<Box<dyn Fn(&str, &RuneConfig) + Send + Sync>>,
    config: Config,
}

impl App {
    pub fn new() -> Self;
    pub fn with_config(path: &str) -> Self;

    // ── Kit 注册 ──
    pub fn kit(&mut self, k: impl Kit + 'static) -> &mut Self;

    // ── Rune 注册 ──
    pub fn rune(&mut self, config: RuneConfig, handler: impl RuneHandler) -> &mut Self;
    pub fn stream_rune(&mut self, config: RuneConfig, handler: impl StreamRuneHandler) -> &mut Self;

    // ── Kit 在 init() 中调用的注册方法 ──
    pub fn add_interceptor(&mut self, i: Arc<dyn Interceptor>);
    pub fn on_rune_registered(&mut self, f: impl Fn(&str, &RuneConfig) + Send + Sync + 'static);
    pub fn set_resolver(&mut self, r: impl Resolver + 'static);

    // ── 启动 ──
    pub async fn run(&mut self) -> Result<(), anyhow::Error>;
    // run() 内部：
    //   1. 拓扑排序 kits（根据 depends_on）
    //   2. 按排序后顺序 kit.init()
    //   3. 启动 SessionManager（gRPC Server）
    //   4. 等待 SIGINT/SIGTERM
    //   5. 逆序 kit.shutdown()
    //   6. 关闭 SessionManager（drain pending requests, drain_timeout 可配）
    //   7. 关闭 gRPC Server
}
```

**Gate 作为独立 Kit：**

```rust
// ─────────────── rune-gate/src/gate_kit.rs ───────────────

/// Gate 本身是一个 Kit，在 init 时启动 HTTP Server。
/// axum 只出现在 rune-gate crate 内，rune-core 完全不知道 axum 的存在。
pub struct GateKit {
    addr: SocketAddr,
    codec: Arc<dyn PayloadCodec>,
    extra_routes: Vec<axum::Router>,  // 其他 Kit 注册的路由
}

impl Kit for GateKit {
    fn name(&self) -> &str { "gate" }

    async fn init(&self, app: &mut App) -> Result<()> {
        // 从 Relay 读取所有声明了 GateConfig 的 Rune
        // 为每个自动生成 axum handler
        // 合并 extra_routes
        // 启动 HTTP Server（后台 tokio task）
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        // 优雅关闭 HTTP Server
        Ok(())
    }
}

/// 其他 Kit 通过 GateKit 的扩展方法注册路由（不经过 App）
impl GateKit {
    pub fn add_routes(&mut self, router: axum::Router);
}
```

**axum 完全封装在 rune-gate 内。rune-core 的 App struct 里没有任何 axum 类型。**

### 5.9 测试策略

| 层 | 测试方式 | 覆盖 |
|----|---------|------|
| **Relay** | 单元测试 | register / find / remove_caster / list |
| **Resolver** | 单元测试 | RoundRobin 分配、空候选、单候选 |
| **Invoker** | 单元测试 + MockHandler | LocalInvoker 正确调 handler、错误传播 |
| **SessionManager** | 集成测试 | Attach → Execute → Result 完整流程 |
| **Gate** | 集成测试 | HTTP → Invoker → 200 / 400 / 404 / 503 |
| **端到端** | Python Caster e2e | Caster attach → curl → 远程执行 → 返回 |

**POC v1 至少包含：** Relay + Resolver 的单元测试、Gate 的集成测试、Python Caster 端到端测试。CI 用 `cargo test` + pytest。

---

## 6. 请求流转

### 6.1 sync（进程内和远程统一路径）

```
HTTP POST /translate
  → axum handler (rune-gate)
  → Codec.decode(body)
  → Relay.find("translate") → 候选列表
  → Resolver.pick(candidates) → 选中一个 RuneEntry
  → entry.invoker.invoke(ctx, input)
      如果 LocalInvoker  → 直接调 handler.execute()
      如果 RemoteInvoker → session.execute() → gRPC → Caster → 等结果
  → Codec.encode(output)
  → HTTP 200 JSON
```

**上层代码只看到 `invoker.invoke()`，不知道也不关心 Rune 在哪执行。**

### 6.3 stream — SSE

```
HTTP POST /llm/chat?stream=true
  → axum handler (rune-gate)
  → 创建 SSE response stream
  → Relay.resolve("llm_chat")
  → 如果 Local: handler.execute(ctx, input, StreamSender)
    如果 Remote: session.send(ExecuteRequest), 然后监听 StreamEvent
  → 每收到一个 StreamEvent → 写入 SSE: data: {...}
  → 收到 StreamEnd → 关闭 SSE
```

### 6.4 async

```
HTTP POST /heavy-task?async=true
  → axum handler (rune-gate)
  → 生成 task_id
  → spawn 后台 tokio task:
      → Relay.resolve("heavy-task")
      → 执行（同 sync 路径）
      → 结果存到 task store
  → 立即返回 202 { task_id }

HTTP GET /tasks/{task_id}
  → 查询 task store
  → 返回状态 + 结果（如果完成）
```

### 6.5 Flow (Workflow)

```
HTTP POST /flows/pipeline/run
  → rune-flow engine
  → 解析 DAG
  → 按拓扑序执行各节点：
      节点 "parse":  Relay.resolve → Local handler
      节点 "embed":  Relay.resolve → Remote Caster (Python)
      节点 "store":  Relay.resolve → Remote Caster (Go)
  → 全部完成 → 返回最终输出
```

---

## 7. 技术选型

| 组件 | 选择 | 原因 |
|------|------|------|
| async runtime | **tokio** | tonic 依赖它，生态最大 |
| gRPC | **tonic** | Rust 最成熟的 gRPC，基于 tokio |
| HTTP | **axum** | 基于 tokio + tower，和 tonic 共享 runtime |
| proto codegen | **tonic-build** | 配合 tonic |
| JSON Schema 校验 | **jsonschema** crate | 支持 Draft 7+，纯 Rust |
| 配置 | **toml** crate | 简单、Rust 生态标准 |
| 日志 | **tracing** | tokio 生态标准，支持结构化日志 + span |
| 并发 map | **dashmap** | Relay 需要并发读写 |
| 错误处理 | **anyhow** + **thiserror** | anyhow 用于应用层，thiserror 用于库 |
| 序列化 | **serde** + **serde_json** | 仅在 Gate 层（HTTP 边界），核心层不碰 JSON |

**关键约束：rune-core 不依赖 serde_json。** JSON 序列化/反序列化只发生在 rune-gate（HTTP 边界）。核心层传的是 `Bytes`，不关心内容格式。

---

## 8. Caster SDK 设计

### 8.1 Rust SDK (rune-sdk-rust)

```rust
use rune_sdk::prelude::*;

#[tokio::main]
async fn main() {
    let mut caster = Caster::connect("http://localhost:9090").await.unwrap();

    caster.register("translate", RuneConfig {
        description: "翻译文本".into(),
        input_schema: Some(include_bytes!("schemas/translate_input.json").into()),
        output_schema: Some(include_bytes!("schemas/translate_output.json").into()),
        ..Default::default()
    }, |ctx, input: Bytes| async move {
        let req: TranslateRequest = serde_json::from_slice(&input)?;
        let result = deepl::translate(&req.text, &req.lang).await?;
        Ok(serde_json::to_vec(&TranslateResponse { result })?.into())
    }).await;

    caster.run().await.unwrap();  // 阻塞，接收任务
}
```

### 8.2 Python SDK

**纯 Python + grpcio，不用 PyO3 绑定 Rust。** SDK 要让 Python 开发者觉得原生。

```python
from rune import Caster, RuneConfig

caster = Caster("localhost:9090")

@caster.rune("translate", config=RuneConfig(
    description="翻译文本",
    input_schema="schemas/translate_input.json",
    output_schema="schemas/translate_output.json",
    gate={"path": "/translate"},
))
def translate(ctx, input: dict) -> dict:
    result = deepl.translate(input["text"], input["lang"])
    return {"result": result}

# 流式 Rune
@caster.stream_rune("llm_chat", config=RuneConfig(
    description="LLM 对话",
    supports_stream=True,
    gate={"path": "/llm/chat"},
))
async def llm_chat(ctx, input: dict, stream):
    async for chunk in call_llm(input["messages"]):
        await stream.emit({"content": chunk})
    await stream.end()

caster.run()
```

### 8.3 TypeScript SDK

```typescript
import { Caster } from '@rune/sdk';

const caster = new Caster('localhost:9090');

caster.rune('translate', {
  description: '翻译文本',
  gate: { path: '/translate' },
}, async (ctx, input) => {
  const result = await deepl.translate(input.text, input.lang);
  return { result };
});

await caster.run();
```

### 8.4 SDK 设计原则

- **各语言原生实现**，不用 FFI 绑定。Python 用 grpcio，TypeScript 用 @grpc/grpc-js，Rust 用 tonic。
- **SDK 处理序列化**。核心协议传 `bytes`，SDK 负责 JSON ↔ bytes 的转换。
- **SDK 处理重连**。Caster 断线后自动重连 + 重新注册。
- **SDK 处理心跳**。后台 goroutine/task 定期发 Heartbeat。

---

## 9. POC 路线（三期）

### POC v1：串行 echo（验证协议和桥接）

**约束：单 Caster、单 Rune、串行执行、无并发。**

| 组件 | 范围 |
|------|------|
| `proto/` | 完整的 rune.proto |
| `rune-proto/` | tonic-build 生成 |
| `rune-core/` | Relay + Resolver(轮询) + Invoker(Local+Remote) + SessionManager(串行) |
| `rune-gate/` | sync 模式（一个 axum handler）|
| `rune-server/` | main.rs 启动 gRPC + HTTP |
| `examples/hello/` | 进程内 Rune 示例 |
| `examples/python-caster/` | Python Caster echo |

**验证场景：**

```
场景 A（进程内）:
  curl POST :8080/api/v1/runes/hello/run -d '{"name":"world"}'
  → Gate → Relay → LocalInvoker → handler → 返回

场景 B（远程 Caster）:
  启动 Python Caster，注册 "echo"
  curl POST :8080/api/v1/runes/echo/run -d '{"text":"hello"}'
  → Gate → Relay → RemoteInvoker → gRPC → Python → 返回
```

**验证了什么：** 协议可用、注册可用、Invoker 透明性（进程内和远程同一路径）、多语言互通。

**不做：** 并发、取消、重试、流式、async、flow、schema、auth、metrics。

**估算：~500 行 Rust + ~50 行 Python。**

### POC v2：并发 + 状态机（验证生产可行性）

在 v1 基础上加：

| 能力 | 说明 |
|------|------|
| 并发复用 | SessionManager 的 pending map，多个 request_id 同时执行 |
| Heartbeat | 双向心跳，超时检测 |
| 断线清理 | Caster 断线 → pending 全部 Failed → Relay 移除 |
| Timeout | 请求级超时（ExecutionOptions.timeout_ms） |
| Cancel | CancelRequest 发送 + 迟到结果忽略 |
| max_concurrent | 背压控制（active_count >= max → Resolver 跳过该 Caster） |

### POC v3：流式 + 异步 + 基础 Flow

| 能力 | 说明 |
|------|------|
| stream | SSE 桥接，StreamEvent/StreamEnd 映射 |
| async | 后台 Task + task store + 轮询 |
| Flow | 最小 DAG（Chain 顺序执行） |

---

## 10. 文件清单（POC v1）

```
rune/
├── Cargo.toml                              # workspace
├── proto/rune/wire/v1/rune.proto           # ~80 行
├── rune-proto/
│   ├── Cargo.toml
│   ├── build.rs                            # ~10 行
│   └── src/lib.rs                          # ~5 行
├── rune-core/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                          # ~20 行
│       ├── rune.rs                         # ~60 行 (types + traits)
│       ├── invoker.rs                      # ~80 行 (Local + Remote)
│       ├── relay.rs                        # ~60 行 (注册表)
│       ├── resolver.rs                     # ~30 行 (轮询)
│       └── session.rs                      # ~120 行 (串行，无并发)
├── rune-gate/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                          # ~10 行
│       └── gate.rs                         # ~80 行 (sync only)
├── rune-server/
│   ├── Cargo.toml
│   └── src/main.rs                         # ~60 行
└── examples/
    ├── hello/main.rs                       # ~20 行 (进程内 Rune)
    └── python-caster/
        ├── requirements.txt
        ├── gen.sh
        └── caster.py                       # ~50 行
```

**总计：~500 行 Rust + ~50 行 Python + ~80 行 proto。**
