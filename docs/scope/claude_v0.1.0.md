# Rune 0.1.0 Scope — Claude Draft

> 基于 POC v1-v4 全部源码、dev-design.md、两轮 Codex review、Linus review，综合产出。

---

## 1. Summary

Rune 0.1.0 的定位：

**一个协议优先的多语言函数执行框架。**

本仓库包含 Rune Wire Protocol、Rust 参考 runtime、官方 Python SDK、官方示例与一致性测试。

0.1.0 不是"完整平台版"，不是"自动契约/文档全家桶版"。
目标是把最核心的 **统一执行模型** 做对、做稳、做可验证：

- 定义 Rune，获得真实 HTTP 暴露
- 同一个 Rune 可本地执行，也可远程由 Caster 执行
- 支持 sync / real stream / async 三种调用模式
- 支持最小顺序 Flow
- 支持官方 Python SDK
- 保证协议、runtime、gate、flow 四层语义一致

**第一原则：correctness > capability breadth。**

先把协议状态机、调用语义、流式语义、取消/超时/断线语义做对，再扩能力面。

---

## 2. Product Positioning

0.1.0 对外的正式产品表述收敛为：

> 定义函数，获得统一执行、自动 HTTP 暴露、真实流式、异步任务、最小顺序 Flow。

**0.1.0 不再宣传以下能力：**

- 自动 Schema 校验
- 自动 OpenAPI 文档
- 完整 API contract 平台
- Durable execution
- 完整 DAG workflow 引擎
- 全语言官方 SDK 同步成熟

README、介绍材料、示例文案都必须和上述定位保持一致。

---

## 3. Monorepo Structure

0.1.0 迁移到仓库根目录，不再以 `demo/` 作为正式实现承载位置。

```
rune/
├── Cargo.toml                 # Workspace root
├── Cargo.lock
│
├── proto/                     # 唯一协议源（所有语言共享）
│   └── rune/wire/v1/
│       └── rune.proto
│
├── runtime/                   # Rust 参考 runtime
│   ├── rune-proto/            #   proto 代码生成（tonic-build）
│   ├── rune-core/             #   Layer 1: Runtime 核心
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── rune.rs        #     类型定义 + RuneHandler + StreamRuneHandler
│   │       ├── invoker.rs     #     RuneInvoker trait（unary + stream）
│   │       ├── relay.rs       #     Relay 纯注册表
│   │       ├── resolver.rs    #     Resolver trait + RoundRobin
│   │       ├── session.rs     #     SessionManager 重构
│   │       ├── app.rs         #     App 生命周期
│   │       └── config.rs      #     AppConfig
│   ├── rune-gate/             #   Layer 2: HTTP Gateway
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── gate.rs        #     路由构建 + gate_path 动态路由
│   │       ├── stream.rs      #     SSE 真流式处理
│   │       └── error.rs       #     错误码映射
│   ├── rune-flow/             #   Layer 3: Flow 引擎
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── dsl.rs
│   │       └── engine.rs
│   └── rune-server/           #   独立二进制
│       └── src/main.rs        #     使用 App API
│
├── sdks/                      # 官方 SDK
│   ├── python/                #   v0.1.0 正式交付
│   │   ├── pyproject.toml
│   │   ├── src/rune_sdk/
│   │   │   ├── __init__.py
│   │   │   ├── caster.py      #     Caster class, grpc.aio, 重连
│   │   │   ├── handler.py     #     handler 注册, 装饰器
│   │   │   ├── stream.py      #     StreamSender (emit/end)
│   │   │   ├── types.py       #     RuneContext, RuneConfig, type hints
│   │   │   └── _proto/        #     生成的 pb2 文件
│   │   └── tests/
│   └── rust/                  #   占位，不承诺稳定
│
├── examples/                  # 官方最小示例
│   ├── local-hello/           #   进程内 Rune（Rust）
│   ├── python-caster/         #   使用 rune-sdk 的 Python Caster
│   └── mixed-flow/            #   本地 + 远程混合 Flow
│
├── tests/                     # 跨模块集成测试 / 协议一致性测试 / e2e
│
├── docs/                      # scope、协议保证、架构文档
│   ├── dev-design.md
│   ├── scope/
│   └── protocol-guarantees.md #   v0.1.0 交付物
│
└── demo/                      # POC 历史存档（只读）
    ├── poc-v1/
    ├── poc-v2/
    ├── poc-v3/
    └── poc-v4/
```

### 仓库定位

- `proto/` 是唯一 wire contract 源
- `runtime/` 是 Rust 参考实现
- `sdks/` 是官方 SDK 承载区
- 未来 Python/Go/Rust/TS SDK 都放在本仓库
- 0.1.0 只正式承诺 Python SDK

### SDK 成熟度

| SDK | 0.1.0 状态 |
|-----|-----------|
| Python | Stable |
| Rust runtime | Reference |
| Go / TS / Rust SDK | Not promised |

### Workspace Cargo.toml

```toml
[workspace]
members = [
    "runtime/rune-proto",
    "runtime/rune-core",
    "runtime/rune-gate",
    "runtime/rune-flow",
    "runtime/rune-server",
]
resolver = "2"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
tonic = "0.12"
prost = "0.13"
axum = "0.7"
bytes = "1"
dashmap = "6"
thiserror = "2"
anyhow = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
async-trait = "0.1"
```

### Proto 代码生成策略

- v0.1.0：Rust 用 `tonic-build`（build.rs），Python 用 `grpcio-tools`
- v0.2.0+：引入 `buf.yaml` 统一多语言代码生成

---

## 4. Scope Detail

### 4.1 协议层

0.1.0 固定使用 `rune.wire.v1`，作为唯一正式支持的 wire protocol。

**正式保留并落地的协议消息：**

- CasterAttach / AttachAck / CasterDetach
- ExecuteRequest / ExecuteResult
- StreamEvent / StreamEnd
- CancelRequest
- Heartbeat

**正式要求 runtime 真正使用的声明字段：**

| 字段 | 所属消息 | runtime 行为 |
|------|---------|-------------|
| `name` | RuneDeclaration | 注册到 Relay，用于路由 |
| `version` | RuneDeclaration | 存储到 RuneConfig，不做版本路由 |
| `description` | RuneDeclaration | 存储到 RuneConfig |
| `supports_stream` | RuneDeclaration | Gate 据此允许/拒绝 `?stream=true` |
| `gate.path` | RuneDeclaration | Gate 据此生成业务 HTTP 路由 |
| `gate.method` | RuneDeclaration | Gate 据此决定 HTTP 方法 |
| `max_concurrent` | CasterAttach | Semaphore 初始化容量 |
| `context` | ExecuteRequest | 透传到 RuneContext.context |
| `timeout_ms` | ExecuteRequest | 作为 per-request timeout |

**处理原则：**

- proto 中保留的字段，runtime 必须真实保留和解释
- **不允许再出现"proto 有、实现忽略、README 当成已支持"的情况**
- `labels` 在 proto 中保留，0.1.0 只做附带元数据，不进入调度语义
- `input_schema` / `output_schema` 从 0.1.0 scope 中移除。proto 中保留但必须在文档中明确标注：`reserved for future contract support, unused in 0.1.0`

### 4.2 Runtime 核心

**正式接口：**

```rust
/// Rune 的执行上下文
pub struct RuneContext {
    pub rune_name: String,
    pub request_id: String,
    pub context: HashMap<String, String>,  // 透传 trace_id, tenant_id 等
}

/// Rune 注册配置
pub struct RuneConfig {
    pub name: String,
    pub version: String,
    pub description: String,
    pub supports_stream: bool,
    pub gate: Option<GateConfig>,
}

pub struct GateConfig {
    pub path: String,
    pub method: String,  // 默认 "POST"
}

/// 一次性 Rune handler
pub type RuneHandler = Arc<
    dyn Fn(RuneContext, Bytes) -> Pin<Box<dyn Future<Output = Result<Bytes, RuneError>> + Send>>
        + Send + Sync,
>;

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

/// StreamSender — handler 用它向下游推送流式事件
pub struct StreamSender { /* mpsc::Sender<StreamEvent> */ }
impl StreamSender {
    pub async fn emit(&self, data: Bytes) -> Result<(), RuneError>;
    pub async fn end(self) -> Result<(), RuneError>;
}

/// 统一调用接口 — 上层不关心 Rune 在哪执行
#[async_trait]
pub trait RuneInvoker: Send + Sync {
    async fn invoke(&self, ctx: RuneContext, input: Bytes)
        -> Result<Bytes, RuneError>;

    async fn invoke_stream(&self, ctx: RuneContext, input: Bytes)
        -> Result<mpsc::Receiver<Result<Bytes, RuneError>>, RuneError>;
}

/// Relay — 纯注册表
pub struct Relay { entries: DashMap<String, Vec<RuneEntry>> }
impl Relay {
    pub fn register(&self, config: RuneConfig, invoker: Arc<dyn RuneInvoker>, caster_id: Option<String>);
    pub fn remove_caster(&self, caster_id: &str);
    pub fn find(&self, name: &str) -> Option<Vec<&RuneEntry>>;
    pub fn list(&self) -> Vec<(String, &RuneConfig)>;
}

/// Resolver — 调度策略，与 Relay 分离
pub trait Resolver: Send + Sync {
    fn pick<'a>(&self, candidates: &'a [RuneEntry]) -> Option<&'a RuneEntry>;
}
pub struct RoundRobinResolver { /* AtomicUsize counters */ }

/// SessionManager — session 状态机、pending、取消、超时、断线、permit
pub struct SessionManager { sessions: DashMap<String, CasterState> }

/// App — 最小生命周期管理
pub struct App { /* relay, resolver, session_mgr, config */ }
impl App {
    pub fn new() -> Self;
    pub fn rune(&mut self, config: RuneConfig, handler: RuneHandler) -> &mut Self;
    pub fn stream_rune(&mut self, config: RuneConfig, handler: impl StreamRuneHandler) -> &mut Self;
    pub fn set_resolver(&mut self, r: impl Resolver + 'static) -> &mut Self;
    pub async fn run(&mut self) -> Result<(), anyhow::Error>;
    // run(): 启动 gRPC + HTTP Gate + 信号处理 + graceful shutdown
}
```

**关键实现约束：**

| 约束 | 说明 |
|------|------|
| `RuneInvoker` 必须明确区分 unary 和 stream | `invoke()` + `invoke_stream()` 两个方法，无默认实现 |
| `LocalInvoker` 对 stream | 如果 handler 是 `StreamRuneHandler`，走真流式；否则 invoke 后单次发送 |
| `RemoteInvoker` 对 stream | 调用 `SessionManager::execute_stream()`，桥接 gRPC StreamEvent |
| Permit 归还 | 必须与 `pending.remove()` 成功绑定，不允许无条件归还 |
| Per-request timeout | 从 `ExecuteRequest.timeout_ms` 读取，不再硬编码 30s |
| 心跳超时 | 触发完整断线清理，不仅仅 warn+break |

**0.1.0 runtime 明确不做：**

- 重试策略（RetryPolicy）
- 高级权重/标签调度
- 多 runtime 节点协调
- Durable execution / 持久任务恢复
- Kit 系统完整版 / Interceptor trait

### 4.3 Gate / HTTP

**正式路由：**

| 路由 | 方法 | 说明 |
|------|------|------|
| `/health` | GET | 健康检查 |
| `/api/v1/runes` | GET | 列出所有注册 Rune |
| `/api/v1/runes/:name/run` | POST | 通用调试入口（不是主产品路由） |
| `/api/v1/tasks/:id` | GET | 查询异步任务状态 |
| `/api/v1/tasks/:id` | DELETE | 取消异步任务 |
| `/api/v1/flows` | GET | 列出所有 Flow |
| `/api/v1/flows/:name/run` | POST | 执行 Flow |
| 业务 Rune 路由 | 按 gate.method | **按每个 Rune 的 `gate.path` + `gate.method` 自动生成** |

**语义要求：**

- 声明了 `gate.path=/translate` 的 Rune，必须真能通过 `POST /translate` 调用
- `stream=true` 必须走真实流式执行链路，**不能"先执行完再切片"**
- `async=true` 必须生成 task 并支持查询结果
- 未声明 `gate.path` 的 Rune 不自动对外暴露业务 HTTP 路由
- `supports_stream=false` 的 Rune 请求 stream 时必须返回明确错误
- 错误码与 HTTP 状态映射必须统一固定

**0.1.0 Gate 不做：**

OpenAPI、Schema 校验、auth middleware、rate limiting、content negotiation。

### 4.4 Async / Task

0.1.0 的 async 是最小正式版，不是 durable job system。

**正式能力：**

- sync 请求可切换为 async 执行
- 返回 task_id
- 查询 task 状态与结果
- task 状态：`pending` / `running` / `completed` / `failed` / `cancelled`
- task 可承载 Rune 调用结果和 Flow 调用结果
- 支持框架级 cancel 语义

**实现边界：**

- task store 为单进程内存实现（DashMap）
- 不要求重启恢复、持久化、分页、搜索、租户隔离
- 可有最小清理策略，但不做复杂生命周期管理

### 4.5 Flow

Flow 纳入 0.1.0，scope 只到**最小顺序 Chain**。

**正式能力：**

- Flow 注册 / 列表 / sync 执行 / async 执行
- step 复用 Rune 注册表与 invoker
- 支持 local + remote 混合 step
- 错误能定位到 step index 和 rune name
- 输入输出为 bytes 级串联
- Flow request 和 step request 必须可追踪

**不做：** 并行、分支、循环、子流程、flow streaming、compensation/saga。

### 4.6 Python SDK

定位为**最小正式 SDK**，不是高级体验 SDK。

**必须支持：**

- connect / attach / re-attach（指数退避重连）
- `@caster.rune()` 装饰器注册
- `@caster.stream_rune()` 流式注册 + StreamSender
- heartbeat
- once 执行 / stream 执行
- cancel 感知
- 基础错误映射
- grpc.aio（asyncio 原生，不用 threading）
- type hints
- pip-installable

**不做：**

自动 schema 声明、代码生成、类型安全 I/O 模型、插件系统、复杂配置。

**原则：** 先保证协议一致性、运行正确性、示例可运行、测试可验证，不追求 DX 华丽。

---

## 5. Problems 0.1.0 Must Solve

### 5.1 Product Problems

- 避免用户手写 HTTP 路由、远程 dispatch、worker 协议桥接、简易 flow 胶水
- 让"函数"成为 API、远程执行单元、flow 节点的统一原子单位
- 让多语言执行成为协议扩展问题，而不是框架重写问题

### 5.2 Engineering Problems — POC 遗留 Bug

以下 bug 经 Codex review 发现，我逐行验证确认存在于 v4：

#### Bug 1: Permit 双重归还（严重）

**位置：** `demo/poc-v4/rune-core/src/session.rs:141-145`（Result 路径），`:170-174`（StreamEnd 路径）

**机制：** `add_permits(1)` 在第 144 行**无条件执行**，不管第 146 行 `pending.remove()` 是否成功。当超时清理已归还 permit（第 91-93 行）后，迟到的 Result 又无条件归还一次。两次归还使 semaphore 容量膨胀，`max_concurrent` 失效。

**修复：** `add_permits(1)` 必须移入 `pending.remove()` 返回 `Some(_)` 的分支内。迟到的 Result/StreamEnd 在 pending 中找不到 entry 时静默丢弃。

#### Bug 2: 假流式（严重）

**位置：** `demo/poc-v4/rune-gate/src/gate.rs:121-151`

**机制：** Gate 的 `stream=true` 分支调用 `invoker.invoke()` 拿到完整结果后，按 20 字符切片发 SSE。`RuneInvoker` trait 只有 `invoke()` 方法。`SessionManager::execute_stream()` 存在但未通过 `RuneInvoker` 暴露。gate.rs 第 121 行注释甚至写着 `// POC 用 invoker 同步执行后模拟 chunk`。

**修复：**
1. `RuneInvoker` trait 增加 `invoke_stream()` 方法
2. `RemoteInvoker` 实现 `invoke_stream()` 调用 `session.execute_stream()`
3. `LocalInvoker` 对 `StreamRuneHandler` 走真流式
4. Gate SSE 路径调 `invoke_stream()`，桥接 `mpsc::Receiver` 到 SSE event stream

#### Bug 3: gate_path 未映射（严重）

**位置：** `demo/poc-v4/rune-gate/src/gate.rs:41-49` 硬编码 6 条路由

**机制：** `RuneConfig.gate_path` 被存储到 Relay 并在 `list_runes` 中展示，但从未用于路由生成。`POST /echo` 返回 404。

**修复：** Gate 通过 fallback handler + Relay 查表实现动态 gate_path 路由。

#### Bug 4: 心跳不强制驱逐

**位置：** `demo/poc-v4/rune-core/src/session.rs:64-76`

**机制：** 心跳超时只 `warn + break` 后台 task，依赖连接关闭级联间接触发清理。

**修复：** 心跳超时后主动 drop outbound channel，强制触发消息循环退出和完整断线清理。

#### Bug 5: cancel() 无上游入口

`cancel()` 方法存在（session.rs:277），但仓库中没有任何代码调用它。

#### Bug 6: 协议字段大面积悬空

`version`、`supports_stream`、`context`、`timeout_ms`、`gate.path`、`gate.method` 在 proto 中声明但 runtime 忽略。

#### Bug 7: Session/Gate 零测试

只有 relay（4 个）和 flow engine（5 个）有测试。Session 并发语义、Gate HTTP 行为完全没有自动化约束。

#### Bug 8: Python SDK 是脚本

无 asyncio、无 type hints、无重连、`cancelled_requests` 内存泄漏。

### 5.3 架构问题

- Relay 混合了注册表和调度逻辑，需要分离为 Relay + Resolver
- `RuneInvoker` 没有 stream 方法，是**架构层断裂**
- SessionManager 301 行单体函数，职责过多
- README 承诺与实现行为不一致

---

## 6. Guarantees 0.1.0 Must Provide

### 6.1 Behavioral Guarantees

| 保证 | 说明 |
|------|------|
| 调用统一 | Local Rune 与 Remote Rune 在调用语义上统一 |
| 模式一致 | 同一 Rune 的 sync / stream / async 共享同一注册信息与错误语义 |
| 真流式 | stream 首个事件来自真实执行过程，不是完成后切片 |
| 状态收敛 | cancel / timeout / disconnect 后，状态必须收敛到确定终态 |
| 无污染 | late result / late stream end 不得污染 runtime 状态 |
| 背压严格 | max_concurrent 严格生效，不因异常路径失真 |
| Flow 复用 | Flow step 调用与普通 Rune 调用走同一内核逻辑 |

### 6.2 Product Guarantees

| 保证 | 说明 |
|------|------|
| 真实路由 | 业务 Rune 路由真实存在，不靠管理路由伪装 |
| Python 可用 | Python Caster 可参与正式运行链路 |
| 混合 Flow | mixed local/remote flow 可工作 |
| 示例+测试 | 所有对外承诺能力都有可运行示例与测试覆盖 |

---

## 7. Acceptance Tests

### 7.1 协议 / Session / Backpressure

- attach 成功注册
- attach 后 metadata 可查询
- heartbeat 正常续命
- heartbeat 超时触发驱逐
- detach 正常清理
- disconnect 清理 pending once
- disconnect 清理 pending stream
- timeout 结束 once 请求
- timeout 结束 stream 请求
- cancel 结束 once 请求
- cancel 结束 stream 请求
- late result 被忽略且不重复归还 permit
- late stream end 被忽略且不重复归还 permit
- max_concurrent=1 时容量严格生效
- timeout/cancel/disconnect 后 permit 恢复正确

### 7.2 Gate / HTTP

- gate.path 自动暴露真实业务路由
- 业务路由与 `/api/v1/runes/:name/run` 行为一致
- supports_stream=false 的 Rune 被拒绝 stream 请求
- real stream Rune 首事件在 handler 完成前发出
- async Rune 返回 task_id
- task 查询返回正确状态与结果
- unknown rune 返回固定错误
- 未声明 gate.path 的 Rune 不暴露业务路由

### 7.3 Flow

- 本地 chain 成功
- 远程 chain 成功
- 本地 + 远程混合 chain 成功
- step not found
- step failed
- empty flow
- async flow 成功
- flow task 查询成功

### 7.4 Python SDK E2E

- Python unary rune attach + invoke
- Python stream rune attach + invoke
- Python slow rune async
- Python rune cancel
- Python caster disconnect + reconnect
- Python step 参与 mixed flow

### 7.5 Regression Cases

专门锁死 POC 阶段暴露过的问题：

| 回归用例 | 锁定的 bug |
|---------|-----------|
| fake stream regression | stream=true 不能先执行完再切片 |
| gate_path display-only regression | gate_path 必须生成真实 HTTP 路由 |
| double permit release regression | 迟到 Result 不能膨胀 semaphore |
| heartbeat timeout not evicting regression | 心跳超时必须触发完整清理 |
| proto metadata dropped by runtime regression | 声明的字段 runtime 必须使用 |

---

## 8. Build Sequence

### Phase 0: 项目结构迁移

1. 在根目录创建 `runtime/` + `sdks/` + `proto/` + `examples/` + `tests/`
2. 从 `demo/poc-v4/` 复制各 crate 到 `runtime/` 下
3. 创建根 `Cargo.toml`（workspace），修正所有 path 依赖
4. 将 `demo/poc-v4/proto/` 复制到根 `proto/`
5. `cargo build && cargo test` 通过

**Gate:** 仓库根目录 `cargo build` 成功，现有 relay + flow 测试通过。

### Phase 1: 修正确性（Ground Truth）

6. 修 permit 双重归还（`add_permits` 移入 `pending.remove` 的 Some 分支）
7. 心跳超时强制驱逐（主动 drop outbound channel）
8. 编写 session.rs 单元测试（覆盖 permit 正常/超时/迟到/断线/cancel 5 个场景）

**Gate:** session 测试全绿，迟到 Result 不膨胀 semaphore。

### Phase 2: 建正确抽象

9. Relay / Resolver 分离
10. RuneConfig / RuneContext 扩展（version, supports_stream, context, timeout）
11. RuneInvoker 增加 `invoke_stream()`（unary 和 stream 明确区分）
12. StreamRuneHandler trait + StreamSender
13. App struct + 生命周期 + 信号处理

**Gate:** rune-server main.rs 从 ~145 行缩减到 ~30 行。

### Phase 3: 真流式 + 路由

14. Gate SSE 真流式（桥接 `invoke_stream()` → SSE event stream）
15. gate_path 动态路由（fallback handler + Relay 查表）
16. Gate 与 rune-flow 解耦
17. 错误码标准映射
18. `DELETE /api/v1/tasks/:id` 取消 API
19. supports_stream 检查
20. Gate 集成测试

**Gate:** Python Caster StreamEvent → Gate SSE 逐条转发；`curl POST /echo` 通过 gate_path 返回 200。

### Phase 4: Python SDK

21. `sdks/python/` 包结构（pyproject.toml, src/rune_sdk/）
22. Proto 代码生成（grpcio-tools）
23. Caster class + `@caster.rune()` + asyncio
24. 指数退避重连
25. `@caster.stream_rune()` + StreamSender
26. pytest

**Gate:** `pip install -e sdks/python && python examples/python-caster/main.py` 启动成功。

### Phase 5: 收尾 + 交付物

27. Flow 改用 Resolver trait
28. examples 更新（local-hello, python-caster, mixed-flow）
29. tests/ 跨模块集成测试 + 回归用例
30. `docs/protocol-guarantees.md`
31. README 更新（反映 0.1.0 真实能力）
32. demo/ 标记为存档

**Gate:** `git clone && cargo build && cargo test` 全绿。README 中每个 curl 命令都能跑通。

---

## 9. Not In Scope (v0.2.0+)

| 功能 | 理由 |
|------|------|
| Schema 校验 / OpenAPI | 先建好字段传递链路 |
| Kit trait + Interceptor | 没有第二个用户 |
| PayloadCodec | 只有 JSON |
| TOML 配置文件 | 代码配置即可 |
| Go / TS / Rust SDK | 先稳定 Python |
| Flow DAG 并行 / 分支 / 循环 | Chain 已验证 |
| rune-persist / durable execution | 内存 task store 即可 |
| rune-auth / rune-metrics | 无认证无指标 |
| Draining + drain_timeout | 优雅关闭高级行为 |
| RetryPolicy | 自动重试 |
| labels 调度 | Resolver 只有 RoundRobin |
| 反向索引（caster → runes） | O(n) 可接受 |
| 多租户 / 权限 / rate limiting | |

---

## 10. Delivery Artifacts

0.1.0 发布前必须同时具备：

- [ ] `docs/scope/v0.1.0.md` — 正式 scope 文档
- [ ] `docs/protocol-guarantees.md` — 协议行为保证
- [ ] README — 精简且真实，不含未实现能力
- [ ] `examples/local-hello/` — 本地 Rune 官方示例
- [ ] `examples/python-caster/` — Python Caster 官方示例
- [ ] `examples/mixed-flow/` — 混合 Flow 官方示例
- [ ] CI 单元测试 + 集成测试 + e2e 测试 + 回归测试

---

## 11. Code Estimation

| 模块 | POC v4 | v0.1.0 预估 |
|------|--------|------------|
| rune-core | 532 行 | ~1400 行（session 重构 + app + resolver + StreamRuneHandler + 测试） |
| rune-gate | 258 行 | ~600 行（stream.rs + error.rs + gate_path + 测试） |
| rune-flow | 308 行 | ~350 行（小幅改进） |
| rune-server | 145 行 | ~40 行（使用 App） |
| rune-proto | ~15 行 | ~15 行 |
| **Rust 总计** | **~1260** | **~2400** |
| Python SDK | 189 行 | ~500 行 |
| Proto | 120 行 | ~125 行 |
| tests/ | 0 行 | ~300 行（集成 + e2e + 回归） |
| **总计** | **~1570** | **~3325** |

---

## 12. Key Source Files

| 文件 | 改动类型 | 说明 |
|------|---------|------|
| `runtime/rune-core/src/session.rs` | 重构 | permit 修复、心跳驱逐、拆分职责、per-request timeout |
| `runtime/rune-core/src/invoker.rs` | 扩展 | invoke_stream()、StreamRuneHandler 集成 |
| `runtime/rune-core/src/rune.rs` | 扩展 | RuneConfig 扩展、StreamRuneHandler、StreamSender |
| `runtime/rune-core/src/relay.rs` | 重构 | 纯注册表，移除调度逻辑 |
| `runtime/rune-core/src/resolver.rs` | 新建 | Resolver trait + RoundRobin |
| `runtime/rune-core/src/app.rs` | 新建 | App 生命周期 |
| `runtime/rune-core/src/config.rs` | 新建 | AppConfig |
| `runtime/rune-gate/src/gate.rs` | 重构 | gate_path 路由、与 flow 解耦 |
| `runtime/rune-gate/src/stream.rs` | 新建 | SSE 真流式 |
| `runtime/rune-gate/src/error.rs` | 新建 | 错误码映射 |
| `sdks/python/src/rune_sdk/` | 新建 | 完整 Python SDK |
