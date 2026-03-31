# Rune v0.1.0 Scope

## Context

Rune 已完成 4 轮 POC（v1-v4），验证了协议桥接、本地/远程统一调用、并发会话、SSE、异步任务和最小链式 Flow。多轮 review 暴露出一个共同结论：

- Rune 的方向是对的。
- Rune 还不能以当前 POC 形态直接作为正式框架发布。
- 真正阻碍发布的不是功能不够多，而是 correctness 和承诺闭环还不够强。

v0.1.0 的目标不是做成“功能丰富版”，而是做成第一个 **正确、可交付、可验证** 的正式版本。

一句话定义 v0.1.0：

> Rune v0.1.0 是一个协议驱动的多语言函数执行框架的首个正式版本，提供 Rust 参考 runtime、官方 Python Caster SDK，以及正确的 sync / real stream / async / chain flow 语义。

## Product Positioning

### Official Positioning

Rune 是一个 **协议优先的多语言框架**。核心产出有 3 个：

1. Rune Wire Protocol（RWP）：唯一正式协议源。
2. Rust Runtime：官方参考服务端实现。
3. 多语言 Caster SDK：各语言原生实现，先从 Python 开始。

### Runtime First

v0.1.0 采用 `Runtime First` 路线，而不是“完整 API contract 平台”路线。

v0.1.0 正式宣传的能力只有：

- 定义 Rune 并自动暴露真实 HTTP 路由
- 本地执行与远程 Caster 执行统一
- sync / real stream / async 三种调用模式
- 最小顺序 Flow
- 官方 Python SDK

v0.1.0 不再宣传：

- Schema 校验
- OpenAPI 生成
- 完整 DAG 工作流
- durable execution
- 多语言 SDK 同步成熟

README、官网和示例文案必须同步修订，只承诺上述真实能力。

### Success Criteria

以下用户故事在 v0.1.0 中必须真实成立：

1. 用户 `pip install rune-sdk`。
2. 用户注册一个带 `gate="/translate"` 的 Python Rune。
3. `POST /translate` 正常返回。
4. `POST /translate?stream=true` 走真实流式，不是“先执行完再切片”。
5. `POST /translate?async=true` 返回 task id，后续可以查询结果。
6. 本地 Rune、远程 Rune、Flow step 的错误与超时语义一致。

## Repository Structure

v0.1.0 开始，正式实现迁移到仓库根目录，不再以 `demo/` 目录承载正式开发。

```text
rune/
├── Cargo.toml
├── Cargo.lock
│
├── proto/
│   └── rune/wire/v1/
│       └── rune.proto
│
├── crates/
│   ├── rune-proto/
│   ├── rune-core/
│   ├── rune-gate/
│   ├── rune-flow/
│   └── rune-server/
│
├── sdks/
│   ├── python/          # v0.1.0 stable
│   ├── typescript/      # placeholder only
│   ├── go/              # placeholder only
│   └── rust/            # placeholder only
│
├── examples/
│   ├── hello/
│   ├── python-caster/
│   └── mixed-flow/
│
├── tests/
│
├── docs/
│   ├── dev-design.md
│   ├── scope/
│   └── ...
│
└── demo/
    ├── poc-v1/
    ├── poc-v2/
    ├── poc-v3/
    └── poc-v4/
```

### Structure Decisions

- 使用 monorepo，因为 proto 是跨语言契约，必须和 runtime / SDK 同步演进。
- `crates/` 和 `sdks/` 并列，明确 Rune 不是“纯 Rust 项目”。
- `proto/` 在根目录，避免协议从属于某一个实现。
- `demo/` 保留为只读历史存档，不再继续叠代开发。
- `sdks/typescript`、`sdks/go`、`sdks/rust` 仅创建占位 README，不进入 v0.1.0 交付承诺。

### Release Promise vs Repo Layout

目录存在不等于版本承诺。v0.1.0 的正式支持范围只有：

- Stable: Rust runtime, Python SDK
- Not in release promise: Go SDK, TypeScript SDK, Rust SDK

## Scope

### 1. Must-Fix Correctness Issues

v0.1.0 在新增功能前，必须先修完以下 correctness 问题：

1. `permit` 双重归还
   超时或取消后收到迟到 `Result` / `StreamEnd` 时，不得重复归还容量。
2. 假流式
   `stream=true` 必须桥接真实执行流，不允许对完整结果做 HTTP 层切片。
3. `gate_path` 悬空
   声明了 `gate.path` 的 Rune，必须暴露真实业务路由。
4. 心跳不强制驱逐
   heartbeat 超时必须触发 session 驱逐、relay 清理和 pending fail。
5. cancel 没有正式入口
   async task 必须有框架级 cancel 入口。
6. session / gate 缺少测试基线
   不能再只靠 relay/flow 单测支撑发布。

### 2. Protocol Scope

v0.1.0 固定使用 `rune.wire.v1`，并把它作为唯一正式支持的 wire contract。

#### Officially Supported Messages

- `CasterAttach`
- `AttachAck`
- `CasterDetach`
- `ExecuteRequest`
- `ExecuteResult`
- `StreamEvent`
- `StreamEnd`
- `CancelRequest`
- `Heartbeat`

#### Officially Supported Declaration Fields

- `RuneDeclaration.name`
- `RuneDeclaration.version`
- `RuneDeclaration.description`
- `RuneDeclaration.supports_stream`
- `RuneDeclaration.gate.path`
- `RuneDeclaration.gate.method`
- `CasterAttach.max_concurrent`
- `CasterAttach.labels` 仅作为透明元数据保留，不进入 v0.1.0 调度语义
- `ExecuteRequest.context`
- `ExecuteRequest.timeout_ms`

#### Out of Scope for v0.1.0

`input_schema` 和 `output_schema` 不属于 v0.1.0 正式能力。为了避免“协议写了但产品不支持”的再次发生，v0.1.0 发布前应执行以下二选一策略，并以“删除”为默认：

- 默认方案：从稳定 proto 中移除 schema 字段，后续版本再新增。
- 保守方案：若暂时保留字段编号，则文档中必须明确标注 `reserved / unsupported in v0.1.0`，SDK 和 runtime 不暴露该能力。

v0.1.0 默认采用 **删除 unsupported schema 字段** 的方案。

### 3. Runtime Core Scope

v0.1.0 的 runtime 要收敛成一个统一、稳定、可测试的执行内核。

#### Public Types

- `RuneConfig`
  包含 `name`、`version`、`description`、`supports_stream`、`gate`
- `GateConfig`
  包含 `path`、`method`
- `RuneContext`
  包含 `rune_name`、`request_id`、`context`、`timeout`
- `RuneHandler`
  unary handler
- `StreamRuneHandler`
  streaming handler

#### Core Runtime Interfaces

- `RuneInvoker`
  必须正式支持 `invoke_once()` 和 `invoke_stream()`
- `Relay`
  只负责注册表
- `Resolver`
  只负责候选挑选
- `SessionManager`
  负责 attach、pending request、stream request、cancel、timeout、heartbeat、disconnect 和 permit 管理
- `App`
  负责 runtime 生命周期、组件装配、gRPC/HTTP 启停

#### Required Runtime Behaviors

- local Rune 和 remote Rune 必须走统一调用抽象
- local stream 和 remote stream 必须语义一致
- `max_concurrent` 必须严格生效
- timeout / cancel / disconnect / late result 必须状态收敛
- flow step 必须复用同一套 invoker 语义

#### Session State Machine

v0.1.0 采用最小显式状态机：

- `Attaching`
- `Active`
- `Disconnected`

`Draining` 和 `drain_timeout` 不进入 v0.1.0。

#### Explicit Non-Goals

- Retry policy
- Weighted / label-based resolver
- Multi-runtime cluster coordination
- Durable execution
- Plugin / kit system
- Auth / metrics / persistence

### 4. Gate / HTTP Scope

v0.1.0 的 Gate 只做“真实路由 + 正确语义”，不做契约平台。

#### Required HTTP Capabilities

- 按 `gate.path + gate.method` 暴露真实业务路由
- 支持 sync
- 支持 real stream SSE
- 支持 async task
- 支持 async task cancel
- 支持 flow list / flow run
- 支持 health

#### Official Routes

- `GET /health`
- `GET /api/v1/runes`
- `GET /api/v1/tasks/:id`
- `DELETE /api/v1/tasks/:id`
- `GET /api/v1/flows`
- `POST /api/v1/flows/:name/run`
- 业务 Rune 路由：由每个 Rune 的 `gate.path + method` 自动生成
- `POST /api/v1/runes/:name/run`
  允许保留为调试入口，但不是主业务路由

#### Required Gate Behaviors

- 声明了 `gate.path=/echo` 的 Rune，必须可以通过 `/echo` 调用
- `stream=true` 必须走 `invoke_stream()`，直接桥接到 SSE
- `supports_stream=false` 的 Rune 请求 stream 时必须返回固定错误
- `async=true` 必须返回 task id，并能查询结果
- sync / stream / async 必须共享同一套错误映射

#### Flow Integration Rule

Gate 不直接依赖 `FlowEngine` 具体实现，但必须提供通用 route registration 能力，允许 Flow 模块注册：

- `GET /api/v1/flows`
- `POST /api/v1/flows/:name/run`

换句话说，Gate 负责 HTTP 容器，Flow 负责自己的业务路由接入。

#### Explicit Non-Goals

- Schema validation
- OpenAPI generation
- Auth middleware
- Rate limiting
- Content negotiation complexity

### 5. Async / Task Scope

v0.1.0 的 async 是最小正式版，不是 job platform。

#### Required Capabilities

- async Rune execution
- async Flow execution
- in-memory task store
- task result query
- task cancel

#### Task States

- `pending`
- `running`
- `completed`
- `failed`
- `cancelled`

#### Required Semantics

- `DELETE /api/v1/tasks/:id` 只适用于 async task
- sync 请求不提供独立 cancel API
- stream 请求的 cancel 来自客户端断开连接，runtime 需映射为取消
- cancel 至少要保证框架内语义成立：
  请求方收到 cancelled、pending 清理、permit 回收、迟到结果不污染状态

#### Explicit Non-Goals

- Task persistence
- Restart recovery
- Pagination / search
- Multi-tenant isolation

### 6. Flow Scope

Flow 纳入 v0.1.0，但只到“最小顺序 Chain”。

#### Required Capabilities

- Flow registration
- Flow listing
- Flow sync execution
- Flow async execution
- local + remote mixed chain
- step-level error reporting

#### Required Behaviors

- 只支持 `Chain`
- Flow step 统一复用 Rune registry 和 invoker
- Flow input/output 以 bytes 串联
- step request_id 必须可追踪并从 flow request 派生

#### Explicit Non-Goals

- Parallel DAG
- Branch / loop / subflow
- YAML / JSON flow loading
- Flow persistence
- Flow streaming
- Compensation / saga

### 7. Python SDK Scope

Python SDK 纳入 v0.1.0，但定位是 **最小正式 SDK**，不是高阶 DX 版本。

#### Package Shape

```text
sdks/python/
├── pyproject.toml
├── src/rune_sdk/
│   ├── __init__.py
│   ├── caster.py
│   ├── handler.py
│   ├── stream.py
│   ├── types.py
│   └── _proto/
└── tests/
```

#### Required Capabilities

- connect / attach / re-attach
- unary rune registration
- stream rune registration
- heartbeat
- once execution
- stream execution
- cancel awareness
- reconnect with backoff
- minimum official examples

#### SDK API Expectations

- `Caster(...)`
- `@caster.rune(...)`
- `@caster.stream_rune(...)`
- `await stream.emit(...)`
- `await stream.end()`
- `caster.run()`

#### Explicit Non-Goals

- Typed schema system
- Codegen-based models
- Fancy decorator ergonomics
- Plugin system
- Advanced config layering

## Guarantees

v0.1.0 必须对外保证以下效果：

1. 本地 Rune 和远程 Rune 在调用语义上统一。
2. sync / stream / async 是同一 Rune 的三种正式模式，而不是三套不同实现。
3. stream 首事件来自真实执行过程。
4. `gate.path` 是真实业务路由，不是 metadata。
5. timeout / cancel / disconnect 后，runtime 状态必须收敛。
6. late result / late stream end 不得污染容量和任务状态。
7. Python Caster 可以参与完整正式链路。
8. mixed local/remote flow 可以成功运行。

## Acceptance Test Matrix

### 1. Session / Protocol Tests

必须覆盖：

- attach 成功注册
- metadata 正确保留
- heartbeat 续命
- heartbeat 超时驱逐
- detach 清理
- disconnect 清理 pending once
- disconnect 清理 pending stream
- timeout 结束 once
- timeout 结束 stream
- cancel 结束 once
- cancel 结束 stream
- late result 不重复归还 permit
- late stream end 不重复归还 permit
- `max_concurrent=1` 时容量严格生效

### 2. Gate / HTTP Tests

必须覆盖：

- `gate.path` 自动暴露真实路由
- 业务路由与调试路由结果一致
- `supports_stream=false` 拒绝 stream 请求
- 真实 stream 首个 chunk 在 handler 完成前发出
- async Rune 返回 task id
- task query 返回正确状态和结果
- task cancel 生效
- unknown rune 返回稳定错误

### 3. Flow Tests

必须覆盖：

- local chain success
- remote chain success
- mixed chain success
- step not found
- step failed
- empty flow
- async flow success

### 4. Python SDK E2E Tests

必须覆盖：

- Python unary Rune attach + invoke
- Python stream Rune attach + invoke
- Python slow Rune async
- Python task cancel
- Python caster reconnect
- Python steps in mixed flow

### 5. Regression Tests

必须单独锁死历史问题：

- fake stream regression
- `gate_path` 悬空 regression
- double permit release regression
- heartbeat timeout not evicting regression

## Delivery Order

### Phase 0: Repository Migration

1. 创建根 workspace 和 `crates/ / sdks/ / proto/ / examples/ / tests/`
2. 从 `demo/poc-v4` 复制现有实现到正式目录
3. 修正 path dependencies
4. 为 `sdks/typescript`、`sdks/go`、`sdks/rust` 建立占位 README

### Phase 1: Correctness Ground Truth

1. 修复 permit 双归还
2. 修复 heartbeat 超时驱逐
3. 建立 session / protocol 单元测试

### Phase 2: Runtime Core Closure

1. 引入 `Resolver`
2. 扩展 `RuneConfig` / `RuneContext`
3. 引入 `StreamRuneHandler`
4. 让 `RuneInvoker` 正式支持 once/stream
5. 引入 `App` 与最小生命周期管理

### Phase 3: Gate Closure

1. 落地 `gate.path`
2. 落地 real stream SSE
3. 加入 task cancel API
4. 完成 gate integration tests

### Phase 4: Python SDK

1. 建立 pip-installable 包结构
2. 重构为正式 SDK
3. 完成 pytest + e2e

### Phase 5: Examples and Docs

1. 更新 examples
2. 修订 README
3. 标记 `demo/` 为只读存档

## Non-Goals

以下能力明确不属于 v0.1.0：

- Schema validation
- OpenAPI generation
- Retry policy
- Advanced resolver strategies
- Auth / metrics / persistence
- Durable tasks
- DAG parallelism
- Flow file loading
- Go / TS / Rust SDK 正式交付

## Default Assumptions

- v0.1.0 采用 Runtime First 路线
- Python SDK 纳入 v0.1.0
- schema / openapi 不纳入 v0.1.0
- monorepo 根目录结构从 v0.1.0 开始生效
- stable release promise 仅覆盖 Rust runtime 与 Python SDK
