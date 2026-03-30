# Rune

**协议优先的多语言函数执行框架。定义函数，获得路由、流式、异步、编排 -- 全部开箱即用。**

## 特性

- **三种调用模式** -- sync / stream (SSE) / async，同一函数自动获得三种调用方式
- **DAG 工作流编排** -- 带条件分支、并行执行、input mapping 的有向无环图引擎
- **三语言 SDK** -- Python / TypeScript / Rust Caster SDK，统一 Wire Protocol
- **Schema 校验 + OpenAPI 生成** -- JSON Schema 输入输出校验，自动生成 OpenAPI 3.0 文档
- **文件传输** -- multipart 上传 + FileBroker 内存中转 + 下载端点
- **API Key 认证** -- Gate Key (HTTP) + Caster Key (gRPC)，两层隔离
- **Rate Limiting** -- 基于滑动窗口的请求频率限制
- **高级调度** -- round-robin / random / least-load / priority / label-based 五种策略
- **CLI 工具** -- 单二进制管理 Runtime、调用 Rune、管理 Key 和 Flow
- **SQLite 持久化** -- 异步任务、调用日志、API Key 全部持久化

## 快速开始

### Docker（推荐）

```bash
# 拉取镜像
docker pull ghcr.io/chasey-myagi/rune-server:latest

# 启动 Runtime
docker run -d -p 50060:50060 -p 50070:50070 ghcr.io/chasey-myagi/rune-server:latest

# 验证
curl http://localhost:50060/health  # => ok
```

或通过 docker-compose（适合下游项目集成）：

```yaml
services:
  rune:
    image: ghcr.io/chasey-myagi/rune-server:latest
    ports:
      - "50060:50060"  # HTTP API
      - "50070:50070"  # gRPC (Caster 连接)
    environment:
      RUNE_LOG_LEVEL: info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:50060/health"]
      interval: 10s
```

**环境变量：**

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RUNE_HOST` | `0.0.0.0` | 监听地址 |
| `RUNE_HTTP_PORT` | `50060` | HTTP API 端口 |
| `RUNE_GRPC_PORT` | `50070` | gRPC 端口 |
| `RUNE_LOG_LEVEL` | `info` | 日志级别 |

### 从源码启动

```bash
# 开发模式（跳过认证，绑定 127.0.0.1）
rune start --dev

# 生产模式（使用 rune.toml 配置）
rune start --config rune.toml
```

### Python Caster 示例

```bash
pip install rune-sdk
```

```python
from rune_sdk import Caster

caster = Caster("localhost:50070")

@caster.rune("translate", gate="/translate", input_schema={
    "type": "object",
    "properties": {"text": {"type": "string"}, "lang": {"type": "string"}},
    "required": ["text", "lang"]
})
async def translate(ctx, input):
    data = json.loads(input)
    return json.dumps({"translated": do_translate(data["text"], data["lang"])})

@caster.stream_rune("generate", gate="/generate")
async def generate(ctx, input, sender):
    for token in model.stream(input):
        await sender.emit(token)

caster.run()
```

### TypeScript Caster 示例

```typescript
import { Caster } from '@rune-sdk/caster';

const caster = new Caster({ key: 'rk_xxx' });

caster.rune({ name: 'greet', gate: { path: '/greet' } }, async (ctx, input) => {
  return { message: `Hello, ${input.name}!` };
});

caster.streamRune({ name: 'count', gate: { path: '/count' } }, async (ctx, input, stream) => {
  for (let i = 0; i < 10; i++) {
    stream.emit(Buffer.from(String(i)));
  }
});

await caster.run();
```

### CLI 调用

```bash
# 列出在线 Rune
rune list

# 同步调用
rune call translate '{"text": "hello", "lang": "zh"}'

# 流式调用
rune call generate '{"prompt": "write a poem"}' --stream

# 异步调用
rune call translate '{"text": "hello", "lang": "zh"}' --async
# 查询异步任务
rune task <task-id>
```

## 架构

```
Client ---> HTTP API ---> Gate ---> Relay ---> Invoker ---> Caster
                           |                                  |
                      Auth + Rate Limit              gRPC (Wire Protocol)
                      Schema Validation
                      File Broker                 Python / TypeScript / Rust
```

```
Runtime (Rust)
├── rune-core    -- 核心抽象（Relay, Resolver, Session, Auth）
├── rune-gate    -- HTTP 网关（Axum, 路由, 中间件）
├── rune-flow    -- DAG 工作流引擎
├── rune-schema  -- JSON Schema 校验 + OpenAPI 生成
├── rune-store   -- SQLite 持久化（任务, 日志, Key）
├── rune-proto   -- gRPC protobuf 定义
├── rune-cli     -- CLI 工具
└── rune-server  -- 入口二进制
```

## SDK

### Python (`rune-sdk`)

```bash
pip install rune-sdk
```

- `Caster` -- 连接 Runtime，注册 handler
- `@caster.rune()` -- 注册 unary handler（支持 schema, gate, priority, files）
- `@caster.stream_rune()` -- 注册 streaming handler
- `StreamSender` -- 流式发送器（支持 bytes/str/dict/list）
- 自动重连 + 指数退避
- 完整协议参与（attach, heartbeat, execute, cancel, reconnect）

### TypeScript (`@rune-sdk/caster`)

```bash
npm install @rune-sdk/caster
```

- `Caster` -- 连接 Runtime，注册 handler
- `caster.rune()` -- 注册 unary handler（支持 schema, gate, priority, files）
- `caster.streamRune()` -- 注册 streaming handler
- `StreamSender` -- 流式发送器
- `AbortSignal` 取消感知
- 自动重连 + 指数退避

### Rust (`rune-sdk`)

```toml
[dependencies]
rune-sdk = { path = "sdks/rust" }
```

- `Caster` -- 连接 Runtime，注册 handler
- `caster.rune()` / `caster.rune_with_files()` -- 注册 unary handler
- `caster.stream_rune()` / `caster.stream_rune_with_files()` -- 注册 streaming handler
- `StreamSender` -- 流式发送器
- `CancellationToken` 取消感知
- 自动重连 + 指数退避

## CLI

```bash
rune start [--dev] [--config <path>]   # 启动 Runtime
rune stop                               # 停止 Runtime
rune status                             # 查看 Runtime 状态
rune list                               # 列出在线 Rune
rune call <name> [input] [--stream] [--async]  # 调用 Rune
rune task <id>                          # 查询异步任务
rune key create --type <gate|caster> --label <label>  # 创建 API Key
rune key list                           # 列出 API Key
rune key revoke <id>                    # 吊销 API Key
rune flow register <file>               # 注册 Flow
rune flow list                          # 列出 Flow
rune flow run <name> [input]            # 执行 Flow
rune flow delete <name>                 # 删除 Flow
rune logs [--rune <name>] [--limit N]   # 查看调用日志
rune stats                              # 查看调用统计
rune config init                        # 生成默认配置
rune config show                        # 显示当前配置
```

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |
| GET | `/api/v1/runes` | 列出在线 Rune |
| POST | `/api/v1/runes/:name/run` | 调用 Rune（debug 端点） |
| GET | `/api/v1/tasks/:id` | 查询异步任务 |
| DELETE | `/api/v1/tasks/:id` | 取消异步任务 |
| GET | `/api/v1/status` | Runtime 状态 |
| GET | `/api/v1/casters` | 在线 Caster 列表 |
| GET | `/api/v1/stats` | 调用统计 |
| GET | `/api/v1/logs` | 调用日志 |
| POST | `/api/v1/keys` | 创建 API Key |
| GET | `/api/v1/keys` | 列出 API Key |
| DELETE | `/api/v1/keys/:id` | 吊销 API Key |
| GET | `/api/v1/openapi.json` | OpenAPI 3.0 文档 |
| GET | `/api/v1/files/:id` | 下载文件 |
| POST | `/api/v1/flows` | 创建 Flow |
| GET | `/api/v1/flows` | 列出 Flow |
| GET | `/api/v1/flows/:name` | 获取 Flow 详情 |
| DELETE | `/api/v1/flows/:name` | 删除 Flow |
| POST | `/api/v1/flows/:name/run` | 执行 Flow |
| * | `/{gate_path}` | 动态业务路由（由 Rune 的 gate.path 声明） |

完整 API 参考见 [docs/api-reference.md](docs/api-reference.md)。

## 配置

Runtime 通过 `rune.toml` 配置，支持环境变量覆盖。

```toml
[server]
http_port = 50060
grpc_port = 50070
dev_mode = false
drain_timeout_secs = 15

[auth]
enabled = true
exempt_routes = ["/health"]

[store]
db_path = "rune.db"
log_retention_days = 30

[session]
heartbeat_interval_secs = 10
heartbeat_timeout_secs = 35
max_request_timeout_secs = 30

[gate]
cors_origins = []
max_upload_size_mb = 10

[resolver]
strategy = "round_robin"  # round_robin | random | least_load | priority

[rate_limit]
requests_per_minute = 600

[log]
level = "info"
```

完整配置参考见 [docs/configuration.md](docs/configuration.md)。

## Protocol Guarantees

Rune 定义了 18 条行为保证契约，任何违反都是 bug。涵盖：

- **执行语义** (1-3) -- 统一调用、三种模式、真流式
- **状态收敛** (4-7) -- 超时/取消/断连后状态完全清理
- **背压** (8) -- 严格 max_concurrent 信号量
- **路由** (9-11) -- 真实业务路由、无隐式暴露、路由冲突硬错误
- **Flow** (12) -- Flow 使用同一 Invoker
- **SDK** (13) -- 完整协议参与
- **Schema** (14) -- 有 schema 必校验，无 schema 不拦截
- **文件** (15) -- multipart 有大小上限
- **标签路由** (16) -- 标签不匹配返回 503
- **Flow 隔离** (17) -- 单步失败不污染其他分支已完成的结果
- **优雅停机** (18) -- drain 期间拒绝新请求，等待在飞请求完成

完整列表见 [docs/protocol-guarantees.md](docs/protocol-guarantees.md)。

## 路线图

| 版本 | 状态 | 主要交付 |
|------|------|----------|
| v0.1 | 已完成 | 核心 Runtime + Python SDK + 三种调用模式 + 顺序 Flow |
| v0.2 | 已完成 | API Key 认证 + SQLite 持久化 + 调用日志 |
| v0.3 | 已完成 | JSON Schema 校验 + OpenAPI 生成 + 文件传输 |
| v0.4 | 已完成 | DAG 工作流引擎（并行执行、条件分支、input mapping） |
| v0.5 | 已完成 | TypeScript SDK + CLI 工具 |
| v0.6 | 已完成 | 高级调度 + Rate Limiting + 优雅停机 + Rust SDK |
| v0.7 | 已完成 | 文档全面更新 + 稳定化 |
| v1.0 | 即将发布 | 正式稳定版本 |

## 文档

| 文档 | 说明 |
|------|------|
| [Protocol Guarantees](docs/protocol-guarantees.md) | 18 条行为保证契约 |
| [API Reference](docs/api-reference.md) | HTTP 端点完整参考 |
| [Configuration](docs/configuration.md) | rune.toml 配置参考 |
| [CLI](docs/cli.md) | CLI 命令用法参考 |
| [Python SDK](sdks/python/README.md) | Python Caster SDK |
| [TypeScript SDK](sdks/typescript/README.md) | TypeScript Caster SDK |
| [Rust SDK](sdks/rust/README.md) | Rust Caster SDK |

## License

Private project.
