# Rune 配置参考

Runtime 通过 TOML 配置文件控制行为。

## 配置文件加载优先级

1. CLI 显式指定：`rune start --config /path/to/rune.toml`
2. 当前目录：`./rune.toml`
3. 用户配置：`~/.config/rune/rune.toml`
4. 内置默认值

## 环境变量覆盖

所有配置项均可通过环境变量覆盖，格式为 `RUNE_{SECTION}__{FIELD}`（双下划线分隔 section 和 field）。

环境变量优先级高于配置文件。无效值会被静默忽略。

## 开发模式

`rune start --dev` 会自动应用以下覆盖：

- `server.dev_mode = true`
- `server.grpc_host = 127.0.0.1`
- `server.http_host = 127.0.0.1`
- `auth.enabled = false`

---

## 完整配置参考

### `[server]` -- 服务器

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `grpc_host` | IP | `0.0.0.0` | `RUNE_SERVER__GRPC_HOST` | gRPC 监听地址 |
| `grpc_port` | u16 | `50070` | `RUNE_SERVER__GRPC_PORT` | gRPC 监听端口（Caster 连接） |
| `http_host` | IP | `0.0.0.0` | `RUNE_SERVER__HTTP_HOST` | HTTP 监听地址 |
| `http_port` | u16 | `50060` | `RUNE_SERVER__HTTP_PORT` | HTTP 监听端口（API 调用） |
| `dev_mode` | bool | `false` | `RUNE_SERVER__DEV_MODE` | 开发模式（绑定 localhost，关闭认证） |
| `drain_timeout_secs` | u64 | `15` | `RUNE_SERVER__DRAIN_TIMEOUT_SECS` | 优雅停机等待时间（秒） |

### `[auth]` -- 认证

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `enabled` | bool | `true` | `RUNE_AUTH__ENABLED` | 是否启用 API Key 认证 |
| `exempt_routes` | string[] | `["/health"]` | -- | 免认证路由列表 |

### `[store]` -- 持久化

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `db_path` | string | `"rune.db"` | `RUNE_STORE__DB_PATH` | SQLite 数据库文件路径 |
| `log_retention_days` | u32 | `30` | `RUNE_STORE__LOG_RETENTION_DAYS` | 调用日志保留天数 |

### `[session]` -- 会话管理

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `heartbeat_interval_secs` | u64 | `10` | `RUNE_SESSION__HEARTBEAT_INTERVAL_SECS` | 心跳发送间隔（秒） |
| `heartbeat_timeout_secs` | u64 | `35` | `RUNE_SESSION__HEARTBEAT_TIMEOUT_SECS` | 心跳超时时间（秒），超时视为断连 |
| `max_request_timeout_secs` | u64 | `30` | `RUNE_SESSION__MAX_REQUEST_TIMEOUT_SECS` | 单次请求最大超时（秒） |

### `[gate]` -- HTTP 网关

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `cors_origins` | string[] | `[]` | -- | CORS 允许的 origin 列表，空则允许所有 |
| `max_upload_size_mb` | u64 | `10` | `RUNE_GATE__MAX_UPLOAD_SIZE_MB` | 文件上传最大体积（MB） |

### `[resolver]` -- 调度策略

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `strategy` | string | `"round_robin"` | `RUNE_RESOLVER__STRATEGY` | 调度策略 |

可选策略：

| 策略 | 说明 |
|------|------|
| `round_robin` | 轮询（默认）。同一 Rune 有多个 Caster 时按顺序分配 |
| `random` | 随机选择一个可用 Caster |
| `least_load` | 最少负载。选择当前可用信号量最多（最空闲）的 Caster |
| `priority` | 优先级优先。先筛选最高优先级的 Caster，再在同级中用内部策略（round-robin）选择 |

### `[rate_limit]` -- 频率限制

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `requests_per_minute` | u32 | `600` | `RUNE_RATE_LIMIT__REQUESTS_PER_MINUTE` | 每分钟最大请求数（全局） |

### `[log]` -- 日志

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `level` | string | `"info"` | `RUNE_LOG__LEVEL` | 日志级别（trace/debug/info/warn/error） |
| `file` | string? | `null` | `RUNE_LOG__FILE` | 日志文件路径，null 则输出到 stderr |

---

## 完整示例

```toml
[server]
grpc_host = "0.0.0.0"
grpc_port = 50070
http_host = "0.0.0.0"
http_port = 50060
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
cors_origins = ["https://myapp.com"]
max_upload_size_mb = 10

[resolver]
strategy = "round_robin"

[rate_limit]
requests_per_minute = 600

[log]
level = "info"
# file = "/var/log/rune/runtime.log"
```

## 生成默认配置

```bash
rune config init    # 在当前目录生成 rune.toml
rune config show    # 显示当前生效的完整配置
```
