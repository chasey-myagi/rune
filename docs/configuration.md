# Rune 配置参考

Rune 有两套独立的配置体系：

| 配置 | 文件 | 控制对象 |
|------|------|----------|
| **CLI 配置** | `~/.rune/config.toml` | `rune` CLI 工具的行为（启动模式、镜像、端口映射等） |
| **Runtime 配置** | `rune.toml`（或 `--config` 指定） | `rune-server` 进程的行为（认证、调度、存储、TLS 等） |

两者互不影响。CLI 配置决定「如何启动 Runtime」，Runtime 配置决定「Runtime 启动后如何运行」。

---

## 1. CLI 配置（`~/.rune/config.toml`）

CLI 配置控制 `rune start` 的行为：使用 Docker 还是本地二进制、映射哪些端口、输出格式等。

### 生成与查看

```bash
rune config init   # 生成默认配置到 ~/.rune/config.toml
rune config show   # 显示当前配置内容
rune config path   # 打印配置文件路径
```

### 完整字段

```toml
[runtime]
# 启动模式: "docker"（默认）或 "binary"
mode = "docker"

# Docker 镜像
image = "ghcr.io/chasey-myagi/rune-server"
tag = "latest"

# 端口映射（映射到宿主机的端口）
http_port = 50060
grpc_port = 50070

# 使用本地二进制时取消注释：
# binary = "/usr/local/bin/rune-server"

[auth]
# enabled = false

[output]
format = "text"    # 输出格式
color = "auto"     # 颜色：auto / always / never
```

#### `[runtime]` 字段说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `mode` | string | `"docker"` | 启动模式。`docker` 使用容器，`binary` 使用本地二进制 |
| `image` | string | `"ghcr.io/chasey-myagi/rune-server"` | Docker 镜像地址 |
| `tag` | string | `"latest"` | Docker 镜像 tag |
| `http_port` | u16 | `50060` | 映射到宿主机的 HTTP 端口 |
| `grpc_port` | u16 | `50070` | 映射到宿主机的 gRPC 端口 |
| `binary` | string? | -- | 本地 `rune-server` 二进制路径（`mode = "binary"` 时使用） |

#### `[output]` 字段说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `format` | string | `"text"` | 输出格式 |
| `color` | string | `"auto"` | 颜色输出模式 |

---

## 2. Runtime 配置（`rune.toml`）

Runtime 配置控制 `rune-server` 进程的所有运行时行为。

### 加载路径优先级

1. 命令行指定：`rune-server --config /path/to/rune.toml`
2. 当前目录：`./rune.toml`
3. 用户配置目录：`~/.config/rune/rune.toml`
4. 内置默认值

找到第一个存在的文件即停止搜索。如果都不存在，使用内置默认值。

### 环境变量覆盖

所有配置项均可通过环境变量覆盖，格式为 `RUNE_{SECTION}__{FIELD}`（双下划线分隔 section 和 field）。

环境变量优先级高于配置文件。无效值会被静默忽略。

### 开发模式

`rune-server --dev` 会自动应用以下覆盖：

- `server.dev_mode = true`
- `server.grpc_host = 127.0.0.1`
- `server.http_host = 127.0.0.1`
- `auth.enabled = false`
- Store 使用内存数据库（当 `db_path` 为默认值 `"rune.db"` 时）
- Rate limit 关闭
- TLS 强制禁用（即使配置了证书路径）

---

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
| `hmac_secret` | string? | `null` | `RUNE_AUTH__HMAC_SECRET` | HMAC 密钥，用于 API Key 哈希。设置后使用 HMAC-SHA256 替代 SHA-256，已有 SHA-256 key 自动兼容。未设置且非 dev 模式时生成临时随机密钥（重启后失效） |

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
| `requests_per_minute` | u32 | `600` | `RUNE_RATE_LIMIT__REQUESTS_PER_MINUTE` | 每分钟最大请求数（全局）。dev 模式下不生效 |

### `[log]` -- 日志

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `level` | string | `"info"` | `RUNE_LOG__LEVEL` | 日志级别（trace / debug / info / warn / error） |
| `file` | string? | `null` | `RUNE_LOG__FILE` | 日志文件路径，`null` 则输出到 stderr。注意：文件日志尚未实现，当前始终输出到 stderr |

### `[telemetry]` -- 遥测

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `otlp_endpoint` | string? | `null` | `RUNE_TELEMETRY__OTLP_ENDPOINT` | OTLP gRPC 端点（如 `"http://localhost:4317"`）。设置后启用 OpenTelemetry tracing layer |
| `prometheus_port` | u16? | `null` | `RUNE_TELEMETRY__PROMETHEUS_PORT` | Prometheus `/metrics` 端点监听端口。设置后在 `0.0.0.0:<port>` 启动 metrics exporter |

当两个字段均为 `null` 时，系统仅使用 `tracing_subscriber::fmt` 输出日志到 stderr。

### `[tls]` -- TLS 加密

| 字段 | 类型 | 默认值 | 环境变量 | 说明 |
|------|------|--------|----------|------|
| `cert_path` | string? | `null` | `RUNE_TLS__CERT_PATH` | TLS 证书文件路径（PEM 格式） |
| `key_path` | string? | `null` | `RUNE_TLS__KEY_PATH` | TLS 私钥文件路径（PEM 格式） |

必须同时设置 `cert_path` 和 `key_path` 才能启用 TLS。只设置其中一个会回退到明文传输并输出警告。启用后 HTTP 和 gRPC 服务器均使用 TLS。dev 模式下 TLS 强制禁用。

---

## 3. 环境变量完整列表

| 环境变量 | 对应配置 | 类型 |
|----------|----------|------|
| `RUNE_SERVER__GRPC_HOST` | `server.grpc_host` | IP |
| `RUNE_SERVER__GRPC_PORT` | `server.grpc_port` | u16 |
| `RUNE_SERVER__HTTP_HOST` | `server.http_host` | IP |
| `RUNE_SERVER__HTTP_PORT` | `server.http_port` | u16 |
| `RUNE_SERVER__DEV_MODE` | `server.dev_mode` | bool |
| `RUNE_SERVER__DRAIN_TIMEOUT_SECS` | `server.drain_timeout_secs` | u64 |
| `RUNE_AUTH__ENABLED` | `auth.enabled` | bool |
| `RUNE_AUTH__HMAC_SECRET` | `auth.hmac_secret` | string |
| `RUNE_STORE__DB_PATH` | `store.db_path` | string |
| `RUNE_STORE__LOG_RETENTION_DAYS` | `store.log_retention_days` | u32 |
| `RUNE_SESSION__HEARTBEAT_INTERVAL_SECS` | `session.heartbeat_interval_secs` | u64 |
| `RUNE_SESSION__HEARTBEAT_TIMEOUT_SECS` | `session.heartbeat_timeout_secs` | u64 |
| `RUNE_SESSION__MAX_REQUEST_TIMEOUT_SECS` | `session.max_request_timeout_secs` | u64 |
| `RUNE_GATE__MAX_UPLOAD_SIZE_MB` | `gate.max_upload_size_mb` | u64 |
| `RUNE_RESOLVER__STRATEGY` | `resolver.strategy` | string |
| `RUNE_RATE_LIMIT__REQUESTS_PER_MINUTE` | `rate_limit.requests_per_minute` | u32 |
| `RUNE_LOG__LEVEL` | `log.level` | string |
| `RUNE_LOG__FILE` | `log.file` | string |
| `RUNE_TELEMETRY__OTLP_ENDPOINT` | `telemetry.otlp_endpoint` | string |
| `RUNE_TELEMETRY__PROMETHEUS_PORT` | `telemetry.prometheus_port` | u16 |
| `RUNE_TLS__CERT_PATH` | `tls.cert_path` | string |
| `RUNE_TLS__KEY_PATH` | `tls.key_path` | string |

空字符串值会将 `Option<T>` 字段重置为 `None`。非 `Option` 字段的无效值会被静默忽略。

---

## 4. 完整 Runtime 配置示例

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
# hmac_secret = "your-secret-here"

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

[telemetry]
# otlp_endpoint = "http://localhost:4317"
# prometheus_port = 9090

[tls]
# cert_path = "/etc/rune/cert.pem"
# key_path = "/etc/rune/key.pem"
```
