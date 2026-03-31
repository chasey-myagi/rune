# Rune CLI 参考

`rune` — Rune Runtime 的命令行工具。通过 HTTP API 与 Runtime 通信，管理本地或远程实例。

安装方式：从 [GitHub Releases](https://github.com/chasey-myagi/rune/releases) 下载预编译二进制，或 `cargo install rune-cli`。

---

## 全局选项

| 选项 | 说明 |
|------|------|
| `--remote <url>` | 连接远程 Runtime（环境变量 `RUNE_ADDR`） |
| `--json` | 输出为 JSON 格式（机器可读） |
| `--version` | 显示版本号 |
| `--help` | 显示帮助信息 |

`--remote` 和 `--json` 是全局选项，可用于任何子命令。

## 环境变量

| 变量 | 说明 |
|------|------|
| `RUNE_ADDR` | Runtime HTTP 地址（可被 `--remote` 覆盖） |
| `RUNE_KEY` | API Key，用于认证 |

优先级：`--remote` > `RUNE_ADDR` > 默认值 `http://127.0.0.1:50060`

---

## Runtime 管理

### `rune start`

启动本地 Runtime。默认通过 Docker 拉取并运行 `ghcr.io/chasey-myagi/rune-server` 镜像；也可通过 `--binary` 使用本地二进制。

```
rune start [选项]
```

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--dev` | flag | — | 开发模式（跳过认证，加载 demo runes） |
| `--binary <path>` | string | — | 使用本地二进制替代 Docker |
| `--image <image>` | string | `ghcr.io/chasey-myagi/rune-server` | Docker 镜像 |
| `--tag <tag>` | string | `latest` | Docker 镜像 tag |
| `--http-port <port>` | u16 | `50060` | HTTP 端口 |
| `--grpc-port <port>` | u16 | `50070` | gRPC 端口 |
| `--foreground` | flag | — | 前台运行（日志输出到 stdout，Ctrl+C 停止） |

示例：

```bash
# 开发模式启动
rune start --dev

# 使用本地编译的 server 二进制
rune start --binary ./target/release/rune-server --dev

# 指定镜像和端口
rune start --image ghcr.io/chasey-myagi/rune-server --tag v0.2.0 --http-port 8080

# 前台运行（调试用）
rune start --dev --foreground
```

### `rune stop`

停止后台运行的本地 Runtime。

```
rune stop [选项]
```

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--force` | flag | — | 强制停止（SIGKILL / docker kill） |
| `--timeout <seconds>` | u64 | `10` | 优雅关闭超时秒数 |

示例：

```bash
# 优雅停止
rune stop

# 强制停止
rune stop --force

# 设置 30 秒超时
rune stop --timeout 30
```

### `rune status`

查看 Runtime 运行状态（uptime、Caster 数量、Rune 数量等）。

```
rune status
```

---

## Rune 调用

### `rune list`

列出所有在线 Rune。

```
rune list
```

### `rune call`

调用指定 Rune。支持同步、流式（SSE）和异步三种模式。

```
rune call <name> [input] [选项]
```

| 参数/选项 | 类型 | 默认值 | 说明 |
|-----------|------|--------|------|
| `name` | string（必填） | — | Rune 名称或 gate 路径（如 `echo` 或 `/echo`） |
| `input` | string | — | 输入 JSON 字符串 |
| `--stream` | flag | — | 流式模式（SSE），与 `--async` 互斥 |
| `--async` | flag | — | 异步模式（返回 task ID），与 `--stream` 互斥 |
| `--input-file <path>` | string | — | 从文件读取输入 |
| `--timeout <seconds>` | u64 | `30` | 请求超时秒数 |

示例：

```bash
# 同步调用
rune call translate '{"text": "hello", "lang": "zh"}'

# 流式调用
rune call generate '{"prompt": "write a poem"}' --stream

# 异步调用（返回 task ID）
rune call translate '{"text": "hello"}' --async

# 从文件读取输入
rune call summarize --input-file ./article.json

# 设置超时
rune call slow-task '{"data": "..."}' --timeout 120

# JSON 格式输出
rune call echo '{"msg": "hi"}' --json
```

---

## 异步任务

### `rune task get`

获取异步任务的状态和结果。

```
rune task get <id>
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `id` | string（必填） | 任务 ID |

示例：

```bash
rune task get r-18e8f3a1b-0
```

### `rune task list`

列出所有任务，支持按状态和 Rune 名称过滤。

```
rune task list [选项]
```

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--status <status>` | string | — | 按状态过滤（pending / running / completed / failed / cancelled） |
| `--rune <name>` | string | — | 按 Rune 名称过滤 |
| `--limit <n>` | u32 | `50` | 最大返回条数 |

示例：

```bash
# 列出所有任务
rune task list

# 只看运行中的任务
rune task list --status running

# 只看某个 Rune 的任务
rune task list --rune translate --limit 10
```

### `rune task wait`

阻塞等待任务完成，完成后返回结果。

```
rune task wait <id> [选项]
```

| 参数/选项 | 类型 | 默认值 | 说明 |
|-----------|------|--------|------|
| `id` | string（必填） | — | 任务 ID |
| `--timeout <seconds>` | u64 | `300` | 等待超时秒数 |

示例：

```bash
# 等待任务完成
rune task wait r-18e8f3a1b-0

# 最多等 60 秒
rune task wait r-18e8f3a1b-0 --timeout 60
```

### `rune task delete`

删除一个任务。

```
rune task delete <id>
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `id` | string（必填） | 任务 ID |

示例：

```bash
rune task delete r-18e8f3a1b-0
```

---

## Caster 查看

### `rune casters`

查看当前连接的 Caster 列表。

```
rune casters
```

---

## API Key 管理

### `rune key create`

创建新的 API Key。

```
rune key create --type <gate|caster> --label <label>
```

| 选项 | 类型 | 说明 |
|------|------|------|
| `--type <type>` | string（必填） | Key 类型：`gate`（HTTP 调用）或 `caster`（gRPC 连接） |
| `--label <label>` | string（必填） | 人类可读标签 |

示例：

```bash
rune key create --type gate --label "my-app"
# 输出: rk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# 注意: raw key 只显示一次，请妥善保存
```

### `rune key list`

列出所有 API Key。

```
rune key list
```

### `rune key revoke`

吊销 API Key。吊销后立即失效。

```
rune key revoke <key_id>
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `key_id` | string（必填） | Key ID |

示例：

```bash
rune key revoke 1
```

---

## Flow 工作流

### `rune flow register`

从 YAML/JSON 文件注册 DAG Flow。

```
rune flow register <file>
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `file` | string（必填） | Flow 定义文件路径（.yaml / .yml / .json） |

示例：

```bash
rune flow register pipeline.yaml
```

### `rune flow list`

列出所有已注册 Flow。

```
rune flow list
```

### `rune flow get`

查看 Flow 详细信息。

```
rune flow get <name>
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `name` | string（必填） | Flow 名称 |

示例：

```bash
rune flow get translate-pipeline
```

### `rune flow run`

执行 Flow。

```
rune flow run <name> [input]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `name` | string（必填） | Flow 名称 |
| `input` | string | 输入 JSON 字符串 |

示例：

```bash
rune flow run translate-pipeline '{"text": "hello world"}'
```

### `rune flow delete`

删除 Flow。

```
rune flow delete <name>
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `name` | string（必填） | Flow 名称 |

示例：

```bash
rune flow delete translate-pipeline
```

---

## 运维

### `rune logs`

查看调用日志。

```
rune logs [选项]
```

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--rune <name>` | string | — | 按 Rune 名称过滤 |
| `--limit <n>` | u32 | `20` | 返回条数 |

示例：

```bash
# 最近 20 条日志
rune logs

# 只看 translate 的日志
rune logs --rune translate

# 最近 100 条
rune logs --limit 100
```

### `rune stats`

查看运行时统计信息（总调用数、按 Rune 分组的延迟和成功率等）。

```
rune stats
```

---

## 配置

### `rune config init`

在 `~/.rune/config.toml` 生成默认配置文件。

```
rune config init
```

### `rune config show`

显示当前生效的完整配置（合并配置文件、环境变量和默认值后的结果）。

```
rune config show
```

### `rune config path`

打印配置文件路径。

```
rune config path
```

---

## 典型工作流

```bash
# 1. 初始化配置
rune config init

# 2. 启动 Runtime（开发模式）
rune start --dev

# 3. 查看状态
rune status

# 4. 创建 API Key（生产环境使用）
rune key create --type gate --label "my-app"
rune key create --type caster --label "python-worker"

# 5. 启动 Caster（在另一个终端）
# Python / TypeScript / Rust ...

# 6. 检查 Rune 和 Caster
rune list
rune casters

# 7. 调用 Rune
rune call translate '{"text": "hello"}'

# 8. 异步调用 + 等待结果
rune call slow-task '{"data": "..."}' --async
rune task wait <task-id>

# 9. 注册并执行 Flow
rune flow register pipeline.yaml
rune flow run translate-pipeline '{"text": "hello world"}'

# 10. 查看统计和日志
rune stats
rune logs --rune translate
```
