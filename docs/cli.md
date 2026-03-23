# Rune CLI 参考

`rune` 是 Rune Runtime 的命令行管理工具。通过 HTTP API 与 Runtime 通信，可管理本地或远程实例。

## 全局选项

| 选项 | 说明 |
|------|------|
| `--remote <url>` | 连接远程 Runtime（默认 `http://127.0.0.1:50060`） |

## 环境变量

| 变量 | 说明 |
|------|------|
| `RUNE_ADDR` | Runtime HTTP 地址（可被 `--remote` 覆盖） |
| `RUNE_KEY` | API Key，用于认证 |

优先级：`--remote` > `RUNE_ADDR` > 默认值 `http://127.0.0.1:50060`

---

## Runtime 管理

### `rune start`

启动 Runtime 进程。

```bash
rune start              # 使用默认配置
rune start --dev        # 开发模式（跳过认证，绑定 localhost）
rune start --config rune.toml  # 指定配置文件
```

| 选项 | 说明 |
|------|------|
| `--dev` | 开发模式 |
| `--config <path>` | 配置文件路径 |

### `rune stop`

停止后台运行的 Runtime。

```bash
rune stop
```

### `rune status`

查看 Runtime 状态（uptime、Caster 数量、Rune 数量等）。

```bash
rune status
```

输出示例：
```
Runtime: online
Uptime: 3600s
Casters: 3
Runes: 5
Dev mode: false
```

---

## Rune 调用

### `rune list`

列出所有在线 Rune。

```bash
rune list
```

输出示例：
```
NAME          GATE PATH
translate     /translate
summarize     /summarize
internal      -
```

### `rune call`

调用指定 Rune。

```bash
rune call <name> [input] [--stream] [--async]
```

| 参数 | 说明 |
|------|------|
| `name` | Rune 名称 |
| `input` | 输入 JSON 字符串（可选） |
| `--stream` | 流式模式 |
| `--async` | 异步模式 |

示例：

```bash
# 同步调用
rune call translate '{"text": "hello", "lang": "zh"}'

# 流式调用
rune call generate '{"prompt": "write a poem"}' --stream

# 异步调用
rune call translate '{"text": "hello"}' --async
```

### `rune task`

查询异步任务状态。

```bash
rune task <id>
```

| 参数 | 说明 |
|------|------|
| `id` | 任务 ID |

示例：

```bash
rune task r-18e8f3a1b-0
```

输出示例：
```json
{
  "task_id": "r-18e8f3a1b-0",
  "rune_name": "translate",
  "status": "completed",
  "output": "{\"translated\": \"你好\"}"
}
```

---

## API Key 管理

### `rune key create`

创建新的 API Key。

```bash
rune key create --type <gate|caster> --label <label>
```

| 选项 | 说明 |
|------|------|
| `--type` | Key 类型：`gate`（HTTP 调用）或 `caster`（gRPC 连接） |
| `--label` | 人类可读标签 |

示例：

```bash
rune key create --type gate --label "my-app"
# 输出: rk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# 注意: raw key 只显示一次
```

### `rune key list`

列出所有 API Key。

```bash
rune key list
```

输出示例：
```
ID  TYPE    LABEL    PREFIX     CREATED             REVOKED
1   gate    my-app   rk_xxxx   2026-03-23 10:00    -
2   caster  worker   rk_yyyy   2026-03-23 11:00    -
```

### `rune key revoke`

吊销 API Key。吊销后立即失效。

```bash
rune key revoke <key_id>
```

| 参数 | 说明 |
|------|------|
| `key_id` | Key ID |

---

## Flow 管理

### `rune flow register`

从 JSON/YAML 文件注册 DAG Flow。

```bash
rune flow register <file>
```

| 参数 | 说明 |
|------|------|
| `file` | Flow 定义文件路径 |

Flow 定义文件示例（JSON）：

```json
{
  "name": "translate-pipeline",
  "steps": [
    {"name": "detect", "rune": "detect-language", "depends_on": []},
    {"name": "translate", "rune": "translate", "depends_on": ["detect"]}
  ]
}
```

### `rune flow list`

列出所有已注册 Flow。

```bash
rune flow list
```

### `rune flow run`

执行 Flow。

```bash
rune flow run <name> [input]
```

| 参数 | 说明 |
|------|------|
| `name` | Flow 名称 |
| `input` | 输入 JSON 字符串（可选） |

示例：

```bash
rune flow run translate-pipeline '{"text": "hello world"}'
```

### `rune flow delete`

删除 Flow。

```bash
rune flow delete <name>
```

| 参数 | 说明 |
|------|------|
| `name` | Flow 名称 |

---

## 日志和统计

### `rune logs`

查看调用日志。

```bash
rune logs [--rune <name>] [--limit <n>]
```

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `--rune` | -- | 按 Rune 名称过滤 |
| `--limit` | 50 | 返回条数 |

示例：

```bash
rune logs                       # 最近 50 条
rune logs --rune translate      # 只看 translate 的日志
rune logs --limit 100           # 最近 100 条
```

### `rune stats`

查看调用统计（总调用数、按 Rune 分组的延迟和成功率）。

```bash
rune stats
```

---

## 配置管理

### `rune config init`

在当前目录生成默认的 `rune.toml` 配置文件。

```bash
rune config init
```

### `rune config show`

显示当前生效的完整配置（合并配置文件、环境变量和默认值后的结果）。

```bash
rune config show
```

---

## 典型工作流

```bash
# 1. 初始化配置
rune config init

# 2. 启动 Runtime（开发模式）
rune start --dev

# 3. 创建 API Key（生产环境）
rune key create --type gate --label "my-app"
rune key create --type caster --label "python-worker"

# 4. 启动 Caster（Python/TypeScript/Rust）
# ... (在另一个终端)

# 5. 检查 Rune 是否注册
rune list

# 6. 调用 Rune
rune call translate '{"text": "hello"}'

# 7. 注册并执行 Flow
rune flow register pipeline.json
rune flow run translate-pipeline '{"text": "hello world"}'

# 8. 查看统计
rune stats
rune logs --rune translate
```
