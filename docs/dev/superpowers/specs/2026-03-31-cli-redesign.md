# Rune CLI 重构设计方案

> 日期：2026-03-31
> 状态：Draft
> 范围：rune-cli crate 完整重构

---

## 1. 定位与边界

### CLI 是什么

Rune CLI（`rune` 命令）是**开发者的统一入口工具**，面向人类用户（非代码调用）。核心价值：让"本地跑起来试一下"这件事变得一条命令搞定。

### 组件边界

```
CLI  → 面向人，管 Runtime 生命周期 + 调用/管理 Rune
SDK  → 面向代码，Caster 库（连接 + 注册 handler + 执行）
Runtime → 内核进程（rune-server），被 CLI 或 Docker/K8s 拉起
```

- CLI **不是** SDK 的一部分，独立安装、独立分发
- SDK **不启动** Runtime，连不上就报错
- CLI 和 SDK 唯一共享的是 Runtime 的 HTTP/gRPC 协议

### 使用场景优先级

| 场景 | CLI 角色 | 优先级 |
|------|---------|--------|
| 本地开发（写 Rune、调试） | 必须 — 启停 Runtime + 调用测试 | P0 |
| 远程运维（查状态、管 Key） | 可选 — HTTP 客户端 | P1 |
| CI/CD（自动化调用） | 可选 — JSON 输出模式 | P2 |

---

## 2. 用户画像与用户故事

### 画像 A：Rune 开发者（P0）

> 我是一个后端开发者，用 Python/TS/Rust SDK 写 Rune function。我需要在本地快速启动 Runtime，注册我的 Caster，调用测试，看日志。

**用户故事：**

- **US-A1** 作为开发者，我 `rune start` 一键启动本地 Runtime，不需要知道 Docker 命令或编译 Rust
- **US-A2** 作为开发者，我 `rune status` 查看 Runtime 是否在运行、有哪些 Caster 连上了
- **US-A3** 作为开发者，我 `rune call echo '{"msg":"hi"}'` 快速测试我写的 Rune
- **US-A4** 作为开发者，我 `rune call translate --stream '{"text":"hello"}'` 看到流式输出实时打印
- **US-A5** 作为开发者，我 `rune logs --rune echo --follow` 实时查看调用日志排查问题
- **US-A6** 作为开发者，我 `rune stop` 结束开发，干净关闭 Runtime
- **US-A7** 作为开发者，我 `rune list` 看到当前注册的所有 Rune 及其 gate path、来源 Caster

### 画像 B：运维/平台工程师（P1）

> 我管理生产环境的 Rune Runtime（K8s / 云上）。我需要远程查看状态、管理 API Key、注册 Flow。

**用户故事：**

- **US-B1** 作为运维，我 `rune --remote https://prod:50060 status` 查看远程 Runtime 状态
- **US-B2** 作为运维，我 `rune key create --type gate --label "frontend-v2"` 创建 API Key
- **US-B3** 作为运维，我 `rune flow register pipeline.yaml` 注册工作流
- **US-B4** 作为运维，我 `rune casters` 查看所有已连接 Caster 的状态和容量

### 画像 C：CI/CD 脚本（P2）

> 我是自动化脚本，需要调用 Rune、检查状态、所有输出必须是可解析的 JSON。

**用户故事：**

- **US-C1** 作为脚本，我 `rune call echo '{}' --json` 得到纯 JSON 输出（无装饰文本）
- **US-C2** 作为脚本，我 `rune status --json` 得到结构化状态信息用于监控
- **US-C3** 作为脚本，我用 `$RUNE_ADDR` 和 `$RUNE_KEY` 环境变量配置连接，无需交互

---

## 3. 分发策略

### 产物与渠道

| 组件 | 渠道 | 说明 |
|------|------|------|
| Runtime 二进制 | GitHub Releases + Docker (ghcr.io) | 跨平台：linux/darwin × amd64/arm64 |
| CLI 二进制 | GitHub Releases + Homebrew tap | 跨平台：同上 |
| Python SDK | PyPI (`rune-sdk`) | 已有 |
| TypeScript SDK | NPM (`@rune-sdk/caster`) | 已有 |
| Rust SDK | crates.io (`rune-sdk`) | 待发布 |

### GitHub Releases 产物

每次 tag `v*` 发布：

```
rune-v0.2.0-linux-amd64.tar.gz     → 包含 rune (CLI) + rune-server (Runtime)
rune-v0.2.0-linux-arm64.tar.gz
rune-v0.2.0-darwin-amd64.tar.gz
rune-v0.2.0-darwin-arm64.tar.gz
checksums.sha256
```

CLI 和 Runtime 打包在同一个 tarball 里（同版本号），用户解压后两个二进制都有。

### CLI 安装优先级

1. **Homebrew**（P0）：`brew install chasey-myagi/tap/rune`
2. **GitHub Releases 直接下载**（P0）：curl + chmod
3. **cargo install**（P1）：`cargo install rune-cli`
4. **npm -g / pip**（P2）：wrapper 包，内部下载二进制

### 版本号

CLI 和 Runtime **同版本号**，从 `0.2.0` 开始（当前 1.0.0 不合适）。

---

## 4. 全局选项与配置

### 全局 CLI 选项

```
rune [GLOBAL OPTIONS] <COMMAND> [ARGS]

Global Options:
  --remote <URL>     连接远程 Runtime（覆盖配置和环境变量）
  --json             所有输出使用 JSON 格式（机器可读）
  --quiet, -q        最少输出（仅关键信息）
  --verbose, -v      详细输出（调试用）
  --config <FILE>    指定配置文件路径
  --version          显示版本号
  --help             显示帮助
```

### 环境变量

| 变量 | 用途 | 默认值 |
|------|------|--------|
| `RUNE_ADDR` | Runtime HTTP 地址 | `http://127.0.0.1:50060` |
| `RUNE_KEY` | API Key（Bearer token） | 无 |
| `RUNE_CONFIG` | 配置文件路径 | `~/.rune/config.toml` |
| `RUNE_RUNTIME` | Runtime 启动方式（`docker` / `binary`） | `docker` |

### 配置文件 `~/.rune/config.toml`

```toml
[runtime]
# 启动方式: "docker" (默认) 或 "binary"
mode = "docker"

# Docker 模式配置
image = "ghcr.io/chasey-myagi/rune-server"
tag = "latest"             # 或固定版本 "0.2.0"

# Binary 模式配置（覆盖 Docker）
# binary = "/usr/local/bin/rune-server"

# 端口
http_port = 50060
grpc_port = 50070

[auth]
# 默认 API Key（可选）
# key = "rk_..."

[output]
# 默认输出格式: "text" 或 "json"
format = "text"
# 是否启用颜色
color = "auto"   # auto / always / never
```

### 状态文件 `~/.rune/state.json`

CLI 管理 Runtime 生命周期时写入的状态文件（取代 PID 文件）：

```json
{
  "mode": "docker",
  "container_id": "a1b2c3d4...",
  "pid": null,
  "http_port": 50060,
  "grpc_port": 50070,
  "started_at": "2026-03-31T10:00:00Z",
  "version": "0.2.0",
  "dev_mode": true
}
```

Docker 模式记录 `container_id`，binary 模式记录 `pid`。`stop` 根据 mode 决定用 `docker stop` 还是 SIGTERM。

### 优先级链

```
--remote flag  >  RUNE_ADDR env  >  config.toml  >  state.json (本地启动的)  >  默认 localhost:50060
```

---

## 5. 输出格式策略

### 双模式输出

所有命令支持两种输出模式：

**Text 模式（默认）**：人类友好，表格 + 颜色 + 图标

```
$ rune list
NAME        VERSION  MODE     GATE PATH     CASTER         STATUS
echo        1.0.0    unary    /echo         python-caster  ● online
translate   1.0.0    stream   /translate    ts-caster      ● online
summarize   1.0.0    async    /summarize    ts-caster      ● online
```

**JSON 模式（`--json`）**：机器可读，标准 JSON 输出到 stdout

```json
[
  {"name": "echo", "version": "1.0.0", "mode": "unary", "gate_path": "/echo", "caster": "python-caster", "status": "online"},
  ...
]
```

### 输出规则

1. **信息性文本**（提示、进度）→ stderr（不影响管道）
2. **数据输出**（结果、列表）→ stdout
3. **错误信息** → stderr
4. `--json` 时 stdout 只输出合法 JSON，stderr 输出人类文本
5. `--quiet` 时只输出最关键的信息（如 call 的返回值、task ID）

### 实现方式

引入 `output` 模块，所有 command handler 返回结构化数据，由 output 层统一渲染：

```rust
pub enum OutputFormat { Text, Json }

pub trait Renderable {
    fn render_text(&self, w: &mut impl Write) -> Result<()>;
    fn render_json(&self, w: &mut impl Write) -> Result<()>;
}
```

---

## 6. 命令详细设计

### 6.1 `rune start` — 启动本地 Runtime

**Synopsis:**

```
rune start [OPTIONS]

Options:
  --dev              开发模式（禁用认证，注册 demo rune）
  --image <IMAGE>    Docker 镜像（覆盖配置，仅 docker 模式）
  --tag <TAG>        镜像 tag（覆盖配置，仅 docker 模式）
  --binary <PATH>    使用本地二进制启动（覆盖配置，切换为 binary 模式）
  --http-port <PORT> HTTP 端口（默认 50060）
  --grpc-port <PORT> gRPC 端口（默认 50070）
  --foreground       前台运行（日志输出到 stdout，Ctrl+C 停止）
```

**行为：**

1. 读取 `~/.rune/state.json`，如果已有运行中的 Runtime → 检查是否存活
   - 存活 → 提示 "Runtime already running (PID/Container xxx)" + 退出
   - 不存活 → 清理旧 state，继续启动
2. 确定启动模式：`--binary` flag > `RUNE_RUNTIME` env > config.toml > 默认 docker
3. **Docker 模式：**
   - 检查 `docker` 命令是否可用 → 不可用则报错并提示安装 Docker 或用 `--binary`
   - `docker run -d --name rune-runtime -p {http_port}:50060 -p {grpc_port}:50070 {image}:{tag} [--dev]`
   - 记录 container_id 到 state.json
4. **Binary 模式：**
   - 检查二进制是否存在且可执行
   - spawn 子进程（后台），记录 PID 到 state.json
   - `--foreground` 时 exec（替换当前进程），不写 state
5. 等待 Runtime 就绪：轮询 `GET /health`（最多 30 秒，每 500ms 一次）
6. 输出：`Runtime started (docker, port 50060). Ready.`

**错误处理：**

- Docker 未安装 → `Error: Docker is not installed. Install Docker or use --binary <path>`
- 端口被占用 → `Error: Port 50060 is already in use. Use --http-port to specify a different port`
- 健康检查超时 → `Error: Runtime failed to start within 30s. Check logs with: docker logs rune-runtime`
- 镜像拉取失败 → `Error: Failed to pull image ghcr.io/chasey-myagi/rune-server:latest`

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| S01 | Docker 模式正常启动 | Docker 可用，端口空闲 | `rune start` | 容器运行中，health 返回 ok，state.json 写入 |
| S02 | Docker 模式 + --dev | 同上 | `rune start --dev` | 容器带 --dev flag，无需 API Key 即可调用 |
| S03 | Binary 模式正常启动 | rune-server 在 PATH 或指定路径 | `rune start --binary ./rune-server` | 进程运行中，PID 写入 state.json |
| S04 | 重复启动 | Runtime 已运行 | `rune start` | 提示已运行，不创建新实例，退出码 0 |
| S05 | Docker 未安装 | 无 docker 命令 | `rune start` | 报错提示安装 Docker 或用 --binary |
| S06 | 端口占用 | 50060 被其他进程占用 | `rune start` | 报错提示端口冲突 |
| S07 | 健康检查超时 | Runtime 启动异常慢或崩溃 | `rune start` | 30s 超时后报错，提示查日志 |
| S08 | 自定义端口 | 端口空闲 | `rune start --http-port 8080 --grpc-port 8081` | 使用自定义端口启动，state.json 记录端口 |
| S09 | 前台模式 | 端口空闲 | `rune start --foreground` | 日志输出到 stdout，Ctrl+C 停止，不写 state |
| S10 | 自定义镜像 tag | Docker 可用 | `rune start --tag 0.2.0` | 使用指定版本镜像 |
| S11 | 残留 state 清理 | state.json 存在但 Runtime 已死 | `rune start` | 清理旧 state，正常启动 |

---

### 6.2 `rune stop` — 停止本地 Runtime

**Synopsis:**

```
rune stop [OPTIONS]

Options:
  --force            强制停止（SIGKILL / docker kill）
  --timeout <SECS>   优雅关闭等待时间（默认 10 秒）
```

**行为：**

1. 读取 `~/.rune/state.json` → 不存在则报错
2. 检查 Runtime 是否存活
   - 不存活 → 清理 state.json，提示 "Runtime is not running (cleaned up stale state)"
3. **Docker 模式：** `docker stop --time {timeout} {container_id}` → `docker rm {container_id}`
4. **Binary 模式：** SIGTERM → 轮询 `is_process_alive()` → 超时后 SIGKILL
5. 等待进程/容器退出（尊重 --timeout）
6. 确认退出后删除 state.json
7. 输出：`Runtime stopped.`

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| P01 | 正常停止 (Docker) | Docker Runtime 运行中 | `rune stop` | 容器停止并移除，state.json 删除 |
| P02 | 正常停止 (Binary) | Binary Runtime 运行中 | `rune stop` | 进程退出，state.json 删除 |
| P03 | 未运行时停止 | 无 state.json | `rune stop` | 提示未运行，退出码 0 |
| P04 | 残留 state 清理 | state.json 存在但进程已死 | `rune stop` | 清理 state.json，提示已清理 |
| P05 | 强制停止 | Runtime 忽略 SIGTERM | `rune stop --force` | 立即 SIGKILL/docker kill |
| P06 | 超时升级 | Runtime 优雅关闭超过默认时间 | `rune stop --timeout 3` | 3 秒后 SIGKILL |
| P07 | stop → start 循环 | — | `rune start` → `rune stop` → `rune start` | 第二次 start 正常工作 |

---

### 6.3 `rune status` — 查看 Runtime 状态

**Synopsis:**

```
rune status [OPTIONS]

Options:
  --watch            持续刷新（每 2 秒）
```

**行为：**

1. 读取 state.json 获取本地管理信息
2. 请求 `GET /api/v1/status` 获取 Runtime 详细状态
3. 请求 `GET /api/v1/runes` 获取 Rune 统计
4. 合并输出

**Text 输出：**

```
Runtime:    running (docker: a1b2c3d4)
Version:    0.2.0
Uptime:     2h 15m
Mode:       development
HTTP:       http://127.0.0.1:50060
gRPC:       127.0.0.1:50070

Casters:    2 connected
Runes:      5 registered
Flows:      3 registered
```

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| T01 | 运行中状态 | Runtime 运行 | `rune status` | 显示完整状态信息 |
| T02 | 未运行状态 | Runtime 未启动 | `rune status` | 显示 "not running"，退出码 1 |
| T03 | 远程状态 | 远程 Runtime 可达 | `rune --remote https://x status` | 显示远程状态（无 state.json 信息） |
| T04 | JSON 输出 | Runtime 运行 | `rune status --json` | 返回结构化 JSON |
| T05 | 有 Caster 连接 | Caster 已注册 | `rune status` | Casters 计数正确 |
| T06 | watch 模式 | Runtime 运行 | `rune status --watch` | 持续刷新直到 Ctrl+C |

---

### 6.4 `rune list` — 列出已注册 Rune

**Synopsis:**

```
rune list [OPTIONS]

Options:
  --all, -a          显示离线 Rune（Caster 断开的）
```

**Text 输出：**

```
NAME        VERSION  MODE     GATE PATH     CASTER         STATUS
echo        1.0.0    unary    /echo         python-caster  ● online
translate   1.0.0    stream   /translate    ts-caster      ● online
summarize   1.0.0    async    /summarize    ts-caster      ● online

3 runes registered (3 online)
```

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| L01 | 无 Rune | Runtime 运行，无 Caster | `rune list` | 空表格 + "No runes registered" |
| L02 | 有 Rune | Caster 已注册 Rune | `rune list` | 显示表格 |
| L03 | JSON 输出 | 同上 | `rune list --json` | JSON 数组 |
| L04 | 包含离线 | Caster 曾注册后断开 | `rune list --all` | 显示离线 Rune（标记 ○ offline） |

---

### 6.5 `rune call` — 调用 Rune

**Synopsis:**

```
rune call <NAME> [INPUT] [OPTIONS]

Arguments:
  <NAME>             Rune 名称（或 gate path 如 /echo）
  [INPUT]            JSON 输入（字符串）

Options:
  --stream           流式模式（SSE，实时输出）
  --async            异步模式（返回 task ID）
  --input-file <F>   从文件读取输入 JSON
  --timeout <SECS>   请求超时（默认 30 秒）
```

**行为：**

1. 解析输入：`[INPUT]` 参数 > `--input-file` > stdin（如果有管道）> `{}`
2. **输入必须是合法 JSON**，解析失败立即报错（不做自动包装）
3. 根据 flag 选择模式：
   - `--stream` 和 `--async` 互斥（clap `conflicts_with`）
   - 默认：sync 模式
4. 发送请求，输出结果

**Sync 模式输出（Text）：**

```
$ rune call echo '{"msg": "hello"}'
{
  "msg": "hello"
}
```

**Stream 模式输出（Text）：**

```
$ rune call translate --stream '{"text": "hello"}'
翻译
结果
已完成

Stream completed (3 events)
```

**Async 模式输出（Text）：**

```
$ rune call summarize --async '{"url": "https://..."}'
Task submitted: task_a1b2c3d4

Query with: rune task task_a1b2c3d4
```

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| C01 | Sync 调用 | echo Rune 已注册 | `rune call echo '{"msg":"hi"}'` | 返回响应 JSON |
| C02 | Stream 调用 | stream Rune 已注册 | `rune call translate --stream '{"text":"hi"}'` | 实时输出 SSE 事件 |
| C03 | Async 调用 | Rune 已注册 | `rune call echo --async '{}'` | 返回 task ID |
| C04 | 不存在的 Rune | 无此 Rune | `rune call nonexist '{}'` | 报错 "Rune 'nonexist' not found" |
| C05 | 非法 JSON 输入 | Rune 已注册 | `rune call echo '{bad'` | 报错 "Invalid JSON input: ..." |
| C06 | --stream + --async 互斥 | — | `rune call echo --stream --async` | clap 报错互斥冲突 |
| C07 | stdin 输入 | Rune 已注册 | `echo '{"msg":"hi"}' \| rune call echo` | 从 stdin 读取输入 |
| C08 | 文件输入 | input.json 存在 | `rune call echo --input-file input.json` | 从文件读取输入 |
| C09 | JSON 输出 | Rune 已注册 | `rune call echo '{}' --json` | 纯 JSON stdout，无装饰 |
| C10 | 超时 | Rune 执行缓慢 | `rune call slow --timeout 2` | 2 秒后超时报错 |
| C11 | 空输入 | Rune 已注册 | `rune call echo` | 发送 `{}` |
| C12 | 通过 gate path 调用 | Rune gate=/echo | `rune call /echo '{"msg":"hi"}'` | 等价于 `rune call echo` |
| C13 | 需要 API Key | auth enabled | `RUNE_KEY=xxx rune call echo '{}'` | 带 Bearer token 调用成功 |
| C14 | 缺少 API Key | auth enabled | `rune call echo '{}'` | 401 报错，提示设置 RUNE_KEY |

---

### 6.6 `rune task` — 异步任务管理

**Synopsis:**

```
rune task <SUBCOMMAND>

Subcommands:
  get <ID>           查看任务状态和结果
  list               列出所有任务
  wait <ID>          轮询等待任务完成（阻塞）
  delete <ID>        删除任务
```

**`rune task get` Text 输出：**

```
$ rune task get task_a1b2c3d4
Task:     task_a1b2c3d4
Rune:     summarize
Status:   completed
Created:  2m ago
Duration: 1.2s

Result:
{
  "summary": "..."
}
```

**`rune task wait` 行为：**

每 1 秒轮询 `GET /api/v1/tasks/:id`，pending 时显示 spinner，完成后输出结果。支持 `--timeout`。

**`rune task list` Text 输出：**

```
ID              RUNE        STATUS      CREATED    DURATION
task_a1b2c3d4   summarize   completed   2m ago     1.2s
task_e5f6g7h8   analyze     pending     10s ago    —

2 tasks
```

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| K01 | 查看已完成任务 | 有已完成 task | `rune task get <id>` | 显示状态 + 结果 |
| K02 | 查看进行中任务 | 有 pending task | `rune task get <id>` | 显示 pending 状态 |
| K03 | 等待任务完成 | 有 pending task | `rune task wait <id>` | 阻塞 → 完成后输出结果 |
| K04 | 等待超时 | 任务长时间未完成 | `rune task wait <id> --timeout 5` | 5 秒后超时报错 |
| K05 | 列出任务 | 有多个 task | `rune task list` | 表格输出 |
| K06 | 删除任务 | 有 task | `rune task delete <id>` | 删除成功 |
| K07 | 查看不存在的任务 | — | `rune task get invalid` | 报错 "Task not found" |
| K08 | JSON 输出 | 有 task | `rune task get <id> --json` | 纯 JSON |
| K09 | Async call → wait 完整流程 | Rune 已注册 | `rune call x --async` → `rune task wait <id>` | 端到端成功 |

---

### 6.7 `rune casters` — 查看已连接 Caster（新增）

**Synopsis:**

```
rune casters [OPTIONS]
```

**Text 输出：**

```
ID              RUNES   CAPACITY   LABELS          STATUS
python-caster   2       10/10      gpu=false       ● connected
ts-caster       3       5/8        gpu=true,v2     ● connected

2 casters connected
```

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| R01 | 无 Caster | 无连接 | `rune casters` | "No casters connected" |
| R02 | 有 Caster | Caster 已连接 | `rune casters` | 表格输出 |
| R03 | JSON 输出 | Caster 已连接 | `rune casters --json` | JSON 数组 |

---

### 6.8 `rune key` — API Key 管理

**Synopsis:**

```
rune key <SUBCOMMAND>

Subcommands:
  create --type <gate|caster> --label <LABEL>   创建 API Key
  list                                           列出所有 Key
  revoke <ID>                                    吊销 Key
```

**`rune key create` 输出：**

```
$ rune key create --type gate --label "frontend-v2"

API Key created:

  Key:    rk_a1b2c3d4e5f6g7h8i9j0...
  Type:   gate
  Label:  frontend-v2

⚠ Save this key now. It will not be shown again.
```

**`rune key list` 输出：**

```
ID    TYPE    LABEL           CREATED      STATUS
1     gate    frontend-v2     2h ago       active
2     caster  python-worker   1d ago       active
3     gate    old-key         7d ago       revoked
```

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| Y01 | 创建 gate key | Runtime 运行 | `rune key create --type gate --label test` | 显示完整 key |
| Y02 | 创建 caster key | 同上 | `rune key create --type caster --label worker` | 显示完整 key |
| Y03 | 列出 key | 有 key | `rune key list` | 表格（key 值脱敏） |
| Y04 | 吊销 key | 有 active key | `rune key revoke 1` | 吊销成功 |
| Y05 | 吊销后使用 | key 已吊销 | 用已吊销 key 调用 | 401 Unauthorized |
| Y06 | 创建 key → 使用 → 吊销完整流程 | auth enabled | 创建 → RUNE_KEY=xxx call → revoke → call | 创建成功 → 调用成功 → 吊销 → 401 |
| Y07 | 无效 type | — | `rune key create --type invalid --label x` | 报错 "key_type must be 'gate' or 'caster'" |
| Y08 | JSON 输出 | 有 key | `rune key list --json` | JSON 数组 |

---

### 6.9 `rune flow` — 工作流管理

**Synopsis:**

```
rune flow <SUBCOMMAND>

Subcommands:
  register <FILE>        从 YAML/JSON 文件注册 Flow
  list                   列出所有 Flow
  get <NAME>             查看 Flow 详情
  run <NAME> [INPUT]     执行 Flow
  delete <NAME>          删除 Flow
```

**`rune flow register` 行为：**

1. 读取文件（必须是 `.json`、`.yaml`、`.yml` 后缀，其他报错）
2. 解析并发送到 `POST /api/v1/flows`
3. 输出：`Flow 'pipeline' registered (3 steps)`

**`rune flow list` 输出：**

```
NAME        STEPS   CREATED
pipeline    3       2h ago
etl-job     5       1d ago

2 flows registered
```

**`rune flow get` 输出：**

```
$ rune flow get pipeline
Name:     pipeline
Steps:    3
Created:  2h ago

Steps:
  1. extract  → Rune: fetch-data
  2. transform → Rune: process
  3. load     → Rune: store-result

DAG: extract → transform → load
```

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| F01 | 从 YAML 注册 | 有效 YAML 文件 | `rune flow register pipeline.yaml` | 注册成功 |
| F02 | 从 JSON 注册 | 有效 JSON 文件 | `rune flow register pipeline.json` | 注册成功 |
| F03 | 无效文件格式 | .txt 文件 | `rune flow register pipeline.txt` | 报错 "Unsupported format, use .yaml/.yml/.json" |
| F04 | 无效内容 | YAML 语法错误 | `rune flow register bad.yaml` | 报错 parse 错误详情 |
| F05 | 列出 Flow | 有 Flow | `rune flow list` | 表格输出 |
| F06 | 查看 Flow 详情 | 有 Flow | `rune flow get pipeline` | 显示步骤详情 |
| F07 | 执行 Flow | Flow 和对应 Rune 都在线 | `rune flow run pipeline '{}'` | 执行成功，返回结果 |
| F08 | 删除 Flow | 有 Flow | `rune flow delete pipeline` | 删除成功 |
| F09 | 执行不存在的 Flow | — | `rune flow run nonexist` | 报错 "Flow not found" |
| F10 | 注册 → 执行 → 删除完整流程 | Rune 在线 | register → run → delete | 端到端成功 |
| F11 | JSON 输出 | 有 Flow | `rune flow list --json` | JSON |

---

### 6.10 `rune logs` — 查看调用日志

**Synopsis:**

```
rune logs [OPTIONS]

Options:
  --rune <NAME>      按 Rune 名称过滤
  --follow, -f       持续输出新日志（tail -f 风格）
  --limit <N>        显示条数（默认 20）
  --since <DURATION> 只看最近一段时间（如 5m、1h）
```

**Text 输出：**

```
$ rune logs --rune echo --limit 3
TIME                  RUNE    STATUS  DURATION  INPUT SIZE
2026-03-31 10:05:12   echo    200     12ms      128B
2026-03-31 10:05:10   echo    200     8ms       64B
2026-03-31 10:04:58   echo    500     3ms       256B
```

**`--follow` 行为：**

轮询 `GET /api/v1/logs?since={last_timestamp}` 每 1 秒，增量输出新日志。Ctrl+C 退出。

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| G01 | 查看最近日志 | 有调用记录 | `rune logs` | 显示最近 20 条 |
| G02 | 按 Rune 过滤 | 有多个 Rune 调用 | `rune logs --rune echo` | 只显示 echo |
| G03 | 限制条数 | 有调用记录 | `rune logs --limit 5` | 最多 5 条 |
| G04 | follow 模式 | Runtime 运行 | `rune logs -f` + 触发调用 | 实时输出新日志 |
| G05 | 无日志 | 无调用记录 | `rune logs` | "No logs found" |
| G06 | JSON 输出 | 有调用记录 | `rune logs --json` | JSON 数组 |
| G07 | since 过滤 | 有调用记录 | `rune logs --since 5m` | 只显示最近 5 分钟 |

---

### 6.11 `rune stats` — 运行时统计

**Synopsis:**

```
rune stats
```

**Text 输出：**

```
Uptime:           2h 15m
Total Calls:      1,234
  Success:        1,200 (97.2%)
  Failed:         34 (2.8%)
Active Tasks:     3
Avg Latency:      45ms
Casters:          2
Runes:            5
```

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| A01 | 空状态统计 | Runtime 刚启动 | `rune stats` | 全部归零 |
| A02 | 有调用后统计 | 执行过若干调用 | `rune stats` | 计数正确 |
| A03 | JSON 输出 | — | `rune stats --json` | JSON 对象 |

---

### 6.12 `rune config` — 配置管理

**Synopsis:**

```
rune config <SUBCOMMAND>

Subcommands:
  init               生成默认配置文件 ~/.rune/config.toml
  show               显示当前生效的配置（含环境变量覆盖）
  path               输出配置文件路径
```

**`rune config show` 输出：**

```
# Effective configuration (config file + env overrides)
# Source: /Users/x/.rune/config.toml

[runtime]
mode = "docker"                    # from config
image = "ghcr.io/chasey-myagi/rune-server"
tag = "latest"
http_port = 50060
grpc_port = 50070

[auth]
key = "rk_***"                     # from env: RUNE_KEY

[output]
format = "text"
color = "auto"
```

**E2E 测试场景：**

| ID | 场景 | 前置条件 | 操作 | 预期结果 |
|----|------|---------|------|---------|
| N01 | 初始化配置 | 无 config.toml | `rune config init` | 创建默认配置 |
| N02 | 重复初始化 | 已有 config.toml | `rune config init` | 提示已存在，不覆盖 |
| N03 | 显示配置 | 有 config.toml | `rune config show` | 显示合并后配置 |
| N04 | 显示配置路径 | — | `rune config path` | 输出路径 |
| N05 | 环境变量覆盖 | RUNE_ADDR 已设置 | `rune config show` | 显示 env override 标注 |

---

## 7. 跨功能需求

### 7.1 退出码

| 码 | 含义 |
|----|------|
| 0 | 成功 |
| 1 | 运行时错误（连接失败、请求失败） |
| 2 | 用户输入错误（参数错误、JSON 格式错误） |
| 3 | Runtime 未运行 |

### 7.2 错误信息规范

所有错误信息遵循格式：

```
Error: <一句话描述问题>

<可选：建议的修复动作>
```

示例：

```
Error: Cannot connect to Runtime at http://127.0.0.1:50060

Is the Runtime running? Start it with: rune start
```

禁止：
- 裸 HTTP 状态码（"Server returned 404"）
- 框架堆栈跟踪（anyhow backtrace）
- 空白输出（`unwrap_or_default()` 造成的）

### 7.3 信号处理

- `Ctrl+C`（SIGINT）：优雅退出当前操作
- 在 `--follow` / `--watch` / `--foreground` 模式下，Ctrl+C 正常退出（退出码 0）
- 在 `rune call --stream` 中，Ctrl+C 中断流但不报错

### 7.4 版本兼容

CLI 在启动时不检查 Runtime 版本。但 `rune status` 输出中显示 Runtime 版本，方便用户自行判断。

未来可加 `rune version` 同时显示 CLI 和 Runtime 版本。

---

## 8. 架构设计

### 8.1 Crate 结构

保持 `rune-cli` 单 crate，内部拆分模块：

```
rune-cli/
├── Cargo.toml
├── src/
│   ├── lib.rs            ← CLI 类型导出（供测试用）
│   ├── main.rs           ← 仅 parse + dispatch
│   ├── client.rs         ← RuneClient (HTTP)，保留现有实现
│   ├── runtime/           ← Runtime 生命周期管理（新）
│   │   ├── mod.rs
│   │   ├── docker.rs     ← Docker 模式实现
│   │   ├── binary.rs     ← Binary 模式实现
│   │   └── state.rs      ← state.json 读写
│   ├── output/            ← 输出格式化（新）
│   │   ├── mod.rs
│   │   ├── text.rs       ← 表格/人类友好输出
│   │   └── json.rs       ← JSON 输出
│   ├── commands/          ← 命令实现
│   │   ├── mod.rs
│   │   ├── start.rs      ← 重写
│   │   ├── stop.rs       ← 重写
│   │   ├── status.rs     ← 重写
│   │   ├── list.rs
│   │   ├── call.rs       ← 重写（输入校验 + 输出格式）
│   │   ├── task.rs       ← 新增（子命令结构）
│   │   ├── casters.rs    ← 新增
│   │   ├── key.rs
│   │   ├── flow.rs
│   │   ├── logs.rs
│   │   ├── stats.rs
│   │   └── config.rs
│   └── config.rs          ← 配置加载（新）
├── tests/
│   ├── e2e/               ← E2E 测试（需要真实 Runtime）
│   │   ├── start_stop.rs
│   │   ├── call.rs
│   │   ├── flow.rs
│   │   └── ...
│   └── unit/              ← 单元测试
│       ├── cli_parse.rs
│       ├── output.rs
│       └── ...
```

### 8.2 删除的模块

- `daemon.rs` → 被 `runtime/binary.rs` + `runtime/state.rs` 取代

### 8.3 新增依赖

| 依赖 | 用途 |
|------|------|
| `comfy-table` 或 `tabled` | Text 模式表格输出 |
| `indicatif` | 进度条 / spinner（start 等待、task wait） |
| `console` | 颜色输出 + terminal 检测 |

### 8.4 E2E 测试基础设施

E2E 测试需要一个真实的 Runtime 进程。方案：

```rust
/// 测试 fixture：启动一个临时 Runtime
struct TestRuntime {
    container_id: String,
    http_port: u16,
    grpc_port: u16,
}

impl TestRuntime {
    /// 启动一个随机端口的 Runtime 容器
    async fn start() -> Self { ... }
}

impl Drop for TestRuntime {
    fn drop(&mut self) {
        // docker rm -f {container_id}
    }
}
```

每个 E2E 测试文件共享一个 `TestRuntime`（通过 `#[ctor]` 或 `lazy_static`），避免每个测试都启停容器。

---

## 9. 从当前 CLI 迁移

### 保留

- `client.rs` — HTTP 客户端层质量好，保留核心逻辑，增加新端点方法
- `config.rs` — DEFAULT_CONFIG 和 TOML 测试保留，扩展字段
- `Cargo.toml` — 保留依赖，新增 comfy-table / indicatif / console

### 重写

- `main.rs` — 新命令结构（task/casters 子命令、全局 --json/--quiet）
- `commands/runtime.rs` → `commands/start.rs` + `commands/stop.rs` + `commands/status.rs`
- `commands/rune.rs` → `commands/call.rs` + `commands/list.rs`

### 删除

- `daemon.rs` — 替换为 `runtime/` 模块
- 测试中的 `#[path]` hack — 改用 lib.rs 导出

### 版本号

`Cargo.toml` version 从 `1.0.0` 改为 `0.2.0`。

---

## 10. 里程碑建议

| 阶段 | 内容 | 命令 |
|------|------|------|
| M1 | 核心生命周期 + 基础调用 | start, stop, status, list, call |
| M2 | 完整功能 | task, casters, key, flow, logs, stats, config |
| M3 | 分发与安装 | GitHub Releases CI, brew tap, cross-compile |
| M4 | 高级体验 | --follow, --watch, spinner, 颜色, 自动补全 |
