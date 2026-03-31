# Rune CLI Redesign — M1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重构 Rune CLI 的核心生命周期（start/stop/status）和基础调用（list/call），实现 Docker-first Runtime 管理和双模式输出。

**Architecture:** CLI 拆分为 lib.rs（类型导出）+ main.rs（入口），引入 output 模块（text/json 双模式）、config 模块（TOML + env）、runtime 模块（Docker/Binary 两种后端 + state.json 状态管理）。所有 command handler 返回结构化数据，由 output 层统一渲染。

**Tech Stack:** Rust, clap 4 (derive), reqwest 0.12, tokio, serde, comfy-table (表格), chrono (时间格式化)

**Spec:** `docs/superpowers/specs/2026-03-31-cli-redesign.md`

---

## File Structure

### 新建文件

| 文件 | 职责 |
|------|------|
| `src/lib.rs` | CLI 类型导出（Cli, Commands, 子命令 enum） |
| `src/output.rs` | OutputFormat enum + print_json/print_table 辅助函数 |
| `src/config.rs` | CliConfig 加载（TOML + env 覆盖） |
| `src/runtime/mod.rs` | RuntimeManager trait + 工厂函数 |
| `src/runtime/state.rs` | RuntimeState（state.json 读写） |
| `src/runtime/docker.rs` | DockerManager（docker run/stop/status） |
| `src/runtime/binary.rs` | BinaryManager（spawn/kill/signal） |
| `src/commands/start.rs` | start 命令 |
| `src/commands/stop.rs` | stop 命令 |
| `src/commands/status.rs` | status 命令 |
| `src/commands/list.rs` | list 命令 |
| `src/commands/call.rs` | call 命令（sync/stream/async） |
| `src/commands/casters.rs` | casters 命令 |
| `src/commands/task.rs` | task 子命令 |
| `tests/unit/cli_parse_test.rs` | CLI 解析测试（替换旧 cli_test.rs） |
| `tests/unit/output_test.rs` | 输出格式化测试 |
| `tests/unit/config_test.rs` | 配置加载测试 |
| `tests/unit/state_test.rs` | state.json 测试 |
| `tests/unit/runtime_test.rs` | Docker/Binary manager 测试 |
| `tests/unit/call_test.rs` | 输入解析 + 互斥测试 |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/main.rs` | 精简为仅入口（parse + dispatch），类型移至 lib.rs |
| `src/client.rs` | 新增 casters() 端点方法，修正 parse_input 严格 JSON |
| `src/commands/mod.rs` | 更新模块声明 |
| `src/commands/key.rs` | 使用 output 模块替代 println |
| `src/commands/flow.rs` | 使用 output 模块 + 文件后缀校验 |
| `src/commands/logs.rs` | 使用 output 模块 |
| `src/commands/config.rs` | 扩展 DEFAULT_CONFIG 字段 |
| `Cargo.toml` | version → 0.2.0，新增 comfy-table / chrono 依赖 |

### 删除文件

| 文件 | 理由 |
|------|------|
| `src/daemon.rs` | 被 runtime/ 模块取代 |
| `src/commands/runtime.rs` | 拆分为 start.rs / stop.rs / status.rs |
| `src/commands/rune.rs` | 拆分为 call.rs / list.rs |
| `tests/cli_test.rs` | 替换为 tests/unit/cli_parse_test.rs |
| `tests/client_test.rs` | 合并到 client.rs 内部 #[cfg(test)] |
| `tests/daemon_test.rs` | 删除（daemon.rs 已删） |

---

## Task 1: 更新 Cargo.toml 依赖和版本号

**Files:**
- Modify: `runtime/rune-cli/Cargo.toml`

- [ ] **Step 1: 更新 Cargo.toml**

```toml
[package]
name = "rune-cli"
version = "0.2.0"
edition = "2021"

[lib]
name = "rune_cli"
path = "src/lib.rs"

[[bin]]
name = "rune"
path = "src/main.rs"

[dependencies]
clap = { version = "4", features = ["derive"] }
reqwest = { version = "0.12", features = ["json", "stream"] }
futures = "0.3"
toml = "0.8"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"
tokio = { version = "1", features = ["full"] }
anyhow = "1"
dirs = "6"
libc = "0.2"
urlencoding = "2"
comfy-table = "7"
chrono = { version = "0.4", features = ["serde"] }

[dev-dependencies]
tempfile = "3"
```

关键变更：
- version 1.0.0 → 0.2.0
- 新增 `[lib]` section（支持测试直接引用 crate）
- 新增 comfy-table（表格输出）和 chrono（时间格式化）
- 移除 dev-dependencies 中的 tempfile（已经在 workspace 级别）

- [ ] **Step 2: 验证编译**

Run: `cd /Users/chasey/cc/projects/rune && cargo check -p rune-cli`
Expected: 编译通过（此时 lib.rs 还不存在，下一步创建）

- [ ] **Step 3: Commit**

```bash
git add runtime/rune-cli/Cargo.toml
git commit -m "chore(cli): bump version to 0.2.0, add comfy-table and chrono deps"
```

---

## Task 2: 创建 lib.rs — 提取 CLI 类型定义

**Files:**
- Create: `runtime/rune-cli/src/lib.rs`
- Modify: `runtime/rune-cli/src/main.rs`

- [ ] **Step 1: 创建 lib.rs，定义新的 CLI 结构**

```rust
// runtime/rune-cli/src/lib.rs
use clap::{Parser, Subcommand};

pub mod client;
pub mod commands;
pub mod config;
pub mod output;
pub mod runtime;

#[derive(Parser, Debug)]
#[command(name = "rune", about = "Rune Runtime CLI", version)]
pub struct Cli {
    /// Connect to a remote Runtime instance
    #[arg(long, global = true, env = "RUNE_ADDR")]
    pub remote: Option<String>,

    /// Output format as JSON (machine-readable)
    #[arg(long, global = true)]
    pub json: bool,

    /// Suppress non-essential output
    #[arg(long, short, global = true)]
    pub quiet: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the local Runtime
    Start {
        /// Enable development mode (no auth, demo runes)
        #[arg(long)]
        dev: bool,

        /// Use a local binary instead of Docker
        #[arg(long)]
        binary: Option<String>,

        /// Docker image (default: ghcr.io/chasey-myagi/rune-server)
        #[arg(long)]
        image: Option<String>,

        /// Docker image tag (default: latest)
        #[arg(long)]
        tag: Option<String>,

        /// HTTP port (default: 50060)
        #[arg(long)]
        http_port: Option<u16>,

        /// gRPC port (default: 50070)
        #[arg(long)]
        grpc_port: Option<u16>,

        /// Run in foreground (logs to stdout, Ctrl+C to stop)
        #[arg(long)]
        foreground: bool,
    },

    /// Stop the local Runtime
    Stop {
        /// Force stop (SIGKILL / docker kill)
        #[arg(long)]
        force: bool,

        /// Graceful shutdown timeout in seconds
        #[arg(long, default_value = "10")]
        timeout: u64,
    },

    /// Show Runtime status
    Status {
        /// Continuously refresh (every 2s)
        #[arg(long)]
        watch: bool,
    },

    /// List online Runes
    List {
        /// Include offline Runes
        #[arg(long, short)]
        all: bool,
    },

    /// Call a Rune
    Call {
        /// Rune name or gate path (e.g., echo or /echo)
        name: String,

        /// Input JSON string
        input: Option<String>,

        /// Enable streaming mode (SSE)
        #[arg(long, conflicts_with = "async_mode")]
        stream: bool,

        /// Enable async mode (returns task ID)
        #[arg(long = "async", conflicts_with = "stream")]
        async_mode: bool,

        /// Read input from file
        #[arg(long)]
        input_file: Option<String>,

        /// Request timeout in seconds
        #[arg(long, default_value = "30")]
        timeout: u64,
    },

    /// Manage async tasks
    #[command(subcommand)]
    Task(TaskCommands),

    /// View connected Casters
    Casters,

    /// API Key management
    #[command(subcommand)]
    Key(KeyCommands),

    /// Flow orchestration
    #[command(subcommand)]
    Flow(FlowCommands),

    /// View invocation logs
    Logs {
        /// Filter by Rune name
        #[arg(long)]
        rune: Option<String>,

        /// Number of log entries to show
        #[arg(long, default_value = "20")]
        limit: u32,

        /// Follow mode (tail -f style)
        #[arg(long, short)]
        follow: bool,
    },

    /// Show runtime statistics
    Stats,

    /// Configuration management
    #[command(subcommand)]
    Config(ConfigCommands),
}

#[derive(Subcommand, Debug)]
pub enum TaskCommands {
    /// Get task status and result
    Get {
        /// Task ID
        id: String,
    },

    /// List all tasks
    List,

    /// Wait for task to complete (blocking)
    Wait {
        /// Task ID
        id: String,

        /// Timeout in seconds
        #[arg(long, default_value = "300")]
        timeout: u64,
    },

    /// Delete a task
    Delete {
        /// Task ID
        id: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum KeyCommands {
    /// Create a new API key
    Create {
        /// Key type: gate or caster
        #[arg(long = "type")]
        key_type: String,

        /// Human-readable label
        #[arg(long)]
        label: String,
    },

    /// List all API keys
    List,

    /// Revoke an API key
    Revoke {
        /// Key ID to revoke
        key_id: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum FlowCommands {
    /// Register a flow from YAML/JSON file
    Register {
        /// Path to flow definition file (.yaml, .yml, .json)
        file: String,
    },

    /// List all registered flows
    List,

    /// Show flow details
    Get {
        /// Flow name
        name: String,
    },

    /// Run a flow
    Run {
        /// Flow name
        name: String,

        /// Input JSON
        input: Option<String>,
    },

    /// Delete a flow
    Delete {
        /// Flow name
        name: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConfigCommands {
    /// Generate default configuration at ~/.rune/config.toml
    Init,

    /// Show current effective configuration
    Show,

    /// Print configuration file path
    Path,
}
```

- [ ] **Step 2: 精简 main.rs 为纯入口**

```rust
// runtime/rune-cli/src/main.rs
use clap::Parser;
use rune_cli::{Cli, Commands, ConfigCommands, FlowCommands, KeyCommands, TaskCommands};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Resolve base URL
    let base_url = cli
        .remote
        .as_deref()
        .unwrap_or("http://127.0.0.1:50060");

    let api_key = std::env::var("RUNE_KEY").ok();
    let client = rune_cli::client::RuneClient::new(base_url, api_key.as_deref());
    let json_mode = cli.json;

    match cli.command {
        Commands::Start {
            dev,
            binary,
            image,
            tag,
            http_port,
            grpc_port,
            foreground,
        } => {
            rune_cli::commands::start::run(dev, binary, image, tag, http_port, grpc_port, foreground)
                .await
        }
        Commands::Stop { force, timeout } => {
            rune_cli::commands::stop::run(force, timeout).await
        }
        Commands::Status { watch } => {
            rune_cli::commands::status::run(&client, watch, json_mode).await
        }
        Commands::List { all } => {
            rune_cli::commands::list::run(&client, all, json_mode).await
        }
        Commands::Call {
            name,
            input,
            stream,
            async_mode,
            input_file,
            timeout,
        } => {
            rune_cli::commands::call::run(
                &client, &name, input.as_deref(), stream, async_mode,
                input_file.as_deref(), timeout, json_mode,
            )
            .await
        }
        Commands::Task(cmd) => match cmd {
            TaskCommands::Get { id } => {
                rune_cli::commands::task::get(&client, &id, json_mode).await
            }
            TaskCommands::List => {
                rune_cli::commands::task::list(&client, json_mode).await
            }
            TaskCommands::Wait { id, timeout } => {
                rune_cli::commands::task::wait(&client, &id, timeout, json_mode).await
            }
            TaskCommands::Delete { id } => {
                rune_cli::commands::task::delete(&client, &id, json_mode).await
            }
        },
        Commands::Casters => {
            rune_cli::commands::casters::run(&client, json_mode).await
        }
        Commands::Key(cmd) => match cmd {
            KeyCommands::Create { key_type, label } => {
                rune_cli::commands::key::create(&client, &key_type, &label, json_mode).await
            }
            KeyCommands::List => {
                rune_cli::commands::key::list(&client, json_mode).await
            }
            KeyCommands::Revoke { key_id } => {
                rune_cli::commands::key::revoke(&client, &key_id, json_mode).await
            }
        },
        Commands::Flow(cmd) => match cmd {
            FlowCommands::Register { file } => {
                rune_cli::commands::flow::register(&client, &file, json_mode).await
            }
            FlowCommands::List => {
                rune_cli::commands::flow::list(&client, json_mode).await
            }
            FlowCommands::Get { name } => {
                rune_cli::commands::flow::get(&client, &name, json_mode).await
            }
            FlowCommands::Run { name, input } => {
                rune_cli::commands::flow::run(&client, &name, input.as_deref(), json_mode).await
            }
            FlowCommands::Delete { name } => {
                rune_cli::commands::flow::delete(&client, &name, json_mode).await
            }
        },
        Commands::Logs {
            rune,
            limit,
            follow,
        } => {
            rune_cli::commands::logs::run(&client, rune.as_deref(), limit, follow, json_mode).await
        }
        Commands::Stats => {
            rune_cli::commands::logs::stats(&client, json_mode).await
        }
        Commands::Config(cmd) => match cmd {
            ConfigCommands::Init => rune_cli::commands::config::init().await,
            ConfigCommands::Show => rune_cli::commands::config::show().await,
            ConfigCommands::Path => rune_cli::commands::config::path().await,
        },
    }
}
```

- [ ] **Step 3: 创建空模块占位（让编译通过）**

创建以下空模块文件（每个后续 Task 会填充内容）：

`src/output.rs`:
```rust
use serde_json::Value;

/// Print JSON value in pretty format to stdout.
pub fn print_json(value: &Value) {
    println!("{}", serde_json::to_string_pretty(value).expect("JSON serialization failed"));
}

/// Print raw JSON value (compact) to stdout.
pub fn print_json_compact(value: &Value) {
    println!("{}", serde_json::to_string(value).expect("JSON serialization failed"));
}
```

`src/config.rs`:
```rust
// CLI configuration — will be implemented in Task 4
```

`src/runtime/mod.rs`:
```rust
pub mod state;
pub mod docker;
pub mod binary;
```

`src/runtime/state.rs`:
```rust
// Runtime state management — will be implemented in Task 4
```

`src/runtime/docker.rs`:
```rust
// Docker runtime manager — will be implemented in Task 5
```

`src/runtime/binary.rs`:
```rust
// Binary runtime manager — will be implemented in Task 5
```

`src/commands/mod.rs`:
```rust
pub mod call;
pub mod casters;
pub mod config;
pub mod flow;
pub mod key;
pub mod list;
pub mod logs;
pub mod start;
pub mod status;
pub mod stop;
pub mod task;
```

`src/commands/start.rs`:
```rust
use anyhow::Result;

pub async fn run(
    _dev: bool, _binary: Option<String>, _image: Option<String>,
    _tag: Option<String>, _http_port: Option<u16>, _grpc_port: Option<u16>,
    _foreground: bool,
) -> Result<()> {
    todo!("start command — Task 6")
}
```

`src/commands/stop.rs`:
```rust
use anyhow::Result;

pub async fn run(_force: bool, _timeout: u64) -> Result<()> {
    todo!("stop command — Task 7")
}
```

`src/commands/status.rs`:
```rust
use anyhow::Result;
use crate::client::RuneClient;

pub async fn run(_client: &RuneClient, _watch: bool, _json: bool) -> Result<()> {
    todo!("status command — Task 8")
}
```

`src/commands/list.rs`:
```rust
use anyhow::Result;
use crate::client::RuneClient;

pub async fn run(client: &RuneClient, _all: bool, json_mode: bool) -> Result<()> {
    let result = client.list_runes().await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result); // placeholder — Task 9 adds table
    }
    Ok(())
}
```

`src/commands/call.rs`:
```rust
use anyhow::Result;
use crate::client::RuneClient;

pub async fn run(
    client: &RuneClient, name: &str, input: Option<&str>,
    stream: bool, async_mode: bool, input_file: Option<&str>,
    _timeout: u64, json_mode: bool,
) -> Result<()> {
    let input_json = resolve_input(input, input_file)?;
    if stream {
        client.call_rune_stream(name, Some(&input_json)).await?;
    } else if async_mode {
        let result = client.call_rune_async(name, Some(&input_json)).await?;
        crate::output::print_json(&result);
    } else {
        let result = client.call_rune(name, Some(&input_json)).await?;
        crate::output::print_json(&result);
    }
    Ok(())
}

/// Resolve input from argument, file, or stdin. Must be valid JSON.
fn resolve_input(input: Option<&str>, input_file: Option<&str>) -> Result<String> {
    let raw = if let Some(file) = input_file {
        std::fs::read_to_string(file)
            .map_err(|e| anyhow::anyhow!("Failed to read input file '{}': {}", file, e))?
    } else if let Some(s) = input {
        s.to_string()
    } else {
        "{}".to_string()
    };

    // Validate JSON
    serde_json::from_str::<serde_json::Value>(&raw)
        .map_err(|e| anyhow::anyhow!("Invalid JSON input: {}", e))?;

    Ok(raw)
}
```

`src/commands/task.rs`:
```rust
use anyhow::Result;
use crate::client::RuneClient;

pub async fn get(client: &RuneClient, id: &str, _json: bool) -> Result<()> {
    let result = client.get_task(id).await?;
    crate::output::print_json(&result);
    Ok(())
}

pub async fn list(client: &RuneClient, _json: bool) -> Result<()> {
    // TODO: need GET /api/v1/tasks endpoint
    let _ = client;
    eprintln!("task list: not yet implemented");
    Ok(())
}

pub async fn wait(client: &RuneClient, id: &str, timeout: u64, _json: bool) -> Result<()> {
    let _ = (client, id, timeout);
    eprintln!("task wait: not yet implemented");
    Ok(())
}

pub async fn delete(client: &RuneClient, id: &str, _json: bool) -> Result<()> {
    let _ = (client, id);
    eprintln!("task delete: not yet implemented");
    Ok(())
}
```

`src/commands/casters.rs`:
```rust
use anyhow::Result;
use crate::client::RuneClient;

pub async fn run(client: &RuneClient, json_mode: bool) -> Result<()> {
    let result = client.casters().await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result); // placeholder — M2 adds table
    }
    Ok(())
}
```

更新 `src/commands/key.rs`:
```rust
use anyhow::Result;
use crate::client::RuneClient;

pub async fn create(client: &RuneClient, key_type: &str, label: &str, json_mode: bool) -> Result<()> {
    let result = client.create_key(key_type, label).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result); // placeholder — M2 adds formatted output
    }
    Ok(())
}

pub async fn list(client: &RuneClient, json_mode: bool) -> Result<()> {
    let result = client.list_keys().await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn revoke(client: &RuneClient, key_id: &str, json_mode: bool) -> Result<()> {
    let result = client.revoke_key(key_id).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}
```

更新 `src/commands/flow.rs`:
```rust
use anyhow::{Context, Result};
use crate::client::RuneClient;

pub async fn register(client: &RuneClient, file: &str, json_mode: bool) -> Result<()> {
    // Validate file extension
    let ext = std::path::Path::new(file)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    if !["json", "yaml", "yml"].contains(&ext) {
        anyhow::bail!("Unsupported file format '.{}'. Use .yaml, .yml, or .json", ext);
    }

    let content =
        std::fs::read_to_string(file).with_context(|| format!("Failed to read file: {}", file))?;

    let definition: serde_json::Value = if ext == "json" {
        serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse JSON from: {}", file))?
    } else {
        serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from: {}", file))?
    };

    let result = client.register_flow(definition).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        eprintln!("Flow registered successfully");
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn list(client: &RuneClient, json_mode: bool) -> Result<()> {
    let result = client.list_flows().await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn get(client: &RuneClient, name: &str, json_mode: bool) -> Result<()> {
    let path = client.build_path("/api/v1/flows/{name}", &[("name", name)]);
    let req = client.get_request(&path);
    let result = client.send_json(req).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn run(client: &RuneClient, name: &str, input: Option<&str>, json_mode: bool) -> Result<()> {
    let result = client.run_flow(name, input).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn delete(client: &RuneClient, name: &str, json_mode: bool) -> Result<()> {
    let result = client.delete_flow(name).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        eprintln!("Flow '{}' deleted", name);
    }
    Ok(())
}
```

更新 `src/commands/logs.rs`:
```rust
use anyhow::Result;
use crate::client::RuneClient;

pub async fn run(client: &RuneClient, rune: Option<&str>, limit: u32, _follow: bool, json_mode: bool) -> Result<()> {
    let result = client.get_logs(rune, limit).await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}

pub async fn stats(client: &RuneClient, json_mode: bool) -> Result<()> {
    let result = client.get_stats().await?;
    if json_mode {
        crate::output::print_json(&result);
    } else {
        crate::output::print_json(&result);
    }
    Ok(())
}
```

更新 `src/commands/config.rs`:
```rust
use anyhow::{Context, Result};

const DEFAULT_CONFIG: &str = r#"# Rune CLI Configuration

[runtime]
# Startup mode: "docker" (default) or "binary"
mode = "docker"

# Docker image
image = "ghcr.io/chasey-myagi/rune-server"
tag = "latest"

# Ports
http_port = 50060
grpc_port = 50070

# Uncomment to use local binary instead of Docker:
# binary = "/usr/local/bin/rune-server"

[auth]
# enabled = false

[output]
format = "text"
color = "auto"
"#;

pub async fn init() -> Result<()> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    let config_dir = home.join(".rune");
    let config_path = config_dir.join("config.toml");

    if config_path.exists() {
        eprintln!("Configuration file already exists: {}", config_path.display());
        eprintln!("To overwrite, delete it first and run `rune config init` again.");
        return Ok(());
    }

    std::fs::create_dir_all(&config_dir)
        .with_context(|| format!("Failed to create directory: {}", config_dir.display()))?;
    std::fs::write(&config_path, DEFAULT_CONFIG)
        .with_context(|| format!("Failed to write config: {}", config_path.display()))?;

    println!("Configuration written to {}", config_path.display());
    Ok(())
}

pub async fn show() -> Result<()> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    let config_path = home.join(".rune").join("config.toml");

    if !config_path.exists() {
        eprintln!("No configuration file found at {}", config_path.display());
        eprintln!("Run `rune config init` to create one.");
        return Ok(());
    }

    let content = std::fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read config: {}", config_path.display()))?;

    println!("# {}", config_path.display());
    println!("{}", content);
    Ok(())
}

pub async fn path() -> Result<()> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    println!("{}", home.join(".rune").join("config.toml").display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid_toml() {
        let parsed: toml::Value =
            toml::from_str(DEFAULT_CONFIG).expect("DEFAULT_CONFIG must be valid TOML");
        let runtime = parsed.get("runtime").expect("must have [runtime] section");
        assert!(runtime.get("http_port").is_some());
        assert!(runtime.get("grpc_port").is_some());
        assert_eq!(runtime.get("mode").unwrap().as_str(), Some("docker"));
    }
}
```

- [ ] **Step 4: 更新 client.rs — 增加 casters 端点 + 公开辅助方法 + 修正 parse_input**

在 `client.rs` 中做以下修改：

1. 将 `request()` 和 `send_json()` 的可见性改为 `pub`（让 flow::get 能用）
2. 新增 `pub fn get_request(&self, path: &str)` 便捷方法
3. 新增 `casters()` 方法
4. 修正 `parse_input` — 非法 JSON 直接报错，不自动包装

```rust
// 在 impl RuneClient 中添加：

/// Convenience: build a GET request for a path.
pub fn get_request(&self, path: &str) -> reqwest::RequestBuilder {
    self.request(reqwest::Method::GET, path)
}

/// GET /api/v1/casters
pub async fn casters(&self) -> Result<Value> {
    let req = self.request(reqwest::Method::GET, "/api/v1/casters");
    self.send_json(req).await
}
```

修改 `parse_input`:
```rust
/// Parse an input string as JSON. Returns error if not valid JSON.
fn parse_input(input: Option<&str>) -> Result<Value> {
    match input {
        Some(s) => serde_json::from_str(s)
            .map_err(|e| anyhow::anyhow!("Invalid JSON input: {}", e)),
        None => Ok(json!({})),
    }
}
```

修改所有调用 `parse_input` 的地方从 `Self::parse_input(input)` 改为 `Self::parse_input(input)?`。

修改 `request` 和 `send_json` 的可见性：
```rust
pub fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder { ... }
pub async fn send_json(&self, req: reqwest::RequestBuilder) -> Result<Value> { ... }
```

- [ ] **Step 5: 删除旧文件**

```bash
rm runtime/rune-cli/src/daemon.rs
rm runtime/rune-cli/src/commands/runtime.rs
rm runtime/rune-cli/src/commands/rune.rs
rm runtime/rune-cli/tests/cli_test.rs
rm runtime/rune-cli/tests/client_test.rs
rm runtime/rune-cli/tests/daemon_test.rs
```

将 `client_test.rs` 中的安全测试迁移到 `client.rs` 的 `#[cfg(test)] mod tests` 中（已有部分测试在那里，追加 URL encoding 测试）。

- [ ] **Step 6: 编译验证**

Run: `cd /Users/chasey/cc/projects/rune && cargo check -p rune-cli`
Expected: 编译通过

Run: `cd /Users/chasey/cc/projects/rune && cargo test -p rune-cli`
Expected: 现有内联测试通过

- [ ] **Step 7: Commit**

```bash
git add -A runtime/rune-cli/
git commit -m "refactor(cli): extract lib.rs, new command structure, remove daemon.rs"
```

---

## Task 3: 新 CLI 解析测试

**Files:**
- Create: `runtime/rune-cli/tests/unit/cli_parse_test.rs`

- [ ] **Step 1: 创建 tests/unit/ 目录和测试文件**

```rust
// runtime/rune-cli/tests/unit/cli_parse_test.rs
use clap::Parser;
use rune_cli::{Cli, Commands, TaskCommands, KeyCommands, FlowCommands, ConfigCommands};

fn parse(args: &[&str]) -> Cli {
    let mut full = vec!["rune"];
    full.extend_from_slice(args);
    Cli::parse_from(full)
}

// ── Start ──────────────────────────────────────────────────────

#[test]
fn start_default() {
    let cli = parse(&["start"]);
    match cli.command {
        Commands::Start { dev, binary, foreground, http_port, grpc_port, .. } => {
            assert!(!dev);
            assert!(binary.is_none());
            assert!(!foreground);
            assert!(http_port.is_none());
            assert!(grpc_port.is_none());
        }
        _ => panic!("expected Start"),
    }
}

#[test]
fn start_dev_with_binary() {
    let cli = parse(&["start", "--dev", "--binary", "/usr/local/bin/rune-server"]);
    match cli.command {
        Commands::Start { dev, binary, .. } => {
            assert!(dev);
            assert_eq!(binary.as_deref(), Some("/usr/local/bin/rune-server"));
        }
        _ => panic!("expected Start"),
    }
}

#[test]
fn start_custom_ports() {
    let cli = parse(&["start", "--http-port", "8080", "--grpc-port", "8081"]);
    match cli.command {
        Commands::Start { http_port, grpc_port, .. } => {
            assert_eq!(http_port, Some(8080));
            assert_eq!(grpc_port, Some(8081));
        }
        _ => panic!("expected Start"),
    }
}

// ── Stop ───────────────────────────────────────────────────────

#[test]
fn stop_default() {
    let cli = parse(&["stop"]);
    match cli.command {
        Commands::Stop { force, timeout } => {
            assert!(!force);
            assert_eq!(timeout, 10);
        }
        _ => panic!("expected Stop"),
    }
}

#[test]
fn stop_force_with_timeout() {
    let cli = parse(&["stop", "--force", "--timeout", "3"]);
    match cli.command {
        Commands::Stop { force, timeout } => {
            assert!(force);
            assert_eq!(timeout, 3);
        }
        _ => panic!("expected Stop"),
    }
}

// ── Status ─────────────────────────────────────────────────────

#[test]
fn status_default() {
    let cli = parse(&["status"]);
    match cli.command {
        Commands::Status { watch } => assert!(!watch),
        _ => panic!("expected Status"),
    }
}

// ── List ───────────────────────────────────────────────────────

#[test]
fn list_default() {
    let cli = parse(&["list"]);
    match cli.command {
        Commands::List { all } => assert!(!all),
        _ => panic!("expected List"),
    }
}

#[test]
fn list_all() {
    let cli = parse(&["list", "--all"]);
    match cli.command {
        Commands::List { all } => assert!(all),
        _ => panic!("expected List"),
    }
}

// ── Call ───────────────────────────────────────────────────────

#[test]
fn call_with_input() {
    let cli = parse(&["call", "echo", r#"{"msg":"hi"}"#]);
    match cli.command {
        Commands::Call { name, input, stream, async_mode, .. } => {
            assert_eq!(name, "echo");
            assert_eq!(input.as_deref(), Some(r#"{"msg":"hi"}"#));
            assert!(!stream);
            assert!(!async_mode);
        }
        _ => panic!("expected Call"),
    }
}

#[test]
fn call_stream() {
    let cli = parse(&["call", "echo", "--stream"]);
    match cli.command {
        Commands::Call { stream, async_mode, .. } => {
            assert!(stream);
            assert!(!async_mode);
        }
        _ => panic!("expected Call"),
    }
}

#[test]
fn call_async() {
    let cli = parse(&["call", "echo", "--async"]);
    match cli.command {
        Commands::Call { stream, async_mode, .. } => {
            assert!(!stream);
            assert!(async_mode);
        }
        _ => panic!("expected Call"),
    }
}

#[test]
fn call_stream_async_conflict() {
    let result = Cli::try_parse_from(["rune", "call", "echo", "--stream", "--async"]);
    assert!(result.is_err(), "--stream and --async must conflict");
}

#[test]
fn call_with_input_file() {
    let cli = parse(&["call", "echo", "--input-file", "data.json"]);
    match cli.command {
        Commands::Call { input_file, .. } => {
            assert_eq!(input_file.as_deref(), Some("data.json"));
        }
        _ => panic!("expected Call"),
    }
}

#[test]
fn call_no_input() {
    let cli = parse(&["call", "echo"]);
    match cli.command {
        Commands::Call { name, input, .. } => {
            assert_eq!(name, "echo");
            assert!(input.is_none());
        }
        _ => panic!("expected Call"),
    }
}

// ── Task ───────────────────────────────────────────────────────

#[test]
fn task_get() {
    let cli = parse(&["task", "get", "task-123"]);
    match cli.command {
        Commands::Task(TaskCommands::Get { id }) => assert_eq!(id, "task-123"),
        _ => panic!("expected Task Get"),
    }
}

#[test]
fn task_list() {
    let cli = parse(&["task", "list"]);
    assert!(matches!(cli.command, Commands::Task(TaskCommands::List)));
}

#[test]
fn task_wait() {
    let cli = parse(&["task", "wait", "task-123", "--timeout", "60"]);
    match cli.command {
        Commands::Task(TaskCommands::Wait { id, timeout }) => {
            assert_eq!(id, "task-123");
            assert_eq!(timeout, 60);
        }
        _ => panic!("expected Task Wait"),
    }
}

#[test]
fn task_delete() {
    let cli = parse(&["task", "delete", "task-123"]);
    match cli.command {
        Commands::Task(TaskCommands::Delete { id }) => assert_eq!(id, "task-123"),
        _ => panic!("expected Task Delete"),
    }
}

// ── Casters ────────────────────────────────────────────────────

#[test]
fn casters() {
    let cli = parse(&["casters"]);
    assert!(matches!(cli.command, Commands::Casters));
}

// ── Key ────────────────────────────────────────────────────────

#[test]
fn key_create() {
    let cli = parse(&["key", "create", "--type", "gate", "--label", "test"]);
    match cli.command {
        Commands::Key(KeyCommands::Create { key_type, label }) => {
            assert_eq!(key_type, "gate");
            assert_eq!(label, "test");
        }
        _ => panic!("expected Key Create"),
    }
}

#[test]
fn key_list() {
    let cli = parse(&["key", "list"]);
    assert!(matches!(cli.command, Commands::Key(KeyCommands::List)));
}

#[test]
fn key_revoke() {
    let cli = parse(&["key", "revoke", "key-42"]);
    match cli.command {
        Commands::Key(KeyCommands::Revoke { key_id }) => assert_eq!(key_id, "key-42"),
        _ => panic!("expected Key Revoke"),
    }
}

// ── Flow ───────────────────────────────────────────────────────

#[test]
fn flow_register() {
    let cli = parse(&["flow", "register", "flow.yaml"]);
    match cli.command {
        Commands::Flow(FlowCommands::Register { file }) => assert_eq!(file, "flow.yaml"),
        _ => panic!("expected Flow Register"),
    }
}

#[test]
fn flow_list() {
    let cli = parse(&["flow", "list"]);
    assert!(matches!(cli.command, Commands::Flow(FlowCommands::List)));
}

#[test]
fn flow_get() {
    let cli = parse(&["flow", "get", "pipeline"]);
    match cli.command {
        Commands::Flow(FlowCommands::Get { name }) => assert_eq!(name, "pipeline"),
        _ => panic!("expected Flow Get"),
    }
}

#[test]
fn flow_run() {
    let cli = parse(&["flow", "run", "pipeline", r#"{"x":1}"#]);
    match cli.command {
        Commands::Flow(FlowCommands::Run { name, input }) => {
            assert_eq!(name, "pipeline");
            assert_eq!(input.as_deref(), Some(r#"{"x":1}"#));
        }
        _ => panic!("expected Flow Run"),
    }
}

#[test]
fn flow_delete() {
    let cli = parse(&["flow", "delete", "pipeline"]);
    match cli.command {
        Commands::Flow(FlowCommands::Delete { name }) => assert_eq!(name, "pipeline"),
        _ => panic!("expected Flow Delete"),
    }
}

// ── Logs ───────────────────────────────────────────────────────

#[test]
fn logs_default() {
    let cli = parse(&["logs"]);
    match cli.command {
        Commands::Logs { rune, limit, follow } => {
            assert!(rune.is_none());
            assert_eq!(limit, 20);
            assert!(!follow);
        }
        _ => panic!("expected Logs"),
    }
}

#[test]
fn logs_with_filter_and_follow() {
    let cli = parse(&["logs", "--rune", "echo", "--limit", "100", "-f"]);
    match cli.command {
        Commands::Logs { rune, limit, follow } => {
            assert_eq!(rune.as_deref(), Some("echo"));
            assert_eq!(limit, 100);
            assert!(follow);
        }
        _ => panic!("expected Logs"),
    }
}

// ── Stats ──────────────────────────────────────────────────────

#[test]
fn stats() {
    let cli = parse(&["stats"]);
    assert!(matches!(cli.command, Commands::Stats));
}

// ── Config ─────────────────────────────────────────────────────

#[test]
fn config_init() {
    let cli = parse(&["config", "init"]);
    assert!(matches!(cli.command, Commands::Config(ConfigCommands::Init)));
}

#[test]
fn config_show() {
    let cli = parse(&["config", "show"]);
    assert!(matches!(cli.command, Commands::Config(ConfigCommands::Show)));
}

#[test]
fn config_path() {
    let cli = parse(&["config", "path"]);
    assert!(matches!(cli.command, Commands::Config(ConfigCommands::Path)));
}

// ── Global flags ───────────────────────────────────────────────

#[test]
fn global_json_flag() {
    let cli = parse(&["--json", "list"]);
    assert!(cli.json);
}

#[test]
fn global_quiet_flag() {
    let cli = parse(&["-q", "status"]);
    assert!(cli.quiet);
}

#[test]
fn global_remote_flag() {
    let cli = parse(&["--remote", "https://rune.prod.io", "status"]);
    assert_eq!(cli.remote.as_deref(), Some("https://rune.prod.io"));
}

#[test]
fn remote_with_call() {
    let cli = parse(&["--remote", "http://10.0.0.1:8080", "call", "echo", r#"{"x":1}"#]);
    assert_eq!(cli.remote.as_deref(), Some("http://10.0.0.1:8080"));
    match cli.command {
        Commands::Call { name, input, .. } => {
            assert_eq!(name, "echo");
            assert_eq!(input.as_deref(), Some(r#"{"x":1}"#));
        }
        _ => panic!("expected Call"),
    }
}
```

- [ ] **Step 2: 运行测试**

Run: `cd /Users/chasey/cc/projects/rune && cargo test -p rune-cli --test cli_parse_test`
Expected: 全部通过（~35 个测试），包括 `call_stream_async_conflict` 验证互斥

- [ ] **Step 3: Commit**

```bash
git add runtime/rune-cli/tests/
git commit -m "test(cli): add CLI parse tests for new command structure"
```

---

## Task 4: Runtime State 管理模块

**Files:**
- Create: `runtime/rune-cli/src/runtime/state.rs`

- [ ] **Step 1: 编写 state.rs 测试**

在 `src/runtime/state.rs` 底部添加测试：

```rust
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeState {
    pub mode: RuntimeMode,
    /// Docker container ID (if mode == Docker)
    pub container_id: Option<String>,
    /// Process PID (if mode == Binary)
    pub pid: Option<u32>,
    pub http_port: u16,
    pub grpc_port: u16,
    pub started_at: DateTime<Utc>,
    pub version: String,
    pub dev_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeMode {
    Docker,
    Binary,
}

impl RuntimeState {
    pub fn new_docker(container_id: &str, http_port: u16, grpc_port: u16, dev: bool) -> Self {
        Self {
            mode: RuntimeMode::Docker,
            container_id: Some(container_id.to_string()),
            pid: None,
            http_port,
            grpc_port,
            started_at: Utc::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            dev_mode: dev,
        }
    }

    pub fn new_binary(pid: u32, http_port: u16, grpc_port: u16, dev: bool) -> Self {
        Self {
            mode: RuntimeMode::Binary,
            container_id: None,
            pid: Some(pid),
            http_port,
            grpc_port,
            started_at: Utc::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            dev_mode: dev,
        }
    }
}

/// Path to the state file: ~/.rune/state.json
pub fn state_file_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    Ok(home.join(".rune").join("state.json"))
}

/// Read current runtime state, if it exists.
pub fn read_state() -> Result<Option<RuntimeState>> {
    let path = state_file_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read state file: {}", path.display()))?;
    let state: RuntimeState = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse state file: {}", path.display()))?;
    Ok(Some(state))
}

/// Write runtime state to disk.
pub fn write_state(state: &RuntimeState) -> Result<()> {
    let path = state_file_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(state)?;
    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write state file: {}", path.display()))?;
    Ok(())
}

/// Remove the state file.
pub fn remove_state() -> Result<()> {
    let path = state_file_path()?;
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docker_state_roundtrip() {
        let state = RuntimeState::new_docker("abc123", 50060, 50070, true);
        let json = serde_json::to_string_pretty(&state).unwrap();
        let parsed: RuntimeState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.mode, RuntimeMode::Docker);
        assert_eq!(parsed.container_id.as_deref(), Some("abc123"));
        assert!(parsed.pid.is_none());
        assert_eq!(parsed.http_port, 50060);
        assert_eq!(parsed.grpc_port, 50070);
        assert!(parsed.dev_mode);
    }

    #[test]
    fn test_binary_state_roundtrip() {
        let state = RuntimeState::new_binary(12345, 50060, 50070, false);
        let json = serde_json::to_string_pretty(&state).unwrap();
        let parsed: RuntimeState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.mode, RuntimeMode::Binary);
        assert!(parsed.container_id.is_none());
        assert_eq!(parsed.pid, Some(12345));
        assert!(!parsed.dev_mode);
    }

    #[test]
    fn test_state_file_path_contains_rune_dir() {
        let path = state_file_path().unwrap();
        assert!(path.ends_with(".rune/state.json"));
    }
}
```

- [ ] **Step 2: 运行测试**

Run: `cd /Users/chasey/cc/projects/rune && cargo test -p rune-cli state::tests`
Expected: 3 个测试通过

- [ ] **Step 3: Commit**

```bash
git add runtime/rune-cli/src/runtime/state.rs
git commit -m "feat(cli): add RuntimeState module for state.json management"
```

---

## Task 5: Docker 和 Binary Runtime Manager

**Files:**
- Create: `runtime/rune-cli/src/runtime/docker.rs`
- Create: `runtime/rune-cli/src/runtime/binary.rs`
- Modify: `runtime/rune-cli/src/runtime/mod.rs`

- [ ] **Step 1: 实现 Docker manager**

```rust
// runtime/rune-cli/src/runtime/docker.rs
use anyhow::{Context, Result};
use std::process::Command;

/// Check if Docker CLI is available.
pub fn is_docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Start a Runtime container. Returns the container ID.
pub fn start_container(
    image: &str,
    tag: &str,
    http_port: u16,
    grpc_port: u16,
    dev: bool,
) -> Result<String> {
    let image_ref = format!("{}:{}", image, tag);
    let mut args = vec![
        "run", "-d",
        "--name", "rune-runtime",
        "-p", &format!("{}:50060", http_port),
        "-p", &format!("{}:50070", grpc_port),
    ];

    // Pass dev mode as environment variable
    if dev {
        args.extend_from_slice(&["-e", "RUNE_DEV=true"]);
    }

    args.push(&image_ref);

    if dev {
        args.push("--dev");
    }

    let output = Command::new("docker")
        .args(&args)
        .output()
        .context("Failed to execute 'docker run'")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);

        if stderr.contains("is already in use") {
            anyhow::bail!(
                "Container name 'rune-runtime' is already in use.\n\
                 Run `rune stop` first, or `docker rm -f rune-runtime`"
            );
        }
        if stderr.contains("address already in use") || stderr.contains("port is already allocated") {
            anyhow::bail!(
                "Port {} or {} is already in use.\n\
                 Use --http-port / --grpc-port to specify different ports",
                http_port, grpc_port
            );
        }

        anyhow::bail!("docker run failed: {}", stderr.trim());
    }

    let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(container_id)
}

/// Stop and remove a container by ID.
pub fn stop_container(container_id: &str, timeout: u64) -> Result<()> {
    let status = Command::new("docker")
        .args(["stop", "--time", &timeout.to_string(), container_id])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .context("Failed to execute 'docker stop'")?;

    if !status.success() {
        // Container may already be stopped — try rm anyway
        eprintln!("Warning: docker stop returned non-zero, attempting cleanup...");
    }

    // Remove the container
    let _ = Command::new("docker")
        .args(["rm", "-f", container_id])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}

/// Force-kill a container.
pub fn kill_container(container_id: &str) -> Result<()> {
    Command::new("docker")
        .args(["kill", container_id])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .context("Failed to execute 'docker kill'")?;

    let _ = Command::new("docker")
        .args(["rm", "-f", container_id])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    Ok(())
}

/// Check if a container is running.
pub fn is_container_running(container_id: &str) -> bool {
    Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", container_id])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
        .unwrap_or(false)
}
```

- [ ] **Step 2: 实现 Binary manager**

```rust
// runtime/rune-cli/src/runtime/binary.rs
use anyhow::{Context, Result};
use std::process::Command;

/// Check if a binary exists and is executable.
pub fn is_binary_available(path: &str) -> bool {
    std::fs::metadata(path)
        .map(|m| !m.is_dir())
        .unwrap_or(false)
}

/// Spawn the runtime binary in the background. Returns the child PID.
pub fn spawn_binary(path: &str, dev: bool, http_port: u16, grpc_port: u16) -> Result<u32> {
    let mut cmd = Command::new(path);

    if dev {
        cmd.arg("--dev");
    }

    cmd.env("RUNE_HTTP_PORT", http_port.to_string());
    cmd.env("RUNE_GRPC_PORT", grpc_port.to_string());

    // Detach from terminal
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    let child = cmd.spawn()
        .with_context(|| format!("Failed to start binary: {}", path))?;

    Ok(child.id())
}

/// Check whether a process with the given PID is alive.
pub fn is_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

/// Send SIGTERM to a process.
pub fn send_sigterm(pid: u32) -> Result<()> {
    send_signal(pid, libc::SIGTERM)
}

/// Send SIGKILL to a process.
pub fn send_sigkill(pid: u32) -> Result<()> {
    send_signal(pid, libc::SIGKILL)
}

fn send_signal(pid: u32, sig: i32) -> Result<()> {
    if pid == 0 {
        anyhow::bail!("Invalid PID: 0 would signal the entire process group");
    }
    #[cfg(unix)]
    {
        let ret = unsafe { libc::kill(pid as i32, sig) };
        if ret != 0 {
            anyhow::bail!(
                "kill({}, {}) failed: {}",
                pid, sig, std::io::Error::last_os_error()
            );
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (pid, sig);
        anyhow::bail!("Signal sending is not supported on this platform")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pid_zero_rejected() {
        assert!(!is_process_alive(0));
        assert!(send_sigterm(0).is_err());
    }

    #[test]
    fn test_own_process_alive() {
        let pid = std::process::id();
        assert!(is_process_alive(pid));
    }
}
```

- [ ] **Step 3: 更新 runtime/mod.rs**

```rust
// runtime/rune-cli/src/runtime/mod.rs
pub mod binary;
pub mod docker;
pub mod state;

use anyhow::Result;
use state::RuntimeState;

/// Wait for Runtime to become healthy by polling /health.
pub async fn wait_for_healthy(base_url: &str, timeout_secs: u64) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/health", base_url);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

    loop {
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "Runtime failed to start within {}s. Check logs with:\n  docker logs rune-runtime",
                timeout_secs
            );
        }

        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
}

/// Check if the recorded runtime is still alive.
pub fn is_runtime_alive(state: &RuntimeState) -> bool {
    match state.mode {
        state::RuntimeMode::Docker => {
            state.container_id.as_ref()
                .map(|id| docker::is_container_running(id))
                .unwrap_or(false)
        }
        state::RuntimeMode::Binary => {
            state.pid
                .map(|pid| binary::is_process_alive(pid))
                .unwrap_or(false)
        }
    }
}
```

- [ ] **Step 4: 运行测试**

Run: `cd /Users/chasey/cc/projects/rune && cargo test -p rune-cli binary::tests`
Expected: 2 个测试通过

- [ ] **Step 5: Commit**

```bash
git add runtime/rune-cli/src/runtime/
git commit -m "feat(cli): add Docker and Binary runtime managers + state module"
```

---

## Task 6: `rune start` 命令

**Files:**
- Modify: `runtime/rune-cli/src/commands/start.rs`

- [ ] **Step 1: 实现 start 命令**

```rust
// runtime/rune-cli/src/commands/start.rs
use anyhow::Result;
use crate::runtime::{self, docker, binary, state};

const DEFAULT_IMAGE: &str = "ghcr.io/chasey-myagi/rune-server";
const DEFAULT_TAG: &str = "latest";
const DEFAULT_HTTP_PORT: u16 = 50060;
const DEFAULT_GRPC_PORT: u16 = 50070;

pub async fn run(
    dev: bool,
    binary_path: Option<String>,
    image: Option<String>,
    tag: Option<String>,
    http_port: Option<u16>,
    grpc_port: Option<u16>,
    foreground: bool,
) -> Result<()> {
    let http_port = http_port.unwrap_or(DEFAULT_HTTP_PORT);
    let grpc_port = grpc_port.unwrap_or(DEFAULT_GRPC_PORT);

    // Check if already running
    if let Ok(Some(existing)) = state::read_state() {
        if runtime::is_runtime_alive(&existing) {
            let id_info = match existing.mode {
                state::RuntimeMode::Docker => {
                    format!("container {}", existing.container_id.as_deref().unwrap_or("?"))
                }
                state::RuntimeMode::Binary => {
                    format!("PID {}", existing.pid.unwrap_or(0))
                }
            };
            eprintln!("Runtime is already running ({}).", id_info);
            eprintln!("Run `rune stop` first to restart.");
            return Ok(());
        }
        // Stale state — clean up
        eprintln!("Cleaning up stale runtime state...");
        state::remove_state()?;
    }

    if let Some(ref bin) = binary_path {
        start_binary(bin, dev, http_port, grpc_port, foreground).await
    } else {
        start_docker(
            image.as_deref().unwrap_or(DEFAULT_IMAGE),
            tag.as_deref().unwrap_or(DEFAULT_TAG),
            dev, http_port, grpc_port,
        )
        .await
    }
}

async fn start_docker(
    image: &str, tag: &str, dev: bool, http_port: u16, grpc_port: u16,
) -> Result<()> {
    if !docker::is_docker_available() {
        anyhow::bail!(
            "Docker is not installed or not running.\n\
             Install Docker: https://docs.docker.com/get-docker/\n\
             Or use a local binary: rune start --binary /path/to/rune-server"
        );
    }

    eprintln!("Starting Runtime (docker)...");

    let container_id = docker::start_container(image, tag, http_port, grpc_port, dev)?;

    let rt_state = state::RuntimeState::new_docker(
        &container_id, http_port, grpc_port, dev,
    );
    state::write_state(&rt_state)?;

    let base_url = format!("http://127.0.0.1:{}", http_port);
    eprintln!("Waiting for Runtime to be ready...");

    match runtime::wait_for_healthy(&base_url, 30).await {
        Ok(()) => {
            eprintln!("Runtime started (docker: {}).", &container_id[..12.min(container_id.len())]);
            eprintln!("  HTTP: http://127.0.0.1:{}", http_port);
            eprintln!("  gRPC: 127.0.0.1:{}", grpc_port);
            if dev {
                eprintln!("  Mode: development (no auth)");
            }
            Ok(())
        }
        Err(e) => {
            // Cleanup on failure
            eprintln!("Startup failed, cleaning up...");
            let _ = docker::stop_container(&container_id, 5);
            state::remove_state()?;
            Err(e)
        }
    }
}

async fn start_binary(
    path: &str, dev: bool, http_port: u16, grpc_port: u16, foreground: bool,
) -> Result<()> {
    if !binary::is_binary_available(path) {
        anyhow::bail!("Binary not found: {}", path);
    }

    if foreground {
        eprintln!("Starting Runtime (binary, foreground)...");
        eprintln!("Press Ctrl+C to stop.");

        // In foreground mode, exec directly — don't write state
        let status = tokio::process::Command::new(path)
            .args(if dev { vec!["--dev"] } else { vec![] })
            .env("RUNE_HTTP_PORT", http_port.to_string())
            .env("RUNE_GRPC_PORT", grpc_port.to_string())
            .status()
            .await?;

        if !status.success() {
            anyhow::bail!("Runtime exited with status: {}", status);
        }
        return Ok(());
    }

    eprintln!("Starting Runtime (binary)...");

    let pid = binary::spawn_binary(path, dev, http_port, grpc_port)?;

    let rt_state = state::RuntimeState::new_binary(pid, http_port, grpc_port, dev);
    state::write_state(&rt_state)?;

    let base_url = format!("http://127.0.0.1:{}", http_port);
    eprintln!("Waiting for Runtime to be ready...");

    match runtime::wait_for_healthy(&base_url, 30).await {
        Ok(()) => {
            eprintln!("Runtime started (PID {}).", pid);
            eprintln!("  HTTP: http://127.0.0.1:{}", http_port);
            eprintln!("  gRPC: 127.0.0.1:{}", grpc_port);
            Ok(())
        }
        Err(e) => {
            eprintln!("Startup failed, cleaning up...");
            let _ = binary::send_sigterm(pid);
            state::remove_state()?;
            Err(e)
        }
    }
}
```

- [ ] **Step 2: 编译验证**

Run: `cd /Users/chasey/cc/projects/rune && cargo check -p rune-cli`
Expected: 编译通过

- [ ] **Step 3: Commit**

```bash
git add runtime/rune-cli/src/commands/start.rs
git commit -m "feat(cli): implement rune start with Docker-first + binary fallback"
```

---

## Task 7: `rune stop` 命令

**Files:**
- Modify: `runtime/rune-cli/src/commands/stop.rs`

- [ ] **Step 1: 实现 stop 命令**

```rust
// runtime/rune-cli/src/commands/stop.rs
use anyhow::Result;
use crate::runtime::{self, binary, docker, state};

pub async fn run(force: bool, timeout: u64) -> Result<()> {
    let current = match state::read_state()? {
        Some(s) => s,
        None => {
            eprintln!("Runtime is not running (no state file found).");
            return Ok(());
        }
    };

    if !runtime::is_runtime_alive(&current) {
        eprintln!("Runtime is not running (stale state). Cleaning up...");
        state::remove_state()?;
        return Ok(());
    }

    match current.mode {
        state::RuntimeMode::Docker => {
            let cid = current.container_id.as_deref().unwrap_or("unknown");
            if force {
                eprintln!("Force-stopping Runtime (container {})...", &cid[..12.min(cid.len())]);
                docker::kill_container(cid)?;
            } else {
                eprintln!("Stopping Runtime (container {})...", &cid[..12.min(cid.len())]);
                docker::stop_container(cid, timeout)?;
            }
        }
        state::RuntimeMode::Binary => {
            let pid = current.pid.unwrap_or(0);
            if force {
                eprintln!("Force-stopping Runtime (PID {})...", pid);
                binary::send_sigkill(pid)?;
            } else {
                eprintln!("Stopping Runtime (PID {})...", pid);
                binary::send_sigterm(pid)?;

                // Wait for process to exit
                let deadline = tokio::time::Instant::now()
                    + std::time::Duration::from_secs(timeout);

                loop {
                    if !binary::is_process_alive(pid) {
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        eprintln!("Graceful shutdown timed out after {}s. Sending SIGKILL...", timeout);
                        binary::send_sigkill(pid)?;
                        // Brief wait for SIGKILL to take effect
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
    }

    state::remove_state()?;
    eprintln!("Runtime stopped.");
    Ok(())
}
```

- [ ] **Step 2: 编译验证**

Run: `cd /Users/chasey/cc/projects/rune && cargo check -p rune-cli`
Expected: 编译通过

- [ ] **Step 3: Commit**

```bash
git add runtime/rune-cli/src/commands/stop.rs
git commit -m "feat(cli): implement rune stop with graceful shutdown + SIGKILL fallback"
```

---

## Task 8: `rune status` 命令

**Files:**
- Modify: `runtime/rune-cli/src/commands/status.rs`

- [ ] **Step 1: 实现 status 命令**

```rust
// runtime/rune-cli/src/commands/status.rs
use anyhow::Result;
use serde_json::json;
use crate::client::RuneClient;
use crate::runtime::{self, state};

pub async fn run(client: &RuneClient, _watch: bool, json_mode: bool) -> Result<()> {
    // Local state info
    let local_state = state::read_state()?;
    let is_alive = local_state.as_ref().map(|s| runtime::is_runtime_alive(s)).unwrap_or(false);

    // Remote API info
    let api_status = client.status().await.ok();
    let api_reachable = api_status.is_some();

    if json_mode {
        let result = json!({
            "running": is_alive || api_reachable,
            "local_state": local_state.as_ref().map(|s| json!({
                "mode": format!("{:?}", s.mode).to_lowercase(),
                "container_id": s.container_id,
                "pid": s.pid,
                "http_port": s.http_port,
                "grpc_port": s.grpc_port,
                "started_at": s.started_at.to_rfc3339(),
                "dev_mode": s.dev_mode,
            })),
            "api": api_status,
        });
        crate::output::print_json(&result);
        return Ok(());
    }

    // Text output
    match (&local_state, is_alive) {
        (Some(s), true) => {
            let id_info = match s.mode {
                state::RuntimeMode::Docker => {
                    format!("docker: {}", s.container_id.as_deref().unwrap_or("?"))
                }
                state::RuntimeMode::Binary => {
                    format!("PID {}", s.pid.unwrap_or(0))
                }
            };
            let elapsed = chrono::Utc::now().signed_duration_since(s.started_at);
            let uptime = format_duration(elapsed);

            println!("Runtime:    running ({})", id_info);
            println!("Uptime:     {}", uptime);
            println!("HTTP:       http://127.0.0.1:{}", s.http_port);
            println!("gRPC:       127.0.0.1:{}", s.grpc_port);
            if s.dev_mode {
                println!("Mode:       development");
            }
        }
        (Some(_), false) => {
            println!("Runtime:    not running (stale state)");
            eprintln!("Hint: Run `rune stop` to clean up, then `rune start`");
        }
        (None, _) => {
            if api_reachable {
                println!("Runtime:    running (remote: {})", client.base_url);
            } else {
                println!("Runtime:    not running");
                eprintln!("\nStart with: rune start");
                return Ok(());
            }
        }
    }

    // API details
    if let Some(status) = api_status {
        println!();
        if let Some(runes) = status.get("runes_count") {
            println!("Runes:      {}", runes);
        }
        if let Some(casters) = status.get("casters_count") {
            println!("Casters:    {}", casters);
        }
        if let Some(version) = status.get("version") {
            println!("Version:    {}", version);
        }
    } else if is_alive {
        println!();
        eprintln!("API:        unreachable ({})", client.base_url);
    }

    Ok(())
}

fn format_duration(d: chrono::Duration) -> String {
    let total_secs = d.num_seconds();
    if total_secs < 60 {
        format!("{}s", total_secs)
    } else if total_secs < 3600 {
        format!("{}m {}s", total_secs / 60, total_secs % 60)
    } else {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    }
}
```

- [ ] **Step 2: 编译验证**

Run: `cd /Users/chasey/cc/projects/rune && cargo check -p rune-cli`
Expected: 编译通过

- [ ] **Step 3: Commit**

```bash
git add runtime/rune-cli/src/commands/status.rs
git commit -m "feat(cli): implement rune status with local state + API health"
```

---

## Task 9: `rune list` 命令 — 表格输出

**Files:**
- Modify: `runtime/rune-cli/src/commands/list.rs`
- Modify: `runtime/rune-cli/src/output.rs`

- [ ] **Step 1: 扩展 output.rs — 增加表格辅助**

```rust
// runtime/rune-cli/src/output.rs
use comfy_table::{Table, ContentArrangement, presets};
use serde_json::Value;

/// Print JSON value in pretty format to stdout.
pub fn print_json(value: &Value) {
    println!("{}", serde_json::to_string_pretty(value).expect("JSON serialization failed"));
}

/// Create a styled table with headers.
pub fn new_table(headers: &[&str]) -> Table {
    let mut table = Table::new();
    table
        .load_preset(presets::NOTHING)
        .set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(headers.iter().map(|h| h.to_uppercase()).collect::<Vec<_>>());
    table
}
```

- [ ] **Step 2: 实现 list 命令**

```rust
// runtime/rune-cli/src/commands/list.rs
use anyhow::Result;
use crate::client::RuneClient;
use crate::output;

pub async fn run(client: &RuneClient, _all: bool, json_mode: bool) -> Result<()> {
    let result = client.list_runes().await?;

    if json_mode {
        output::print_json(&result);
        return Ok(());
    }

    // Parse as array
    let runes = match result.as_array() {
        Some(arr) => arr,
        None => {
            // Might be wrapped in { "runes": [...] }
            result.get("runes")
                .and_then(|v| v.as_array())
                .unwrap_or(&vec![])
        }
    };

    if runes.is_empty() {
        println!("No runes registered.");
        return Ok(());
    }

    let mut table = output::new_table(&["Name", "Version", "Mode", "Gate Path", "Caster", "Status"]);

    for rune in runes {
        table.add_row(vec![
            rune.get("name").and_then(|v| v.as_str()).unwrap_or("-"),
            rune.get("version").and_then(|v| v.as_str()).unwrap_or("-"),
            rune.get("mode").and_then(|v| v.as_str()).unwrap_or("unary"),
            rune.get("gate_path").or(rune.get("gate")).and_then(|v| v.as_str()).unwrap_or("-"),
            rune.get("caster_id").or(rune.get("caster")).and_then(|v| v.as_str()).unwrap_or("-"),
            "online",
        ]);
    }

    println!("{table}");
    println!("\n{} rune(s) registered", runes.len());
    Ok(())
}
```

- [ ] **Step 3: 编译验证**

Run: `cd /Users/chasey/cc/projects/rune && cargo check -p rune-cli`
Expected: 编译通过

- [ ] **Step 4: Commit**

```bash
git add runtime/rune-cli/src/output.rs runtime/rune-cli/src/commands/list.rs
git commit -m "feat(cli): implement rune list with table output"
```

---

## Task 10: `rune call` 命令 — 完整实现

**Files:**
- Modify: `runtime/rune-cli/src/commands/call.rs`

- [ ] **Step 1: 编写 input 解析测试**

在 `src/commands/call.rs` 底部添加：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_input_valid_json() {
        let result = resolve_input(Some(r#"{"key":"val"}"#), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), r#"{"key":"val"}"#);
    }

    #[test]
    fn test_resolve_input_invalid_json_errors() {
        let result = resolve_input(Some("{bad json"), None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid JSON input"), "got: {}", err);
    }

    #[test]
    fn test_resolve_input_none_defaults_to_empty_object() {
        let result = resolve_input(None, None).unwrap();
        assert_eq!(result, "{}");
    }

    #[test]
    fn test_resolve_input_plain_string_errors() {
        let result = resolve_input(Some("hello"), None);
        assert!(result.is_err(), "plain string should not be valid JSON input");
    }

    #[test]
    fn test_resolve_input_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.json");
        std::fs::write(&path, r#"{"from":"file"}"#).unwrap();
        let result = resolve_input(None, Some(path.to_str().unwrap())).unwrap();
        assert_eq!(result, r#"{"from":"file"}"#);
    }

    #[test]
    fn test_resolve_input_file_not_found() {
        let result = resolve_input(None, Some("/nonexistent/file.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_input_file_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, "not json").unwrap();
        let result = resolve_input(None, Some(path.to_str().unwrap()));
        assert!(result.is_err());
    }
}
```

- [ ] **Step 2: 运行测试**

Run: `cd /Users/chasey/cc/projects/rune && cargo test -p rune-cli call::tests`
Expected: 7 个测试通过

- [ ] **Step 3: Commit**

```bash
git add runtime/rune-cli/src/commands/call.rs
git commit -m "feat(cli): implement rune call with strict JSON validation"
```

---

## Task 11: 全量编译 + 集成检查

**Files:** 无新文件，验证全部工作正常

- [ ] **Step 1: 编译整个 workspace**

Run: `cd /Users/chasey/cc/projects/rune && cargo build -p rune-cli`
Expected: 编译成功

- [ ] **Step 2: 运行所有测试**

Run: `cd /Users/chasey/cc/projects/rune && cargo test -p rune-cli`
Expected: 所有测试通过（CLI 解析 ~35 + state 3 + binary 2 + call 7 + config 1 + client 2 = ~50 个）

- [ ] **Step 3: 验证 CLI help 输出**

Run: `cd /Users/chasey/cc/projects/rune && cargo run -p rune-cli -- --help`
Expected: 显示新命令列表（start/stop/status/list/call/task/casters/key/flow/logs/stats/config）

Run: `cd /Users/chasey/cc/projects/rune && cargo run -p rune-cli -- call --help`
Expected: 显示 --stream 和 --async 互斥说明

Run: `cd /Users/chasey/cc/projects/rune && cargo run -p rune-cli -- --version`
Expected: 显示 `rune 0.2.0`

- [ ] **Step 4: 验证互斥**

Run: `cd /Users/chasey/cc/projects/rune && cargo run -p rune-cli -- call echo --stream --async 2>&1`
Expected: clap 报错，提示 --stream 和 --async 不能同时使用

- [ ] **Step 5: Commit**

```bash
git add -A runtime/rune-cli/
git commit -m "feat(cli): M1 complete — new command structure, runtime lifecycle, strict call"
```

---

## Summary

| Task | 内容 | 预估测试数 |
|------|------|-----------|
| 1 | Cargo.toml 依赖 + 版本号 | 0 |
| 2 | lib.rs 提取 + 新命令结构 + 所有模块占位 | 0 |
| 3 | CLI 解析测试（新结构全覆盖） | ~35 |
| 4 | RuntimeState 模块（state.json） | 3 |
| 5 | Docker + Binary manager | 2 |
| 6 | `rune start`（Docker-first + binary） | 0（E2E 在 M3） |
| 7 | `rune stop`（graceful + SIGKILL） | 0（E2E 在 M3） |
| 8 | `rune status`（local + API） | 0 |
| 9 | `rune list`（表格输出） | 0 |
| 10 | `rune call`（strict JSON + 互斥） | 7 |
| 11 | 全量编译 + 集成检查 | 验证 ~50 |

M1 完成后的 CLI 能力：
- ✅ `rune start` 实际启动 Runtime（Docker 或 binary）
- ✅ `rune stop` 优雅关闭 + 超时 SIGKILL + state 清理
- ✅ `rune status` 显示本地 + API 状态
- ✅ `rune list` 表格输出已注册 Rune
- ✅ `rune call` 严格 JSON 校验 + --stream/--async 互斥
- ✅ 全局 `--json` / `--quiet` / `--remote` flag
- ✅ 其他命令（key/flow/logs/stats/config/task/casters）有占位实现，JSON 输出可用
