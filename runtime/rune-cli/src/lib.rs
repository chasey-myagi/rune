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
