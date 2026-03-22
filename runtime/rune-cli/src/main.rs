use clap::{Parser, Subcommand};

pub mod client;
pub mod commands;
pub mod daemon;

#[derive(Parser, Debug)]
#[command(name = "rune", about = "Rune Runtime CLI")]
pub struct Cli {
    /// Connect to a remote Runtime instance
    #[arg(long, global = true)]
    pub remote: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the Runtime
    Start {
        /// Enable development mode
        #[arg(long)]
        dev: bool,

        /// Path to configuration file
        #[arg(long)]
        config: Option<String>,
    },

    /// Stop the background Runtime
    Stop,

    /// Show Runtime status
    Status,

    /// List online Runes
    List,

    /// Call a Rune
    Call {
        /// Name of the Rune to call
        name: String,

        /// Input JSON
        input: Option<String>,

        /// Enable streaming mode
        #[arg(long)]
        stream: bool,

        /// Enable async mode
        #[arg(long = "async")]
        async_mode: bool,
    },

    /// Query an async task
    Task {
        /// Task ID
        id: String,
    },

    /// Key management
    #[command(subcommand)]
    Key(KeyCommands),

    /// Flow orchestration
    #[command(subcommand)]
    Flow(FlowCommands),

    /// View logs
    Logs {
        /// Filter by Rune name
        #[arg(long)]
        rune: Option<String>,

        /// Number of log entries to show
        #[arg(long, default_value = "50")]
        limit: u32,
    },

    /// Show runtime statistics
    Stats,

    /// Configuration management
    #[command(subcommand)]
    Config(ConfigCommands),
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
        /// Path to flow definition file
        file: String,
    },

    /// List all registered flows
    List,

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
    /// Generate default configuration
    Init,

    /// Show current configuration
    Show,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Resolve base URL: --remote flag > RUNE_ADDR env > default
    let env_addr = std::env::var("RUNE_ADDR").ok();
    let base_url = cli
        .remote
        .as_deref()
        .or(env_addr.as_deref())
        .unwrap_or("http://127.0.0.1:50060");

    // API key from RUNE_KEY env var
    let api_key = std::env::var("RUNE_KEY").ok();

    let client = client::RuneClient::new(base_url, api_key.as_deref());

    match cli.command {
        Commands::Start { dev, config } => {
            commands::runtime::start(dev, config.as_deref()).await
        }
        Commands::Stop => commands::runtime::stop().await,
        Commands::Status => commands::runtime::status(&client).await,
        Commands::List => commands::rune::list(&client).await,
        Commands::Call {
            name,
            input,
            stream,
            async_mode,
        } => commands::rune::call(&client, &name, input.as_deref(), stream, async_mode).await,
        Commands::Task { id } => commands::rune::task(&client, &id).await,
        Commands::Key(cmd) => match cmd {
            KeyCommands::Create { key_type, label } => {
                commands::key::create(&client, &key_type, &label).await
            }
            KeyCommands::List => commands::key::list(&client).await,
            KeyCommands::Revoke { key_id } => commands::key::revoke(&client, &key_id).await,
        },
        Commands::Flow(cmd) => match cmd {
            FlowCommands::Register { file } => commands::flow::register(&client, &file).await,
            FlowCommands::List => commands::flow::list(&client).await,
            FlowCommands::Run { name, input } => {
                commands::flow::run(&client, &name, input.as_deref()).await
            }
            FlowCommands::Delete { name } => commands::flow::delete(&client, &name).await,
        },
        Commands::Logs { rune, limit } => {
            commands::logs::logs(&client, rune.as_deref(), limit).await
        }
        Commands::Stats => commands::logs::stats(&client).await,
        Commands::Config(cmd) => match cmd {
            ConfigCommands::Init => commands::config::init().await,
            ConfigCommands::Show => commands::config::show().await,
        },
    }
}
