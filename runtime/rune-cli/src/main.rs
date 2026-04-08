use clap::Parser;
use rune_cli::{Cli, Commands, ConfigCommands, FlowCommands, KeyCommands, TaskCommands};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Resolve base URL
    let base_url = cli.remote.as_deref().unwrap_or("http://127.0.0.1:50060");

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
            rune_cli::commands::start::run(
                dev, binary, image, tag, http_port, grpc_port, foreground,
            )
            .await
        }
        Commands::Stop { force, timeout } => rune_cli::commands::stop::run(force, timeout).await,
        Commands::Status => rune_cli::commands::status::run(&client, json_mode).await,
        Commands::List => rune_cli::commands::list::run(&client, json_mode).await,
        Commands::Call {
            name,
            input,
            stream,
            async_mode,
            input_file,
            timeout,
        } => {
            rune_cli::commands::call::run(
                &client,
                &name,
                input.as_deref(),
                input_file.as_deref(),
                stream,
                async_mode,
                timeout,
                json_mode,
            )
            .await
        }
        Commands::Task(cmd) => match cmd {
            TaskCommands::Get { id } => {
                rune_cli::commands::task::get(&client, &id, json_mode).await
            }
            TaskCommands::List {
                status,
                rune,
                limit,
            } => {
                rune_cli::commands::task::list(
                    &client,
                    status.as_deref(),
                    rune.as_deref(),
                    limit,
                    json_mode,
                )
                .await
            }
            TaskCommands::Wait { id, timeout } => {
                rune_cli::commands::task::wait(&client, &id, timeout, json_mode).await
            }
            TaskCommands::Delete { id } => {
                rune_cli::commands::task::delete(&client, &id, json_mode).await
            }
        },
        Commands::Casters => rune_cli::commands::casters::run(&client, json_mode).await,
        Commands::Key(cmd) => match cmd {
            KeyCommands::Create { key_type, label } => {
                rune_cli::commands::key::create(&client, &key_type, &label, json_mode).await
            }
            KeyCommands::List => rune_cli::commands::key::list(&client, json_mode).await,
            KeyCommands::Revoke { key_id } => {
                rune_cli::commands::key::revoke(&client, &key_id, json_mode).await
            }
            KeyCommands::Bootstrap {
                label,
                db_path,
                force,
            } => rune_cli::commands::key::bootstrap(&db_path, &label, force, json_mode).await,
        },
        Commands::Flow(cmd) => match cmd {
            FlowCommands::Register { file } => {
                rune_cli::commands::flow::register(&client, &file, json_mode).await
            }
            FlowCommands::List => rune_cli::commands::flow::list(&client, json_mode).await,
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
        Commands::Logs { rune, limit } => {
            rune_cli::commands::logs::run(&client, rune.as_deref(), limit, json_mode).await
        }
        Commands::Stats => rune_cli::commands::logs::stats(&client, json_mode).await,
        Commands::Config(cmd) => match cmd {
            ConfigCommands::Init => rune_cli::commands::config::init().await,
            ConfigCommands::Show => rune_cli::commands::config::show().await,
            ConfigCommands::Path => rune_cli::commands::config::path().await,
        },
    }
}
