use clap::Parser;

// Re-use the CLI types from the binary crate.
// We need to import from the crate root, which is exposed as a library
// via the `pub mod` declarations in main.rs.
// Since rune-cli is a binary-only crate, we reproduce the CLI types here
// to test parsing independently.

#[path = "../src/main.rs"]
#[allow(dead_code)]
mod cli;

use cli::{Cli, Commands, ConfigCommands, FlowCommands, KeyCommands};

/// Helper: parse a command line into Cli, panicking on failure.
fn parse(args: &[&str]) -> Cli {
    let mut full_args = vec!["rune"];
    full_args.extend_from_slice(args);
    Cli::parse_from(full_args)
}

// ── Start ───────────────────────────────────────────────────────────────

#[test]
fn test_start_default() {
    let cli = parse(&["start"]);
    match cli.command {
        Commands::Start { dev, config } => {
            assert!(!dev);
            assert!(config.is_none());
        }
        _ => panic!("expected Start command"),
    }
}

#[test]
fn test_start_dev() {
    let cli = parse(&["start", "--dev"]);
    match cli.command {
        Commands::Start { dev, .. } => assert!(dev),
        _ => panic!("expected Start command"),
    }
}

#[test]
fn test_start_config() {
    let cli = parse(&["start", "--config", "path.toml"]);
    match cli.command {
        Commands::Start { config, .. } => {
            assert_eq!(config.as_deref(), Some("path.toml"));
        }
        _ => panic!("expected Start command"),
    }
}

// ── Stop / Status ───────────────────────────────────────────────────────

#[test]
fn test_stop() {
    let cli = parse(&["stop"]);
    assert!(matches!(cli.command, Commands::Stop));
}

#[test]
fn test_status() {
    let cli = parse(&["status"]);
    assert!(matches!(cli.command, Commands::Status));
}

// ── List ────────────────────────────────────────────────────────────────

#[test]
fn test_list() {
    let cli = parse(&["list"]);
    assert!(matches!(cli.command, Commands::List));
}

// ── Call ────────────────────────────────────────────────────────────────

#[test]
fn test_call_with_input() {
    let cli = parse(&["call", "echo", r#"{"key":"val"}"#]);
    match cli.command {
        Commands::Call {
            name,
            input,
            stream,
            async_mode,
        } => {
            assert_eq!(name, "echo");
            assert_eq!(input.as_deref(), Some(r#"{"key":"val"}"#));
            assert!(!stream);
            assert!(!async_mode);
        }
        _ => panic!("expected Call command"),
    }
}

#[test]
fn test_call_stream() {
    let cli = parse(&["call", "echo", "--stream"]);
    match cli.command {
        Commands::Call { name, stream, .. } => {
            assert_eq!(name, "echo");
            assert!(stream);
        }
        _ => panic!("expected Call command"),
    }
}

#[test]
fn test_call_async() {
    let cli = parse(&["call", "echo", "--async"]);
    match cli.command {
        Commands::Call {
            name, async_mode, ..
        } => {
            assert_eq!(name, "echo");
            assert!(async_mode);
        }
        _ => panic!("expected Call command"),
    }
}

// ── Task ────────────────────────────────────────────────────────────────

#[test]
fn test_task() {
    let cli = parse(&["task", "abc-123"]);
    match cli.command {
        Commands::Task { id } => assert_eq!(id, "abc-123"),
        _ => panic!("expected Task command"),
    }
}

// ── Key ─────────────────────────────────────────────────────────────────

#[test]
fn test_key_create() {
    let cli = parse(&["key", "create", "--type", "gate", "--label", "test"]);
    match cli.command {
        Commands::Key(KeyCommands::Create { key_type, label }) => {
            assert_eq!(key_type, "gate");
            assert_eq!(label, "test");
        }
        _ => panic!("expected Key Create command"),
    }
}

#[test]
fn test_key_list() {
    let cli = parse(&["key", "list"]);
    assert!(matches!(cli.command, Commands::Key(KeyCommands::List)));
}

#[test]
fn test_key_revoke() {
    let cli = parse(&["key", "revoke", "key-id"]);
    match cli.command {
        Commands::Key(KeyCommands::Revoke { key_id }) => {
            assert_eq!(key_id, "key-id");
        }
        _ => panic!("expected Key Revoke command"),
    }
}

// ── Flow ────────────────────────────────────────────────────────────────

#[test]
fn test_flow_register() {
    let cli = parse(&["flow", "register", "flow.yaml"]);
    match cli.command {
        Commands::Flow(FlowCommands::Register { file }) => {
            assert_eq!(file, "flow.yaml");
        }
        _ => panic!("expected Flow Register command"),
    }
}

#[test]
fn test_flow_list() {
    let cli = parse(&["flow", "list"]);
    assert!(matches!(
        cli.command,
        Commands::Flow(FlowCommands::List)
    ));
}

#[test]
fn test_flow_run() {
    let cli = parse(&["flow", "run", "pipeline", r#"{"input":"test"}"#]);
    match cli.command {
        Commands::Flow(FlowCommands::Run { name, input }) => {
            assert_eq!(name, "pipeline");
            assert_eq!(input.as_deref(), Some(r#"{"input":"test"}"#));
        }
        _ => panic!("expected Flow Run command"),
    }
}

#[test]
fn test_flow_delete() {
    let cli = parse(&["flow", "delete", "pipeline"]);
    match cli.command {
        Commands::Flow(FlowCommands::Delete { name }) => {
            assert_eq!(name, "pipeline");
        }
        _ => panic!("expected Flow Delete command"),
    }
}

// ── Logs ────────────────────────────────────────────────────────────────

#[test]
fn test_logs_default_limit() {
    let cli = parse(&["logs"]);
    match cli.command {
        Commands::Logs { rune, limit } => {
            assert!(rune.is_none());
            assert_eq!(limit, 50);
        }
        _ => panic!("expected Logs command"),
    }
}

#[test]
fn test_logs_with_options() {
    let cli = parse(&["logs", "--rune", "echo", "--limit", "100"]);
    match cli.command {
        Commands::Logs { rune, limit } => {
            assert_eq!(rune.as_deref(), Some("echo"));
            assert_eq!(limit, 100);
        }
        _ => panic!("expected Logs command"),
    }
}

// ── Stats ───────────────────────────────────────────────────────────────

#[test]
fn test_stats() {
    let cli = parse(&["stats"]);
    assert!(matches!(cli.command, Commands::Stats));
}

// ── Config ──────────────────────────────────────────────────────────────

#[test]
fn test_config_init() {
    let cli = parse(&["config", "init"]);
    assert!(matches!(
        cli.command,
        Commands::Config(ConfigCommands::Init)
    ));
}

#[test]
fn test_config_show() {
    let cli = parse(&["config", "show"]);
    assert!(matches!(
        cli.command,
        Commands::Config(ConfigCommands::Show)
    ));
}

// ── Remote flag ─────────────────────────────────────────────────────────

#[test]
fn test_remote_flag() {
    let cli = parse(&["--remote", "https://rune.example.com", "status"]);
    assert_eq!(cli.remote.as_deref(), Some("https://rune.example.com"));
    assert!(matches!(cli.command, Commands::Status));
}

// ── Additional edge-case tests ──────────────────────────────────────────

#[test]
fn test_call_name_only() {
    let cli = parse(&["call", "my-rune"]);
    match cli.command {
        Commands::Call {
            name,
            input,
            stream,
            async_mode,
        } => {
            assert_eq!(name, "my-rune");
            assert!(input.is_none());
            assert!(!stream);
            assert!(!async_mode);
        }
        _ => panic!("expected Call command"),
    }
}

#[test]
fn test_flow_run_no_input() {
    let cli = parse(&["flow", "run", "pipeline"]);
    match cli.command {
        Commands::Flow(FlowCommands::Run { name, input }) => {
            assert_eq!(name, "pipeline");
            assert!(input.is_none());
        }
        _ => panic!("expected Flow Run command"),
    }
}

#[test]
fn test_remote_with_call() {
    let cli = parse(&["--remote", "http://10.0.0.1:8080", "call", "echo", r#"{"x":1}"#]);
    assert_eq!(cli.remote.as_deref(), Some("http://10.0.0.1:8080"));
    match cli.command {
        Commands::Call { name, input, .. } => {
            assert_eq!(name, "echo");
            assert_eq!(input.as_deref(), Some(r#"{"x":1}"#));
        }
        _ => panic!("expected Call command"),
    }
}
