use clap::Parser;
use rune_cli::{Cli, Commands, ConfigCommands, FlowCommands, KeyCommands, TaskCommands};

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
        Commands::Start {
            dev,
            binary,
            foreground,
            http_port,
            grpc_port,
            ..
        } => {
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
        Commands::Start {
            http_port,
            grpc_port,
            ..
        } => {
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
        Commands::Call {
            name,
            input,
            stream,
            async_mode,
            ..
        } => {
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
        Commands::Call {
            stream,
            async_mode,
            ..
        } => {
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
        Commands::Call {
            stream,
            async_mode,
            ..
        } => {
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
        Commands::Logs {
            rune,
            limit,
            follow,
        } => {
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
        Commands::Logs {
            rune,
            limit,
            follow,
        } => {
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
    assert!(matches!(
        cli.command,
        Commands::Config(ConfigCommands::Init)
    ));
}

#[test]
fn config_show() {
    let cli = parse(&["config", "show"]);
    assert!(matches!(
        cli.command,
        Commands::Config(ConfigCommands::Show)
    ));
}

#[test]
fn config_path() {
    let cli = parse(&["config", "path"]);
    assert!(matches!(
        cli.command,
        Commands::Config(ConfigCommands::Path)
    ));
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
    let cli = parse(&[
        "--remote",
        "http://10.0.0.1:8080",
        "call",
        "echo",
        r#"{"x":1}"#,
    ]);
    assert_eq!(cli.remote.as_deref(), Some("http://10.0.0.1:8080"));
    match cli.command {
        Commands::Call { name, input, .. } => {
            assert_eq!(name, "echo");
            assert_eq!(input.as_deref(), Some(r#"{"x":1}"#));
        }
        _ => panic!("expected Call"),
    }
}
