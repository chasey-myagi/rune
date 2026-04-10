use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use fs2::FileExt;
use rune_proto::rune_service_client::RuneServiceClient;
use rune_proto::{
    session_message, CasterAttach, Heartbeat, ScaleAction, ScaleSignal, SessionMessage,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::sync::{mpsc, watch, Mutex};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PilotRegistration {
    caster_id: String,
    pid: u32,
    /// Process start time captured at registration.  Used together with
    /// `pid` to form a unique identity — guards against PID reuse.
    #[serde(default)]
    start_time: Option<u64>,
    group: String,
    spawn_command: String,
    shutdown_signal: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum PilotRequest {
    Register {
        caster_id: String,
        pid: u32,
        group: String,
        spawn_command: String,
        shutdown_signal: String,
    },
    Deregister {
        caster_id: String,
    },
    Status,
    Stop,
}

#[derive(Debug, Serialize, Deserialize)]
struct PilotResponse {
    ok: bool,
    pilot_id: String,
    runtime: String,
    casters: Vec<PilotRegistration>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
enum PilotRuntimeState {
    Connecting,
    Ready,
    Failed(String),
}

impl PilotRuntimeState {
    fn into_status_fields(self) -> (bool, Option<String>) {
        match self {
            Self::Ready => (true, None),
            Self::Connecting => (false, Some("runtime session not attached".into())),
            Self::Failed(error) => (false, Some(error)),
        }
    }
}

pub async fn run_daemon(runtime: &str, json_mode: bool) -> Result<()> {
    let paths = pilot_paths()?;
    std::fs::create_dir_all(&paths.base_dir)?;
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(lock_path()?)?;
    if let Err(err) = lock_file.try_lock_exclusive() {
        if pid_alive(&paths.pid) {
            if !json_mode {
                println!("pilot already running");
            }
            return Ok(());
        }
        return Err(err).context("failed to acquire pilot lock");
    }
    if paths.socket.exists() {
        let _ = std::fs::remove_file(&paths.socket);
    }

    let listener = UnixListener::bind(&paths.socket)
        .with_context(|| format!("failed to bind pilot socket at {}", paths.socket.display()))?;
    std::fs::write(&paths.pid, std::process::id().to_string())?;

    let runtime = normalize_runtime(runtime);
    let pilot_id = format!("pilot-{}", std::process::id());
    let registry = Arc::new(Mutex::new(HashMap::<String, PilotRegistration>::new()));
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (runtime_state_tx, runtime_state_rx) = watch::channel(PilotRuntimeState::Connecting);
    let session_exit_notify = Arc::new(tokio::sync::Notify::new());
    let session_runtime = runtime.clone();
    let session_pilot_id = pilot_id.clone();
    let session_registry = Arc::clone(&registry);

    let session_task = tokio::spawn({
        let runtime_state_tx = runtime_state_tx.clone();
        let mut shutdown_watch = shutdown_rx.clone();
        let session_exit = Arc::clone(&session_exit_notify);
        async move {
            let base_delay = Duration::from_millis(500);
            let max_delay = Duration::from_secs(30);
            let mut attempts: u32 = 0;

            loop {
                let _ = runtime_state_tx.send(PilotRuntimeState::Connecting);
                let result = run_pilot_session(
                    session_runtime.clone(),
                    session_pilot_id.clone(),
                    Arc::clone(&session_registry),
                    shutdown_watch.clone(),
                    runtime_state_tx.clone(),
                )
                .await;

                // If shutdown was requested, exit the loop.
                if *shutdown_watch.borrow() {
                    session_exit.notify_one();
                    break;
                }

                // Terminal errors (e.g. attach rejected) should not be retried.
                if let Err(err) = &result {
                    let msg = err.to_string();
                    if msg.contains("attach rejected") {
                        let _ = runtime_state_tx.send(PilotRuntimeState::Failed(msg));
                        session_exit.notify_one();
                        break;
                    }
                }

                // Transient failure — we will retry, so advertise Connecting
                // (not Failed) so SDK clients classify this as retryable.
                let _ = runtime_state_tx.send(PilotRuntimeState::Connecting);

                attempts = attempts.saturating_add(1);
                let delay = std::cmp::min(
                    base_delay.saturating_mul(1u32.wrapping_shl(attempts.min(6))),
                    max_delay,
                );

                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    changed = shutdown_watch.changed() => {
                        if changed.is_ok() && *shutdown_watch.borrow() {
                            break;
                        }
                    }
                }
            }
        }
    });

    let reaper_registry = Arc::clone(&registry);
    let reaper_shutdown = shutdown_tx.clone();
    let reaper_task = tokio::spawn(async move {
        // Grace period: give the first caster time to discover the pilot
        // and call register() before we start checking for emptiness.
        // If session_task exits early (e.g. attach rejected), wake up
        // immediately instead of sleeping the full 15 seconds.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(15)) => {}
            _ = session_exit_notify.notified() => {}
        }

        loop {
            let mut registrations = reaper_registry.lock().await;
            registrations.retain(|_, registration| {
                process_alive_strict(registration.pid, registration.start_time)
            });
            if registrations.is_empty() {
                let _ = reaper_shutdown.send(true);
                break;
            }
            drop(registrations);
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    break;
                }
            }
            accept = listener.accept() => {
                let (mut stream, _) = accept?;
                let response = handle_client(
                    &mut stream,
                    &pilot_id,
                    &runtime,
                    Arc::clone(&registry),
                    shutdown_tx.clone(),
                    runtime_state_rx.clone(),
                ).await;
                let payload = serde_json::to_vec(&response?)?;
                stream.write_all(&payload).await?;
            }
        }
    }

    session_task.abort();
    reaper_task.abort();
    cleanup_paths(&paths);
    let _ = lock_file.unlock();

    if !json_mode {
        println!("pilot stopped");
    }
    Ok(())
}

pub async fn status(_json_mode: bool) -> Result<()> {
    let response = send_request(PilotRequest::Status).await?;
    // TODO(M2): implement text mode table output for non-JSON mode
    crate::output::print_json(&serde_json::to_value(&response)?);
    Ok(())
}

pub async fn stop(_json_mode: bool) -> Result<()> {
    let response = send_request(PilotRequest::Stop).await?;
    crate::output::print_json(&serde_json::to_value(&response)?);
    Ok(())
}

async fn handle_client(
    stream: &mut UnixStream,
    pilot_id: &str,
    runtime: &str,
    registry: Arc<Mutex<HashMap<String, PilotRegistration>>>,
    shutdown_tx: watch::Sender<bool>,
    runtime_state: watch::Receiver<PilotRuntimeState>,
) -> Result<PilotResponse> {
    const MAX_REQUEST_SIZE: u64 = 64 * 1024; // 64KB
    let mut buf = Vec::new();
    stream.take(MAX_REQUEST_SIZE).read_to_end(&mut buf).await?;
    let request: PilotRequest = serde_json::from_slice(&buf)?;

    let mut registrations = registry.lock().await;
    let (ok, error) = match request {
        PilotRequest::Register {
            caster_id,
            pid,
            group,
            spawn_command,
            shutdown_signal,
        } => {
            registrations.insert(
                caster_id.clone(),
                PilotRegistration {
                    caster_id,
                    start_time: process_start_time(pid),
                    pid,
                    group,
                    spawn_command,
                    shutdown_signal,
                },
            );
            (true, None)
        }
        PilotRequest::Deregister { caster_id } => {
            registrations.remove(&caster_id);
            if registrations.is_empty() {
                let _ = shutdown_tx.send(true);
            }
            (true, None)
        }
        PilotRequest::Status => runtime_state.borrow().clone().into_status_fields(),
        PilotRequest::Stop => {
            let _ = shutdown_tx.send(true);
            (true, None)
        }
    };

    Ok(PilotResponse {
        ok,
        pilot_id: pilot_id.to_string(),
        runtime: runtime.to_string(),
        casters: registrations.values().cloned().collect(),
        error,
    })
}

async fn run_pilot_session(
    runtime: String,
    pilot_id: String,
    registry: Arc<Mutex<HashMap<String, PilotRegistration>>>,
    mut shutdown_rx: watch::Receiver<bool>,
    runtime_state_tx: watch::Sender<PilotRuntimeState>,
) -> Result<()> {
    let endpoint = if runtime.starts_with("http://") || runtime.starts_with("https://") {
        runtime.clone()
    } else {
        format!("http://{}", runtime)
    };

    let channel = tonic::transport::Channel::from_shared(endpoint)?
        .connect()
        .await?;
    let mut client = RuneServiceClient::new(channel);
    let (tx, rx) = mpsc::channel::<SessionMessage>(32);
    let outbound = ReceiverStream::new(rx);
    let response = client.session(outbound).await?;
    let mut inbound = response.into_inner();

    tx.send(SessionMessage {
        payload: Some(session_message::Payload::Attach(CasterAttach {
            caster_id: pilot_id.clone(),
            runes: Vec::new(),
            labels: HashMap::new(),
            max_concurrent: 1,
            key: std::env::var("RUNE_KEY").unwrap_or_default(),
            role: "pilot".into(),
        })),
    })
    .await?;

    let heartbeat_tx = tx.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if heartbeat_tx
                .send(SessionMessage {
                    payload: Some(session_message::Payload::Heartbeat(Heartbeat {
                        timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                    })),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    break;
                }
            }
            message = inbound.message() => {
                let Some(message) = message? else {
                    break;
                };
                match message.payload {
                    Some(session_message::Payload::AttachAck(ack)) => {
                        if !ack.accepted {
                            let error = format!("pilot attach rejected: {}", ack.reason);
                            let _ = runtime_state_tx.send(PilotRuntimeState::Failed(error.clone()));
                            return Err(anyhow!(error));
                        }
                        let _ = runtime_state_tx.send(PilotRuntimeState::Ready);
                    }
                    Some(session_message::Payload::ScaleSignal(signal)) => {
                        handle_scale_signal(signal, Arc::clone(&registry)).await?;
                    }
                    Some(session_message::Payload::Heartbeat(_)) => {}
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

async fn handle_scale_signal(
    signal: ScaleSignal,
    registry: Arc<Mutex<HashMap<String, PilotRegistration>>>,
) -> Result<()> {
    match signal.action() {
        ScaleAction::Up => {
            let registrations = registry.lock().await;
            let current = registrations
                .values()
                .filter(|registration| registration.group == signal.group_id)
                .count();
            let needed = signal.desired_replicas.saturating_sub(current as u32);
            if needed == 0 {
                return Ok(());
            }

            if let Some(template) = registrations
                .values()
                .find(|registration| registration.group == signal.group_id)
            {
                for _ in 0..needed {
                    let mut child = Command::new("sh")
                        .arg("-c")
                        .arg(&template.spawn_command)
                        .stdout(Stdio::null())
                        .stderr(Stdio::piped())
                        .spawn()
                        .with_context(|| format!("failed to spawn '{}'", template.spawn_command))?;
                    // Reap the child process in the background and log stderr
                    // to aid debugging spawn failures.
                    let spawn_cmd = template.spawn_command.clone();
                    tokio::spawn(async move {
                        if let Some(stderr) = child.stderr.take() {
                            let mut reader = tokio::io::BufReader::new(stderr);
                            let mut line = String::new();
                            use tokio::io::AsyncBufReadExt;
                            while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                                eprintln!("[pilot::spawn] {}: {}", spawn_cmd, line.trim());
                                line.clear();
                            }
                        }
                        let _ = child.wait().await;
                    });
                }
            }
        }
        ScaleAction::Down => {
            if let Some(caster_id) = signal.reason.strip_prefix("force_kill:") {
                let registrations = registry.lock().await;
                if let Some(registration) = registrations.get(caster_id) {
                    if let Some(safe_pid) = validate_pid(registration.pid) {
                        if process_alive_strict(registration.pid, registration.start_time) {
                            unsafe {
                                libc::kill(safe_pid, libc::SIGKILL);
                            }
                        }
                    }
                }
            }
        }
        ScaleAction::Unspecified => {}
    }

    Ok(())
}

async fn send_request(request: PilotRequest) -> Result<PilotResponse> {
    let paths = pilot_paths()?;
    let mut stream = UnixStream::connect(&paths.socket)
        .await
        .with_context(|| format!("pilot is not running at {}", paths.socket.display()))?;
    let payload = serde_json::to_vec(&request)?;
    stream.write_all(&payload).await?;
    AsyncWriteExt::shutdown(&mut stream).await?;
    let mut response_buf = Vec::new();
    stream.read_to_end(&mut response_buf).await?;
    Ok(serde_json::from_slice(&response_buf)?)
}

fn normalize_runtime(runtime: &str) -> String {
    runtime.trim().trim_end_matches('/').to_string()
}

/// Validate that a PID is safe to pass to `libc::kill`.
/// Returns `None` for PID 0 (which would signal the entire process group)
/// and for values exceeding `i32::MAX` (which would overflow on cast).
fn validate_pid(pid: u32) -> Option<i32> {
    if pid == 0 || pid > i32::MAX as u32 {
        None
    } else {
        Some(pid as i32)
    }
}

fn process_alive(pid: u32) -> bool {
    let Some(safe_pid) = validate_pid(pid) else {
        return false;
    };
    let rc = unsafe { libc::kill(safe_pid, 0) };
    if rc == 0 {
        true
    } else {
        let errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or_default();
        errno == libc::EPERM
    }
}

/// Return the process start time (platform-specific).
///
/// Used together with PID to form a unique process identity that survives
/// PID reuse — the OS will never assign the same `(pid, start_time)` pair
/// to two different processes.
#[cfg(target_os = "linux")]
fn process_start_time(pid: u32) -> Option<u64> {
    // Field 22 of /proc/<pid>/stat is `starttime` (clock ticks since boot).
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The comm field (field 2) is wrapped in parens and may contain spaces,
    // so find the last ')' first, then split the remainder.
    let after_comm = stat.get(stat.rfind(')')? + 2..)?;
    // /proc/pid/stat fields are 1-indexed.  After stripping pid (1) and
    // comm (2, in parens), the remainder starts at field 3 (state).
    // starttime is field 22 → 0-based offset from field 3 = 22 - 3 = 19.
    // See proc(5), section /proc/pid/stat.
    after_comm.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(target_os = "macos")]
fn process_start_time(pid: u32) -> Option<u64> {
    use std::mem;
    let mut info: libc::proc_bsdinfo = unsafe { mem::zeroed() };
    let size = mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let ret = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTBSDINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        )
    };
    if ret <= 0 {
        return None;
    }
    Some(info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_start_time(_pid: u32) -> Option<u64> {
    None
}

/// Like [`process_alive`] but also checks the process start time to guard
/// against PID reuse.  Falls back to plain PID check when start time is
/// unavailable on either side.
fn process_alive_strict(pid: u32, expected_start_time: Option<u64>) -> bool {
    if !process_alive(pid) {
        return false;
    }
    match (expected_start_time, process_start_time(pid)) {
        (Some(expected), Some(actual)) => expected == actual,
        _ => true, // cannot verify — fall back to PID-only check
    }
}

fn cleanup_paths(paths: &PilotPaths) {
    let _ = std::fs::remove_file(&paths.socket);
    let _ = std::fs::remove_file(&paths.pid);
}

struct PilotPaths {
    base_dir: PathBuf,
    pid: PathBuf,
    socket: PathBuf,
}

fn pilot_paths() -> Result<PilotPaths> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("failed to locate home directory"))?;
    let base_dir = home.join(".rune");
    Ok(PilotPaths {
        pid: base_dir.join("pilot.pid"),
        socket: base_dir.join("pilot.sock"),
        base_dir,
    })
}

pub fn lock_path() -> Result<PathBuf> {
    Ok(pilot_paths()?.base_dir.join("pilot.lock"))
}

pub fn pid_path() -> Result<PathBuf> {
    Ok(pilot_paths()?.pid)
}

pub fn socket_path() -> Result<PathBuf> {
    Ok(pilot_paths()?.socket)
}

pub fn find_rune_binary() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("RUNE_BIN") {
        return Ok(PathBuf::from(path));
    }

    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            let candidate = dir.join("rune");
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }

    Err(anyhow!(
        "failed to locate rune binary; set RUNE_BIN or add rune to PATH"
    ))
}

pub fn pid_alive(pid_path: &Path) -> bool {
    let Ok(contents) = std::fs::read_to_string(pid_path) else {
        return false;
    };
    let Ok(pid) = contents.trim().parse::<u32>() else {
        return false;
    };
    process_alive(pid)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct HomeGuard(Option<std::ffi::OsString>);

    impl HomeGuard {
        fn set(path: &Path) -> Self {
            let previous = std::env::var_os("HOME");
            std::env::set_var("HOME", path);
            Self(previous)
        }
    }

    impl Drop for HomeGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.0.take() {
                std::env::set_var("HOME", previous);
            } else {
                std::env::remove_var("HOME");
            }
        }
    }

    async fn wait_for_socket() {
        for _ in 0..50 {
            if socket_path().is_ok_and(|path| path.exists()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("pilot socket did not appear in time");
    }

    #[tokio::test]
    async fn test_fix_pilot_status_not_ready_before_attach() {
        let temp = tempfile::tempdir().unwrap();
        let _home = HomeGuard::set(temp.path());

        let daemon = tokio::spawn(async { run_daemon("127.0.0.1:9", true).await });
        wait_for_socket().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = send_request(PilotRequest::Status).await.unwrap();
        assert!(
            !status.ok,
            "pilot should not report ready before runtime attach"
        );
        assert!(status.error.is_some());

        let _ = send_request(PilotRequest::Stop).await;
        let _ = daemon.await;
    }
}
