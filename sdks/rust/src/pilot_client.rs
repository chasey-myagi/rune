use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::process::Command;

use crate::config::ScalePolicy;
use crate::error::{SdkError, SdkResult};

const PILOT_CONNECTING_ERROR: &str = "runtime session not attached";
const DEFAULT_PILOT_ENSURE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PILOT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

fn pilot_ensure_timeout() -> Duration {
    std::env::var("RUNE_PILOT_ENSURE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_PILOT_ENSURE_TIMEOUT)
}

fn pilot_request_timeout() -> Duration {
    std::env::var("RUNE_PILOT_REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_PILOT_REQUEST_TIMEOUT)
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Deserialize)]
struct PilotResponse {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    pilot_id: String,
    #[serde(default)]
    runtime: String,
    error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PilotClient {
    pilot_id: String,
}

impl PilotClient {
    pub async fn ensure(runtime: &str, key: Option<&str>) -> SdkResult<Self> {
        let normalized = normalize_runtime(runtime);
        let deadline = tokio::time::Instant::now() + pilot_ensure_timeout();
        if let Ok(response) = send_request(&PilotRequest::Status).await {
            match Self::classify_status(response, &normalized) {
                EnsureStatus::Ready(client) => return Ok(client),
                EnsureStatus::Retry => {
                    return Self::wait_until_ready(&normalized, deadline, Some(runtime), key).await;
                }
                EnsureStatus::Mismatch => {
                    // Existing pilot is bound to a different runtime — stop it first.
                    let _ = send_request(&PilotRequest::Stop).await;
                }
                EnsureStatus::Failed(error) => return Err(SdkError::Other(error)),
            }
        }

        start_pilot(runtime, key).await?;
        Self::wait_until_ready(&normalized, deadline, Some(runtime), key).await
    }

    /// Poll until the pilot reports ready. When `start_runtime`/`start_key`
    /// are provided, re-attempt `start_pilot` on connection failure so that
    /// a slow predecessor release doesn't doom the single initial spawn.
    async fn wait_until_ready(
        normalized: &str,
        deadline: tokio::time::Instant,
        start_runtime: Option<&str>,
        start_key: Option<&str>,
    ) -> SdkResult<Self> {
        let mut last_start = tokio::time::Instant::now();
        loop {
            match send_request(&PilotRequest::Status).await {
                Ok(response) => match Self::classify_status(response, normalized) {
                    EnsureStatus::Ready(client) => return Ok(client),
                    EnsureStatus::Retry | EnsureStatus::Mismatch => {}
                    EnsureStatus::Failed(error) => return Err(SdkError::Other(error)),
                },
                Err(_) => {
                    // Connection failed — re-attempt start if enough time has passed.
                    if let Some(rt) = start_runtime {
                        if last_start.elapsed() >= Duration::from_secs(1) {
                            let _ = start_pilot(rt, start_key).await;
                            last_start = tokio::time::Instant::now();
                        }
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(SdkError::Other("pilot did not become ready".into()))
    }

    pub fn pilot_id(&self) -> &str {
        &self.pilot_id
    }

    pub async fn register(&self, caster_id: &str, policy: &ScalePolicy) -> SdkResult<()> {
        let response = send_request(&PilotRequest::Register {
            caster_id: caster_id.to_string(),
            pid: std::process::id(),
            group: policy.group.clone(),
            spawn_command: policy.spawn_command.clone(),
            shutdown_signal: policy.shutdown_signal.clone(),
        })
        .await?;
        Self::ensure_ok(response)
    }

    pub async fn deregister(&self, caster_id: &str) -> SdkResult<()> {
        let response = send_request(&PilotRequest::Deregister {
            caster_id: caster_id.to_string(),
        })
        .await?;
        Self::ensure_ok(response)
    }

    fn ensure_ok(response: PilotResponse) -> SdkResult<()> {
        if response.ok {
            Ok(())
        } else {
            Err(SdkError::Other(
                response
                    .error
                    .unwrap_or_else(|| "pilot request failed".into()),
            ))
        }
    }

    fn classify_status(response: PilotResponse, normalized: &str) -> EnsureStatus {
        if response.runtime != normalized {
            return EnsureStatus::Mismatch;
        }
        if response.ok {
            return EnsureStatus::Ready(Self {
                pilot_id: response.pilot_id,
            });
        }
        match response.error {
            Some(error) if error == PILOT_CONNECTING_ERROR || error.is_empty() => {
                EnsureStatus::Retry
            }
            Some(error) => EnsureStatus::Failed(error),
            None => EnsureStatus::Retry,
        }
    }
}

enum EnsureStatus {
    Ready(PilotClient),
    Retry,
    Mismatch,
    Failed(String),
}

async fn send_request(request: &PilotRequest) -> SdkResult<PilotResponse> {
    tokio::time::timeout(pilot_request_timeout(), send_request_inner(request))
        .await
        .map_err(|_| SdkError::Other("pilot request timed out".into()))?
}

async fn send_request_inner(request: &PilotRequest) -> SdkResult<PilotResponse> {
    let socket_path = socket_path()?;
    let mut stream = UnixStream::connect(&socket_path)
        .await
        .map_err(|err| SdkError::Other(format!("failed to connect to pilot: {err}")))?;
    let payload = serde_json::to_vec(request)
        .map_err(|err| SdkError::Other(format!("failed to encode pilot request: {err}")))?;
    stream
        .write_all(&payload)
        .await
        .map_err(|err| SdkError::Other(format!("failed to write pilot request: {err}")))?;
    stream
        .shutdown()
        .await
        .map_err(|err| SdkError::Other(format!("failed to flush pilot request: {err}")))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|err| SdkError::Other(format!("failed to read pilot response: {err}")))?;
    serde_json::from_slice(&response)
        .map_err(|err| SdkError::Other(format!("failed to decode pilot response: {err}")))
}

async fn start_pilot(runtime: &str, key: Option<&str>) -> SdkResult<()> {
    let mut command = Command::new(find_rune_binary()?);
    command
        .arg("pilot")
        .arg("daemon")
        .arg("--runtime")
        .arg(normalize_runtime(runtime))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(key) = key {
        command.env("RUNE_KEY", key);
    }
    #[cfg(unix)]
    unsafe {
        command.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    command
        .spawn()
        .map_err(|err| SdkError::Other(format!("failed to start pilot daemon: {err}")))?;
    Ok(())
}

fn normalize_runtime(runtime: &str) -> String {
    runtime.trim().trim_end_matches('/').to_string()
}

fn socket_path() -> SdkResult<PathBuf> {
    Ok(home_dir()?.join(".rune").join("pilot.sock"))
}

fn find_rune_binary() -> SdkResult<PathBuf> {
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

    Err(SdkError::Other(
        "failed to locate rune binary; set RUNE_BIN or add rune to PATH".into(),
    ))
}

fn home_dir() -> SdkResult<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| SdkError::Other("failed to determine HOME".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::net::UnixListener;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct HomeGuard {
        previous: Option<OsString>,
        root: PathBuf,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl HomeGuard {
        fn set() -> Self {
            let lock = ENV_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let root = PathBuf::from(format!("/tmp/rpc-{}-{unique}", std::process::id()));
            fs::create_dir_all(root.join(".rune")).unwrap();
            let previous = std::env::var_os("HOME");
            std::env::set_var("HOME", &root);
            Self {
                previous,
                root,
                _lock: lock,
            }
        }
    }

    impl Drop for HomeGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.take() {
                std::env::set_var("HOME", previous);
            } else {
                std::env::remove_var("HOME");
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    async fn spawn_status_server(
        socket: &Path,
        responses: Vec<serde_json::Value>,
    ) -> tokio::task::JoinHandle<()> {
        let _ = fs::remove_file(socket);
        let listener = UnixListener::bind(socket).unwrap();
        let responses = Mutex::new(VecDeque::from(responses));
        tokio::spawn(async move {
            loop {
                let Some(response) = responses
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .pop_front()
                else {
                    break;
                };
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                stream.read_to_end(&mut request).await.unwrap();
                let payload: serde_json::Value = serde_json::from_slice(&request).unwrap();
                assert_eq!(payload["command"], "status");
                stream
                    .write_all(&serde_json::to_vec(&response).unwrap())
                    .await
                    .unwrap();
            }
        })
    }

    #[tokio::test]
    async fn test_fix_ensure_waits_for_matching_runtime_to_become_ready() {
        let _home = HomeGuard::set();
        let socket = socket_path().unwrap();
        let server = spawn_status_server(
            &socket,
            vec![
                json!({
                    "ok": false,
                    "pilot_id": "pilot-1",
                    "runtime": "127.0.0.1:50051",
                    "error": "runtime session not attached"
                }),
                json!({
                    "ok": true,
                    "pilot_id": "pilot-1",
                    "runtime": "127.0.0.1:50051",
                    "error": null
                }),
            ],
        )
        .await;

        let client = PilotClient::ensure("127.0.0.1:50051", None)
            .await
            .expect("ensure should keep polling while pilot is still connecting");
        assert_eq!(client.pilot_id(), "pilot-1");

        server.await.unwrap();
    }

    /// Regression: PilotResponse must deserialize even when `ok` and `pilot_id`
    /// are absent (e.g. error-only responses from pilot daemon).
    #[test]
    fn test_fix_pilot_response_deserialize_without_pilot_id() {
        let json = r#"{"ok": false, "error": "connection refused"}"#;
        let resp: PilotResponse =
            serde_json::from_str(json).expect("should deserialize error response missing pilot_id");
        assert!(!resp.ok);
        assert_eq!(resp.pilot_id, "");
        assert_eq!(resp.error.as_deref(), Some("connection refused"));

        // Minimal error-only payload (no ok, no pilot_id, no runtime)
        let json_minimal = r#"{"error": "socket not found"}"#;
        let resp2: PilotResponse = serde_json::from_str(json_minimal)
            .expect("should deserialize minimal error-only response");
        assert!(!resp2.ok);
        assert_eq!(resp2.pilot_id, "");
        assert_eq!(resp2.runtime, "");
        assert_eq!(resp2.error.as_deref(), Some("socket not found"));
    }

    /// Regression: empty-string error must retry (aligned with Python/TS SDKs).
    /// Rust previously treated Some("") as Failed(""), while Python/TS treated
    /// falsy error as retry.
    #[test]
    fn test_fix_classify_status_empty_error_retries() {
        let response = PilotResponse {
            ok: false,
            pilot_id: "pilot-1".into(),
            runtime: "127.0.0.1:50051".into(),
            error: Some("".into()),
        };
        let result = PilotClient::classify_status(response, "127.0.0.1:50051");
        assert!(
            matches!(result, EnsureStatus::Retry),
            "empty error string should classify as Retry, not Failed"
        );
    }
}
