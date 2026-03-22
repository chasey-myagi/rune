#[path = "../src/daemon.rs"]
mod daemon;

// ── Security: PID validation ────────────────────────────────────────────

#[test]
fn test_fix_is_process_alive_rejects_pid_zero() {
    // PID 0 would send signal to entire process group — must return false
    assert!(!daemon::is_process_alive(0));
}

#[test]
fn test_fix_send_signal_rejects_pid_zero() {
    // send_signal with PID 0 must fail
    let result = daemon::send_signal(0, libc::SIGTERM);
    assert!(result.is_err());
}

#[test]
fn test_fix_send_signal_succeeds_for_valid_pid() {
    // Sending signal 0 to our own process should succeed
    let pid = std::process::id();
    let result = daemon::send_signal(pid, 0);
    assert!(result.is_ok());
}
