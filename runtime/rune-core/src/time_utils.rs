//! Centralized time utility functions.
//!
//! All crates in the workspace should use these instead of rolling their own.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);
static TRACE_COUNTER: AtomicU64 = AtomicU64::new(0);
static SPAN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns the current time as milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as u64,
        Err(_) => {
            eprintln!("WARNING: system clock is before Unix epoch, returning 0");
            0
        }
    }
}

/// Returns the current time as an ISO 8601 string (UTC, second precision).
///
/// Format: `YYYY-MM-DDThh:mm:ssZ`
pub fn now_iso8601() -> String {
    let dur = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(_) => {
            eprintln!("WARNING: system clock is before Unix epoch, returning epoch");
            std::time::Duration::default()
        }
    };
    let secs = dur.as_secs();
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;
    let (year, month, day) = days_to_ymd(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Generate a unique request ID with the format `rq-{ts_hex}-{seq_hex}`.
pub fn unique_request_id() -> String {
    let seq = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = now_ms();
    format!("rq-{ts:016x}-{seq:016x}")
}

/// Generate a W3C-compatible 128-bit trace ID as 32 lowercase hex chars.
pub fn generate_trace_id() -> String {
    let ts = now_ms();
    let seq = TRACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{ts:016x}{seq:016x}")
}

/// Generate a W3C-compatible 64-bit span ID as 16 lowercase hex chars.
pub fn generate_span_id() -> String {
    let seq = SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = now_ms();
    format!("{:016x}", ts ^ seq.rotate_left(17))
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    days += 719468;
    let era = days / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    #[test]
    fn now_ms_returns_recent_timestamp() {
        let ms = now_ms();
        // Should be after 2024-01-01 and before 2100-01-01
        assert!(ms > 1_704_067_200_000, "timestamp too old: {}", ms);
        assert!(
            ms < 4_102_444_800_000,
            "timestamp too far in the future: {}",
            ms
        );
    }

    #[test]
    fn now_iso8601_format() {
        let s = now_iso8601();
        // Should match YYYY-MM-DDThh:mm:ssZ
        assert_eq!(s.len(), 20);
        assert!(s.ends_with('Z'));
        assert_eq!(&s[4..5], "-");
        assert_eq!(&s[7..8], "-");
        assert_eq!(&s[10..11], "T");
        assert_eq!(&s[13..14], ":");
        assert_eq!(&s[16..17], ":");
    }

    #[test]
    fn now_ms_is_monotonic_ish() {
        let a = now_ms();
        let b = now_ms();
        assert!(b >= a);
    }

    // I-3 回归测试: now_ms 和 now_iso8601 不应该 panic
    // （防御性改进，使用 unwrap_or_default 替换 unwrap）
    #[test]
    fn test_fix_now_ms_does_not_panic() {
        let ms = now_ms();
        assert!(ms > 0);
        let iso = now_iso8601();
        assert!(!iso.is_empty());
    }

    #[test]
    fn unified_request_id_format() {
        let id = unique_request_id();
        assert!(id.starts_with("rq-"));
        let parts: Vec<_> = id.split('-').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[1].len(), 16);
        assert_eq!(parts[2].len(), 16);
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()));
        assert!(parts[2].chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn generate_trace_id_is_32_hex_chars() {
        let trace_id = generate_trace_id();
        assert_eq!(trace_id.len(), 32);
        assert!(trace_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn generate_span_id_is_16_hex_chars() {
        let span_id = generate_span_id();
        assert_eq!(span_id.len(), 16);
        assert!(span_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn unified_request_id_unique_under_concurrency() {
        let ids = Arc::new(Mutex::new(HashSet::new()));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let ids = Arc::clone(&ids);
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        let id = unique_request_id();
                        ids.lock().unwrap().insert(id);
                    }
                })
            })
            .collect();

        for handle in threads {
            handle.join().unwrap();
        }

        assert_eq!(ids.lock().unwrap().len(), 4000);
    }
}
