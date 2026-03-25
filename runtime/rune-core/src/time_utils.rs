//! Centralized time utility functions.
//!
//! All crates in the workspace should use these instead of rolling their own.

use std::time::{SystemTime, UNIX_EPOCH};

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

    #[test]
    fn now_ms_returns_recent_timestamp() {
        let ms = now_ms();
        // Should be after 2024-01-01 and before 2100-01-01
        assert!(ms > 1_704_067_200_000, "timestamp too old: {}", ms);
        assert!(ms < 4_102_444_800_000, "timestamp too far in the future: {}", ms);
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
}
