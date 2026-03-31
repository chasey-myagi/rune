use serde_json::Value;

/// Print JSON value in pretty format to stdout.
pub fn print_json(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("JSON serialization failed")
    );
}

/// Print raw JSON value (compact) to stdout.
pub fn print_json_compact(value: &Value) {
    println!(
        "{}",
        serde_json::to_string(value).expect("JSON serialization failed")
    );
}
