use comfy_table::{presets, ContentArrangement, Table};
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

/// Create a styled table with headers.
///
/// Uses `UTF8_BORDERS_ONLY` preset for clean visual separation.
/// Headers are uppercased and bolded automatically.
pub fn new_table(headers: &[&str]) -> Table {
    let mut table = Table::new();
    table
        .load_preset(presets::UTF8_BORDERS_ONLY)
        .set_content_arrangement(ContentArrangement::Dynamic);

    let header_cells: Vec<comfy_table::Cell> = headers
        .iter()
        .map(|h| {
            comfy_table::Cell::new(h.to_uppercase())
                .add_attribute(comfy_table::Attribute::Bold)
        })
        .collect();
    table.set_header(header_cells);
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_table_creates_table_with_headers() {
        let table = new_table(&["Name", "Version", "Status"]);
        let output = table.to_string();
        assert!(output.contains("NAME"), "table should have uppercased NAME header");
        assert!(output.contains("VERSION"), "table should have uppercased VERSION header");
        assert!(output.contains("STATUS"), "table should have uppercased STATUS header");
    }

    #[test]
    fn test_new_table_with_rows() {
        let mut table = new_table(&["Name", "Value"]);
        table.add_row(vec!["hello", "world"]);
        let output = table.to_string();
        assert!(output.contains("hello"), "table should contain row data");
        assert!(output.contains("world"), "table should contain row data");
    }

    #[test]
    fn test_print_json_format() {
        // Just verify it doesn't panic
        let val = json!({"key": "value"});
        // We can't easily capture stdout in a unit test, but we can verify
        // the underlying serialization works
        let pretty = serde_json::to_string_pretty(&val).unwrap();
        assert!(pretty.contains("key"));
        assert!(pretty.contains("value"));
    }

    #[test]
    fn test_print_json_compact_format() {
        let val = json!({"key": "value"});
        let compact = serde_json::to_string(&val).unwrap();
        assert!(!compact.contains('\n'), "compact JSON should be single line");
    }
}
