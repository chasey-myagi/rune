use anyhow::Result;

use crate::client::RuneClient;
use crate::output;

/// Run the list command: fetch registered runes and display as table or JSON.
pub async fn run(client: &RuneClient, json_mode: bool) -> Result<()> {
    let result: serde_json::Value = client.list_runes().await?;

    if json_mode {
        output::print_json(&result);
        return Ok(());
    }

    // Parse as array — handle both bare array and { "runes": [...] } wrapper
    let empty_vec: Vec<serde_json::Value> = vec![];
    let runes = match result.as_array() {
        Some(arr) => arr,
        None => result
            .get("runes")
            .and_then(|v: &serde_json::Value| v.as_array())
            .unwrap_or(&empty_vec),
    };

    if runes.is_empty() {
        println!("No runes registered.");
        return Ok(());
    }

    let mut table =
        output::new_table(&["Name", "Version", "Mode", "Gate Path", "Caster", "Status"]);

    for rune in runes {
        let name = rune
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        let version = rune
            .get("version")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        let mode = rune
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unary");
        let gate_path = rune
            .get("gate_path")
            .or_else(|| rune.get("gate"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        let caster = rune
            .get("caster_id")
            .or_else(|| rune.get("caster"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        table.add_row(vec![name, version, mode, gate_path, caster, "online"]);
    }

    println!("{table}");
    println!("\n{} rune(s) registered", runes.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_output_format() {
        // Verify table creation with expected headers
        let table =
            output::new_table(&["Name", "Version", "Mode", "Gate Path", "Caster", "Status"]);
        let output = table.to_string();
        assert!(output.contains("NAME"));
        assert!(output.contains("VERSION"));
        assert!(output.contains("MODE"));
        assert!(output.contains("GATE PATH"));
        assert!(output.contains("CASTER"));
        assert!(output.contains("STATUS"));
    }

    #[test]
    fn test_table_with_rune_data() {
        let mut table =
            output::new_table(&["Name", "Version", "Mode", "Gate Path", "Caster", "Status"]);
        table.add_row(vec![
            "hello",
            "1.0",
            "unary",
            "/api/v1/runes/hello",
            "caster-1",
            "online",
        ]);
        let rendered = table.to_string();
        assert!(rendered.contains("hello"), "should contain rune name");
        assert!(rendered.contains("caster-1"), "should contain caster id");
        assert!(rendered.contains("online"), "should contain status");
    }
}
