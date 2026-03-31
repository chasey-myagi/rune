use anyhow::Result;

use crate::client::RuneClient;

pub async fn get(client: &RuneClient, id: &str, json: bool) -> Result<()> {
    let result = client.get_task(id).await?;
    if json {
        crate::output::print_json(&result);
    } else {
        let task_id = result.get("task_id").and_then(|v| v.as_str()).unwrap_or(id);
        let rune = result.get("rune_name").and_then(|v| v.as_str()).unwrap_or("-");
        let status = result.get("status").and_then(|v| v.as_str()).unwrap_or("-");
        println!("Task:     {}", task_id);
        println!("Rune:     {}", rune);
        println!("Status:   {}", status);
        if let Some(output) = result.get("output") {
            if !output.is_null() {
                println!("\nResult:");
                crate::output::print_json(output);
            }
        }
        if let Some(err) = result.get("error").and_then(|v| v.as_str()) {
            println!("\nError: {}", err);
        }
    }
    Ok(())
}

pub async fn list(
    client: &RuneClient,
    status: Option<&str>,
    rune: Option<&str>,
    limit: u32,
    json: bool,
) -> Result<()> {
    let result = client.list_tasks(status, rune, limit).await?;
    if json {
        crate::output::print_json(&result);
    } else {
        let tasks = result["tasks"].as_array();
        match tasks {
            Some(arr) if arr.is_empty() => {
                println!("No tasks found.");
            }
            Some(arr) => {
                let mut table = crate::output::new_table(&["ID", "Rune", "Status", "Created"]);
                for t in arr {
                    table.add_row(vec![
                        t["task_id"].as_str().unwrap_or("-"),
                        t["rune_name"].as_str().unwrap_or("-"),
                        t["status"].as_str().unwrap_or("-"),
                        t["created_at"].as_str().unwrap_or("-"),
                    ]);
                }
                println!("{table}");
            }
            None => {
                crate::output::print_json(&result);
            }
        }
    }
    Ok(())
}

pub async fn wait(client: &RuneClient, id: &str, timeout: u64, json: bool) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout);

    if !json {
        eprintln!("Waiting for task {}...", id);
    }

    let mut interval_secs = 1u64;
    loop {
        let result = client.get_task(id).await?;
        let status = result["status"].as_str().unwrap_or("");

        if is_terminal_status(status) {
            if json {
                crate::output::print_json(&result);
            } else {
                eprintln!("Task {} {}", id, status);
                if status == "completed" {
                    if let Some(output) = result.get("output") {
                        crate::output::print_json(output);
                    }
                } else if status == "failed" {
                    if let Some(err) = result.get("error").and_then(|e| e.as_str()) {
                        eprintln!("Error: {}", err);
                    }
                }
            }
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "Timed out after {}s waiting for task {} (current status: {})",
                timeout,
                id,
                status
            );
        }

        tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        interval_secs = (interval_secs + 1).min(5);
    }
}

pub async fn delete(client: &RuneClient, id: &str, json: bool) -> Result<()> {
    let result = client.delete_task(id).await?;
    if json {
        crate::output::print_json(&result);
    } else {
        let status = result["status"].as_str().unwrap_or("cancelled");
        println!("Task {} {}", id, status);
    }
    Ok(())
}

/// Returns true if the task status represents a terminal (finished) state.
fn is_terminal_status(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_status_completed() {
        assert!(is_terminal_status("completed"));
    }

    #[test]
    fn test_terminal_status_failed() {
        assert!(is_terminal_status("failed"));
    }

    #[test]
    fn test_terminal_status_cancelled() {
        assert!(is_terminal_status("cancelled"));
    }

    #[test]
    fn test_non_terminal_status_pending() {
        assert!(!is_terminal_status("pending"));
    }

    #[test]
    fn test_non_terminal_status_running() {
        assert!(!is_terminal_status("running"));
    }

    #[test]
    fn test_non_terminal_status_empty() {
        assert!(!is_terminal_status(""));
    }
}
