use rune_store::models::*;
use rune_store::RuneStore;
use std::sync::Arc;

fn new_store() -> RuneStore {
    RuneStore::open_in_memory().expect("failed to open in-memory store")
}

// ============================================================
// B1: Schema & Store
// ============================================================

#[tokio::test]
async fn test_open_in_memory() {
    let store = new_store();
    // Verify tables exist by running queries that would fail otherwise
    assert!(store.list_keys().await.is_ok());
    assert!(store.list_tasks(None, None, 10, 0).await.is_ok());
    assert!(store.query_logs(None, 10).await.is_ok());
    assert!(store.list_snapshots().await.is_ok());
}

#[tokio::test]
async fn test_open_file_based() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let store = RuneStore::open(&db_path).expect("failed to open file-based store");
    store.insert_task("t1", "rune_a", None).await.unwrap();

    // Reopen and verify data persists
    drop(store);
    let store2 = RuneStore::open(&db_path).unwrap();
    let task = store2.get_task("t1").await.unwrap();
    assert!(task.is_some());
}

// ============================================================
// B2: API Key CRUD
// ============================================================

#[tokio::test]
async fn test_create_and_verify_gate_key() {
    let store = new_store();
    let result = store.create_key(KeyType::Gate, "test gate").await.unwrap();
    let verified = store
        .verify_key(&result.raw_key, KeyType::Gate)
        .await
        .unwrap();
    assert!(verified.is_some());
    let key = verified.unwrap();
    assert_eq!(key.key_type, KeyType::Gate);
    assert_eq!(key.label, "test gate");
}

#[tokio::test]
async fn test_create_and_verify_caster_key() {
    let store = new_store();
    let result = store
        .create_key(KeyType::Caster, "test caster")
        .await
        .unwrap();
    let verified = store
        .verify_key(&result.raw_key, KeyType::Caster)
        .await
        .unwrap();
    assert!(verified.is_some());
    assert_eq!(verified.unwrap().key_type, KeyType::Caster);
}

#[tokio::test]
async fn test_wrong_key_type_fails() {
    let store = new_store();
    let result = store.create_key(KeyType::Gate, "gate key").await.unwrap();
    // Verify as caster should fail
    let verified = store
        .verify_key(&result.raw_key, KeyType::Caster)
        .await
        .unwrap();
    assert!(verified.is_none());
}

#[tokio::test]
async fn test_invalid_key_fails() {
    let store = new_store();
    let verified = store
        .verify_key("some_random_string_not_a_key", KeyType::Gate)
        .await
        .unwrap();
    assert!(verified.is_none());
}

#[tokio::test]
async fn test_empty_key_fails() {
    let store = new_store();
    let verified = store.verify_key("", KeyType::Gate).await.unwrap();
    assert!(verified.is_none());
}

#[tokio::test]
async fn test_list_keys() {
    let store = new_store();
    store.create_key(KeyType::Gate, "key1").await.unwrap();
    store.create_key(KeyType::Caster, "key2").await.unwrap();
    store.create_key(KeyType::Gate, "key3").await.unwrap();

    let keys = store.list_keys().await.unwrap();
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0].label, "key1");
    assert_eq!(keys[1].label, "key2");
    assert_eq!(keys[2].label, "key3");
}

#[tokio::test]
async fn test_list_keys_hides_hash() {
    let store = new_store();
    store.create_key(KeyType::Gate, "secret").await.unwrap();

    let keys = store.list_keys().await.unwrap();
    assert_eq!(keys.len(), 1);
    assert!(
        keys[0].key_hash.is_none(),
        "key_hash should be None in list results"
    );
}

#[tokio::test]
async fn test_revoke_key() {
    let store = new_store();
    let result = store.create_key(KeyType::Gate, "to revoke").await.unwrap();

    // Verify works before revoke
    assert!(store
        .verify_key(&result.raw_key, KeyType::Gate)
        .await
        .unwrap()
        .is_some());

    // Revoke
    store.revoke_key(result.api_key.id).await.unwrap();

    // Verify fails after revoke
    assert!(store
        .verify_key(&result.raw_key, KeyType::Gate)
        .await
        .unwrap()
        .is_none());

    // Check revoked_at is set in list
    let keys = store.list_keys().await.unwrap();
    assert!(keys[0].revoked_at.is_some());
}

#[tokio::test]
async fn test_revoke_nonexistent_key() {
    let store = new_store();
    // Should not error
    let result = store.revoke_key(99999).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_create_multiple_same_label() {
    let store = new_store();
    let r1 = store.create_key(KeyType::Gate, "same-label").await.unwrap();
    let r2 = store.create_key(KeyType::Gate, "same-label").await.unwrap();

    // Both should be different keys
    assert_ne!(r1.raw_key, r2.raw_key);
    assert_ne!(r1.api_key.id, r2.api_key.id);

    let keys = store.list_keys().await.unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].label, "same-label");
    assert_eq!(keys[1].label, "same-label");
}

#[tokio::test]
async fn test_key_format() {
    let store = new_store();
    let result = store.create_key(KeyType::Gate, "fmt test").await.unwrap();
    let raw = &result.raw_key;

    // Format: rk_{32_hex_chars}
    assert!(raw.starts_with("rk_"), "key should start with rk_");
    assert_eq!(raw.len(), 35, "key should be 35 chars: 'rk_' + 32 hex");

    // Verify hex portion
    let hex_part = &raw[3..];
    assert_eq!(hex_part.len(), 32);
    assert!(
        hex_part.chars().all(|c| c.is_ascii_hexdigit()),
        "suffix should be hex"
    );
}

// ============================================================
// B3: Task CRUD
// ============================================================

#[tokio::test]
async fn test_task_insert_and_get() {
    let store = new_store();
    let input = r#"{"prompt": "hello"}"#;
    let task = store
        .insert_task("task-001", "echo_rune", Some(input))
        .await
        .unwrap();

    assert_eq!(task.task_id, "task-001");
    assert_eq!(task.rune_name, "echo_rune");
    assert_eq!(task.status, TaskStatus::Pending);
    assert_eq!(task.input.as_deref(), Some(input));

    let fetched = store.get_task("task-001").await.unwrap().unwrap();
    assert_eq!(fetched.task_id, "task-001");
    assert_eq!(fetched.input.as_deref(), Some(input));
}

#[tokio::test]
async fn test_task_not_found() {
    let store = new_store();
    let result = store.get_task("nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_task_lifecycle() {
    let store = new_store();
    store
        .insert_task("lc-1", "rune_a", Some("{}"))
        .await
        .unwrap();

    // pending → running
    store
        .update_task_status("lc-1", TaskStatus::Running, None, None)
        .await
        .unwrap();
    let task = store.get_task("lc-1").await.unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Running);
    assert!(task.started_at.is_some());

    // running → completed
    store
        .update_task_status(
            "lc-1",
            TaskStatus::Completed,
            Some(r#"{"result": 42}"#),
            None,
        )
        .await
        .unwrap();
    let task = store.get_task("lc-1").await.unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Completed);
    assert!(task.completed_at.is_some());
    assert_eq!(task.output.as_deref(), Some(r#"{"result": 42}"#));
}

#[tokio::test]
async fn test_task_failed() {
    let store = new_store();
    store.insert_task("fail-1", "rune_a", None).await.unwrap();
    store
        .update_task_status("fail-1", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status(
            "fail-1",
            TaskStatus::Failed,
            None,
            Some("timeout after 30s"),
        )
        .await
        .unwrap();

    let task = store.get_task("fail-1").await.unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Failed);
    assert_eq!(task.error.as_deref(), Some("timeout after 30s"));
    assert!(task.completed_at.is_some());
}

#[tokio::test]
async fn test_task_cancelled() {
    let store = new_store();
    store.insert_task("cancel-1", "rune_a", None).await.unwrap();
    store
        .update_task_status("cancel-1", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("cancel-1", TaskStatus::Cancelled, None, None)
        .await
        .unwrap();

    let task = store.get_task("cancel-1").await.unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Cancelled);
    assert!(task.completed_at.is_some());
}

#[tokio::test]
async fn test_list_tasks_by_status() {
    let store = new_store();
    store.insert_task("s1", "rune_a", None).await.unwrap();
    store.insert_task("s2", "rune_a", None).await.unwrap();
    store.insert_task("s3", "rune_a", None).await.unwrap();
    store
        .update_task_status("s2", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("s3", TaskStatus::Completed, Some("done"), None)
        .await
        .unwrap();

    let pending = store
        .list_tasks(Some(TaskStatus::Pending), None, 100, 0)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].task_id, "s1");

    let running = store
        .list_tasks(Some(TaskStatus::Running), None, 100, 0)
        .await
        .unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].task_id, "s2");

    let completed = store
        .list_tasks(Some(TaskStatus::Completed), None, 100, 0)
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].task_id, "s3");
}

#[tokio::test]
async fn test_list_tasks_by_rune() {
    let store = new_store();
    store.insert_task("r1", "alpha", None).await.unwrap();
    store.insert_task("r2", "beta", None).await.unwrap();
    store.insert_task("r3", "alpha", None).await.unwrap();

    let alpha = store.list_tasks(None, Some("alpha"), 100, 0).await.unwrap();
    assert_eq!(alpha.len(), 2);

    let beta = store.list_tasks(None, Some("beta"), 100, 0).await.unwrap();
    assert_eq!(beta.len(), 1);
}

#[tokio::test]
async fn test_list_tasks_pagination() {
    let store = new_store();
    for i in 0..10 {
        store
            .insert_task(&format!("p{}", i), "rune_a", None)
            .await
            .unwrap();
    }

    let page1 = store.list_tasks(None, None, 3, 0).await.unwrap();
    assert_eq!(page1.len(), 3);

    let page2 = store.list_tasks(None, None, 3, 3).await.unwrap();
    assert_eq!(page2.len(), 3);

    // Pages should not overlap
    let ids1: Vec<_> = page1.iter().map(|t| &t.task_id).collect();
    let ids2: Vec<_> = page2.iter().map(|t| &t.task_id).collect();
    for id in &ids1 {
        assert!(!ids2.contains(id), "pagination overlap detected");
    }

    // Beyond range
    let page_far = store.list_tasks(None, None, 3, 100).await.unwrap();
    assert_eq!(page_far.len(), 0);
}

#[tokio::test]
async fn test_list_tasks_empty() {
    let store = new_store();
    let result = store.list_tasks(None, None, 100, 0).await.unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_update_nonexistent_task() {
    let store = new_store();
    // Should not error
    let result = store
        .update_task_status("ghost", TaskStatus::Running, None, None)
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_task_with_large_output() {
    let store = new_store();
    store.insert_task("big-1", "rune_a", None).await.unwrap();

    // Generate a large JSON output (~1MB)
    let large_obj: serde_json::Value = serde_json::json!({
        "data": "x".repeat(1_000_000),
        "nested": {
            "array": (0..1000).collect::<Vec<i32>>(),
        }
    });
    let large_output = serde_json::to_string(&large_obj).unwrap();

    store
        .update_task_status("big-1", TaskStatus::Completed, Some(&large_output), None)
        .await
        .unwrap();

    let task = store.get_task("big-1").await.unwrap().unwrap();
    assert_eq!(task.output.as_deref(), Some(large_output.as_str()));

    // Verify the JSON can be parsed back
    let parsed: serde_json::Value = serde_json::from_str(task.output.as_ref().unwrap()).unwrap();
    assert_eq!(parsed["data"].as_str().unwrap().len(), 1_000_000);
}

#[tokio::test]
async fn test_task_concurrent_updates() {
    let store = Arc::new(new_store());

    // Create 20 tasks
    for i in 0..20 {
        store
            .insert_task(&format!("conc-{}", i), "rune_a", None)
            .await
            .unwrap();
    }

    // Spawn threads to update different tasks concurrently
    let mut handles = vec![];
    for i in 0..20 {
        let store = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            let task_id = format!("conc-{}", i);
            store
                .update_task_status(&task_id, TaskStatus::Running, None, None)
                .await
                .unwrap();
            // Small work simulation
            std::thread::sleep(std::time::Duration::from_millis(1));
            store
                .update_task_status(
                    &task_id,
                    TaskStatus::Completed,
                    Some(&format!(r#"{{"thread": {}}}"#, i)),
                    None,
                )
                .await
                .unwrap();
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify all tasks are completed
    let tasks = store
        .list_tasks(Some(TaskStatus::Completed), None, 100, 0)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 20);
}

// ============================================================
// B4: Call Logs
// ============================================================

fn make_log(rune_name: &str, request_id: &str, timestamp: &str) -> CallLog {
    CallLog {
        id: 0,
        request_id: request_id.to_string(),
        rune_name: rune_name.to_string(),
        mode: "sync".to_string(),
        caster_id: Some("caster-1".to_string()),
        latency_ms: 42,
        status_code: 200,
        input_size: 100,
        output_size: 200,
        timestamp: timestamp.to_string(),
    }
}

#[tokio::test]
async fn test_insert_and_query_logs() {
    let store = new_store();
    let log = make_log("rune_a", "req-1", "2026-01-01T00:00:00Z");
    let id = store.insert_log(&log).await.unwrap();
    assert!(id > 0);

    let logs = store.query_logs(None, 10).await.unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].request_id, "req-1");
    assert_eq!(logs[0].rune_name, "rune_a");
    assert_eq!(logs[0].latency_ms, 42);
}

#[tokio::test]
async fn test_query_logs_by_rune() {
    let store = new_store();
    store
        .insert_log(&make_log("alpha", "r1", "2026-01-01T00:00:01Z"))
        .await
        .unwrap();
    store
        .insert_log(&make_log("beta", "r2", "2026-01-01T00:00:02Z"))
        .await
        .unwrap();
    store
        .insert_log(&make_log("alpha", "r3", "2026-01-01T00:00:03Z"))
        .await
        .unwrap();

    let alpha_logs = store.query_logs(Some("alpha"), 100).await.unwrap();
    assert_eq!(alpha_logs.len(), 2);

    let beta_logs = store.query_logs(Some("beta"), 100).await.unwrap();
    assert_eq!(beta_logs.len(), 1);
}

#[tokio::test]
async fn test_query_logs_limit() {
    let store = new_store();
    for i in 0..10 {
        store
            .insert_log(&make_log(
                "rune_a",
                &format!("r{}", i),
                &format!("2026-01-01T00:00:{:02}Z", i),
            ))
            .await
            .unwrap();
    }

    let logs = store.query_logs(None, 3).await.unwrap();
    assert_eq!(logs.len(), 3);
}

#[tokio::test]
async fn test_query_logs_order() {
    let store = new_store();
    store
        .insert_log(&make_log("rune_a", "old", "2025-01-01T00:00:00Z"))
        .await
        .unwrap();
    store
        .insert_log(&make_log("rune_a", "new", "2026-06-01T00:00:00Z"))
        .await
        .unwrap();
    store
        .insert_log(&make_log("rune_a", "mid", "2025-06-01T00:00:00Z"))
        .await
        .unwrap();

    let logs = store.query_logs(None, 10).await.unwrap();
    assert_eq!(logs.len(), 3);
    // Should be newest first
    assert_eq!(logs[0].request_id, "new");
    assert_eq!(logs[1].request_id, "mid");
    assert_eq!(logs[2].request_id, "old");
}

#[tokio::test]
async fn test_cleanup_old_logs() {
    let store = new_store();
    store
        .insert_log(&make_log("rune_a", "old1", "2024-01-01T00:00:00Z"))
        .await
        .unwrap();
    store
        .insert_log(&make_log("rune_a", "old2", "2024-06-01T00:00:00Z"))
        .await
        .unwrap();
    store
        .insert_log(&make_log("rune_a", "recent", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();

    let deleted = store
        .cleanup_logs_before("2025-01-01T00:00:00Z")
        .await
        .unwrap();
    assert_eq!(deleted, 2);

    let remaining = store.query_logs(None, 100).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].request_id, "recent");
}

#[tokio::test]
async fn test_log_with_null_caster() {
    let store = new_store();
    let mut log = make_log("rune_a", "no-caster", "2026-01-01T00:00:00Z");
    log.caster_id = None;
    store.insert_log(&log).await.unwrap();

    let logs = store.query_logs(None, 10).await.unwrap();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].caster_id.is_none());
}

// ============================================================
// B4: Snapshots
// ============================================================

fn make_snapshot(name: &str) -> RuneSnapshot {
    RuneSnapshot {
        rune_name: name.to_string(),
        version: "0.1.0".to_string(),
        description: format!("A rune called {}", name),
        supports_stream: false,
        gate_path: format!("/rune/{}", name),
        gate_method: "POST".to_string(),
        last_seen: String::new(), // will be set by upsert
    }
}

#[tokio::test]
async fn test_upsert_snapshot() {
    let store = new_store();
    let snap = make_snapshot("echo");
    store.upsert_snapshot(&snap).await.unwrap();

    let list = store.list_snapshots().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].rune_name, "echo");
    assert_eq!(list[0].version, "0.1.0");

    // Update same rune
    let mut snap2 = make_snapshot("echo");
    snap2.version = "0.2.0".to_string();
    snap2.supports_stream = true;
    store.upsert_snapshot(&snap2).await.unwrap();

    let list = store.list_snapshots().await.unwrap();
    assert_eq!(list.len(), 1, "upsert should not duplicate");
    assert_eq!(list[0].version, "0.2.0");
    assert!(list[0].supports_stream);
}

#[tokio::test]
async fn test_list_snapshots() {
    let store = new_store();
    store
        .upsert_snapshot(&make_snapshot("alpha"))
        .await
        .unwrap();
    store.upsert_snapshot(&make_snapshot("beta")).await.unwrap();
    store
        .upsert_snapshot(&make_snapshot("gamma"))
        .await
        .unwrap();

    let list = store.list_snapshots().await.unwrap();
    assert_eq!(list.len(), 3);
    // Ordered by rune_name
    assert_eq!(list[0].rune_name, "alpha");
    assert_eq!(list[1].rune_name, "beta");
    assert_eq!(list[2].rune_name, "gamma");
}

#[tokio::test]
async fn test_snapshot_preserves_all_fields() {
    let store = new_store();
    let snap = RuneSnapshot {
        rune_name: "full_test".to_string(),
        version: "1.2.3".to_string(),
        description: "A fully specified rune with special chars: 你好 & <xml>".to_string(),
        supports_stream: true,
        gate_path: "/api/v1/full_test".to_string(),
        gate_method: "PUT".to_string(),
        last_seen: String::new(),
    };
    store.upsert_snapshot(&snap).await.unwrap();

    let list = store.list_snapshots().await.unwrap();
    let s = &list[0];
    assert_eq!(s.rune_name, "full_test");
    assert_eq!(s.version, "1.2.3");
    assert!(s.description.contains("你好"));
    assert!(s.description.contains("<xml>"));
    assert!(s.supports_stream);
    assert_eq!(s.gate_path, "/api/v1/full_test");
    assert_eq!(s.gate_method, "PUT");
    assert!(!s.last_seen.is_empty(), "last_seen should be set by upsert");
}

#[tokio::test]
async fn test_upsert_updates_last_seen() {
    let store = new_store();
    store
        .upsert_snapshot(&make_snapshot("ticker"))
        .await
        .unwrap();
    let first = store.list_snapshots().await.unwrap();
    let first_seen = first[0].last_seen.clone();

    // Small delay to ensure timestamp differs
    std::thread::sleep(std::time::Duration::from_millis(1100));

    store
        .upsert_snapshot(&make_snapshot("ticker"))
        .await
        .unwrap();
    let second = store.list_snapshots().await.unwrap();
    let second_seen = second[0].last_seen.clone();

    assert!(
        second_seen >= first_seen,
        "last_seen should be updated: {} >= {}",
        second_seen,
        first_seen
    );
}

// ============================================================
// B5: Additional coverage
// ============================================================

/// Helper: create a CallLog with a specific latency_ms.
fn make_log_with_latency(
    rune_name: &str,
    request_id: &str,
    timestamp: &str,
    latency_ms: i64,
) -> CallLog {
    CallLog {
        id: 0,
        request_id: request_id.to_string(),
        rune_name: rune_name.to_string(),
        mode: "sync".to_string(),
        caster_id: Some("caster-1".to_string()),
        latency_ms,
        status_code: 200,
        input_size: 100,
        output_size: 200,
        timestamp: timestamp.to_string(),
    }
}

// ------ 1. call_stats() direct test ------

#[tokio::test]
async fn test_call_stats_returns_correct_counts_and_avg_latency() {
    let store = new_store();

    // Insert logs for "alpha": latencies 10, 20, 30 → avg = 20, count = 3
    store
        .insert_log(&make_log_with_latency(
            "alpha",
            "a1",
            "2026-01-01T00:00:01Z",
            10,
        ))
        .await
        .unwrap();
    store
        .insert_log(&make_log_with_latency(
            "alpha",
            "a2",
            "2026-01-01T00:00:02Z",
            20,
        ))
        .await
        .unwrap();
    store
        .insert_log(&make_log_with_latency(
            "alpha",
            "a3",
            "2026-01-01T00:00:03Z",
            30,
        ))
        .await
        .unwrap();

    // Insert logs for "beta": latencies 100, 200 → avg = 150, count = 2
    store
        .insert_log(&make_log_with_latency(
            "beta",
            "b1",
            "2026-01-01T00:00:04Z",
            100,
        ))
        .await
        .unwrap();
    store
        .insert_log(&make_log_with_latency(
            "beta",
            "b2",
            "2026-01-01T00:00:05Z",
            200,
        ))
        .await
        .unwrap();

    let (total, by_rune) = store.call_stats().await.unwrap();
    assert_eq!(total, 5);
    assert_eq!(by_rune.len(), 2);

    // Ordered by count desc: alpha(3) then beta(2)
    assert_eq!(by_rune[0].0, "alpha");
    assert_eq!(by_rune[0].1, 3); // count
    assert_eq!(by_rune[0].2, 20); // avg latency

    assert_eq!(by_rune[1].0, "beta");
    assert_eq!(by_rune[1].1, 2); // count
    assert_eq!(by_rune[1].2, 150); // avg latency
}

#[tokio::test]
async fn test_call_stats_empty_table() {
    let store = new_store();
    let (total, by_rune) = store.call_stats().await.unwrap();
    assert_eq!(total, 0);
    assert!(by_rune.is_empty());
}

// ------ 2. Task duplicate insert ------

#[tokio::test]
async fn test_task_duplicate_insert_returns_error() {
    let store = new_store();
    store
        .insert_task("dup-1", "rune_a", Some("first"))
        .await
        .unwrap();

    // task_id is PRIMARY KEY, so inserting the same id again should fail
    // with a UNIQUE constraint violation.
    let result = store.insert_task("dup-1", "rune_b", Some("second")).await;
    assert!(
        result.is_err(),
        "Duplicate task_id insert should fail because task_id is PRIMARY KEY"
    );

    // Original task should remain intact
    let task = store.get_task("dup-1").await.unwrap().unwrap();
    assert_eq!(task.rune_name, "rune_a");
    assert_eq!(task.input.as_deref(), Some("first"));
}

// ------ 3. Task illegal state transition: Completed → Running ------

#[tokio::test]
async fn test_task_illegal_state_transition_completed_to_running() {
    let store = new_store();
    store
        .insert_task("illegal-1", "rune_a", None)
        .await
        .unwrap();
    store
        .update_task_status("illegal-1", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("illegal-1", TaskStatus::Completed, Some("done"), None)
        .await
        .unwrap();

    // NOTE: The current implementation does NOT enforce state machine transitions.
    // It allows any status to be set at any time via a plain SQL UPDATE.
    // Completed → Running should ideally be rejected, but currently it succeeds.
    let result = store
        .update_task_status("illegal-1", TaskStatus::Running, None, None)
        .await;
    assert!(
        result.is_ok(),
        "Current impl allows arbitrary state transitions (no state machine guard)"
    );

    let task = store.get_task("illegal-1").await.unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Running);
    // started_at gets overwritten by the second Running transition
    assert!(task.started_at.is_some());
}

// ------ 4. cleanup_logs on empty table ------

#[tokio::test]
async fn test_cleanup_logs_empty_table() {
    let store = new_store();
    // Should not panic or return error when no logs exist
    let deleted = store
        .cleanup_logs_before("2099-12-31T23:59:59Z")
        .await
        .unwrap();
    assert_eq!(deleted, 0);
}

// ------ 5. Concurrent key creation and verification ------

#[tokio::test]
async fn test_concurrent_key_create_and_verify() {
    let store = Arc::new(new_store());
    let num_threads = 10;
    let mut handles = vec![];

    for i in 0..num_threads {
        let store = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            // Each thread creates a key and immediately verifies it
            let label = format!("concurrent-key-{}", i);
            let result = store.create_key(KeyType::Gate, &label).await.unwrap();
            let verified = store
                .verify_key(&result.raw_key, KeyType::Gate)
                .await
                .unwrap();
            assert!(
                verified.is_some(),
                "Key created in thread {} should be verifiable",
                i
            );
            assert_eq!(verified.unwrap().label, label);
            result.raw_key
        });
        handles.push(handle);
    }

    let mut raw_keys = Vec::new();
    for h in handles {
        raw_keys.push(h.await.unwrap());
    }

    // All keys should be distinct
    let mut unique = raw_keys.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        num_threads,
        "All generated keys should be unique"
    );

    // Verify all keys are still valid after all threads finish
    for raw_key in &raw_keys {
        let v = store.verify_key(raw_key, KeyType::Gate).await.unwrap();
        assert!(v.is_some());
    }

    let keys = store.list_keys().await.unwrap();
    assert_eq!(keys.len(), num_threads);
}

// ------ 6. Revoke key then create new key with same label ------

#[tokio::test]
async fn test_revoke_key_then_create_same_label() {
    let store = new_store();

    // Create first key
    let first = store
        .create_key(KeyType::Gate, "reusable-label")
        .await
        .unwrap();
    let first_raw = first.raw_key.clone();
    let first_id = first.api_key.id;

    // Revoke it
    store.revoke_key(first_id).await.unwrap();

    // Old key should be invalid
    assert!(store
        .verify_key(&first_raw, KeyType::Gate)
        .await
        .unwrap()
        .is_none());

    // Create a new key with the same label
    let second = store
        .create_key(KeyType::Gate, "reusable-label")
        .await
        .unwrap();
    let second_raw = second.raw_key.clone();

    // New key should be valid
    let verified = store.verify_key(&second_raw, KeyType::Gate).await.unwrap();
    assert!(
        verified.is_some(),
        "Newly created key with same label should be valid"
    );
    assert_eq!(verified.unwrap().label, "reusable-label");

    // Old key should still be invalid
    assert!(
        store
            .verify_key(&first_raw, KeyType::Gate)
            .await
            .unwrap()
            .is_none(),
        "Revoked key should remain invalid after creating a new key with same label"
    );

    // Both should appear in the key list
    let keys = store.list_keys().await.unwrap();
    assert_eq!(keys.len(), 2);
    let labels: Vec<&str> = keys.iter().map(|k| k.label.as_str()).collect();
    assert_eq!(labels, vec!["reusable-label", "reusable-label"]);
}

// ------ 7. Very long label (1000 chars) ------

#[tokio::test]
async fn test_key_with_very_long_label() {
    let store = new_store();
    let long_label = "a".repeat(1000);
    let result = store
        .create_key(KeyType::Caster, &long_label)
        .await
        .unwrap();

    // Verify the key works
    let verified = store
        .verify_key(&result.raw_key, KeyType::Caster)
        .await
        .unwrap();
    assert!(verified.is_some());
    assert_eq!(verified.unwrap().label.len(), 1000);

    // Verify it appears correctly in list
    let keys = store.list_keys().await.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].label.len(), 1000);
}

// ------ 8. Special character labels (emoji, Chinese, SQL injection) ------

#[tokio::test]
async fn test_key_with_emoji_label() {
    let store = new_store();
    let label = "rocket-key-\u{1F680}\u{2728}\u{1F525}";
    let result = store.create_key(KeyType::Gate, label).await.unwrap();
    let verified = store
        .verify_key(&result.raw_key, KeyType::Gate)
        .await
        .unwrap();
    assert!(verified.is_some());
    assert_eq!(verified.unwrap().label, label);
}

#[tokio::test]
async fn test_key_with_chinese_label() {
    let store = new_store();
    let label =
        "\u{4F60}\u{597D}\u{4E16}\u{754C}\u{FF0C}\u{8FD9}\u{662F}\u{4E00}\u{4E2A}\u{6D4B}\u{8BD5}";
    let result = store.create_key(KeyType::Gate, label).await.unwrap();
    let verified = store
        .verify_key(&result.raw_key, KeyType::Gate)
        .await
        .unwrap();
    assert!(verified.is_some());
    assert_eq!(verified.unwrap().label, label);
}

#[tokio::test]
async fn test_key_with_sql_injection_label() {
    let store = new_store();

    // Attempt various SQL injection patterns as labels
    let injection_labels = vec![
        "' OR 1=1 --",
        "'; DROP TABLE api_keys; --",
        "\" OR \"\"=\"",
        "1; SELECT * FROM api_keys",
        "label' UNION SELECT key_hash FROM api_keys --",
    ];

    for (i, label) in injection_labels.iter().enumerate() {
        let result = store.create_key(KeyType::Gate, label).await.unwrap();
        let verified = store
            .verify_key(&result.raw_key, KeyType::Gate)
            .await
            .unwrap();
        assert!(
            verified.is_some(),
            "SQL injection label #{} should be stored safely",
            i
        );
        assert_eq!(
            verified.unwrap().label,
            *label,
            "SQL injection label #{} should be preserved verbatim",
            i
        );
    }

    // Verify the table is still intact
    let keys = store.list_keys().await.unwrap();
    assert_eq!(keys.len(), injection_labels.len());

    // Verify we can still create tasks (other tables not affected)
    store
        .insert_task("post-injection", "rune_a", None)
        .await
        .unwrap();
    assert!(store.get_task("post-injection").await.unwrap().is_some());
}

// ------ 9. Task filter: status + rune_name combined ------

#[tokio::test]
async fn test_list_tasks_filter_by_status_and_rune_name() {
    let store = new_store();

    // Create tasks across two runes with different statuses
    store.insert_task("t1", "alpha", None).await.unwrap(); // pending, alpha
    store.insert_task("t2", "alpha", None).await.unwrap(); // running, alpha
    store.insert_task("t3", "beta", None).await.unwrap(); // pending, beta
    store.insert_task("t4", "alpha", None).await.unwrap(); // completed, alpha
    store.insert_task("t5", "beta", None).await.unwrap(); // running, beta

    store
        .update_task_status("t2", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("t4", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("t4", TaskStatus::Completed, Some("done"), None)
        .await
        .unwrap();
    store
        .update_task_status("t5", TaskStatus::Running, None, None)
        .await
        .unwrap();

    // Filter: pending + alpha → only t1
    let result = store
        .list_tasks(Some(TaskStatus::Pending), Some("alpha"), 100, 0)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].task_id, "t1");

    // Filter: running + alpha → only t2
    let result = store
        .list_tasks(Some(TaskStatus::Running), Some("alpha"), 100, 0)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].task_id, "t2");

    // Filter: running + beta → only t5
    let result = store
        .list_tasks(Some(TaskStatus::Running), Some("beta"), 100, 0)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].task_id, "t5");

    // Filter: completed + alpha → only t4
    let result = store
        .list_tasks(Some(TaskStatus::Completed), Some("alpha"), 100, 0)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].task_id, "t4");

    // Filter: completed + beta → none
    let result = store
        .list_tasks(Some(TaskStatus::Completed), Some("beta"), 100, 0)
        .await
        .unwrap();
    assert!(result.is_empty());

    // Filter: pending + nonexistent rune → none
    let result = store
        .list_tasks(Some(TaskStatus::Pending), Some("nonexistent"), 100, 0)
        .await
        .unwrap();
    assert!(result.is_empty());
}

// ------ 10. Log query for nonexistent rune_name returns empty ------

#[tokio::test]
async fn test_query_logs_nonexistent_rune_returns_empty() {
    let store = new_store();

    // Insert some logs for existing runes
    store
        .insert_log(&make_log("alpha", "r1", "2026-01-01T00:00:01Z"))
        .await
        .unwrap();
    store
        .insert_log(&make_log("beta", "r2", "2026-01-01T00:00:02Z"))
        .await
        .unwrap();

    // Query for a rune that has no logs
    let result = store
        .query_logs(Some("nonexistent_rune"), 100)
        .await
        .unwrap();
    assert!(
        result.is_empty(),
        "Querying logs for a nonexistent rune should return empty vec"
    );
}

#[tokio::test]
async fn test_query_logs_empty_store_returns_empty() {
    let store = new_store();

    // No logs inserted at all
    let result = store.query_logs(Some("anything"), 100).await.unwrap();
    assert!(result.is_empty());

    let result = store.query_logs(None, 100).await.unwrap();
    assert!(result.is_empty());
}

// ============================================================
// B6: Extended boundary, error, and state-combination tests
// ============================================================

// ------ Key module: double revoke ------

#[tokio::test]
async fn test_double_revoke_is_idempotent() {
    // Revoking an already-revoked key should be a no-op: no error, revoked_at unchanged.
    let store = new_store();
    let result = store
        .create_key(KeyType::Gate, "double-revoke")
        .await
        .unwrap();
    let key_id = result.api_key.id;

    store.revoke_key(key_id).await.unwrap();
    let keys_after_first = store.list_keys().await.unwrap();
    let revoked_at_first = keys_after_first[0].revoked_at.clone().unwrap();

    // Small delay to ensure timestamp would differ if re-written
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Second revoke — should succeed without error
    store.revoke_key(key_id).await.unwrap();
    let keys_after_second = store.list_keys().await.unwrap();
    let revoked_at_second = keys_after_second[0].revoked_at.clone().unwrap();

    // revoked_at must remain the same (idempotent)
    assert_eq!(
        revoked_at_first, revoked_at_second,
        "Double revoke must not update revoked_at"
    );
}

// ------ Key module: verify a never-existing key with valid prefix ------

#[tokio::test]
async fn test_verify_nonexistent_key_with_valid_prefix() {
    // A well-formed key that was never created should return None, not error.
    let store = new_store();
    let fake_key = "rk_00000000000000000000000000000000";
    assert_eq!(fake_key.len(), 35);
    let verified = store.verify_key(fake_key, KeyType::Gate).await.unwrap();
    assert!(
        verified.is_none(),
        "A never-created key should verify as None"
    );
}

// ------ Key module: concurrent revoke same key ------

#[tokio::test]
async fn test_concurrent_revoke_same_key() {
    // Multiple threads revoking the same key concurrently: no panic, no error.
    let store = Arc::new(new_store());
    let result = store
        .create_key(KeyType::Gate, "concurrent-revoke")
        .await
        .unwrap();
    let key_id = result.api_key.id;

    let mut handles = vec![];
    for _ in 0..10 {
        let store = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            store.revoke_key(key_id).await.unwrap();
        });
        handles.push(handle);
    }
    for h in handles {
        h.await.unwrap();
    }

    // Key should be revoked
    let verified = store
        .verify_key(&result.raw_key, KeyType::Gate)
        .await
        .unwrap();
    assert!(
        verified.is_none(),
        "Key should be revoked after concurrent revoke"
    );
}

// ------ Key module: create 100 keys then list all ------

#[tokio::test]
async fn test_create_100_keys_then_list_all() {
    let store = new_store();
    for i in 0..100 {
        store
            .create_key(KeyType::Gate, &format!("key-{}", i))
            .await
            .unwrap();
    }

    let keys = store.list_keys().await.unwrap();
    assert_eq!(keys.len(), 100, "All 100 keys should appear in list");
}

// ------ Key module: key_hash uniqueness ------

#[tokio::test]
async fn test_key_hash_uniqueness() {
    // Two independently created keys must have different underlying hashes.
    // We verify this indirectly: both raw keys are different and both verify independently.
    let store = new_store();
    let r1 = store.create_key(KeyType::Gate, "unique-1").await.unwrap();
    let r2 = store.create_key(KeyType::Gate, "unique-2").await.unwrap();

    assert_ne!(r1.raw_key, r2.raw_key, "Raw keys must differ");
    // key_hash from CreateKeyResult is available
    assert_ne!(
        r1.api_key.key_hash, r2.api_key.key_hash,
        "Key hashes must differ"
    );

    // Each key should only verify as itself
    let v1 = store
        .verify_key(&r1.raw_key, KeyType::Gate)
        .await
        .unwrap()
        .unwrap();
    let v2 = store
        .verify_key(&r2.raw_key, KeyType::Gate)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(v1.id, v2.id);
}

// ------ Task module: status transition matrix ------

#[tokio::test]
async fn test_task_status_transition_matrix_legal() {
    // Test all legal transitions:
    //   pending -> running
    //   running -> completed
    //   running -> failed
    //   running -> cancelled
    //   pending -> cancelled
    let store = new_store();

    // pending -> running
    store.insert_task("tr-pr", "rune_a", None).await.unwrap();
    store
        .update_task_status("tr-pr", TaskStatus::Running, None, None)
        .await
        .unwrap();
    let t = store.get_task("tr-pr").await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Running);
    assert!(t.started_at.is_some());

    // running -> completed
    store.insert_task("tr-rc", "rune_a", None).await.unwrap();
    store
        .update_task_status("tr-rc", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("tr-rc", TaskStatus::Completed, Some("ok"), None)
        .await
        .unwrap();
    let t = store.get_task("tr-rc").await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Completed);
    assert!(t.completed_at.is_some());
    assert_eq!(t.output.as_deref(), Some("ok"));

    // running -> failed
    store.insert_task("tr-rf", "rune_a", None).await.unwrap();
    store
        .update_task_status("tr-rf", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("tr-rf", TaskStatus::Failed, None, Some("boom"))
        .await
        .unwrap();
    let t = store.get_task("tr-rf").await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Failed);
    assert!(t.completed_at.is_some());
    assert_eq!(t.error.as_deref(), Some("boom"));

    // running -> cancelled
    store.insert_task("tr-rx", "rune_a", None).await.unwrap();
    store
        .update_task_status("tr-rx", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("tr-rx", TaskStatus::Cancelled, None, None)
        .await
        .unwrap();
    let t = store.get_task("tr-rx").await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Cancelled);
    assert!(t.completed_at.is_some());

    // pending -> cancelled (skip running)
    store.insert_task("tr-pc", "rune_a", None).await.unwrap();
    store
        .update_task_status("tr-pc", TaskStatus::Cancelled, None, None)
        .await
        .unwrap();
    let t = store.get_task("tr-pc").await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Cancelled);
    assert!(t.completed_at.is_some());
}

// ------ Task module: illegal transitions ------

#[tokio::test]
async fn test_task_illegal_transitions_documented() {
    // NOTE: The current implementation does NOT enforce a state machine.
    // All transitions below should ideally be rejected but currently succeed.
    // This test documents the current permissive behavior.
    let store = new_store();

    // failed -> running (illegal)
    store.insert_task("ill-fr", "rune_a", None).await.unwrap();
    store
        .update_task_status("ill-fr", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("ill-fr", TaskStatus::Failed, None, Some("err"))
        .await
        .unwrap();
    let result = store
        .update_task_status("ill-fr", TaskStatus::Running, None, None)
        .await;
    // KNOWN: Should return an error, but current impl has no state machine guard.
    assert!(
        result.is_ok(),
        "Current impl allows failed->running (no state machine guard)"
    );
    let t = store.get_task("ill-fr").await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Running);

    // cancelled -> completed (illegal)
    store.insert_task("ill-cc", "rune_a", None).await.unwrap();
    store
        .update_task_status("ill-cc", TaskStatus::Cancelled, None, None)
        .await
        .unwrap();
    let result = store
        .update_task_status("ill-cc", TaskStatus::Completed, Some("late"), None)
        .await;
    // KNOWN: Should return an error, but current impl has no state machine guard.
    assert!(
        result.is_ok(),
        "Current impl allows cancelled->completed (no state machine guard)"
    );
    let t = store.get_task("ill-cc").await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Completed);

    // completed -> pending (illegal)
    store.insert_task("ill-cp", "rune_a", None).await.unwrap();
    store
        .update_task_status("ill-cp", TaskStatus::Running, None, None)
        .await
        .unwrap();
    store
        .update_task_status("ill-cp", TaskStatus::Completed, Some("done"), None)
        .await
        .unwrap();
    let result = store
        .update_task_status("ill-cp", TaskStatus::Pending, None, None)
        .await;
    // KNOWN: Should return an error, but current impl has no state machine guard.
    assert!(
        result.is_ok(),
        "Current impl allows completed->pending (no state machine guard)"
    );
    let t = store.get_task("ill-cp").await.unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Pending);
}

// ------ Task module: input/output with special content ------

#[tokio::test]
async fn test_task_input_with_unicode() {
    let store = new_store();
    let unicode_input = r#"{"msg": "你好世界 🚀 Ñoño"}"#;
    let task = store
        .insert_task("uni-1", "rune_a", Some(unicode_input))
        .await
        .unwrap();
    assert_eq!(task.input.as_deref(), Some(unicode_input));

    let fetched = store.get_task("uni-1").await.unwrap().unwrap();
    assert_eq!(fetched.input.as_deref(), Some(unicode_input));
}

#[tokio::test]
async fn test_task_output_with_100kb_json() {
    let store = new_store();
    store.insert_task("big-json", "rune_a", None).await.unwrap();

    // Build a ~100KB JSON string
    let big_data = "x".repeat(100_000);
    let big_json = format!(r#"{{"data": "{}"}}"#, big_data);
    assert!(big_json.len() > 100_000);

    store
        .update_task_status("big-json", TaskStatus::Completed, Some(&big_json), None)
        .await
        .unwrap();
    let t = store.get_task("big-json").await.unwrap().unwrap();
    assert_eq!(t.output.as_deref(), Some(big_json.as_str()));
}

#[tokio::test]
async fn test_task_input_deeply_nested_json() {
    let store = new_store();

    // Build a 100-level nested JSON object: {"a":{"a":{..."a":1}...}}
    let mut json = String::from("1");
    for _ in 0..100 {
        json = format!(r#"{{"a":{}}}"#, json);
    }

    let task = store
        .insert_task("nested-100", "rune_a", Some(&json))
        .await
        .unwrap();
    assert_eq!(task.input.as_deref(), Some(json.as_str()));

    let fetched = store.get_task("nested-100").await.unwrap().unwrap();
    // Verify it round-trips correctly
    let parsed: serde_json::Value = serde_json::from_str(fetched.input.as_ref().unwrap()).unwrap();
    // Traverse 100 levels of "a" to reach the inner value
    let mut val = &parsed;
    for _ in 0..100 {
        val = &val["a"];
    }
    assert_eq!(val, &serde_json::json!(1));
}

// ------ Task module: list ordering ------

#[tokio::test]
async fn test_task_list_ordered_by_created_at_desc() {
    // Timestamp resolution is 1 second, so we need >1s delay between inserts
    // to guarantee different created_at values.
    let store = new_store();

    store.insert_task("order-1", "rune_a", None).await.unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    store.insert_task("order-2", "rune_a", None).await.unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    store.insert_task("order-3", "rune_a", None).await.unwrap();

    let tasks = store.list_tasks(None, None, 100, 0).await.unwrap();
    assert_eq!(tasks.len(), 3);

    // Verify created_at values are actually distinct
    assert_ne!(
        tasks[0].created_at, tasks[2].created_at,
        "Tasks must have distinct timestamps for this test to be meaningful"
    );

    // ORDER BY created_at DESC => newest first
    assert_eq!(tasks[0].task_id, "order-3");
    assert_eq!(tasks[1].task_id, "order-2");
    assert_eq!(tasks[2].task_id, "order-1");
}

// ------ Task module: offset beyond range ------

#[tokio::test]
async fn test_task_list_offset_beyond_total() {
    let store = new_store();
    store.insert_task("off-1", "rune_a", None).await.unwrap();
    store.insert_task("off-2", "rune_a", None).await.unwrap();

    let result = store.list_tasks(None, None, 100, 999).await.unwrap();
    assert!(result.is_empty(), "Offset beyond total should return empty");
}

// ------ Log module: exact boundary cleanup ------

#[tokio::test]
async fn test_cleanup_exact_boundary_timestamp() {
    // cleanup_logs_before uses strict "<", so a log with the exact boundary timestamp
    // should NOT be deleted.
    let store = new_store();
    let boundary = "2025-06-01T00:00:00Z";

    store
        .insert_log(&make_log("rune_a", "before", "2025-05-31T23:59:59Z"))
        .await
        .unwrap();
    store
        .insert_log(&make_log("rune_a", "exact", boundary))
        .await
        .unwrap();
    store
        .insert_log(&make_log("rune_a", "after", "2025-06-01T00:00:01Z"))
        .await
        .unwrap();

    let deleted = store.cleanup_logs_before(boundary).await.unwrap();
    // Only "before" should be deleted (strict <)
    assert_eq!(
        deleted, 1,
        "Only logs strictly before the boundary should be deleted"
    );

    let remaining = store.query_logs(None, 100).await.unwrap();
    assert_eq!(remaining.len(), 2);
    let ids: Vec<&str> = remaining.iter().map(|l| l.request_id.as_str()).collect();
    assert!(ids.contains(&"exact"), "Exact boundary log should remain");
    assert!(ids.contains(&"after"), "After boundary log should remain");
}

// ------ Log module: large query with limit ------

#[tokio::test]
async fn test_query_1000_logs_with_limit_10() {
    let store = new_store();
    for i in 0..1000 {
        store
            .insert_log(&make_log(
                "rune_a",
                &format!("req-{}", i),
                &format!(
                    "2026-01-01T{:02}:{:02}:{:02}Z",
                    i / 3600,
                    (i % 3600) / 60,
                    i % 60
                ),
            ))
            .await
            .unwrap();
    }

    let logs = store.query_logs(None, 10).await.unwrap();
    assert_eq!(logs.len(), 10, "Limit should cap results at 10");
}

// ------ Log module: all three modes ------

#[tokio::test]
async fn test_log_mode_all_variants() {
    // The call_logs table has CHECK(mode IN ('sync', 'stream', 'async'))
    let store = new_store();

    for mode in &["sync", "stream", "async"] {
        let log = CallLog {
            id: 0,
            request_id: format!("req-{}", mode),
            rune_name: "rune_a".to_string(),
            mode: mode.to_string(),
            caster_id: None,
            latency_ms: 10,
            status_code: 200,
            input_size: 50,
            output_size: 100,
            timestamp: "2026-01-01T00:00:00Z".to_string(),
        };
        store.insert_log(&log).await.unwrap();
    }

    let logs = store.query_logs(None, 100).await.unwrap();
    assert_eq!(logs.len(), 3);
    let modes: Vec<&str> = logs.iter().map(|l| l.mode.as_str()).collect();
    assert!(modes.contains(&"sync"));
    assert!(modes.contains(&"stream"));
    assert!(modes.contains(&"async"));
}

// ------ Log module: latency boundary values ------

#[tokio::test]
async fn test_log_latency_zero() {
    let store = new_store();
    let log = make_log_with_latency("rune_a", "lat-0", "2026-01-01T00:00:00Z", 0);
    store.insert_log(&log).await.unwrap();

    let logs = store.query_logs(None, 10).await.unwrap();
    assert_eq!(logs[0].latency_ms, 0);
}

#[tokio::test]
async fn test_log_latency_i64_max() {
    let store = new_store();
    let log = make_log_with_latency("rune_a", "lat-max", "2026-01-01T00:00:00Z", i64::MAX);
    store.insert_log(&log).await.unwrap();

    let logs = store.query_logs(None, 10).await.unwrap();
    assert_eq!(logs[0].latency_ms, i64::MAX);
}

// ------ Log module: various status_code values ------

#[tokio::test]
async fn test_log_status_codes() {
    let store = new_store();
    let codes = [200, 400, 401, 404, 409, 500];

    for (i, &code) in codes.iter().enumerate() {
        let mut log = make_log(
            "rune_a",
            &format!("sc-{}", i),
            &format!("2026-01-01T00:00:{:02}Z", i),
        );
        log.status_code = code;
        store.insert_log(&log).await.unwrap();
    }

    let logs = store.query_logs(None, 100).await.unwrap();
    assert_eq!(logs.len(), codes.len());
    let stored_codes: Vec<i32> = logs.iter().map(|l| l.status_code).collect();
    for &code in &codes {
        assert!(
            stored_codes.contains(&code),
            "Status code {} should be present",
            code
        );
    }
}

// ------ Snapshot module: 50 different snapshots ------

#[tokio::test]
async fn test_50_snapshots_list_all() {
    let store = new_store();
    for i in 0..50 {
        store
            .upsert_snapshot(&make_snapshot(&format!("rune-{:03}", i)))
            .await
            .unwrap();
    }

    let list = store.list_snapshots().await.unwrap();
    assert_eq!(list.len(), 50, "All 50 snapshots should appear in list");
    // Verify alphabetical ordering
    for i in 1..list.len() {
        assert!(
            list[i].rune_name >= list[i - 1].rune_name,
            "Snapshots should be ordered by rune_name"
        );
    }
}

// ------ Snapshot module: empty optional fields ------

#[tokio::test]
async fn test_snapshot_with_empty_string_fields() {
    // gate_path and gate_method are NOT NULL in schema, but can be empty strings.
    // This tests the boundary of "no meaningful value" without NULL.
    let store = new_store();
    let snap = RuneSnapshot {
        rune_name: "minimal".to_string(),
        version: "0.0.1".to_string(),
        description: "".to_string(),
        supports_stream: false,
        gate_path: "".to_string(),
        gate_method: "".to_string(),
        last_seen: String::new(),
    };
    store.upsert_snapshot(&snap).await.unwrap();

    let list = store.list_snapshots().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].gate_path, "");
    assert_eq!(list[0].gate_method, "");
    assert_eq!(list[0].description, "");
}

// ------ Snapshot module: rune_name with special characters ------

#[tokio::test]
async fn test_snapshot_rune_name_special_chars() {
    let store = new_store();
    let names = ["my/rune", "my-rune", "my_rune", "中文rune"];

    for name in &names {
        store.upsert_snapshot(&make_snapshot(name)).await.unwrap();
    }

    let list = store.list_snapshots().await.unwrap();
    assert_eq!(list.len(), names.len());
    let stored_names: Vec<&str> = list.iter().map(|s| s.rune_name.as_str()).collect();
    for name in &names {
        assert!(
            stored_names.contains(name),
            "Rune name '{}' should be present in snapshot list",
            name
        );
    }
}

// ------ Snapshot module: rapid consecutive upsert ------

#[tokio::test]
async fn test_snapshot_rapid_upsert_10_times() {
    let store = new_store();

    for i in 0..10 {
        let mut snap = make_snapshot("rapid-rune");
        snap.version = format!("0.{}.0", i);
        snap.supports_stream = i % 2 == 0;
        store.upsert_snapshot(&snap).await.unwrap();
    }

    let list = store.list_snapshots().await.unwrap();
    assert_eq!(list.len(), 1, "Upsert should not create duplicates");
    // The last upsert should win
    assert_eq!(list[0].version, "0.9.0");
    // i=9, 9%2 != 0, so supports_stream = false
    assert!(!list[0].supports_stream);
}

// ------ Cross-module: store reopen persistence ------

#[tokio::test]
async fn test_store_reopen_data_persists() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("persist.db");

    // Phase 1: write data
    {
        let store = RuneStore::open(&db_path).unwrap();

        store
            .create_key(KeyType::Gate, "persist-key")
            .await
            .unwrap();
        store
            .insert_task("persist-task", "persist-rune", Some(r#"{"x":1}"#))
            .await
            .unwrap();
        store
            .insert_log(&make_log(
                "persist-rune",
                "persist-req",
                "2026-01-01T00:00:00Z",
            ))
            .await
            .unwrap();
        store
            .upsert_snapshot(&make_snapshot("persist-snap"))
            .await
            .unwrap();
    }
    // store is dropped (closed)

    // Phase 2: reopen and verify all data
    {
        let store = RuneStore::open(&db_path).unwrap();

        let keys = store.list_keys().await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].label, "persist-key");

        let task = store.get_task("persist-task").await.unwrap().unwrap();
        assert_eq!(task.rune_name, "persist-rune");
        assert_eq!(task.input.as_deref(), Some(r#"{"x":1}"#));

        let logs = store.query_logs(None, 100).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].request_id, "persist-req");

        let snaps = store.list_snapshots().await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].rune_name, "persist-snap");
    }
}

// ------ Cross-module: empty database all queries ------

#[tokio::test]
async fn test_empty_database_all_queries() {
    let store = new_store();

    // Keys
    let keys = store.list_keys().await.unwrap();
    assert!(keys.is_empty());

    // Verify non-existent key
    let v = store
        .verify_key("rk_aaaabbbbccccddddaaaabbbbccccdddd", KeyType::Gate)
        .await
        .unwrap();
    assert!(v.is_none());

    // Revoke non-existent key
    assert!(store.revoke_key(1).await.is_ok());

    // Tasks
    let tasks = store.list_tasks(None, None, 100, 0).await.unwrap();
    assert!(tasks.is_empty());

    let tasks_by_status = store
        .list_tasks(Some(TaskStatus::Pending), None, 100, 0)
        .await
        .unwrap();
    assert!(tasks_by_status.is_empty());

    let tasks_by_rune = store
        .list_tasks(None, Some("anything"), 100, 0)
        .await
        .unwrap();
    assert!(tasks_by_rune.is_empty());

    let task = store.get_task("nonexistent").await.unwrap();
    assert!(task.is_none());

    let update_result = store
        .update_task_status("ghost", TaskStatus::Running, None, None)
        .await;
    assert!(update_result.is_ok());

    // Logs
    let logs = store.query_logs(None, 100).await.unwrap();
    assert!(logs.is_empty());

    let logs_filtered = store.query_logs(Some("anything"), 100).await.unwrap();
    assert!(logs_filtered.is_empty());

    let deleted = store
        .cleanup_logs_before("2099-12-31T23:59:59Z")
        .await
        .unwrap();
    assert_eq!(deleted, 0);

    let (total, by_rune) = store.call_stats().await.unwrap();
    assert_eq!(total, 0);
    assert!(by_rune.is_empty());

    // Snapshots
    let snaps = store.list_snapshots().await.unwrap();
    assert!(snaps.is_empty());
}

// ============================================================
// Performance regression: call_stats_enhanced correctness
// Ensures optimized SQL produces identical results to N+1 version
// ============================================================

#[tokio::test]
async fn test_call_stats_enhanced_correctness() {
    let store = new_store();

    // Insert logs across 3 runes with known patterns
    // rune_a: 4 calls, all success (200), latencies: [10, 20, 30, 40]
    // rune_b: 3 calls, 1 failure (500), latencies: [5, 15, 25]
    // rune_c: 1 call, success, latency: [100]
    let logs = vec![
        ("rune_a", 10, 200),
        ("rune_a", 20, 200),
        ("rune_a", 30, 200),
        ("rune_a", 40, 200),
        ("rune_b", 5, 200),
        ("rune_b", 15, 500),
        ("rune_b", 25, 200),
        ("rune_c", 100, 200),
    ];

    for (i, (rune, latency, status)) in logs.iter().enumerate() {
        let log = CallLog {
            id: 0,
            request_id: format!("req-{}", i),
            rune_name: rune.to_string(),
            mode: "sync".into(),
            caster_id: None,
            latency_ms: *latency,
            status_code: *status,
            input_size: 100,
            output_size: 50,
            timestamp: format!("2026-03-23T00:00:{:02}Z", i),
        };
        store.insert_log(&log).await.unwrap();
    }

    let (total, stats) = store.call_stats_enhanced().await.unwrap();

    // Total should be 8
    assert_eq!(total, 8, "total call count");

    // Should have 3 runes
    assert_eq!(stats.len(), 3, "number of runes");

    // Find each rune's stats (name, count, avg_latency, success_rate, p95)
    let rune_a = stats
        .iter()
        .find(|s| s.0 == "rune_a")
        .expect("rune_a missing");
    let rune_b = stats
        .iter()
        .find(|s| s.0 == "rune_b")
        .expect("rune_b missing");
    let rune_c = stats
        .iter()
        .find(|s| s.0 == "rune_c")
        .expect("rune_c missing");

    // rune_a: count=4, avg=25, success_rate=1.0, p95=40
    assert_eq!(rune_a.1, 4, "rune_a count");
    assert_eq!(rune_a.2, 25, "rune_a avg_latency"); // (10+20+30+40)/4 = 25
    assert!(
        (rune_a.3 - 1.0).abs() < 0.001,
        "rune_a success_rate should be 1.0, got {}",
        rune_a.3
    );
    assert!(
        (rune_a.4 - 40.0).abs() < 0.001,
        "rune_a p95 should be 40.0, got {}",
        rune_a.4
    );

    // rune_b: count=3, avg=15, success_rate=2/3, p95=25
    assert_eq!(rune_b.1, 3, "rune_b count");
    assert_eq!(rune_b.2, 15, "rune_b avg_latency"); // (5+15+25)/3 = 15
    let expected_rate = 2.0 / 3.0;
    assert!(
        (rune_b.3 - expected_rate).abs() < 0.001,
        "rune_b success_rate should be ~0.667, got {}",
        rune_b.3
    );
    assert!(
        (rune_b.4 - 25.0).abs() < 0.001,
        "rune_b p95 should be 25.0, got {}",
        rune_b.4
    );

    // rune_c: count=1, avg=100, success_rate=1.0, p95=100
    assert_eq!(rune_c.1, 1, "rune_c count");
    assert_eq!(rune_c.2, 100, "rune_c avg_latency");
    assert!((rune_c.3 - 1.0).abs() < 0.001, "rune_c success_rate");
    assert!((rune_c.4 - 100.0).abs() < 0.001, "rune_c p95");
}

#[tokio::test]
async fn test_call_stats_enhanced_empty() {
    let store = new_store();
    let (total, stats) = store.call_stats_enhanced().await.unwrap();
    assert_eq!(total, 0);
    assert!(stats.is_empty());
}

#[tokio::test]
async fn test_call_stats_enhanced_many_runes() {
    let store = new_store();

    // Insert 200 logs across 5 runes (same pattern as benchmark)
    for i in 0..200u32 {
        let log = CallLog {
            id: 0,
            request_id: format!("req-{}", i),
            rune_name: format!("rune_{}", i % 5),
            mode: "sync".into(),
            caster_id: None,
            latency_ms: (i % 100 + 1) as i64,
            status_code: if i % 20 == 0 { 500 } else { 200 },
            input_size: 100,
            output_size: 50,
            timestamp: format!("2026-03-23T00:{:02}:{:02}Z", (i / 60) % 60, i % 60),
        };
        store.insert_log(&log).await.unwrap();
    }

    let (total, stats) = store.call_stats_enhanced().await.unwrap();
    assert_eq!(total, 200);
    assert_eq!(stats.len(), 5);

    // Each rune should have 40 calls
    for stat in &stats {
        assert_eq!(
            stat.1, 40,
            "each rune should have 40 calls, {} has {}",
            stat.0, stat.1
        );
        // success_rate: i%20==0 gives status 500, each rune gets different distributions
        // rune_0 (i%5==0): 10 failures (i=0,20,40,...,180), rate=30/40=0.75
        // rune_1..4: fewer failures since i%20==0 AND i%5==k requires i%lcm(20,5)==k%5
        assert!(
            stat.3 > 0.5,
            "success rate should be > 50%, {} got {}",
            stat.0,
            stat.3
        );
        assert!(stat.3 <= 1.0, "success rate should be <= 1.0");
        // p95 should be > 0
        assert!(stat.4 > 0.0, "p95 should be > 0");
    }
}

// ============================================================
// Performance regression: schema validator caching
// Ensures cached validation returns identical results
// ============================================================

// ============================================================
// Issue 1 Regression: store operations use spawn_blocking
// ============================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_store_operations_dont_block_async_runtime() {
    let store = Arc::new(new_store());

    let store_clone = Arc::clone(&store);
    let store_handle = tokio::spawn(async move {
        store_clone
            .insert_task("blocking-test", "echo", Some("data"))
            .await
            .unwrap();
        store_clone.get_task("blocking-test").await.unwrap()
    });

    let fast_handle = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        42u64
    });

    let task = store_handle.await.unwrap();
    assert!(task.is_some());
    assert_eq!(task.unwrap().task_id, "blocking-test");

    let fast_result = fast_handle.await.unwrap();
    assert_eq!(fast_result, 42);
}

// ============================================================
// B8: Mutex poisoning resilience
// ============================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_store_survives_poisoned_mutex() {
    // Simulate a panic inside a spawn_blocking closure that holds the Mutex.
    // After the panic, the Mutex becomes poisoned. Subsequent operations on
    // the same store should still succeed (not propagate the poison panic).
    let store = Arc::new(new_store());

    // Insert a task before the poison event
    store
        .insert_task("before-poison", "rune_a", None)
        .await
        .unwrap();

    // Poison the mutex by panicking while holding the lock
    store.poison_mutex();

    // Now the Mutex is poisoned. Verify that subsequent operations still work.
    let task = store.get_task("before-poison").await.unwrap();
    assert!(
        task.is_some(),
        "Should be able to read from store after mutex poisoning"
    );
    assert_eq!(task.unwrap().task_id, "before-poison");

    // Insert a new task after poisoning
    let insert_result = store
        .insert_task("after-poison", "rune_b", Some("{}"))
        .await;
    assert!(
        insert_result.is_ok(),
        "Should be able to insert after mutex poisoning"
    );

    // Verify the new task is readable
    let task2 = store.get_task("after-poison").await.unwrap();
    assert!(task2.is_some());
    assert_eq!(task2.unwrap().rune_name, "rune_b");

    // Verify other operations also work
    let logs_result = store.query_logs(None, 10).await;
    assert!(
        logs_result.is_ok(),
        "query_logs should work after mutex poisoning"
    );

    let keys_result = store.list_keys().await;
    assert!(
        keys_result.is_ok(),
        "list_keys should work after mutex poisoning"
    );

    let snapshots_result = store.list_snapshots().await;
    assert!(
        snapshots_result.is_ok(),
        "list_snapshots should work after mutex poisoning"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_store_operations() {
    // Verify that many concurrent operations across different modules
    // do not deadlock or panic.
    let store = Arc::new(new_store());

    let mut handles = vec![];

    // Spawn 10 task inserters
    for i in 0..10 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            s.insert_task(&format!("conc-task-{}", i), "rune_a", Some("{}"))
                .await
                .unwrap();
        }));
    }

    // Spawn 10 log inserters
    for i in 0..10 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let log = CallLog {
                id: 0,
                request_id: format!("conc-req-{}", i),
                rune_name: "rune_a".to_string(),
                mode: "sync".to_string(),
                caster_id: None,
                latency_ms: 10 + i as i64,
                status_code: 200,
                input_size: 50,
                output_size: 100,
                timestamp: "2026-01-01T00:00:00Z".to_string(),
            };
            s.insert_log(&log).await.unwrap();
        }));
    }

    // Spawn 5 snapshot upserters
    for i in 0..5 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let snap = RuneSnapshot {
                rune_name: format!("conc-rune-{}", i),
                version: "0.1.0".to_string(),
                description: "concurrent test".to_string(),
                supports_stream: false,
                gate_path: format!("/rune/conc-{}", i),
                gate_method: "POST".to_string(),
                last_seen: String::new(),
            };
            s.upsert_snapshot(&snap).await.unwrap();
        }));
    }

    // Spawn 5 key creators
    for i in 0..5 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            s.create_key(KeyType::Gate, &format!("conc-key-{}", i))
                .await
                .unwrap();
        }));
    }

    // Spawn 5 readers in parallel with writers
    for _ in 0..5 {
        let s = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let _ = s.list_tasks(None, None, 100, 0).await.unwrap();
            let _ = s.query_logs(None, 100).await.unwrap();
            let _ = s.list_snapshots().await.unwrap();
            let _ = s.list_keys().await.unwrap();
        }));
    }

    // Wait for all handles — any deadlock would cause a timeout, any panic would surface here
    for h in handles {
        h.await.expect("task should not panic or deadlock");
    }

    // Verify final state is consistent
    let tasks = store.list_tasks(None, None, 100, 0).await.unwrap();
    assert_eq!(tasks.len(), 10, "All 10 tasks should have been inserted");

    let logs = store.query_logs(None, 100).await.unwrap();
    assert_eq!(logs.len(), 10, "All 10 logs should have been inserted");

    let snapshots = store.list_snapshots().await.unwrap();
    assert_eq!(
        snapshots.len(),
        5,
        "All 5 snapshots should have been upserted"
    );

    let keys = store.list_keys().await.unwrap();
    assert_eq!(keys.len(), 5, "All 5 keys should have been created");
}

// ============================================================
// MF-3: KeyType::parse returns None for unknown values
// ============================================================

#[test]
fn test_key_type_from_str_unknown_returns_none() {
    // from_str should return None for unknown key types, not silently fallback
    assert!(KeyType::parse("INVALID_TYPE").is_none());
    assert!(KeyType::parse("unknown").is_none());
    assert!(KeyType::parse("").is_none());
    assert!(KeyType::parse("GATE").is_none()); // case-sensitive
    assert!(KeyType::parse("Caster").is_none()); // case-sensitive
}

#[test]
fn test_key_type_from_str_valid_values() {
    assert_eq!(KeyType::parse("gate"), Some(KeyType::Gate));
    assert_eq!(KeyType::parse("caster"), Some(KeyType::Caster));
    assert_eq!(KeyType::parse("admin"), Some(KeyType::Admin));
}

// ============================================================
// WAL mode & busy_timeout
// ============================================================

#[tokio::test]
async fn test_wal_mode_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test_wal.db");
    let store = RuneStore::open(&db_path).unwrap();

    let mode = store.journal_mode().unwrap();
    assert_eq!(
        mode.to_lowercase(),
        "wal",
        "journal_mode should be WAL, got: {}",
        mode
    );
}

#[tokio::test]
async fn test_busy_timeout_set() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test_timeout.db");
    let store = RuneStore::open(&db_path).unwrap();

    let timeout = store.busy_timeout().unwrap();
    assert!(
        timeout >= 5000,
        "busy_timeout should be >= 5000, got: {}",
        timeout
    );
}
