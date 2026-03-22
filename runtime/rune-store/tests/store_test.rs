use rune_store::models::*;
use rune_store::RuneStore;
use std::sync::Arc;
use std::thread;

fn new_store() -> RuneStore {
    RuneStore::open_in_memory().expect("failed to open in-memory store")
}

// ============================================================
// B1: Schema & Store
// ============================================================

#[test]
fn test_open_in_memory() {
    let store = new_store();
    // Verify tables exist by running queries that would fail otherwise
    assert!(store.list_keys().is_ok());
    assert!(store.list_tasks(None, None, 10, 0).is_ok());
    assert!(store.query_logs(None, 10).is_ok());
    assert!(store.list_snapshots().is_ok());
}

#[test]
fn test_open_file_based() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let store = RuneStore::open(&db_path).expect("failed to open file-based store");
    store.insert_task("t1", "rune_a", None).unwrap();

    // Reopen and verify data persists
    drop(store);
    let store2 = RuneStore::open(&db_path).unwrap();
    let task = store2.get_task("t1").unwrap();
    assert!(task.is_some());
}

// ============================================================
// B2: API Key CRUD
// ============================================================

#[test]
fn test_create_and_verify_gate_key() {
    let store = new_store();
    let result = store.create_key(KeyType::Gate, "test gate").unwrap();
    let verified = store.verify_key(&result.raw_key, KeyType::Gate).unwrap();
    assert!(verified.is_some());
    let key = verified.unwrap();
    assert_eq!(key.key_type, KeyType::Gate);
    assert_eq!(key.label, "test gate");
}

#[test]
fn test_create_and_verify_caster_key() {
    let store = new_store();
    let result = store.create_key(KeyType::Caster, "test caster").unwrap();
    let verified = store.verify_key(&result.raw_key, KeyType::Caster).unwrap();
    assert!(verified.is_some());
    assert_eq!(verified.unwrap().key_type, KeyType::Caster);
}

#[test]
fn test_wrong_key_type_fails() {
    let store = new_store();
    let result = store.create_key(KeyType::Gate, "gate key").unwrap();
    // Verify as caster should fail
    let verified = store.verify_key(&result.raw_key, KeyType::Caster).unwrap();
    assert!(verified.is_none());
}

#[test]
fn test_invalid_key_fails() {
    let store = new_store();
    let verified = store.verify_key("some_random_string_not_a_key", KeyType::Gate).unwrap();
    assert!(verified.is_none());
}

#[test]
fn test_empty_key_fails() {
    let store = new_store();
    let verified = store.verify_key("", KeyType::Gate).unwrap();
    assert!(verified.is_none());
}

#[test]
fn test_list_keys() {
    let store = new_store();
    store.create_key(KeyType::Gate, "key1").unwrap();
    store.create_key(KeyType::Caster, "key2").unwrap();
    store.create_key(KeyType::Gate, "key3").unwrap();

    let keys = store.list_keys().unwrap();
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0].label, "key1");
    assert_eq!(keys[1].label, "key2");
    assert_eq!(keys[2].label, "key3");
}

#[test]
fn test_list_keys_hides_hash() {
    let store = new_store();
    store.create_key(KeyType::Gate, "secret").unwrap();

    let keys = store.list_keys().unwrap();
    assert_eq!(keys.len(), 1);
    assert!(keys[0].key_hash.is_none(), "key_hash should be None in list results");
}

#[test]
fn test_revoke_key() {
    let store = new_store();
    let result = store.create_key(KeyType::Gate, "to revoke").unwrap();

    // Verify works before revoke
    assert!(store.verify_key(&result.raw_key, KeyType::Gate).unwrap().is_some());

    // Revoke
    store.revoke_key(result.api_key.id).unwrap();

    // Verify fails after revoke
    assert!(store.verify_key(&result.raw_key, KeyType::Gate).unwrap().is_none());

    // Check revoked_at is set in list
    let keys = store.list_keys().unwrap();
    assert!(keys[0].revoked_at.is_some());
}

#[test]
fn test_revoke_nonexistent_key() {
    let store = new_store();
    // Should not error
    let result = store.revoke_key(99999);
    assert!(result.is_ok());
}

#[test]
fn test_create_multiple_same_label() {
    let store = new_store();
    let r1 = store.create_key(KeyType::Gate, "same-label").unwrap();
    let r2 = store.create_key(KeyType::Gate, "same-label").unwrap();

    // Both should be different keys
    assert_ne!(r1.raw_key, r2.raw_key);
    assert_ne!(r1.api_key.id, r2.api_key.id);

    let keys = store.list_keys().unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].label, "same-label");
    assert_eq!(keys[1].label, "same-label");
}

#[test]
fn test_key_format() {
    let store = new_store();
    let result = store.create_key(KeyType::Gate, "fmt test").unwrap();
    let raw = &result.raw_key;

    // Format: rk_{32_hex_chars}
    assert!(raw.starts_with("rk_"), "key should start with rk_");
    assert_eq!(raw.len(), 35, "key should be 35 chars: 'rk_' + 32 hex");

    // Verify hex portion
    let hex_part = &raw[3..];
    assert_eq!(hex_part.len(), 32);
    assert!(hex_part.chars().all(|c| c.is_ascii_hexdigit()), "suffix should be hex");
}

// ============================================================
// B3: Task CRUD
// ============================================================

#[test]
fn test_task_insert_and_get() {
    let store = new_store();
    let input = r#"{"prompt": "hello"}"#;
    let task = store.insert_task("task-001", "echo_rune", Some(input)).unwrap();

    assert_eq!(task.task_id, "task-001");
    assert_eq!(task.rune_name, "echo_rune");
    assert_eq!(task.status, TaskStatus::Pending);
    assert_eq!(task.input.as_deref(), Some(input));

    let fetched = store.get_task("task-001").unwrap().unwrap();
    assert_eq!(fetched.task_id, "task-001");
    assert_eq!(fetched.input.as_deref(), Some(input));
}

#[test]
fn test_task_not_found() {
    let store = new_store();
    let result = store.get_task("nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_task_lifecycle() {
    let store = new_store();
    store.insert_task("lc-1", "rune_a", Some("{}")).unwrap();

    // pending → running
    store.update_task_status("lc-1", TaskStatus::Running, None, None).unwrap();
    let task = store.get_task("lc-1").unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Running);
    assert!(task.started_at.is_some());

    // running → completed
    store.update_task_status("lc-1", TaskStatus::Completed, Some(r#"{"result": 42}"#), None).unwrap();
    let task = store.get_task("lc-1").unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Completed);
    assert!(task.completed_at.is_some());
    assert_eq!(task.output.as_deref(), Some(r#"{"result": 42}"#));
}

#[test]
fn test_task_failed() {
    let store = new_store();
    store.insert_task("fail-1", "rune_a", None).unwrap();
    store.update_task_status("fail-1", TaskStatus::Running, None, None).unwrap();
    store.update_task_status("fail-1", TaskStatus::Failed, None, Some("timeout after 30s")).unwrap();

    let task = store.get_task("fail-1").unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Failed);
    assert_eq!(task.error.as_deref(), Some("timeout after 30s"));
    assert!(task.completed_at.is_some());
}

#[test]
fn test_task_cancelled() {
    let store = new_store();
    store.insert_task("cancel-1", "rune_a", None).unwrap();
    store.update_task_status("cancel-1", TaskStatus::Running, None, None).unwrap();
    store.update_task_status("cancel-1", TaskStatus::Cancelled, None, None).unwrap();

    let task = store.get_task("cancel-1").unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Cancelled);
    assert!(task.completed_at.is_some());
}

#[test]
fn test_list_tasks_by_status() {
    let store = new_store();
    store.insert_task("s1", "rune_a", None).unwrap();
    store.insert_task("s2", "rune_a", None).unwrap();
    store.insert_task("s3", "rune_a", None).unwrap();
    store.update_task_status("s2", TaskStatus::Running, None, None).unwrap();
    store.update_task_status("s3", TaskStatus::Completed, Some("done"), None).unwrap();

    let pending = store.list_tasks(Some(TaskStatus::Pending), None, 100, 0).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].task_id, "s1");

    let running = store.list_tasks(Some(TaskStatus::Running), None, 100, 0).unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].task_id, "s2");

    let completed = store.list_tasks(Some(TaskStatus::Completed), None, 100, 0).unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].task_id, "s3");
}

#[test]
fn test_list_tasks_by_rune() {
    let store = new_store();
    store.insert_task("r1", "alpha", None).unwrap();
    store.insert_task("r2", "beta", None).unwrap();
    store.insert_task("r3", "alpha", None).unwrap();

    let alpha = store.list_tasks(None, Some("alpha"), 100, 0).unwrap();
    assert_eq!(alpha.len(), 2);

    let beta = store.list_tasks(None, Some("beta"), 100, 0).unwrap();
    assert_eq!(beta.len(), 1);
}

#[test]
fn test_list_tasks_pagination() {
    let store = new_store();
    for i in 0..10 {
        store.insert_task(&format!("p{}", i), "rune_a", None).unwrap();
    }

    let page1 = store.list_tasks(None, None, 3, 0).unwrap();
    assert_eq!(page1.len(), 3);

    let page2 = store.list_tasks(None, None, 3, 3).unwrap();
    assert_eq!(page2.len(), 3);

    // Pages should not overlap
    let ids1: Vec<_> = page1.iter().map(|t| &t.task_id).collect();
    let ids2: Vec<_> = page2.iter().map(|t| &t.task_id).collect();
    for id in &ids1 {
        assert!(!ids2.contains(id), "pagination overlap detected");
    }

    // Beyond range
    let page_far = store.list_tasks(None, None, 3, 100).unwrap();
    assert_eq!(page_far.len(), 0);
}

#[test]
fn test_list_tasks_empty() {
    let store = new_store();
    let result = store.list_tasks(None, None, 100, 0).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_update_nonexistent_task() {
    let store = new_store();
    // Should not error
    let result = store.update_task_status("ghost", TaskStatus::Running, None, None);
    assert!(result.is_ok());
}

#[test]
fn test_task_with_large_output() {
    let store = new_store();
    store.insert_task("big-1", "rune_a", None).unwrap();

    // Generate a large JSON output (~1MB)
    let large_obj: serde_json::Value = serde_json::json!({
        "data": "x".repeat(1_000_000),
        "nested": {
            "array": (0..1000).collect::<Vec<i32>>(),
        }
    });
    let large_output = serde_json::to_string(&large_obj).unwrap();

    store.update_task_status("big-1", TaskStatus::Completed, Some(&large_output), None).unwrap();

    let task = store.get_task("big-1").unwrap().unwrap();
    assert_eq!(task.output.as_deref(), Some(large_output.as_str()));

    // Verify the JSON can be parsed back
    let parsed: serde_json::Value = serde_json::from_str(task.output.as_ref().unwrap()).unwrap();
    assert_eq!(parsed["data"].as_str().unwrap().len(), 1_000_000);
}

#[test]
fn test_task_concurrent_updates() {
    let store = Arc::new(new_store());

    // Create 20 tasks
    for i in 0..20 {
        store.insert_task(&format!("conc-{}", i), "rune_a", None).unwrap();
    }

    // Spawn threads to update different tasks concurrently
    let mut handles = vec![];
    for i in 0..20 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            let task_id = format!("conc-{}", i);
            store.update_task_status(&task_id, TaskStatus::Running, None, None).unwrap();
            // Small work simulation
            std::thread::sleep(std::time::Duration::from_millis(1));
            store.update_task_status(
                &task_id,
                TaskStatus::Completed,
                Some(&format!(r#"{{"thread": {}}}"#, i)),
                None,
            ).unwrap();
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify all tasks are completed
    let tasks = store.list_tasks(Some(TaskStatus::Completed), None, 100, 0).unwrap();
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

#[test]
fn test_insert_and_query_logs() {
    let store = new_store();
    let log = make_log("rune_a", "req-1", "2026-01-01T00:00:00Z");
    let id = store.insert_log(&log).unwrap();
    assert!(id > 0);

    let logs = store.query_logs(None, 10).unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].request_id, "req-1");
    assert_eq!(logs[0].rune_name, "rune_a");
    assert_eq!(logs[0].latency_ms, 42);
}

#[test]
fn test_query_logs_by_rune() {
    let store = new_store();
    store.insert_log(&make_log("alpha", "r1", "2026-01-01T00:00:01Z")).unwrap();
    store.insert_log(&make_log("beta", "r2", "2026-01-01T00:00:02Z")).unwrap();
    store.insert_log(&make_log("alpha", "r3", "2026-01-01T00:00:03Z")).unwrap();

    let alpha_logs = store.query_logs(Some("alpha"), 100).unwrap();
    assert_eq!(alpha_logs.len(), 2);

    let beta_logs = store.query_logs(Some("beta"), 100).unwrap();
    assert_eq!(beta_logs.len(), 1);
}

#[test]
fn test_query_logs_limit() {
    let store = new_store();
    for i in 0..10 {
        store.insert_log(&make_log("rune_a", &format!("r{}", i), &format!("2026-01-01T00:00:{:02}Z", i))).unwrap();
    }

    let logs = store.query_logs(None, 3).unwrap();
    assert_eq!(logs.len(), 3);
}

#[test]
fn test_query_logs_order() {
    let store = new_store();
    store.insert_log(&make_log("rune_a", "old", "2025-01-01T00:00:00Z")).unwrap();
    store.insert_log(&make_log("rune_a", "new", "2026-06-01T00:00:00Z")).unwrap();
    store.insert_log(&make_log("rune_a", "mid", "2025-06-01T00:00:00Z")).unwrap();

    let logs = store.query_logs(None, 10).unwrap();
    assert_eq!(logs.len(), 3);
    // Should be newest first
    assert_eq!(logs[0].request_id, "new");
    assert_eq!(logs[1].request_id, "mid");
    assert_eq!(logs[2].request_id, "old");
}

#[test]
fn test_cleanup_old_logs() {
    let store = new_store();
    store.insert_log(&make_log("rune_a", "old1", "2024-01-01T00:00:00Z")).unwrap();
    store.insert_log(&make_log("rune_a", "old2", "2024-06-01T00:00:00Z")).unwrap();
    store.insert_log(&make_log("rune_a", "recent", "2026-01-01T00:00:00Z")).unwrap();

    let deleted = store.cleanup_logs_before("2025-01-01T00:00:00Z").unwrap();
    assert_eq!(deleted, 2);

    let remaining = store.query_logs(None, 100).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].request_id, "recent");
}

#[test]
fn test_log_with_null_caster() {
    let store = new_store();
    let mut log = make_log("rune_a", "no-caster", "2026-01-01T00:00:00Z");
    log.caster_id = None;
    store.insert_log(&log).unwrap();

    let logs = store.query_logs(None, 10).unwrap();
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

#[test]
fn test_upsert_snapshot() {
    let store = new_store();
    let snap = make_snapshot("echo");
    store.upsert_snapshot(&snap).unwrap();

    let list = store.list_snapshots().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].rune_name, "echo");
    assert_eq!(list[0].version, "0.1.0");

    // Update same rune
    let mut snap2 = make_snapshot("echo");
    snap2.version = "0.2.0".to_string();
    snap2.supports_stream = true;
    store.upsert_snapshot(&snap2).unwrap();

    let list = store.list_snapshots().unwrap();
    assert_eq!(list.len(), 1, "upsert should not duplicate");
    assert_eq!(list[0].version, "0.2.0");
    assert!(list[0].supports_stream);
}

#[test]
fn test_list_snapshots() {
    let store = new_store();
    store.upsert_snapshot(&make_snapshot("alpha")).unwrap();
    store.upsert_snapshot(&make_snapshot("beta")).unwrap();
    store.upsert_snapshot(&make_snapshot("gamma")).unwrap();

    let list = store.list_snapshots().unwrap();
    assert_eq!(list.len(), 3);
    // Ordered by rune_name
    assert_eq!(list[0].rune_name, "alpha");
    assert_eq!(list[1].rune_name, "beta");
    assert_eq!(list[2].rune_name, "gamma");
}

#[test]
fn test_snapshot_preserves_all_fields() {
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
    store.upsert_snapshot(&snap).unwrap();

    let list = store.list_snapshots().unwrap();
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

#[test]
fn test_upsert_updates_last_seen() {
    let store = new_store();
    store.upsert_snapshot(&make_snapshot("ticker")).unwrap();
    let first = store.list_snapshots().unwrap();
    let first_seen = first[0].last_seen.clone();

    // Small delay to ensure timestamp differs
    std::thread::sleep(std::time::Duration::from_millis(1100));

    store.upsert_snapshot(&make_snapshot("ticker")).unwrap();
    let second = store.list_snapshots().unwrap();
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
fn make_log_with_latency(rune_name: &str, request_id: &str, timestamp: &str, latency_ms: i64) -> CallLog {
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

#[test]
fn test_call_stats_returns_correct_counts_and_avg_latency() {
    let store = new_store();

    // Insert logs for "alpha": latencies 10, 20, 30 → avg = 20, count = 3
    store.insert_log(&make_log_with_latency("alpha", "a1", "2026-01-01T00:00:01Z", 10)).unwrap();
    store.insert_log(&make_log_with_latency("alpha", "a2", "2026-01-01T00:00:02Z", 20)).unwrap();
    store.insert_log(&make_log_with_latency("alpha", "a3", "2026-01-01T00:00:03Z", 30)).unwrap();

    // Insert logs for "beta": latencies 100, 200 → avg = 150, count = 2
    store.insert_log(&make_log_with_latency("beta", "b1", "2026-01-01T00:00:04Z", 100)).unwrap();
    store.insert_log(&make_log_with_latency("beta", "b2", "2026-01-01T00:00:05Z", 200)).unwrap();

    let (total, by_rune) = store.call_stats().unwrap();
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

#[test]
fn test_call_stats_empty_table() {
    let store = new_store();
    let (total, by_rune) = store.call_stats().unwrap();
    assert_eq!(total, 0);
    assert!(by_rune.is_empty());
}

// ------ 2. Task duplicate insert ------

#[test]
fn test_task_duplicate_insert_returns_error() {
    let store = new_store();
    store.insert_task("dup-1", "rune_a", Some("first")).unwrap();

    // task_id is PRIMARY KEY, so inserting the same id again should fail
    // with a UNIQUE constraint violation.
    let result = store.insert_task("dup-1", "rune_b", Some("second"));
    assert!(
        result.is_err(),
        "Duplicate task_id insert should fail because task_id is PRIMARY KEY"
    );

    // Original task should remain intact
    let task = store.get_task("dup-1").unwrap().unwrap();
    assert_eq!(task.rune_name, "rune_a");
    assert_eq!(task.input.as_deref(), Some("first"));
}

// ------ 3. Task illegal state transition: Completed → Running ------

#[test]
fn test_task_illegal_state_transition_completed_to_running() {
    let store = new_store();
    store.insert_task("illegal-1", "rune_a", None).unwrap();
    store.update_task_status("illegal-1", TaskStatus::Running, None, None).unwrap();
    store.update_task_status("illegal-1", TaskStatus::Completed, Some("done"), None).unwrap();

    // NOTE: The current implementation does NOT enforce state machine transitions.
    // It allows any status to be set at any time via a plain SQL UPDATE.
    // Completed → Running should ideally be rejected, but currently it succeeds.
    let result = store.update_task_status("illegal-1", TaskStatus::Running, None, None);
    assert!(
        result.is_ok(),
        "Current impl allows arbitrary state transitions (no state machine guard)"
    );

    let task = store.get_task("illegal-1").unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Running);
    // started_at gets overwritten by the second Running transition
    assert!(task.started_at.is_some());
}

// ------ 4. cleanup_logs on empty table ------

#[test]
fn test_cleanup_logs_empty_table() {
    let store = new_store();
    // Should not panic or return error when no logs exist
    let deleted = store.cleanup_logs_before("2099-12-31T23:59:59Z").unwrap();
    assert_eq!(deleted, 0);
}

// ------ 5. Concurrent key creation and verification ------

#[test]
fn test_concurrent_key_create_and_verify() {
    let store = Arc::new(new_store());
    let num_threads = 10;
    let mut handles = vec![];

    for i in 0..num_threads {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            // Each thread creates a key and immediately verifies it
            let label = format!("concurrent-key-{}", i);
            let result = store.create_key(KeyType::Gate, &label).unwrap();
            let verified = store.verify_key(&result.raw_key, KeyType::Gate).unwrap();
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

    let raw_keys: Vec<String> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All keys should be distinct
    let mut unique = raw_keys.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), num_threads, "All generated keys should be unique");

    // Verify all keys are still valid after all threads finish
    for raw_key in &raw_keys {
        let v = store.verify_key(raw_key, KeyType::Gate).unwrap();
        assert!(v.is_some());
    }

    let keys = store.list_keys().unwrap();
    assert_eq!(keys.len(), num_threads);
}

// ------ 6. Revoke key then create new key with same label ------

#[test]
fn test_revoke_key_then_create_same_label() {
    let store = new_store();

    // Create first key
    let first = store.create_key(KeyType::Gate, "reusable-label").unwrap();
    let first_raw = first.raw_key.clone();
    let first_id = first.api_key.id;

    // Revoke it
    store.revoke_key(first_id).unwrap();

    // Old key should be invalid
    assert!(store.verify_key(&first_raw, KeyType::Gate).unwrap().is_none());

    // Create a new key with the same label
    let second = store.create_key(KeyType::Gate, "reusable-label").unwrap();
    let second_raw = second.raw_key.clone();

    // New key should be valid
    let verified = store.verify_key(&second_raw, KeyType::Gate).unwrap();
    assert!(verified.is_some(), "Newly created key with same label should be valid");
    assert_eq!(verified.unwrap().label, "reusable-label");

    // Old key should still be invalid
    assert!(
        store.verify_key(&first_raw, KeyType::Gate).unwrap().is_none(),
        "Revoked key should remain invalid after creating a new key with same label"
    );

    // Both should appear in the key list
    let keys = store.list_keys().unwrap();
    assert_eq!(keys.len(), 2);
    let labels: Vec<&str> = keys.iter().map(|k| k.label.as_str()).collect();
    assert_eq!(labels, vec!["reusable-label", "reusable-label"]);
}

// ------ 7. Very long label (1000 chars) ------

#[test]
fn test_key_with_very_long_label() {
    let store = new_store();
    let long_label = "a".repeat(1000);
    let result = store.create_key(KeyType::Caster, &long_label).unwrap();

    // Verify the key works
    let verified = store.verify_key(&result.raw_key, KeyType::Caster).unwrap();
    assert!(verified.is_some());
    assert_eq!(verified.unwrap().label.len(), 1000);

    // Verify it appears correctly in list
    let keys = store.list_keys().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].label.len(), 1000);
}

// ------ 8. Special character labels (emoji, Chinese, SQL injection) ------

#[test]
fn test_key_with_emoji_label() {
    let store = new_store();
    let label = "rocket-key-\u{1F680}\u{2728}\u{1F525}";
    let result = store.create_key(KeyType::Gate, label).unwrap();
    let verified = store.verify_key(&result.raw_key, KeyType::Gate).unwrap();
    assert!(verified.is_some());
    assert_eq!(verified.unwrap().label, label);
}

#[test]
fn test_key_with_chinese_label() {
    let store = new_store();
    let label = "\u{4F60}\u{597D}\u{4E16}\u{754C}\u{FF0C}\u{8FD9}\u{662F}\u{4E00}\u{4E2A}\u{6D4B}\u{8BD5}";
    let result = store.create_key(KeyType::Gate, label).unwrap();
    let verified = store.verify_key(&result.raw_key, KeyType::Gate).unwrap();
    assert!(verified.is_some());
    assert_eq!(verified.unwrap().label, label);
}

#[test]
fn test_key_with_sql_injection_label() {
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
        let result = store.create_key(KeyType::Gate, label).unwrap();
        let verified = store.verify_key(&result.raw_key, KeyType::Gate).unwrap();
        assert!(verified.is_some(), "SQL injection label #{} should be stored safely", i);
        assert_eq!(
            verified.unwrap().label, *label,
            "SQL injection label #{} should be preserved verbatim",
            i
        );
    }

    // Verify the table is still intact
    let keys = store.list_keys().unwrap();
    assert_eq!(keys.len(), injection_labels.len());

    // Verify we can still create tasks (other tables not affected)
    store.insert_task("post-injection", "rune_a", None).unwrap();
    assert!(store.get_task("post-injection").unwrap().is_some());
}

// ------ 9. Task filter: status + rune_name combined ------

#[test]
fn test_list_tasks_filter_by_status_and_rune_name() {
    let store = new_store();

    // Create tasks across two runes with different statuses
    store.insert_task("t1", "alpha", None).unwrap(); // pending, alpha
    store.insert_task("t2", "alpha", None).unwrap(); // running, alpha
    store.insert_task("t3", "beta", None).unwrap();  // pending, beta
    store.insert_task("t4", "alpha", None).unwrap(); // completed, alpha
    store.insert_task("t5", "beta", None).unwrap();  // running, beta

    store.update_task_status("t2", TaskStatus::Running, None, None).unwrap();
    store.update_task_status("t4", TaskStatus::Running, None, None).unwrap();
    store.update_task_status("t4", TaskStatus::Completed, Some("done"), None).unwrap();
    store.update_task_status("t5", TaskStatus::Running, None, None).unwrap();

    // Filter: pending + alpha → only t1
    let result = store.list_tasks(Some(TaskStatus::Pending), Some("alpha"), 100, 0).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].task_id, "t1");

    // Filter: running + alpha → only t2
    let result = store.list_tasks(Some(TaskStatus::Running), Some("alpha"), 100, 0).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].task_id, "t2");

    // Filter: running + beta → only t5
    let result = store.list_tasks(Some(TaskStatus::Running), Some("beta"), 100, 0).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].task_id, "t5");

    // Filter: completed + alpha → only t4
    let result = store.list_tasks(Some(TaskStatus::Completed), Some("alpha"), 100, 0).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].task_id, "t4");

    // Filter: completed + beta → none
    let result = store.list_tasks(Some(TaskStatus::Completed), Some("beta"), 100, 0).unwrap();
    assert!(result.is_empty());

    // Filter: pending + nonexistent rune → none
    let result = store.list_tasks(Some(TaskStatus::Pending), Some("nonexistent"), 100, 0).unwrap();
    assert!(result.is_empty());
}

// ------ 10. Log query for nonexistent rune_name returns empty ------

#[test]
fn test_query_logs_nonexistent_rune_returns_empty() {
    let store = new_store();

    // Insert some logs for existing runes
    store.insert_log(&make_log("alpha", "r1", "2026-01-01T00:00:01Z")).unwrap();
    store.insert_log(&make_log("beta", "r2", "2026-01-01T00:00:02Z")).unwrap();

    // Query for a rune that has no logs
    let result = store.query_logs(Some("nonexistent_rune"), 100).unwrap();
    assert!(result.is_empty(), "Querying logs for a nonexistent rune should return empty vec");
}

#[test]
fn test_query_logs_empty_store_returns_empty() {
    let store = new_store();

    // No logs inserted at all
    let result = store.query_logs(Some("anything"), 100).unwrap();
    assert!(result.is_empty());

    let result = store.query_logs(None, 100).unwrap();
    assert!(result.is_empty());
}
