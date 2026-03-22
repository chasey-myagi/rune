#[path = "../src/client.rs"]
mod client;

use client::RuneClient;

#[test]
fn test_client_base_url() {
    let c = RuneClient::new("http://127.0.0.1:3000", None);
    assert_eq!(c.base_url, "http://127.0.0.1:3000");
}

#[test]
fn test_client_base_url_trailing_slash_stripped() {
    let c = RuneClient::new("http://127.0.0.1:3000/", None);
    assert_eq!(c.base_url, "http://127.0.0.1:3000");
}

#[test]
fn test_client_no_api_key() {
    let c = RuneClient::new("http://localhost:3000", None);
    assert!(c.api_key.is_none());
}

#[test]
fn test_client_with_api_key() {
    let c = RuneClient::new("http://localhost:3000", Some("secret-key"));
    assert_eq!(c.api_key.as_deref(), Some("secret-key"));
}

// ── Security: URL path segment encoding ─────────────────────────────────

#[test]
fn test_fix_url_encodes_rune_name_with_slash() {
    let c = RuneClient::new("http://localhost:3000", None);
    let url = c.build_path("/api/v1/runes/{name}/run", &[("name", "../admin")]);
    assert_eq!(url, "/api/v1/runes/..%2Fadmin/run");
}

#[test]
fn test_fix_url_encodes_rune_name_with_special_chars() {
    let c = RuneClient::new("http://localhost:3000", None);
    let url = c.build_path("/api/v1/runes/{name}/run", &[("name", "hello world")]);
    assert_eq!(url, "/api/v1/runes/hello%20world/run");
}

#[test]
fn test_fix_url_encodes_flow_name() {
    let c = RuneClient::new("http://localhost:3000", None);
    let url = c.build_path("/api/v1/flows/{name}/run", &[("name", "my/flow")]);
    assert_eq!(url, "/api/v1/flows/my%2Fflow/run");
}

#[test]
fn test_fix_url_encodes_key_id() {
    let c = RuneClient::new("http://localhost:3000", None);
    let url = c.build_path("/api/v1/keys/{id}", &[("id", "key/../../../etc")]);
    assert_eq!(url, "/api/v1/keys/key%2F..%2F..%2F..%2Fetc");
}

#[test]
fn test_fix_url_encodes_query_param_rune_name() {
    let c = RuneClient::new("http://localhost:3000", None);
    // Query 参数中的特殊字符也应被编码
    let url = c.build_path_with_query(
        "/api/v1/logs",
        &[],
        &[("limit", "50"), ("rune", "evil&admin=true")],
    );
    assert_eq!(url, "/api/v1/logs?limit=50&rune=evil%26admin%3Dtrue");
}

// ── Integration tests (require a running server) ────────────────────────

#[tokio::test]
#[ignore]
async fn test_status_requires_server() {
    let c = RuneClient::new("http://127.0.0.1:3000", None);
    let _result = c.status().await;
}

#[tokio::test]
#[ignore]
async fn test_list_runes_requires_server() {
    let c = RuneClient::new("http://127.0.0.1:3000", None);
    let _result = c.list_runes().await;
}
