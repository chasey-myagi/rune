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
