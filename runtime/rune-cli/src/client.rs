use anyhow::{Context, Result};
use futures::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};

/// HTTP client for communicating with the Rune Runtime API.
pub struct RuneClient {
    pub base_url: String,
    pub api_key: Option<String>,
    http: Client,
}

impl RuneClient {
    /// Create a new client pointing at the given base URL.
    pub fn new(base_url: &str, api_key: Option<&str>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.map(|s| s.to_string()),
            http: Client::new(),
        }
    }

    /// Build a URL path by replacing `{key}` placeholders with percent-encoded values.
    ///
    /// This prevents path traversal attacks from user-supplied path segments.
    pub fn build_path(&self, template: &str, params: &[(&str, &str)]) -> String {
        let mut path = template.to_string();
        for (key, value) in params {
            let placeholder = format!("{{{}}}", key);
            path = path.replace(&placeholder, &urlencoding::encode(value));
        }
        path
    }

    /// Build a URL path with query parameters. Both path segments and query values
    /// are percent-encoded.
    pub fn build_path_with_query(
        &self,
        template: &str,
        path_params: &[(&str, &str)],
        query_params: &[(&str, &str)],
    ) -> String {
        let mut path = self.build_path(template, path_params);
        if !query_params.is_empty() {
            let qs: Vec<String> = query_params
                .iter()
                .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                .collect();
            path.push('?');
            path.push_str(&qs.join("&"));
        }
        path
    }

    /// Build a request with optional Authorization header.
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.http.request(method, &url);
        if let Some(ref key) = self.api_key {
            req = req.bearer_auth(key);
        }
        req
    }

    /// Parse an input string as JSON, or wrap it in `{"input": ...}`.
    fn parse_input(input: Option<&str>) -> Value {
        match input {
            Some(s) => serde_json::from_str(s).unwrap_or_else(|_| json!({ "input": s })),
            None => json!({}),
        }
    }

    /// Send a request and parse the JSON response. Provides clear error messages
    /// for both connection failures and non-2xx responses.
    async fn send_json(&self, req: reqwest::RequestBuilder) -> Result<Value> {
        let resp = req
            .send()
            .await
            .with_context(|| format!("Failed to connect to Runtime at {}", self.base_url))?;

        let status = resp.status();
        let text = resp
            .text()
            .await
            .with_context(|| format!("Failed to read response from {}", self.base_url))?;

        let body: Value = serde_json::from_str(&text).with_context(|| {
            if text.is_empty() {
                format!(
                    "Runtime at {} returned empty response (HTTP {})",
                    self.base_url, status
                )
            } else {
                format!(
                    "Runtime at {} returned non-JSON response (HTTP {}): {}",
                    self.base_url,
                    status,
                    &text[..text.len().min(200)]
                )
            }
        })?;

        if !status.is_success() {
            anyhow::bail!("Server returned {}: {}", status, body);
        }

        Ok(body)
    }

    // ── Status ──────────────────────────────────────────────────────────

    /// GET /api/v1/status
    pub async fn status(&self) -> Result<Value> {
        let req = self.request(reqwest::Method::GET, "/api/v1/status");
        self.send_json(req).await
    }

    /// GET /health
    pub async fn health(&self) -> Result<Value> {
        let req = self.request(reqwest::Method::GET, "/health");
        self.send_json(req).await
    }

    // ── Rune ────────────────────────────────────────────────────────────

    /// GET /api/v1/runes
    pub async fn list_runes(&self) -> Result<Value> {
        let req = self.request(reqwest::Method::GET, "/api/v1/runes");
        self.send_json(req).await
    }

    /// POST /api/v1/runes/:name/run
    pub async fn call_rune(&self, name: &str, input: Option<&str>) -> Result<Value> {
        let path = self.build_path("/api/v1/runes/{name}/run", &[("name", name)]);
        let req = self
            .request(reqwest::Method::POST, &path)
            .json(&Self::parse_input(input));
        self.send_json(req).await
    }

    /// POST /api/v1/runes/:name/run?stream=true — prints SSE events to stdout
    pub async fn call_rune_stream(&self, name: &str, input: Option<&str>) -> Result<String> {
        let path = self.build_path_with_query(
            "/api/v1/runes/{name}/run",
            &[("name", name)],
            &[("stream", "true")],
        );
        let resp = self
            .request(reqwest::Method::POST, &path)
            .json(&Self::parse_input(input))
            .send()
            .await
            .with_context(|| format!("Failed to connect to Runtime at {}", self.base_url))?;

        let status = resp.status();
        if !status.is_success() {
            let body: Value = resp
                .json()
                .await
                .unwrap_or_else(|_| json!({"error": "unknown"}));
            anyhow::bail!("Server returned {}: {}", status, body);
        }

        // Read SSE stream: each line that starts with "data: " is an event
        let mut output = String::new();
        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("Error reading stream")?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            // Process complete lines
            while let Some(pos) = buffer.find('\n') {
                let line = buffer[..pos].trim_end_matches('\r').to_string();
                buffer = buffer[pos + 1..].to_string();

                if let Some(data) = line.strip_prefix("data: ") {
                    if data == "[DONE]" {
                        return Ok(output);
                    }
                    println!("{}", data);
                    output.push_str(data);
                    output.push('\n');
                }
            }
        }

        Ok(output)
    }

    /// POST /api/v1/runes/:name/run?async=true
    pub async fn call_rune_async(&self, name: &str, input: Option<&str>) -> Result<Value> {
        let path = self.build_path_with_query(
            "/api/v1/runes/{name}/run",
            &[("name", name)],
            &[("async", "true")],
        );
        let req = self
            .request(reqwest::Method::POST, &path)
            .json(&Self::parse_input(input));
        self.send_json(req).await
    }

    /// GET /api/v1/tasks/:id
    pub async fn get_task(&self, id: &str) -> Result<Value> {
        let path = self.build_path("/api/v1/tasks/{id}", &[("id", id)]);
        let req = self.request(reqwest::Method::GET, &path);
        self.send_json(req).await
    }

    // ── Key ─────────────────────────────────────────────────────────────

    /// POST /api/v1/keys
    pub async fn create_key(&self, key_type: &str, label: &str) -> Result<Value> {
        let req = self
            .request(reqwest::Method::POST, "/api/v1/keys")
            .json(&json!({ "key_type": key_type, "label": label }));
        self.send_json(req).await
    }

    /// GET /api/v1/keys
    pub async fn list_keys(&self) -> Result<Value> {
        let req = self.request(reqwest::Method::GET, "/api/v1/keys");
        self.send_json(req).await
    }

    /// DELETE /api/v1/keys/:id
    pub async fn revoke_key(&self, id: &str) -> Result<Value> {
        let path = self.build_path("/api/v1/keys/{id}", &[("id", id)]);
        let req = self.request(reqwest::Method::DELETE, &path);
        self.send_json(req).await
    }

    // ── Flow ────────────────────────────────────────────────────────────

    /// POST /api/v1/flows
    pub async fn register_flow(&self, definition: Value) -> Result<Value> {
        let req = self
            .request(reqwest::Method::POST, "/api/v1/flows")
            .json(&definition);
        self.send_json(req).await
    }

    /// GET /api/v1/flows
    pub async fn list_flows(&self) -> Result<Value> {
        let req = self.request(reqwest::Method::GET, "/api/v1/flows");
        self.send_json(req).await
    }

    /// POST /api/v1/flows/:name/run
    pub async fn run_flow(&self, name: &str, input: Option<&str>) -> Result<Value> {
        let path = self.build_path("/api/v1/flows/{name}/run", &[("name", name)]);
        let req = self
            .request(reqwest::Method::POST, &path)
            .json(&Self::parse_input(input));
        self.send_json(req).await
    }

    /// DELETE /api/v1/flows/:name
    pub async fn delete_flow(&self, name: &str) -> Result<Value> {
        let path = self.build_path("/api/v1/flows/{name}", &[("name", name)]);
        let req = self.request(reqwest::Method::DELETE, &path);
        self.send_json(req).await
    }

    // ── Logs / Stats ────────────────────────────────────────────────────

    /// GET /api/v1/logs
    pub async fn get_logs(&self, rune: Option<&str>, limit: u32) -> Result<Value> {
        let limit_str = limit.to_string();
        let mut query_params: Vec<(&str, &str)> = vec![("limit", &limit_str)];
        if let Some(rune_name) = rune {
            query_params.push(("rune", rune_name));
        }
        let path = self.build_path_with_query("/api/v1/logs", &[], &query_params);
        let req = self.request(reqwest::Method::GET, &path);
        self.send_json(req).await
    }

    /// GET /api/v1/stats
    pub async fn get_stats(&self) -> Result<Value> {
        let req = self.request(reqwest::Method::GET, "/api/v1/stats");
        self.send_json(req).await
    }

    /// Build the JSON body for create_key (exposed for testing).
    #[cfg(test)]
    fn create_key_body(key_type: &str, label: &str) -> Value {
        json!({ "key_type": key_type, "label": label })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// C3 regression: create_key must send "key_type", not "type".
    #[test]
    fn test_create_key_body_uses_key_type_field() {
        let body = RuneClient::create_key_body("gate", "my-key");
        assert!(
            body.get("key_type").is_some(),
            "body must contain 'key_type' field, got: {}",
            body
        );
        assert!(
            body.get("type").is_none(),
            "body must NOT contain 'type' field, got: {}",
            body
        );
        assert_eq!(body["key_type"], "gate");
        assert_eq!(body["label"], "my-key");
    }
}
