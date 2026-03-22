use rune_schema::openapi::{generate_openapi, RuneInfo};

// =============================================================================
// Helper
// =============================================================================

fn make_rune(
    name: &str,
    gate_path: Option<&str>,
    method: &str,
    input_schema: Option<&str>,
    output_schema: Option<&str>,
    description: &str,
) -> RuneInfo {
    RuneInfo {
        name: name.to_string(),
        gate_path: gate_path.map(|s| s.to_string()),
        gate_method: method.to_string(),
        input_schema: input_schema.map(|s| s.to_string()),
        output_schema: output_schema.map(|s| s.to_string()),
        description: description.to_string(),
    }
}

fn user_schema() -> &'static str {
    r#"{
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": { "type": "string" }
        }
    }"#
}

fn result_schema() -> &'static str {
    r#"{
        "type": "object",
        "properties": {
            "result": { "type": "string" }
        }
    }"#
}

// =============================================================================
// OpenAPI 基础结构验证
// =============================================================================

#[test]
fn test_no_runes_empty_paths() {
    let openapi = generate_openapi(&[]);

    // Must have required OpenAPI 3.0 top-level fields
    assert_eq!(openapi["openapi"].as_str().unwrap(), "3.0.0",
        "should be OpenAPI 3.0.0");
    assert!(openapi["info"].is_object(),
        "must have info object");
    assert!(openapi["info"]["title"].is_string(),
        "info must have title");
    assert!(openapi["info"]["version"].is_string(),
        "info must have version");

    // paths should be empty or an empty object
    let paths = &openapi["paths"];
    assert!(paths.is_object(), "paths must be an object");
    assert_eq!(paths.as_object().unwrap().len(), 0,
        "no runes = no paths");
}

#[test]
fn test_one_rune_with_schema() {
    let runes = vec![make_rune(
        "echo",
        Some("/echo"),
        "POST",
        Some(user_schema()),
        Some(result_schema()),
        "Echo rune",
    )];

    let openapi = generate_openapi(&runes);

    // Verify top-level structure
    assert_eq!(openapi["openapi"].as_str().unwrap(), "3.0.0");

    // Verify path exists
    let paths = &openapi["paths"];
    assert!(paths["/echo"].is_object(),
        "path /echo should exist in paths");

    // Verify method
    let post = &paths["/echo"]["post"];
    assert!(post.is_object(),
        "/echo should have a 'post' operation");

    // Verify requestBody contains schema
    let request_body = &post["requestBody"];
    assert!(request_body.is_object(),
        "POST /echo should have requestBody");
    let content = &request_body["content"]["application/json"]["schema"];
    assert!(content.is_object(),
        "requestBody should have application/json schema");

    // Verify response schema
    let responses = &post["responses"];
    assert!(responses["200"].is_object(),
        "should have 200 response");
    let resp_schema = &responses["200"]["content"]["application/json"]["schema"];
    assert!(resp_schema.is_object(),
        "200 response should have schema");

    // Verify description/summary
    assert!(
        post["summary"].is_string() || post["description"].is_string(),
        "operation should have summary or description"
    );
}

#[test]
fn test_multiple_runes_multiple_paths() {
    let runes = vec![
        make_rune("echo", Some("/echo"), "POST", Some(user_schema()), None, "Echo"),
        make_rune("translate", Some("/translate"), "POST", Some(user_schema()), Some(result_schema()), "Translate"),
        make_rune("health-check", Some("/check"), "GET", None, Some(result_schema()), "Health"),
    ];

    let openapi = generate_openapi(&runes);
    let paths = openapi["paths"].as_object().unwrap();

    assert_eq!(paths.len(), 3, "should have 3 paths");
    assert!(paths.contains_key("/echo"), "should have /echo");
    assert!(paths.contains_key("/translate"), "should have /translate");
    assert!(paths.contains_key("/check"), "should have /check");

    // Verify methods match
    assert!(openapi["paths"]["/echo"]["post"].is_object());
    assert!(openapi["paths"]["/translate"]["post"].is_object());
    assert!(openapi["paths"]["/check"]["get"].is_object());
}

#[test]
fn test_rune_without_gate_path_excluded() {
    let runes = vec![
        make_rune("visible", Some("/visible"), "POST", None, None, "Visible"),
        make_rune("internal", None, "POST", None, None, "Internal rune"),
    ];

    let openapi = generate_openapi(&runes);
    let paths = openapi["paths"].as_object().unwrap();

    assert_eq!(paths.len(), 1, "only rune with gate_path should appear");
    assert!(paths.contains_key("/visible"), "should have /visible");
    assert!(!paths.contains_key("/internal"), "internal should not appear");
}

#[test]
fn test_rune_without_schema_has_path_but_no_request_body_schema() {
    let runes = vec![make_rune(
        "simple",
        Some("/simple"),
        "POST",
        None,  // no input_schema
        None,  // no output_schema
        "Simple rune",
    )];

    let openapi = generate_openapi(&runes);

    // Path should exist
    assert!(openapi["paths"]["/simple"]["post"].is_object(),
        "path should exist even without schema");

    // requestBody schema should be absent or empty
    let post = &openapi["paths"]["/simple"]["post"];
    let has_request_body_schema = post["requestBody"]["content"]["application/json"]["schema"].is_object();
    // When no input_schema, requestBody should either be absent or not have a specific schema
    assert!(
        !has_request_body_schema || post["requestBody"].is_null(),
        "no input_schema means no specific requestBody schema"
    );
}

#[test]
fn test_rune_unregistered_removed_from_openapi() {
    // Simulate: first generate with 2 runes, then regenerate with 1 (after unregistration)
    let runes_before = vec![
        make_rune("a", Some("/a"), "POST", None, None, "Rune A"),
        make_rune("b", Some("/b"), "POST", None, None, "Rune B"),
    ];
    let openapi_before = generate_openapi(&runes_before);
    assert_eq!(openapi_before["paths"].as_object().unwrap().len(), 2);

    // After "b" is unregistered
    let runes_after = vec![
        make_rune("a", Some("/a"), "POST", None, None, "Rune A"),
    ];
    let openapi_after = generate_openapi(&runes_after);
    let paths = openapi_after["paths"].as_object().unwrap();
    assert_eq!(paths.len(), 1, "after unregistration, only 1 path should remain");
    assert!(paths.contains_key("/a"), "rune A should still be present");
    assert!(!paths.contains_key("/b"), "rune B should be removed");
}

#[test]
fn test_valid_openapi_format() {
    // Verify the generated document has all mandatory OpenAPI 3.0 fields
    let runes = vec![make_rune(
        "test",
        Some("/test"),
        "POST",
        Some(user_schema()),
        Some(result_schema()),
        "Test rune",
    )];

    let openapi = generate_openapi(&runes);

    // Mandatory fields per OpenAPI 3.0 spec
    assert!(openapi["openapi"].is_string(), "must have 'openapi' version string");
    let version = openapi["openapi"].as_str().unwrap();
    assert!(version.starts_with("3."), "version should start with 3., got: {}", version);

    assert!(openapi["info"].is_object(), "must have 'info' object");
    assert!(openapi["info"]["title"].is_string(), "info.title is required");
    assert!(openapi["info"]["version"].is_string(), "info.version is required");

    assert!(openapi["paths"].is_object(), "must have 'paths' object");

    // Validate the generated JSON is well-formed by re-serializing
    let serialized = serde_json::to_string_pretty(&openapi);
    assert!(serialized.is_ok(), "OpenAPI JSON should be serializable");
}

// =============================================================================
// 边界场景
// =============================================================================

#[test]
fn test_same_path_different_methods() {
    // Two runes with the same path but different methods (GET vs POST)
    let runes = vec![
        make_rune("reader", Some("/resource"), "GET", None, Some(result_schema()), "Read resource"),
        make_rune("writer", Some("/resource"), "POST", Some(user_schema()), None, "Write resource"),
    ];

    let openapi = generate_openapi(&runes);
    let resource = &openapi["paths"]["/resource"];

    assert!(resource["get"].is_object(), "should have GET operation");
    assert!(resource["post"].is_object(), "should have POST operation");
}

#[test]
fn test_rune_description_in_operation() {
    let runes = vec![make_rune(
        "described",
        Some("/described"),
        "POST",
        None,
        None,
        "This is a detailed description of what this rune does",
    )];

    let openapi = generate_openapi(&runes);
    let post = &openapi["paths"]["/described"]["post"];

    // Description should appear somewhere in the operation
    let has_desc = post["description"].is_string() || post["summary"].is_string();
    assert!(has_desc, "rune description should appear in operation");
}

#[test]
fn test_many_runes_generates_many_paths() {
    let runes: Vec<RuneInfo> = (0..50)
        .map(|i| make_rune(
            &format!("rune_{}", i),
            Some(&format!("/rune/{}", i)),
            "POST",
            None,
            None,
            &format!("Rune number {}", i),
        ))
        .collect();

    let openapi = generate_openapi(&runes);
    let paths = openapi["paths"].as_object().unwrap();
    assert_eq!(paths.len(), 50, "should have 50 paths for 50 runes");
}
