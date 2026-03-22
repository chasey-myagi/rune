use rune_schema::validator::{validate_input, validate_output, SchemaError};

// =============================================================================
// Helper: common JSON Schema strings
// =============================================================================

fn user_schema() -> &'static str {
    r#"{
        "type": "object",
        "required": ["name", "age"],
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "integer" }
        }
    }"#
}

fn nested_schema() -> &'static str {
    r#"{
        "type": "object",
        "required": ["user"],
        "properties": {
            "user": {
                "type": "object",
                "required": ["email"],
                "properties": {
                    "email": { "type": "string" }
                }
            }
        }
    }"#
}

fn array_schema() -> &'static str {
    r#"{
        "type": "object",
        "required": ["items"],
        "properties": {
            "items": {
                "type": "array",
                "items": { "type": "integer" }
            }
        }
    }"#
}

// =============================================================================
// 正常路径
// =============================================================================

#[test]
fn test_valid_input_passes_validation() {
    let input = br#"{"name": "Alice", "age": 30}"#;
    let result = validate_input(Some(user_schema()), input);
    assert!(result.is_ok(), "valid input should pass: {:?}", result);
}

#[test]
fn test_valid_output_passes_validation() {
    let output = br#"{"name": "Bob", "age": 25}"#;
    let result = validate_output(Some(user_schema()), output);
    assert!(result.is_ok(), "valid output should pass: {:?}", result);
}

#[test]
fn test_no_schema_skips_validation_input() {
    // schema = None means skip validation, always Ok
    let result = validate_input(None, b"anything goes here");
    assert!(result.is_ok(), "None schema should skip validation");
}

#[test]
fn test_no_schema_skips_validation_output() {
    let result = validate_output(None, b"anything goes here");
    assert!(result.is_ok(), "None schema should skip validation");
}

// =============================================================================
// 错误路径
// =============================================================================

#[test]
fn test_missing_required_field() {
    // Missing "age" field
    let input = br#"{"name": "Alice"}"#;
    let result = validate_input(Some(user_schema()), input);
    assert!(result.is_err(), "missing required field should fail");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("age") || err_msg.contains("required"),
        "error should mention the missing field or 'required', got: {}",
        err_msg
    );
}

#[test]
fn test_type_mismatch() {
    // "age" should be integer, not string
    let input = br#"{"name": "Alice", "age": "thirty"}"#;
    let result = validate_input(Some(user_schema()), input);
    assert!(result.is_err(), "type mismatch should fail");
}

#[test]
fn test_nested_object_validation_failure() {
    // "user.email" is required but missing
    let input = br#"{"user": {}}"#;
    let result = validate_input(Some(nested_schema()), input);
    assert!(result.is_err(), "nested object missing required field should fail");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("email") || err_msg.contains("required"),
        "error should reference nested field, got: {}",
        err_msg
    );
}

#[test]
fn test_array_element_validation_failure() {
    // Array should contain integers, but has a string
    let input = br#"{"items": [1, 2, "three"]}"#;
    let result = validate_input(Some(array_schema()), input);
    assert!(result.is_err(), "array element type mismatch should fail");
}

#[test]
fn test_empty_body_with_schema_fails() {
    // Empty byte slice is not valid JSON
    let result = validate_input(Some(user_schema()), b"");
    assert!(result.is_err(), "empty body with schema should fail");
}

#[test]
fn test_invalid_json_schema_string_graceful() {
    // The schema itself is not valid JSON
    let bad_schema = "this is not json {{{";
    let result = validate_input(Some(bad_schema), br#"{"name": "Alice"}"#);
    assert!(result.is_err(), "invalid schema string should return error, not panic");

    match result.unwrap_err() {
        SchemaError::InvalidSchema(msg) => {
            assert!(!msg.is_empty(), "InvalidSchema message should not be empty");
        }
        other => panic!("expected InvalidSchema, got: {:?}", other),
    }
}

// =============================================================================
// 边界情况
// =============================================================================

#[test]
fn test_empty_object_with_permissive_schema() {
    // Schema allows additional properties (default behavior), empty object is valid
    let schema = r#"{"type": "object"}"#;
    let input = br#"{}"#;
    let result = validate_input(Some(schema), input);
    assert!(result.is_ok(), "empty object should pass permissive schema: {:?}", result);
}

#[test]
fn test_large_json_over_100_fields() {
    // Build a schema with >100 properties
    let mut props = String::new();
    let mut required = String::new();
    for i in 0..120 {
        if i > 0 {
            props.push_str(", ");
            required.push_str(", ");
        }
        props.push_str(&format!(r#""field_{}": {{"type": "integer"}}"#, i));
        required.push_str(&format!(r#""field_{}""#, i));
    }
    let schema = format!(
        r#"{{"type": "object", "required": [{}], "properties": {{{}}}}}"#,
        required, props
    );

    // Build matching input
    let mut fields = String::new();
    for i in 0..120 {
        if i > 0 {
            fields.push_str(", ");
        }
        fields.push_str(&format!(r#""field_{}": {}"#, i, i));
    }
    let input = format!("{{{}}}", fields);

    let result = validate_input(Some(&schema), input.as_bytes());
    assert!(result.is_ok(), "large JSON with 120 fields should validate: {:?}", result);
}

#[test]
fn test_unicode_field_names() {
    let schema = r#"{
        "type": "object",
        "required": ["名前", "年齢"],
        "properties": {
            "名前": { "type": "string" },
            "年齢": { "type": "integer" }
        }
    }"#;
    let input = r#"{"名前": "太郎", "年齢": 25}"#;
    let result = validate_input(Some(schema), input.as_bytes());
    assert!(result.is_ok(), "unicode field names should work: {:?}", result);
}

#[test]
fn test_minimal_schema_type_object() {
    // Minimal schema: just "type": "object"
    let schema = r#"{"type": "object"}"#;

    // Object should pass
    let result = validate_input(Some(schema), br#"{"any": "thing"}"#);
    assert!(result.is_ok(), "object should pass minimal object schema: {:?}", result);

    // Non-object (array) should fail
    let result = validate_input(Some(schema), br#"[1, 2, 3]"#);
    assert!(result.is_err(), "array should fail minimal object schema");

    // Non-object (string) should fail
    let result = validate_input(Some(schema), br#""hello""#);
    assert!(result.is_err(), "string should fail minimal object schema");

    // Non-object (number) should fail
    let result = validate_input(Some(schema), br#"42"#);
    assert!(result.is_err(), "number should fail minimal object schema");
}

#[test]
fn test_schema_allows_null() {
    let schema = r#"{"type": ["object", "null"]}"#;

    let result = validate_input(Some(schema), br#"null"#);
    assert!(result.is_ok(), "null should pass when schema allows null: {:?}", result);

    let result = validate_input(Some(schema), br#"{"key": "value"}"#);
    assert!(result.is_ok(), "object should also pass: {:?}", result);
}

#[test]
fn test_additional_properties_false() {
    let schema = r#"{
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        },
        "additionalProperties": false
    }"#;

    // Valid: only declared property
    let result = validate_input(Some(schema), br#"{"name": "Alice"}"#);
    assert!(result.is_ok(), "only declared property should pass: {:?}", result);

    // Invalid: extra property "extra_field"
    let result = validate_input(Some(schema), br#"{"name": "Alice", "extra_field": true}"#);
    assert!(result.is_err(), "extra property with additionalProperties:false should fail");
}

#[test]
fn test_enum_validation_pass() {
    let schema = r#"{
        "type": "object",
        "properties": {
            "status": { "type": "string", "enum": ["active", "inactive", "pending"] }
        },
        "required": ["status"]
    }"#;

    let result = validate_input(Some(schema), br#"{"status": "active"}"#);
    assert!(result.is_ok(), "enum match should pass: {:?}", result);
}

#[test]
fn test_enum_validation_fail() {
    let schema = r#"{
        "type": "object",
        "properties": {
            "status": { "type": "string", "enum": ["active", "inactive", "pending"] }
        },
        "required": ["status"]
    }"#;

    let result = validate_input(Some(schema), br#"{"status": "deleted"}"#);
    assert!(result.is_err(), "enum mismatch should fail");
}

#[test]
fn test_pattern_validation_pass() {
    let schema = r#"{
        "type": "object",
        "properties": {
            "email": { "type": "string", "pattern": "^[^@]+@[^@]+\\.[^@]+$" }
        },
        "required": ["email"]
    }"#;

    let result = validate_input(Some(schema), br#"{"email": "user@example.com"}"#);
    assert!(result.is_ok(), "pattern match should pass: {:?}", result);
}

#[test]
fn test_pattern_validation_fail() {
    let schema = r#"{
        "type": "object",
        "properties": {
            "email": { "type": "string", "pattern": "^[^@]+@[^@]+\\.[^@]+$" }
        },
        "required": ["email"]
    }"#;

    let result = validate_input(Some(schema), br#"{"email": "not-an-email"}"#);
    assert!(result.is_err(), "pattern mismatch should fail");
}

// =============================================================================
// validate_output 独立测试（确保 output 走同样的校验逻辑）
// =============================================================================

#[test]
fn test_output_validation_failure() {
    // Output schema expects integer age, but output has string
    let output = br#"{"name": "Alice", "age": "not-a-number"}"#;
    let result = validate_output(Some(user_schema()), output);
    assert!(result.is_err(), "output with wrong type should fail validation");
}

#[test]
fn test_output_no_schema_skips() {
    let result = validate_output(None, br#"literally anything"#);
    assert!(result.is_ok(), "None schema should always skip");
}

// =============================================================================
// 多重错误场景
// =============================================================================

#[test]
fn test_multiple_missing_required_fields() {
    // Both "name" and "age" are missing
    let input = br#"{}"#;
    let result = validate_input(Some(user_schema()), input);
    assert!(result.is_err(), "all required fields missing should fail");
}

#[test]
fn test_invalid_json_input() {
    // Input is not valid JSON
    let result = validate_input(Some(user_schema()), b"not json at all");
    assert!(result.is_err(), "invalid JSON input should fail");
}

#[test]
fn test_null_input_with_object_schema() {
    // null input against an object-only schema should fail
    let schema = r#"{"type": "object"}"#;
    let result = validate_input(Some(schema), br#"null"#);
    assert!(result.is_err(), "null should fail against type:object schema");
}

#[test]
fn test_boolean_schema_true() {
    // JSON Schema: true means everything is valid
    let schema = "true";
    let result = validate_input(Some(schema), br#"{"anything": 42}"#);
    assert!(result.is_ok(), "boolean schema 'true' should accept anything: {:?}", result);
}

#[test]
fn test_boolean_schema_false() {
    // JSON Schema: false means nothing is valid
    let schema = "false";
    let result = validate_input(Some(schema), br#"{"anything": 42}"#);
    assert!(result.is_err(), "boolean schema 'false' should reject everything");
}
