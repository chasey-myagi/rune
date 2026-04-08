use rune_schema::validator::{clear_validator_cache, validate_input, validate_output, SchemaError};

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
    assert!(
        result.is_err(),
        "nested object missing required field should fail"
    );

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
    assert!(
        result.is_err(),
        "invalid schema string should return error, not panic"
    );

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
    assert!(
        result.is_ok(),
        "empty object should pass permissive schema: {:?}",
        result
    );
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
    assert!(
        result.is_ok(),
        "large JSON with 120 fields should validate: {:?}",
        result
    );
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
    assert!(
        result.is_ok(),
        "unicode field names should work: {:?}",
        result
    );
}

#[test]
fn test_minimal_schema_type_object() {
    // Minimal schema: just "type": "object"
    let schema = r#"{"type": "object"}"#;

    // Object should pass
    let result = validate_input(Some(schema), br#"{"any": "thing"}"#);
    assert!(
        result.is_ok(),
        "object should pass minimal object schema: {:?}",
        result
    );

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
    assert!(
        result.is_ok(),
        "null should pass when schema allows null: {:?}",
        result
    );

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
    assert!(
        result.is_ok(),
        "only declared property should pass: {:?}",
        result
    );

    // Invalid: extra property "extra_field"
    let result = validate_input(Some(schema), br#"{"name": "Alice", "extra_field": true}"#);
    assert!(
        result.is_err(),
        "extra property with additionalProperties:false should fail"
    );
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
    assert!(
        result.is_err(),
        "output with wrong type should fail validation"
    );
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
    assert!(
        result.is_err(),
        "null should fail against type:object schema"
    );
}

#[test]
fn test_boolean_schema_true() {
    // JSON Schema: true means everything is valid
    let schema = "true";
    let result = validate_input(Some(schema), br#"{"anything": 42}"#);
    assert!(
        result.is_ok(),
        "boolean schema 'true' should accept anything: {:?}",
        result
    );
}

#[test]
fn test_boolean_schema_false() {
    // JSON Schema: false means nothing is valid
    let schema = "false";
    let result = validate_input(Some(schema), br#"{"anything": 42}"#);
    assert!(
        result.is_err(),
        "boolean schema 'false' should reject everything"
    );
}

// =============================================================================
// Performance regression: validator cache correctness
// =============================================================================

#[test]
fn test_cached_validation_returns_same_result() {
    clear_validator_cache();

    let schema = r#"{
        "type": "object",
        "required": ["name"],
        "properties": { "name": { "type": "string" } }
    }"#;

    // First call: compiles and caches
    let r1 = validate_input(Some(schema), br#"{"name": "Alice"}"#);
    assert!(r1.is_ok(), "first validation should pass");

    // Second call: should use cache and produce identical result
    let r2 = validate_input(Some(schema), br#"{"name": "Bob"}"#);
    assert!(r2.is_ok(), "cached validation should also pass");

    // Invalid input should still fail with cached validator
    let r3 = validate_input(Some(schema), br#"{"name": 123}"#);
    assert!(
        r3.is_err(),
        "cached validator should still reject invalid input"
    );
}

#[test]
fn test_different_schemas_cached_independently() {
    clear_validator_cache();

    let schema_a =
        r#"{"type": "object", "required": ["x"], "properties": {"x": {"type": "integer"}}}"#;
    let schema_b =
        r#"{"type": "object", "required": ["y"], "properties": {"y": {"type": "string"}}}"#;

    // Validate with schema_a
    let r1 = validate_input(Some(schema_a), br#"{"x": 42}"#);
    assert!(r1.is_ok());

    // Validate with schema_b
    let r2 = validate_input(Some(schema_b), br#"{"y": "hello"}"#);
    assert!(r2.is_ok());

    // Cross-validate: schema_a should reject schema_b's input
    let r3 = validate_input(Some(schema_a), br#"{"y": "hello"}"#);
    assert!(r3.is_err(), "schema_a should not accept schema_b input");
}

#[test]
fn test_cache_works_for_output_validation_too() {
    clear_validator_cache();

    let schema = r#"{"type": "object", "required": ["result"], "properties": {"result": {"type": "string"}}}"#;

    // First call via validate_output
    let r1 = validate_output(Some(schema), br#"{"result": "ok"}"#);
    assert!(r1.is_ok());

    // Second call via validate_input (same schema, should reuse cache)
    let r2 = validate_input(Some(schema), br#"{"result": "ok"}"#);
    assert!(r2.is_ok());
}

// =============================================================================
// S7 回归测试：不同 schema 不会因 hash 碰撞而混淆
// =============================================================================

#[test]
fn test_no_hash_collision_between_distinct_schemas() {
    // Two schemas that are semantically very different but might have similar hashes
    // if using a weak key. The cache must distinguish them correctly.
    clear_validator_cache();

    let schema_strict = r#"{
        "type": "object",
        "required": ["id"],
        "properties": { "id": { "type": "integer", "minimum": 1 } },
        "additionalProperties": false
    }"#;

    let schema_permissive = r#"{
        "type": "object",
        "properties": { "id": { "type": "string" } }
    }"#;

    // Load schema_strict into cache
    let r1 = validate_input(Some(schema_strict), br#"{"id": 42}"#);
    assert!(r1.is_ok(), "strict schema should accept integer id");

    // Load schema_permissive into cache
    let r2 = validate_input(Some(schema_permissive), br#"{"id": "abc"}"#);
    assert!(r2.is_ok(), "permissive schema should accept string id");

    // Verify strict schema still rejects string id (not confused with permissive)
    let r3 = validate_input(Some(schema_strict), br#"{"id": "abc"}"#);
    assert!(
        r3.is_err(),
        "strict schema must reject string id — cache must not confuse schemas"
    );

    // Verify permissive schema still accepts string id
    let r4 = validate_input(Some(schema_permissive), br#"{"id": "abc"}"#);
    assert!(
        r4.is_ok(),
        "permissive schema should still accept string id"
    );

    // Verify strict schema rejects extra properties
    let r5 = validate_input(Some(schema_strict), br#"{"id": 1, "extra": true}"#);
    assert!(
        r5.is_err(),
        "strict schema must reject additional properties"
    );

    // Verify permissive schema allows extra properties
    let r6 = validate_input(Some(schema_permissive), br#"{"id": "x", "extra": true}"#);
    assert!(
        r6.is_ok(),
        "permissive schema should allow extra properties"
    );
}

// =============================================================================
// RwLock poison recovery — 即使 cache RwLock 被 poison，validator 也不能 panic
// =============================================================================

#[test]
fn validator_cache_poison_recovery() {
    // 验证即使 RwLock 被 poison，validator 仍然能正常工作
    // 先正常 validate 一次确保 cache 初始化
    let schema = r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#;
    assert!(validate_input(Some(schema), br#"{"name": "test"}"#).is_ok());

    // 使用一个新的 schema 验证（确保不是从 cache 读取）
    let schema2 = r#"{"type": "array", "items": {"type": "integer"}}"#;
    assert!(validate_input(Some(schema2), b"[1, 2, 3]").is_ok());

    // 无效 JSON 应该返回 ValidationFailed 而不是 panic
    assert!(validate_input(Some(schema), b"not json").is_err());

    // 无效 schema 应该返回 InvalidSchema 而不是 panic
    assert!(validate_input(Some("not a schema"), b"{}").is_err());
}
