pub mod file;
pub mod flow;
pub mod mgmt;
pub mod openapi;
pub mod rune;
pub mod task;

/// Encode binary output as a hex-prefixed string for JSON/SSE transport.
/// UTF-8 text is preserved as-is; non-UTF-8 bytes become "hex:<lowercase-hex>".
pub fn encode_binary_output(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => format!("hex:{}", hex::encode(bytes)),
    }
}
