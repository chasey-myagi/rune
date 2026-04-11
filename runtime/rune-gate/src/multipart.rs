use bytes::Bytes;

use crate::error::error_response_with_id;
use crate::state::GateState;
use axum::http::StatusCode;

/// Threshold for inline vs broker transfer: 4 MB.
/// Files smaller than this are transferred inline in the JSON response;
/// larger files go through the file broker and are fetched via `/api/v1/files/:id`.
const INLINE_THRESHOLD: usize = 4 * 1024 * 1024;

/// File metadata for the response JSON.
#[derive(serde::Serialize, Clone)]
pub struct FileMetadata {
    pub file_id: String,
    pub filename: String,
    pub mime_type: String,
    pub size: usize,
    pub transfer_mode: String,
}

/// Sanitize a filename: remove path components, limit length.
pub fn sanitize_filename(name: &str) -> String {
    // Normalize Windows backslashes to forward slashes before extraction
    let normalized = name.replace('\\', "/");

    // Use std::path to safely extract just the file name component
    let extracted = std::path::Path::new(&normalized)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    // Trim leading dots (prevent hidden files / traversal remnants)
    let name = extracted.trim_start_matches('.').to_string();

    // If empty after extraction, generate a placeholder
    if name.is_empty() {
        return format!("upload_{}", uuid::Uuid::new_v4());
    }

    // Limit length to 255 characters (char-based, safe for multi-byte UTF-8)
    if name.chars().count() > 255 {
        // Try to preserve extension
        if let Some(dot_pos) = name.rfind('.') {
            let ext: String = name[dot_pos..].to_string();
            let ext_chars = ext.chars().count();
            if ext_chars < 255 {
                let stem_chars = 255 - ext_chars;
                let stem: String = name.chars().take(stem_chars).collect();
                return format!("{}{}", stem, ext);
            }
        }
        name.chars().take(255).collect()
    } else {
        name
    }
}

/// Extract the boundary from a multipart/form-data content-type header.
fn extract_boundary(content_type: &str) -> Option<String> {
    content_type.split(';').find_map(|part| {
        part.trim()
            .strip_prefix("boundary=")
            .map(|val| val.trim_matches('"').to_string())
    })
}

/// Result of parsing a multipart body.
pub struct MultipartParseResult {
    /// JSON input part (if any), to be passed to the rune handler.
    pub json_input: Option<Bytes>,
    /// File metadata and stored file_ids.
    pub files: Vec<FileMetadata>,
    /// file_ids for cleanup after rune execution.
    pub file_ids: Vec<String>,
}

/// Parse a multipart body, store files in the broker, and return parsed data.
pub async fn parse_multipart(
    state: &GateState,
    content_type: &str,
    body: Bytes,
    request_id: &str,
) -> Result<MultipartParseResult, axum::response::Response> {
    let boundary = match extract_boundary(content_type) {
        Some(b) => b,
        None => {
            return Err(error_response_with_id(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "missing boundary in multipart content-type",
                Some(request_id),
            ));
        }
    };

    let max_size_bytes = state.rune.max_upload_size_mb as usize * 1024 * 1024;

    let stream = futures_free_multipart_stream(body, &boundary);
    let mut json_input: Option<Bytes> = None;
    let mut files: Vec<FileMetadata> = Vec::new();
    let mut file_ids: Vec<String> = Vec::new();
    let mut total_file_size: usize = 0;

    for part in stream {
        let part = match part {
            Ok(p) => p,
            Err(e) => {
                // Clean up any files already stored
                for fid in &file_ids {
                    state.rune.file_broker.remove(fid);
                }
                return Err(error_response_with_id(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    &format!("failed to parse multipart body: {}", e),
                    Some(request_id),
                ));
            }
        };

        if part.filename.is_some()
            || (part.name != "input" && part.content_type.as_deref() != Some("application/json"))
        {
            // This is a file part
            let raw_filename = part.filename.unwrap_or_default();
            let filename = sanitize_filename(&raw_filename);
            let mime_type = part
                .content_type
                .unwrap_or_else(|| "application/octet-stream".to_string());
            let data = Bytes::from(part.data);
            let size = data.len();

            total_file_size += size;

            // Check size limit
            if total_file_size > max_size_bytes {
                // Clean up any files already stored
                for fid in &file_ids {
                    state.rune.file_broker.remove(fid);
                }
                return Err(error_response_with_id(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "PAYLOAD_TOO_LARGE",
                    "total upload size exceeds maximum allowed size",
                    Some(request_id),
                ));
            }

            // Determine transfer mode
            let transfer_mode = if size <= INLINE_THRESHOLD {
                "inline".to_string()
            } else {
                "broker".to_string()
            };

            // Store in broker
            let file_id =
                state
                    .rune
                    .file_broker
                    .store(filename.clone(), mime_type.clone(), data, request_id);
            file_ids.push(file_id.clone());

            files.push(FileMetadata {
                file_id,
                filename,
                mime_type,
                size,
                transfer_mode,
            });
        } else {
            // This is a JSON input part
            json_input = Some(Bytes::from(part.data));
        }
    }

    Ok(MultipartParseResult {
        json_input,
        files,
        file_ids,
    })
}

/// A simple multipart part.
struct MultipartPart {
    name: String,
    filename: Option<String>,
    content_type: Option<String>,
    data: Vec<u8>,
}

/// Parse multipart body without external dependencies (simple synchronous parser).
fn futures_free_multipart_stream(
    body: Bytes,
    boundary: &str,
) -> Vec<Result<MultipartPart, String>> {
    let body = body.to_vec();
    parse_multipart_binary(&body, boundary)
}

fn parse_multipart_binary(body: &[u8], boundary: &str) -> Vec<Result<MultipartPart, String>> {
    let delimiter = format!("--{}", boundary).into_bytes();
    let end_delimiter = format!("--{}--", boundary).into_bytes();
    let mut results = Vec::new();

    // Check for end delimiter presence
    let has_end_delimiter = body
        .windows(end_delimiter.len())
        .any(|w| w == end_delimiter.as_slice());

    // Find all delimiter positions
    let mut positions = Vec::new();
    let mut pos = 0;
    while pos + delimiter.len() <= body.len() {
        if &body[pos..pos + delimiter.len()] == delimiter.as_slice() {
            positions.push(pos);
        }
        pos += 1;
    }

    if positions.is_empty() {
        results.push(Err("no multipart boundaries found".to_string()));
        return results;
    }

    if !has_end_delimiter {
        results.push(Err(
            "truncated multipart body: missing closing boundary".to_string()
        ));
        return results;
    }

    let mut _found_end = false;

    for i in 0..positions.len() {
        let start = positions[i] + delimiter.len();
        // Check if this is the end delimiter
        if start + 2 <= body.len() && &body[start..start + 2] == b"--" {
            _found_end = true;
            break; // End delimiter
        }

        let end = if i + 1 < positions.len() {
            positions[i + 1]
        } else {
            body.len()
        };

        let part_data = &body[start..end];

        // Skip leading \r\n after delimiter
        let part_data = if part_data.starts_with(b"\r\n") {
            &part_data[2..]
        } else if part_data.starts_with(b"\n") {
            &part_data[1..]
        } else {
            part_data
        };

        // Find the header/body separator (double CRLF or double LF)
        let (header_end, sep_len) = match find_double_crlf(part_data) {
            Some(v) => v,
            None => {
                // Might be end delimiter or malformed
                continue;
            }
        };
        let header_bytes = &part_data[..header_end];
        let body_start = header_end + sep_len;

        let headers_str = match std::str::from_utf8(header_bytes) {
            Ok(s) => s,
            Err(_) => {
                results.push(Err("invalid UTF-8 in headers".to_string()));
                continue;
            }
        };

        // Parse headers
        let mut name = String::new();
        let mut filename: Option<String> = None;
        let mut content_type: Option<String> = None;

        for line in headers_str.lines() {
            let line = line.trim();
            if let Some(val) = line.strip_prefix("Content-Disposition:") {
                let val = val.trim();
                // Extract name
                if let Some(n) = extract_header_param(val, "name") {
                    name = n;
                }
                // Extract filename
                if let Some(f) = extract_header_param(val, "filename") {
                    filename = Some(f);
                }
            } else if let Some(val) = line.strip_prefix("Content-Type:") {
                content_type = Some(val.trim().to_string());
            }
        }

        // Get body data (strip trailing \r\n before next delimiter)
        let mut body_data = if body_start < part_data.len() {
            part_data[body_start..].to_vec()
        } else {
            Vec::new()
        };

        // Strip trailing \r\n
        if body_data.ends_with(b"\r\n") {
            body_data.truncate(body_data.len() - 2);
        }

        results.push(Ok(MultipartPart {
            name,
            filename,
            content_type,
            data: body_data,
        }));
    }

    if results.is_empty() {
        results.push(Err("no valid parts found in multipart body".to_string()));
    }

    results
}

/// Returns (position, separator_length) — separator is either \r\n\r\n (4) or \n\n (2).
fn find_double_crlf(data: &[u8]) -> Option<(usize, usize)> {
    // First try standard \r\n\r\n
    for i in 0..data.len().saturating_sub(3) {
        if &data[i..i + 4] == b"\r\n\r\n" {
            return Some((i, 4));
        }
    }
    // Fallback: try \n\n (some clients send LF-only)
    for i in 0..data.len().saturating_sub(1) {
        if &data[i..i + 2] == b"\n\n" {
            return Some((i, 2));
        }
    }
    None
}

fn extract_header_param(header: &str, param: &str) -> Option<String> {
    let search = format!("{}=\"", param);
    if let Some(start) = header.find(&search) {
        let start = start + search.len();
        if let Some(end) = header[start..].find('"') {
            return Some(header[start..start + end].to_string());
        }
    }
    // Try without quotes
    let search = format!("{}=", param);
    if let Some(start) = header.find(&search) {
        let start = start + search.len();
        let end = header[start..].find(';').unwrap_or(header[start..].len());
        let val = header[start..start + end].trim().to_string();
        if !val.is_empty() {
            return Some(val);
        }
    }
    None
}

/// Check if the content-type indicates multipart/form-data.
pub fn is_multipart(content_type: &str) -> bool {
    content_type.starts_with("multipart/form-data")
}

/// Build a multipart body — exposed for test helpers.
pub fn build_multipart_body(
    boundary: &str,
    parts: &[(&str, Option<&str>, &str, &[u8])],
) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, filename, content_type, data) in parts {
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        match filename {
            Some(fname) => {
                body.extend_from_slice(
                    format!(
                        "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n",
                        name, fname
                    )
                    .as_bytes(),
                );
            }
            None => {
                body.extend_from_slice(
                    format!("Content-Disposition: form-data; name=\"{}\"\r\n", name).as_bytes(),
                );
            }
        }
        body.extend_from_slice(format!("Content-Type: {}\r\n\r\n", content_type).as_bytes());
        body.extend_from_slice(data);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    // === MF-2 regression tests ===

    #[test]
    fn test_sanitize_double_dot_bypass() {
        // "..../file" with single replace("..","") becomes "../file" — must be safe
        let result = sanitize_filename("..../file.txt");
        assert!(
            !result.contains(".."),
            "should not contain '..' after sanitize, got: {}",
            result
        );
        assert!(
            !result.contains('/'),
            "should not contain '/' after sanitize, got: {}",
            result
        );
        assert!(
            result.contains("file"),
            "should preserve 'file' part, got: {}",
            result
        );
    }

    #[test]
    fn test_sanitize_nested_dots_no_slash() {
        // Pure filename with nested dots should be handled safely
        let result = sanitize_filename("....secret");
        assert!(
            !result.contains(".."),
            "should not contain '..' after sanitize, got: {}",
            result
        );
    }

    #[test]
    fn test_sanitize_long_multibyte_no_panic() {
        // 256 multi-byte chars: byte-level truncation `name[..255]` would panic
        // on a char boundary. Must use char-based truncation.
        let long_name: String = "\u{00e9}".repeat(256); // each é is 2 bytes
        let result = sanitize_filename(&long_name);
        assert!(result.chars().count() <= 255);
    }

    #[test]
    fn test_sanitize_long_with_extension_multibyte() {
        // Long multibyte stem + extension, the extension-preserving path
        // would also panic on `&name[..stem_len]` if stem_len is mid-char
        let stem: String = "\u{00e9}".repeat(250);
        let name = format!("{}.txt", stem);
        let result = sanitize_filename(&name);
        assert!(result.chars().count() <= 255);
        assert!(result.ends_with(".txt"));
    }

    #[test]
    fn test_sanitize_path_traversal() {
        let result = sanitize_filename("../../etc/passwd");
        assert!(!result.contains(".."), "got: {}", result);
        assert_eq!(result, "passwd");
    }

    #[test]
    fn test_sanitize_preserves_normal_filename() {
        assert_eq!(sanitize_filename("photo.jpg"), "photo.jpg");
        assert_eq!(sanitize_filename("my document.pdf"), "my document.pdf");
    }

    #[test]
    fn test_sanitize_strips_directory_components() {
        assert_eq!(sanitize_filename("/usr/local/bin/test.sh"), "test.sh");
        assert_eq!(sanitize_filename("C:\\Users\\test\\file.txt"), "file.txt");
    }

    #[test]
    fn test_sanitize_long_utf8_filename() {
        // 300 multi-byte chars should be truncated to 255 chars, not panic
        let long_name: String = "a".repeat(300);
        let result = sanitize_filename(&long_name);
        assert!(result.chars().count() <= 255);

        // Multi-byte chars: should not panic on boundary
        let long_cjk: String = "\u{4e2d}".repeat(300);
        let result = sanitize_filename(&long_cjk);
        assert!(result.chars().count() <= 255);
    }

    #[test]
    fn test_sanitize_empty_filename() {
        let result = sanitize_filename("");
        assert!(
            !result.is_empty(),
            "empty filenames should get a generated name"
        );
        assert!(result.starts_with("upload_"));
    }

    #[test]
    fn test_find_double_crlf_with_lf_only() {
        // find_double_crlf should also match \n\n as fallback
        let data = b"Header: value\n\nbody content";
        let result = find_double_crlf(data);
        assert!(result.is_some(), "should find \\n\\n as fallback");
        let (pos, len) = result.unwrap();
        assert_eq!(pos, 13);
        assert_eq!(len, 2);
    }

    #[test]
    fn test_find_double_crlf_prefers_crlf() {
        let data = b"Header: value\r\n\r\nbody content";
        let (pos, len) = find_double_crlf(data).unwrap();
        assert_eq!(pos, 13);
        assert_eq!(len, 4);
    }
}
