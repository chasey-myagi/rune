use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::error::error_response;
use crate::state::GateState;

/// Sanitize the stored MIME type to block browser-executable content types.
///
/// The uploader controls `Content-Type`, so a malicious client could store
/// `text/html` or `application/javascript`.  Even with `X-Content-Type-Options:
/// nosniff` and `Content-Disposition: attachment`, some browsers (and non-browser
/// clients) will render or execute certain MIME types.  We override the small set
/// of truly dangerous scriptable types with `application/octet-stream` while
/// preserving all other types (including `text/plain`, images, audio, video).
fn safe_mime(raw: &str) -> String {
    let base = raw
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    // Block only the types that browsers can execute as scripts or active content.
    // text/svg+xml is included because SVG files can embed <script> elements.
    let dangerous = matches!(
        base.as_str(),
        "text/html"
            | "application/javascript"
            | "application/x-javascript"
            | "text/javascript"
            | "application/xhtml+xml"
            | "text/xml"
            | "application/xml"
            | "text/svg+xml"
            | "image/svg+xml"
            | "application/x-shockwave-flash"
            | "application/vnd.ms-powerpoint"
    );
    if dangerous {
        "application/octet-stream".to_owned()
    } else {
        raw.to_owned()
    }
}

pub async fn download_file(
    State(state): State<GateState>,
    Path(file_id): Path<String>,
) -> axum::response::Response {
    // Files are cleaned up by complete_request() after the rune handler returns.
    // Use get() here so downloads can be retried if the connection drops.
    match state.rune.file_broker.get(&file_id) {
        Some(stored) => {
            let sanitized_mime = safe_mime(&stored.mime_type);
            let headers = [
                (axum::http::header::CONTENT_TYPE, sanitized_mime),
                (
                    axum::http::header::CONTENT_DISPOSITION,
                    format!(
                        "attachment; filename=\"{}\"",
                        stored.filename.replace('\\', "\\\\").replace('"', "\\\"")
                    ),
                ),
            ];
            match stored.data_async().await {
                Ok(data) => (StatusCode::OK, headers, data).into_response(),
                Err(_) => error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "FILE_READ_ERROR",
                    "failed to read file data",
                ),
            }
        }
        None => error_response(StatusCode::NOT_FOUND, "NOT_FOUND", "file not found"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_html_becomes_octet_stream() {
        assert_eq!(safe_mime("text/html"), "application/octet-stream");
        assert_eq!(
            safe_mime("text/html; charset=utf-8"),
            "application/octet-stream"
        );
    }

    #[test]
    fn javascript_becomes_octet_stream() {
        assert_eq!(
            safe_mime("application/javascript"),
            "application/octet-stream"
        );
        assert_eq!(
            safe_mime("application/xhtml+xml"),
            "application/octet-stream"
        );
    }

    #[test]
    fn safe_types_pass_through() {
        assert_eq!(safe_mime("image/png"), "image/png");
        assert_eq!(safe_mime("image/jpeg"), "image/jpeg");
        assert_eq!(
            safe_mime("application/octet-stream"),
            "application/octet-stream"
        );
        assert_eq!(safe_mime("application/pdf"), "application/pdf");
        assert_eq!(safe_mime("text/plain"), "text/plain");
        assert_eq!(safe_mime("text/csv"), "text/csv");
    }
}
