use axum::http::{HeaderMap, HeaderValue};
use rune_core::trace::{self, format_traceparent, TRACE_ID_KEY};
use std::collections::HashMap;

pub(crate) fn context_from_headers(headers: &HeaderMap) -> HashMap<String, String> {
    let mut context = trace::extract_trace_context(
        headers
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|value| (name.as_str(), value))),
    );
    trace::ensure_trace_defaults(&mut context);
    context
}

pub(crate) fn apply_trace_response_headers(
    response: &mut axum::response::Response,
    request_id: &str,
    context: &HashMap<String, String>,
) {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        response.headers_mut().insert("x-request-id", value);
    }

    if let Some(trace_id) = context.get(TRACE_ID_KEY) {
        if let Ok(value) = HeaderValue::from_str(trace_id) {
            response.headers_mut().insert("x-trace-id", value);
        }
    }

    if let Some(traceparent) = format_traceparent(context) {
        if let Ok(value) = HeaderValue::from_str(&traceparent) {
            response.headers_mut().insert("traceparent", value);
        }
    }
}
