use crate::time_utils::{generate_span_id, generate_trace_id};
use std::collections::HashMap;

pub const TRACE_ID_KEY: &str = "trace_id";
pub const SPAN_ID_KEY: &str = "span_id";
pub const PARENT_SPAN_ID_KEY: &str = "parent_span_id";
pub const PARENT_REQUEST_ID_KEY: &str = "parent_request_id";
pub const TRACE_FLAGS_KEY: &str = "trace_flags";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: String,
    pub parent_span_id: String,
    pub trace_flags: String,
}

pub fn parse_traceparent(header: &str) -> Option<TraceContext> {
    let parts: Vec<_> = header.split('-').collect();
    if parts.len() != 4 || parts[0] != "00" {
        return None;
    }

    let trace_id = parts[1];
    let parent_span_id = parts[2];
    let trace_flags = parts[3];

    if trace_id.len() != 32
        || parent_span_id.len() != 16
        || trace_flags.len() != 2
        || !trace_id.chars().all(|c| c.is_ascii_hexdigit())
        || !parent_span_id.chars().all(|c| c.is_ascii_hexdigit())
        || !trace_flags.chars().all(|c| c.is_ascii_hexdigit())
    {
        return None;
    }

    Some(TraceContext {
        trace_id: trace_id.to_ascii_lowercase(),
        parent_span_id: parent_span_id.to_ascii_lowercase(),
        trace_flags: trace_flags.to_ascii_lowercase(),
    })
}

pub fn extract_trace_context<'a, I>(headers: I) -> HashMap<String, String>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut traceparent = None;
    let mut x_trace_id = None;

    for (name, value) in headers {
        let name = name.to_ascii_lowercase();
        match name.as_str() {
            "traceparent" => traceparent = Some(value.trim().to_string()),
            "x-trace-id" => x_trace_id = Some(value.trim().to_string()),
            _ => {}
        }
    }

    let mut context = HashMap::new();
    if let Some(header) = traceparent {
        if let Some(parsed) = parse_traceparent(&header) {
            context.insert(TRACE_ID_KEY.to_string(), parsed.trace_id);
            context.insert(PARENT_SPAN_ID_KEY.to_string(), parsed.parent_span_id);
            context.insert(TRACE_FLAGS_KEY.to_string(), parsed.trace_flags);
            return context;
        }
    }

    context.insert(
        TRACE_ID_KEY.to_string(),
        x_trace_id.unwrap_or_else(generate_trace_id),
    );
    context.insert(TRACE_FLAGS_KEY.to_string(), "01".to_string());
    context
}

pub fn ensure_trace_defaults(context: &mut HashMap<String, String>) {
    context
        .entry(TRACE_ID_KEY.to_string())
        .or_insert_with(generate_trace_id);
    context
        .entry(TRACE_FLAGS_KEY.to_string())
        .or_insert_with(|| "01".to_string());
    context
        .entry(SPAN_ID_KEY.to_string())
        .or_insert_with(generate_span_id);
}

pub fn format_traceparent(context: &HashMap<String, String>) -> Option<String> {
    let trace_id = context.get(TRACE_ID_KEY)?;
    let span_id = context.get(SPAN_ID_KEY)?;
    let flags = context
        .get(TRACE_FLAGS_KEY)
        .cloned()
        .unwrap_or_else(|| "01".to_string());
    Some(format!("00-{trace_id}-{span_id}-{flags}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_traceparent_valid() {
        let parsed =
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
        assert_eq!(parsed.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(parsed.parent_span_id, "00f067aa0ba902b7");
        assert_eq!(parsed.trace_flags, "01");
    }

    #[test]
    fn parse_traceparent_invalid_version() {
        assert!(
            parse_traceparent("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").is_none()
        );
    }

    #[test]
    fn parse_traceparent_invalid_format() {
        assert!(parse_traceparent("00-short-00f067aa0ba902b7-01").is_none());
        assert!(parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-01").is_none());
    }

    #[test]
    fn extract_traceparent_priority_over_x_trace_id() {
        let context = extract_trace_context([
            (
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            ),
            ("x-trace-id", "11111111111111111111111111111111"),
        ]);

        assert_eq!(
            context.get(TRACE_ID_KEY).map(String::as_str),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        assert_eq!(
            context.get(PARENT_SPAN_ID_KEY).map(String::as_str),
            Some("00f067aa0ba902b7")
        );
    }

    #[test]
    fn extract_generates_trace_id_when_missing() {
        let context = extract_trace_context(std::iter::empty());
        assert_eq!(context.get(TRACE_ID_KEY).unwrap().len(), 32);
        assert_eq!(context.get(TRACE_FLAGS_KEY).map(String::as_str), Some("01"));
    }

    #[test]
    fn format_traceparent_uses_context_fields() {
        let mut context = HashMap::new();
        context.insert(
            TRACE_ID_KEY.to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
        );
        context.insert(SPAN_ID_KEY.to_string(), "00f067aa0ba902b7".to_string());
        context.insert(TRACE_FLAGS_KEY.to_string(), "01".to_string());

        assert_eq!(
            format_traceparent(&context).as_deref(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
    }
}
