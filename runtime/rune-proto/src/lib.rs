pub mod rune {
    pub mod wire {
        pub mod v1 {
            tonic::include_proto!("rune.wire.v1");
        }
    }
}

pub use rune::wire::v1::*;

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    // ========================================================================
    // SessionMessage round-trip
    // ========================================================================

    #[test]
    fn session_message_heartbeat_round_trip() {
        let msg = SessionMessage {
            payload: Some(session_message::Payload::Heartbeat(Heartbeat {
                timestamp_ms: 1700000000000,
            })),
        };

        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert!(!buf.is_empty());

        let decoded = SessionMessage::decode(&buf[..]).unwrap();
        match decoded.payload {
            Some(session_message::Payload::Heartbeat(hb)) => {
                assert_eq!(hb.timestamp_ms, 1700000000000);
            }
            other => panic!("expected Heartbeat, got {:?}", other),
        }
    }

    #[test]
    fn session_message_attach_round_trip() {
        let msg = SessionMessage {
            payload: Some(session_message::Payload::Attach(CasterAttach {
                caster_id: "caster-1".into(),
                protocol_version: String::new(),
                runes: vec![RuneDeclaration {
                    name: "echo".into(),
                    version: "1.0.0".into(),
                    description: "echo rune".into(),
                    input_schema: r#"{"type":"string"}"#.into(),
                    output_schema: String::new(),
                    supports_stream: true,
                    gate: Some(GateConfig {
                        path: "/api/echo".into(),
                        method: "POST".into(),
                    }),
                    priority: 5,
                }],
                labels: [("env".to_string(), "prod".to_string())]
                    .into_iter()
                    .collect(),
                max_concurrent: 10,
                key: "rk_test_key".into(),
                role: "caster".into(),
            })),
        };

        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let decoded = SessionMessage::decode(&buf[..]).unwrap();

        match decoded.payload {
            Some(session_message::Payload::Attach(attach)) => {
                assert_eq!(attach.caster_id, "caster-1");
                assert_eq!(attach.max_concurrent, 10);
                assert_eq!(attach.runes.len(), 1);
                let rune = &attach.runes[0];
                assert_eq!(rune.name, "echo");
                assert_eq!(rune.version, "1.0.0");
                assert_eq!(rune.description, "echo rune");
                assert_eq!(rune.input_schema, r#"{"type":"string"}"#);
                assert!(rune.supports_stream);
                assert_eq!(rune.priority, 5);
                let gate = rune.gate.as_ref().unwrap();
                assert_eq!(gate.path, "/api/echo");
                assert_eq!(gate.method, "POST");
                assert_eq!(attach.labels.get("env").map(|s| s.as_str()), Some("prod"));
            }
            other => panic!("expected Attach, got {:?}", other),
        }
    }

    // ========================================================================
    // RuneDeclaration with schema round-trip
    // ========================================================================

    #[test]
    fn rune_declaration_with_schema_round_trip() {
        let decl = RuneDeclaration {
            name: "validator".into(),
            version: "2.1.0".into(),
            description: "validates input".into(),
            input_schema: r#"{"type":"object","properties":{"name":{"type":"string"}}}"#.into(),
            output_schema: r#"{"type":"boolean"}"#.into(),
            supports_stream: false,
            gate: None,
            priority: 0,
        };

        let mut buf = Vec::new();
        decl.encode(&mut buf).unwrap();
        let decoded = RuneDeclaration::decode(&buf[..]).unwrap();

        assert_eq!(decoded.name, "validator");
        assert_eq!(decoded.version, "2.1.0");
        assert_eq!(decoded.description, "validates input");
        assert_eq!(
            decoded.input_schema,
            r#"{"type":"object","properties":{"name":{"type":"string"}}}"#
        );
        assert_eq!(decoded.output_schema, r#"{"type":"boolean"}"#);
        assert!(!decoded.supports_stream);
        assert!(decoded.gate.is_none());
    }

    // ========================================================================
    // Status enum values
    // ========================================================================

    #[test]
    fn status_enum_values_correct() {
        assert_eq!(Status::Unspecified as i32, 0);
        assert_eq!(Status::Completed as i32, 1);
        assert_eq!(Status::Failed as i32, 2);
        assert_eq!(Status::Cancelled as i32, 3);
    }

    #[test]
    fn status_from_i32_round_trip() {
        assert_eq!(Status::try_from(0).unwrap(), Status::Unspecified);
        assert_eq!(Status::try_from(1).unwrap(), Status::Completed);
        assert_eq!(Status::try_from(2).unwrap(), Status::Failed);
        assert_eq!(Status::try_from(3).unwrap(), Status::Cancelled);
        assert!(Status::try_from(99).is_err());
    }

    // ========================================================================
    // ErrorDetail round-trip
    // ========================================================================

    #[test]
    fn error_detail_round_trip() {
        let err = ErrorDetail {
            code: "VALIDATION_ERROR".into(),
            message: "field 'name' is required".into(),
            details: b"extra context bytes".to_vec(),
        };

        let mut buf = Vec::new();
        err.encode(&mut buf).unwrap();
        let decoded = ErrorDetail::decode(&buf[..]).unwrap();

        assert_eq!(decoded.code, "VALIDATION_ERROR");
        assert_eq!(decoded.message, "field 'name' is required");
        assert_eq!(decoded.details, b"extra context bytes");
    }

    #[test]
    fn error_detail_empty_details_round_trip() {
        let err = ErrorDetail {
            code: "NOT_FOUND".into(),
            message: "resource gone".into(),
            details: Vec::new(),
        };

        let mut buf = Vec::new();
        err.encode(&mut buf).unwrap();
        let decoded = ErrorDetail::decode(&buf[..]).unwrap();

        assert_eq!(decoded.code, "NOT_FOUND");
        assert_eq!(decoded.message, "resource gone");
        assert!(decoded.details.is_empty());
    }

    // ========================================================================
    // ExecuteResult with error round-trip
    // ========================================================================

    #[test]
    fn execute_result_with_error_round_trip() {
        let result = ExecuteResult {
            request_id: "req-fail".into(),
            status: Status::Failed as i32,
            output: Vec::new(),
            error: Some(ErrorDetail {
                code: "RUNTIME_ERR".into(),
                message: "out of memory".into(),
                details: Vec::new(),
            }),
            attachments: vec![],
        };

        let mut buf = Vec::new();
        result.encode(&mut buf).unwrap();
        let decoded = ExecuteResult::decode(&buf[..]).unwrap();

        assert_eq!(decoded.request_id, "req-fail");
        assert_eq!(decoded.status(), Status::Failed);
        assert!(decoded.output.is_empty());
        let err = decoded.error.unwrap();
        assert_eq!(err.code, "RUNTIME_ERR");
        assert_eq!(err.message, "out of memory");
    }

    // ========================================================================
    // StreamEvent and StreamEnd round-trip
    // ========================================================================

    #[test]
    fn stream_event_round_trip() {
        let event = StreamEvent {
            request_id: "stream-1".into(),
            data: b"partial output".to_vec(),
            event_type: "progress".into(),
        };

        let mut buf = Vec::new();
        event.encode(&mut buf).unwrap();
        let decoded = StreamEvent::decode(&buf[..]).unwrap();

        assert_eq!(decoded.request_id, "stream-1");
        assert_eq!(decoded.data, b"partial output");
        assert_eq!(decoded.event_type, "progress");
    }

    #[test]
    fn stream_end_round_trip() {
        let end = StreamEnd {
            request_id: "stream-1".into(),
            status: Status::Completed as i32,
            error: None,
        };

        let mut buf = Vec::new();
        end.encode(&mut buf).unwrap();
        let decoded = StreamEnd::decode(&buf[..]).unwrap();

        assert_eq!(decoded.request_id, "stream-1");
        assert_eq!(decoded.status(), Status::Completed);
        assert!(decoded.error.is_none());
    }

    // ========================================================================
    // CancelRequest round-trip
    // ========================================================================

    #[test]
    fn cancel_request_round_trip() {
        let cancel = CancelRequest {
            request_id: "req-to-cancel".into(),
            reason: "user abort".into(),
        };

        let mut buf = Vec::new();
        cancel.encode(&mut buf).unwrap();
        let decoded = CancelRequest::decode(&buf[..]).unwrap();

        assert_eq!(decoded.request_id, "req-to-cancel");
        assert_eq!(decoded.reason, "user abort");
    }
}
