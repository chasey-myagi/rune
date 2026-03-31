"""Unit tests for Rune Python SDK — test-matrix Part 1 (U-01 ~ U-37).

No runtime required. Tests SDK classes and registration logic only.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from rune import Caster, RuneConfig, RuneContext, FileAttachment, StreamSender


# ============================================================
# 1.1 Types and Configuration  (U-01 ~ U-10)
# ============================================================


def test_u01_rune_config_defaults():
    """U-01: RuneConfig with only name — others take defaults."""
    cfg = RuneConfig(name="echo")
    assert cfg.name == "echo"
    assert cfg.version == "0.0.0"
    assert cfg.supports_stream is False
    assert cfg.gate is None
    assert cfg.priority == 0


def test_u02_rune_config_all_fields():
    """U-02: RuneConfig with all fields explicitly set."""
    cfg = RuneConfig(
        name="translate",
        version="2.1.0",
        description="Translate text",
        supports_stream=True,
        gate="/translate",
        gate_method="PUT",
        input_schema={"type": "object"},
        output_schema={"type": "string"},
        priority=5,
    )
    assert cfg.name == "translate"
    assert cfg.version == "2.1.0"
    assert cfg.description == "Translate text"
    assert cfg.supports_stream is True
    assert cfg.gate == "/translate"
    assert cfg.gate_method == "PUT"
    assert cfg.input_schema == {"type": "object"}
    assert cfg.output_schema == {"type": "string"}
    assert cfg.priority == 5


def test_u03_rune_config_input_schema():
    """U-03: RuneConfig input_schema as JSON Schema dict."""
    schema = {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
    }
    cfg = RuneConfig(name="x", input_schema=schema)
    assert cfg.input_schema == schema


def test_u04_rune_config_output_schema():
    """U-04: RuneConfig output_schema as JSON Schema dict."""
    schema = {
        "type": "object",
        "properties": {"result": {"type": "number"}},
    }
    cfg = RuneConfig(name="x", output_schema=schema)
    assert cfg.output_schema == schema


def test_u05_gate_config_default_method():
    """U-05: GateConfig default method is POST."""
    cfg = RuneConfig(name="x", gate="/path")
    assert cfg.gate_method == "POST"


def test_u06_rune_context_fields():
    """U-06: RuneContext contains rune_name, request_id, context."""
    ctx = RuneContext(
        rune_name="echo",
        request_id="req-123",
        context={"user": "alice"},
    )
    assert ctx.rune_name == "echo"
    assert ctx.request_id == "req-123"
    assert ctx.context == {"user": "alice"}


def test_u07_caster_default_addr():
    """U-07: Caster default runtime address is localhost:50070."""
    c = Caster()
    assert c._addr == "localhost:50070"


def test_u08_file_attachment_fields():
    """U-08: FileAttachment contains filename, data, mime_type."""
    att = FileAttachment(filename="doc.pdf", data=b"binary", mime_type="application/pdf")
    assert att.filename == "doc.pdf"
    assert att.data == b"binary"
    assert att.mime_type == "application/pdf"


def test_u09_caster_id_default():
    """U-09: Caster default caster_id is not empty and has a default value."""
    c = Caster()
    assert c._caster_id == "python-caster"
    # default should not be empty
    assert len(c._caster_id) > 0


def test_u10_caster_id_custom():
    """U-10: Caster custom caster_id is stored."""
    c = Caster("localhost:50070", caster_id="my-caster-42")
    assert c._caster_id == "my-caster-42"


# ============================================================
# 1.2 Rune Registration  (U-11 ~ U-20)
# ============================================================


def test_u11_rune_register_unary():
    """U-11: rune() registers a unary handler."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("echo")
    async def echo(ctx, input):
        return input

    assert "echo" in c._runes
    assert c._runes["echo"].is_stream is False


def test_u12_stream_rune_register():
    """U-12: stream_rune() registers a stream handler with supports_stream=True."""
    c = Caster("localhost:50070", caster_id="test")

    @c.stream_rune("chat")
    async def chat(ctx, input, stream):
        await stream.emit(b"hello")

    assert "chat" in c._runes
    assert c._runes["chat"].config.supports_stream is True
    assert c._runes["chat"].is_stream is True


def test_u13_register_multiple_runes():
    """U-13: Register 3 runes — all present in registry."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("a")
    async def a(ctx, input):
        return input

    @c.rune("b")
    async def b(ctx, input):
        return input

    @c.stream_rune("c")
    async def c_handler(ctx, input, stream):
        pass

    assert len(c._runes) == 3
    assert "a" in c._runes
    assert "b" in c._runes
    assert "c" in c._runes


def test_u14_duplicate_rune_name_overwrites():
    """U-14: Registering duplicate name overwrites the previous handler."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("echo", version="1.0.0")
    async def echo_v1(ctx, input):
        return b"v1"

    @c.rune("echo", version="2.0.0")
    async def echo_v2(ctx, input):
        return b"v2"

    assert len(c._runes) == 1
    assert c._runes["echo"].config.version == "2.0.0"


def test_u15_register_with_gate_path():
    """U-15: Registration with gate path stores it correctly."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("translate", gate="/translate")
    async def translate(ctx, input):
        return input

    assert c._runes["translate"].config.gate == "/translate"


def test_u16_register_with_input_schema():
    """U-16: Registration with input_schema stores it correctly."""
    schema = {"type": "object", "properties": {"text": {"type": "string"}}}
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("x", input_schema=schema)
    async def handler(ctx, input):
        return input

    assert c._runes["x"].config.input_schema == schema


def test_u17_register_with_output_schema():
    """U-17: Registration with output_schema stores it correctly."""
    schema = {"type": "object", "properties": {"result": {"type": "number"}}}
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("x", output_schema=schema)
    async def handler(ctx, input):
        return input

    assert c._runes["x"].config.output_schema == schema


def test_u18_register_with_priority():
    """U-18: Registration with priority stores it correctly."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("important", priority=10)
    async def handler(ctx, input):
        return input

    assert c._runes["important"].config.priority == 10


@pytest.mark.asyncio
async def test_u19_handler_direct_call():
    """U-19: Unary handler can be called directly (not via gRPC)."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("echo")
    async def echo(ctx, input):
        return input.upper() if isinstance(input, bytes) else input

    ctx = RuneContext(rune_name="echo", request_id="r-1")
    result = await c._runes["echo"].handler(ctx, b"hello")
    assert result == b"HELLO"


@pytest.mark.asyncio
async def test_u20_stream_handler_direct_call():
    """U-20: Stream handler can be called directly, emit is invoked."""
    c = Caster("localhost:50070", caster_id="test")
    emitted = []

    @c.stream_rune("streamer")
    async def streamer(ctx, input, stream):
        await stream.emit(b"chunk1")
        await stream.emit(b"chunk2")
        await stream.end()

    async def mock_send(data: bytes) -> None:
        emitted.append(data)

    ctx = RuneContext(rune_name="streamer", request_id="r-1")
    sender = StreamSender(mock_send)
    await c._runes["streamer"].handler(ctx, b"", sender)

    assert len(emitted) == 2
    assert emitted[0] == b"chunk1"
    assert emitted[1] == b"chunk2"


# ============================================================
# 1.3 StreamSender  (U-21 ~ U-27)
# ============================================================


@pytest.mark.asyncio
async def test_u21_emit_bytes():
    """U-21: emit(bytes) sends data without error."""
    sent = []

    async def mock_send(data: bytes) -> None:
        sent.append(data)

    s = StreamSender(mock_send)
    await s.emit(b"hello")
    assert sent == [b"hello"]


@pytest.mark.asyncio
async def test_u22_emit_string_auto_encode():
    """U-22: emit(string) auto-encodes to UTF-8 bytes."""
    sent = []

    async def mock_send(data: bytes) -> None:
        sent.append(data)

    s = StreamSender(mock_send)
    await s.emit("hello world")
    assert sent == [b"hello world"]


@pytest.mark.asyncio
async def test_u23_emit_dict_auto_json():
    """U-23: emit(dict) auto-serializes to JSON bytes."""
    sent = []

    async def mock_send(data: bytes) -> None:
        sent.append(data)

    s = StreamSender(mock_send)
    await s.emit({"key": "value"})
    assert len(sent) == 1
    parsed = json.loads(sent[0])
    assert parsed == {"key": "value"}


@pytest.mark.asyncio
async def test_u24_end_marks_stream_ended():
    """U-24: end() marks stream as ended; subsequent emit raises."""
    async def mock_send(data: bytes) -> None:
        pass

    s = StreamSender(mock_send)
    await s.end()
    assert s._ended is True


@pytest.mark.asyncio
async def test_u25_emit_after_end_raises():
    """U-25: emit after end() raises RuntimeError."""
    async def mock_send(data: bytes) -> None:
        pass

    s = StreamSender(mock_send)
    await s.end()
    with pytest.raises(RuntimeError, match="stream already ended"):
        await s.emit(b"too late")


@pytest.mark.asyncio
async def test_u26_multiple_emits():
    """U-26: Multiple emit() calls all succeed."""
    sent = []

    async def mock_send(data: bytes) -> None:
        sent.append(data)

    s = StreamSender(mock_send)
    for i in range(5):
        await s.emit(f"chunk-{i}".encode())
    assert len(sent) == 5


@pytest.mark.asyncio
async def test_u27_end_idempotent():
    """U-27: end() is idempotent — multiple calls don't raise."""
    async def mock_send(data: bytes) -> None:
        pass

    s = StreamSender(mock_send)
    await s.end()
    await s.end()
    await s.end()
    assert s._ended is True


# ============================================================
# 1.4 Connection Configuration  (U-28 ~ U-33)
# ============================================================


def test_u28_reconnect_base_delay_configurable():
    """U-28: Reconnect initial delay is configurable."""
    c = Caster("localhost:50070", reconnect_base_delay=5.0)
    assert c._reconnect_base_delay == 5.0


def test_u29_reconnect_max_delay_configurable():
    """U-29: Reconnect max delay is configurable."""
    c = Caster("localhost:50070", reconnect_max_delay=120.0)
    assert c._reconnect_max_delay == 120.0


def test_u30_heartbeat_interval_configurable():
    """U-30: Heartbeat interval is configurable."""
    c = Caster("localhost:50070", heartbeat_interval=5.0)
    assert c._heartbeat_interval == 5.0


def test_u31_max_concurrent_configurable():
    """U-31: maxConcurrent is configurable."""
    c = Caster("localhost:50070", max_concurrent=50)
    assert c._max_concurrent == 50


def test_u32_labels_configurable():
    """U-32: labels is configurable."""
    labels = {"env": "prod", "region": "us-east-1"}
    c = Caster("localhost:50070", labels=labels)
    assert c._labels == labels


def test_u33_api_key_stored():
    """U-33: API key is passed and stored internally."""
    c = Caster("localhost:50070", api_key="sk-secret-key-123")
    assert c._api_key == "sk-secret-key-123"


# ============================================================
# 1.5 FileAttachment Detection  (U-34 ~ U-37)
# ============================================================


def test_u34_unary_handler_no_files():
    """U-34: handler(ctx, input) — accepts_files = False."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("no_files")
    async def handler(ctx, input):
        return input

    assert c._runes["no_files"].accepts_files is False


def test_u35_unary_handler_with_files():
    """U-35: handler(ctx, input, files) — accepts_files = True."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("with_files")
    async def handler(ctx, input, files):
        return input

    assert c._runes["with_files"].accepts_files is True


def test_u36_stream_handler_no_files():
    """U-36: stream handler(ctx, input, stream) — accepts_files = False."""
    c = Caster("localhost:50070", caster_id="test")

    @c.stream_rune("stream_no_files")
    async def handler(ctx, input, stream):
        pass

    assert c._runes["stream_no_files"].accepts_files is False


def test_u37_stream_handler_with_files():
    """U-37: stream handler(ctx, input, files, stream) — accepts_files = True."""
    c = Caster("localhost:50070", caster_id="test")

    @c.stream_rune("stream_with_files")
    async def handler(ctx, input, files, stream):
        pass

    assert c._runes["stream_with_files"].accepts_files is True


# ============================================================
# 1.6 Error & Edge Cases  (U-38 ~ U-55)
# ============================================================


def test_u38_caster_empty_string_addr():
    """U-38: Caster with empty string addr — does not crash."""
    c = Caster("")
    assert c._addr == ""


def test_u39_caster_none_caster_id():
    """U-39: Caster with None-like parameters — handles gracefully."""
    # caster_id defaults to "python-caster" when not provided
    c = Caster("localhost:50070")
    assert c._caster_id == "python-caster"


def test_u40_rune_config_empty_name():
    """U-40: RuneConfig with empty string name — does not crash."""
    cfg = RuneConfig(name="")
    assert cfg.name == ""
    assert cfg.version == "0.0.0"


def test_u41_rune_config_long_name():
    """U-41: RuneConfig with super-long name (1000 chars) — stores correctly."""
    long_name = "a" * 1000
    cfg = RuneConfig(name=long_name)
    assert cfg.name == long_name
    assert len(cfg.name) == 1000


def test_u42_rune_config_invalid_json_schema():
    """U-42: RuneConfig with arbitrary dict as input_schema — no validation at SDK level."""
    weird_schema = {"not": "a real schema", "random_key": [1, 2, 3]}
    cfg = RuneConfig(name="x", input_schema=weird_schema)
    assert cfg.input_schema == weird_schema


@pytest.mark.asyncio
async def test_u43_handler_returns_none():
    """U-43: Handler returns None — handler call succeeds (SDK doesn't validate)."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("returns_none")
    async def handler(ctx, input):
        return None

    ctx = RuneContext(rune_name="returns_none", request_id="r-1")
    result = await c._runes["returns_none"].handler(ctx, b"hello")
    assert result is None


@pytest.mark.asyncio
async def test_u44_handler_returns_string():
    """U-44: Handler returns a string instead of bytes — SDK doesn't enforce type."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("returns_str")
    async def handler(ctx, input):
        return "a string, not bytes"

    ctx = RuneContext(rune_name="returns_str", request_id="r-1")
    result = await c._runes["returns_str"].handler(ctx, b"")
    assert result == "a string, not bytes"


@pytest.mark.asyncio
async def test_u45_handler_returns_int():
    """U-45: Handler returns int — SDK doesn't enforce return type."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("returns_int")
    async def handler(ctx, input):
        return 42

    ctx = RuneContext(rune_name="returns_int", request_id="r-1")
    result = await c._runes["returns_int"].handler(ctx, b"")
    assert result == 42


@pytest.mark.asyncio
async def test_u46_handler_returns_dict():
    """U-46: Handler returns dict — SDK doesn't enforce return type."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("returns_dict")
    async def handler(ctx, input):
        return {"key": "value"}

    ctx = RuneContext(rune_name="returns_dict", request_id="r-1")
    result = await c._runes["returns_dict"].handler(ctx, b"")
    assert result == {"key": "value"}


@pytest.mark.asyncio
async def test_u47_stream_emit_empty_bytes():
    """U-47: StreamSender emit empty bytes — succeeds."""
    sent = []

    async def mock_send(data: bytes) -> None:
        sent.append(data)

    s = StreamSender(mock_send)
    await s.emit(b"")
    assert sent == [b""]


@pytest.mark.asyncio
async def test_u48_stream_emit_large_data():
    """U-48: StreamSender emit 1MB data — succeeds."""
    sent = []

    async def mock_send(data: bytes) -> None:
        sent.append(data)

    s = StreamSender(mock_send)
    big = b"x" * (1024 * 1024)
    await s.emit(big)
    assert len(sent) == 1
    assert len(sent[0]) == 1024 * 1024


def test_u49_file_attachment_empty_data():
    """U-49: FileAttachment with empty bytes data — no crash."""
    att = FileAttachment(filename="empty.bin", data=b"", mime_type="application/octet-stream")
    assert att.data == b""
    assert len(att.data) == 0


def test_u50_file_attachment_path_separator_in_filename():
    """U-50: FileAttachment filename with path separator — stored as-is."""
    att = FileAttachment(
        filename="../../etc/passwd",
        data=b"danger",
        mime_type="text/plain",
    )
    assert att.filename == "../../etc/passwd"


@pytest.mark.asyncio
async def test_u51_registered_handler_direct_call():
    """U-51: Access handler through _runes registry and call directly."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("direct")
    async def handler(ctx, input):
        return b"direct-" + input

    reg = c._runes["direct"]
    assert reg.config.name == "direct"
    assert reg.is_stream is False

    ctx = RuneContext(rune_name="direct", request_id="r-1")
    result = await reg.handler(ctx, b"call")
    assert result == b"direct-call"


@pytest.mark.asyncio
async def test_u52_stream_handler_no_emit():
    """U-52: Stream handler that doesn't call emit — ends cleanly."""
    c = Caster("localhost:50070", caster_id="test")
    emitted = []

    @c.stream_rune("no_emit")
    async def handler(ctx, input, stream):
        # Intentionally not calling emit or end
        pass

    async def mock_send(data: bytes) -> None:
        emitted.append(data)

    ctx = RuneContext(rune_name="no_emit", request_id="r-1")
    sender = StreamSender(mock_send)
    await c._runes["no_emit"].handler(ctx, b"", sender)

    assert emitted == []


# ============================================================
# 1.7 Handler Signature Backward Compatibility  (U-53 ~ U-56)
# ============================================================


@pytest.mark.asyncio
async def test_u53_old_unary_signature_ctx_input():
    """U-53: Old unary handler (ctx, input) still works and accepts_files=False."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("old_unary")
    async def handler(ctx, input):
        return input.upper() if isinstance(input, bytes) else input

    assert c._runes["old_unary"].accepts_files is False
    ctx = RuneContext(rune_name="old_unary", request_id="r-1")
    result = await c._runes["old_unary"].handler(ctx, b"abc")
    assert result == b"ABC"


@pytest.mark.asyncio
async def test_u54_old_stream_signature_ctx_input_stream():
    """U-54: Old stream handler (ctx, input, stream) still works and accepts_files=False."""
    c = Caster("localhost:50070", caster_id="test")
    emitted = []

    @c.stream_rune("old_stream")
    async def handler(ctx, input, stream):
        await stream.emit(b"ok")

    assert c._runes["old_stream"].accepts_files is False

    async def mock_send(data: bytes) -> None:
        emitted.append(data)

    ctx = RuneContext(rune_name="old_stream", request_id="r-1")
    sender = StreamSender(mock_send)
    await c._runes["old_stream"].handler(ctx, b"", sender)
    assert emitted == [b"ok"]


@pytest.mark.asyncio
async def test_u55_new_unary_signature_ctx_input_files():
    """U-55: New unary handler (ctx, input, files) works and accepts_files=True."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("new_unary")
    async def handler(ctx, input, files):
        return json.dumps({"file_count": len(files)}).encode()

    assert c._runes["new_unary"].accepts_files is True
    ctx = RuneContext(rune_name="new_unary", request_id="r-1")
    att = FileAttachment(filename="test.txt", data=b"hi", mime_type="text/plain")
    result = await c._runes["new_unary"].handler(ctx, b"", [att])
    parsed = json.loads(result)
    assert parsed["file_count"] == 1


@pytest.mark.asyncio
async def test_u56_new_stream_signature_ctx_input_files_stream():
    """U-56: New stream handler (ctx, input, files, stream) works and accepts_files=True."""
    c = Caster("localhost:50070", caster_id="test")
    emitted = []

    @c.stream_rune("new_stream")
    async def handler(ctx, input, files, stream):
        await stream.emit(f"got {len(files)} files".encode())

    assert c._runes["new_stream"].accepts_files is True

    async def mock_send(data: bytes) -> None:
        emitted.append(data)

    ctx = RuneContext(rune_name="new_stream", request_id="r-1")
    sender = StreamSender(mock_send)
    att = FileAttachment(filename="a.txt", data=b"aaa", mime_type="text/plain")
    await c._runes["new_stream"].handler(ctx, b"", [att], sender)
    assert emitted == [b"got 1 files"]


# ============================================================
# 2.0 CasterAttach Message  (C-01 ~ C-02)
# ============================================================


def test_c01_attach_message_contains_key():
    """C-01: CasterAttach message includes api_key when provided."""
    c = Caster("localhost:50070", caster_id="test", api_key="sk-secret-123")

    @c.rune("echo")
    async def echo(ctx, input):
        return input

    msg = c._build_attach_message()
    assert msg.attach.key == "sk-secret-123"


def test_c01b_attach_message_key_defaults_empty():
    """C-01b: CasterAttach message key defaults to empty string when no api_key."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("echo")
    async def echo(ctx, input):
        return input

    msg = c._build_attach_message()
    assert msg.attach.key == ""


def test_c02_attach_message_contains_labels():
    """C-02: CasterAttach message includes labels when provided."""
    labels = {"env": "prod", "region": "us-east-1"}
    c = Caster("localhost:50070", caster_id="test", labels=labels)

    @c.rune("echo")
    async def echo(ctx, input):
        return input

    msg = c._build_attach_message()
    assert dict(msg.attach.labels) == labels


def test_c02b_attach_message_labels_defaults_empty():
    """C-02b: CasterAttach message labels defaults to empty when no labels."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("echo")
    async def echo(ctx, input):
        return input

    msg = c._build_attach_message()
    assert dict(msg.attach.labels) == {}


# ============================================================
# MF-5: Non-async handler should produce clear error, not TypeError
# ============================================================


@pytest.mark.asyncio
async def test_mf5_sync_handler_gives_clear_error():
    """MF-5: A non-async (sync) handler registered via @rune should either
    be auto-wrapped or produce a clear error at execute time, not a raw
    'object NoneType can't be used in await expression' TypeError."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("sync_echo")
    def sync_handler(ctx, input):
        return input.upper() if isinstance(input, bytes) else input

    ctx = RuneContext(rune_name="sync_echo", request_id="r-1")

    # After the fix, calling a sync handler through the internal
    # execute path should work (auto-wrapped) or raise a clear error.
    # We verify by calling _execute_once with a mock outbound queue.
    outbound: asyncio.Queue = asyncio.Queue()

    from unittest.mock import MagicMock

    # Build a minimal fake ExecuteRequest
    req = MagicMock()
    req.rune_name = "sync_echo"
    req.request_id = "r-sync-1"
    req.input = b"hello"
    req.context = {}
    req.attachments = []

    registered = c._runes["sync_echo"]

    # This should not crash with a raw TypeError
    await c._execute_once(registered, ctx, req, outbound)

    # Should get a completed result in the queue
    msg = await outbound.get()
    # Result should be STATUS_COMPLETED (value 2) or the output should match
    assert msg.result.output == b"HELLO"


@pytest.mark.asyncio
async def test_mf5_sync_stream_handler_gives_clear_error():
    """MF-5b: A non-async stream handler also works or gives clear error."""
    c = Caster("localhost:50070", caster_id="test")

    @c.stream_rune("sync_stream")
    def sync_stream(ctx, input, stream):
        # This is a sync function, should be handled gracefully
        pass

    ctx = RuneContext(rune_name="sync_stream", request_id="r-1")
    outbound: asyncio.Queue = asyncio.Queue()

    from unittest.mock import MagicMock

    req = MagicMock()
    req.rune_name = "sync_stream"
    req.request_id = "r-sync-stream-1"
    req.input = b""
    req.context = {}
    req.attachments = []

    registered = c._runes["sync_stream"]
    await c._execute_stream(registered, ctx, req, outbound)

    # Should get stream_end in the queue, not a TypeError
    msg = await outbound.get()
    assert msg.stream_end.request_id == "r-sync-stream-1"


# ============================================================
# NF-8: _cancelled set should be cleaned up in finally block
# ============================================================


@pytest.mark.asyncio
async def test_nf8_cancelled_set_cleaned_after_execute():
    """NF-8: request_id should be discarded from _cancelled in finally block
    of _handle_execute, preventing unbounded growth."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("slow")
    async def slow_handler(ctx, input):
        return input

    outbound: asyncio.Queue = asyncio.Queue()

    from unittest.mock import MagicMock

    req = MagicMock()
    req.rune_name = "slow"
    req.request_id = "r-cancel-test"
    req.input = b"data"
    req.context = {}
    req.attachments = []

    # Simulate a cancel happening before execution
    c._cancelled.add("r-cancel-test")
    assert "r-cancel-test" in c._cancelled

    await c._handle_execute(req, outbound)

    # After _handle_execute finishes, request_id should be cleaned up
    assert "r-cancel-test" not in c._cancelled


@pytest.mark.asyncio
async def test_nf8_cancelled_set_cleaned_even_on_error():
    """NF-8b: _cancelled cleanup also happens when handler raises."""
    c = Caster("localhost:50070", caster_id="test")

    @c.rune("failing")
    async def failing_handler(ctx, input):
        raise RuntimeError("intentional failure")

    outbound: asyncio.Queue = asyncio.Queue()

    from unittest.mock import MagicMock

    req = MagicMock()
    req.rune_name = "failing"
    req.request_id = "r-fail-cancel"
    req.input = b"data"
    req.context = {}
    req.attachments = []

    c._cancelled.add("r-fail-cancel")
    await c._handle_execute(req, outbound)

    # Should be cleaned up even though handler failed
    assert "r-fail-cancel" not in c._cancelled
