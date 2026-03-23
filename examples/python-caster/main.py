#!/usr/bin/env python3
"""
Example Python Caster — registers runes and connects to Rune runtime.

Usage:
    1. Start the Rune server: cargo run -p rune-server
    2. Run this caster: python examples/python-caster/main.py

Requires: pip install rune-sdk (from sdks/python/)
"""
import asyncio
import json
import logging
import time

from rune_sdk import Caster, RuneContext, FileAttachment

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

caster = Caster("localhost:50070", caster_id="example-python-caster", max_concurrent=10)


# ---------------------------------------------------------------------------
# 1. Basic echo rune (unary)
# ---------------------------------------------------------------------------

@caster.rune("echo", version="1.0.0", description="Echo rune", gate="/echo")
async def echo(ctx: RuneContext, input: bytes) -> bytes:
    """Echo: return input as-is."""
    return input


# ---------------------------------------------------------------------------
# 2. Slow rune — simulated long-running task
# ---------------------------------------------------------------------------

@caster.rune("slow", version="1.0.0", description="Slow rune (3s delay)", gate="/slow")
async def slow(ctx: RuneContext, input: bytes) -> bytes:
    """Slow: simulate a 3-second task."""
    await asyncio.sleep(3)
    data = json.loads(input)
    data["slow"] = True
    return json.dumps(data).encode()


# ---------------------------------------------------------------------------
# 3. Rune with input_schema — demonstrates schema declaration
# ---------------------------------------------------------------------------

@caster.rune(
    "translate",
    version="1.0.0",
    description="Translate text (mock)",
    gate="/translate",
    input_schema={
        "type": "object",
        "properties": {
            "text": {"type": "string", "minLength": 1},
            "target_lang": {"type": "string", "enum": ["en", "zh", "ja", "ko"]},
        },
        "required": ["text", "target_lang"],
    },
    output_schema={
        "type": "object",
        "properties": {
            "translated": {"type": "string"},
            "source_lang": {"type": "string"},
        },
    },
    priority=5,
)
async def translate(ctx: RuneContext, input: bytes) -> bytes:
    """Translate: mock translation that returns the input text reversed."""
    data = json.loads(input)
    result = {
        "translated": data["text"][::-1],  # mock: reverse the text
        "source_lang": "auto",
    }
    return json.dumps(result).encode()


# ---------------------------------------------------------------------------
# 4. Rune that accepts file attachments
# ---------------------------------------------------------------------------

@caster.rune(
    "file_info",
    version="1.0.0",
    description="Return metadata about uploaded files",
    gate="/file-info",
)
async def file_info(ctx: RuneContext, input: bytes, files: list[FileAttachment]) -> bytes:
    """File info: return filename, size, and MIME type for each uploaded file."""
    info_list = []
    for f in files:
        info_list.append({
            "filename": f.filename,
            "size": len(f.data),
            "mime_type": f.mime_type,
        })
    return json.dumps({"files": info_list, "count": len(info_list)}).encode()


# ---------------------------------------------------------------------------
# 5. Flow steps (used by mixed-flow example)
# ---------------------------------------------------------------------------

@caster.rune("step_a", version="1.0.0", description="Flow step A")
async def step_a(ctx: RuneContext, input: bytes) -> bytes:
    """Step A: add step_a field to JSON."""
    data = json.loads(input)
    data["step_a"] = True
    return json.dumps(data).encode()


@caster.rune("step_c", version="1.0.0", description="Flow step C")
async def step_c(ctx: RuneContext, input: bytes) -> bytes:
    """Step C: add step_c field to JSON."""
    data = json.loads(input)
    data["step_c"] = True
    return json.dumps(data).encode()


# ---------------------------------------------------------------------------
# 6. Streaming rune — emits chunks with delay
# ---------------------------------------------------------------------------

@caster.stream_rune("streamer", version="1.0.0", description="Stream rune", gate="/stream")
async def streamer(ctx: RuneContext, input: bytes, stream):
    """Streamer: sends 5 chunks with 200ms delay."""
    data = json.loads(input)
    for i in range(5):
        chunk = json.dumps({"chunk": i, "source": data.get("source", "unknown")}).encode()
        await stream.emit(chunk)
        await asyncio.sleep(0.2)


if __name__ == "__main__":
    print("Starting Python Caster — connecting to localhost:50070")
    print("Registered runes: echo, slow, translate, file_info, step_a, step_c, streamer")
    caster.run()
