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

from rune_sdk import Caster, RuneContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

caster = Caster("localhost:50070", caster_id="example-python-caster", max_concurrent=10)


@caster.rune("echo", version="1.0.0", description="Echo rune", gate="/echo")
async def echo(ctx: RuneContext, input: bytes) -> bytes:
    """Echo: return input as-is."""
    return input


@caster.rune("slow", version="1.0.0", description="Slow rune (3s delay)", gate="/slow")
async def slow(ctx: RuneContext, input: bytes) -> bytes:
    """Slow: simulate a 3-second task."""
    await asyncio.sleep(3)
    data = json.loads(input)
    data["slow"] = True
    return json.dumps(data).encode()


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
    print("Registered runes: echo, slow, step_a, step_c, streamer")
    caster.run()
