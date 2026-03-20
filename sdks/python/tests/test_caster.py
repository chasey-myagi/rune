"""Tests for the Caster class (registration only, no gRPC)."""
import pytest

from rune_sdk import Caster
from rune_sdk.types import RuneContext


def test_rune_decorator_registers():
    caster = Caster("localhost:50070", caster_id="test")

    @caster.rune("echo", version="1.0.0", gate="/echo")
    async def echo(ctx: RuneContext, input: bytes) -> bytes:
        return input

    assert "echo" in caster._runes
    assert caster._runes["echo"].config.name == "echo"
    assert caster._runes["echo"].config.gate == "/echo"
    assert caster._runes["echo"].is_stream is False


def test_stream_rune_decorator_registers():
    caster = Caster("localhost:50070")

    @caster.stream_rune("chat", version="1.0.0", gate="/chat")
    async def chat(ctx, input, stream):
        await stream.emit(b"hello")
        await stream.end()

    assert "chat" in caster._runes
    assert caster._runes["chat"].config.supports_stream is True
    assert caster._runes["chat"].is_stream is True


def test_multiple_runes():
    caster = Caster("localhost:50070")

    @caster.rune("a")
    async def a(ctx, input):
        return input

    @caster.rune("b")
    async def b(ctx, input):
        return input

    @caster.stream_rune("c")
    async def c(ctx, input, stream):
        pass

    assert len(caster._runes) == 3


@pytest.mark.asyncio
async def test_handler_invocation():
    """Test that registered handler can be called directly."""
    caster = Caster("localhost:50070")

    @caster.rune("echo")
    async def echo(ctx: RuneContext, input: bytes) -> bytes:
        return input.upper()

    ctx = RuneContext(rune_name="echo", request_id="r-1")
    result = await caster._runes["echo"].handler(ctx, b"hello")
    assert result == b"HELLO"
