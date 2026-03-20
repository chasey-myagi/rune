"""Handler registry with decorator support."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Awaitable

from .types import RuneConfig, RuneContext


# Handler function signatures
OnceHandler = Callable[[RuneContext, bytes], Awaitable[bytes]]
StreamHandler = Callable  # async def(ctx, input, stream) -> None


@dataclass
class RegisteredRune:
    """A registered rune with its config and handler."""

    config: RuneConfig
    handler: OnceHandler | StreamHandler
    is_stream: bool
