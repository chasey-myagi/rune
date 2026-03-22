"""Handler registry with decorator support."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Awaitable

from .types import RuneConfig, RuneContext, FileAttachment
from .stream import StreamSender


# Handler function signatures
OnceHandler = Callable[[RuneContext, bytes], Awaitable[bytes]]
StreamHandler = Callable  # async def(ctx, input, stream) -> None

# Extended handler signatures with file attachments
OnceHandlerWithFiles = Callable[[RuneContext, bytes, list[FileAttachment]], Awaitable[bytes]]
StreamHandlerWithFiles = Callable[[RuneContext, bytes, list[FileAttachment], StreamSender], Awaitable[None]]


@dataclass
class RegisteredRune:
    """A registered rune with its config and handler."""

    config: RuneConfig
    handler: OnceHandler | StreamHandler | OnceHandlerWithFiles | StreamHandlerWithFiles
    is_stream: bool
    accepts_files: bool = False
