"""StreamSender for streaming Rune handlers."""
from __future__ import annotations


class StreamSender:
    """Sends stream events to the runtime."""

    def __init__(self, send_fn):
        self._send = send_fn
        self._ended = False

    async def emit(self, data: bytes) -> None:
        """Send a stream event."""
        if self._ended:
            raise RuntimeError("stream already ended")
        await self._send(data)

    async def end(self) -> None:
        """Signal end of stream."""
        self._ended = True
