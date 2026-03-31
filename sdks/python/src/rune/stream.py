"""StreamSender for streaming Rune handlers."""
from __future__ import annotations

import json


class StreamSender:
    """Sends stream events to the runtime."""

    def __init__(self, send_fn):
        self._send = send_fn
        self._ended = False

    async def emit(self, data: bytes | str | dict | list) -> None:
        """Send a stream event.

        Accepts bytes, str (auto-encoded to UTF-8), or dict/list (auto-serialized to JSON bytes).
        """
        if self._ended:
            raise RuntimeError("stream already ended")
        if isinstance(data, bytes):
            raw = data
        elif isinstance(data, str):
            raw = data.encode("utf-8")
        elif isinstance(data, (dict, list)):
            raw = json.dumps(data).encode("utf-8")
        else:
            raw = str(data).encode("utf-8")
        await self._send(raw)

    async def end(self) -> None:
        """Signal end of stream."""
        self._ended = True
