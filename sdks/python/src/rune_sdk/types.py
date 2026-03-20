"""Core types for the Rune Python SDK."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RuneConfig:
    """Configuration for a Rune registration."""
    name: str
    version: str = "0.0.0"
    description: str = ""
    supports_stream: bool = False
    gate: str | None = None  # gate path, e.g. "/translate"
    gate_method: str = "POST"


@dataclass
class RuneContext:
    """Execution context passed to handler."""
    rune_name: str
    request_id: str
    context: dict[str, str] = field(default_factory=dict)
