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
    input_schema: dict | None = None   # JSON Schema for input validation
    output_schema: dict | None = None  # JSON Schema for output validation
    priority: int = 0                  # Caster priority (higher wins)


@dataclass
class FileAttachment:
    """File attachment for ExecuteRequest / ExecuteResult."""
    filename: str
    data: bytes
    mime_type: str


@dataclass
class ScalePolicy:
    """Auto-scaling metadata announced to Pilot and Runtime."""
    group: str
    spawn_command: str
    scale_up_threshold: float = 0.8
    scale_down_threshold: float = 0.2
    sustained_secs: int = 30
    min_replicas: int = 1
    max_replicas: int = 1
    shutdown_signal: str = "SIGTERM"


@dataclass
class LoadReport:
    """Static load telemetry sent with health updates."""
    pressure: float = 0.0
    metrics: dict[str, float] = field(default_factory=dict)


@dataclass
class RuneContext:
    """Execution context passed to handler."""
    rune_name: str
    request_id: str
    context: dict[str, str] = field(default_factory=dict)

    @property
    def trace_id(self) -> str | None:
        return self.context.get("trace_id")

    @property
    def parent_request_id(self) -> str | None:
        return self.context.get("parent_request_id")
