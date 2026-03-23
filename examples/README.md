# Rune Examples

End-to-end examples demonstrating the Rune framework's capabilities.

## Overview

| Example | Description | Key Features |
|---------|-------------|--------------|
| [local-hello](local-hello/) | Minimal local Rune registration | App builder, RuneConfig, gate, schema, priority, labels |
| [python-caster](python-caster/) | Python SDK caster | Unary, stream, schema validation, file attachments |
| [ts-caster](ts-caster/) | TypeScript SDK caster | Unary, stream, schema declaration, labels |
| [rust-caster](rust-caster/) | Rust SDK caster | Unary, stream, schema, CasterConfig |
| [mixed-flow](mixed-flow/) | Rust + Python mixed pipeline | Cross-language flow, sequential steps |
| [dag-flow](dag-flow/) | DAG workflow orchestration | Parallel execution, fan-in, input_mapping |
| [file-upload](file-upload/) | File upload & download | Multipart, file broker, download API |

## Quick Start

Most examples require the Rune server running:

```bash
cargo run -p rune-server
```

Then start a caster in a separate terminal:

```bash
# Python
cd sdks/python && pip install -e .
python examples/python-caster/main.py

# TypeScript
cd examples/ts-caster && npm install && npx tsx main.ts

# Rust
cargo run -p rust-caster-example
```

## Architecture

```
┌─────────────┐     gRPC     ┌──────────────┐     HTTP     ┌────────┐
│ Caster(s)   │◄────────────►│ Rune Server  │◄────────────►│ Client │
│ Python/TS/  │   Session    │ (runtime)    │  Gate API    │ (curl) │
│ Rust/Go     │              └──────────────┘              └────────┘
└─────────────┘
```

- **Caster** — Registers Rune handlers, receives execution requests via gRPC
- **Server** — Manages sessions, routes requests, executes flows
- **Gate** — HTTP API layer with auth, schema validation, file handling
