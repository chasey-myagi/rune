# Rune

**Protocol-first polyglot function execution framework. Define functions, get routing, streaming, async, orchestration — all out of the box.**

## Features

- **Three invocation modes** — sync / stream (SSE) / async — one function, three calling patterns automatically
- **DAG workflow orchestration** — directed acyclic graph engine with conditional branching, parallel execution, and input mapping
- **Polyglot SDKs** — Python / TypeScript / Rust Caster SDKs, unified Wire Protocol
- **Schema validation + OpenAPI generation** — JSON Schema input/output validation with auto-generated OpenAPI 3.0 docs
- **File transfer** — multipart upload + in-memory FileBroker relay + download endpoints
- **API key auth** — Gate Key (HTTP) + Caster Key (gRPC), two-layer isolation
- **Retry & circuit breaker** — automatic retry for remote calls + per-Caster circuit breaker with exponential backoff and configurable thresholds
- **Request tracing** — unified `request_id` across the full call chain, W3C Trace Context propagation
- **Rate limiting** — global + per-Rune sliding window rate limits, Caster concurrency caps
- **Caster health checks** — protocol-level HealthReport + pressure-aware scheduling
- **Advanced scheduling** — round-robin / random / least-load / priority / label-based — five strategies
- **Auto scaling** — Pilot daemon + pressure-aware scheduling + multi-language SDK scaling policies
- **CLI** — single binary to manage the Runtime, invoke Runes, manage Keys, and run Flows
- **SQLite persistence** — async tasks, invocation logs, API keys all persisted with read/write connection pool separation

## Install

### CLI (recommended)

```bash
# macOS / Linux (Homebrew)
brew install chasey-myagi/tap/rune

# Or download the binary for your platform directly from the release page
# https://github.com/chasey-myagi/rune/releases/latest
```

After installation you get two binaries:
- `rune` — CLI tool (manage Runtime + invoke Runes)
- `rune-server` — Runtime server

### SDK

```bash
pip install rune-framework            # Python
npm install @rune-framework/caster    # TypeScript
```

## Quick Start

```bash
# 1. Start the Runtime (auto-pulls the Docker image)
rune start --dev

# 2. Run your Caster (in another terminal)
python my_caster.py

# 3. Invoke
rune call echo '{"msg": "hello"}'

# 4. Check status
rune status
rune list

# 5. Stop
rune stop
```

### Manual Docker Start (without CLI)

```bash
docker run -d -p 50060:50060 -p 50070:50070 ghcr.io/chasey-myagi/rune-server:latest
curl http://localhost:50060/health  # => ok
```

docker-compose:

```yaml
services:
  rune:
    image: ghcr.io/chasey-myagi/rune-server:latest
    ports:
      - "50060:50060"  # HTTP API
      - "50070:50070"  # gRPC (Caster connections)
    environment:
      RUNE_LOG__LEVEL: info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:50060/health"]
      interval: 10s
```

**Environment Variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `RUNE_SERVER__HTTP_HOST` | `0.0.0.0` | HTTP listen address |
| `RUNE_SERVER__HTTP_PORT` | `50060` | HTTP API port |
| `RUNE_SERVER__GRPC_HOST` | `0.0.0.0` | gRPC listen address |
| `RUNE_SERVER__GRPC_PORT` | `50070` | gRPC port |
| `RUNE_LOG__LEVEL` | `info` | Log level |

### Python Caster Example

```bash
pip install rune-framework
```

```python
from rune import Caster

caster = Caster("localhost:50070")

@caster.rune("translate", gate="/translate", input_schema={
    "type": "object",
    "properties": {"text": {"type": "string"}, "lang": {"type": "string"}},
    "required": ["text", "lang"]
})
async def translate(ctx, input):
    data = json.loads(input)
    return json.dumps({"translated": do_translate(data["text"], data["lang"])})

@caster.stream_rune("generate", gate="/generate")
async def generate(ctx, input, sender):
    for token in model.stream(input):
        await sender.emit(token)

caster.run()
```

### TypeScript Caster Example

```typescript
import { Caster } from '@rune-framework/caster';

const caster = new Caster({ key: 'rk_xxx' });

caster.rune({ name: 'greet', gate: { path: '/greet' } }, async (ctx, input) => {
  return { message: `Hello, ${input.name}!` };
});

caster.streamRune({ name: 'count', gate: { path: '/count' } }, async (ctx, input, stream) => {
  for (let i = 0; i < 10; i++) {
    stream.emit(Buffer.from(String(i)));
  }
});

await caster.run();
```

### CLI Invocation

```bash
# List online Runes
rune list

# Sync call
rune call translate '{"text": "hello", "lang": "zh"}'

# Stream call
rune call generate '{"prompt": "write a poem"}' --stream

# Async call
rune call translate '{"text": "hello", "lang": "zh"}' --async
# Query async task
rune task get <task-id>
```

## Architecture

```
Client ---> HTTP API ---> Gate ---> Relay ---> Invoker ---> Caster
                           |                                  |
                      Auth + Rate Limit              gRPC (Wire Protocol)
                      Schema Validation
                      File Broker                 Python / TypeScript / Rust
```

```
Runtime (Rust)
├── rune-core    — Core abstractions (Relay, Resolver, Session, Auth)
├── rune-gate    — HTTP gateway (Axum, routing, middleware)
├── rune-flow    — DAG workflow engine
├── rune-schema  — JSON Schema validation + OpenAPI generation
├── rune-store   — SQLite persistence (tasks, logs, keys)
├── rune-proto   — gRPC protobuf definitions
├── rune-cli     — CLI tool
└── rune-server  — Entry binary
```

## SDK

### Python (`rune-framework`)

```bash
pip install rune-framework
```

- `Caster` — connect to Runtime, register handlers
- `@caster.rune()` — register a unary handler (supports schema, gate, priority, files)
- `@caster.stream_rune()` — register a streaming handler
- `StreamSender` — streaming emitter (supports bytes/str/dict/list)
- Auto-reconnect with exponential backoff
- Full protocol participation (attach, heartbeat, execute, cancel, reconnect)

### TypeScript (`@rune-framework/caster`)

```bash
npm install @rune-framework/caster
```

- `Caster` — connect to Runtime, register handlers
- `caster.rune()` — register a unary handler (supports schema, gate, priority, files)
- `caster.streamRune()` — register a streaming handler
- `StreamSender` — streaming emitter
- `AbortSignal` cancellation awareness
- Auto-reconnect with exponential backoff

### Rust (`rune-framework`)

```toml
[dependencies]
rune-framework = { path = "sdks/rust" }
```

- `Caster` — connect to Runtime, register handlers
- `caster.rune()` / `caster.rune_with_files()` — register a unary handler
- `caster.stream_rune()` / `caster.stream_rune_with_files()` — register a streaming handler
- `StreamSender` — streaming emitter
- `CancellationToken` cancellation awareness
- Auto-reconnect with exponential backoff

## CLI

```bash
# Runtime lifecycle
rune start [--dev] [--binary <path>]    # Start Runtime (Docker-first, or --binary for local binary)
rune stop [--force] [--timeout 10]      # Stop Runtime (graceful → SIGKILL)
rune status                             # View Runtime status
rune pilot daemon --runtime <addr>      # Start local Pilot daemon
rune pilot status                       # View local Pilot status
rune pilot stop                         # Stop local Pilot

# Rune invocation
rune list                               # List online Runes
rune call <name> [input] [--stream|--async] [--timeout 30]  # Invoke a Rune
rune casters                            # View connected Casters

# Async tasks
rune task get <id>                      # View task status and result
rune task list [--status <s>] [--rune <name>]  # List tasks
rune task wait <id> [--timeout 300]     # Wait for task completion (backoff polling)
rune task delete <id>                   # Cancel/delete task

# Key management
rune key create --type <gate|caster> --label <label>
rune key list
rune key revoke <id>

# Flow workflows
rune flow register <file.yaml>          # Register a Flow (.yaml/.yml/.json)
rune flow list
rune flow get <name>                    # View Flow details
rune flow run <name> [input]
rune flow delete <name>

# Operations
rune logs [--rune <name>] [--limit 20]  # View invocation logs
rune stats                              # View invocation statistics
rune config init                        # Generate default config ~/.rune/config.toml
rune config show                        # Show current config
rune config path                        # Print config file path

# Global options
--remote <URL>    # Connect to remote Runtime (or RUNE_ADDR env var)
--json            # JSON output (machine-readable)
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/runes` | List online Runes |
| POST | `/api/v1/runes/{name}/run` | Invoke a Rune (debug endpoint) |
| GET | `/api/v1/tasks` | List async tasks |
| GET | `/api/v1/tasks/{id}` | Query async task |
| DELETE | `/api/v1/tasks/{id}` | Cancel async task |
| GET | `/api/v1/status` | Runtime status |
| GET | `/api/v1/casters` | Online Caster list |
| GET | `/api/v1/stats` | Invocation statistics |
| GET | `/api/v1/stats/casters` | Per-Caster aggregated statistics |
| GET | `/api/v1/scaling/status` | Auto-scaling status |
| GET | `/api/v1/logs` | Invocation logs |
| POST | `/api/v1/keys` | Create API key |
| GET | `/api/v1/keys` | List API keys |
| DELETE | `/api/v1/keys/{id}` | Revoke API key |
| GET | `/api/v1/openapi.json` | OpenAPI 3.0 spec |
| GET | `/api/v1/files/{id}` | Download file |
| POST | `/api/v1/flows` | Create Flow |
| GET | `/api/v1/flows` | List Flows |
| GET | `/api/v1/flows/{name}` | Get Flow details |
| DELETE | `/api/v1/flows/{name}` | Delete Flow |
| POST | `/api/v1/flows/{name}/run` | Execute Flow |
| * | `/{gate_path}` | Dynamic business routes (declared via Rune's gate.path) |

Full API reference: [docs/api-reference.md](docs/api-reference.md).

## Configuration

The Runtime is configured via `rune.toml`, with environment variable overrides supported.

```toml
[server]
http_port = 50060
grpc_port = 50070
dev_mode = false
drain_timeout_secs = 15

[auth]
enabled = true
exempt_routes = ["/health"]

[store]
db_path = "rune.db"
log_retention_days = 30

[session]
heartbeat_interval_secs = 10
heartbeat_timeout_secs = 35
max_request_timeout_secs = 30

[gate]
cors_origins = []
max_upload_size_mb = 10

[resolver]
strategy = "round_robin"  # round_robin | random | least_load | priority

[scaling]
enabled = false
eval_interval_secs = 30

[rate_limit]
requests_per_minute = 600

[log]
level = "info"
```

Full configuration reference: [docs/configuration.md](docs/configuration.md).

### Auto Scaling

- Enable the auto-scaling evaluation loop via `[scaling]` in the Runtime config.
- Run `rune pilot daemon --runtime localhost:50070` to start a single Pilot process shared by Python / TypeScript / Rust SDKs.
- `GET /api/v1/casters` now returns `role`, `max_concurrent`, `available_permits`, `pressure`, `metrics`, `health_status`.
- `GET /api/v1/stats/casters` and `GET /api/v1/scaling/status` provide per-Caster aggregated stats and group scaling status.

## Protocol Guarantees

Rune defines 18 behavioral guarantee contracts. Any violation is a bug. Covering:

- **Execution semantics** (1-3) — unified invocation, three modes, true streaming
- **State convergence** (4-7) — full state cleanup after timeout/cancel/disconnect
- **Backpressure** (8) — strict `max_concurrent` semaphore
- **Routing** (9-11) — real business routes, no implicit exposure, route conflicts are hard errors
- **Flow** (12) — Flows use the same Invoker
- **SDK** (13) — full protocol participation
- **Schema** (14) — validate when schema exists, pass through when it doesn't
- **Files** (15) — multipart with size limits
- **Label routing** (16) — label mismatch returns 503
- **Flow isolation** (17) — single-step failure does not pollute completed results of other branches
- **Graceful shutdown** (18) — reject new requests during drain, wait for in-flight requests to complete

Full list: [docs/protocol-guarantees.md](docs/protocol-guarantees.md).

## Roadmap

### POC Phase (demo/ archived in git history)

Core Runtime, polyglot SDKs, three invocation modes, DAG Flow, Schema validation, auth, persistence, advanced scheduling.

### Releases

| Version | Status | Key Deliverables |
|---------|--------|------------------|
| **v0.2.0** | **Released** | CLI overhaul (Docker-first Runtime management, dual-mode output, full task commands), cross-platform releases (GitHub Releases + Homebrew), demo/ cleanup |
| v0.3.0 | Planned | CLI text mode completion (casters/key/flow/logs table output), `--watch`/`--follow` live mode, config.toml enforcement |
| v1.0.0 | Planned | Stable release |

## Documentation

| Document | Description |
|----------|-------------|
| [Protocol Guarantees](docs/protocol-guarantees.md) | 18 behavioral guarantee contracts |
| [API Reference](docs/api-reference.md) | Full HTTP endpoint reference |
| [Configuration](docs/configuration.md) | rune.toml configuration reference |
| [CLI](docs/cli.md) | CLI command reference |
| [Python SDK](sdks/python/README.md) | Python Caster SDK |
| [TypeScript SDK](sdks/typescript/README.md) | TypeScript Caster SDK |
| [Rust SDK](sdks/rust/README.md) | Rust Caster SDK |

## License

Private project.
