# Mixed Flow Example

Demonstrates a sequential flow pipeline with mixed local (Rust) and remote (Python) steps.

## Pipeline

```
step_a (Python) → step_b (Rust, local) → step_c (Python)
```

Each step adds a field to the JSON input.

## How to Run

### 1. Start the Rune server

```bash
cargo run -p rune-server
```

The server registers `step_b` as a local Rust Rune and starts:
- gRPC on `localhost:50070`
- HTTP on `localhost:50060`

### 2. Start the Python Caster

```bash
cd sdks/python && pip install -e .
python examples/python-caster/main.py
```

This registers `step_a` and `step_c` as remote Python Runes.

### 3. Execute the flow

Sync:
```bash
curl -X POST http://localhost:50060/api/v1/flows/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"input": "test"}'
```

Expected: `{"output": {"input": "test", "step_a": true, "step_b": true, "step_c": true}, "steps_executed": 3}`

Async:
```bash
curl -X POST "http://localhost:50060/api/v1/flows/pipeline/run?async=true" \
  -H "Content-Type: application/json" \
  -d '{"input": "test"}'
```

Returns: `{"task_id": "...", "status": "running"}`

Poll result:
```bash
curl http://localhost:50060/api/v1/tasks/{task_id}
```
