# DAG Flow Example

Demonstrates DAG (Directed Acyclic Graph) workflow orchestration with parallel execution and result merging.

## Pipeline Structure

```
extract ──┬── analyze   ──┬── merge
          └── translate ──┘
```

| Layer | Steps              | Description               |
|-------|--------------------|---------------------------|
| 1     | extract            | Extract raw data          |
| 2     | analyze, translate | Run in parallel           |
| 3     | merge              | Fan-in: merge both results|

The `merge` step depends on both `analyze` and `translate`, and uses `input_mapping` to receive outputs from both upstream steps.

## Flow Definition

See `pipeline.json` — a standard Rune Flow JSON that can be registered via HTTP API or CLI.

Key fields per step:
- `name` — unique step identifier
- `rune` — name of the Rune to invoke
- `depends_on` — list of upstream step names (empty = root)
- `input_mapping` — required when a step has multiple upstreams; maps input keys to upstream step names

## How to Run

### Prerequisites

1. Start the Rune server:
   ```bash
   cargo run -p rune-server
   ```

2. Start casters that provide the `extract`, `analyze`, `translate`, and `merge` runes.

### Using the Script

```bash
./examples/dag-flow/run.sh
```

The script will:
1. Register the flow from `pipeline.json`
2. List all flows
3. Execute the flow synchronously
4. Execute the flow asynchronously and poll the task
5. Clean up (delete the flow)

### Using the CLI

```bash
# Register
rune flow register examples/dag-flow/pipeline.json

# List
rune flow list

# Run
rune flow run etl-pipeline '{"text": "hello", "target_lang": "zh"}'

# Delete
rune flow delete etl-pipeline
```

### Using the HTTP API directly

```bash
# Register flow
curl -X POST http://localhost:50060/api/v1/flows \
  -H "Content-Type: application/json" \
  -d @examples/dag-flow/pipeline.json

# Execute (sync)
curl -X POST http://localhost:50060/api/v1/flows/etl-pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"text": "hello world", "target_lang": "zh"}'

# Execute (async)
curl -X POST "http://localhost:50060/api/v1/flows/etl-pipeline/run?async=true" \
  -H "Content-Type: application/json" \
  -d '{"text": "hello world"}'

# Execute (SSE stream)
curl -N "http://localhost:50060/api/v1/flows/etl-pipeline/run?stream=true" \
  -H "Content-Type: application/json" \
  -d '{"text": "hello world"}'
```
