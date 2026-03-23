# Python Caster Example

Example Python Caster that registers runes and connects to a Rune runtime server.

## Features Demonstrated

- Basic unary rune (`echo`, `slow`)
- JSON Schema declaration (`translate` — `input_schema` + `output_schema`)
- File attachment handler (`file_info` — receives `files` parameter)
- Streaming rune (`streamer`)
- Priority-based routing (`translate` with `priority=5`)
- Flow step runes (`step_a`, `step_c` — used by mixed-flow example)

## Prerequisites

```bash
cd sdks/python && pip install -e .
```

## Usage

```bash
# Start the server first
cargo run -p rune-server

# Then run the caster
python examples/python-caster/main.py
```

## Testing

```bash
# Echo
curl -X POST http://localhost:50060/echo -d '{"text":"hello"}'

# Translate (with schema validation)
curl -X POST http://localhost:50060/translate \
  -H "Content-Type: application/json" \
  -d '{"text":"hello world","target_lang":"zh"}'

# File info (multipart upload)
curl -X POST http://localhost:50060/file-info \
  -F 'input={"tag":"test"};type=application/json' \
  -F 'file=@README.md;type=text/plain'

# Stream
curl -N http://localhost:50060/api/v1/runes/streamer/run?stream=true \
  -H "Content-Type: application/json" \
  -d '{"source":"demo"}'
```

Registered runes: echo, slow, translate, file_info, step_a, step_c, streamer
