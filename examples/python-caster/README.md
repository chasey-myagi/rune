# Python Caster Example

Example Python Caster that registers runes and connects to a Rune runtime server.

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

Registered runes: echo, slow, step_a, step_c, streamer
