# Local Hello Example

Minimal example showing how to register local Runes using the App builder API.

## Features Demonstrated

- Basic Rune registration with `RuneConfig`
- Gate endpoint configuration (`/hello`, `/echo`)
- JSON Schema validation (`input_schema` / `output_schema`)
- Priority-based routing (`priority`)
- Label metadata (`labels`)

## Code

See `main.rs` for the complete example.

## Note

This example registers Runes in-process and does **not** start a server.
For a full server setup, see `rune-server`.
