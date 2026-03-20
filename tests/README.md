# Rune v0.1.0 Test Catalog

## Unit Tests (cargo test)

### rune-core (9 tests)
- **relay**: register_and_resolve, resolve_not_found, round_robin, remove_caster
- **session**: execute_returns_permit_on_normal_completion, late_result_does_not_inflate_permits, cancel_returns_permit, max_concurrent_enforced, disconnect_clears_all_pending

### rune-flow (8 tests)
- **dsl**: chain_build, step_build, empty_flow
- **engine**: execute_chain, step_fail, empty_chain, flow_not_found, rune_not_found_in_step

## Python SDK Tests (pytest)
- test_rune_config_defaults, test_rune_context
- test_rune_decorator_registers, test_stream_rune_decorator_registers, test_multiple_runes, test_handler_invocation

## Regression Tests

These tests specifically lock down bugs found during POC review. If any of these fail, it indicates a correctness regression.

| Test | Bug It Locks |
|------|-------------|
| `session::test_late_result_does_not_inflate_permits` | Permit double-return on late Result/StreamEnd |
| `session::test_max_concurrent_enforced` | max_concurrent must be strict |
| *(planned)* gate_path_generates_real_route | gate_path was display-only, not a real route |
| *(planned)* stream_is_real_not_fake | stream=true was chunking complete results |
| *(planned)* heartbeat_timeout_triggers_cleanup | heartbeat timeout was warn-only |
| *(planned)* proto_fields_used_by_runtime | declared proto fields were ignored |

## Integration / E2E Tests (planned)

These require a running server + caster and will be added in a later iteration:
- Python unary rune attach + invoke
- Python stream rune attach + invoke
- Mixed flow (local + remote)
- Task cancel via DELETE
- SSE disconnect triggers cancel

## Running Tests

```bash
# Rust tests
cargo test

# Python SDK tests
cd sdks/python && .venv/bin/pytest tests/ -v
```
