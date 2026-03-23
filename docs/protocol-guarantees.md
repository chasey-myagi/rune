# Rune Protocol Guarantees

This document defines the behavioral guarantees that Rune provides. These are contracts -- if any of these are violated, it's a bug.

## Execution Semantics

1. **Unified invocation**: Local Rune and Remote Rune have identical calling semantics. Callers (Gate, Flow) cannot distinguish them.

2. **Three modes, one Rune**: sync, stream, and async are three modes of the same Rune registration. They share the same config, error semantics, and routing.

3. **Real streaming**: The first SSE event from a `?stream=true` request comes from actual handler execution, not from post-completion chunking. If the handler takes 5 seconds to produce its first output, the first SSE event arrives after 5 seconds -- not after the full execution completes.

## State Convergence

4. **Timeout convergence**: After a request times out, the runtime state (pending map, semaphore permits) is fully cleaned up. The caller receives a Timeout error.

5. **Cancel convergence**: After a request is cancelled, the runtime state is fully cleaned up. The caller receives a Cancelled error.

6. **Disconnect convergence**: When a Caster disconnects (graceful or crash), all pending requests for that Caster receive an Internal error. The Caster's registrations are removed from the Relay.

7. **No state pollution**: Late-arriving Result or StreamEnd messages (after timeout/cancel/disconnect has already cleaned up the request) are silently ignored. They do not return phantom permits or corrupt runtime state.

## Backpressure

8. **Strict max_concurrent**: The `max_concurrent` declared by a Caster is enforced via Semaphore. If all permits are consumed, new requests to that Caster return Unavailable. Permit count never exceeds the declared maximum, regardless of error paths.

## Routing

9. **Real business routes**: A Rune declaring `gate.path=/translate` is accessible via `POST /translate`. This is a real HTTP route, not metadata.

10. **No implicit exposure**: A Rune without `gate.path` is NOT automatically exposed as a business route. It can only be called via the debug endpoint `/api/v1/runes/:name/run`.

11. **Route conflict is a hard error**: Two different rune names declaring the same `gate.path + method` causes a registration failure (AttachAck rejected or startup panic). Same rune name from multiple Casters shares the route via the configured resolver strategy.

## Flow

12. **Flow uses the same invoker**: Flow steps call Runes through the same RuneInvoker interface as direct requests. A Rune behaves identically whether called directly or as a flow step.

## SDK

13. **Full protocol participation**: All official SDKs (Python, TypeScript, Rust) implement the complete Rune Wire Protocol: attach, heartbeat, execute (once + stream), cancel awareness, and reconnect with exponential backoff.

## Schema Gate

14. **Schema gate**: When a Rune declares `input_schema` or `output_schema`, the Gate validates input before dispatching and output before returning. Validation failure returns `422 VALIDATION_FAILED` (input) or `500 OUTPUT_VALIDATION_FAILED` (output). When no schema is declared, the Gate does not intercept -- raw bytes pass through unchanged.

## Upload Gate

15. **Upload gate**: File uploads via multipart are subject to a configurable size limit (`gate.max_upload_size_mb`, default 10 MB). Uploads exceeding this limit are rejected with `400 BAD_REQUEST` before any handler execution occurs. Files are stored in the in-memory FileBroker and are downloadable via `/api/v1/files/:id`.

## Label Strictness

16. **Label strictness**: When a request includes `X-Rune-Labels` header, the Relay only considers Casters whose labels are a superset of the requested labels. If no matching Caster is found, the request returns `503 NO_MATCHING_CASTER` -- it does NOT fall back to an unlabeled Caster.

## Flow Failure Isolation

17. **Flow failure isolation**: In a DAG flow, when a step fails, only its downstream dependents are affected (they are not executed). Steps on independent branches that have already completed retain their results. The flow returns a `StepFailed` error with the failing step's name and error detail, but completed steps' outputs are preserved in the flow's step status map.

## Graceful Drain

18. **Graceful drain**: When the Runtime receives a shutdown signal, it enters drain mode. During drain: (a) `/health` returns `503` to signal load balancers; (b) new incoming HTTP requests are rejected; (c) in-flight requests are allowed to complete up to `server.drain_timeout_secs` (default 15s); (d) after the timeout, remaining requests are force-terminated and the process exits.
