# Repository Guidelines

## Project Structure & Module Organization
`runtime/` contains the Rust workspace crates: `rune-core` (core runtime logic), `rune-gate` (HTTP gateway), `rune-flow` (DAG engine), `rune-store` (SQLite persistence), `rune-schema`, `rune-cli`, `rune-server`, and `rune-proto`. Shared protocol definitions live in `proto/rune/wire/v1/`. SDKs are under `sdks/python`, `sdks/typescript`, and `sdks/rust`. Integration examples live in `examples/`, and longer-form design and API docs live in `docs/`.

## Build, Test, and Development Commands
Use the workspace root for Rust commands:

- `cargo build` builds all Rust crates.
- `cargo test --all-features` runs the CI Rust test suite.
- `cargo test -p rune-core` runs a single crate while iterating.
- `cargo run -p rune-server -- --dev` starts the runtime in local dev mode.
- `cargo fmt --all -- --check` verifies formatting.
- `cargo clippy --all-targets --all-features -- -D warnings` enforces lint cleanliness.
- `bash scripts/sync-proto.sh` regenerates SDK protocol artifacts after changing `proto/`.
- `cd sdks/typescript && npm test` runs Vitest tests; `npm run build` compiles the package.
- `cd sdks/python && pip install -e ".[dev]" && pytest tests/ -v` runs Python SDK tests.

## Coding Style & Naming Conventions
Rust uses edition 2021, 4-space indentation, `snake_case` module/file names, and descriptive test names such as `start_default` or `config_test.rs`. Keep warnings at zero: CI and the pre-commit flow expect `cargo fmt` and `cargo clippy -D warnings` to pass. TypeScript uses ES modules and `camelCase` APIs with tests in `*.test.ts`. Python targets 3.10+, follows `snake_case`, and keeps tests in `test_*.py`.

## Testing Guidelines
Place Rust unit and integration tests beside each crate in `runtime/*/tests`. Prefer focused crate-level runs before `cargo test --all-features`. Python E2E tests are marked with `@pytest.mark.e2e` and require a running `rune-server`. If you touch the wire protocol, add or update tests and commit regenerated files in `sdks/typescript/proto/` and `sdks/python/src/rune/_proto/`.

## Commit & Pull Request Guidelines
Branch from `dev`, not `main`. Follow Conventional Commits already used in history, for example `feat: implement v1.2.0 Module A` or `fix: resolve all clippy warnings`. PRs also target `dev` and should fill in the repo template: `What`, `Why`, `How`, and a concrete `Test Plan`. Note any breaking changes explicitly and ensure CI passes before requesting review.
