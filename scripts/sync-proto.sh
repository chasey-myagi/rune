#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_SRC="$ROOT_DIR/proto/rune/wire/v1/rune.proto"
TS_PROTO="$ROOT_DIR/sdks/typescript/proto/rune.proto"
PY_OUT_DIR="$ROOT_DIR/sdks/python/src/rune/_proto"
TMP_VENV="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_VENV"
}
trap cleanup EXIT

cp "$PROTO_SRC" "$TS_PROTO"

python3 -m venv "$TMP_VENV"
"$TMP_VENV/bin/pip" install --quiet --disable-pip-version-check "grpcio-tools==1.62.3"
"$TMP_VENV/bin/python" -m grpc_tools.protoc \
  -I"$ROOT_DIR/proto" \
  --python_out="$PY_OUT_DIR" \
  --grpc_python_out="$PY_OUT_DIR" \
  "$PROTO_SRC"

echo "Synchronized proto into TypeScript and Python SDKs."
