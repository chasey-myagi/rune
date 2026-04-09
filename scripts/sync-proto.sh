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

python3 - "$PY_OUT_DIR/rune/wire/v1/rune_pb2_grpc.py" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()
text = text.replace(
    "from rune.wire.v1 import rune_pb2 as rune_dot_wire_dot_v1_dot_rune__pb2",
    "from . import rune_pb2 as rune_dot_wire_dot_v1_dot_rune__pb2",
)
path.write_text(text)
PY

echo "Synchronized proto into TypeScript and Python SDKs."
