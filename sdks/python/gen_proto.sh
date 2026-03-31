#!/bin/bash
# Generate Python gRPC code from proto/
set -e
cd "$(dirname "$0")"

python3 -m grpc_tools.protoc \
    -I../../proto \
    --python_out=src/rune/_proto \
    --grpc_python_out=src/rune/_proto \
    ../../proto/rune/wire/v1/rune.proto

echo "Generated Python gRPC code in src/rune/_proto/"
