#!/bin/bash
# 从 proto 生成 Python gRPC 代码
set -e
cd "$(dirname "$0")"

python3 -m grpc_tools.protoc \
    -I../../proto \
    --python_out=. \
    --grpc_python_out=. \
    ../../proto/rune/wire/v1/rune.proto

echo "Generated Python gRPC code."
