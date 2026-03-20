"""Generated protobuf/gRPC code. Do not edit manually."""
import sys as _sys
from pathlib import Path as _Path

# Add this directory to sys.path so that the generated gRPC code
# can resolve `from rune.wire.v1 import rune_pb2` correctly.
_proto_dir = str(_Path(__file__).resolve().parent)
if _proto_dir not in _sys.path:
    _sys.path.insert(0, _proto_dir)
